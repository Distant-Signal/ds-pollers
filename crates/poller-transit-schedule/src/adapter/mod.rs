//! Source adapters (transit spec §6.2): each turns a network's published
//! timetable into a [`crate::model::TransitModel`]: GTFS ([`gtfs`],
//! Metrolink from BODS) and `TransXChange` ([`txc`], Nottingham NET from
//! TNDS, fetched over FTP by [`ftp`]; NET design §4, §5).
//!
//! Upstream data is untrusted (spec §13.5): every download is capped, and
//! every zip entry is read through a cap ([`CappedReader`]), so a zip bomb
//! costs bounded time and the parse streams, never holding a whole file.

pub(crate) mod diagnostics;
pub(crate) mod ftp;
pub(crate) mod gtfs;
pub(crate) mod sirisx;
pub(crate) mod txc;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use self::diagnostics::{
    Class, Failure, PREVIEW_BYTES, ResponseInfo, Seen, attempted, report, retry_pause,
    safe_url_str, unreadable_body,
};

/// Why a feed was refused. Each is counted as
/// `transit_feed_refused_total{network, reason}` ([`FeedError::reason`]),
/// and the previous version stays active.
#[derive(Debug)]
pub(crate) enum FeedError {
    /// A route of the network that no line and no replacement-bus name in
    /// the network file claims: a new line needs a config change.
    UnmappedRoute(String),
    /// A download or a zip entry over its cap.
    Oversize(String),
    /// A file is missing or does not parse.
    Malformed(String),
    /// The download or the file failed.
    Io(String),
}

impl std::error::Error for FeedError {}

impl std::fmt::Display for FeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnmappedRoute(name) => write!(
                f,
                "route {name:?} maps to no line in the network file (unmapped_route)"
            ),
            Self::Oversize(what) => write!(f, "{what} is over its cap"),
            Self::Malformed(what) => write!(f, "malformed feed: {what}"),
            Self::Io(what) => write!(f, "{what}"),
        }
    }
}

impl FeedError {
    /// The `reason` label.
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::UnmappedRoute(_) => "unmapped_route",
            Self::Oversize(_) => "oversize",
            Self::Malformed(_) => "malformed",
            Self::Io(_) => "fetch",
        }
    }
}

/// Reads at most `limit` bytes from `inner`, then fails (a zip bomb guard:
/// the declared size of a zip entry is the attacker's to choose).
pub(crate) struct CappedReader<R> {
    inner: R,
    left: u64,
    what: String,
}

impl<R: Read> CappedReader<R> {
    pub(crate) fn new(inner: R, limit: u64, what: impl Into<String>) -> Self {
        Self {
            inner,
            left: limit,
            what: what.into(),
        }
    }
}

impl<R: Read> Read for CappedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.left == 0 {
            // One more byte would be over the cap: is there one?
            let mut probe = [0u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => Err(std::io::Error::other(format!(
                    "{} is over its inflated-size cap",
                    self.what
                ))),
            };
        }
        let want = usize::try_from(self.left)
            .unwrap_or(usize::MAX)
            .min(buf.len());
        let n = self.inner.read(&mut buf[..want])?;
        self.left -= n as u64;
        Ok(n)
    }
}

/// Where a download to `path` is written until it is complete
/// (`feed.zip.part`).
pub(crate) fn part_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

/// Finishes a download written to `part` ([`part_path`]): a complete one is
/// renamed to `path`; after a failed one neither file is left behind (a
/// partial feed can be most of 159 MB).
pub(crate) async fn finish_download(
    result: Result<u64, FeedError>,
    part: &Path,
    path: &Path,
) -> Result<u64, FeedError> {
    match result {
        Ok(bytes) => match tokio::fs::rename(part, path).await {
            Ok(()) => Ok(bytes),
            Err(err) => {
                let _ = tokio::fs::remove_file(part).await;
                Err(FeedError::Io(format!(
                    "moving {} into place: {err}",
                    part.display()
                )))
            }
        },
        Err(err) => {
            let _ = tokio::fs::remove_file(part).await;
            let _ = tokio::fs::remove_file(path).await;
            Err(err)
        }
    }
}

/// The download attempts a zip gets before the run fails (a truncated
/// body is the usual cause of a retry).
pub(crate) const DOWNLOAD_ATTEMPTS: u32 = 3;
/// The pause before attempt `n + 1` is `n` times this, plus up to half of
/// it again as jitter.
pub(crate) const DOWNLOAD_BACKOFF: Duration = Duration::from_secs(5);
/// No retry starts once the first attempt began this long ago. An attempt
/// runs up to the request timeout ([`crate::REQUEST_TIMEOUT`]), so this
/// keeps a run of slow failures from piling up retries.
pub(crate) const DOWNLOAD_RETRY_WINDOW: Duration = Duration::from_secs(10 * 60);

/// How [`download_zip_to_file`] (and the situations fetch) retries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Retry<'a> {
    /// The `network` label of `transit_feed_download_attempts_total`.
    pub network: &'a str,
    /// Its `source` label.
    pub source: &'a str,
    pub attempts: u32,
    pub backoff: Duration,
    pub window: Duration,
}

impl<'a> Retry<'a> {
    pub(crate) fn new(network: &'a str, source: &'a str) -> Self {
        Self {
            network,
            source,
            attempts: DOWNLOAD_ATTEMPTS,
            backoff: DOWNLOAD_BACKOFF,
            window: DOWNLOAD_RETRY_WINDOW,
        }
    }
}

/// A body written in full.
struct Fetched {
    bytes: u64,
    seen: Seen,
}

/// Downloads `url` to `path`, as one attempt without the zip check (a
/// test helper: [`download_zip_to_file`] is the downloader).
#[cfg(test)]
pub(crate) async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    max_bytes: u64,
) -> Result<u64, FeedError> {
    let result = fetch_to_file(client, url, path, max_bytes).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(path).await;
    }
    result.map(|fetched| fetched.bytes).map_err(|f| f.err)
}

/// Whether `path` is a zip whose central directory reads (a truncated
/// download has lost its end-of-central-directory record).
async fn check_archive(path: &Path) -> Result<(), String> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let file = std::fs::File::open(&path)
            .map_err(|err| format!("opening {}: {err}", path.display()))?;
        zip::ZipArchive::new(std::io::BufReader::new(file))
            .map(|_| ())
            .map_err(|err| err.to_string())
    })
    .await
    .map_err(|err| format!("the archive check failed: {err}"))?
}

/// Downloads the zip at `url` to `path` and checks it is complete before
/// returning: the byte count must equal a declared `Content-Length`, and
/// the archive's central directory must read. Each failed attempt logs
/// what the response looked like ([`diagnostics::report`]) and is counted
/// by [`Class`]. A truncated body (a cut connection, a short body, a
/// missing end record) is downloaded again, up to `retry.attempts` times
/// with a growing, jittered pause and never past `retry.window`; a body
/// that is not a zip at all (an error page) is retried once; a 429 or 5xx
/// is retried honouring a short `Retry-After`; any other status, and an
/// over-cap body, fail at once. The bytes; after a failure no file is left
/// at `path`, and nothing was parsed.
pub(crate) async fn download_zip_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    max_bytes: u64,
    retry: &Retry<'_>,
) -> Result<u64, FeedError> {
    let started = std::time::Instant::now();
    let attempts = retry.attempts.max(1);
    let mut attempt = 1;
    loop {
        let failure = match fetch_to_file(client, url, path, max_bytes).await {
            Ok(fetched) => match check_archive(path).await {
                Ok(()) => {
                    attempted(retry.network, retry.source, "ok");
                    tracing::info!(
                        network = retry.network,
                        attempt,
                        received_bytes = fetched.bytes,
                        expected_bytes = ?fetched.seen.declared,
                        "downloaded the timetable"
                    );
                    return Ok(fetched.bytes);
                }
                Err(zip_error) => unreadable_body(fetched.seen, &zip_error, ""),
            },
            Err(failure) => failure,
        };
        report(retry.network, retry.source, attempt, &failure);
        let _ = tokio::fs::remove_file(path).await;
        let Some(pause) = retry_pause(
            &failure,
            attempt,
            attempts,
            retry.backoff,
            started.elapsed(),
            retry.window,
        ) else {
            return Err(failure.err);
        };
        tokio::time::sleep(pause).await;
        attempt += 1;
    }
}

#[expect(clippy::too_many_lines, reason = "one request, checked at each step")]
async fn fetch_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    max_bytes: u64,
) -> Result<Fetched, Box<Failure>> {
    let target = safe_url_str(url);
    let mut seen = Seen::default();
    let fail = |seen: &Seen, class, retryable, err| {
        Box::new(Failure {
            err,
            class,
            retryable,
            seen: seen.clone(),
        })
    };
    // A transport error, which another attempt may not repeat (reqwest's
    // own text names the URL, query included, so it is left out).
    let transport = |seen: &Seen, err: reqwest::Error| {
        let class = if seen.info.is_some() {
            Class::TruncatedZip
        } else {
            Class::Error
        };
        fail(
            seen,
            class,
            true,
            FeedError::Io(format!("downloading {target}: {}", err.without_url())),
        )
    };
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|err| transport(&seen, err))?;
    let info = ResponseInfo::from_response(&response);
    seen.info = Some(info.clone());
    let status = response.status();
    if !status.is_success() {
        // Counts an unfollowed redirect (`upstream_redirect_refused_total`).
        let _ = poller_http::check_status(&response, "transit-schedule");
        let after = info
            .retry_after
            .map_or_else(String::new, |d| format!(", retry-after {}s", d.as_secs()));
        return Err(fail(
            &seen,
            Class::HttpStatus,
            status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
            FeedError::Io(format!("downloading {target}: HTTP {status}{after}")),
        ));
    }
    seen.declared = response.content_length();
    if let Some(declared) = seen.declared
        && declared > max_bytes
    {
        return Err(fail(
            &seen,
            Class::Error,
            false,
            FeedError::Oversize(format!(
                "{target} (Content-Length {declared}, cap {max_bytes})"
            )),
        ));
    }
    let local = |seen: &Seen, what: &str, err: std::io::Error| {
        fail(
            seen,
            Class::Error,
            false,
            FeedError::Io(format!("{what} {}: {err}", path.display())),
        )
    };
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| local(&seen, "creating", err))?;
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(err) => return Err(transport(&seen, err)),
        };
        if seen.head.len() < PREVIEW_BYTES {
            let take = (PREVIEW_BYTES - seen.head.len()).min(chunk.len());
            seen.head.extend_from_slice(&chunk[..take]);
        }
        seen.received += chunk.len() as u64;
        if seen.received > max_bytes {
            return Err(fail(
                &seen,
                Class::Error,
                false,
                FeedError::Oversize(format!("{target} (over {max_bytes} bytes while streaming)")),
            ));
        }
        file.write_all(&chunk)
            .await
            .map_err(|err| local(&seen, "writing", err))?;
    }
    file.flush()
        .await
        .map_err(|err| local(&seen, "writing", err))?;
    if let Some(declared) = seen.declared
        && declared != seen.received
    {
        return Err(fail(
            &seen,
            Class::TruncatedZip,
            true,
            FeedError::Io(format!(
                "downloading {target}: received {} of {declared} bytes",
                seen.received
            )),
        ));
    }
    Ok(Fetched {
        bytes: seen.received,
        seen,
    })
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[test]
    fn a_capped_reader_stops_at_its_cap() {
        let mut ok = CappedReader::new(&[1u8; 10][..], 10, "x");
        let mut out = Vec::new();
        assert_eq!(ok.read_to_end(&mut out).unwrap(), 10);
        let mut over = CappedReader::new(&[1u8; 11][..], 10, "stop_times.txt");
        let err = over.read_to_end(&mut Vec::new()).unwrap_err();
        assert!(err.to_string().contains("stop_times.txt"), "{err}");
    }

    async fn serve(body: Vec<u8>, chunked: bool) -> MockServer {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(200).set_body_bytes(body);
        if chunked {
            response = response.append_header("transfer-encoding", "chunked");
        }
        Mock::given(method("GET"))
            .respond_with(response)
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn downloads_are_capped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let client = reqwest::Client::new();

        let server = serve(vec![7; 16], false).await;
        assert_eq!(
            download_to_file(&client, &server.uri(), &path, 1024)
                .await
                .unwrap(),
            16
        );
        assert_eq!(std::fs::read(&path).unwrap(), vec![7; 16]);

        let server = serve(vec![1; 2048], false).await;
        let err = download_to_file(&client, &server.uri(), &path, 1024)
            .await
            .unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");

        let server = serve(vec![1; 4096], true).await;
        let err = download_to_file(&client, &server.uri(), &path, 1024)
            .await
            .unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(!path.exists(), "a partial download is deleted");
    }

    #[tokio::test]
    async fn a_body_cut_short_leaves_no_partial_file() {
        // A Content-Length longer than the body: the stream fails part way.
        use std::io::Write as _;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nPK partial")
                .unwrap();
            socket.shutdown(std::net::Shutdown::Both).unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let err = download_to_file(
            &reqwest::Client::new(),
            &format!("http://{addr}/"),
            &path,
            1024,
        )
        .await
        .unwrap_err();
        assert_eq!(err.reason(), "fetch", "{err}");
        assert!(!path.exists(), "a partial download is deleted");
    }

    #[tokio::test]
    async fn a_download_lands_under_its_name_only_when_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let part = part_path(&path);
        assert_eq!(part, dir.path().join("feed.zip.part"));

        std::fs::write(&part, b"whole").unwrap();
        assert_eq!(finish_download(Ok(5), &part, &path).await.unwrap(), 5);
        assert_eq!(std::fs::read(&path).unwrap(), b"whole");
        assert!(!part.exists());

        // A failure leaves neither the part nor an older feed behind.
        std::fs::write(&part, b"wh").unwrap();
        let err = finish_download(Err(FeedError::Io("cut".to_owned())), &part, &path)
            .await
            .unwrap_err();
        assert_eq!(err.reason(), "fetch");
        assert!(!part.exists());
        assert!(!path.exists());
    }

    // -- download_zip_to_file: scripted raw responses, one per connection.

    fn small_zip() -> Vec<u8> {
        use std::io::Write as _;
        let mut buf = Vec::new();
        {
            let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            writer
                .start_file("agency.txt", zip::write::SimpleFileOptions::default())
                .unwrap();
            writer.write_all(&[b'x'; 2000]).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    /// What one connection of [`scripted`] sends.
    struct Reply {
        status: &'static str,
        /// The `Content-Length` header (`None`: the body ends at the close).
        declared: Option<usize>,
        headers: Vec<&'static str>,
        body: Vec<u8>,
    }

    impl Reply {
        fn ok(declared: Option<usize>, body: Vec<u8>) -> Self {
            Self {
                status: "200 OK",
                declared,
                headers: Vec::new(),
                body,
            }
        }
    }

    /// Serves `replies` in order, one per connection (the last repeats),
    /// each closed after its body. The URL (with a query, as a key would
    /// be), and the connection count.
    async fn scripted(
        replies: Vec<Reply>,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::fmt::Write as _;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/feed?api_key=SECRET",
            listener.local_addr().unwrap()
        );
        let served = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&served);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let n = count.fetch_add(1, Ordering::SeqCst);
                let reply = &replies[n.min(replies.len() - 1)];
                let mut request = [0u8; 2048];
                let _ = socket.read(&mut request).await;
                let mut head = format!("HTTP/1.1 {}\r\nConnection: close\r\n", reply.status);
                if let Some(declared) = reply.declared {
                    let _ = write!(head, "Content-Length: {declared}\r\n");
                }
                for header in &reply.headers {
                    head.push_str(header);
                    head.push_str("\r\n");
                }
                head.push_str("\r\n");
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&reply.body).await;
                let _ = socket.shutdown().await;
            }
        });
        (url, served)
    }

    fn connections(served: &std::sync::atomic::AtomicUsize) -> usize {
        served.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn fast_retry() -> Retry<'static> {
        Retry {
            network: "test",
            source: "gtfs",
            attempts: 3,
            backoff: Duration::from_millis(5),
            window: Duration::from_secs(30),
        }
    }

    fn cut(zip: &[u8], declared: bool) -> Reply {
        Reply::ok(
            declared.then_some(zip.len()),
            zip[..zip.len() - 40].to_vec(),
        )
    }

    fn whole(zip: &[u8]) -> Reply {
        Reply::ok(Some(zip.len()), zip.to_vec())
    }

    async fn run(url: &str, dir: &tempfile::TempDir) -> (Result<u64, FeedError>, PathBuf) {
        let path = dir.path().join("feed.zip");
        let result =
            download_zip_to_file(&reqwest::Client::new(), url, &path, 1 << 20, &fast_retry()).await;
        (result, path)
    }

    #[tokio::test]
    async fn a_truncated_body_is_downloaded_again() {
        let zip = small_zip();
        // A cut connection (a Content-Length the body falls short of), then
        // a body that ends cleanly without its end record, then the file.
        let (url, served) = scripted(vec![cut(&zip, true), cut(&zip, false), whole(&zip)]).await;
        let dir = tempfile::tempdir().unwrap();
        let (result, path) = run(&url, &dir).await;
        assert_eq!(result.unwrap(), zip.len() as u64);
        assert_eq!(std::fs::read(path).unwrap(), zip);
        assert_eq!(connections(&served), 3);
    }

    #[tokio::test]
    async fn a_persistently_truncated_body_fails_after_the_attempts_leaving_nothing() {
        let zip = small_zip();
        let (url, served) = scripted(vec![cut(&zip, false)]).await;
        let dir = tempfile::tempdir().unwrap();
        let (result, path) = run(&url, &dir).await;
        let err = result.unwrap_err();
        // The same refusal as before the retries: the alerts key on it.
        assert_eq!(err.reason(), "malformed", "{err}");
        assert!(err.to_string().contains("zip"), "{err}");
        assert_eq!(connections(&served), 3);
        assert!(!path.exists(), "a truncated feed is never left to parse");

        let (url, served) = scripted(vec![cut(&zip, true)]).await;
        let (result, path) = run(&url, &dir).await;
        let err = result.unwrap_err();
        assert_eq!(err.reason(), "fetch");
        assert!(!err.to_string().contains("SECRET"), "{err}");
        assert_eq!(connections(&served), 3);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn without_a_content_length_the_archive_is_the_check() {
        let zip = small_zip();
        let (url, served) = scripted(vec![Reply::ok(None, zip.clone())]).await;
        let dir = tempfile::tempdir().unwrap();
        let (result, _) = run(&url, &dir).await;
        assert_eq!(result.unwrap(), zip.len() as u64);
        assert_eq!(connections(&served), 1);

        // Cut, no length to compare: the archive check catches it and retries.
        let (url, served) = scripted(vec![cut(&zip, false), Reply::ok(None, zip.clone())]).await;
        let (result, _) = run(&url, &dir).await;
        assert_eq!(result.unwrap(), zip.len() as u64);
        assert_eq!(connections(&served), 2);
    }

    #[tokio::test]
    async fn a_page_that_is_not_a_zip_is_tried_once_more_then_refused() {
        // Complete by its Content-Length: an error page, not a cut file.
        let body = b"<html>Access denied</html>".to_vec();
        let mut reply = Reply::ok(Some(body.len()), body);
        reply.headers.push("Content-Type: text/html");
        let (url, served) = scripted(vec![reply]).await;
        let dir = tempfile::tempdir().unwrap();
        let (result, path) = run(&url, &dir).await;
        let err = result.unwrap_err();
        assert_eq!(err.reason(), "malformed", "{err}");
        let text = err.to_string();
        assert!(text.contains("not a zip"), "{text}");
        assert!(text.contains("HTTP 200"), "{text}");
        assert!(text.contains("text/html"), "{text}");
        assert!(!text.contains("SECRET"), "{text}");
        assert_eq!(connections(&served), 2, "one retry, not three");
        assert!(!path.exists());

        // An empty body is the same class; a good file after it succeeds.
        let zip = small_zip();
        let (url, served) = scripted(vec![Reply::ok(Some(0), Vec::new()), whole(&zip)]).await;
        let (result, _) = run(&url, &dir).await;
        assert_eq!(result.unwrap(), zip.len() as u64);
        assert_eq!(connections(&served), 2);
    }

    #[tokio::test]
    async fn a_rate_limit_is_retried_after_its_retry_after() {
        let zip = small_zip();
        let mut limited = Reply::ok(Some(0), Vec::new());
        limited.status = "429 Too Many Requests";
        limited.headers.push("Retry-After: 1");
        let (url, served) = scripted(vec![limited, whole(&zip)]).await;
        let dir = tempfile::tempdir().unwrap();
        let started = std::time::Instant::now();
        let (result, _) = run(&url, &dir).await;
        assert_eq!(result.unwrap(), zip.len() as u64);
        assert_eq!(connections(&served), 2);
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "Retry-After is honoured"
        );

        // Not retryable: the status is the answer.
        let mut gone = Reply::ok(Some(0), Vec::new());
        gone.status = "404 Not Found";
        let (url, served) = scripted(vec![gone]).await;
        let (result, path) = run(&url, &dir).await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("HTTP 404"), "{err}");
        assert!(!err.to_string().contains("SECRET"), "{err}");
        assert_eq!(connections(&served), 1);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn a_server_error_is_retried_up_to_the_attempts() {
        let mut broken = Reply::ok(Some(0), Vec::new());
        broken.status = "503 Service Unavailable";
        let (url, served) = scripted(vec![broken]).await;
        let dir = tempfile::tempdir().unwrap();
        let (result, _) = run(&url, &dir).await;
        assert!(result.unwrap_err().to_string().contains("HTTP 503"));
        assert_eq!(connections(&served), 3);
    }

    #[tokio::test]
    async fn the_retry_window_stops_further_attempts() {
        let zip = small_zip();
        let (url, served) = scripted(vec![cut(&zip, false)]).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let retry = Retry {
            window: Duration::ZERO,
            ..fast_retry()
        };
        let result =
            download_zip_to_file(&reqwest::Client::new(), &url, &path, 1 << 20, &retry).await;
        assert!(result.is_err());
        assert_eq!(connections(&served), 1);
    }
}
