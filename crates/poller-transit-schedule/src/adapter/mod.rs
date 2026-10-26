//! Source adapters (transit spec §6.2): each turns a network's published
//! timetable into a [`crate::model::TransitModel`]: GTFS ([`gtfs`],
//! Metrolink from BODS) and `TransXChange` ([`txc`], Nottingham NET from
//! TNDS, fetched over FTP by [`ftp`]; NET design §4, §5).
//!
//! Upstream data is untrusted (spec §13.5): every download is capped, and
//! every zip entry is read through a cap ([`CappedReader`]), so a zip bomb
//! costs bounded time and the parse streams, never holding a whole file.

pub(crate) mod ftp;
pub(crate) mod gtfs;
pub(crate) mod sirisx;
pub(crate) mod txc;

use std::io::Read;
use std::path::Path;

use tokio::io::AsyncWriteExt;

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

/// Downloads `url` to `path` (a zip needs a seekable file), refusing a
/// declared `Content-Length` over `max_bytes` before reading and aborting
/// once the streamed body passes it. The bytes written.
pub(crate) async fn download_to_file(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    max_bytes: u64,
) -> Result<u64, FeedError> {
    let io = |err: &dyn std::fmt::Display| FeedError::Io(format!("downloading {url}: {err}"));
    let mut response = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|err| io(&err))?;
    if let Some(declared) = response.content_length()
        && declared > max_bytes
    {
        return Err(FeedError::Oversize(format!(
            "{url} (Content-Length {declared}, cap {max_bytes})"
        )));
    }
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| FeedError::Io(format!("creating {}: {err}", path.display())))?;
    let mut written: u64 = 0;
    while let Some(chunk) = response.chunk().await.map_err(|err| io(&err))? {
        written += chunk.len() as u64;
        if written > max_bytes {
            drop(file);
            let _ = tokio::fs::remove_file(path).await;
            return Err(FeedError::Oversize(format!(
                "{url} (over {max_bytes} bytes while streaming)"
            )));
        }
        file.write_all(&chunk)
            .await
            .map_err(|err| FeedError::Io(format!("writing {}: {err}", path.display())))?;
    }
    file.flush()
        .await
        .map_err(|err| FeedError::Io(format!("writing {}: {err}", path.display())))?;
    Ok(written)
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
}
