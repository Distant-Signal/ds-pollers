//! A minimal FTP client for the TNDS download (NET design §5): TNDS
//! serves its regional `TransXChange` zips only over FTP, behind a free
//! Traveline login. One file, binary, passive mode (`EPSV`, falling back
//! to `PASV`), under the same byte cap as an HTTPS download. Small enough
//! to need no new dependency.
//!
//! Plain FTP: the login crosses the network in clear, as TNDS offers it.
//! It is a free registration guarding nothing else (the runbook says so).
//! The password never appears in an error or a log line.
//!
//! The data connection goes to the control connection's peer address,
//! whatever address a `PASV` reply names (the usual guard against a server
//! steering the client elsewhere, and against NAT-mangled replies).

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use super::FeedError;

/// The longest reply line read (a hostile server cannot make the client
/// buffer more).
const MAX_REPLY_LINE: u64 = 8 * 1024;
/// The most lines of one (multi-line) reply.
const MAX_REPLY_LINES: usize = 200;
/// Connecting, to either connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// An FTP login. `Debug` keeps the password out.
#[derive(Clone)]
pub(crate) struct Login {
    pub user: String,
    pub password: String,
}

impl std::fmt::Debug for Login {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Login")
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

/// An `ftp://host[:port]/path` URL, split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FtpUrl {
    pub host: String,
    pub port: u16,
    /// Relative to the login directory (RFC 1738), so no leading `/`.
    pub path: String,
}

impl FtpUrl {
    /// Parses `url`, which must be `ftp://` with a host and a file path and
    /// no credentials (they come from the Secret, never from a value).
    pub(crate) fn parse(url: &str) -> Result<Self, FeedError> {
        let bad = |why: &str| FeedError::Io(format!("bad FTP URL: {why}"));
        let parsed = reqwest::Url::parse(url).map_err(|_| bad("does not parse"))?;
        if parsed.scheme() != "ftp" {
            return Err(bad("not ftp://"));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(bad("credentials belong in the Secret, not the URL"));
        }
        let host = parsed
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or_else(|| bad("no host"))?
            .to_owned();
        let path = parsed.path().trim_start_matches('/').to_owned();
        if path.is_empty() || path.contains(['\r', '\n']) {
            return Err(bad("no file path"));
        }
        Ok(Self {
            host,
            port: parsed.port().unwrap_or(21),
            path,
        })
    }
}

fn io(host: &str, what: impl std::fmt::Display) -> FeedError {
    FeedError::Io(format!("FTP {host}: {what}"))
}

/// The control connection.
struct Control {
    host: String,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    peer: SocketAddr,
}

impl Control {
    async fn connect(host: &str, port: u16) -> Result<Self, FeedError> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect((host, port)))
            .await
            .map_err(|_| io(host, "connect timed out"))?
            .map_err(|err| io(host, format!("connect: {err}")))?;
        let peer = stream
            .peer_addr()
            .map_err(|err| io(host, format!("peer address: {err}")))?;
        let (read, writer) = stream.into_split();
        let mut control = Self {
            host: host.to_owned(),
            reader: BufReader::new(read),
            writer,
            peer,
        };
        control.expect_reply(&[220], "greeting").await?;
        Ok(control)
    }

    /// One whole reply: `ddd text`, or `ddd-text` lines up to the one that
    /// starts `ddd ` (RFC 959 §4.2).
    async fn reply(&mut self) -> Result<(u16, String), FeedError> {
        let mut first: Option<(u16, String)> = None;
        for _ in 0..MAX_REPLY_LINES {
            let mut line = Vec::new();
            let n = (&mut self.reader)
                .take(MAX_REPLY_LINE)
                .read_until(b'\n', &mut line)
                .await
                .map_err(|err| io(&self.host, format!("reading a reply: {err}")))?;
            if n == 0 {
                return Err(io(&self.host, "the server closed the connection"));
            }
            if line.last() != Some(&b'\n') {
                return Err(io(&self.host, "a reply line over its cap"));
            }
            let text = String::from_utf8_lossy(&line).trim_end().to_owned();
            let code = text
                .get(..3)
                .and_then(|c| c.parse::<u16>().ok())
                .filter(|_| text.len() == 3 || matches!(text.as_bytes()[3], b' ' | b'-'));
            match (&first, code) {
                (None, Some(code)) => {
                    if text.as_bytes().get(3) != Some(&b'-') {
                        return Ok((code, text));
                    }
                    first = Some((code, text));
                }
                (None, None) => return Err(io(&self.host, format!("malformed reply {text:?}"))),
                (Some((want, _)), Some(code))
                    if code == *want && text.as_bytes().get(3) != Some(&b'-') =>
                {
                    return Ok((code, text));
                }
                (Some(_), _) => {}
            }
        }
        Err(io(&self.host, "a reply of too many lines"))
    }

    async fn expect_reply(&mut self, ok: &[u16], what: &str) -> Result<String, FeedError> {
        let (code, text) = self.reply().await?;
        if ok.contains(&code) {
            Ok(text)
        } else {
            Err(io(&self.host, format!("{what}: {text}")))
        }
    }

    async fn send(&mut self, command: &str) -> Result<(u16, String), FeedError> {
        self.writer
            .write_all(format!("{command}\r\n").as_bytes())
            .await
            .map_err(|err| io(&self.host, format!("sending a command: {err}")))?;
        self.reply().await
    }

    /// Sends `command` and expects one of `ok`; `what` names it in an
    /// error (the command itself may hold the password).
    async fn command(
        &mut self,
        command: &str,
        ok: &[u16],
        what: &str,
    ) -> Result<String, FeedError> {
        let (code, text) = self.send(command).await?;
        if ok.contains(&code) {
            Ok(text)
        } else {
            Err(io(&self.host, format!("{what}: {text}")))
        }
    }

    async fn login(&mut self, login: &Login) -> Result<(), FeedError> {
        if login.user.contains(['\r', '\n']) || login.password.contains(['\r', '\n']) {
            return Err(io(&self.host, "the login holds a line break"));
        }
        let (code, text) = self.send(&format!("USER {}", login.user)).await?;
        match code {
            230 => Ok(()),
            331 | 332 => {
                self.command(
                    &format!("PASS {}", login.password),
                    &[230, 202],
                    "login refused",
                )
                .await?;
                Ok(())
            }
            _ => Err(io(&self.host, format!("login refused: {text}"))),
        }
    }

    /// Opens the passive data connection: `EPSV`, else `PASV`, always to
    /// the control connection's peer.
    async fn data_connection(&mut self) -> Result<TcpStream, FeedError> {
        let (code, text) = self.send("EPSV").await?;
        let port = if code == 229 {
            parse_epsv(&text).ok_or_else(|| io(&self.host, format!("EPSV reply {text:?}")))?
        } else {
            let text = self.command("PASV", &[227], "passive mode refused").await?;
            parse_pasv(&text).ok_or_else(|| io(&self.host, format!("PASV reply {text:?}")))?
        };
        let addr = SocketAddr::new(self.peer.ip(), port);
        tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| io(&self.host, "data connection timed out"))?
            .map_err(|err| io(&self.host, format!("data connection: {err}")))
    }
}

/// The port of `229 Entering Extended Passive Mode (|||6446|)`.
fn parse_epsv(text: &str) -> Option<u16> {
    let inner = text.split_once('(')?.1.split_once(')')?.0;
    let delimiter = inner.chars().next()?;
    let fields: Vec<&str> = inner.split(delimiter).collect();
    // "|||6446|" splits to ["", "", "", "6446", ""].
    fields.get(3)?.parse().ok().filter(|p| *p != 0)
}

/// The port of `227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)`: the last
/// six numbers after the reply code (servers vary the wrapping).
fn parse_pasv(text: &str) -> Option<u16> {
    let numbers: Vec<u32> = text
        .get(4..)?
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().ok())
        .collect::<Option<_>>()?;
    let six = numbers.get(numbers.len().checked_sub(6)?..)?;
    if six.iter().any(|n| *n > 255) {
        return None;
    }
    u16::try_from(six[4] * 256 + six[5])
        .ok()
        .filter(|p| *p != 0)
}

/// Downloads `url` to `path` as `login`, refusing a file over `max_bytes`
/// (by its `SIZE` when the server answers it, and while streaming), all
/// within `timeout`. The bytes written; a partial file is deleted.
pub(crate) async fn download_to_file(
    url: &str,
    login: &Login,
    path: &Path,
    max_bytes: u64,
    timeout: Duration,
) -> Result<u64, FeedError> {
    let target = FtpUrl::parse(url)?;
    let result = tokio::time::timeout(timeout, fetch(&target, login, path, max_bytes))
        .await
        .unwrap_or_else(|_| Err(io(&target.host, format!("over the {timeout:?} timeout"))));
    if result.is_err() {
        let _ = tokio::fs::remove_file(path).await;
    }
    result
}

async fn fetch(
    target: &FtpUrl,
    login: &Login,
    path: &Path,
    max_bytes: u64,
) -> Result<u64, FeedError> {
    let mut control = Control::connect(&target.host, target.port).await?;
    control.login(login).await?;
    control
        .command("TYPE I", &[200], "binary mode refused")
        .await?;
    let (code, text) = control.send(&format!("SIZE {}", target.path)).await?;
    if code == 213
        && let Some(size) = text.get(4..).and_then(|s| s.trim().parse::<u64>().ok())
        && size > max_bytes
    {
        return Err(FeedError::Oversize(format!(
            "ftp://{}/{} (SIZE {size}, cap {max_bytes})",
            target.host, target.path
        )));
    }
    let mut data = control.data_connection().await?;
    control
        .command(
            &format!("RETR {}", target.path),
            &[125, 150],
            "download refused",
        )
        .await?;
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| FeedError::Io(format!("creating {}: {err}", path.display())))?;
    let mut written: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = data
            .read(&mut buf)
            .await
            .map_err(|err| io(&target.host, format!("reading the file: {err}")))?;
        if n == 0 {
            break;
        }
        written += n as u64;
        if written > max_bytes {
            return Err(FeedError::Oversize(format!(
                "ftp://{}/{} (over {max_bytes} bytes while streaming)",
                target.host, target.path
            )));
        }
        file.write_all(&buf[..n])
            .await
            .map_err(|err| FeedError::Io(format!("writing {}: {err}", path.display())))?;
    }
    drop(data);
    file.flush()
        .await
        .map_err(|err| FeedError::Io(format!("writing {}: {err}", path.display())))?;
    control
        .expect_reply(&[226, 250], "the transfer did not complete")
        .await?;
    let _ = control.send("QUIT").await;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::net::TcpListener;

    use super::*;

    /// How the fake server behaves.
    #[derive(Clone)]
    struct Fake {
        file: Vec<u8>,
        epsv: bool,
        size: bool,
        multi_line_greeting: bool,
    }

    impl Default for Fake {
        fn default() -> Self {
            Self {
                file: b"PK zip bytes".to_vec(),
                epsv: true,
                size: true,
                multi_line_greeting: false,
            }
        }
    }

    /// A one-session FTP server on localhost for `user`/`secret`, serving
    /// `EM.zip`. Its URL.
    async fn serve(fake: Fake) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let fake = Arc::new(fake);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            if fake.multi_line_greeting {
                write
                    .write_all(b"220-Welcome\r\n Terms apply\r\n220 Ready\r\n")
                    .await
                    .unwrap();
            } else {
                write.write_all(b"220 Ready\r\n").await.unwrap();
            }
            let mut data: Option<TcpListener> = None;
            while let Ok(Some(line)) = lines.next_line().await {
                let (verb, arg) = line.split_once(' ').unwrap_or((line.as_str(), ""));
                let reply = match verb {
                    "USER" if arg == "user" => "331 Password please".to_owned(),
                    "USER" => "530 No".to_owned(),
                    "PASS" if arg == "secret" => "230 In".to_owned(),
                    "PASS" => "530 Login incorrect".to_owned(),
                    "TYPE" => "200 Binary".to_owned(),
                    "SIZE" if fake.size && arg == "EM.zip" => format!("213 {}", fake.file.len()),
                    "SIZE" => "550 No size".to_owned(),
                    "EPSV" if fake.epsv => {
                        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let port = listener.local_addr().unwrap().port();
                        data = Some(listener);
                        format!("229 Entering Extended Passive Mode (|||{port}|)")
                    }
                    "EPSV" => "500 Unknown command".to_owned(),
                    "PASV" => {
                        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                        let port = listener.local_addr().unwrap().port();
                        data = Some(listener);
                        // A bogus address: the client must use the peer's.
                        format!(
                            "227 Entering Passive Mode (10,9,8,7,{},{})",
                            port / 256,
                            port % 256
                        )
                    }
                    "RETR" if arg == "EM.zip" => {
                        write.write_all(b"150 Opening\r\n").await.unwrap();
                        let (mut conn, _) = data.take().unwrap().accept().await.unwrap();
                        conn.write_all(&fake.file).await.unwrap();
                        drop(conn);
                        "226 Transfer complete".to_owned()
                    }
                    "RETR" => "550 No such file".to_owned(),
                    "QUIT" => {
                        let _ = write.write_all(b"221 Bye\r\n").await;
                        return;
                    }
                    _ => "502 Not implemented".to_owned(),
                };
                if write
                    .write_all(format!("{reply}\r\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        format!("ftp://127.0.0.1:{port}/EM.zip")
    }

    fn login(password: &str) -> Login {
        Login {
            user: "user".to_owned(),
            password: password.to_owned(),
        }
    }

    async fn download(fake: Fake, password: &str, cap: u64) -> (Result<u64, FeedError>, Vec<u8>) {
        let url = serve(fake).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let result =
            download_to_file(&url, &login(password), &path, cap, Duration::from_secs(10)).await;
        let bytes = std::fs::read(&path).unwrap_or_default();
        (result, bytes)
    }

    #[tokio::test]
    async fn a_file_is_fetched_in_extended_passive_mode() {
        let (result, bytes) = download(Fake::default(), "secret", 1024).await;
        assert_eq!(result.unwrap(), 12);
        assert_eq!(bytes, b"PK zip bytes");
    }

    #[tokio::test]
    async fn pasv_is_the_fallback_and_its_address_is_ignored() {
        let fake = Fake {
            epsv: false,
            multi_line_greeting: true,
            ..Fake::default()
        };
        let (result, bytes) = download(fake, "secret", 1024).await;
        assert_eq!(result.unwrap(), 12);
        assert_eq!(bytes, b"PK zip bytes");
    }

    #[tokio::test]
    async fn a_refused_login_fails_without_the_password() {
        let (result, bytes) = download(Fake::default(), "wrong-password", 1024).await;
        let err = result.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("530"), "{text}");
        assert!(!text.contains("wrong-password"), "{text}");
        assert!(bytes.is_empty());
        assert!(!format!("{:?}", login("wrong-password")).contains("wrong-password"));
    }

    #[tokio::test]
    async fn the_cap_applies_by_size_and_while_streaming() {
        let big = Fake {
            file: vec![7; 4096],
            ..Fake::default()
        };
        let (result, _) = download(big.clone(), "secret", 1024).await;
        let err = result.unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(err.to_string().contains("SIZE 4096"), "{err}");

        let no_size = Fake { size: false, ..big };
        let (result, bytes) = download(no_size, "secret", 1024).await;
        let err = result.unwrap_err();
        assert_eq!(err.reason(), "oversize", "{err}");
        assert!(bytes.is_empty(), "a partial download is deleted");
    }

    #[test]
    fn urls_and_replies_parse() {
        assert_eq!(
            FtpUrl::parse("ftp://ftp.tnds.co.uk/EM.zip").unwrap(),
            FtpUrl {
                host: "ftp.tnds.co.uk".to_owned(),
                port: 21,
                path: "EM.zip".to_owned()
            }
        );
        assert_eq!(
            FtpUrl::parse("ftp://h:2121/a/EM.zip").unwrap().path,
            "a/EM.zip"
        );
        for bad in [
            "https://ftp.tnds.co.uk/EM.zip",
            "ftp://ftp.tnds.co.uk/",
            "ftp://user:pw@ftp.tnds.co.uk/EM.zip",
            "not a url",
        ] {
            assert!(FtpUrl::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(
            parse_epsv("229 Entering Extended Passive Mode (|||6446|)"),
            Some(6446)
        );
        assert_eq!(parse_epsv("229 nothing"), None);
        assert_eq!(
            parse_pasv("227 Entering Passive Mode (213,155,0,230,19,137)"),
            Some(19 * 256 + 137)
        );
        assert_eq!(
            parse_pasv("227 Entering Passive Mode (1,2,3,4,256,1)"),
            None
        );
        assert_eq!(parse_pasv("227 =1,2,3,4,0,21"), Some(21));
    }
}
