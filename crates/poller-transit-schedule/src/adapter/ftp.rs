//! A minimal FTP client for the TNDS download (NET design §5): TNDS
//! serves its regional `TransXChange` zips only over FTP, behind a free
//! Traveline login. One file, binary, passive mode (`EPSV`, falling back
//! to `PASV`), under the same byte cap as an HTTPS download. Hand-written,
//! with rustls for FTPS.
//!
//! **Security** ([`FtpSecurity`], `TRANSIT_TXC_FTP_SECURITY`):
//!
//! - `plain` (the default): no TLS, the login and the file cross the
//!   network in clear, as plain FTP does. The first download of a process
//!   logs a warning. It is a free registration guarding nothing else (the
//!   runbook says so).
//! - `explicit`: explicit FTPS (RFC 4217). `AUTH TLS` is the first command
//!   after the greeting, and `USER`/`PASS` follow only once the control
//!   connection is TLS. If the server answers `AUTH` with a 5xx (it does
//!   not offer FTPS) the download carries on in plain FTP and logs a
//!   warning. Any other failure of the upgrade ends it. A network attacker
//!   can forge that 5xx, so this mode protects against passive listeners
//!   only: use `required` once the provider is known to offer FTPS.
//! - `required`: as `explicit`, but a server that does not complete the
//!   upgrade is an error, raised before `USER` is sent.
//!
//! With TLS on, `PBSZ 0` and `PROT P` follow the login (as curl does), so
//! the data connection is TLS too, handshaken after `RETR` is sent. Many
//! servers require the data connection to resume the control connection's
//! TLS session: both connections share one rustls `ClientConfig`, whose
//! session store resumes by server name (a TLS 1.3 ticket, or a TLS 1.2
//! session id or ticket). A server that ends the data connection without a
//! TLS `close_notify` (common) is accepted only when the transfer reply
//! says it completed and the bytes received equal the `SIZE` the server
//! gave; with no `SIZE` it is an error (a forged close could truncate the
//! file undetected).
//!
//! The server certificate is verified against the system's roots, plus the
//! PEM bundle at `TRANSIT_TXC_FTP_CA_FILE` when set (for a provider whose
//! certificate comes from a private or unlisted CA), for the URL's host
//! name. There is no way to skip verification. The password never appears
//! in an error or a log line.
//!
//! The data connection goes to the control connection's peer address,
//! whatever address a `PASV` reply names (the usual guard against a server
//! steering the client elsewhere, and against NAT-mangled replies).

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf,
};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

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

/// Whether, and how, the connection is protected with TLS (the module
/// header has the modes).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum FtpSecurity {
    /// No TLS.
    #[default]
    Plain,
    /// Explicit FTPS, falling back to plain FTP if the server answers
    /// `AUTH TLS` with a 5xx.
    Explicit,
    /// Explicit FTPS or nothing.
    Required,
}

/// The FTP security settings.
#[derive(Debug, Clone, Default)]
pub(crate) struct Security {
    pub mode: FtpSecurity,
    /// A PEM bundle of extra trusted CA certificates (`None`: only the
    /// system's roots).
    pub ca_file: Option<PathBuf>,
}

/// The TLS client shared by the control and data connections: one
/// `ClientConfig`, so the data connection resumes the control
/// connection's session.
struct Tls {
    connector: TlsConnector,
    name: ServerName<'static>,
}

impl Tls {
    fn new(host: &str, ca_file: Option<&Path>) -> Result<Self, FeedError> {
        let mut roots = RootCertStore::empty();
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
        if let Some(ca_file) = ca_file {
            let bad = |err: &dyn std::fmt::Display| {
                io(host, format!("the CA file {}: {err}", ca_file.display()))
            };
            let pem = std::fs::read(ca_file).map_err(|err| bad(&err))?;
            let mut added = 0_usize;
            for cert in CertificateDer::pem_slice_iter(&pem) {
                let cert = cert.map_err(|err| bad(&err))?;
                roots.add(cert).map_err(|err| bad(&err))?;
                added += 1;
            }
            if added == 0 {
                return Err(bad(&"holds no certificate"));
            }
        }
        if roots.is_empty() {
            return Err(io(host, "no trusted CA certificates for FTPS"));
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|err| io(host, format!("TLS versions: {err}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let name = ServerName::try_from(host.to_owned())
            .map_err(|err| io(host, format!("not a valid TLS server name: {err}")))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            name,
        })
    }

    /// The handshake over `tcp`; the certificate is checked for the
    /// URL's host.
    async fn handshake(
        &self,
        host: &str,
        tcp: TcpStream,
    ) -> Result<TlsStream<TcpStream>, FeedError> {
        tokio::time::timeout(
            CONNECT_TIMEOUT,
            self.connector.connect(self.name.clone(), tcp),
        )
        .await
        .map_err(|_| io(host, "the TLS handshake timed out"))?
        .map_err(|err| io(host, format!("the TLS handshake failed: {err}")))
    }
}

/// A connection, plain or TLS.
enum Conn {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// The control connection.
struct Control {
    host: String,
    stream: BufReader<Conn>,
    peer: SocketAddr,
}

/// How `AUTH TLS` ended.
enum Secured {
    /// The control connection is TLS; the client is for the data
    /// connection too.
    Tls(Control, Tls),
    /// The server answered 5xx and `Explicit` allows plain FTP.
    Declined(Control),
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
        let mut control = Self {
            host: host.to_owned(),
            stream: BufReader::new(Conn::Plain(stream)),
            peer,
        };
        control.expect_reply(&[220], "greeting").await?;
        Ok(control)
    }

    /// Upgrades the control connection to TLS (the `AUTH TLS` reply was
    /// 234). Bytes the server sent after that reply but before the
    /// handshake would be plaintext injected into the TLS session
    /// (STARTTLS injection), so they end the download.
    async fn start_tls(self, tls: &Tls) -> Result<Self, FeedError> {
        let Self { host, stream, peer } = self;
        if !stream.buffer().is_empty() {
            return Err(io(&host, "data sent before the TLS handshake"));
        }
        let Conn::Plain(tcp) = stream.into_inner() else {
            return Err(io(&host, "the control connection is already TLS"));
        };
        let tls_stream = tls.handshake(&host, tcp).await?;
        Ok(Self {
            host,
            stream: BufReader::new(Conn::Tls(Box::new(tls_stream))),
            peer,
        })
    }

    /// Explicit FTPS: `AUTH TLS`, then the handshake. A 5xx reply is
    /// `Declined` for `Explicit` and an error for `Required`; any other
    /// refusal is an error.
    async fn auth_tls(mut self, tls: Tls, mode: FtpSecurity) -> Result<Secured, FeedError> {
        let (code, text) = self.send("AUTH TLS").await?;
        match code {
            234 => {
                let control = self.start_tls(&tls).await?;
                Ok(Secured::Tls(control, tls))
            }
            500..=599 if mode == FtpSecurity::Explicit => {
                tracing::warn!(
                    host = %self.host,
                    reply = %text,
                    "the FTP server does not offer AUTH TLS: continuing over plain FTP, \
                     the login and the file cross the network unprotected"
                );
                Ok(Secured::Declined(self))
            }
            _ => Err(io(&self.host, format!("AUTH TLS refused: {text}"))),
        }
    }

    /// One whole reply: `ddd text`, or `ddd-text` lines up to the one that
    /// starts `ddd ` (RFC 959 §4.2).
    async fn reply(&mut self) -> Result<(u16, String), FeedError> {
        let mut first: Option<(u16, String)> = None;
        for _ in 0..MAX_REPLY_LINES {
            let mut line = Vec::new();
            let n = (&mut self.stream)
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

    /// Sends `command` without reading the reply.
    async fn write_command(&mut self, command: &str) -> Result<(), FeedError> {
        let conn = self.stream.get_mut();
        conn.write_all(format!("{command}\r\n").as_bytes())
            .await
            .map_err(|err| io(&self.host, format!("sending a command: {err}")))?;
        conn.flush()
            .await
            .map_err(|err| io(&self.host, format!("sending a command: {err}")))
    }

    async fn send(&mut self, command: &str) -> Result<(u16, String), FeedError> {
        self.write_command(command).await?;
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
    security: &Security,
    path: &Path,
    max_bytes: u64,
    timeout: Duration,
) -> Result<u64, FeedError> {
    let target = FtpUrl::parse(url)?;
    let result = tokio::time::timeout(timeout, fetch(&target, login, security, path, max_bytes))
        .await
        .unwrap_or_else(|_| Err(io(&target.host, format!("over the {timeout:?} timeout"))));
    if result.is_err() {
        let _ = tokio::fs::remove_file(path).await;
    }
    result
}

/// The control connection, secured as `security` asks, and the TLS
/// client for the data connection (`None`: plain FTP). Nothing but
/// `AUTH TLS` is sent before TLS is up unless the mode is `Plain` or
/// `Explicit` with a server that answered 5xx.
async fn open_control(
    target: &FtpUrl,
    security: &Security,
) -> Result<(Control, Option<Tls>), FeedError> {
    if security.mode == FtpSecurity::Plain {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                host = %target.host,
                "downloading over plain FTP: the login and the file cross the network unprotected"
            );
        });
        return Ok((Control::connect(&target.host, target.port).await?, None));
    }
    // Built first: a bad CA file or host name fails before any connection.
    let tls = Tls::new(&target.host, security.ca_file.as_deref())?;
    let control = Control::connect(&target.host, target.port).await?;
    match control.auth_tls(tls, security.mode).await? {
        Secured::Tls(control, tls) => Ok((control, Some(tls))),
        Secured::Declined(control) => Ok((control, None)),
    }
}

async fn fetch(
    target: &FtpUrl,
    login: &Login,
    security: &Security,
    path: &Path,
    max_bytes: u64,
) -> Result<u64, FeedError> {
    let (mut control, tls) = open_control(target, security).await?;
    control.login(login).await?;
    if tls.is_some() {
        // No protection buffer (RFC 4217 §9), and a private data channel.
        control.command("PBSZ 0", &[200], "PBSZ refused").await?;
        control.command("PROT P", &[200], "PROT P refused").await?;
    }
    control
        .command("TYPE I", &[200], "binary mode refused")
        .await?;
    let (code, text) = control.send(&format!("SIZE {}", target.path)).await?;
    let mut expected: Option<u64> = None;
    if code == 213
        && let Some(size) = text.get(4..).and_then(|s| s.trim().parse::<u64>().ok())
    {
        if size > max_bytes {
            return Err(FeedError::Oversize(format!(
                "ftp://{}/{} (SIZE {size}, cap {max_bytes})",
                target.host, target.path
            )));
        }
        expected = Some(size);
    }
    let tcp = control.data_connection().await?;
    let mut data = if let Some(tls) = &tls {
        // The server starts the data connection's TLS once it has the
        // transfer command; some answer `150` only after the handshake,
        // so the reply and the handshake run together.
        control
            .write_command(&format!("RETR {}", target.path))
            .await?;
        let (_, stream) = tokio::try_join!(
            control.expect_reply(&[125, 150], "download refused"),
            tls.handshake(&target.host, tcp)
        )?;
        Conn::Tls(Box::new(stream))
    } else {
        control
            .command(
                &format!("RETR {}", target.path),
                &[125, 150],
                "download refused",
            )
            .await?;
        Conn::Plain(tcp)
    };
    let tls_data = tls.is_some();
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| FeedError::Io(format!("creating {}: {err}", path.display())))?;
    let mut written: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match data.read(&mut buf).await {
            Ok(n) => n,
            // A TLS peer closing without `close_notify` is common, and an
            // attacker can also forge it to truncate the file: the `226`
            // (on the authenticated control connection) and the byte count
            // against `SIZE` stand in for it. Without a `SIZE` there is
            // nothing to check the count against, so it fails closed.
            Err(err) if tls_data && err.kind() == ErrorKind::UnexpectedEof => match expected {
                Some(size) if written == size => 0,
                Some(size) => {
                    return Err(io(
                        &target.host,
                        format!("the data connection ended at {written} of {size} bytes"),
                    ));
                }
                None => {
                    return Err(io(
                        &target.host,
                        "the data connection ended without a TLS close_notify and the server \
                         gave no SIZE to check the file against",
                    ));
                }
            },
            Err(err) => return Err(io(&target.host, format!("reading the file: {err}"))),
        };
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
    // A TLS `close_notify` from this side; the server may already be gone.
    let _ = data.shutdown().await;
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
    use std::sync::Mutex;

    use rustls::HandshakeKind;
    use rustls::pki_types::PrivateKeyDer;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    use super::*;

    /// The test CA that signed the fake server's certificate
    /// (`ftp-test-fixtures/README`).
    const CA_FILE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/adapter/ftp-test-fixtures/ca.pem"
    );
    const SERVER_CERT: &[u8] = include_bytes!("ftp-test-fixtures/server.pem");
    const SERVER_KEY: &[u8] = include_bytes!("ftp-test-fixtures/server.key");

    trait Rw: AsyncRead + AsyncWrite + Unpin + Send {}
    impl<T: AsyncRead + AsyncWrite + Unpin + Send> Rw for T {}
    type Io = BufReader<Box<dyn Rw>>;

    #[derive(Clone, Copy)]
    enum Versions {
        Tls13,
        Tls12,
    }

    /// How the fake server behaves.
    #[derive(Clone)]
    #[expect(clippy::struct_excessive_bools, reason = "a fake server's switches")]
    struct Fake {
        file: Vec<u8>,
        epsv: bool,
        size: bool,
        multi_line_greeting: bool,
        /// Offers `AUTH TLS` with these protocol versions (`None`: a
        /// plain-only server, which answers `AUTH` with a 502).
        tls: Option<Versions>,
        /// Answers `AUTH TLS` with this instead of 234.
        auth_reply: Option<&'static str>,
        /// Refuses a data connection whose TLS session does not resume the
        /// control connection's.
        require_reuse: bool,
        /// Ends a TLS data connection without `close_notify`.
        unclean_close: bool,
    }

    impl Default for Fake {
        fn default() -> Self {
            Self {
                file: b"PK zip bytes".to_vec(),
                epsv: true,
                size: true,
                multi_line_greeting: false,
                tls: None,
                auth_reply: None,
                require_reuse: false,
                unclean_close: false,
            }
        }
    }

    /// What the fake server saw.
    #[derive(Default)]
    struct Seen {
        /// The control connection's bytes while it was still plaintext.
        clear: Vec<u8>,
        /// Every command's verb, in order.
        verbs: Vec<String>,
        /// Each TLS handshake the server completed (control, then data):
        /// whether it resumed an earlier session.
        resumed: Vec<bool>,
    }

    fn server_config(versions: Versions) -> Arc<rustls::ServerConfig> {
        let certs = CertificateDer::pem_slice_iter(SERVER_CERT)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_slice(SERVER_KEY).unwrap();
        let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ));
        let builder = match versions {
            Versions::Tls13 => builder.with_protocol_versions(&[&rustls::version::TLS13]),
            Versions::Tls12 => builder.with_protocol_versions(&[&rustls::version::TLS12]),
        }
        .unwrap();
        Arc::new(
            builder
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .unwrap(),
        )
    }

    async fn say(io: &mut Io, text: &str) {
        let conn = io.get_mut();
        conn.write_all(format!("{text}\r\n").as_bytes())
            .await
            .unwrap();
        conn.flush().await.unwrap();
    }

    async fn passive_listener(seen_port: &mut Option<TcpListener>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        *seen_port = Some(listener);
        port
    }

    /// One FTP session on `tcp` for `user`/`secret`, serving `EM.zip`.
    #[expect(clippy::too_many_lines, reason = "one fake server's command loop")]
    async fn session(tcp: TcpStream, fake: Arc<Fake>, seen: Arc<Mutex<Seen>>) {
        let acceptor = fake.tls.map(|v| TlsAcceptor::from(server_config(v)));
        let mut io: Io = BufReader::new(Box::new(tcp));
        let mut tls_control = false;
        let mut protected = false;
        let mut data: Option<TcpListener> = None;
        if fake.multi_line_greeting {
            io.get_mut()
                .write_all(b"220-Welcome\r\n Terms apply\r\n220 Ready\r\n")
                .await
                .unwrap();
        } else {
            say(&mut io, "220 Ready").await;
        }
        loop {
            let mut line = String::new();
            if io.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            let line = line.trim_end().to_owned();
            let (verb, arg) = line.split_once(' ').unwrap_or((line.as_str(), ""));
            {
                let mut seen = seen.lock().unwrap();
                if !tls_control {
                    seen.clear.extend_from_slice(line.as_bytes());
                    seen.clear.extend_from_slice(b"\r\n");
                }
                seen.verbs.push(verb.to_owned());
            }
            if verb == "AUTH"
                && arg == "TLS"
                && !tls_control
                && fake.auth_reply.is_none()
                && let Some(acceptor) = &acceptor
            {
                say(&mut io, "234 Proceed with negotiation").await;
                let Ok(stream) = acceptor.accept(io.into_inner()).await else {
                    return;
                };
                seen.lock().unwrap().resumed.push(matches!(
                    stream.get_ref().1.handshake_kind(),
                    Some(HandshakeKind::Resumed)
                ));
                io = BufReader::new(Box::new(stream));
                tls_control = true;
                continue;
            }
            let reply = match verb {
                "AUTH" if acceptor.is_some() && fake.auth_reply.is_some() => {
                    fake.auth_reply.unwrap().to_owned()
                }
                "PBSZ" | "PROT" if !tls_control => "503 AUTH TLS first".to_owned(),
                "PBSZ" => "200 PBSZ=0".to_owned(),
                "PROT" if arg == "P" => {
                    protected = true;
                    "200 Protection level set to Private".to_owned()
                }
                "PROT" => "534 Only P".to_owned(),
                "USER" if arg == "user" => "331 Password please".to_owned(),
                "USER" => "530 No".to_owned(),
                "PASS" if arg == "secret" => "230 In".to_owned(),
                "PASS" => "530 Login incorrect".to_owned(),
                "TYPE" => "200 Binary".to_owned(),
                "SIZE" if fake.size && arg == "EM.zip" => format!("213 {}", fake.file.len()),
                "SIZE" => "550 No size".to_owned(),
                "EPSV" if fake.epsv => {
                    let port = passive_listener(&mut data).await;
                    format!("229 Entering Extended Passive Mode (|||{port}|)")
                }
                "EPSV" => "500 Unknown command".to_owned(),
                "PASV" => {
                    let port = passive_listener(&mut data).await;
                    // A bogus address: the client must use the peer's.
                    format!(
                        "227 Entering Passive Mode (10,9,8,7,{},{})",
                        port / 256,
                        port % 256
                    )
                }
                "RETR" if arg == "EM.zip" => {
                    say(&mut io, "150 Opening").await;
                    let (tcp, _) = data.take().unwrap().accept().await.unwrap();
                    let mut conn: Box<dyn Rw> = if protected {
                        let Ok(stream) = acceptor.as_ref().unwrap().accept(tcp).await else {
                            return;
                        };
                        let resumed = matches!(
                            stream.get_ref().1.handshake_kind(),
                            Some(HandshakeKind::Resumed)
                        );
                        seen.lock().unwrap().resumed.push(resumed);
                        if fake.require_reuse && !resumed {
                            drop(stream);
                            say(&mut io, "522 TLS session reuse required").await;
                            continue;
                        }
                        Box::new(stream)
                    } else {
                        Box::new(tcp)
                    };
                    conn.write_all(&fake.file).await.unwrap();
                    if protected && fake.unclean_close {
                        conn.flush().await.unwrap();
                    } else {
                        let _ = conn.shutdown().await;
                    }
                    drop(conn);
                    "226 Transfer complete".to_owned()
                }
                "RETR" => "550 No such file".to_owned(),
                "QUIT" => {
                    say(&mut io, "221 Bye").await;
                    return;
                }
                _ => "502 Not implemented".to_owned(),
            };
            say(&mut io, &reply).await;
        }
    }

    /// A one-session FTP(S) server on localhost. Its port, and what it
    /// saw.
    async fn serve(fake: Fake) -> (u16, Arc<Mutex<Seen>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Seen::default()));
        let task_seen = Arc::clone(&seen);
        let fake = Arc::new(fake);
        tokio::spawn(async move {
            if let Ok((tcp, _)) = listener.accept().await {
                session(tcp, fake, task_seen).await;
            }
        });
        (port, seen)
    }

    fn login(password: &str) -> Login {
        Login {
            user: "user".to_owned(),
            password: password.to_owned(),
        }
    }

    fn security(mode: FtpSecurity, ca: bool) -> Security {
        Security {
            mode,
            ca_file: ca.then(|| PathBuf::from(CA_FILE)),
        }
    }

    struct Run {
        result: Result<u64, FeedError>,
        bytes: Vec<u8>,
        /// What the server saw on the control connection in the clear.
        clear: String,
        seen: Arc<Mutex<Seen>>,
    }

    /// Downloads `EM.zip` from a fake server as `host` (the server is on
    /// 127.0.0.1, and its certificate is for `localhost`).
    async fn run(fake: Fake, host: &str, security: &Security, password: &str, cap: u64) -> Run {
        let (port, seen) = serve(fake).await;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feed.zip");
        let result = download_to_file(
            &format!("ftp://{host}:{port}/EM.zip"),
            &login(password),
            security,
            &path,
            cap,
            Duration::from_secs(10),
        )
        .await;
        let bytes = std::fs::read(&path).unwrap_or_default();
        let clear = String::from_utf8_lossy(&seen.lock().unwrap().clear).into_owned();
        Run {
            result,
            bytes,
            clear,
            seen,
        }
    }

    async fn download(fake: Fake, password: &str, cap: u64) -> (Result<u64, FeedError>, Vec<u8>) {
        let run = run(fake, "127.0.0.1", &Security::default(), password, cap).await;
        (run.result, run.bytes)
    }

    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<(tracing::Level, String)>>>);

    struct Message(String);

    impl tracing::field::Visit for Message {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    impl tracing::Subscriber for Logs {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut message = Message(String::new());
            event.record(&mut message);
            self.0
                .lock()
                .unwrap()
                .push((*event.metadata().level(), message.0));
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
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
        assert_eq!(bytes, [] as [u8; 0]);
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

    #[tokio::test]
    async fn plain_mode_sends_the_login_in_clear() {
        let run = run(
            Fake::default(),
            "127.0.0.1",
            &Security::default(),
            "secret",
            1024,
        )
        .await;
        run.result.unwrap();
        let clear = run.clear;
        assert!(clear.contains("USER user\r\n"), "{clear}");
        assert!(clear.contains("PASS secret\r\n"), "{clear}");
        assert!(!run.seen.lock().unwrap().verbs.contains(&"AUTH".to_owned()));
    }

    #[tokio::test]
    async fn explicit_ftps_protects_both_connections_and_resumes_the_session() {
        for versions in [Versions::Tls13, Versions::Tls12] {
            let fake = Fake {
                tls: Some(versions),
                ..Fake::default()
            };
            let run = run(
                fake,
                "localhost",
                &security(FtpSecurity::Explicit, true),
                "secret",
                1024,
            )
            .await;
            assert_eq!(run.result.unwrap(), 12);
            assert_eq!(run.bytes, b"PK zip bytes");
            // Only AUTH TLS crossed the network before TLS: no USER, no
            // PASS, no password.
            assert_eq!(run.clear, "AUTH TLS\r\n");
            let seen = run.seen.lock().unwrap();
            assert_eq!(
                seen.verbs[..6],
                ["AUTH", "USER", "PASS", "PBSZ", "PROT", "TYPE"]
            );
            // The control connection, then the data connection, which
            // resumed the control connection's session.
            assert_eq!(seen.resumed, [false, true]);
        }
    }

    #[tokio::test]
    async fn required_ftps_works_against_a_server_that_demands_session_reuse() {
        for versions in [Versions::Tls13, Versions::Tls12] {
            let fake = Fake {
                tls: Some(versions),
                require_reuse: true,
                ..Fake::default()
            };
            let run = run(
                fake,
                "localhost",
                &security(FtpSecurity::Required, true),
                "secret",
                1024,
            )
            .await;
            assert_eq!(run.result.unwrap(), 12);
            assert_eq!(run.bytes, b"PK zip bytes");
            assert_eq!(run.clear, "AUTH TLS\r\n");
            assert_eq!(run.seen.lock().unwrap().resumed, [false, true]);
        }
    }

    #[tokio::test]
    async fn a_tls_data_connection_ended_without_close_notify_still_completes() {
        let fake = Fake {
            tls: Some(Versions::Tls13),
            unclean_close: true,
            ..Fake::default()
        };
        let run = run(
            fake,
            "localhost",
            &security(FtpSecurity::Required, true),
            "secret",
            1024,
        )
        .await;
        assert_eq!(run.result.unwrap(), 12);
        assert_eq!(run.bytes, b"PK zip bytes");
    }

    #[tokio::test]
    async fn an_unclean_tls_close_without_a_size_fails_closed() {
        let fake = Fake {
            tls: Some(Versions::Tls13),
            unclean_close: true,
            size: false,
            ..Fake::default()
        };
        let run = run(
            fake,
            "localhost",
            &security(FtpSecurity::Required, true),
            "secret",
            1024,
        )
        .await;
        let err = run.result.unwrap_err().to_string();
        assert!(err.contains("close_notify"), "{err}");
        assert_eq!(run.bytes, [] as [u8; 0]);
    }

    #[tokio::test]
    async fn required_refuses_a_plain_only_server_before_sending_credentials() {
        let run = run(
            Fake::default(),
            "localhost",
            &security(FtpSecurity::Required, true),
            "secret",
            1024,
        )
        .await;
        let err = run.result.unwrap_err().to_string();
        assert!(err.contains("AUTH TLS refused"), "{err}");
        assert!(!err.contains("secret"), "{err}");
        assert_eq!(run.clear, "AUTH TLS\r\n");
        assert_eq!(run.bytes, [] as [u8; 0]);
    }

    #[tokio::test]
    async fn explicit_falls_back_to_plain_when_auth_gets_a_5xx_and_warns() {
        let logs = Logs::default();
        let _guard = tracing::subscriber::set_default(logs.clone());
        let run = run(
            Fake::default(),
            "localhost",
            &security(FtpSecurity::Explicit, true),
            "secret",
            1024,
        )
        .await;
        assert_eq!(run.result.unwrap(), 12);
        assert_eq!(run.bytes, b"PK zip bytes");
        let clear = run.clear;
        assert!(clear.starts_with("AUTH TLS\r\nUSER user\r\n"), "{clear}");
        let logs = logs.0.lock().unwrap();
        assert!(
            logs.iter().any(|(level, message)| {
                *level == tracing::Level::WARN && message.contains("does not offer AUTH TLS")
            }),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn explicit_does_not_fall_back_on_a_4xx_auth_reply() {
        let fake = Fake {
            tls: Some(Versions::Tls13),
            auth_reply: Some("431 TLS is unavailable just now"),
            ..Fake::default()
        };
        let run = run(
            fake,
            "localhost",
            &security(FtpSecurity::Explicit, true),
            "secret",
            1024,
        )
        .await;
        let err = run.result.unwrap_err().to_string();
        assert!(err.contains("431"), "{err}");
        assert_eq!(run.clear, "AUTH TLS\r\n");
    }

    #[tokio::test]
    async fn a_certificate_for_another_host_name_is_refused() {
        let fake = Fake {
            tls: Some(Versions::Tls13),
            ..Fake::default()
        };
        // The certificate is for `localhost`, not 127.0.0.1.
        let run = run(
            fake,
            "127.0.0.1",
            &security(FtpSecurity::Required, true),
            "secret",
            1024,
        )
        .await;
        let err = run.result.unwrap_err().to_string();
        assert!(err.contains("TLS handshake failed"), "{err}");
        assert_eq!(run.clear, "AUTH TLS\r\n");
        assert_eq!(run.bytes, [] as [u8; 0]);
    }

    #[tokio::test]
    async fn a_certificate_from_an_unknown_ca_is_refused() {
        let fake = Fake {
            tls: Some(Versions::Tls13),
            ..Fake::default()
        };
        // No CA file: only the system's roots, which do not hold the test
        // CA.
        let run = run(
            fake,
            "localhost",
            &security(FtpSecurity::Required, false),
            "secret",
            1024,
        )
        .await;
        let err = run.result.unwrap_err().to_string();
        assert!(err.contains("TLS handshake failed"), "{err}");
        assert_eq!(run.clear, "AUTH TLS\r\n");
    }

    #[tokio::test]
    async fn a_bad_ca_file_fails_before_any_connection() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        for ca_file in [empty, dir.path().join("missing.pem")] {
            let security = Security {
                mode: FtpSecurity::Required,
                ca_file: Some(ca_file),
            };
            let (port, seen) = serve(Fake::default()).await;
            let result = download_to_file(
                &format!("ftp://localhost:{port}/EM.zip"),
                &login("secret"),
                &security,
                &dir.path().join("feed.zip"),
                1024,
                Duration::from_secs(10),
            )
            .await;
            let err = result.unwrap_err().to_string();
            assert!(err.contains("CA file"), "{err}");
            assert_eq!(seen.lock().unwrap().verbs, Vec::<String>::new());
        }
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
