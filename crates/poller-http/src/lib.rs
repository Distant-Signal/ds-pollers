//! What every poller's upstream HTTP client shares: a redirect policy and
//! a hard cap on how much of a response body is read.
//!
//! **Redirects.** reqwest strips only `Authorization` and `Cookie` when a
//! redirect crosses hosts, so a custom key header (RDM's `x-apikey`, the
//! `TfL` key) would follow a redirect to another host, and a redirect could
//! aim a blind GET at an address inside the cluster. A client that sends a
//! credential uses [`no_redirects`]; a client that sends none but whose
//! feed legitimately redirects to a CDN (the NIR open-data CSVs and the
//! SIRI-SX archive answer 302 to a signed object-store URL) uses
//! [`https_redirects`]. Either way a redirect that is not followed comes
//! back as a 3xx response, which [`check_status`] turns into an error. The
//! `Location` is never logged: a signed URL carries a signature.
//!
//! **Body caps.** A response is read with [`read_capped`] (or
//! [`read_capped_text`]), which streams the decoded body and fails past a
//! hard byte limit, so a misbehaving or hostile upstream cannot fill the
//! pod's memory. Each feed picks its own cap, well above the real size.

use reqwest::redirect::{Attempt, Policy};
use reqwest::{Response, Url};

/// Most redirects [`https_redirects`] follows.
const MAX_REDIRECTS: usize = 3;

/// The most of an error response's body kept for a message.
pub const ERROR_BODY_LIMIT: usize = 4 * 1024;

/// A response body went past its cap.
#[derive(Debug)]
pub struct BodyTooLarge {
    /// The feed's name, as in the metric's label.
    pub feed: &'static str,
    /// The cap that was exceeded, in bytes.
    pub cap: usize,
}

impl std::fmt::Display for BodyTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} response body exceeded the {}-byte cap; refusing to read it",
            self.feed, self.cap
        )
    }
}

impl std::error::Error for BodyTooLarge {}

/// Why a capped read failed.
#[derive(Debug)]
pub enum ReadError {
    /// The body went past the cap.
    TooLarge(BodyTooLarge),
    /// The connection failed or timed out part-way.
    Transport(reqwest::Error),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge(err) => std::fmt::Display::fmt(err, f),
            Self::Transport(err) => std::fmt::Display::fmt(err, f),
        }
    }
}

impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TooLarge(err) => Some(err),
            Self::Transport(err) => Some(err),
        }
    }
}

/// Never follow a redirect. For every client that sends a credential.
#[must_use]
pub fn no_redirects() -> Policy {
    Policy::none()
}

/// Follow up to [`MAX_REDIRECTS`] redirects, each only to an `https://`
/// URL on a public-looking domain name (see [`redirect_target_allowed`]).
/// For a client that sends no credential (not even a custom header) to a
/// feed that redirects to a CDN. Anything else is not followed, and comes
/// back as the 3xx response.
#[must_use]
pub fn https_redirects() -> Policy {
    Policy::custom(|attempt: Attempt<'_>| {
        if attempt.previous().len() >= MAX_REDIRECTS || !redirect_target_allowed(attempt.url()) {
            attempt.stop()
        } else {
            attempt.follow()
        }
    })
}

/// Whether [`https_redirects`] follows a redirect to `url`: `https`, and a
/// dotted domain name that is not `localhost` or an in-cluster or private
/// name (no IP literals, no `.local`, `.internal`, `.svc`, `.localhost`).
/// Defence in depth: it does not stop a public name that resolves to a
/// private address, which the cluster's network policy covers.
#[must_use]
pub fn redirect_target_allowed(url: &Url) -> bool {
    if url.scheme() != "https" {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.starts_with('[') || host.parse::<std::net::IpAddr>().is_ok() || !host.contains('.') {
        return false;
    }
    ![".local", ".internal", ".svc", ".localhost", ".lan"]
        .iter()
        .any(|suffix| host.ends_with(suffix))
}

/// Registers `upstream_redirect_refused_total` for each of `status_feeds`
/// (the feeds that go through [`check_status`]) and
/// `upstream_body_cap_exceeded_total` for each of `capped_feeds` (the feeds
/// that go through [`read_capped`]) at 0, so `rate()` works before the
/// first refusal. Call it once at startup.
pub fn register_metrics(status_feeds: &[&'static str], capped_feeds: &[&'static str]) {
    for feed in status_feeds {
        metrics::counter!(
            ds_runtime::metrics::metric_name("upstream_redirect_refused_total"),
            "feed" => *feed
        )
        .increment(0);
    }
    for feed in capped_feeds {
        metrics::counter!(
            ds_runtime::metrics::metric_name("upstream_body_cap_exceeded_total"),
            "feed" => *feed
        )
        .increment(0);
    }
}

/// Fails on a non-success status, and says so plainly for a redirect that
/// was not followed (never naming its `Location`). `feed` labels the log
/// line and the `upstream_redirect_refused_total` metric.
///
/// # Errors
/// A 3xx, 4xx or 5xx status.
pub fn check_status(response: &Response, feed: &'static str) -> anyhow::Result<()> {
    let status = response.status();
    if status.is_redirection() {
        metrics::counter!(
            ds_runtime::metrics::metric_name("upstream_redirect_refused_total"),
            "feed" => feed
        )
        .increment(1);
        tracing::warn!(feed, %status, "upstream answered with a redirect that was not followed");
        anyhow::bail!("{feed}: upstream answered {status}, a redirect that was not followed");
    }
    response.error_for_status_ref()?;
    Ok(())
}

/// [`check_status`], then the response back.
///
/// # Errors
/// As [`check_status`].
pub fn ensure_success(response: Response, feed: &'static str) -> anyhow::Result<Response> {
    check_status(&response, feed)?;
    Ok(response)
}

/// Reads the decoded body, at most `cap` bytes. A declared length over the
/// cap fails before reading; otherwise the stream is counted as it grows
/// (the length may be absent, wrong or, for a compressed body, the encoded
/// size). Streams through `Response::chunk`, so no extra reqwest feature.
///
/// # Errors
/// [`ReadError::TooLarge`] (also counted in
/// `upstream_body_cap_exceeded_total{feed}`) or a transport error.
pub async fn read_capped(
    mut response: Response,
    cap: usize,
    feed: &'static str,
) -> Result<Vec<u8>, ReadError> {
    let too_large = || {
        metrics::counter!(
            ds_runtime::metrics::metric_name("upstream_body_cap_exceeded_total"),
            "feed" => feed
        )
        .increment(1);
        tracing::error!(feed, cap, "upstream response body exceeded its cap");
        ReadError::TooLarge(BodyTooLarge { feed, cap })
    };
    let declared = response.content_length();
    if declared.is_some_and(|declared| declared > cap as u64) {
        return Err(too_large());
    }
    // A declared length (within the cap) sizes the buffer once, so a large
    // feed does not pay for doubling reallocations.
    let mut body = Vec::with_capacity(declared.map_or(0, |d| usize::try_from(d).unwrap_or(0)));
    while let Some(chunk) = response.chunk().await.map_err(ReadError::Transport)? {
        if body.len().saturating_add(chunk.len()) > cap {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// [`read_capped`] as UTF-8 text (invalid bytes replaced, as reqwest's
/// `text()` does; the feeds are UTF-8).
///
/// # Errors
/// As [`read_capped`].
pub async fn read_capped_text(
    response: Response,
    cap: usize,
    feed: &'static str,
) -> Result<String, ReadError> {
    let bytes = read_capped(response, cap, feed).await?;
    Ok(String::from_utf8(bytes)
        .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned()))
}

/// The start of an error response's body for a message: at most
/// [`ERROR_BODY_LIMIT`] bytes, lossy, never failing (a body that cannot be
/// read is an empty one).
pub async fn error_body(mut response: Response) -> String {
    let mut body = Vec::new();
    while body.len() < ERROR_BODY_LIMIT {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    body.truncate(ERROR_BODY_LIMIT);
    String::from_utf8_lossy(&body).into_owned()
}

/// `text` cut to at most `limit` bytes on a character boundary, for a
/// debug log line.
#[must_use]
pub fn truncate_for_log(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn client(policy: Policy) -> reqwest::Client {
        reqwest::Client::builder().redirect(policy).build().unwrap()
    }

    #[tokio::test]
    async fn a_body_within_the_cap_is_read() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello"))
            .mount(&server)
            .await;
        let response = reqwest::get(server.uri()).await.unwrap();
        assert_eq!(
            read_capped_text(response, 5, "test").await.unwrap(),
            "hello"
        );
    }

    #[tokio::test]
    async fn a_body_over_the_cap_fails_the_read() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 10_000]))
            .mount(&server)
            .await;
        let response = reqwest::get(server.uri()).await.unwrap();
        let err = read_capped(response, 1_000, "test").await.unwrap_err();
        assert!(matches!(
            err,
            ReadError::TooLarge(BodyTooLarge { cap: 1_000, .. })
        ));
        assert!(err.to_string().contains("1000-byte cap"), "{err}");
    }

    #[tokio::test]
    async fn an_error_body_is_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500).set_body_bytes(vec![b'y'; 100_000]))
            .mount(&server)
            .await;
        let response = reqwest::get(server.uri()).await.unwrap();
        assert!(error_body(response).await.len() <= ERROR_BODY_LIMIT);
    }

    #[test]
    fn log_truncation_keeps_character_boundaries() {
        assert_eq!(truncate_for_log("abcdef", 3), "abc");
        assert_eq!(truncate_for_log("ab", 3), "ab");
        // "é" is two bytes: cutting at 1 must not split it.
        assert_eq!(truncate_for_log("éa", 1), "");
    }

    /// The point of [`no_redirects`]: a 307 to another server is not
    /// followed, so that server sees nothing (not even the key header), and
    /// the 3xx is an error.
    #[tokio::test]
    async fn a_redirect_is_not_followed_and_the_target_gets_nothing() {
        let target = MockServer::start().await;
        let origin = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/feed"))
            .and(header("x-apikey", "secret"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/stolen", target.uri())),
            )
            .expect(1)
            .mount(&origin)
            .await;

        let response = client(no_redirects())
            .get(format!("{}/feed", origin.uri()))
            .header("x-apikey", "secret")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 307);
        let err = check_status(&response, "test").unwrap_err().to_string();
        assert!(err.contains("redirect"), "{err}");
        assert!(
            !err.contains(&target.uri()),
            "the Location must not leak: {err}"
        );
        assert!(target.received_requests().await.unwrap().is_empty());
    }

    /// A plain-HTTP target is refused by [`https_redirects`] too.
    #[tokio::test]
    async fn the_https_policy_refuses_an_http_target() {
        let target = MockServer::start().await;
        let origin = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", format!("{}/x", target.uri())),
            )
            .mount(&origin)
            .await;
        let response = client(https_redirects())
            .get(origin.uri())
            .send()
            .await
            .unwrap();
        assert!(check_status(&response, "test").is_err());
        assert!(target.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn redirect_targets_are_vetted() {
        let allowed = |url: &str| redirect_target_allowed(&Url::parse(url).unwrap());
        assert!(allowed("https://example.r2.cloudflarestorage.com/a?sig=1"));
        assert!(!allowed("http://example.org/a"));
        assert!(!allowed("https://127.0.0.1/a"));
        assert!(!allowed("https://10.1.2.3/a"));
        assert!(!allowed("https://[::1]/a"));
        assert!(!allowed("https://localhost/a"));
        assert!(!allowed("https://api/a"));
        assert!(!allowed("https://api.default.svc/a"));
        assert!(!allowed("https://redis.ds.svc.cluster.local/a"));
        assert!(!allowed("https://metadata.internal/a"));
        assert!(!allowed("https://thing.local/a"));
    }
}
