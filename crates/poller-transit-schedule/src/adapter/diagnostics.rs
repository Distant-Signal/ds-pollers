//! What a failed zip download looked like, and whether to try again.
//!
//! A feed that answers 200 with something other than the archive (an error
//! page, an empty body, a changed format) reads in the parsers only as
//! "Could not find EOCD". Every failed attempt of a zip download
//! ([`super::download_zip_to_file`], the SIRI-SX fetch) logs one structured
//! line from [`report`]: the status, the final URL's host and path (never
//! its query, which can carry a key), a few headers, the bytes received,
//! whether the body starts with the zip magic and, when it does not, a
//! sanitised preview of its start. [`Class`] is the label of
//! `transit_feed_download_attempts_total` and decides the retries.

use std::time::Duration;

use ds_runtime::metrics::metric_name;

use super::FeedError;

/// The longest preview of a body that is not a zip.
pub(crate) const PREVIEW_BYTES: usize = 120;
/// The longest `Retry-After` honoured (a longer one is cut to this).
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
const ZIP_MAGIC: &[u8] = b"PK\x03\x04";

/// `transit_feed_download_attempts_total{network, source, outcome}`: `ok`,
/// or the [`Class`] of the failure.
pub(crate) const DOWNLOAD_ATTEMPTS_METRIC: &str = "transit_feed_download_attempts_total";

/// Why an attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    /// A complete body that does not start with the zip magic: an error
    /// page or a changed format. Retried once.
    NotAZip,
    /// The zip magic, but the archive does not read, or the body is
    /// shorter than its `Content-Length` or the connection was cut.
    TruncatedZip,
    /// A non-2xx status (an unfollowed redirect included).
    HttpStatus,
    /// Anything else (the connection, an over-cap body, the work dir).
    Error,
}

impl Class {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NotAZip => "not_a_zip",
            Self::TruncatedZip => "truncated_zip",
            Self::HttpStatus => "http_status",
            Self::Error => "error",
        }
    }
}

/// The response's status, where it came from and a few of its headers.
#[derive(Debug, Clone)]
pub(crate) struct ResponseInfo {
    pub status: u16,
    /// The final URL's scheme, host and path.
    pub target: String,
    /// The headers worth logging, as `name=value` pairs.
    pub headers: String,
    pub content_type: Option<String>,
    pub retry_after: Option<Duration>,
}

/// What a failed attempt saw.
#[derive(Debug, Clone, Default)]
pub(crate) struct Seen {
    pub info: Option<ResponseInfo>,
    pub received: u64,
    pub declared: Option<u64>,
    /// The first [`PREVIEW_BYTES`] of the body.
    pub head: Vec<u8>,
}

/// A failed attempt.
#[derive(Debug)]
pub(crate) struct Failure {
    pub err: FeedError,
    pub class: Class,
    pub retryable: bool,
    pub seen: Seen,
}

/// The URL's scheme, host and path: no userinfo, query or fragment.
pub(crate) fn safe_url(url: &reqwest::Url) -> String {
    format!(
        "{}://{}{}",
        url.scheme(),
        url.host_str().unwrap_or("?"),
        url.path()
    )
}

/// [`safe_url`] of a configured URL (which may carry a key in its query).
pub(crate) fn safe_url_str(url: &str) -> String {
    reqwest::Url::parse(url).map_or_else(|_| "an unparsable URL".to_owned(), |u| safe_url(&u))
}

/// Printable ASCII kept, every other byte a `.`, at most `limit` bytes.
pub(crate) fn sanitise(bytes: &[u8], limit: usize) -> String {
    bytes
        .iter()
        .take(limit)
        .map(|&b| {
            if (0x20..0x7f).contains(&b) {
                char::from(b)
            } else {
                '.'
            }
        })
        .collect()
}

pub(crate) fn has_zip_magic(head: &[u8]) -> bool {
    head.starts_with(ZIP_MAGIC)
}

impl ResponseInfo {
    pub(crate) fn from_response(response: &reqwest::Response) -> Self {
        let mut headers = Vec::new();
        let mut content_type = None;
        let mut retry_after = None;
        for (name, value) in response.headers() {
            let name = name.as_str();
            let wanted = matches!(
                name,
                "content-type" | "content-length" | "content-encoding" | "server" | "retry-after"
            ) || name.starts_with("x-ratelimit")
                || name.starts_with("ratelimit");
            if !wanted {
                continue;
            }
            let value = sanitise(value.as_bytes(), 100);
            if name == "content-type" {
                content_type = Some(value.clone());
            }
            if name == "retry-after" {
                // Delta-seconds only; an HTTP date is not honoured.
                retry_after = value.trim().parse::<u64>().ok().map(Duration::from_secs);
            }
            headers.push(format!("{name}={value}"));
        }
        Self {
            status: response.status().as_u16(),
            target: safe_url(response.url()),
            headers: headers.join(", "),
            content_type,
            retry_after,
        }
    }

    /// `HTTP 200, content-type text/html` for an error message.
    pub(crate) fn summary(&self) -> String {
        format!(
            "HTTP {}, content-type {}",
            self.status,
            self.content_type.as_deref().unwrap_or("not sent")
        )
    }
}

/// The failure of a body that arrived whole but is not a readable zip:
/// `NotAZip` without the magic, else `TruncatedZip` (`zip_error`: what the
/// reader said). `what` names the feed in the message.
pub(crate) fn unreadable_body(seen: Seen, zip_error: &str, what: &str) -> Box<Failure> {
    let summary = seen
        .info
        .as_ref()
        .map_or_else(String::new, |info| format!(" ({})", info.summary()));
    let (class, err) = if has_zip_magic(&seen.head) {
        let expected = seen
            .declared
            .map_or_else(String::new, |d| format!(" of {d}"));
        (
            Class::TruncatedZip,
            FeedError::Malformed(format!(
                "{what}zip: {zip_error}; received {}{expected} bytes{summary}",
                seen.received
            )),
        )
    } else {
        (
            Class::NotAZip,
            FeedError::Malformed(format!(
                "{what}the response is not a zip archive: {} bytes received{summary}",
                seen.received
            )),
        )
    };
    Box::new(Failure {
        err,
        class,
        retryable: true,
        seen,
    })
}

/// Counts an attempt in `transit_feed_download_attempts_total`.
pub(crate) fn attempted(network: &str, source: &str, outcome: &'static str) {
    metrics::counter!(
        metric_name(DOWNLOAD_ATTEMPTS_METRIC),
        "network" => network.to_owned(),
        "source" => source.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

/// Logs the one structured line for a failed attempt and counts it.
pub(crate) fn report(network: &str, source: &str, attempt: u32, failure: &Failure) {
    attempted(network, source, failure.class.as_str());
    let seen = &failure.seen;
    let zip_magic = has_zip_magic(&seen.head);
    // A body that is not a zip is previewed; a zip never is.
    let preview =
        (!zip_magic && !seen.head.is_empty()).then(|| sanitise(&seen.head, PREVIEW_BYTES));
    tracing::warn!(
        network,
        source,
        attempt,
        class = failure.class.as_str(),
        retryable = failure.retryable,
        status = seen.info.as_ref().map(|i| i.status),
        target = seen.info.as_ref().map(|i| i.target.as_str()),
        headers = seen.info.as_ref().map(|i| i.headers.as_str()),
        received_bytes = seen.received,
        expected_bytes = ?seen.declared,
        zip_magic,
        preview = preview.as_deref(),
        error = %failure.err,
        "the timetable download failed"
    );
}

/// A pseudo-random duration below `max`: it only spreads retries.
pub(crate) fn jitter(max: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    let max_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX).max(1);
    Duration::from_nanos(nanos % max_nanos)
}

/// The pause before attempt `attempt + 1`, or `None` to stop: the failure
/// is not worth retrying, `attempt` was the last, or the pause would end
/// past `window` (measured from `elapsed`). A body that is not a zip gets
/// one retry only; a `Retry-After` (up to [`MAX_RETRY_AFTER`]) lengthens
/// the pause.
pub(crate) fn retry_pause(
    failure: &Failure,
    attempt: u32,
    attempts: u32,
    backoff: Duration,
    elapsed: Duration,
    window: Duration,
) -> Option<Duration> {
    let limit = if failure.class == Class::NotAZip {
        attempts.min(2)
    } else {
        attempts
    };
    if !failure.retryable || attempt >= limit {
        return None;
    }
    let mut pause = backoff * attempt + jitter(backoff * attempt / 2);
    if let Some(after) = failure.seen.info.as_ref().and_then(|i| i.retry_after) {
        pause = pause.max(after.min(MAX_RETRY_AFTER));
    }
    (elapsed + pause <= window).then_some(pause)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_preview_is_printable_ascii_and_short() {
        assert_eq!(sanitise(b"<html>\r\n\x00\xff ok", 120), "<html>.... ok");
        assert_eq!(sanitise(&[b'a'; 500], PREVIEW_BYTES).len(), PREVIEW_BYTES);
    }

    #[test]
    fn a_logged_url_has_no_query_or_userinfo() {
        let url =
            reqwest::Url::parse("https://user:pw@data.example/a/b.zip?api_key=K#frag").unwrap();
        assert_eq!(safe_url(&url), "https://data.example/a/b.zip");
        assert_eq!(
            safe_url_str("https://data.example/x?token=T"),
            "https://data.example/x"
        );
        assert!(!safe_url_str("not a url ?key=K").contains('K'));
    }

    #[test]
    fn the_class_follows_the_zip_magic() {
        let seen = |head: &[u8]| Seen {
            head: head.to_vec(),
            received: head.len() as u64,
            ..Seen::default()
        };
        let html = unreadable_body(seen(b"<html>"), "no EOCD", "");
        assert_eq!(html.class, Class::NotAZip);
        let empty = unreadable_body(seen(b""), "no EOCD", "");
        assert_eq!(empty.class, Class::NotAZip);
        let cut = unreadable_body(seen(b"PK\x03\x04abc"), "no EOCD", "");
        assert_eq!(cut.class, Class::TruncatedZip);
        assert!(cut.err.to_string().contains("no EOCD"));
    }

    #[test]
    fn a_page_gets_one_retry_a_cut_file_the_attempts() {
        let failure = |class| Failure {
            err: FeedError::Io("x".to_owned()),
            class,
            retryable: true,
            seen: Seen::default(),
        };
        let pause = |class, attempt| {
            retry_pause(
                &failure(class),
                attempt,
                3,
                Duration::from_millis(1),
                Duration::ZERO,
                Duration::from_secs(60),
            )
        };
        assert!(pause(Class::NotAZip, 1).is_some());
        assert!(pause(Class::NotAZip, 2).is_none());
        assert!(pause(Class::TruncatedZip, 2).is_some());
        assert!(pause(Class::TruncatedZip, 3).is_none());
    }
}
