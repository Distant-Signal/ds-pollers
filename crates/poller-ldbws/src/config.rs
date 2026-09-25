use clap::Parser;

/// CLI/env configuration for the `poller-ldbws` service.
///
/// `ldbws_base_url` deliberately has no default: research found two
/// different RDM product-slug segments in use across sources
/// (`1010-live-departure-board-dep` vs `...-dep1_2`) with no way to
/// reconcile which is currently correct without a live RDM subscription —
/// this must be supplied out of band once confirmed, not guessed.
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `rdm_api_key` is a real
/// RDM Live Departure Board API key; a derived `Debug` would print it in
/// full to any future `tracing::debug!("{config:?}")`, matching the same
/// class of bug already fixed for `common::oauth_client::OAuthCredentials`
/// and `common::service_args::KafkaConnectionArgs`. The hand-written impl
/// below redacts it.
#[derive(Parser)]
pub struct Config {
    /// RDM Live Departure Board base URL, up to and including the
    /// `/LDBWS/api/20220120` segment. The poller appends
    /// `/GetDepBoardWithDetails/{crs}` itself (see `main.rs`).
    #[arg(long, env)]
    pub ldbws_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`). Community sources describe
    /// this as the "consumer key" specifically (as opposed to a paired
    /// "consumer secret") — unconfirmed against RDM's own docs, but
    /// consistent with how the other three pollers authenticate.
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// Number of services requested per station per cycle (LDBWS's own
    /// `numRows` query parameter), used as the *first* attempt each cycle.
    /// Kept at the upstream API's own default (10) rather than lowering it
    /// globally: the repo owner's own empirical finding (a smaller
    /// `numRows` succeeds where 10 fails for a busy terminus like PAD
    /// during rush hour, evidenced by a persisting "500 Internal Server
    /// Error" from `GetDepBoardWithDetails`) points at a busyness-scaled
    /// upstream limit, not a value that's simply too high everywhere --
    /// lowering the global default would needlessly shrink every quiet
    /// station's data too, while still potentially being wrong for the
    /// busiest days at the busiest few. `main.rs`'s `fetch_departures`
    /// instead retries a 500 with progressively smaller `numRows` values
    /// (see `numrows_step_down`) *per station, per cycle*, so this value
    /// stays the richest one that's actually asked for, with a station-
    /// local fallback only when the upstream evidence says it's needed.
    /// See docs/superpowers/specs/2026-08-31-sample-data-availability-design.md's
    /// Correction 1: the aggregator's `min_sample_size` default is only 3
    /// relevant departures, pooled across a whole line's stations, so a
    /// reduced `numRows` at one busy station is far from a lossy trade.
    #[arg(long, env, default_value_t = 10)]
    pub num_rows: u32,

    /// The `api` crate's endpoint for the deduplicated list of stations to
    /// sample (`GET /private/sample-stations`) — not an RDM endpoint.
    #[arg(long, env, default_value = "http://api:8080/private/sample-stations")]
    pub api_sample_stations_url: String,

    /// The `api` crate's ingestion endpoint for station samples.
    #[arg(long, env, default_value = "http://api:8080/private/station-samples")]
    pub api_ingest_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across all 9 real callers).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// DESIGN.md §4's aggregator polling cadence target is "30-60s"; 60 is
    /// the conservative end, given this feed's real rate limit is
    /// unconfirmed (see module docs in `main.rs`).
    #[arg(long, env, default_value_t = 60)]
    pub poll_interval_secs: u64,

    /// Port for this poller's Prometheus `/metrics` endpoint. Stays a
    /// plain field, not part of `MetricsArgs` -- its default differs per
    /// crate and `docker-compose.yml` relies on the code default.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("ldbws_base_url", &self.ldbws_base_url)
            .field("rdm_api_key", &"[REDACTED]")
            .field("num_rows", &self.num_rows)
            .field("api_sample_stations_url", &self.api_sample_stations_url)
            .field("api_ingest_url", &self.api_ingest_url)
            .field("internal_oauth", &self.internal_oauth)
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics", &self.metrics)
            .finish()
    }
}

#[cfg(test)]
mod config_debug_tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_rdm_api_key() {
        let config = Config::try_parse_from([
            "poller-ldbws",
            "--ldbws-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "super-secret-rdm-key",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "svc-password",
        ])
        .expect("required args should parse");

        let debug_output = format!("{config:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "rdm_api_key must be redacted: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-rdm-key"),
            "the real rdm_api_key must never appear in Debug output: {debug_output}"
        );
        assert!(
            !debug_output.contains("svc-password"),
            "the real internal_oauth password must never appear in Debug output: {debug_output}"
        );
    }
}
