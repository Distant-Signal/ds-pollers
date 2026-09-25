use clap::Parser;

/// CLI/env configuration for the `poller-irish-rail-gtfs` service.
///
/// `gtfs_url` DOES have a working default, unlike every RDM poller's own
/// `baseUrl` (which is account-specific and unpublished): the friction doc
/// (docs/superpowers/specs/2026-09-05-ireland-vs-northern-ireland-friction-research.md
/// §1) confirms this is a real, public, key-free, anonymous-GET URL,
/// downloaded and verified directly in that research session -- matching
/// `poller-tfl`'s own precedent (`TFL_BASE_URL` defaults to the real TfL
/// API root) for "a genuinely public endpoint gets a working default,
/// unlike an account-gated one."
///
/// Signal Box Audit, poll-area Low finding -- "secret-bearing config
/// structs derive Debug": does NOT derive `Debug`. `internal_oauth_password`
/// is a real Authentik service-account credential; a derived `Debug` would
/// print it in full to any future `tracing::debug!("{config:?}")`, matching
/// the same class of bug already fixed for
/// `common::oauth_client::OAuthCredentials`/`InternalOAuthArgs` (this crate
/// predates the shared-args dedup pass, so it still hand-rolls the
/// individual `internal_oauth_*` fields rather than flattening
/// `InternalOAuthArgs` in, and therefore needs its own redacting impl). The
/// hand-written impl below redacts it.
#[derive(Parser)]
pub struct Config {
    /// Transport for Ireland's public GTFS zip for Iarnród Éireann.
    #[arg(
        long,
        env,
        default_value = "https://www.transportforireland.ie/transitData/Data/GTFS_Irish_Rail.zip"
    )]
    pub gtfs_url: String,

    /// The `api` crate's ingestion endpoint for the station catalogue.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/island-of-ireland-stations"
    )]
    pub api_stations_ingest_url: String,

    /// The `api` crate's ingestion endpoint for the line catalogue.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/island-of-ireland-lines"
    )]
    pub api_lines_ingest_url: String,

    #[arg(long, env)]
    pub internal_oauth_token_url: String,
    #[arg(long, env)]
    pub internal_oauth_client_id: String,
    #[arg(long, env, default_value = "groups")]
    pub internal_oauth_scope: String,
    #[arg(long, env)]
    pub internal_oauth_username: String,
    #[arg(long, env)]
    pub internal_oauth_password: String,

    /// The friction doc confirms `feed_start_date`/`feed_end_date` show "a
    /// live, rolling one-year window" but never states how often the feed
    /// itself is regenerated -- unlike RDM's `stations`/`tocs` feeds, whose
    /// spec explicitly recommends a 24-hour poll. Defaulted to the same
    /// 24-hour cadence as `poller-stations`/`poller-tocs`
    /// (`crates/poller-stations/src/config.rs`'s own `poll_interval_secs`
    /// default) as the conservative, already-established convention for
    /// "static reference data with an unconfirmed real refresh cadence" --
    /// not a confirmed fact about this specific feed's own update
    /// frequency.
    #[arg(long, env, default_value_t = 86400)]
    pub poll_interval_secs: u64,

    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("gtfs_url", &self.gtfs_url)
            .field("api_stations_ingest_url", &self.api_stations_ingest_url)
            .field("api_lines_ingest_url", &self.api_lines_ingest_url)
            .field("internal_oauth_token_url", &self.internal_oauth_token_url)
            .field("internal_oauth_client_id", &self.internal_oauth_client_id)
            .field("internal_oauth_scope", &self.internal_oauth_scope)
            .field("internal_oauth_username", &self.internal_oauth_username)
            .field("internal_oauth_password", &"[REDACTED]")
            .field("poll_interval_secs", &self.poll_interval_secs)
            .field("metrics_port", &self.metrics_port)
            .field("metrics_enabled", &self.metrics_enabled)
            .finish()
    }
}

#[cfg(test)]
mod config_debug_tests {
    use clap::Parser;

    use super::Config;

    #[test]
    fn debug_redacts_the_internal_oauth_password() {
        let config = Config::try_parse_from([
            "poller-irish-rail-gtfs",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "super-secret-password",
        ])
        .expect("required args should parse");

        let debug_output = format!("{config:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "internal_oauth_password must be redacted: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-password"),
            "the real internal_oauth_password must never appear in Debug output: {debug_output}"
        );
    }
}
