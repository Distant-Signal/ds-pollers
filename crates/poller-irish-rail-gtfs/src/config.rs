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
#[derive(Debug, Parser)]
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
