use clap::Parser;

/// The `User-Agent` every request to `admin.opendatani.gov.uk` MUST carry.
/// That host 403s requests whose `User-Agent` looks automated (empty, or a
/// bot-shaped default) but serves a normal response to anything
/// browser-or-bot-IDENTIFYING -- confirmed directly, twice: once in
/// docs/superpowers/specs/2026-09-05-nir-tier-a-implementation-design.md
/// §1, and again in this crate's own planning pass (`curl -A
/// "<this string>" <either CSV URL>` -> HTTP 200; a bare `curl` with no
/// `-A` against the same URL was not re-tested this session, but the
/// design spec's own §1 account of a default `WebFetch`-tool fetch failing
/// identically is the same finding). This is NOT a workaround for a
/// deliberate anti-bot policy this app should route around quietly -- it's
/// a genuine, load-bearing production requirement: omit this and every
/// poll cycle 403s silently. See
/// docs/superpowers/plans/2026-09-05-nir-tier-a-implementation-plan.md's
/// Global Constraints.
pub const USER_AGENT: &str = "distant-signal-poller-nir-stations/1.0 (+https://github.com/FasterSpeeding/network-rail-status)";

/// CLI/env configuration for the `poller-nir-stations` service.
///
/// Both CSV URLs DO have working defaults, unlike every RDM poller's own
/// `baseUrl`: OpenDataNI's own CKAN download URLs are real, public,
/// key-free, anonymous-GET URLs, fetched and verified directly this
/// session (`curl -sL -A "<User-Agent above>" <url>` -> HTTP 200, full CSV
/// body) -- same "genuinely public endpoint gets a working default"
/// precedent `poller-irish-rail-gtfs::Config::gtfs_url`'s own doc comment
/// already established (`crates/poller-irish-rail-gtfs/src/config.rs:1-12`).
#[derive(Debug, Parser)]
pub struct Config {
    /// OpenDataNI's "Northern Ireland Railways Stations" CSV.
    #[arg(
        long,
        env,
        default_value = "https://admin.opendatani.gov.uk/dataset/5f27f171-b8aa-4511-983d-6df6e87bbf20/resource/967e32c3-1cc2-4aee-b485-92121a32eb4d/download/translink_rail_stations.csv"
    )]
    pub stations_csv_url: String,

    /// OpenDataNI's "Northern Ireland Railways Halts" CSV.
    #[arg(
        long,
        env,
        default_value = "https://admin.opendatani.gov.uk/dataset/1f2a94b9-1e86-4aec-ad9a-90a3de233893/resource/370b0d8a-29b9-46ca-bcc7-91357c28c43d/download/translink_halts.csv"
    )]
    pub halts_csv_url: String,

    /// The `api` crate's ingestion endpoint for the station catalogue --
    /// SAME endpoint `poller-irish-rail-gtfs` posts to (see
    /// docs/superpowers/plans/2026-09-05-nir-tier-a-implementation-plan.md
    /// Task 1's route-auth widening).
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

    /// OpenDataNI's own CKAN metadata confirms `frequency: "irregular"`
    /// for both CSVs (design spec §2.1/§2.2) -- no committed update
    /// cadence, at least as stale-tolerant as GTFS's unconfirmed one.
    /// Defaults to the same 24h convention `poller-irish-rail-gtfs`,
    /// `poller-stations`, and `poller-tocs` already use for reference data
    /// with an unconfirmed real refresh cadence.
    #[arg(long, env, default_value_t = 86400)]
    pub poll_interval_secs: u64,

    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,
    #[arg(long, env, default_value_t = true)]
    pub metrics_enabled: bool,
}
