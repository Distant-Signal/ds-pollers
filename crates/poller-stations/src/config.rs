use clap::Parser;

/// CLI/env configuration for the `poller-stations` service.
///
/// `rdm_stations_base_url` deliberately has no default: RSPS5050 P-03-00
/// Rev A §6 confirms the path suffix (`/stations`), but the host portion of
/// the URL is account-specific and not published in the spec, so a
/// missing/misconfigured URL must fail loudly at startup rather than
/// silently poll the wrong thing.
#[derive(Debug, Parser)]
pub struct Config {
    /// RDM Stations feed base URL, e.g. `https://<host>/json/1.0`. The
    /// poller appends `/stations` itself (see `main.rs`).
    #[arg(long, env)]
    pub rdm_stations_base_url: String,

    /// RDM API key, sent via the `x-apikey` header (see
    /// `RDM_AUTH_HEADER_NAME` in `main.rs`).
    #[arg(long, env)]
    pub rdm_api_key: String,

    /// The `api` crate's ingestion endpoint for stations.
    #[arg(long, env, default_value = "http://api:8080/private/stations")]
    pub api_ingest_url: String,

    /// Shared secret sent via `X-Internal-Token` to reach the ingestion
    /// endpoint (see `crates/api/src/auth.rs`).
    #[arg(long, env)]
    pub internal_token: String,

    /// RSPS5050 P-03-00 Rev A §6: "updated overnight; Poll frequency should
    /// only be once every 24 hours."
    #[arg(long, env, default_value_t = 86400)]
    pub poll_interval_secs: u64,
}
