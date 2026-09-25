use clap::Parser;

/// CLI/env configuration for the `poller-irish-rail-live` service.
///
/// Unlike `poller-irish-rail-gtfs`, this crate needs no `api`-hosted
/// station list at all -- it discovers its own station codes from
/// `api.irishrail.ie`'s own `getAllStationsXML` call each cycle. See
/// docs/superpowers/plans/2026-09-05-ireland-rail-support-plan.md's
/// Judgment Call #1 for why this is deliberate, not a missed
/// simplification.
///
/// Modeled on `crates/poller-ldbws/src/config.rs`'s current shape (the
/// closest existing precedent for "make one API call per station each
/// cycle"): `internal_oauth`/`metrics` are `#[command(flatten)]`d shared
/// arg blocks (`common::oauth_client::InternalOAuthArgs`,
/// `common::service_args::MetricsArgs`) rather than the 5+1 individual
/// fields the sibling `poller-irish-rail-gtfs` crate still hand-rolls --
/// that crate predates
/// docs/superpowers/specs/2026-09-05-rust-service-deduplication-design.md's
/// dedup pass; this one is written after it, so it uses the shared blocks
/// directly. Flattening changes no CLI flag or env var name (`common::oauth_client::InternalOAuthArgs`'s
/// own field names/`#[arg]` attributes are byte-for-byte what every poller
/// already exposed individually).
#[derive(Debug, Parser)]
pub struct Config {
    /// `api.irishrail.ie`'s legacy realtime ASMX service root -- real,
    /// key-free, confirmed reachable directly (friction doc section 1;
    /// re-confirmed live during this crate's own implementation:
    /// `getAllStationsXML` returned 171 stations, `getStationDataByCodeXML`
    /// returned real live departure data for both `BFSTC` and `CNLLY`).
    #[arg(
        long,
        env,
        default_value = "http://api.irishrail.ie/realtime/realtime.asmx"
    )]
    pub irish_rail_base_url: String,

    /// The `api` crate's ingestion endpoint for station samples.
    #[arg(
        long,
        env,
        default_value = "http://api:8080/private/island-of-ireland-station-samples"
    )]
    pub api_ingest_url: String,

    /// Shared, non-secret OAuth2 client-credentials config (same value
    /// across every real caller).
    #[command(flatten)]
    pub internal_oauth: common::oauth_client::InternalOAuthArgs,

    /// Conservative default (5 minutes), NOT `poller-ldbws`'s 60s, despite
    /// the structural similarity -- `api.irishrail.ie`'s real rate limits
    /// and operational durability are unconfirmed (design spec section 8,
    /// open question 4: "apparently unmaintained since ~2012... whether it
    /// has any informal rate limits... are all unconfirmed"), and this
    /// crate polls EVERY station returned by `getAllStationsXML` each
    /// cycle (171 per this crate's own live confirmation, not a curated
    /// subset like GB LDBWS's `sample_stations`, since Tier B has no
    /// line-catalogue coupling to curate against -- see this plan's
    /// Judgment Call #3). 300s bounds the total request volume against an
    /// unconfirmed legacy API more conservatively than GB LDBWS's own 60s
    /// does against a modern, documented, actively-supported one.
    #[arg(long, env, default_value_t = 300)]
    pub poll_interval_secs: u64,

    /// Optional comma-separated allowlist of station codes to poll,
    /// bypassing `getAllStationsXML`'s own full list -- an operator escape
    /// hatch if polling all ~171 stations every cycle turns out to be too
    /// aggressive against this unconfirmed-capacity upstream. Empty (the
    /// default) means "poll everything `getAllStationsXML` returns."
    ///
    /// Deliberately has NO `default_value`. `#[arg(value_delimiter = ',')]`
    /// only splits on the delimiter when a value is actually supplied --
    /// `default_value = ""` (the previous code here) parses into a
    /// one-element `[""]`, not `[]`, because clap applies the default value
    /// as a single already-final token rather than re-running it through
    /// the delimiter split. That made `main.rs`'s
    /// `station_codes_override.is_empty()` check ALWAYS false in
    /// production (the Helm chart only sets `STATION_CODES_OVERRIDE` when
    /// non-empty -- see
    /// `charts/distant-signal/templates/poller-irish-rail-live-deployment.yaml`),
    /// so the `getAllStationsXML` discovery fallback below never ran and
    /// every cycle queried exactly one station with an empty code. Leaving
    /// this field with no `default_value` lets clap fall back to `Vec`'s
    /// own real `Default` (an empty vec) when the flag/env var is absent
    /// entirely, which is what "poll everything" actually needs. See
    /// `tests::station_codes_override_defaults_to_an_empty_vec_not_one_empty_string`
    /// for the regression test that would have caught this.
    #[arg(long, env, value_delimiter = ',')]
    pub station_codes_override: Vec<String>,

    /// Port for this poller's Prometheus `/metrics` endpoint. Stays a
    /// plain field, not part of `MetricsArgs` -- its default differs per
    /// crate and `docker-compose.yml` relies on the code default.
    #[arg(long, env, default_value_t = 9091)]
    pub metrics_port: u16,

    #[command(flatten)]
    pub metrics: common::service_args::MetricsArgs,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Config;

    /// The only args every real deployment supplies -- `internal_oauth`'s 4
    /// required (no-default) fields. Everything else, including
    /// `station_codes_override`, is left unset so this exercises exactly
    /// the "STATION_CODES_OVERRIDE absent" shape the Helm chart's
    /// conditional `{{- if .Values.pollerIrishRailLive.stationCodesOverride }}`
    /// produces in production.
    fn required_oauth_args() -> Vec<&'static str> {
        vec![
            "poller-irish-rail-live",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "secret",
        ]
    }

    #[test]
    fn station_codes_override_defaults_to_an_empty_vec_not_one_empty_string() {
        // Reproduces the real production bug: with no
        // `--station-codes-override`/`STATION_CODES_OVERRIDE` supplied at
        // all (exactly what happens when the Helm chart's conditional env
        // block is skipped), the parsed vec must be genuinely empty so
        // `main.rs`'s `station_codes_override.is_empty()` check takes the
        // `getAllStationsXML` discovery branch. Before this fix,
        // `default_value = ""` made this assert fail: the vec came back as
        // `[""]` (len 1), never empty.
        let config = Config::try_parse_from(required_oauth_args())
            .expect("only the required OAuth args should be needed to parse");
        assert_eq!(
            config.station_codes_override,
            Vec::<String>::new(),
            "an absent STATION_CODES_OVERRIDE must parse to an empty Vec, not [\"\"] -- \
             a non-empty vec here permanently defeats station discovery in production"
        );
        assert!(config.station_codes_override.is_empty());
    }

    #[test]
    fn station_codes_override_still_splits_a_supplied_comma_separated_value() {
        let mut args = required_oauth_args();
        args.push("--station-codes-override");
        args.push("BFSTC,CNLLY");
        let config = Config::try_parse_from(args)
            .expect("required OAuth args plus the override should parse");
        assert_eq!(
            config.station_codes_override,
            vec!["BFSTC".to_string(), "CNLLY".to_string()]
        );
    }
}
