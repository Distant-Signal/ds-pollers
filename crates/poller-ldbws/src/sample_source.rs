//! Where the station list to sample comes from (ingest architecture spec
//! §11.2, plan 4.5): `SAMPLE_STATIONS_SOURCE=http` (the default, today's
//! behaviour) asks the api's `GET /private/sample-stations`; `db` computes
//! the same list here, from the line catalogue in this image (`LINES_DIR`)
//! plus the custom lines and pin counts read through the views
//! `ingest_custom_line_stations` and `ingest_sample_station_pins` (no user
//! ids), as the read-only `ldbws_ro` role. Both run
//! `ds_store::reads::sample_stations::select_sample_stations`, so the two
//! lists are the same by construction, LEG-18's knobs included.

use common::LineDefinition;
use common::ingest::ReadSource;
use ds_store::reads::sample_stations::SampleSelection;

/// `pg_stat_activity.application_name` under `db`.
const APPLICATION_NAME: &str = "distant-signal-poller-ldbws";
/// Spec §6.6: pool 1, role limit 2. One read per cycle.
const DEFAULT_MAX_CONNECTIONS: u32 = 1;

/// `SAMPLE_STATIONS_SOURCE` and what `db` needs.
#[derive(Debug, Default, clap::Args)]
pub(crate) struct SampleStationsArgs {
    /// `http` (the default): `API_SAMPLE_STATIONS_URL`. `db`: computed here
    /// from `LINES_DIR` and Postgres (`DATABASE_URL`).
    #[arg(long, env, value_enum, default_value_t = ReadSource::Http)]
    pub sample_stations_source: ReadSource,

    /// Postgres, for `db` (required then, unused otherwise). Pool size and
    /// timeouts come from the shared `DATABASE_*` variables (`common::pg`);
    /// the default pool is 1 (spec §6.6, role limit 2).
    #[arg(long, env, hide_env_values = true)]
    pub database_url: Option<common::secret::Secret>,

    /// The line catalogue, for `db`: the same `lines/*.toml` the api
    /// serves the list from, baked into the image at `/app/lines`. Read
    /// only under `db`.
    #[arg(long = "lines-dir", env = "LINES_DIR", default_value = "/app/lines")]
    pub lines_dir: String,

    /// Set at startup under `db` ([`SampleStationsArgs::connect`]); `None`
    /// means `http`. Not an argument: it holds the pool and the catalogue,
    /// so `fetch_sample_stations` keeps the signature every caller already
    /// uses.
    #[arg(skip)]
    pub direct: Option<DirectSampleStations>,
}

impl SampleStationsArgs {
    /// `db` needs a `DATABASE_URL`.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if self.sample_stations_source == ReadSource::Db
            && self
                .database_url
                .as_ref()
                .is_none_or(common::secret::Secret::is_empty)
        {
            anyhow::bail!("SAMPLE_STATIONS_SOURCE=db needs DATABASE_URL");
        }
        Ok(())
    }

    /// Under `db`: loads the catalogue (an empty or broken one is a startup
    /// error, not an empty station list), waits for Postgres (beating
    /// `progress`), connects the pool and passes the schema gate as
    /// `ldbws_ro`, then stores the result in [`Self::direct`]. Nothing
    /// under `http`.
    pub(crate) async fn connect(&mut self, progress: &health_http::Progress) -> anyhow::Result<()> {
        if self.sample_stations_source != ReadSource::Db {
            return Ok(());
        }
        let url = self
            .database_url
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("SAMPLE_STATIONS_SOURCE=db needs DATABASE_URL"))?;
        let catalogue = common::config::parse_lines(&self.lines_dir)?;
        anyhow::ensure!(
            !catalogue.is_empty(),
            "no line definitions in {}: refusing to sample from an empty catalogue",
            self.lines_dir
        );
        common::startup::retry_until_ready(
            "Postgres",
            common::startup::CONNECT_BACKOFF,
            Some(progress),
            || async {
                use sqlx::Connection;
                sqlx::PgConnection::connect(url.expose())
                    .await?
                    .close()
                    .await
            },
        )
        .await;
        ds_store::pool::register_metrics();
        let pool =
            ds_store::pool::PoolSettings::from_env(APPLICATION_NAME, DEFAULT_MAX_CONNECTIONS)?
                .connect(url.expose())
                .await?;
        ds_store::schema::wait_for_schema(&pool, ds_store::schema::DbRole::LdbwsRo, Some(progress))
            .await?;
        tracing::info!(
            lines = catalogue.len(),
            "computing the sample stations from the catalogue and Postgres (SAMPLE_STATIONS_SOURCE=db)"
        );
        self.direct = Some(DirectSampleStations {
            pool,
            catalogue: catalogue.to_vec(),
        });
        Ok(())
    }
}

/// `SAMPLE_STATIONS_SOURCE=db`'s state: the pool and the catalogue.
#[derive(Debug)]
pub(crate) struct DirectSampleStations {
    pub pool: sqlx::PgPool,
    pub catalogue: Vec<LineDefinition>,
}

impl DirectSampleStations {
    /// What `GET /private/sample-stations` answers for these knobs.
    pub(crate) async fn select(&self, selection: SampleSelection) -> anyhow::Result<Vec<String>> {
        ds_store::reads::select_sample_stations_from(&self.pool, &self.catalogue, selection).await
    }
}

/// The LEG-18 knobs as the api's query parameters carry them
/// (`sample_stations_url`): 0 is no cap.
pub(crate) fn selection(pinned_lines_only: bool, max_stations: u32) -> SampleSelection {
    SampleSelection {
        pinned_lines_only,
        max_stations: (max_stations > 0)
            .then(|| usize::try_from(max_stations).unwrap_or(usize::MAX)),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::config::Config;

    use super::*;

    fn parse(extra: &[&str]) -> Config {
        let base = [
            "poller-ldbws",
            "--ldbws-base-url",
            "https://example.invalid",
            "--rdm-api-key",
            "key",
            "--internal-oauth-token-url",
            "http://authentik.example/token",
            "--internal-oauth-client-id",
            "client-id",
            "--internal-oauth-username",
            "svc-account",
            "--internal-oauth-password",
            "svc-password",
        ];
        // The test environment may set DATABASE_URL.
        let no_database: &[&str] = if extra.contains(&"--database-url") {
            &[]
        } else {
            &["--database-url", ""]
        };
        Config::try_parse_from(base.iter().chain(no_database).chain(extra)).unwrap()
    }

    /// Plan 4.5: `http` by default; `db` needs a database.
    #[test]
    fn the_source_defaults_to_http_and_db_needs_a_database_url() {
        let config = parse(&[]);
        assert_eq!(config.reads.sample_stations_source, ReadSource::Http);
        assert_eq!(config.reads.lines_dir, "/app/lines");
        assert!(config.reads.direct.is_none());
        config.reads.validate().unwrap();
        let db = parse(&["--sample-stations-source", "db"]);
        assert!(db.reads.validate().is_err());
        let db = parse(&[
            "--sample-stations-source",
            "db",
            "--database-url",
            "postgres://distant_signal_ldbws_ro:pw@postgres/ds",
        ]);
        db.reads.validate().unwrap();
        assert!(!format!("{db:?}").contains(":pw@"));
    }

    /// The chart sets it (templates/poller-deployments.yaml passes it to
    /// `distant-signal.internalReadsEnv`).
    #[test]
    fn the_source_reads_the_chart_s_env_name() {
        use clap::CommandFactory;
        let command = Config::command();
        let env = |id: &str| {
            command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_env())
                .map(|env| env.to_string_lossy().into_owned())
        };
        assert_eq!(
            env("sample_stations_source").as_deref(),
            Some("SAMPLE_STATIONS_SOURCE")
        );
        assert_eq!(env("database_url").as_deref(), Some("DATABASE_URL"));
        let template = std::fs::read_to_string(
            common::manifest_dir!()
                .join("../../charts/distant-signal/templates/poller-deployments.yaml"),
        )
        .unwrap();
        assert!(template.contains("\"SAMPLE_STATIONS_SOURCE\""));
    }

    #[test]
    fn the_knobs_map_onto_the_api_s_query_parameters() {
        assert_eq!(selection(false, 0), SampleSelection::default());
        assert_eq!(
            selection(true, 150),
            SampleSelection {
                pinned_lines_only: true,
                max_stations: Some(150),
            }
        );
    }

    /// The api's own inputs (`custom_lines::list_custom_lines`,
    /// `preferences::count_pins_per_line`) through the same selection.
    async fn api_selection(
        pool: &sqlx::PgPool,
        catalogue: &[LineDefinition],
        selection: SampleSelection,
    ) -> Vec<String> {
        type Row = (
            String,
            String,
            Vec<String>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
        );
        let custom: Vec<Row> = sqlx::query_as(
            "SELECT id, name, operators, stations, headcode_prefixes, destination_crs_filter \
             FROM custom_lines ORDER BY created_at",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        let mut lines = catalogue.to_vec();
        lines.extend(custom.into_iter().map(
            |(id, name, operators, stations, headcode_prefixes, destination_crs_filter)| {
                LineDefinition::from(common::CustomLine {
                    id,
                    name,
                    operators,
                    stations,
                    headcode_prefixes,
                    destination_crs_filter,
                })
            },
        ));
        let pins: std::collections::HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
            "SELECT line_id, COUNT(*) AS pins FROM pinned_lines GROUP BY line_id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .collect();
        ds_store::reads::sample_stations::select_sample_stations(&lines, &pins, selection)
    }

    /// Plan 4.5: with pins and custom lines, the list computed here equals
    /// the api's for every combination of the LEG-18 knobs.
    #[sqlx::test(migrations = "../ds-store/migrations")]
    #[ignore = "needs DATABASE_URL (a role that can create databases)"]
    async fn the_db_selection_equals_the_api_s(pool: sqlx::PgPool) {
        let lines_dir = common::manifest_dir!().join("../../lines");
        let catalogue = common::config::parse_lines(&lines_dir.to_string_lossy())
            .unwrap()
            .to_vec();
        assert!(catalogue.len() > 2);
        for user in ["u-ldbws-1", "u-ldbws-2"] {
            sqlx::query("INSERT INTO users (id) VALUES ($1)")
                .bind(user)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (user, line) in [
            ("u-ldbws-1", catalogue[0].id.as_str()),
            ("u-ldbws-2", catalogue[0].id.as_str()),
            ("u-ldbws-1", catalogue[1].id.as_str()),
            ("u-ldbws-2", "custom-ldbws-commute"),
        ] {
            sqlx::query("INSERT INTO pinned_lines (user_id, line_id) VALUES ($1, $2)")
                .bind(user)
                .bind(line)
                .execute(&pool)
                .await
                .unwrap();
        }
        for (id, user, stations) in [
            (
                "custom-ldbws-commute",
                "u-ldbws-1",
                vec!["ZZA", " wok", "WAT"],
            ),
            ("custom-ldbws-other", "u-ldbws-2", vec!["ZZB"]),
        ] {
            sqlx::query(
                "INSERT INTO custom_lines (id, name, stations, user_id) VALUES ($1, $1, $2, $3)",
            )
            .bind(id)
            .bind(&stations)
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        }

        let direct = DirectSampleStations {
            pool: pool.clone(),
            catalogue: catalogue.clone(),
        };
        for (pinned_lines_only, max_stations) in [(false, 0), (true, 0), (false, 7), (true, 3)] {
            let knobs = selection(pinned_lines_only, max_stations);
            let ours = direct.select(knobs).await.unwrap();
            assert_eq!(
                ours,
                api_selection(&pool, &catalogue, knobs).await,
                "{knobs:?}"
            );
            assert!(!ours.is_empty(), "{knobs:?}");
        }
        let all = direct.select(SampleSelection::default()).await.unwrap();
        assert!(all.contains(&"ZZA".to_string()) && all.contains(&"WOK".to_string()));
    }
}
