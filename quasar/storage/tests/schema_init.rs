//! Smoke tests for the V3 `schema::init.sql` loader.
//!
//! These tests boot an embedded PostgreSQL instance, execute the V3 DDL, and
//! query `information_schema` / `pg_trigger` / `pg_indexes` to assert the
//! database actually contains the objects the design requires. They are
//! `#[ignore]` by default because starting embedded Postgres adds ~5s of
//! latency; run them explicitly with
//!
//! ```text
//! cargo test -p quasar-storage --test schema_init -- --ignored
//! ```

#![allow(clippy::unwrap_used, clippy::expect_used)]

use deadpool_postgres::{Pool, Runtime};
use postgresql_embedded::PostgreSQL;
use quasar_storage::schema;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

struct PgInstance {
    // Kept to extend the embedded Postgres lifetime across the test run.
    #[allow(dead_code)]
    postgresql: PostgreSQL,
    url: String,
}

impl PgInstance {
    async fn get() -> &'static Self {
        PG_INSTANCE
            .get_or_init(|| async {
                let mut postgresql = PostgreSQL::default();
                postgresql.setup().await.expect("PostgreSQL setup failed");
                postgresql.start().await.expect("PostgreSQL start failed");
                postgresql
                    .create_database("quasar_test_v3_schema")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_v3_schema");
                PgInstance { postgresql, url }
            })
            .await
    }
}

fn test_pool(url: &str) -> Pool {
    let config = url
        .parse::<tokio_postgres::Config>()
        .expect("invalid database URL");
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool")
}

/// Drop every object that V3 init.sql owns so each test starts on a blank
/// database. This keeps the embedded PG instance reusable across tests in
/// the same file without leaking state from a prior `initialize` call.
async fn reset_schema(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS
                asset_permissions,
                tabular_asset_versions,
                asset_versions,
                tabular_assets,
                assets,
                namespaces,
                domains,
                tabular_formats,
                asset_types
            CASCADE;
            DROP FUNCTION IF EXISTS ensure_tabular_asset_type() CASCADE;
            DROP FUNCTION IF EXISTS ensure_previous_version_same_asset() CASCADE;
            "#,
        )
        .await
        .expect("failed to reset schema");
}

async fn fetch_existing_table_names(client: &tokio_postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .expect("query information_schema.tables failed");
    rows.into_iter()
        .map(|r| r.get::<_, String>("table_name"))
        .collect()
}

async fn fetch_existing_trigger_names(client: &tokio_postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = 'public'",
            &[],
        )
        .await
        .expect("query information_schema.triggers failed");
    rows.into_iter()
        .map(|r| r.get::<_, String>("trigger_name"))
        .collect()
}

async fn fetch_existing_index_names(client: &tokio_postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
            &[],
        )
        .await
        .expect("query pg_indexes failed");
    rows.into_iter()
        .map(|r| r.get::<_, String>("indexname"))
        .collect()
}

#[tokio::test]
#[ignore]
#[serial]
async fn init_sql_creates_all_v3_objects() {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let client = pool.get().await.expect("failed to get client");

    reset_schema(&client).await;
    schema::initialize(&client)
        .await
        .expect("schema::initialize failed");

    let tables = fetch_existing_table_names(&client).await;
    for expected in [
        "asset_types",
        "tabular_formats",
        "domains",
        "namespaces",
        "assets",
        "tabular_assets",
        "asset_versions",
        "tabular_asset_versions",
        "asset_permissions",
    ] {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected table `{}` to exist, but found {:?}",
            expected,
            tables
        );
    }

    let triggers = fetch_existing_trigger_names(&client).await;
    for expected in [
        "trg_tabular_assets_asset_type",
        "trg_asset_versions_previous_same_asset",
    ] {
        assert!(
            triggers.iter().any(|t| t == expected),
            "expected trigger `{}` to exist, but found {:?}",
            expected,
            triggers
        );
    }

    let indexes = fetch_existing_index_names(&client).await;
    for expected in [
        "uq_assets_active_name",
        "uq_asset_versions_order",
        "idx_asset_versions_latest",
        "idx_tabular_assets_format_asset",
        "idx_assets_type_active",
        "idx_namespaces_domain",
        "idx_asset_versions_previous",
    ] {
        assert!(
            indexes.iter().any(|i| i == expected),
            "expected index `{}` to exist, but found {:?}",
            expected,
            indexes
        );
    }

    // Registry seeds: both initial format rows are present and `iceberg` is
    // marked as `supports_cas_commit = TRUE`.
    let formats = client
        .query(
            "SELECT name, supports_cas_commit FROM tabular_formats ORDER BY name",
            &[],
        )
        .await
        .expect("query tabular_formats failed");
    assert_eq!(formats.len(), 2, "expected two seeded tabular formats");
    assert_eq!(formats[0].get::<_, String>("name"), "iceberg");
    assert!(formats[0].get::<_, bool>("supports_cas_commit"));
    assert_eq!(formats[1].get::<_, String>("name"), "lance");
    assert!(!formats[1].get::<_, bool>("supports_cas_commit"));

    // Default domain seed: a single 'default' row exists after init.
    let default_domain_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::BIGINT AS c FROM domains WHERE name = 'default'",
            &[],
        )
        .await
        .expect("count default domain failed")
        .get("c");
    assert_eq!(
        default_domain_count, 1,
        "expected exactly one seeded 'default' domain row"
    );
}

#[tokio::test]
#[ignore]
#[serial]
async fn init_sql_is_idempotent() {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let client = pool.get().await.expect("failed to get client");

    reset_schema(&client).await;

    // First run creates everything.
    schema::initialize(&client)
        .await
        .expect("first initialize() failed");

    // Second run on the populated database must succeed silently.
    schema::initialize(&client)
        .await
        .expect("re-running initialize() must be idempotent");

    // Registry seeds did not duplicate.
    let asset_type_count: i64 = client
        .query_one("SELECT COUNT(*)::BIGINT AS c FROM asset_types", &[])
        .await
        .expect("count asset_types failed")
        .get("c");
    assert_eq!(asset_type_count, 1);

    let format_count: i64 = client
        .query_one("SELECT COUNT(*)::BIGINT AS c FROM tabular_formats", &[])
        .await
        .expect("count tabular_formats failed")
        .get("c");
    assert_eq!(format_count, 2);

    let domain_count: i64 = client
        .query_one("SELECT COUNT(*)::BIGINT AS c FROM domains", &[])
        .await
        .expect("count domains failed")
        .get("c");
    assert_eq!(
        domain_count, 1,
        "default domain seed must not duplicate on re-initialize"
    );
}
