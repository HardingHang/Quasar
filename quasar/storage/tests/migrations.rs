//! Migration runner tests (DESIGN §8.2 / §8.4).
//!
//! Boots an embedded PostgreSQL instance, applies the embedded migrations
//! via `PgCatalogStore::initialize`, and asserts the resulting schema:
//! every table/index exists, no triggers are used (invariants live in the
//! application layer), seeds are present, and re-running is idempotent.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use quasar_storage::{AssetTypeStore, DomainStore};
use serial_test::serial;

const EXPECTED_TABLES: &[&str] = &[
    "asset_types",
    "formats",
    "domains",
    "namespaces",
    "assets",
    "asset_versions",
    "asset_tags",
    "tabular_assets",
    "view_assets",
    "schema_migrations",
    "iceberg_staged_tables",
    "iceberg_scan_metrics_reports",
    "iceberg_purge_operations",
];

const EXPECTED_INDEXES: &[&str] = &[
    "uq_assets_active_name",
    "idx_assets_type_active",
    "idx_assets_namespace",
    "uq_asset_versions_root",
];

async fn table_names(client: &tokio_postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .expect("query information_schema.tables failed");
    rows.iter().map(|r| r.get(0)).collect()
}

async fn index_names(client: &tokio_postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'",
            &[],
        )
        .await
        .expect("query pg_indexes failed");
    rows.iter().map(|r| r.get(0)).collect()
}

#[tokio::test]
#[serial]
async fn initialize_creates_full_schema() {
    let (_store, pool) = common::setup().await;
    let client = pool.get().await.unwrap();

    let tables = table_names(&client).await;
    for expected in EXPECTED_TABLES {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected table `{expected}`, found {tables:?}"
        );
    }

    let indexes = index_names(&client).await;
    for expected in EXPECTED_INDEXES {
        assert!(
            indexes.iter().any(|i| i == expected),
            "expected index `{expected}`, found {indexes:?}"
        );
    }

    // Cross-row invariants are enforced at the application layer; the
    // schema must not install any triggers (AGENTS.md red line).
    let trigger_count: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema = 'public'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(trigger_count, 0, "schema must not define triggers");

    // The migration runner recorded exactly the baseline version.
    let versions: Vec<i64> = client
        .query("SELECT version FROM schema_migrations", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(versions, vec![1]);
}

#[tokio::test]
#[serial]
async fn initialize_is_idempotent() {
    let (store, pool) = common::setup().await;

    // Second run on the migrated database must succeed silently.
    store.initialize().await.expect("re-initialize failed");

    let client = pool.get().await.unwrap();
    let migration_count: i64 = client
        .query_one("SELECT COUNT(*) FROM schema_migrations", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(migration_count, 1, "migration must not be re-applied");

    // Seeds did not duplicate.
    let domain_count: i64 = client
        .query_one("SELECT COUNT(*) FROM domains WHERE name = 'default'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(domain_count, 1);
}

#[tokio::test]
#[serial]
async fn seeds_are_present_after_initialize() {
    let (store, _pool) = common::setup().await;

    let default = store.get_domain("default").await.unwrap();
    assert_eq!(default.name, "default");

    let table = store.get_asset_type("table").await.unwrap();
    assert_eq!(table.category, "tabular");
    assert_eq!(table.extension_strategy, "dedicated_table");
    assert!(table.supports_native_protocol);

    let view = store.get_asset_type("view").await.unwrap();
    assert_eq!(view.category, "view");

    let iceberg = store.get_format("iceberg").await.unwrap();
    assert_eq!(iceberg.serialization_hint.as_deref(), Some("json"));

    store.get_format("lance").await.unwrap();
}
