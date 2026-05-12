#![allow(clippy::unwrap_used, clippy::expect_used)]

use deadpool_postgres::{Pool, Runtime};
use postgresql_embedded::PostgreSQL;
use quasar_storage::{AssetFormat, CatalogStore, PatchField, PgCatalogStore, StoreError};
use serial_test::serial;
use std::collections::HashMap;
use tokio::sync::OnceCell;

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

struct PgInstance {
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
                    .create_database("quasar_test_v2")
                    .await
                    .expect("create database failed");

                let url = postgresql.settings().url("quasar_test_v2");
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

async fn setup() -> PgCatalogStore {
    let (store, _) = setup_with_pool().await;
    store
}

async fn setup_with_pool() -> (PgCatalogStore, Pool) {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let store = PgCatalogStore::new(pool.clone());

    let client = pool.get().await.expect("failed to get client");

    // Clean slate: drop any existing V1, V2, or V3 catalog objects from the
    // shared embedded PostgreSQL instance (OnceCell-cached across runs) so
    // `initialize()` can rebuild a fresh V3 schema.
    let _ = client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS
                refinery_schema_history,
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
        .await;

    store.initialize().await.expect("initialize failed");

    (store, pool)
}

#[tokio::test]
#[serial]
async fn test_namespace_crud() {
    let store = setup().await;

    let ns = store
        .create_namespace("test_ns", None, HashMap::new())
        .await
        .unwrap();
    assert_eq!(ns.name, "test_ns");

    let list = store.list_namespaces(0, 100).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "test_ns");

    let got = store.get_namespace("test_ns").await.unwrap();
    assert_eq!(got.id, ns.id);

    assert!(store.namespace_exists("test_ns").await.unwrap());
    assert!(!store.namespace_exists("missing").await.unwrap());

    let err = store
        .create_namespace("test_ns", None, HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store.drop_namespace("test_ns").await.unwrap();
    assert!(!store.namespace_exists("test_ns").await.unwrap());

    let err = store.drop_namespace("test_ns").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn test_asset_crud() {
    let store = setup().await;

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();

    let asset = store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "asset_a",
            "lance://ns1/asset_a",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(asset.name, "asset_a");

    let list = store.list_assets("ns1", AssetFormat::Lance).await.unwrap();
    assert_eq!(list.len(), 1);

    let got = store
        .get_asset("ns1", AssetFormat::Lance, "asset_a")
        .await
        .unwrap();
    assert_eq!(got.id, asset.id);

    assert!(store
        .asset_exists("ns1", AssetFormat::Lance, "asset_a")
        .await
        .unwrap());
    assert!(!store
        .asset_exists("ns1", AssetFormat::Lance, "missing")
        .await
        .unwrap());

    let err = store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "asset_a",
            "lance://ns1/asset_a",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store
        .rename_asset("ns1", AssetFormat::Lance, "asset_a", "asset_b")
        .await
        .unwrap();
    assert!(store
        .asset_exists("ns1", AssetFormat::Lance, "asset_b")
        .await
        .unwrap());
    assert!(!store
        .asset_exists("ns1", AssetFormat::Lance, "asset_a")
        .await
        .unwrap());

    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "asset_c",
            "lance://ns1/asset_c",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let err = store
        .rename_asset("ns1", AssetFormat::Lance, "asset_b", "asset_c")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store
        .drop_asset("ns1", AssetFormat::Lance, "asset_b")
        .await
        .unwrap();
    store
        .drop_asset("ns1", AssetFormat::Lance, "asset_c")
        .await
        .unwrap();

    let list = store.list_assets("ns1", AssetFormat::Lance).await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
#[serial]
async fn test_version_commit_and_load() {
    let store = setup().await;

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            "lance://ns1/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let v1 = store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            1,
            "s3://bucket/v1".to_string(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(v1.version.version_key, "1");
    assert_eq!(v1.tabular_version.metadata_location, "s3://bucket/v1");

    let v2 = store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            2,
            "s3://bucket/v2".to_string(),
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(v2.version.version_key, "2");
    assert!(v2.version.previous_version_id.is_some());

    let current = store
        .load_current_version("ns1", AssetFormat::Lance, "tbl")
        .await
        .unwrap();
    assert!(current.is_some());
    assert_eq!(current.unwrap().version.version_key, "2");

    let loaded = store
        .load_version("ns1", AssetFormat::Lance, "tbl", 1)
        .await
        .unwrap();
    assert_eq!(loaded.version.version_key, "1");
    assert_eq!(loaded.tabular_version.metadata_location, "s3://bucket/v1");

    let versions = store
        .list_versions("ns1", AssetFormat::Lance, "tbl")
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version.version_key, "1");
    assert_eq!(versions[1].version.version_key, "2");
}

#[tokio::test]
#[serial]
async fn test_version_conflict() {
    let store = setup().await;

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            "lance://ns1/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            1,
            "s3://bucket/v1".to_string(),
            None,
        )
        .await
        .unwrap();

    let err = store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            2,
            "s3://bucket/v2".to_string(),
            Some(999),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }));
}

#[tokio::test]
#[serial]
async fn test_previous_version_must_belong_to_same_asset() {
    let store = setup().await;

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "tbl1",
            "lance://ns1/tbl1",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "tbl2",
            "lance://ns1/tbl2",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl1",
            1,
            "s3://bucket/tbl1/v1".to_string(),
            None,
        )
        .await
        .unwrap();

    let err = store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl2",
            2,
            "s3://bucket/tbl2/v2".to_string(),
            Some(1),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }));
}

#[tokio::test]
#[serial]
async fn test_drop_asset_cascades_tabular_and_versions() {
    let (store, pool) = setup_with_pool().await;

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            "lance://ns1/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            1,
            "s3://bucket/v1".to_string(),
            None,
        )
        .await
        .unwrap();
    store
        .create_version(
            "ns1",
            AssetFormat::Lance,
            "tbl",
            2,
            "s3://bucket/v2".to_string(),
            Some(1),
        )
        .await
        .unwrap();

    store
        .drop_asset("ns1", AssetFormat::Lance, "tbl")
        .await
        .unwrap();

    let client = pool.get().await.expect("failed to get client");
    for table in [
        "assets",
        "tabular_assets",
        "asset_versions",
        "tabular_asset_versions",
    ] {
        let row = client
            .query_one(
                &format!("SELECT COUNT(*)::BIGINT AS count FROM {}", table),
                &[],
            )
            .await
            .unwrap();
        let count: i64 = row.get("count");
        assert_eq!(count, 0, "{} should be empty after cascade", table);
    }
}

#[tokio::test]
#[serial]
async fn test_not_found_errors() {
    let store = setup().await;

    let err = store.get_namespace("missing").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    let err = store
        .get_asset("missing", AssetFormat::Lance, "tbl")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    store
        .create_namespace("ns1", None, HashMap::new())
        .await
        .unwrap();
    let err = store
        .get_asset("ns1", AssetFormat::Lance, "tbl")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    let current = store
        .load_current_version("ns1", AssetFormat::Lance, "tbl")
        .await
        .unwrap();
    assert!(current.is_none());
}

#[tokio::test]
#[serial]
async fn test_update_namespace_properties() {
    let store = setup().await;

    let mut props = HashMap::new();
    props.insert("owner".to_string(), "team-a".to_string());
    props.insert("env".to_string(), "prod".to_string());

    store.create_namespace("ns1", None, props).await.unwrap();

    // Add new property and update existing
    let mut updates = HashMap::new();
    updates.insert("owner".to_string(), "team-b".to_string());
    updates.insert("region".to_string(), "us-west".to_string());

    let updated = store
        .update_namespace("ns1", PatchField::Missing, &["env".to_string()], &updates)
        .await
        .unwrap();

    assert_eq!(updated.properties.get("owner"), Some(&"team-b".to_string()));
    assert_eq!(
        updated.properties.get("region"),
        Some(&"us-west".to_string())
    );
    assert!(!updated.properties.contains_key("env"));

    // Not found
    let err = store
        .update_namespace("missing", PatchField::Missing, &[], &HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}
