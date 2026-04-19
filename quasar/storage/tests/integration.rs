#![allow(clippy::unwrap_used, clippy::expect_used)]

use deadpool_postgres::{Pool, Runtime};
use quasar_storage::{AssetCommitUpdate, AssetFormat, CatalogStore, PgCatalogStore, StoreError};
use serial_test::serial;
use std::collections::HashMap;

fn test_pool() -> Pool {
    let database_url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set for integration tests. \
         Example: DATABASE_URL=postgres://postgres:postgres@localhost:5432/quasar_test",
    );

    let config = database_url
        .parse::<tokio_postgres::Config>()
        .expect("invalid DATABASE_URL");

    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool")
}

async fn setup() -> PgCatalogStore {
    let pool = test_pool();
    let store = PgCatalogStore::new(pool);

    store.migrate().await.expect("migration failed");

    // Use a separate connection to truncate tables before testing.
    let clean_pool = test_pool();
    let client = clean_pool
        .get()
        .await
        .expect("failed to get client");

    client
        .execute(
            "TRUNCATE asset_versions, assets, namespaces CASCADE",
            &[],
        )
        .await
        .expect("failed to truncate tables");

    store
}

#[tokio::test]
#[serial]
async fn test_namespace_crud() {
    let store = setup().await;

    // create
    let ns = store
        .create_namespace("test_ns", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    assert_eq!(ns.name, "test_ns");
    assert!(matches!(ns.format, AssetFormat::Lance));

    // list
    let list = store.list_namespaces().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "test_ns");

    // get
    let got = store.get_namespace("test_ns").await.unwrap();
    assert_eq!(got.id, ns.id);

    // exists
    assert!(store.namespace_exists("test_ns").await.unwrap());
    assert!(!store.namespace_exists("missing").await.unwrap());

    // duplicate create
    let err = store
        .create_namespace("test_ns", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    // drop
    store.drop_namespace("test_ns").await.unwrap();
    assert!(!store.namespace_exists("test_ns").await.unwrap());

    // drop missing
    let err = store.drop_namespace("test_ns").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn test_asset_crud() {
    let store = setup().await;

    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();

    // create
    let asset = store
        .create_asset("ns1", "asset_a", HashMap::new())
        .await
        .unwrap();
    assert_eq!(asset.name, "asset_a");

    // list
    let list = store.list_assets("ns1").await.unwrap();
    assert_eq!(list.len(), 1);

    // get
    let got = store.get_asset("ns1", "asset_a").await.unwrap();
    assert_eq!(got.id, asset.id);

    // exists
    assert!(store.asset_exists("ns1", "asset_a").await.unwrap());
    assert!(!store.asset_exists("ns1", "missing").await.unwrap());

    // duplicate
    let err = store
        .create_asset("ns1", "asset_a", HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    // rename
    store.rename_asset("ns1", "asset_a", "asset_b").await.unwrap();
    assert!(store.asset_exists("ns1", "asset_b").await.unwrap());
    assert!(!store.asset_exists("ns1", "asset_a").await.unwrap());

    // rename conflict
    store
        .create_asset("ns1", "asset_c", HashMap::new())
        .await
        .unwrap();
    let err = store
        .rename_asset("ns1", "asset_b", "asset_c")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    // drop
    store.drop_asset("ns1", "asset_b").await.unwrap();
    store.drop_asset("ns1", "asset_c").await.unwrap();

    let list = store.list_assets("ns1").await.unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
#[serial]
async fn test_version_commit_and_load() {
    let store = setup().await;

    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset("ns1", "tbl", HashMap::new())
        .await
        .unwrap();

    // first commit (no previous)
    let v1 = store
        .commit_version(
            "ns1",
            "tbl",
            AssetCommitUpdate {
                metadata_location: "s3://bucket/v1".to_string(),
                previous_version_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(v1.version_id, 1);
    assert_eq!(v1.metadata_location, "s3://bucket/v1");

    // second commit
    let v2 = store
        .commit_version(
            "ns1",
            "tbl",
            AssetCommitUpdate {
                metadata_location: "s3://bucket/v2".to_string(),
                previous_version_id: Some(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(v2.version_id, 2);
    assert_eq!(v2.previous_version_id, Some(1));

    // load current
    let current = store.load_current_version("ns1", "tbl").await.unwrap();
    assert_eq!(current.version_id, 2);

    // load specific
    let loaded = store.load_version("ns1", "tbl", 1).await.unwrap();
    assert_eq!(loaded.version_id, 1);
    assert_eq!(loaded.metadata_location, "s3://bucket/v1");

    // list
    let versions = store.list_versions("ns1", "tbl").await.unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0].version_id, 1);
    assert_eq!(versions[1].version_id, 2);
}

#[tokio::test]
#[serial]
async fn test_version_conflict() {
    let store = setup().await;

    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    store
        .create_asset("ns1", "tbl", HashMap::new())
        .await
        .unwrap();

    // commit v1
    store
        .commit_version(
            "ns1",
            "tbl",
            AssetCommitUpdate {
                metadata_location: "s3://bucket/v1".to_string(),
                previous_version_id: None,
            },
        )
        .await
        .unwrap();

    // commit with wrong previous_version_id
    let err = store
        .commit_version(
            "ns1",
            "tbl",
            AssetCommitUpdate {
                metadata_location: "s3://bucket/v2".to_string(),
                previous_version_id: Some(999),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));
}

#[tokio::test]
#[serial]
async fn test_not_found_errors() {
    let store = setup().await;

    // namespace
    let err = store.get_namespace("missing").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // asset (namespace doesn't exist)
    let err = store.get_asset("missing", "tbl").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // asset (namespace exists, asset missing)
    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    let err = store.get_asset("ns1", "tbl").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // version (asset missing)
    let err = store.load_current_version("ns1", "tbl").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}
