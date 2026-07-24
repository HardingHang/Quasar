//! Integration tests for the six Iceberg extension traits (DESIGN §4.1):
//! staged tables, register, multi-table transactions, views, metrics, purge.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use quasar_storage::{
    AssetStore, CatalogError, IcebergMetricsStore, IcebergPurgeStore, IcebergRegisterStore,
    IcebergStagingStore, IcebergTableCommit, IcebergTransactionStore, IcebergViewStore,
    TabularStore, VersionStore,
};
use serial_test::serial;
use uuid::Uuid;

const DEFAULT: &str = "default";
const NS: &str = "ice";

fn metadata_json() -> serde_json::Value {
    serde_json::json!({"format-version": 2, "table-uuid": Uuid::new_v4().to_string()})
}

fn meta_loc(table: &str, seq: &str) -> String {
    format!(
        "s3://wh/{NS}/{table}/metadata/{seq}-{}.metadata.json",
        Uuid::new_v4().simple()
    )
}

// ── IcebergStagingStore ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn staged_table_create_get_delete() {
    let (store, _pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;

    let m1 = meta_loc("t1", "00001");
    store
        .create_staged_table(
            DEFAULT,
            NS,
            "t1",
            Uuid::new_v4(),
            "s3://wh/ice/t1",
            &m1,
            metadata_json(),
            serde_json::json!({"k": "v"}),
        )
        .await
        .unwrap();

    let staged = store.get_staged_table(DEFAULT, NS, "t1").await.unwrap();
    assert!(staged.is_some(), "staged record must be readable");

    // Duplicate non-expired staged record -> AlreadyExists.
    let dup = store
        .create_staged_table(
            DEFAULT,
            NS,
            "t1",
            Uuid::new_v4(),
            "s3://wh/ice/t1b",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Missing record reads as None.
    let none = store.get_staged_table(DEFAULT, NS, "ghost").await.unwrap();
    assert!(none.is_none());

    // Explicit delete (cancel) removes it.
    store.delete_staged_table(DEFAULT, NS, "t1").await.unwrap();
    let gone = store.get_staged_table(DEFAULT, NS, "t1").await.unwrap();
    assert!(gone.is_none());
}

#[tokio::test]
#[serial]
async fn staged_commit_creates_table_with_first_version() {
    let (store, _pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;

    let m1 = meta_loc("t2", "00001");
    store
        .create_staged_table(
            DEFAULT,
            NS,
            "t2",
            Uuid::new_v4(),
            "s3://wh/ice/t2",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    let created = store
        .commit_staged_table(
            DEFAULT,
            NS,
            "t2",
            "s3://wh/ice/t2",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    assert_eq!(created.asset.name, "t2");
    assert_eq!(created.asset.asset_type, "table");
    assert_eq!(created.asset.format.as_deref(), Some("iceberg"));
    // The first mirrored version key is derived from the metadata file name.
    assert_eq!(created.asset.current_version_key.as_deref(), Some("00001"));
    assert_eq!(
        created.tabular.metadata_location.as_deref(),
        Some(m1.as_str())
    );

    let version = store.get_version(created.asset.id, "00001").await.unwrap();
    assert_eq!(version.content_pointer.as_deref(), Some(m1.as_str()));

    // The staged record was consumed by the commit.
    let staged = store.get_staged_table(DEFAULT, NS, "t2").await.unwrap();
    assert!(staged.is_none());

    // Committing again now hits the active-table check first -> AlreadyExists.
    let again = store
        .commit_staged_table(
            DEFAULT,
            NS,
            "t2",
            "s3://wh/ice/t2",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(again, Err(CatalogError::AlreadyExists(_))));

    // Committing a name that was never staged -> NotFound.
    let never_staged = store
        .commit_staged_table(
            DEFAULT,
            NS,
            "ghost",
            "s3://wh/ice/ghost",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(never_staged, Err(CatalogError::NotFound(_))));

    // Staging over an active table -> AlreadyExists.
    let over_active = store
        .create_staged_table(
            DEFAULT,
            NS,
            "t2",
            Uuid::new_v4(),
            "s3://wh/ice/t2x",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(over_active, Err(CatalogError::AlreadyExists(_))));

    // Committing when an active table already exists -> AlreadyExists.
    // (Stage first, register the name active, then try to commit.)
    let m2 = meta_loc("t3", "00001");
    store
        .create_staged_table(
            DEFAULT,
            NS,
            "t3",
            Uuid::new_v4(),
            "s3://wh/ice/t3",
            &m2,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "t3",
            "s3://wh/ice/t3",
            &m2,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let conflict = store
        .commit_staged_table(
            DEFAULT,
            NS,
            "t3",
            "s3://wh/ice/t3",
            &m2,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(conflict, Err(CatalogError::AlreadyExists(_))));
}

#[tokio::test]
#[serial]
async fn staged_expiry_reclaims_name_and_blocks_commit() {
    let (store, pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;

    let m1 = meta_loc("t4", "00001");
    store
        .create_staged_table(
            DEFAULT,
            NS,
            "t4",
            Uuid::new_v4(),
            "s3://wh/ice/t4",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    // Force the record to expire.
    let client = pool.get().await.unwrap();
    client
        .execute(
            "UPDATE iceberg_staged_tables SET expires_at = now() - interval '1 hour' \
             WHERE table_name = 't4'",
            &[],
        )
        .await
        .unwrap();

    // Expired records are invisible to reads.
    let expired = store.get_staged_table(DEFAULT, NS, "t4").await.unwrap();
    assert!(expired.is_none());

    // ... and cannot be committed.
    let commit_expired = store
        .commit_staged_table(
            DEFAULT,
            NS,
            "t4",
            "s3://wh/ice/t4",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(commit_expired, Err(CatalogError::NotFound(_))));

    // A fresh stage-create reclaims the expired record's name.
    store
        .create_staged_table(
            DEFAULT,
            NS,
            "t4",
            Uuid::new_v4(),
            "s3://wh/ice/t4",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert!(store
        .get_staged_table(DEFAULT, NS, "t4")
        .await
        .unwrap()
        .is_some());
}

// ── IcebergRegisterStore ───────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn register_iceberg_table_mirrors_first_version() {
    let (store, _pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;

    let m1 = meta_loc("reg1", "00001");
    let registered = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "reg1",
            "s3://wh/ice/reg1",
            &m1,
            metadata_json(),
            serde_json::json!({"owner": "data-platform"}),
        )
        .await
        .unwrap();

    assert_eq!(registered.asset.asset_type, "table");
    assert_eq!(registered.asset.format.as_deref(), Some("iceberg"));
    assert_eq!(
        registered.asset.current_version_key.as_deref(),
        Some("00001")
    );
    assert_eq!(registered.tabular.location, "s3://wh/ice/reg1");
    assert_eq!(
        registered.tabular.metadata_location.as_deref(),
        Some(m1.as_str())
    );

    // First mirrored version exists and is the root.
    let v1 = store
        .get_version(registered.asset.id, "00001")
        .await
        .unwrap();
    assert!(v1.previous_version_id.is_none());
    assert_eq!(v1.content_pointer.as_deref(), Some(m1.as_str()));

    // Properties passed through onto the asset row.
    assert_eq!(
        registered.asset.properties.unwrap()["owner"],
        "data-platform"
    );

    // Duplicate registration -> AlreadyExists.
    let dup = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "reg1",
            "s3://wh/ice/reg1",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Missing namespace -> NotFound.
    let no_ns = store
        .register_iceberg_table(
            DEFAULT,
            "ghost",
            "reg2",
            "s3://wh/ice/reg2",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(no_ns, Err(CatalogError::NotFound(_))));
}

// ── IcebergTransactionStore ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn transaction_commit_is_atomic() {
    let (store, _pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;

    let t1_m1 = meta_loc("tx1", "00001");
    let t2_m1 = meta_loc("tx2", "00001");
    let t1 = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "tx1",
            "s3://wh/ice/tx1",
            &t1_m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let t2 = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "tx2",
            "s3://wh/ice/tx2",
            &t2_m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    // Happy path: both CAS branches commit in one DB transaction.
    let t1_m2 = meta_loc("tx1", "00002");
    let t2_m2 = meta_loc("tx2", "00002");
    let schema = serde_json::json!({"type": "struct", "fields": []});
    store
        .commit_transaction_tables(vec![
            IcebergTableCommit {
                domain: DEFAULT.to_string(),
                namespace_path: NS.to_string(),
                table: "tx1".to_string(),
                expected_pointer: t1_m1.clone(),
                new_location: t1_m2.clone(),
                new_version_key: "00002".to_string(),
                schema_snapshot: Some(schema.clone()),
            },
            IcebergTableCommit {
                domain: DEFAULT.to_string(),
                namespace_path: NS.to_string(),
                table: "tx2".to_string(),
                expected_pointer: t2_m1.clone(),
                new_location: t2_m2.clone(),
                new_version_key: "00002".to_string(),
                schema_snapshot: None,
            },
        ])
        .await
        .unwrap();

    // Mirrored versions written, pointers + schema cache updated.
    let t1_tab = store.get_tabular_asset(t1.asset.id).await.unwrap();
    assert_eq!(t1_tab.metadata_location.as_deref(), Some(t1_m2.as_str()));
    assert_eq!(t1_tab.schema_snapshot, Some(schema));
    let t2_tab = store.get_tabular_asset(t2.asset.id).await.unwrap();
    assert_eq!(t2_tab.metadata_location.as_deref(), Some(t2_m2.as_str()));

    let t1_v2 = store.get_version(t1.asset.id, "00002").await.unwrap();
    let t1_v1 = store.get_version(t1.asset.id, "00001").await.unwrap();
    assert_eq!(t1_v2.previous_version_id, Some(t1_v1.id));
    assert_eq!(
        store
            .get_asset(t1.asset.id)
            .await
            .unwrap()
            .current_version_key
            .as_deref(),
        Some("00002")
    );

    // Conflict on one table rolls the whole transaction back: tx1 keeps
    // its pointer, gains no mirrored version, keeps its current key.
    let t1_m3 = meta_loc("tx1", "00003");
    let rollback = store
        .commit_transaction_tables(vec![
            IcebergTableCommit {
                domain: DEFAULT.to_string(),
                namespace_path: NS.to_string(),
                table: "tx1".to_string(),
                expected_pointer: t1_m2.clone(),
                new_location: t1_m3.clone(),
                new_version_key: "00003".to_string(),
                schema_snapshot: None,
            },
            IcebergTableCommit {
                domain: DEFAULT.to_string(),
                namespace_path: NS.to_string(),
                table: "tx2".to_string(),
                expected_pointer: "s3://wh/ice/tx2/metadata/stale.metadata.json".to_string(),
                new_location: meta_loc("tx2", "00003"),
                new_version_key: "00003".to_string(),
                schema_snapshot: None,
            },
        ])
        .await;
    assert!(matches!(rollback, Err(CatalogError::Conflict(_))));

    let t1_tab_after = store.get_tabular_asset(t1.asset.id).await.unwrap();
    assert_eq!(
        t1_tab_after.metadata_location.as_deref(),
        Some(t1_m2.as_str())
    );
    let no_v3 = store.get_version(t1.asset.id, "00003").await;
    assert!(matches!(no_v3, Err(CatalogError::NotFound(_))));
    assert_eq!(
        store
            .get_asset(t1.asset.id)
            .await
            .unwrap()
            .current_version_key
            .as_deref(),
        Some("00002")
    );

    // Empty commit list is a no-op; unknown table -> NotFound.
    store.commit_transaction_tables(vec![]).await.unwrap();
    let unknown = store
        .commit_transaction_tables(vec![IcebergTableCommit {
            domain: DEFAULT.to_string(),
            namespace_path: NS.to_string(),
            table: "ghost".to_string(),
            expected_pointer: "x".to_string(),
            new_location: "y".to_string(),
            new_version_key: "00001".to_string(),
            schema_snapshot: None,
        }])
        .await;
    assert!(matches!(unknown, Err(CatalogError::NotFound(_))));
}

// ── IcebergViewStore ───────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn view_lifecycle() {
    let (store, _pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;
    common::make_namespace(&store, DEFAULT, "ice2").await;

    let v1_m1 = meta_loc("v1", "00001");
    let view_uuid = Uuid::new_v4();
    let view = store
        .create_view(
            DEFAULT,
            NS,
            "v1",
            view_uuid,
            "s3://wh/ice/v1",
            &v1_m1,
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert_eq!(view.asset.asset_type, "view");
    assert_eq!(view.asset.format.as_deref(), Some("iceberg"));
    assert_eq!(view.asset.current_version_key.as_deref(), Some("00001"));
    assert_eq!(view.view.view_uuid, Some(view_uuid));
    assert_eq!(view.view.metadata_location.as_deref(), Some(v1_m1.as_str()));

    let loaded = store.get_view(DEFAULT, NS, "v1").await.unwrap();
    assert_eq!(loaded.asset.id, view.asset.id);
    assert!(store.view_exists(DEFAULT, NS, "v1").await.unwrap());
    assert!(!store.view_exists(DEFAULT, NS, "ghost").await.unwrap());

    // Duplicate view name -> AlreadyExists.
    let dup = store
        .create_view(
            DEFAULT,
            NS,
            "v1",
            Uuid::new_v4(),
            "s3://wh/ice/v1x",
            &v1_m1,
            serde_json::json!({}),
        )
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // CAS commit mirrors the new version and moves the pointer.
    let v1_m2 = meta_loc("v1", "00002");
    store
        .commit_view(DEFAULT, NS, "v1", &v1_m1, &v1_m2, "00002")
        .await
        .unwrap();
    let after = store.get_view(DEFAULT, NS, "v1").await.unwrap();
    assert_eq!(
        after.view.metadata_location.as_deref(),
        Some(v1_m2.as_str())
    );
    assert_eq!(after.asset.current_version_key.as_deref(), Some("00002"));
    let v2 = store.get_version(view.asset.id, "00002").await.unwrap();
    let v1 = store.get_version(view.asset.id, "00001").await.unwrap();
    assert_eq!(v2.previous_version_id, Some(v1.id));

    // Stale pointer -> Conflict.
    let stale = store
        .commit_view(DEFAULT, NS, "v1", &v1_m1, &meta_loc("v1", "00003"), "00003")
        .await;
    assert!(matches!(stale, Err(CatalogError::Conflict(_))));

    // Rename in place, then move across namespaces.
    store
        .rename_view(DEFAULT, NS, "v1", DEFAULT, NS, "v1_renamed")
        .await
        .unwrap();
    let old = store.get_view(DEFAULT, NS, "v1").await;
    assert!(matches!(old, Err(CatalogError::NotFound(_))));
    store.get_view(DEFAULT, NS, "v1_renamed").await.unwrap();

    store
        .rename_view(DEFAULT, NS, "v1_renamed", DEFAULT, "ice2", "v1_moved")
        .await
        .unwrap();
    let moved = store.list_views(DEFAULT, "ice2", 0, 100).await.unwrap();
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].name, "v1_moved");
    assert_eq!(moved[0].namespace_path, "ice2");
    assert!(store
        .list_views(DEFAULT, NS, 0, 100)
        .await
        .unwrap()
        .is_empty());

    // Drop removes the view and its mirrored versions.
    store.drop_view(DEFAULT, "ice2", "v1_moved").await.unwrap();
    assert!(!store
        .view_exists(DEFAULT, "ice2", "v1_moved")
        .await
        .unwrap());
    assert!(store
        .list_versions(view.asset.id, 0, 100)
        .await
        .unwrap()
        .is_empty());
    let dropped = store.get_view(DEFAULT, "ice2", "v1_moved").await;
    assert!(matches!(dropped, Err(CatalogError::NotFound(_))));
}

// ── IcebergMetricsStore ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn scan_metrics_reports_are_recorded() {
    let (store, pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;
    let m1 = meta_loc("met", "00001");
    let t = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "met",
            "s3://wh/ice/met",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();

    // Report bound to the asset, plus an unbound report (asset_id NULL).
    store
        .record_scan_metrics_report(
            Some(t.asset.id),
            DEFAULT,
            NS,
            "met",
            serde_json::json!({"report-type": "scan", "metrics": {"total-planning-duration": 42}}),
            Some("spark/3.5"),
        )
        .await
        .unwrap();
    store
        .record_scan_metrics_report(
            None,
            DEFAULT,
            NS,
            "met",
            serde_json::json!({"report-type": "scan"}),
            None,
        )
        .await
        .unwrap();

    let client = pool.get().await.unwrap();
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM iceberg_scan_metrics_reports", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 2);
    let bound: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM iceberg_scan_metrics_reports WHERE asset_id = $1 AND user_agent = 'spark/3.5'",
            &[&t.asset.id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(bound, 1);
}

// ── IcebergPurgeStore ──────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn purge_drops_catalog_and_tracks_operation() {
    let (store, pool) = common::setup().await;
    common::make_namespace(&store, DEFAULT, NS).await;
    let m1 = meta_loc("pg1", "00001");
    let t = store
        .register_iceberg_table(
            DEFAULT,
            NS,
            "pg1",
            "s3://wh/ice/pg1",
            &m1,
            metadata_json(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    let asset_id = t.asset.id;

    let (op_id, location, metadata_location) = store
        .begin_iceberg_purge_and_drop_catalog(DEFAULT, NS, "pg1")
        .await
        .unwrap();
    assert_eq!(location, "s3://wh/ice/pg1");
    assert_eq!(metadata_location.as_deref(), Some(m1.as_str()));

    // Catalog rows are gone: asset, tabular extension, mirrored versions.
    let gone = store.get_asset_by_name(DEFAULT, NS, "pg1").await;
    assert!(matches!(gone, Err(CatalogError::NotFound(_))));
    let no_tabular = store.get_tabular_asset(asset_id).await;
    assert!(matches!(no_tabular, Err(CatalogError::NotFound(_))));
    assert!(store
        .list_versions(asset_id, 0, 100)
        .await
        .unwrap()
        .is_empty());

    // The purge operation was recorded in `catalog_dropped` state and can
    // be driven through its status transitions.
    let client = pool.get().await.unwrap();
    let status: String = client
        .query_one(
            "SELECT status FROM iceberg_purge_operations WHERE id = $1",
            &[&op_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "catalog_dropped");

    store
        .update_purge_operation(op_id, "completed", None)
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT status, completed_at FROM iceberg_purge_operations WHERE id = $1",
            &[&op_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "completed");
    assert!(row
        .get::<_, Option<chrono::DateTime<chrono::Utc>>>("completed_at")
        .is_some());

    store
        .update_purge_operation(op_id, "failed", Some("object store timeout"))
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT status, error_message FROM iceberg_purge_operations WHERE id = $1",
            &[&op_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "failed");
    assert_eq!(
        row.get::<_, Option<String>>("error_message").as_deref(),
        Some("object store timeout")
    );

    // Unknown operation -> NotFound; purging a missing table -> NotFound.
    let missing = store
        .update_purge_operation(Uuid::new_v4(), "completed", None)
        .await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));
    let no_table = store
        .begin_iceberg_purge_and_drop_catalog(DEFAULT, NS, "pg1")
        .await;
    assert!(matches!(no_table, Err(CatalogError::NotFound(_))));
}
