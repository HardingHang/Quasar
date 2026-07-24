//! Iceberg REST Catalog — object store interaction integration tests:
//! metadata.json write/read, and purge semantics on drop.

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use object_store::path::Path;
use serde_json::json;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_objstore").await })
        .await
        .1
}

async fn object_exists(mem: &Arc<dyn object_store::ObjectStore>, location: &str) -> bool {
    let path = Path::from(location.trim_start_matches("s3://bucket/"));
    mem.head(&path).await.is_ok()
}

use std::sync::Arc;

#[tokio::test]
#[serial]
async fn create_and_commit_write_metadata_to_store() {
    let store = fresh_store(db_url().await).await;
    let (app, mem) = test_app(&store);
    create_namespace(&app, &["os1"]).await;

    let created = create_table(&app, "os1", "events").await;
    let loc1 = created["metadata-location"].as_str().unwrap().to_string();
    assert!(
        object_exists(&mem, &loc1).await,
        "create should write initial metadata.json"
    );

    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/os1/tables/events"),
        &json!({"requirements": [], "updates": [{"action": "set-properties", "updates": {"a": "b"}}]})
            .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let loc2 = body_json(resp).await["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        object_exists(&mem, &loc2).await,
        "commit should write new metadata"
    );
    assert!(
        object_exists(&mem, &loc1).await,
        "old metadata stays (immutable)"
    );
}

#[tokio::test]
#[serial]
async fn drop_with_and_without_purge() {
    let store = fresh_store(db_url().await).await;
    let (app, mem) = test_app(&store);
    create_namespace(&app, &["os2"]).await;

    // purgeRequested=false (default): catalog row gone, objects kept.
    let created = create_table(&app, "os2", "keep").await;
    let loc = created["metadata-location"].as_str().unwrap().to_string();
    let resp = delete(&app, &format!("{NS_PREFIX}/namespaces/os2/tables/keep")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(object_exists(&mem, &loc).await, "no purge: objects kept");

    // purgeRequested=true: objects removed.
    let created = create_table(&app, "os2", "purge_me").await;
    let loc = created["metadata-location"].as_str().unwrap().to_string();
    let resp = delete(
        &app,
        &format!("{NS_PREFIX}/namespaces/os2/tables/purge_me?purgeRequested=true"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(
        !object_exists(&mem, &loc).await,
        "purge should delete table objects"
    );
}

#[tokio::test]
#[serial]
async fn purge_rejects_location_outside_warehouse() {
    let store = fresh_store(db_url().await).await;
    let (app, mem) = test_app(&store);
    create_namespace(&app, &["os3"]).await;

    // Register a table whose location escapes the configured warehouse.
    let meta = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": "s3://evil/escape/tbl",
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 1,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]}],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 0,
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {}
    });
    mem.put(
        &Path::from("ext/escape.metadata.json"),
        serde_json::to_vec(&meta).unwrap().into(),
    )
    .await
    .unwrap();
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/os3/register"),
        &json!({"name": "escape", "metadata-location": "s3://bucket/ext/escape.metadata.json"})
            .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Purge must refuse to delete outside the warehouse boundary.
    let resp = delete(
        &app,
        &format!("{NS_PREFIX}/namespaces/os3/tables/escape?purgeRequested=true"),
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::CONFLICT,
        "purge outside warehouse should be rejected, got {status}"
    );
}
