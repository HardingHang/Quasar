//! Iceberg REST Catalog — table CRUD + rename integration tests.

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_table").await })
        .await
        .1
}

#[tokio::test]
#[serial]
async fn table_crud_lifecycle() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["ns1"]).await;

    // Create.
    let created = create_table(&app, "ns1", "events").await;
    assert!(created["metadata-location"].as_str().is_some());

    // Load.
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/ns1/tables/events")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let loaded = body_json(resp).await;
    assert_eq!(
        loaded["metadata-location"], created["metadata-location"],
        "load should return the same metadata pointer"
    );
    assert!(loaded["metadata"]["format-version"].as_i64().is_some());

    // HEAD exists.
    let resp = head(&app, &format!("{NS_PREFIX}/namespaces/ns1/tables/events")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // List contains the table identifier.
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/ns1/tables")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let names: Vec<&str> = body["identifiers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["events"]);

    // Duplicate create conflicts.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/ns1/tables"),
        &json!({"name": "events", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "properties": {}}).to_string(),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "TableAlreadyExistsException").await;

    // Drop, then load 404.
    let resp = delete(&app, &format!("{NS_PREFIX}/namespaces/ns1/tables/events")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/ns1/tables/events")).await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchTableException").await;
}

#[tokio::test]
#[serial]
async fn table_rename_same_and_cross_namespace() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["src"]).await;
    create_namespace(&app, &["dst"]).await;
    create_table(&app, "src", "events").await;

    // Rename within the same namespace.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/tables/rename"),
        &json!({
            "source": {"namespace": ["src"], "name": "events"},
            "destination": {"namespace": ["src"], "name": "events_v2"}
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(
        &app,
        &format!("{NS_PREFIX}/namespaces/src/tables/events_v2"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Move across namespaces within the same domain.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/tables/rename"),
        &json!({
            "source": {"namespace": ["src"], "name": "events_v2"},
            "destination": {"namespace": ["dst"], "name": "events_v2"}
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(
        &app,
        &format!("{NS_PREFIX}/namespaces/dst/tables/events_v2"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = get(
        &app,
        &format!("{NS_PREFIX}/namespaces/src/tables/events_v2"),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchTableException").await;

    // Destination namespace missing -> 404.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/tables/rename"),
        &json!({
            "source": {"namespace": ["dst"], "name": "events_v2"},
            "destination": {"namespace": ["ghost"], "name": "events_v2"}
        })
        .to_string(),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchNamespaceException").await;
}

#[tokio::test]
#[serial]
async fn table_register_external() {
    let store = fresh_store(db_url().await).await;
    let (app, mem) = test_app(&store);
    create_namespace(&app, &["ext"]).await;

    // Seed an external metadata document in the object store.
    let meta = json!({
        "format-version": 2,
        "table-uuid": uuid::Uuid::new_v4().to_string(),
        "location": "s3://bucket/ext/registered",
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
        &object_store::path::Path::from("ext/registered/metadata/00001-abc.metadata.json"),
        serde_json::to_vec(&meta).unwrap().into(),
    )
    .await
    .unwrap();

    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/ext/register"),
        &json!({
            "name": "registered",
            "metadata-location": "s3://bucket/ext/registered/metadata/00001-abc.metadata.json"
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = get(
        &app,
        &format!("{NS_PREFIX}/namespaces/ext/tables/registered"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}
