//! Iceberg REST Catalog — view lifecycle integration tests.

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use quasar_core::{AssetStore, VersionStore};
use serde_json::json;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_view").await })
        .await
        .1
}

fn view_body(name: &str) -> serde_json::Value {
    json!({
        "name": name,
        "location": format!("s3://bucket/v1/{name}"),
        "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]},
        "sql": "SELECT id FROM events",
        "default-namespace": ["v1"],
        "properties": {}
    })
}

#[tokio::test]
#[serial]
async fn view_crud_and_replace() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["v1"]).await;

    // Create.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/v1/views"),
        &view_body("myview").to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let created = body_json(resp).await;
    assert!(created["metadata-location"].as_str().is_some());

    // Load.
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/v1/views/myview")).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // HEAD exists.
    let resp = head(&app, &format!("{NS_PREFIX}/namespaces/v1/views/myview")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // List.
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/v1/views")).await;
    let body = body_json(resp).await;
    assert_eq!(body["identifiers"].as_array().unwrap().len(), 1);

    // Replace (commit) with a property change.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/v1/views/myview"),
        &json!({"requirements": [], "updates": [{"action": "set-properties", "updates": {"k": "v"}}]})
            .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Mirrored version advanced.
    let asset = store
        .get_asset_by_name("default", "v1", "myview")
        .await
        .expect("asset lookup failed");
    assert!(asset.current_version_key.is_some());
    let versions = store
        .list_versions(asset.id, 0, 100)
        .await
        .expect("list versions failed");
    assert!(
        versions.len() >= 2,
        "create + replace should mirror two versions"
    );

    // Drop.
    let resp = delete(&app, &format!("{NS_PREFIX}/namespaces/v1/views/myview")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/v1/views/myview")).await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchViewException").await;
}

#[tokio::test]
#[serial]
async fn view_conflicts_with_same_name_table() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["v2"]).await;
    create_table(&app, "v2", "events").await;

    // A view cannot share the active name of a table in the same namespace.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/v2/views"),
        &view_body("events").to_string(),
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CONFLICT,
        "same-name table should conflict, got {status}"
    );
}

#[tokio::test]
#[serial]
async fn view_rename() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["v3"]).await;
    post(
        &app,
        &format!("{NS_PREFIX}/namespaces/v3/views"),
        &view_body("old_view").to_string(),
    )
    .await;

    let resp = post(
        &app,
        &format!("{NS_PREFIX}/views/rename"),
        &json!({
            "source": {"namespace": ["v3"], "name": "old_view"},
            "destination": {"namespace": ["v3"], "name": "new_view"}
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/v3/views/new_view")).await;
    assert_eq!(resp.status(), StatusCode::OK);
}
