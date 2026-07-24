//! Iceberg REST Catalog — commit (CAS) integration tests.
//!
//! Asserts both the protocol surface (CommitFailedException, requirement
//! handling) and the catalog-internal mirroring: every commit writes a
//! mirrored version into `asset_versions` and advances
//! `assets.current_version_key` (DESIGN §6.1 / §6.4).

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
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_commit").await })
        .await
        .1
}

async fn commit_table(
    app: &axum::Router,
    ns: &str,
    table: &str,
    body: serde_json::Value,
) -> axum::response::Response {
    post(
        app,
        &format!("{NS_PREFIX}/namespaces/{ns}/tables/{table}"),
        &body.to_string(),
    )
    .await
}

#[tokio::test]
#[serial]
async fn commit_success_mirrors_version() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["c1"]).await;
    let created = create_table(&app, "c1", "events").await;
    let base_location = created["metadata-location"].as_str().unwrap().to_string();

    // Commit a property change based on the loaded pointer.
    let resp = commit_table(
        &app,
        "c1",
        "events",
        json!({
            "requirements": [],
            "updates": [{"action": "set-properties", "updates": {"gc.enabled": "false"}}]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let committed = body_json(resp).await;
    let new_location = committed["metadata-location"].as_str().unwrap().to_string();
    assert_ne!(
        new_location, base_location,
        "commit must advance the pointer"
    );

    // Mirrored version chain: 00001 (create) -> 00002 (commit), linked.
    let asset = store
        .get_asset_by_name("default", "c1", "events")
        .await
        .expect("asset lookup failed");
    assert_eq!(asset.current_version_key.as_deref(), Some("00002"));
    let latest = store
        .get_latest_version(asset.id)
        .await
        .expect("latest version failed");
    assert_eq!(latest.version_key, "00002");
    assert_eq!(
        latest.content_pointer.as_deref(),
        Some(new_location.as_str())
    );
    let first = store
        .get_version(asset.id, "00001")
        .await
        .expect("first version failed");
    assert_eq!(latest.previous_version_id, Some(first.id));
    assert_eq!(
        first.content_pointer.as_deref(),
        Some(base_location.as_str())
    );
}

#[tokio::test]
#[serial]
async fn commit_requirement_failure_and_stale_cas() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["c2"]).await;
    create_table(&app, "c2", "events").await;

    // assert-ref-snapshot-id against a snapshot that does not exist.
    let resp = commit_table(
        &app,
        "c2",
        "events",
        json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 999}],
            "updates": [{"action": "set-properties", "updates": {"a": "b"}}]
        }),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "CommitFailedException").await;

    // A successful commit advances the pointer; a second commit whose
    // requirement pins the pre-commit state fails the requirement check.
    let resp = commit_table(
        &app,
        "c2",
        "events",
        json!({"requirements": [], "updates": [{"action": "set-properties", "updates": {"x": "1"}}]}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = commit_table(
        &app,
        "c2",
        "events",
        json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
            "updates": [{"action": "set-properties", "updates": {"x": "2"}}]
        }),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "CommitFailedException").await;
}

#[tokio::test]
#[serial]
async fn staged_create_and_commit() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["c3"]).await;

    // stage-create: catalog entry is not yet visible.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/c3/tables"),
        &json!({
            "name": "staged",
            "location": "s3://bucket/c3/staged",
            "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]},
            "properties": {},
            "stage-create": true
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/c3/tables/staged")).await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchTableException").await;

    // Commit with assert-create materializes the table and its first version.
    let resp = commit_table(
        &app,
        "c3",
        "staged",
        json!({
            "requirements": [{"type": "assert-create"}],
            "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
        }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/c3/tables/staged")).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // A second assert-create commit fails: the table already exists.
    let resp = commit_table(
        &app,
        "c3",
        "staged",
        json!({
            "requirements": [{"type": "assert-create"}],
            "updates": [{"action": "set-properties", "updates": {"k": "v2"}}]
        }),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "CommitFailedException").await;
}

#[tokio::test]
#[serial]
async fn commit_rejects_encryption_key_updates() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["c4"]).await;
    create_table(&app, "c4", "events").await;

    let resp = commit_table(
        &app,
        "c4",
        "events",
        json!({
            "requirements": [],
            "updates": [{"action": "remove-encryption-key", "key-id": "k1"}]
        }),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::NOT_IMPLEMENTED, "NotImplementedException").await;
}
