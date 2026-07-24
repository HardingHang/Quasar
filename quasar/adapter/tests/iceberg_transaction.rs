//! Iceberg REST Catalog — multi-table transaction commit integration tests.

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
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_tx").await })
        .await
        .1
}

fn table_change(
    ns: &str,
    name: &str,
    requirements: serde_json::Value,
    key: &str,
) -> serde_json::Value {
    json!({
        "identifier": {"namespace": [ns], "name": name},
        "requirements": requirements,
        "updates": [{"action": "set-properties", "updates": {"tx": key}}]
    })
}

#[tokio::test]
#[serial]
async fn transaction_commit_success_mirrors_all_tables() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["tx"]).await;
    create_table(&app, "tx", "t1").await;
    create_table(&app, "tx", "t2").await;

    let resp = post(
        &app,
        &format!("{NS_PREFIX}/transactions/commit"),
        &json!({"table-changes": [
            table_change("tx", "t1", json!([]), "a"),
            table_change("tx", "t2", json!([]), "b")
        ]})
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Both tables advanced with mirrored versions.
    for name in ["t1", "t2"] {
        let asset = store
            .get_asset_by_name("default", "tx", name)
            .await
            .expect("asset lookup failed");
        assert_eq!(
            asset.current_version_key.as_deref(),
            Some("00002"),
            "{name}"
        );
        let tip = store
            .get_latest_version(asset.id)
            .await
            .expect("latest version failed");
        assert!(
            tip.previous_version_id.is_some(),
            "{name} version chain linked"
        );
    }
}

#[tokio::test]
#[serial]
async fn transaction_conflict_rolls_back_all() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["tx2"]).await;
    create_table(&app, "tx2", "good").await;
    create_table(&app, "tx2", "bad").await;

    // The second change carries an impossible requirement; the whole
    // transaction must roll back atomically.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/transactions/commit"),
        &json!({"table-changes": [
            table_change("tx2", "good", json!([]), "a"),
            table_change("tx2", "bad", json!([{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 999}]), "b")
        ]})
        .to_string(),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "CommitFailedException").await;

    // The good table is untouched: no new pointer, no new version.
    let asset = store
        .get_asset_by_name("default", "tx2", "good")
        .await
        .expect("asset lookup failed");
    assert_eq!(asset.current_version_key.as_deref(), Some("00001"));
    let versions = store
        .list_versions(asset.id, 0, 100)
        .await
        .expect("list versions failed");
    assert_eq!(
        versions.len(),
        1,
        "rollback must not leave mirrored versions"
    );
}
