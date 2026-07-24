//! Cross-format isolation integration tests: assets created through one
//! native protocol are invisible to the other (DESIGN §3.2 assets.format).

#![cfg(all(feature = "iceberg", feature = "lance"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use quasar_adapter::lance;
use quasar_core::CatalogStore;
use serde_json::json;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_format_isolation").await })
        .await
        .1
}

fn lance_app(store: &Arc<quasar_storage::PgCatalogStore>) -> axum::Router {
    let store: Arc<dyn CatalogStore> = store.clone();
    lance::routes()
        .layer(axum::Extension(lance::LanceConfig::default()))
        .with_state(store)
}

#[tokio::test]
#[serial]
async fn iceberg_table_invisible_to_lance() {
    let store = fresh_store(db_url().await).await;
    let (iceberg_app, _mem) = test_app(&store);
    let lance_app = lance_app(&store);
    create_namespace(&iceberg_app, &["iso"]).await;
    create_table(&iceberg_app, "iso", "events").await;

    // Lance describe on the same name: TableNotFound (not the iceberg asset).
    let resp = post(
        &lance_app,
        "/lance/v1/table/default$iso$events/describe",
        "{}",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["code"], "TableNotFound");

    // Lance drop must not touch the iceberg asset either.
    let resp = post(&lance_app, "/lance/v1/table/default$iso$events/drop", "{}").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let resp = get(
        &iceberg_app,
        &format!("{NS_PREFIX}/namespaces/iso/tables/events"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK, "iceberg asset untouched");
}

#[tokio::test]
#[serial]
async fn lance_table_invisible_to_iceberg() {
    let store = fresh_store(db_url().await).await;
    let (iceberg_app, _mem) = test_app(&store);
    let lance_app = lance_app(&store);

    // Create a Lance table through its own protocol.
    let resp = post(&lance_app, "/lance/v1/namespace/default$iso/create", "{}").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &lance_app,
        "/lance/v1/table/default$iso$logs/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Iceberg load on the same name: NoSuchTableException.
    let resp = get(
        &iceberg_app,
        &format!("{NS_PREFIX}/namespaces/iso/tables/logs"),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchTableException").await;
}

#[tokio::test]
#[serial]
async fn cross_format_same_name_conflict() {
    let store = fresh_store(db_url().await).await;
    let (iceberg_app, _mem) = test_app(&store);
    let lance_app = lance_app(&store);
    create_namespace(&iceberg_app, &["iso2"]).await;
    create_table(&iceberg_app, "iso2", "events").await;

    // Active asset names are unique per namespace across formats
    // (uq_assets_active_name): Lance cannot declare the same name.
    let resp = post(
        &lance_app,
        "/lance/v1/table/default$iso2$events/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let body = body_json(resp).await;
    assert_eq!(body["code"], "TableAlreadyExists");
}
