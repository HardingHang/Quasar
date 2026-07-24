//! Iceberg REST Catalog — GET /iceberg/v1/config integration tests.

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_config").await })
        .await
        .1
}

#[tokio::test]
#[serial]
async fn config_returns_defaults_and_endpoints() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);

    let resp = get(&app, "/iceberg/v1/config").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["defaults"]["warehouse"], "s3://bucket");
    // Endpoint advertisement matches reality and contains no scan planning.
    let endpoints: Vec<&str> = body["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e.as_str().unwrap())
        .collect();
    assert!(endpoints.contains(&"POST /v1/{prefix}/transactions/commit"));
    assert!(endpoints.contains(&"GET /v1/{prefix}/namespaces/{namespace}/views/{view}"));
    assert!(!endpoints.iter().any(|e| e.contains("plan")));
    assert!(!endpoints.iter().any(|e| e.contains("tasks")));
}

#[tokio::test]
#[serial]
async fn config_warehouse_query_validation() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);

    // Matching the configured default warehouse is accepted and echoed.
    let resp = get(&app, "/iceberg/v1/config?warehouse=warehouse").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["overrides"]["warehouse"], "warehouse");

    // Unknown warehouse is rejected per spec.
    let resp = get(&app, "/iceberg/v1/config?warehouse=ghost").await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchWarehouseException").await;
}
