#![cfg(all(feature = "lance", feature = "iceberg", feature = "unified"))]
//! Server — health / readiness / metrics endpoint integration tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { start_pg("quasar_test_server_health").await })
        .await
        .1
}

#[tokio::test]
#[serial]
async fn healthz_always_ok() {
    let pool = fresh_pool(db_url().await).await;
    let app = test_app(pool);

    let resp = get(&app, "/healthz").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["status"], "ok");
}

#[tokio::test]
#[serial]
async fn readyz_checks_database() {
    let pool = fresh_pool(db_url().await).await;
    let app = test_app(pool);

    let resp = get(&app, "/readyz").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "ready");
    assert_eq!(body["checks"]["database"], "ok");
}

#[tokio::test]
#[serial]
async fn metrics_endpoint_renders_prometheus_text() {
    let pool = fresh_pool(db_url().await).await;
    let app = test_app(pool);

    // Generate some traffic, then read the metrics output.
    get(&app, "/healthz").await;
    let resp = get(&app, "/metrics").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let text = String::from_utf8(
        http_body_util::BodyExt::collect(resp.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("http_requests"), "metrics text: {text}");
}

#[tokio::test]
#[serial]
async fn protocol_routers_mounted() {
    let pool = fresh_pool(db_url().await).await;
    let app = test_app(pool);

    // Iceberg config endpoint.
    let resp = get(&app, "/iceberg/v1/config").await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Lance namespace list (root).
    let resp = get(&app, "/lance/v1/namespace/$/list").await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Unified domain list.
    let resp = get(&app, "/unified/v1/domains").await;
    assert_eq!(resp.status(), StatusCode::OK);
}
