#![cfg(all(feature = "lance", feature = "iceberg", feature = "unified"))]
//! Server — cross-protocol smoke tests against one shared PostgreSQL.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use serde_json::json;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { start_pg("quasar_test_server_smoke").await })
        .await
        .1
}

async fn create_domain_unified(app: &axum::Router, name: &str) {
    let resp = post(
        app,
        "/unified/v1/domains",
        &json!({"name": name}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
#[serial]
async fn dual_instance_stateless_smoke() {
    let pool1 = fresh_pool(db_url().await).await;
    let pool2 = test_pool(db_url().await);
    let app1 = test_app(pool1);
    let app2 = test_app(pool2);

    // Write through instance 1, read through instance 2.
    create_domain_unified(&app1, "shared").await;
    let resp = post(
        &app1,
        "/unified/v1/domains/shared/namespaces",
        &json!({"path": "data/team"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = get(&app2, "/unified/v1/domains/shared/namespaces/data/team").await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "instance 2 sees instance 1 writes"
    );

    let resp = post(
        &app2,
        "/lance/v1/table/shared$data%2Fteam$t1/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = get(
        &app1,
        "/unified/v1/domains/shared/namespaces/data/team/assets",
    )
    .await;
    let items = body_json(resp).await["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1, "instance 1 sees instance 2 writes");
    assert_eq!(items[0]["name"], "t1");
}

#[tokio::test]
#[serial]
async fn lance_full_lifecycle() {
    let app = test_app(fresh_pool(db_url().await).await);

    let resp = post(&app, "/lance/v1/namespace/default$ns/create", "{}").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &app,
        "/lance/v1/table/default$ns$tbl/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &app,
        "/lance/v1/table/default$ns$tbl/version/create",
        &json!({"version": 1, "manifest_path": "s3://bucket/ns/tbl/1.manifest"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = get(&app, "/lance/v1/table/default$ns$tbl/version/list").await;
    assert_eq!(body_json(resp).await["versions"], json!([1]));
    let resp = post(&app, "/lance/v1/table/default$ns$tbl/drop", "{}").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(&app, "/lance/v1/table/default$ns$tbl/exists", "{}").await;
    assert_eq!(body_json(resp).await["exists"], false);
}

#[tokio::test]
#[serial]
async fn iceberg_full_lifecycle() {
    let app = test_app(fresh_pool(db_url().await).await);
    let prefix = "/iceberg/v1/default";

    let resp = post(
        &app,
        &format!("{prefix}/namespaces"),
        &json!({"namespace": ["analytics"], "properties": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = post(
        &app,
        &format!("{prefix}/namespaces/analytics/tables"),
        &json!({
            "name": "events",
            "location": "s3://bucket/analytics/events",
            "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]},
            "properties": {}
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = get(
        &app,
        &format!("{prefix}/namespaces/analytics/tables/events"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Commit a property change.
    let resp = post(
        &app,
        &format!("{prefix}/namespaces/analytics/tables/events"),
        &json!({"requirements": [], "updates": [{"action": "set-properties", "updates": {"a": "b"}}]})
            .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = delete(
        &app,
        &format!("{prefix}/namespaces/analytics/tables/events"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(
        &app,
        &format!("{prefix}/namespaces/analytics/tables/events"),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn domain_isolation_same_namespace_name() {
    let app = test_app(fresh_pool(db_url().await).await);
    create_domain_unified(&app, "prod").await;
    create_domain_unified(&app, "staging").await;

    for domain in ["prod", "staging"] {
        let resp = post(
            &app,
            &format!("/unified/v1/domains/{domain}/namespaces"),
            &json!({"path": "analytics"}).to_string(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED, "domain {domain}");
    }

    // Same table name in the same namespace path under different domains.
    let resp = post(
        &app,
        "/lance/v1/table/prod$analytics$events/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &app,
        "/lance/v1/table/staging$analytics$events/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK, "other domain unaffected");
}

#[tokio::test]
#[serial]
async fn cross_format_name_conflict_server_level() {
    let app = test_app(fresh_pool(db_url().await).await);

    let resp = post(
        &app,
        "/iceberg/v1/default/namespaces",
        &json!({"namespace": ["core"], "properties": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = post(
        &app,
        "/iceberg/v1/default/namespaces/core/tables",
        &json!({
            "name": "events",
            "location": "s3://bucket/core/events",
            "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]},
            "properties": {}
        })
        .to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Lance cannot declare the same active name in the same namespace.
    let resp = post(
        &app,
        "/lance/v1/table/default$core$events/declare",
        &json!({"options": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
#[serial]
async fn non_empty_domain_delete_protection() {
    let app = test_app(fresh_pool(db_url().await).await);
    create_domain_unified(&app, "protected").await;
    let resp = post(
        &app,
        "/unified/v1/domains/protected/namespaces",
        &json!({"path": "ns"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = delete(&app, "/unified/v1/domains/protected").await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}
