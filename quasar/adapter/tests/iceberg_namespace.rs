//! Iceberg REST Catalog — namespace endpoint integration tests (hierarchical).

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
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_namespace").await })
        .await
        .1
}

#[tokio::test]
#[serial]
async fn namespace_hierarchical_crud() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);

    // Multi-level creation; intermediates are created implicitly.
    create_namespace(&app, &["analytics", "teams", "finance"]).await;
    for path in ["analytics", "analytics/teams", "analytics/teams/finance"] {
        let resp = get(&app, &format!("{NS_PREFIX}/namespaces/{path}")).await;
        assert_eq!(resp.status(), StatusCode::OK, "path {path} should exist");
    }

    // Duplicate creation conflicts.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces"),
        &json!({"namespace": ["analytics"], "properties": {}}).to_string(),
    )
    .await;
    assert_iceberg_error(
        resp,
        StatusCode::CONFLICT,
        "NamespaceAlreadyExistsException",
    )
    .await;

    // HEAD exists checks (204 per Iceberg spec).
    let resp = head(&app, &format!("{NS_PREFIX}/namespaces/analytics/teams")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = head(&app, &format!("{NS_PREFIX}/namespaces/ghost")).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Non-empty namespace cannot be dropped.
    let resp = delete(&app, &format!("{NS_PREFIX}/namespaces/analytics")).await;
    assert_iceberg_error(resp, StatusCode::CONFLICT, "NamespaceNotEmptyException").await;

    // Drop leaf-to-root.
    for path in ["analytics/teams/finance", "analytics/teams", "analytics"] {
        let resp = delete(&app, &format!("{NS_PREFIX}/namespaces/{path}")).await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT, "drop {path}");
    }
}

#[tokio::test]
#[serial]
async fn namespace_list_with_parent() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["team", "a"]).await;
    create_namespace(&app, &["team", "b"]).await;
    create_namespace(&app, &["other"]).await;

    // Root listing.
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces")).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Parent filter lists the subtree (excluding the parent itself).
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces?parent=team")).await;
    let body = body_json(resp).await;
    let ns_list: Vec<String> = body["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ns| {
            ns.as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap())
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect();
    assert!(ns_list.contains(&"team/a".to_string()));
    assert!(ns_list.contains(&"team/b".to_string()));
    assert!(!ns_list.contains(&"other".to_string()));
}

#[tokio::test]
#[serial]
async fn namespace_properties_update() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["props"]).await;

    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/props/properties"),
        &json!({"removals": [], "updates": {"owner": "team-a", "env": "prod"}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/props")).await;
    let body = body_json(resp).await;
    assert_eq!(body["properties"]["owner"], "team-a");

    // Removal.
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces/props/properties"),
        &json!({"removals": ["env"], "updates": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = get(&app, &format!("{NS_PREFIX}/namespaces/props")).await;
    let body = body_json(resp).await;
    assert!(body["properties"].get("env").is_none());
}

#[tokio::test]
#[serial]
async fn namespace_validation_and_missing_domain() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);

    // Invalid slug segment (uppercase).
    let resp = post(
        &app,
        &format!("{NS_PREFIX}/namespaces"),
        &json!({"namespace": ["BadName"], "properties": {}}).to_string(),
    )
    .await;
    assert_iceberg_error(resp, StatusCode::BAD_REQUEST, "BadRequestException").await;

    // Unknown domain prefix.
    let resp = get(&app, "/iceberg/v1/ghost/namespaces/anything").await;
    assert_iceberg_error(resp, StatusCode::NOT_FOUND, "NoSuchNamespaceException").await;
}
