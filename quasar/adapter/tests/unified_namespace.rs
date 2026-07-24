//! Unified API — domain & namespace endpoint integration tests.
//!
//! Runs the real `unified::routes()` router against an embedded PostgreSQL
//! instance through Tower `oneshot`: domain CRUD, hierarchical namespace
//! CRUD with wildcard paths, token pagination, and RFC-7807 errors.

#![cfg(feature = "unified")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::{PostgreSQL, Settings};
use quasar_adapter::unified;
use quasar_core::CatalogStore;
use quasar_storage::PgCatalogStore;
use serde_json::{json, Value};
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tower::ServiceExt;

// ── Embedded PostgreSQL bootstrap (per test binary) ─────────

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

const ZONKY_RELEASES_URL: &str = "https://github.com/zonkyio/embedded-postgres-binaries";

struct PgInstance {
    #[allow(dead_code)]
    postgresql: PostgreSQL,
    url: String,
}

impl PgInstance {
    async fn get() -> &'static Self {
        PG_INSTANCE
            .get_or_init(|| async {
                let settings = Settings {
                    releases_url: ZONKY_RELEASES_URL.to_string(),
                    ..Default::default()
                };
                let mut postgresql = PostgreSQL::new(settings);
                postgresql.setup().await.expect("PostgreSQL setup failed");
                postgresql.start().await.expect("PostgreSQL start failed");
                postgresql
                    .create_database("quasar_test_unified_namespace")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_unified_namespace");
                PgInstance { postgresql, url }
            })
            .await
    }
}

fn test_pool(url: &str) -> Pool {
    let config = url
        .parse::<tokio_postgres::Config>()
        .expect("invalid database URL");
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool")
}

/// Reset to a clean, fully-migrated state (migrations seed the `default`
/// domain, asset types and formats).
async fn setup() -> Arc<PgCatalogStore> {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    {
        let client = pool.get().await.expect("pool checkout failed");
        client
            .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .await
            .expect("schema reset failed");
    }
    let store = Arc::new(PgCatalogStore::new(pool));
    store.initialize().await.expect("initialize failed");
    store
}

fn test_app(store: Arc<PgCatalogStore>) -> axum::Router {
    let store: Arc<dyn CatalogStore> = store;
    unified::routes()
        .layer(axum::Extension(unified::UnifiedConfig::default()))
        .with_state(store)
}

// ── HTTP helpers ────────────────────────────────────────────

async fn body_json(response: Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn request(app: &axum::Router, method: &str, uri: &str, body: Option<String>) -> Response {
    let builder = Request::builder().method(method).uri(uri);
    let builder = match &body {
        Some(_) => builder.header(header::CONTENT_TYPE, "application/json"),
        None => builder,
    };
    app.clone()
        .oneshot(
            builder
                .body(body.map(Body::from).unwrap_or_else(Body::empty))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn get(app: &axum::Router, uri: &str) -> Response {
    request(app, "GET", uri, None).await
}

async fn post(app: &axum::Router, uri: &str, body: &str) -> Response {
    request(app, "POST", uri, Some(body.to_string())).await
}

async fn patch(app: &axum::Router, uri: &str, body: &str) -> Response {
    request(app, "PATCH", uri, Some(body.to_string())).await
}

async fn delete(app: &axum::Router, uri: &str) -> Response {
    request(app, "DELETE", uri, None).await
}

/// Assert an RFC-7807 problem response with the expected status and machine code.
async fn assert_problem(response: Response, status: StatusCode, code: &str) -> Value {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    let body = body_json(response).await;
    assert_eq!(body["status"], status.as_u16());
    assert_eq!(body["code"], code);
    assert!(body["type"]
        .as_str()
        .unwrap()
        .starts_with("https://quasar.io/errors/"));
    assert!(body["request_id"].as_str().unwrap().len() >= 32);
    body
}

async fn create_domain(app: &axum::Router, name: &str) -> Value {
    let resp = post(
        app,
        "/unified/v1/domains",
        &json!({"name": name}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    body_json(resp).await
}

// ── Domain tests ────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn domain_crud_roundtrip() {
    let app = test_app(setup().await);

    let created = create_domain(&app, "analytics").await;
    assert_eq!(created["name"], "analytics");
    assert!(created["id"].as_str().is_some());
    // storage_config must never appear in responses (sensitive red line).
    assert!(created.get("storage_config").is_none());

    let resp = get(&app, "/unified/v1/domains/analytics").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched = body_json(resp).await;
    assert_eq!(fetched["id"], created["id"]);

    let resp = delete(&app, "/unified/v1/domains/analytics").await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(&app, "/unified/v1/domains/analytics").await;
    assert_problem(resp, StatusCode::NOT_FOUND, "NOT_FOUND").await;
}

#[tokio::test]
#[serial]
async fn domain_create_conflict_and_invalid() {
    let app = test_app(setup().await);

    create_domain(&app, "dup").await;
    let resp = post(
        &app,
        "/unified/v1/domains",
        &json!({"name": "dup"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::CONFLICT, "ALREADY_EXISTS").await;

    // Uppercase is not a valid slug.
    let resp = post(
        &app,
        "/unified/v1/domains",
        &json!({"name": "BadName"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;

    // Invalid storage_type is rejected at the adapter layer.
    let resp = post(
        &app,
        "/unified/v1/domains",
        &json!({"name": "badtype", "storage_type": "ftp"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;
}

#[tokio::test]
#[serial]
async fn domain_patch_three_states() {
    let app = test_app(setup().await);
    create_domain(&app, "patchy").await;

    // Set comment and warehouse.
    let resp = patch(
        &app,
        "/unified/v1/domains/patchy",
        &json!({"comment": "hello", "warehouse": "s3://bucket/root"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["comment"], "hello");
    assert_eq!(body["warehouse"], "s3://bucket/root");

    // Unset comment, leave warehouse untouched (NoChange).
    let resp = patch(
        &app,
        "/unified/v1/domains/patchy",
        &json!({"comment": null}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["comment"].is_null());
    assert_eq!(body["warehouse"], "s3://bucket/root");
}

#[tokio::test]
#[serial]
async fn domain_delete_non_empty_conflict() {
    let app = test_app(setup().await);
    create_domain(&app, "parent").await;
    let resp = post(
        &app,
        "/unified/v1/domains/parent/namespaces",
        &json!({"path": "ns1"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = delete(&app, "/unified/v1/domains/parent").await;
    assert_problem(resp, StatusCode::CONFLICT, "CONFLICT").await;
}

#[tokio::test]
#[serial]
async fn domain_list_pagination() {
    let app = test_app(setup().await);
    for name in ["d1", "d2", "d3"] {
        create_domain(&app, name).await;
    }

    // 3 created + seeded `default` = 4 domains; page size 2 gives two pages.
    let resp = get(&app, "/unified/v1/domains?pageSize=2").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let page1 = body_json(resp).await;
    assert_eq!(page1["items"].as_array().unwrap().len(), 2);
    let token = page1["next_page_token"].as_str().unwrap().to_string();

    let resp = get(
        &app,
        &format!("/unified/v1/domains?pageSize=2&pageToken={token}"),
    )
    .await;
    let page2 = body_json(resp).await;
    assert_eq!(page2["items"].as_array().unwrap().len(), 2);
    // A full page always yields a token (DESIGN §5.4); the emptiness of the
    // following page is only discovered on the next fetch.
    let token2 = page2["next_page_token"].as_str().unwrap().to_string();
    let resp = get(
        &app,
        &format!("/unified/v1/domains?pageSize=2&pageToken={token2}"),
    )
    .await;
    let page3 = body_json(resp).await;
    assert_eq!(page3["items"].as_array().unwrap().len(), 0);
    assert!(page3.get("next_page_token").is_none());

    let names: Vec<&str> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .chain(page2["items"].as_array().unwrap().iter())
        .map(|d| d["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["d1", "d2", "d3", "default"]);

    // pageSize=0 is invalid; oversized pageSize is clamped, not rejected.
    let resp = get(&app, "/unified/v1/domains?pageSize=0").await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;
    let resp = get(&app, "/unified/v1/domains?pageSize=99999").await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ── Namespace tests ─────────────────────────────────────────

#[tokio::test]
#[serial]
async fn namespace_hierarchical_create_and_get() {
    let app = test_app(setup().await);

    // Creating a/b/c implicitly creates a and a/b (FR-N2).
    let resp = post(
        &app,
        "/unified/v1/domains/default/namespaces",
        &json!({"path": "a/b/c", "comment": "deep"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp).await;
    assert_eq!(created["path"], "a/b/c");
    assert_eq!(created["depth"], 3);

    for path in ["a", "a/b", "a/b/c"] {
        let resp = get(
            &app,
            &format!("/unified/v1/domains/default/namespaces/{path}"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "path {path} should exist");
    }

    // Duplicate path conflicts.
    let resp = post(
        &app,
        "/unified/v1/domains/default/namespaces",
        &json!({"path": "a/b/c"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::CONFLICT, "ALREADY_EXISTS").await;

    // Invalid segment (uppercase) rejected.
    let resp = post(
        &app,
        "/unified/v1/domains/default/namespaces",
        &json!({"path": "a/Bad"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;

    // Missing domain 404.
    let resp = get(&app, "/unified/v1/domains/ghost/namespaces/a").await;
    assert_problem(resp, StatusCode::NOT_FOUND, "NOT_FOUND").await;
}

#[tokio::test]
#[serial]
async fn namespace_list_prefix_filter() {
    let app = test_app(setup().await);
    for path in ["team/x", "team/y", "other"] {
        post(
            &app,
            "/unified/v1/domains/default/namespaces",
            &json!({"path": path}).to_string(),
        )
        .await;
    }

    let resp = get(&app, "/unified/v1/domains/default/namespaces?prefix=team").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let paths: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["path"].as_str().unwrap())
        .collect();
    assert!(paths.contains(&"team"));
    assert!(paths.contains(&"team/x"));
    assert!(paths.contains(&"team/y"));
    assert!(!paths.contains(&"other"));
}

#[tokio::test]
#[serial]
async fn namespace_patch_and_delete_rules() {
    let app = test_app(setup().await);
    post(
        &app,
        "/unified/v1/domains/default/namespaces",
        &json!({"path": "life/child"}).to_string(),
    )
    .await;

    let resp = patch(
        &app,
        "/unified/v1/domains/default/namespaces/life",
        &json!({"comment": "updated"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["comment"], "updated");

    // Parent with a child cannot be deleted.
    let resp = delete(&app, "/unified/v1/domains/default/namespaces/life").await;
    assert_problem(resp, StatusCode::CONFLICT, "CONFLICT").await;

    // Delete child first, then parent.
    let resp = delete(&app, "/unified/v1/domains/default/namespaces/life/child").await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = delete(&app, "/unified/v1/domains/default/namespaces/life").await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}
