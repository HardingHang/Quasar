//! Lance REST Namespace adapter — namespace endpoint integration tests.
//!
//! Runs the real `lance::routes()` router against an embedded PostgreSQL
//! instance through Tower `oneshot`, covering the baseline contract:
//! `$`-encoded hierarchical ids, POST describe/drop/exists, token
//! pagination, and RFC-7807 problem responses.

#![cfg(feature = "lance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::{PostgreSQL, Settings};
use quasar_adapter::lance;
use quasar_core::{
    CatalogStore, CreateAsset, CreateDomain, CreateNamespace, DomainStore, NamespaceStore,
    TabularStore,
};
use quasar_storage::PgCatalogStore;
use serde_json::Value;
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
                    .create_database("quasar_test_lance_namespace")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_lance_namespace");
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

/// Reset the database to a clean, fully-migrated state (migrations seed the
/// `default` domain, asset types and formats).
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
    lance::routes()
        .layer(axum::Extension(lance::LanceConfig::default()))
        .with_state(store)
}

// ── HTTP helpers ────────────────────────────────────────────

async fn body_json(response: Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn get(app: &axum::Router, uri: &str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn post(app: &axum::Router, uri: &str, body: &str) -> Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Assert the full RFC-7807 problem shape extended with `code` and
/// `request_id` (DESIGN §7.6).
fn assert_problem(json: &Value, status: u16, code: &str, slug: &str) {
    assert_eq!(json["status"], status, "problem status");
    assert_eq!(json["code"], code, "problem code");
    assert_eq!(
        json["type"],
        format!("https://quasar.io/errors/{slug}"),
        "problem type"
    );
    assert!(json["title"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(json["detail"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(json["instance"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(json["request_id"].as_str().is_some_and(|s| !s.is_empty()));
}

fn assert_problem_content_type(response: &Response) {
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/problem+json")
    );
}

async fn make_namespace(store: &PgCatalogStore, domain: &str, path: &str) {
    store
        .create_namespace(domain, path, CreateNamespace::default())
        .await
        .expect("create namespace failed");
}

// ── create ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn create_and_describe_namespace() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/namespace/default$prod/create",
        r#"{"properties":{"team":"data"}}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");
    assert!(json["id"].is_string());
    assert!(json["created_at"].is_string());

    // describe is POST, not GET.
    let response = post(&app, "/lance/v1/namespace/default$prod/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");
}

#[tokio::test]
#[serial]
async fn create_hierarchical_namespace_via_encoded_path() {
    let store = setup().await;
    let app = test_app(store);

    // The `/` of the hierarchical path is URL-encoded as %2F inside the
    // single {id} path segment.
    let response = post(
        &app,
        "/lance/v1/namespace/default$teams%2Ffinance/create",
        r#"{"properties":{"cost_center":"cc-1"}}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "teams/finance");
    assert_eq!(json["properties"]["cost_center"], "cc-1");

    // The intermediate node is created implicitly by the store.
    let response = post(&app, "/lance/v1/namespace/default$teams/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "teams");

    let response = post(
        &app,
        "/lance/v1/namespace/default$teams%2Ffinance/describe",
        "{}",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "teams/finance");
}

#[tokio::test]
#[serial]
async fn create_duplicate_namespace_returns_409_problem() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default$prod/create", "{}").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(
        &json,
        409,
        "NamespaceAlreadyExists",
        "namespace-already-exists",
    );
    assert_eq!(json["instance"], "/lance/v1/namespace/default$prod/create");
}

#[tokio::test]
#[serial]
async fn create_rejects_invalid_slug_segment() {
    let store = setup().await;
    let app = test_app(store);

    // Uppercase violates the slug rule `^[a-z0-9][a-z0-9_-]{0,62}$`.
    let response = post(&app, "/lance/v1/namespace/default$Prod/create", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

#[tokio::test]
#[serial]
async fn create_rejects_root_and_domain_shaped_ids() {
    let store = setup().await;
    let app = test_app(store);

    // Root `$` is discovery-only.
    let response = post(&app, "/lance/v1/namespace/$/create", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");

    // A single segment addresses a Domain; Domain management is forbidden
    // through the Lance API.
    let response = post(&app, "/lance/v1/namespace/prod/create", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");

    // Three segments are not a valid namespace id.
    let response = post(&app, "/lance/v1/namespace/a$b$c/create", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

#[tokio::test]
#[serial]
async fn create_in_missing_domain_returns_404() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/ghost$prod/create", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "NamespaceNotFound", "namespace-not-found");
}

#[tokio::test]
#[serial]
async fn create_in_non_default_domain() {
    let store = setup().await;
    store
        .create_domain(CreateDomain {
            name: "prod".to_string(),
            ..CreateDomain::default()
        })
        .await
        .unwrap();
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/namespace/prod$analytics/create",
        r#"{"properties":{"region":"us"}}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/namespace/prod$analytics/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "analytics");
    assert_eq!(json["properties"]["region"], "us");
}

// ── list ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn list_at_root_returns_domains() {
    let store = setup().await;
    store
        .create_domain(CreateDomain {
            name: "prod".to_string(),
            ..CreateDomain::default()
        })
        .await
        .unwrap();
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/$/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let names: Vec<&str> = json["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|n| n["name"].as_str())
        .collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"prod"));
}

#[tokio::test]
#[serial]
async fn list_at_domain_returns_namespaces_with_full_paths() {
    let store = setup().await;
    make_namespace(&store, "default", "dev").await;
    make_namespace(&store, "default", "teams/finance").await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let names: Vec<&str> = json["namespaces"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|n| n["name"].as_str())
        .collect();
    // Hierarchical namespaces surface with their full materialized path;
    // the implicitly created intermediate node is listed too.
    assert!(names.contains(&"dev"));
    assert!(names.contains(&"teams"));
    assert!(names.contains(&"teams/finance"));
}

#[tokio::test]
#[serial]
async fn list_at_namespace_shaped_id_returns_empty() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default$prod/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["namespaces"].as_array().unwrap().len(), 0);
    assert!(json.get("next_page_token").is_none());
}

#[tokio::test]
#[serial]
async fn list_namespaces_paginates_with_limit_and_page_token() {
    let store = setup().await;
    for path in ["dev", "prod", "staging"] {
        make_namespace(&store, "default", path).await;
    }
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default/list?limit=2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let page1 = json["namespaces"].as_array().unwrap();
    assert_eq!(page1.len(), 2);
    let token = json["next_page_token"].as_str().unwrap().to_string();
    assert_eq!(token, "2");

    let response = get(
        &app,
        &format!("/lance/v1/namespace/default/list?limit=2&page_token={token}"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let page2 = json["namespaces"].as_array().unwrap();
    assert_eq!(page2.len(), 1);
    // A short page means no further page.
    assert!(json.get("next_page_token").is_none());

    // Pages are disjoint and together cover all namespaces.
    let mut names: Vec<String> = page1
        .iter()
        .chain(page2.iter())
        .map(|n| n["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["dev", "prod", "staging"]);
}

#[tokio::test]
#[serial]
async fn list_root_paginates_domains() {
    let store = setup().await;
    for name in ["alpha", "beta"] {
        store
            .create_domain(CreateDomain {
                name: name.to_string(),
                ..CreateDomain::default()
            })
            .await
            .unwrap();
    }
    let app = test_app(store);

    // 3 domains total (seeded `default` + 2 created); limit=2 fills the page.
    let response = get(&app, "/lance/v1/namespace/$/list?limit=2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["namespaces"].as_array().unwrap().len(), 2);
    assert_eq!(json["next_page_token"], "2");

    let response = get(&app, "/lance/v1/namespace/$/list?limit=2&page_token=2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["namespaces"].as_array().unwrap().len(), 1);
    assert!(json.get("next_page_token").is_none());
}

// ── describe ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn describe_is_post_only() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default$prod/describe").await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
#[serial]
async fn describe_missing_namespace_returns_404_problem() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default$missing/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 404, "NamespaceNotFound", "namespace-not-found");
    assert_eq!(
        json["instance"],
        "/lance/v1/namespace/default$missing/describe"
    );
}

#[tokio::test]
#[serial]
async fn describe_rejects_domain_shaped_id() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

// ── drop ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn drop_namespace_then_404_on_second_drop() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default$prod/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/namespace/default$prod/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);

    let response = post(&app, "/lance/v1/namespace/default$prod/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "NamespaceNotFound", "namespace-not-found");
}

#[tokio::test]
#[serial]
async fn drop_non_empty_namespace_with_table_returns_409() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    store
        .create_tabular_asset(
            CreateAsset {
                domain: "default".to_string(),
                namespace: "prod".to_string(),
                name: "users".to_string(),
                asset_type: "table".to_string(),
                format: Some("lance".to_string()),
                ..CreateAsset::default()
            },
            "lance://prod/users",
            None,
        )
        .await
        .unwrap();
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default$prod/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 409, "NamespaceNotEmpty", "namespace-not-empty");
}

#[tokio::test]
#[serial]
async fn drop_namespace_with_child_returns_409() {
    let store = setup().await;
    make_namespace(&store, "default", "teams/finance").await;
    let app = test_app(store);

    // `teams` has the child `teams/finance` and cannot be dropped.
    let response = post(&app, "/lance/v1/namespace/default$teams/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_problem(&json, 409, "NamespaceNotEmpty", "namespace-not-empty");

    // Dropping the leaf first frees the parent.
    let response = post(
        &app,
        "/lance/v1/namespace/default$teams%2Ffinance/drop",
        "{}",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = post(&app, "/lance/v1/namespace/default$teams/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
}

// ── exists ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn namespace_exists_true_and_false() {
    let store = setup().await;
    make_namespace(&store, "default", "teams/finance").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/namespace/default$teams%2Ffinance/exists",
        "{}",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);

    let response = post(&app, "/lance/v1/namespace/default$missing/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn exists_rejects_domain_shaped_id() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/namespace/default/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}
