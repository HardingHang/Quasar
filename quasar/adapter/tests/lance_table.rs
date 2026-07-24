//! Lance REST Namespace adapter — table endpoint integration tests.
//!
//! Runs the real `lance::routes()` router against an embedded PostgreSQL
//! instance through Tower `oneshot`, covering declare/describe/register/
//! deregister/drop/exists/rename, format isolation against same-named
//! Iceberg assets, token pagination, and RFC-7807 problem responses.

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
    AssetStore, CatalogStore, CreateAsset, CreateNamespace, NamespaceStore, TabularStore,
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
                    .create_database("quasar_test_lance_table")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_lance_table");
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

// ── Store helpers ───────────────────────────────────────────

async fn make_namespace(store: &PgCatalogStore, domain: &str, path: &str) {
    store
        .create_namespace(domain, path, CreateNamespace::default())
        .await
        .expect("create namespace failed");
}

/// Create a `table` asset directly through the store (any format).
async fn make_table(store: &PgCatalogStore, namespace: &str, name: &str, format: &str) {
    store
        .create_tabular_asset(
            CreateAsset {
                domain: "default".to_string(),
                namespace: namespace.to_string(),
                name: name.to_string(),
                asset_type: "table".to_string(),
                format: Some(format.to_string()),
                ..CreateAsset::default()
            },
            &format!("{format}://{namespace}/{name}"),
            None,
        )
        .await
        .expect("create tabular asset failed");
}

async fn declare_lance_table(app: &axum::Router, id: &str) -> Response {
    post(app, &format!("/lance/v1/table/{id}/declare"), "{}").await
}

// ── declare / describe ──────────────────────────────────────

#[tokio::test]
#[serial]
async fn declare_and_describe_table() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = declare_lance_table(&app, "default$prod$users").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "users");
    // No warehouse configured: the adapter mints a lance:// location.
    assert_eq!(json["location"], "lance://prod/users");

    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["location"], "lance://prod/users");
    // A declared table has no mirrored version yet.
    assert!(json["current_version"].is_null());
    assert!(json["created_at"].is_string());
}

#[tokio::test]
#[serial]
async fn declare_in_hierarchical_namespace() {
    let store = setup().await;
    make_namespace(&store, "default", "teams/finance").await;
    let app = test_app(store);

    let response = declare_lance_table(&app, "default$teams%2Ffinance$events").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "events");
    assert_eq!(json["location"], "lance://teams/finance/events");

    let response = post(
        &app,
        "/lance/v1/table/default$teams%2Ffinance$events/describe",
        "{}",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["location"], "lance://teams/finance/events");
}

#[tokio::test]
#[serial]
async fn declare_duplicate_returns_409_problem() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = declare_lance_table(&app, "default$prod$users").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = declare_lance_table(&app, "default$prod$users").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 409, "TableAlreadyExists", "table-already-exists");
}

#[tokio::test]
#[serial]
async fn declare_in_missing_namespace_returns_404() {
    let store = setup().await;
    let app = test_app(store);

    let response = declare_lance_table(&app, "default$prod$users").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

#[tokio::test]
#[serial]
async fn declare_rejects_invalid_table_name_and_id_shape() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    // Uppercase table name violates the slug rule.
    let response = declare_lance_table(&app, "default$prod$Users").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");

    // Two segments are not a table id.
    let response = declare_lance_table(&app, "prod$users").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");

    // Four segments are not a table id either.
    let response = declare_lance_table(&app, "a$b$c$d").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

#[tokio::test]
#[serial]
async fn describe_missing_table_returns_404_problem() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/table/default$prod$missing/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
    assert_eq!(
        json["instance"],
        "/lance/v1/table/default$prod$missing/describe"
    );
}

// ── register ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn register_table_with_external_location() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/register",
        r#"{"location":"s3://bucket/warehouse/prod/users"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["location"], "s3://bucket/warehouse/prod/users");

    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["location"], "s3://bucket/warehouse/prod/users");
}

#[tokio::test]
#[serial]
async fn register_duplicate_returns_409() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/register",
        r#"{"location":"s3://bucket/warehouse/prod/users"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_problem(&json, 409, "TableAlreadyExists", "table-already-exists");
}

// ── list ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn list_tables_in_namespace() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "t1", "lance").await;
    make_table(&store, "prod", "t2", "lance").await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default$prod/table/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    let names: Vec<&str> = tables.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"t1"));
    assert!(names.contains(&"t2"));
}

#[tokio::test]
#[serial]
async fn list_tables_empty_namespace() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default$prod/table/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["tables"].as_array().unwrap().is_empty());
    assert!(json.get("next_page_token").is_none());
}

#[tokio::test]
#[serial]
async fn list_tables_paginates_with_limit_and_page_token() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    for name in ["t1", "t2", "t3"] {
        make_table(&store, "prod", name, "lance").await;
    }
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default$prod/table/list?limit=2").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let page1 = json["tables"].as_array().unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(json["next_page_token"], "2");

    let response = get(
        &app,
        "/lance/v1/namespace/default$prod/table/list?limit=2&page_token=2",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let page2 = json["tables"].as_array().unwrap();
    assert_eq!(page2.len(), 1);
    assert!(json.get("next_page_token").is_none());

    let mut names: Vec<String> = page1
        .iter()
        .chain(page2.iter())
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["t1", "t2", "t3"]);
}

#[tokio::test]
#[serial]
async fn list_tables_rejects_domain_shaped_id() {
    let store = setup().await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/namespace/default/table/list").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

// ── exists ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn table_exists_true_and_false() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);

    // Missing table in an existing namespace.
    let response = post(&app, "/lance/v1/table/default$prod$missing/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);

    // Missing namespace also reads as "not exists".
    let response = post(&app, "/lance/v1/table/default$ghost$users/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}

// ── deregister / drop ───────────────────────────────────────

#[tokio::test]
#[serial]
async fn deregister_table_removes_it_from_catalog() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/table/default$prod$users/deregister", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);

    let response = post(&app, "/lance/v1/table/default$prod$users/deregister", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

#[tokio::test]
#[serial]
async fn drop_table_removes_it_from_catalog() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/table/default$prod$users/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn drop_missing_table_returns_404() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(&app, "/lance/v1/table/default$prod$missing/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

// ── rename ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn rename_table_within_namespace() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"customers"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);

    let response = post(&app, "/lance/v1/table/default$prod$customers/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn rename_with_same_namespace_id_is_accepted() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    // new_namespace_id naming the current namespace is a plain rename.
    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"customers","new_namespace_id":["default","prod"]}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(&app, "/lance/v1/table/default$prod$customers/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn rename_cross_namespace_move_rejected_400() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_namespace(&store, "default", "staging").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    // The baseline contract addresses assets by id and forbids moving a
    // table across namespaces through rename.
    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"users","new_namespace_id":["default","staging"]}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");

    // The table is untouched.
    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn rename_cross_domain_rejected_400() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"users","new_namespace_id":["other","prod"]}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

#[tokio::test]
#[serial]
async fn rename_to_existing_name_returns_409() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    make_table(&store, "prod", "users", "lance").await;
    make_table(&store, "prod", "customers", "lance").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"customers"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_problem(&json, 409, "TableAlreadyExists", "table-already-exists");
}

#[tokio::test]
#[serial]
async fn rename_missing_table_returns_404() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$missing/rename",
        r#"{"new_table_name":"newname"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

// ── format isolation ────────────────────────────────────────

#[tokio::test]
#[serial]
async fn iceberg_table_is_invisible_through_lance_endpoints() {
    let store = setup().await;
    make_namespace(&store, "default", "prod").await;
    // Same-named asset carrying the `iceberg` format, created directly
    // through the store (as the Iceberg adapter would).
    make_table(&store, "prod", "users", "iceberg").await;
    let app = test_app(store.clone());

    // describe → TableNotFound
    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");

    // drop → TableNotFound, and the Iceberg asset survives.
    let response = post(&app, "/lance/v1/table/default$prod$users/drop", "{}").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");

    // rename → TableNotFound
    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/rename",
        r#"{"new_table_name":"customers"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");

    // exists → false (a same-named non-Lance asset does not count)
    let response = post(&app, "/lance/v1/table/default$prod$users/exists", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);

    // The Iceberg table does not leak into the Lance table listing.
    let response = get(&app, "/lance/v1/namespace/default$prod/table/list").await;
    let json = body_json(response).await;
    assert!(json["tables"].as_array().unwrap().is_empty());

    // The Iceberg asset was never touched.
    let asset = store
        .get_asset_by_name("default", "prod", "users")
        .await
        .unwrap();
    assert_eq!(asset.format.as_deref(), Some("iceberg"));
    assert!(asset.deleted_at.is_none());
}
