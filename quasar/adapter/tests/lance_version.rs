//! Lance REST Namespace adapter — version endpoint integration tests.
//!
//! Runs the real `lance::routes()` router against an embedded PostgreSQL
//! instance through Tower `oneshot`, covering version mirroring into
//! `asset_versions` + `assets.current_version_key`, duplicate-version 409,
//! list/describe, and the describe_table `current_version` progression.

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
    VersionStore,
};
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tower::ServiceExt;
use uuid::Uuid;

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
                    .create_database("quasar_test_lance_version")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_lance_version");
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

/// Create namespace `default/prod` plus a Lance table `users`; returns the
/// asset id for direct store-side assertions.
async fn make_lance_table(store: &PgCatalogStore) -> Uuid {
    store
        .create_namespace("default", "prod", CreateNamespace::default())
        .await
        .expect("create namespace failed");
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
        .expect("create tabular asset failed")
        .asset
        .id
}

async fn create_version(app: &axum::Router, version: i64, manifest: &str) -> Response {
    post(
        app,
        "/lance/v1/table/default$prod$users/version/create",
        &format!(r#"{{"version":{version},"manifest_path":"{manifest}"}}"#),
    )
    .await
}

// ── create ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn create_version_mirrors_to_store_and_sets_current_version() {
    let store = setup().await;
    let asset_id = make_lance_table(&store).await;
    let app = test_app(store.clone());

    let response = create_version(
        &app,
        1,
        "s3://bucket/warehouse/prod/users/_versions/1.manifest",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["version"], 1);
    assert_eq!(
        json["manifest_path"],
        "s3://bucket/warehouse/prod/users/_versions/1.manifest"
    );

    // The store mirrors the version into asset_versions and advances
    // assets.current_version_key in the same transaction.
    let version = store.get_latest_version(asset_id).await.unwrap();
    assert_eq!(version.version_key, "1");
    assert_eq!(
        version.content_pointer.as_deref(),
        Some("s3://bucket/warehouse/prod/users/_versions/1.manifest")
    );
    assert!(version.previous_version_id.is_none());

    let asset = store.get_asset(asset_id).await.unwrap();
    assert_eq!(asset.current_version_key.as_deref(), Some("1"));
}

#[tokio::test]
#[serial]
async fn create_versions_link_the_version_chain() {
    let store = setup().await;
    let asset_id = make_lance_table(&store).await;
    let app = test_app(store.clone());

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = create_version(&app, 2, "s3://bucket/v2.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);

    // The second version chains to the first so the single-root
    // constraint holds.
    let v1 = store.get_version(asset_id, "1").await.unwrap();
    let v2 = store.get_version(asset_id, "2").await.unwrap();
    assert_eq!(v2.previous_version_id, Some(v1.id));

    let latest = store.get_latest_version(asset_id).await.unwrap();
    assert_eq!(latest.version_key, "2");
}

#[tokio::test]
#[serial]
async fn create_duplicate_version_returns_409_problem() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = create_version(&app, 1, "s3://bucket/v1-again.manifest").await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 409, "TableAlreadyExists", "table-already-exists");
}

#[tokio::test]
#[serial]
async fn create_version_on_missing_table_returns_404() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", CreateNamespace::default())
        .await
        .unwrap();
    let app = test_app(store);

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

#[tokio::test]
#[serial]
async fn create_version_rejects_invalid_id_shape() {
    let store = setup().await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/prod$users/version/create",
        r#"{"version":1,"manifest_path":"s3://bucket/v1.manifest"}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_problem(&json, 400, "InvalidInput", "invalid-input");
}

// ── list ────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn list_versions_returns_sorted_native_numbers() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    for (v, m) in [
        (2, "s3://bucket/v2.manifest"),
        (1, "s3://bucket/v1.manifest"),
        (3, "s3://bucket/v3.manifest"),
    ] {
        let response = create_version(&app, v, m).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let response = get(&app, "/lance/v1/table/default$prod$users/version/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let versions: Vec<i64> = json["versions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    // Ordered by native version number, not insertion order.
    assert_eq!(versions, vec![1, 2, 3]);
}

#[tokio::test]
#[serial]
async fn list_versions_empty_for_fresh_table() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    let response = get(&app, "/lance/v1/table/default$prod$users/version/list").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["versions"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[serial]
async fn list_versions_on_missing_table_returns_404() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", CreateNamespace::default())
        .await
        .unwrap();
    let app = test_app(store);

    let response = get(&app, "/lance/v1/table/default$prod$users/version/list").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
}

// ── describe ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn describe_version_returns_manifest_path() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/version/describe",
        r#"{"version":1}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["version"], 1);
    assert_eq!(json["manifest_path"], "s3://bucket/v1.manifest");
}

#[tokio::test]
#[serial]
async fn describe_missing_version_returns_404_problem() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/version/describe",
        r#"{"version":99}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_problem_content_type(&response);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");
    assert_eq!(
        json["instance"],
        "/lance/v1/table/default$prod$users/version/describe"
    );
}

// ── describe_table current_version progression ──────────────

#[tokio::test]
#[serial]
async fn describe_table_current_version_advances_with_versions() {
    let store = setup().await;
    make_lance_table(&store).await;
    let app = test_app(store);

    // No mirrored version yet: no current version.
    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["current_version"].is_null());

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["current_version"], 1);

    let response = create_version(&app, 2, "s3://bucket/v2.manifest").await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = post(&app, "/lance/v1/table/default$prod$users/describe", "{}").await;
    let json = body_json(response).await;
    assert_eq!(json["current_version"], 2);
}

// ── format isolation ────────────────────────────────────────

#[tokio::test]
#[serial]
async fn version_endpoints_reject_iceberg_table_as_not_found() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", CreateNamespace::default())
        .await
        .unwrap();
    store
        .create_tabular_asset(
            CreateAsset {
                domain: "default".to_string(),
                namespace: "prod".to_string(),
                name: "users".to_string(),
                asset_type: "table".to_string(),
                format: Some("iceberg".to_string()),
                ..CreateAsset::default()
            },
            "s3://bucket/warehouse/prod/users",
            None,
        )
        .await
        .unwrap();
    let app = test_app(store);

    let response = create_version(&app, 1, "s3://bucket/v1.manifest").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_problem(&json, 404, "TableNotFound", "table-not-found");

    let response = get(&app, "/lance/v1/table/default$prod$users/version/list").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = post(
        &app,
        "/lance/v1/table/default$prod$users/version/describe",
        r#"{"version":1}"#,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
