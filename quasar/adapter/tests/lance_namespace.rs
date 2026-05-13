#![cfg(feature = "lance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::lance;
use quasar_core::{DomainStore, NamespaceStore};
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tower::ServiceExt;

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

struct PgInstance {
    #[allow(dead_code)]
    postgresql: PostgreSQL,
    url: String,
}

impl PgInstance {
    async fn get() -> &'static Self {
        PG_INSTANCE
            .get_or_init(|| async {
                let mut postgresql = PostgreSQL::default();
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

async fn setup() -> Arc<PgCatalogStore> {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    store.initialize().await.expect("initialize failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute(
            "TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions CASCADE",
            &[],
        )
        .await
        .expect("failed to truncate tables");
    // Domains are not truncated above (they hold the seeded "default" domain).
    // Remove every non-seeded domain so tests that create additional domains
    // can start from a clean state.
    client
        .execute("DELETE FROM domains WHERE name <> 'default'", &[])
        .await
        .expect("failed to clear non-default domains");

    store
}

fn test_app(store: Arc<PgCatalogStore>) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn quasar_core::CatalogStore> = store;
    lance::routes()
        .layer(Extension(lance::LanceConfig::default()))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
#[serial]
async fn test_create_and_describe_namespace() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"properties":{"team":"data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_returns_409() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["error"], "NamespaceAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_list_namespaces() {
    let store = setup().await;
    store
        .create_namespace("default", "dev", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/default/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert!(namespaces.iter().any(|ns| ns["name"] == "dev"));
    assert!(namespaces.iter().any(|ns| ns["name"] == "prod"));
}

#[tokio::test]
#[serial]
async fn test_namespace_exists() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], true);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$missing/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn test_drop_namespace() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$prod/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_describe_not_found() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/default$missing/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"], "NamespaceNotFound");
    assert_eq!(json["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_pagination_with_limit() {
    let store = setup().await;
    for name in ["dev", "prod", "staging"] {
        store
            .create_namespace("default", name, None, HashMap::new())
            .await
            .unwrap();
    }

    let app = test_app(store);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/default/list?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert_eq!(json["next_page_token"], "2");
}

#[tokio::test]
#[serial]
async fn test_root_list_returns_domains() {
    let store = setup().await;
    store
        .create_domain(
            "prod",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/$/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    let names: Vec<&str> = namespaces
        .iter()
        .filter_map(|n| n["name"].as_str())
        .collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"prod"));
}

#[tokio::test]
#[serial]
async fn test_single_segment_id_rejected_on_create() {
    let store = setup().await;
    let app = test_app(store);

    // A single-segment id targets a Domain; V3 forbids Lance from creating
    // Domains. Expect 400 InvalidInput.
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["error"], "InvalidInput");
}

#[tokio::test]
#[serial]
async fn test_namespace_in_non_default_domain() {
    let store = setup().await;
    store
        .create_domain(
            "prod",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap();

    let app = test_app(store);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod$analytics/create")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"properties":{"region":"us"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod$analytics/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "analytics");
    assert_eq!(json["properties"]["region"], "us");
}
