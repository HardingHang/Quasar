#![cfg(feature = "lance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::lance;
use quasar_core::{AssetFormat, CatalogStore};
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
                postgresql
                    .setup()
                    .await
                    .expect("PostgreSQL setup failed");
                postgresql
                    .start()
                    .await
                    .expect("PostgreSQL start failed");
                postgresql
                    .create_database("quasar_test")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test");
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

async fn setup() -> PgCatalogStore {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let store = PgCatalogStore::new(pool.clone());
    store.migrate().await.expect("migration failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute("TRUNCATE asset_versions, assets, namespaces CASCADE", &[])
        .await
        .expect("failed to truncate tables");

    store
}

fn test_app(store: PgCatalogStore) -> axum::Router {
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    lance::routes(lance::LanceConfig::default()).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
#[serial]
async fn test_create_and_describe_namespace() {
    let app = test_app(setup().await);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"properties": {"team": "data"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");
    assert!(json["id"].as_str().is_some());

    let describe = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(describe.status(), StatusCode::OK);
    let json = body_json(describe).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_returns_409() {
    let app = test_app(setup().await);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/create")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = body_json(second).await;
    assert_eq!(json["error"], "NamespaceAlreadyExists");
    assert_eq!(json["code"], 409);
    assert!(json["detail"].as_str().unwrap().contains("prod"));
    assert_eq!(json["instance"], "/lance/v1/namespace/prod/create");
}

#[tokio::test]
#[serial]
async fn test_list_namespaces() {
    let store = setup().await;
    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    store
        .create_namespace("ns2", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    store
        .create_namespace("ice_ns", AssetFormat::Iceberg, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/%24/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);

    let names: Vec<&str> = namespaces
        .iter()
        .map(|n| n["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"ns1"));
    assert!(names.contains(&"ns2"));
}

#[tokio::test]
#[serial]
async fn test_namespace_exists() {
    let store = setup().await;
    store
        .create_namespace("prod", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let exists = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exists.status(), StatusCode::OK);
    let json = body_json(exists).await;
    assert_eq!(json["exists"], true);

    let not_exists = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/missing/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(not_exists.status(), StatusCode::OK);
    let json = body_json(not_exists).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn test_drop_namespace() {
    let store = setup().await;
    store
        .create_namespace("prod", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let drop = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(drop.status(), StatusCode::OK);

    let again = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/prod/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(again.status(), StatusCode::NOT_FOUND);
    let json = body_json(again).await;
    assert_eq!(json["error"], "NamespaceNotFound");
    assert_eq!(json["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_describe_not_found() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/namespace/missing/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"], "NamespaceNotFound");
    assert_eq!(json["code"], 404);
    assert_eq!(json["instance"], "/lance/v1/namespace/missing/describe");
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_empty() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/%24/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert!(namespaces.is_empty());
    assert!(json["next_page_token"].is_null());
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_pagination_offset_beyond_total() {
    let store = setup().await;
    store
        .create_namespace("ns1", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/%24/list?page_token=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert!(namespaces.is_empty());
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_pagination_with_limit() {
    let store = setup().await;
    for i in 1..=5 {
        store
            .create_namespace(
                &format!("ns{}", i),
                AssetFormat::Lance,
                HashMap::new(),
            )
            .await
            .unwrap();
    }

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/%24/list?limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_format_isolation() {
    let store = setup().await;
    store
        .create_namespace("lance_ns", AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
    store
        .create_namespace("iceberg_ns", AssetFormat::Iceberg, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/%24/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0]["name"], "lance_ns");
}
