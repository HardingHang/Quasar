#![cfg(feature = "unified")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::unified;
use quasar_core::{NamespaceStore, TabularStore};
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
                    .create_database("quasar_test_unified")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_unified");
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

    store
}

async fn inject_request_id(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    req.extensions_mut().insert(request_id.clone());
    let mut response = next.run(req).await;
    if let Ok(val) = request_id.parse() {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}

fn test_app(store: Arc<PgCatalogStore>) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn quasar_core::CatalogStore> = store;
    unified::routes()
        .layer(Extension(unified::UnifiedConfig::default()))
        .layer(axum::middleware::from_fn(inject_request_id))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
#[serial]
async fn test_create_namespace() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "prod", "comment": "production", "properties": {"team": "data"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["comment"], "production");
    assert_eq!(json["properties"]["team"], "data");
    assert!(json["id"].as_str().is_some());
}

#[tokio::test]
#[serial]
async fn test_list_namespaces() {
    let store = setup().await;
    store
        .create_namespace(
            "default",
            "prod",
            Some("production".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_namespace("default", "dev", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert!(json["next_page_token"].is_null());
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_pagination() {
    let store = setup().await;
    for i in 0..5 {
        store
            .create_namespace("default", &format!("ns{}", i), None, HashMap::new())
            .await
            .unwrap();
    }

    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces?pageSize=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
    let token = json["next_page_token"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/unified/v1/namespaces?pageSize=2&pageToken={}",
                    token
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 2);
    assert!(!json["next_page_token"].is_null());
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_page_size_too_large() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces?pageSize=1001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "PageSizeTooLarge");
    assert_eq!(json["status"], 400);
}

#[tokio::test]
#[serial]
async fn test_list_namespaces_page_size_zero() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces?pageSize=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "InvalidInput");
}

#[tokio::test]
#[serial]
async fn test_get_namespace() {
    let store = setup().await;
    store
        .create_namespace(
            "default",
            "prod",
            Some("production".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["comment"], "production");
}

#[tokio::test]
#[serial]
async fn test_get_namespace_not_found() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["code"], "NamespaceNotFound");
    assert_eq!(json["status"], 404);
    assert!(json["type"]
        .as_str()
        .unwrap()
        .contains("namespace-not-found"));
}

#[tokio::test]
#[serial]
async fn test_delete_namespace() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/unified/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
#[serial]
async fn test_delete_non_empty_namespace() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/unified/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["code"], "NamespaceNotEmpty");
    assert_eq!(json["status"], 409);
}

#[tokio::test]
#[serial]
async fn test_duplicate_create_returns_409() {
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
                .uri("/unified/v1/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "prod"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["code"], "NamespaceAlreadyExists");
    assert_eq!(json["status"], 409);
}

#[tokio::test]
#[serial]
async fn test_create_namespace_invalid_name() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "bad name"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "InvalidInput");
}

#[tokio::test]
#[serial]
async fn test_patch_comment_value() {
    let store = setup().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"comment": "updated"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["comment"], "updated");
}

#[tokio::test]
#[serial]
async fn test_patch_comment_null() {
    let store = setup().await;
    store
        .create_namespace(
            "default",
            "prod",
            Some("before".to_string()),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"comment": null}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["comment"].is_null());
}

#[tokio::test]
#[serial]
async fn test_patch_properties() {
    let store = setup().await;
    let mut props = HashMap::new();
    props.insert("team".to_string(), "data".to_string());
    props.insert("env".to_string(), "prod".to_string());
    store
        .create_namespace("default", "prod", None, props)
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"removals": ["team"], "updates": {"owner": "platform"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["properties"]["team"].is_null());
    assert_eq!(json["properties"]["env"], "prod");
    assert_eq!(json["properties"]["owner"], "platform");
}

#[tokio::test]
#[serial]
async fn test_problem_details_content_type() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(content_type, "application/problem+json");
}

#[tokio::test]
#[serial]
async fn test_request_id_propagation() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/missing")
                .header("X-Request-Id", "test-req-42")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let req_id_header = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(req_id_header, "test-req-42");

    let json = body_json(response).await;
    assert_eq!(json["request_id"], "test-req-42");
}

#[tokio::test]
#[serial]
async fn test_generated_request_id_is_uuid() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let req_id_header = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    uuid::Uuid::parse_str(&req_id_header).unwrap();

    let json = body_json(response).await;
    assert_eq!(json["request_id"], req_id_header);
}
