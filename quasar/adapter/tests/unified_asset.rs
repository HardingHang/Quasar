#![cfg(feature = "unified")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::{unified, CatalogStore};
use quasar_core::{AssetFormat, PatchField};
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
                    .create_database("quasar_test_unified_asset")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_unified_asset");
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
    store.migrate().await.expect("migration failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute(
            "TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces CASCADE",
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
    test_app_with_config(store, unified::UnifiedConfig::default())
}

fn test_app_with_config(
    store: Arc<PgCatalogStore>,
    config: unified::UnifiedConfig,
) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn quasar_core::CatalogStore> = store;
    unified::routes()
        .layer(Extension(config))
        .layer(axum::middleware::from_fn(inject_request_id))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_test_namespace(store: &Arc<PgCatalogStore>, name: &str) {
    store
        .create_namespace(name, None, HashMap::new())
        .await
        .unwrap();
}

async fn create_test_asset(store: &Arc<PgCatalogStore>, ns: &str, format: AssetFormat, name: &str) {
    store
        .create_asset(
            ns,
            format,
            name,
            &format!("s3://bucket/{}/{}", ns, name),
            Some(&format!("s3://bucket/{}/{}/metadata.json", ns, name)),
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
}

// ── List Tests ──────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_list_assets_cross_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "orders").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 2);
    assert!(assets
        .iter()
        .any(|a| a["format"] == "iceberg" && a["name"] == "users"));
    assert!(assets
        .iter()
        .any(|a| a["format"] == "lance" && a["name"] == "orders"));
}

#[tokio::test]
#[serial]
async fn test_list_assets_filter_by_format_iceberg() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "orders").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0]["format"], "iceberg");
    assert_eq!(assets[0]["name"], "users");
}

#[tokio::test]
#[serial]
async fn test_list_assets_filter_by_format_lance() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "orders").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?format=lance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0]["format"], "lance");
}

#[tokio::test]
#[serial]
async fn test_list_assets_filter_by_name() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "orders").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?name=users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0]["name"], "users");
}

#[tokio::test]
#[serial]
async fn test_list_assets_pagination() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    for i in 0..5 {
        create_test_asset(&store, "prod", AssetFormat::Iceberg, &format!("table{}", i)).await;
    }

    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?pageSize=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 2);
    let token = json["next_page_token"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&format!(
                    "/unified/v1/namespaces/prod/assets?pageSize=2&pageToken={}",
                    token
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 2);
    assert!(!json["next_page_token"].is_null());
}

#[tokio::test]
#[serial]
async fn test_list_assets_page_size_too_large() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?pageSize=1001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "PageSizeTooLarge");
}

#[tokio::test]
#[serial]
async fn test_list_assets_page_size_zero() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?pageSize=0")
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
async fn test_list_assets_empty_format_returns_invalid_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?format=")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "InvalidFormat");
}

#[tokio::test]
#[serial]
async fn test_list_assets_order_by_name_then_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?name=users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 2);
    assert_eq!(assets[0]["format"], "iceberg");
    assert_eq!(assets[1]["format"], "lance");
}

#[tokio::test]
#[serial]
async fn test_create_asset_not_allowed() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces/prod/assets")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name":"users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_eq!(content_type, "application/problem+json");
    let json = body_json(response).await;
    assert_eq!(json["code"], "MethodNotAllowed");

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = body_json(response).await;
    assert_eq!(json["assets"].as_array().unwrap().len(), 0);
}

// ── Get Tests ───────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_get_asset_detail() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["format"], "iceberg");
    assert_eq!(json["asset_type"], "table");
    assert!(json["location"].as_str().is_some());
    assert!(json["current_version"].is_null());
}

#[tokio::test]
#[serial]
async fn test_get_asset_missing_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users")
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
async fn test_get_asset_invalid_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "InvalidFormat");
}

#[tokio::test]
#[serial]
async fn test_get_asset_not_found() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/missing?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["code"], "AssetNotFound");
}

// ── Delete Tests ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_delete_asset() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
#[serial]
async fn test_delete_asset_not_found() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/unified/v1/namespaces/prod/assets/missing?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["code"], "AssetNotFound");
}

// ── Patch Tests ─────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_patch_asset_comment() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"comment": "updated comment"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["comment"], "updated comment");
}

#[tokio::test]
#[serial]
async fn test_patch_asset_comment_null() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    store
        .update_asset_properties(
            "prod",
            AssetFormat::Iceberg,
            "users",
            PatchField::Value("before".to_string()),
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
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
async fn test_patch_asset_comment_missing_keeps_existing() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    store
        .update_asset_properties(
            "prod",
            AssetFormat::Iceberg,
            "users",
            PatchField::Value("before".to_string()),
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"updates": {"owner": "platform"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["comment"], "before");
    assert_eq!(json["properties"]["owner"], "platform");
}

#[tokio::test]
#[serial]
async fn test_patch_asset_properties() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"removals": ["old_key"], "updates": {"new_key": "new_value"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["properties"]["old_key"].is_null());
    assert_eq!(json["properties"]["new_key"], "new_value");
}

// ── Rename Tests ────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_rename_asset() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces/prod/assets/users/rename?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "customers"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify old name no longer exists
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // Verify new name exists
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/customers?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "customers");
}

#[tokio::test]
#[serial]
async fn test_rename_asset_to_existing_name() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "customers").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces/prod/assets/users/rename?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "customers"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["code"], "AssetAlreadyExists");
}

#[tokio::test]
#[serial]
async fn test_rename_asset_invalid_new_name() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/namespaces/prod/assets/users/rename?format=iceberg")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "bad name"}"#))
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
async fn test_list_assets_invalid_format() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets?format=parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "InvalidFormat");
}

// ── Version Tests ───────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_get_lance_asset_no_version() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "items").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/items?format=lance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "items");
    assert_eq!(json["format"], "lance");
    assert!(json["current_version"].is_null());
}

#[tokio::test]
#[serial]
async fn test_get_lance_asset_with_version() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Lance, "items").await;

    store
        .create_version(
            "prod",
            AssetFormat::Lance,
            "items",
            1,
            "s3://bucket/v1.manifest".to_string(),
            None,
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/items?format=lance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "items");
    assert_eq!(json["format"], "lance");
    let cv = json["current_version"].as_object().unwrap();
    assert_eq!(cv.get("format").unwrap(), "lance");
    assert_eq!(cv.get("version_id").unwrap(), 1);
    assert_eq!(
        cv.get("metadata_location").unwrap(),
        "s3://bucket/v1.manifest"
    );
    assert!(cv.get("previous_version_id").unwrap().is_null());
}

#[tokio::test]
#[serial]
async fn test_get_iceberg_asset_no_metadata() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["current_version"].is_null());
}

#[cfg(feature = "iceberg")]
#[tokio::test]
#[serial]
async fn test_get_iceberg_asset_with_metadata() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let mem_store =
        Arc::new(object_store::memory::InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "uuid",
        "location": "s3://bucket/prod/users",
        "last-sequence-number": 1,
        "current-snapshot-id": 123,
        "snapshots": [
            {
                "snapshot-id": 123,
                "timestamp-ms": 1700000000000i64
            }
        ]
    });

    let path = object_store::path::Path::from("prod/users/metadata.json");
    let payload = object_store::PutPayload::from(serde_json::to_vec(&metadata).unwrap());
    mem_store.put(&path, payload).await.unwrap();

    let mut config = unified::UnifiedConfig::default();
    config.object_store = Some(mem_store);
    config.s3_bucket = Some("bucket".to_string());

    let app = test_app_with_config(store, config);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let cv = json["current_version"].as_object().unwrap();
    assert_eq!(cv.get("format").unwrap(), "iceberg");
    assert_eq!(cv.get("sequence_number").unwrap(), 1);
    assert_eq!(cv.get("snapshot_id").unwrap(), 123);
    assert_eq!(cv.get("timestamp_ms").unwrap(), 1700000000000i64);
}

#[cfg(feature = "iceberg")]
#[tokio::test]
#[serial]
async fn test_get_iceberg_asset_no_snapshot() {
    let store = setup().await;
    create_test_namespace(&store, "prod").await;
    create_test_asset(&store, "prod", AssetFormat::Iceberg, "users").await;

    let mem_store =
        Arc::new(object_store::memory::InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let metadata = serde_json::json!({
        "format-version": 2,
        "last-sequence-number": 0,
    });

    let path = object_store::path::Path::from("prod/users/metadata.json");
    let payload = object_store::PutPayload::from(serde_json::to_vec(&metadata).unwrap());
    mem_store.put(&path, payload).await.unwrap();

    let mut config = unified::UnifiedConfig::default();
    config.object_store = Some(mem_store);
    config.s3_bucket = Some("bucket".to_string());

    let app = test_app_with_config(store, config);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets/users?format=iceberg")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let cv = json["current_version"].as_object().unwrap();
    assert_eq!(cv.get("sequence_number").unwrap(), 0);
    assert!(cv.get("snapshot_id").unwrap().is_null());
    assert!(cv.get("timestamp_ms").unwrap().is_null());
}
