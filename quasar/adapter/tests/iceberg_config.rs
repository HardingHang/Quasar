#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::iceberg;
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use serial_test::serial;
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
    store.initialize().await.expect("initialize failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute("TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions CASCADE", &[])
        .await
        .expect("failed to truncate tables");

    store
}

fn test_app(store: PgCatalogStore) -> axum::Router {
    use axum::Extension;
    let store: std::sync::Arc<dyn quasar_core::CatalogStore> = std::sync::Arc::new(store);
    let config = iceberg::IcebergConfig {
        default_warehouse: "s3://bucket/prod".to_string(),
        ..Default::default()
    };
    iceberg::routes().layer(Extension(config)).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
#[serial]
async fn test_get_config() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["defaults"].is_object());
    assert!(json["overrides"].is_object());
    assert!(json["endpoints"].is_array());
    assert!(!json["endpoints"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[serial]
async fn test_get_config_with_warehouse() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/config?warehouse=s3://bucket/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["overrides"]["warehouse"], "s3://bucket/prod");
    assert!(json["endpoints"].is_array());
}

#[tokio::test]
#[serial]
async fn test_get_config_endpoints_field() {
    let store = setup().await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let endpoints = json["endpoints"].as_array().unwrap();
    assert!(!endpoints.is_empty());

    // Verify key endpoints are present
    let endpoint_strings: Vec<String> = endpoints
        .iter()
        .map(|e| e.as_str().unwrap().to_string())
        .collect();
    assert!(endpoint_strings.contains(&"GET /v1/config".to_string()));
    assert!(endpoint_strings.contains(&"GET /v1/{prefix}/namespaces".to_string()));
    assert!(
        endpoint_strings.contains(&"POST /v1/{prefix}/namespaces/{namespace}/tables".to_string())
    );
    assert!(
        endpoint_strings.contains(&"POST /v1/{prefix}/namespaces/{namespace}/register".to_string())
    );
}
