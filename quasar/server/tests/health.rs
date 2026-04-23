#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_server::create_app;
use quasar_storage::PgCatalogStore;
use serde_json::Value;
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

                let pool = test_pool(&url);
                let store = PgCatalogStore::new(pool.clone());
                store.migrate().await.expect("migration failed");

                let client = pool.get().await.expect("failed to get client");
                client
                    .execute("TRUNCATE asset_versions, assets, namespaces CASCADE", &[])
                    .await
                    .expect("failed to truncate tables");

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

async fn setup() -> Pool {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);

    let client = pool.get().await.expect("failed to get client");
    client
        .execute("TRUNCATE asset_versions, assets, namespaces CASCADE", &[])
        .await
        .expect("failed to truncate tables");

    pool
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn test_healthz() {
    let pool = setup().await;
    let app = create_app(pool);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn test_readyz() {
    let pool = setup().await;
    let app = create_app(pool);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["status"], "ready");
    assert_eq!(json["checks"]["database"], "ok");
}

#[tokio::test]
#[cfg(feature = "lance")]
async fn test_lance_routes_still_work() {
    let pool = setup().await;
    let app = create_app(pool);

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
    assert!(namespaces.is_empty());
}

#[tokio::test]
#[cfg(feature = "iceberg")]
async fn test_iceberg_routes_are_mounted() {
    let pool = setup().await;
    let app = create_app(pool);

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
}
