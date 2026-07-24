//! Server integration tests — shared embedded PostgreSQL bootstrap.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Request};
use axum::response::Response;
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::{PostgreSQL, Settings};
use serde_json::Value;
use tower::ServiceExt;

pub const ZONKY_RELEASES_URL: &str = "https://github.com/zonkyio/embedded-postgres-binaries";

pub async fn start_pg(db_name: &str) -> (PostgreSQL, String) {
    let settings = Settings {
        releases_url: ZONKY_RELEASES_URL.to_string(),
        ..Default::default()
    };
    let mut postgresql = PostgreSQL::new(settings);
    postgresql.setup().await.expect("PostgreSQL setup failed");
    postgresql.start().await.expect("PostgreSQL start failed");
    postgresql
        .create_database(db_name)
        .await
        .expect("create database failed");
    let url = postgresql.settings().url(db_name);
    (postgresql, url)
}

pub fn test_pool(url: &str) -> Pool {
    let config = url
        .parse::<tokio_postgres::Config>()
        .expect("invalid database URL");
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool")
}

/// Reset the database and run migrations; returns a pool ready for
/// `create_app` (which builds its own store from the pool).
pub async fn fresh_pool(url: &str) -> Pool {
    let pool = test_pool(url);
    {
        let client = pool.get().await.expect("pool checkout failed");
        client
            .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .await
            .expect("schema reset failed");
    }
    let store = quasar_storage::PgCatalogStore::new(pool.clone());
    store.initialize().await.expect("initialize failed");
    pool
}

/// App with an in-memory object store wired for Iceberg (s3://bucket).
pub fn test_app(pool: Pool) -> axum::Router {
    quasar_server::create_app_with_config(
        pool,
        quasar_server::AppConfig {
            iceberg: quasar_adapter::iceberg::IcebergConfig {
                warehouse_path: Some("s3://bucket".to_string()),
                object_store: Some(std::sync::Arc::new(object_store::memory::InMemory::new())),
                s3_bucket: Some("bucket".to_string()),
                default_warehouse: "warehouse".to_string(),
            },
            ..Default::default()
        },
    )
}

pub async fn body_json(response: Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

pub async fn request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<String>,
) -> Response {
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

pub async fn get(app: &axum::Router, uri: &str) -> Response {
    request(app, "GET", uri, None).await
}

pub async fn post(app: &axum::Router, uri: &str, body: &str) -> Response {
    request(app, "POST", uri, Some(body.to_string())).await
}

pub async fn delete(app: &axum::Router, uri: &str) -> Response {
    request(app, "DELETE", uri, None).await
}
