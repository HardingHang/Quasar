//! Iceberg REST Catalog — shared test bootstrap for embedded PostgreSQL.
//!
//! Each iceberg test binary gets its own embedded PG instance and database;
//! tests reset the schema and re-run migrations per case. This module is
//! included via `mod common;` from each iceberg test file.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::{PostgreSQL, Settings};
use quasar_adapter::iceberg::{self, IcebergConfig};
use quasar_core::{CatalogStore, IcebergCatalogStore};
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

pub const ZONKY_RELEASES_URL: &str = "https://github.com/zonkyio/embedded-postgres-binaries";

pub struct PgBootstrap;

impl PgBootstrap {
    pub async fn start(db_name: &str) -> (PostgreSQL, String) {
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

/// Reset to a clean, fully-migrated state.
pub async fn fresh_store(url: &str) -> Arc<PgCatalogStore> {
    let pool = test_pool(url);
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

/// App with an in-memory object store wired to `s3://bucket`.
pub fn test_app(store: &Arc<PgCatalogStore>) -> (axum::Router, Arc<dyn object_store::ObjectStore>) {
    let memory: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let iceberg_store: Arc<dyn IcebergCatalogStore> = store.clone();
    let _catalog: Arc<dyn CatalogStore> = store.clone();
    let app = iceberg::routes()
        .layer(axum::Extension(IcebergConfig {
            warehouse_path: Some("s3://bucket".to_string()),
            object_store: Some(memory.clone()),
            s3_bucket: Some("bucket".to_string()),
            default_warehouse: "warehouse".to_string(),
        }))
        .with_state(iceberg_store);
    (app, memory)
}

// ── HTTP helpers ────────────────────────────────────────────

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

pub async fn head(app: &axum::Router, uri: &str) -> Response {
    request(app, "HEAD", uri, None).await
}

/// Assert an Iceberg error body `{"error": {"message","type","code"}}`.
pub async fn assert_iceberg_error(
    response: Response,
    status: StatusCode,
    error_type: &str,
) -> Value {
    assert_eq!(response.status(), status);
    let body = body_json(response).await;
    assert_eq!(body["error"]["type"], error_type);
    assert_eq!(body["error"]["code"], status.as_u16());
    body
}

pub const NS_PREFIX: &str = "/iceberg/v1/default";

/// Create a namespace via the protocol; `segments` is the hierarchical path.
pub async fn create_namespace(app: &axum::Router, segments: &[&str]) -> Value {
    let ns: Vec<Value> = segments
        .iter()
        .map(|s| Value::String(s.to_string()))
        .collect();
    let resp = post(
        app,
        &format!("{NS_PREFIX}/namespaces"),
        &serde_json::json!({"namespace": ns, "properties": {}}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    body_json(resp).await
}

/// Create a table via the protocol (non-staged), returning the response body.
pub async fn create_table(app: &axum::Router, ns_path: &str, name: &str) -> Value {
    let resp = post(
        app,
        &format!("{NS_PREFIX}/namespaces/{ns_path}/tables"),
        &serde_json::json!({
            "name": name,
            "location": format!("s3://bucket/{ns_path}/{name}"),
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": true}
                ]
            },
            "properties": {}
        })
        .to_string(),
    )
    .await;
    if resp.status() != StatusCode::OK {
        let status = resp.status();
        let body = body_json(resp).await;
        panic!("create_table failed with {status}: {body}");
    }
    body_json(resp).await
}
