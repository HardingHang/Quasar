#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::iceberg;
use quasar_core::{CatalogStore, NamespaceStore, TabularStore};
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
        .execute(
            "TRUNCATE iceberg_staged_tables, iceberg_scan_metrics_reports, iceberg_purge_operations, tabular_asset_versions, asset_versions, tabular_assets, view_assets, assets, namespaces, asset_permissions CASCADE",
            &[],
        )
        .await
        .expect("failed to truncate tables");

    store
}

fn test_app(store: PgCatalogStore) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    let config = iceberg::IcebergConfig {
        default_warehouse: "default".to_string(),
        ..Default::default()
    };
    iceberg::routes().layer(Extension(config)).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store
        .create_namespace("default", name, None, HashMap::new())
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_create_and_load_view() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "daily_sales", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "sql": "SELECT 1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        create.status(),
        StatusCode::OK,
        "create failed: {:?}",
        body_json(create).await
    );
    let json = body_json(create).await;
    assert!(json["metadata-location"]
        .as_str()
        .unwrap()
        .contains("00001-"));
    assert!(json["metadata-location"]
        .as_str()
        .unwrap()
        .ends_with(".metadata.json"));
    assert_eq!(json["metadata"]["format-version"], 1);

    let load = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views/daily_sales")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    if load.status() != StatusCode::OK {
        let err_json = body_json(load).await;
        panic!(
            "load failed with {:?}: {:?}",
            err_json["error"]["code"], err_json["error"]["message"]
        );
    }
    assert_eq!(load.status(), StatusCode::OK);
    let json = body_json(load).await;
    assert!(json["metadata-location"].is_string());
    assert_eq!(json["metadata"]["format-version"], 1);
}

#[tokio::test]
#[serial]
async fn test_create_view_same_name_table_conflict() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create a table first
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Try to create a view with the same name
    let create = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "users", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "sql": "SELECT 1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create.status(), StatusCode::CONFLICT);
}

#[tokio::test]
#[serial]
async fn test_load_view_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let load = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(load.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_list_views() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create two views
    for name in ["view_a", "view_b"] {
        let req = Request::builder()
            .method("POST")
            .uri("/iceberg/v1/default/namespaces/prod/views")
            .header("Content-Type", "application/json")
            .body(Body::from(format!(
                r#"{{"name": "{}", "schema": {{"type": "struct", "schema-id": 0, "fields": []}}, "sql": "SELECT 1"}}"#,
                name
            )))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let list = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list.status(), StatusCode::OK);
    let json = body_json(list).await;
    let identifiers = json["identifiers"].as_array().unwrap();
    assert_eq!(identifiers.len(), 2);
}

#[tokio::test]
#[serial]
async fn test_drop_view() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create a view
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "to_drop", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "sql": "SELECT 1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);

    // Drop the view
    let drop = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/iceberg/v1/default/namespaces/prod/views/to_drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(drop.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let load = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views/to_drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(load.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_head_view() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create a view
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "head_test", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "sql": "SELECT 1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);

    // Head exists
    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/default/namespaces/prod/views/head_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);

    // Head not found
    let head_missing = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/default/namespaces/prod/views/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_missing.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_rename_view() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create a view
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/views")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "old_name", "schema": {"type": "struct", "schema-id": 0, "fields": []}, "sql": "SELECT 1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);

    // Rename
    let rename = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/views/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"source": {"namespace": ["prod"], "name": "old_name"}, "destination": {"namespace": ["prod"], "name": "new_name"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rename.status(), StatusCode::NO_CONTENT);

    // Old name should not exist
    let old = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views/old_name")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.status(), StatusCode::NOT_FOUND);

    // New name should exist
    let new = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/views/new_name")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new.status(), StatusCode::OK);
}
