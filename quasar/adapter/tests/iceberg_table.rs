#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::iceberg;
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
    iceberg::routes(iceberg::IcebergConfig::default()).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store
        .create_namespace(name, AssetFormat::Iceberg, HashMap::new())
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_create_and_load_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users", "location": "s3://bucket/warehouse/prod/users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create.status(), StatusCode::OK);
    let json = body_json(create).await;
    assert!(json["metadata-location"].as_str().unwrap().contains("00001-"));
    assert!(json["metadata-location"].as_str().unwrap().ends_with(".metadata.json"));
    assert_eq!(json["metadata"]["format-version"], 2);
    assert_eq!(json["metadata"]["location"], "s3://bucket/warehouse/prod/users");

    let load = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(load.status(), StatusCode::OK);
    let json = body_json(load).await;
    assert!(json["metadata-location"].is_string());
    assert_eq!(json["metadata"]["format-version"], 2);
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = body_json(second).await;
    assert_eq!(json["error"]["type"], "TableAlreadyExistsException");
    assert_eq!(json["error"]["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_list_tables() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "orders",
            "s3://bucket/warehouse/prod/orders",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let identifiers = json["identifiers"].as_array().unwrap();
    assert_eq!(identifiers.len(), 2);
    let names: Vec<&str> = identifiers
        .iter()
        .map(|i| i["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"users"));
    assert!(names.contains(&"orders"));
}

#[tokio::test]
#[serial]
async fn test_load_table_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_drop_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let drop = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(drop.status(), StatusCode::NO_CONTENT);

    let load = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(load.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_table_exists() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let exists = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exists.status(), StatusCode::OK);

    let not_exists = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/namespaces/prod/tables/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(not_exists.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_rename_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let rename = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/tables/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"source": {"namespace": ["prod"], "name": "users"}, "destination": {"namespace": ["prod"], "name": "customers"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rename.status(), StatusCode::OK);

    let old = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old.status(), StatusCode::NOT_FOUND);

    let new = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/iceberg/v1/namespaces/prod/tables/customers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_rename_cross_namespace() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    create_namespace(&store, "staging").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/tables/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"source": {"namespace": ["prod"], "name": "users"}, "destination": {"namespace": ["staging"], "name": "users"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["error"]["type"], "BadRequestException");
}

#[tokio::test]
#[serial]
async fn test_create_table_namespace_not_found() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"]["type"], "NoSuchNamespaceException");
}
