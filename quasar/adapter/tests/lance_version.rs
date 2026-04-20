#![allow(clippy::unwrap_used)]

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
    lance::routes().with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store
        .create_namespace(name, AssetFormat::Lance, HashMap::new())
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_create_and_describe_version() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset("prod", AssetFormat::Lance, "users", HashMap::new())
        .await
        .unwrap();
    let app = test_app(store);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/create")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"version": 1, "manifest_path": "s3://bucket/warehouse/prod/users/_versions/1.manifest"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create.status(), StatusCode::OK);
    let json = body_json(create).await;
    assert_eq!(json["version"], 1);
    assert_eq!(
        json["manifest_path"],
        "s3://bucket/warehouse/prod/users/_versions/1.manifest"
    );

    let describe = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/describe")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"version": 1}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(describe.status(), StatusCode::OK);
    let json = body_json(describe).await;
    assert_eq!(json["version"], 1);
    assert_eq!(
        json["manifest_path"],
        "s3://bucket/warehouse/prod/users/_versions/1.manifest"
    );
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset("prod", AssetFormat::Lance, "users", HashMap::new())
        .await
        .unwrap();
    let app = test_app(store);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/create")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"version": 1, "manifest_path": "s3://bucket/v1.manifest"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/create")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"version": 1, "manifest_path": "s3://bucket/v1.manifest"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = body_json(second).await;
    assert_eq!(json["error"], "TableVersionAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_list_versions() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset("prod", AssetFormat::Lance, "users", HashMap::new())
        .await
        .unwrap();
    store
        .create_version(
            "prod",
            AssetFormat::Lance,
            "users",
            1,
            "s3://bucket/v1.manifest".to_string(),
        )
        .await
        .unwrap();
    store
        .create_version(
            "prod",
            AssetFormat::Lance,
            "users",
            2,
            "s3://bucket/v2.manifest".to_string(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/table/prod%24users/version/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let versions = json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(versions[0], 1);
    assert_eq!(versions[1], 2);
}

#[tokio::test]
#[serial]
async fn test_describe_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset("prod", AssetFormat::Lance, "users", HashMap::new())
        .await
        .unwrap();
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/describe")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"version": 99}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"], "TableNotFound");
    assert_eq!(json["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_create_table_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/version/create")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"version": 1, "manifest_path": "s3://bucket/v1.manifest"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"], "TableNotFound");
    assert_eq!(json["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_describe_current_version_in_describe_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset("prod", AssetFormat::Lance, "users", HashMap::new())
        .await
        .unwrap();
    store
        .create_version(
            "prod",
            AssetFormat::Lance,
            "users",
            1,
            "s3://bucket/v1.manifest".to_string(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let describe = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(describe.status(), StatusCode::OK);
    let json = body_json(describe).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["current_version"], 1);
}
