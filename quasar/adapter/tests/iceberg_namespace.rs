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
async fn test_create_and_get_namespace() {
    let store = setup().await;
    let app = test_app(store);

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"namespace": ["prod"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create.status(), StatusCode::OK);
    let json = body_json(create).await;
    assert_eq!(json["namespace"], serde_json::json!(["prod"]));

    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let json = body_json(get).await;
    assert_eq!(json["namespace"], serde_json::json!(["prod"]));
}

#[tokio::test]
#[serial]
async fn test_create_duplicate_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"namespace": ["prod"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = body_json(second).await;
    assert_eq!(json["error"]["type"], "NamespaceAlreadyExistsException");
    assert_eq!(json["error"]["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_list_namespaces() {
    let store = setup().await;
    create_namespace(&store, "ns1").await;
    create_namespace(&store, "ns2").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces")
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
async fn test_get_namespace_not_found() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"]["type"], "NoSuchNamespaceException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_drop_namespace() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let drop = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/iceberg/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(drop.status(), StatusCode::NO_CONTENT);

    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn test_drop_non_empty_namespace() {
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

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/iceberg/v1/namespaces/prod")
                .body(Body::empty())
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
async fn test_update_namespace_properties() {
    let store = setup().await;
    let mut props = HashMap::new();
    props.insert("owner".to_string(), "team-a".to_string());
    props.insert("env".to_string(), "prod".to_string());
    store.create_namespace("prod", AssetFormat::Iceberg, props).await.unwrap();

    let app = test_app(store);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/properties")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"removals": ["env"], "updates": {"owner": "team-b", "region": "us-west"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["removed"].as_array().unwrap().contains(&"env".into()));
    assert!(json["updated"].as_array().unwrap().contains(&"owner".into()));
    assert!(json["updated"].as_array().unwrap().contains(&"region".into()));

    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let json = body_json(get).await;
    let props = json["properties"].as_object().unwrap();
    assert_eq!(props.get("owner").unwrap(), "team-b");
    assert_eq!(props.get("region").unwrap(), "us-west");
    assert!(!props.contains_key("env"));
}

#[tokio::test]
#[serial]
async fn test_format_isolation() {
    let store = setup().await;
    store.create_namespace("prod", AssetFormat::Lance, HashMap::new()).await.unwrap();
    store.create_namespace("prod", AssetFormat::Iceberg, HashMap::new()).await.unwrap();
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0], serde_json::json!(["prod"]));
}
