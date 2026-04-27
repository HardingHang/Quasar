#![cfg(feature = "lance")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

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
    store.migrate().await.expect("migration failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute("TRUNCATE asset_versions, assets, namespaces CASCADE", &[])
        .await
        .expect("failed to truncate tables");

    store
}

fn test_app(store: PgCatalogStore) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    lance::routes()
        .layer(Extension(lance::LanceConfig::default()))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store.create_namespace(name, HashMap::new()).await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_declare_and_describe_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let declare = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"options": {"mode": "create"}}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(declare.status(), StatusCode::OK);
    let json = body_json(declare).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["location"], "lance://prod/users");

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
    assert_eq!(json["location"], "lance://prod/users");
    assert!(json["current_version"].is_null());
}

#[tokio::test]
#[serial]
async fn test_declare_duplicate_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let second = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(second.status(), StatusCode::CONFLICT);
    let json = body_json(second).await;
    assert_eq!(json["error"], "TableAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_list_tables() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "t1",
            "lance://prod/t1",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "t2",
            "lance://prod/t2",
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
                .method("GET")
                .uri("/lance/v1/namespace/prod/table/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);

    let names: Vec<&str> = tables.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"t1"));
    assert!(names.contains(&"t2"));
}

#[tokio::test]
#[serial]
async fn test_table_exists() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
            None,
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
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exists.status(), StatusCode::OK);
    let json = body_json(exists).await;
    assert_eq!(json["exists"], true);

    let not_exists = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24missing/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(not_exists.status(), StatusCode::OK);
    let json = body_json(not_exists).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn test_register_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/register")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"location": "s3://bucket/warehouse/prod/users"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "users");
    assert_eq!(json["location"], "s3://bucket/warehouse/prod/users");
}

#[tokio::test]
#[serial]
async fn test_deregister_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let deregister = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/deregister")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(deregister.status(), StatusCode::OK);

    let again = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/deregister")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(again.status(), StatusCode::NOT_FOUND);
    let json = body_json(again).await;
    assert_eq!(json["error"], "TableNotFound");
    assert_eq!(json["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_drop_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
            None,
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
                .method("POST")
                .uri("/lance/v1/table/prod%24users/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(drop.status(), StatusCode::OK);

    let exists = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exists.status(), StatusCode::OK);
    let json = body_json(exists).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn test_rename_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
            None,
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
                .uri("/lance/v1/table/prod%24users/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "customers"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(rename.status(), StatusCode::OK);

    let old_exists = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old_exists.status(), StatusCode::OK);
    let json = body_json(old_exists).await;
    assert_eq!(json["exists"], false);

    let new_exists = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24customers/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new_exists.status(), StatusCode::OK);
    let json = body_json(new_exists).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn test_describe_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24missing/describe")
                .body(Body::empty())
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
async fn test_declare_namespace_not_found() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{}"#))
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
async fn test_register_duplicate_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
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
                .method("POST")
                .uri("/lance/v1/table/prod%24users/register")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"location": "s3://bucket/warehouse/prod/users"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["error"], "TableAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_rename_to_existing_name_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "users",
            "lance://prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    store
        .create_asset(
            "prod",
            AssetFormat::Lance,
            "customers",
            "lance://prod/customers",
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
                .method("POST")
                .uri("/lance/v1/table/prod%24users/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "customers"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let json = body_json(response).await;
    assert_eq!(json["error"], "TableAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_drop_table_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24missing/drop")
                .body(Body::empty())
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
async fn test_rename_table_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24missing/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_name": "newname"}"#))
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
async fn test_list_tables_empty_namespace() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/prod/table/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let tables = json["tables"].as_array().unwrap();
    assert!(tables.is_empty());
    assert!(json["next_page_token"].is_null());
}

#[tokio::test]
#[serial]
async fn test_exists_table_not_found_in_existing_namespace() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24missing/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}

#[tokio::test]
#[serial]
async fn test_exists_namespace_not_found_returns_false() {
    let app = test_app(setup().await);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}
