#![cfg(all(feature = "iceberg", feature = "lance"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::{iceberg, lance};
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

fn iceberg_app(store: Arc<PgCatalogStore>) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = store;
    iceberg::routes()
        .layer(Extension(iceberg::IcebergConfig::default()))
        .with_state(store)
}

fn lance_app(store: Arc<PgCatalogStore>) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = store;
    lance::routes()
        .layer(Extension(lance::LanceConfig::default()))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &Arc<PgCatalogStore>, name: &str) {
    store
        .create_namespace(name, None, HashMap::new())
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_cross_format_list_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
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
            "users",
            "lance://prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let ice_app = iceberg_app(store.clone());
    let ice_response = ice_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ice_response.status(), StatusCode::OK);
    let json = body_json(ice_response).await;
    let identifiers = json["identifiers"].as_array().unwrap();
    assert_eq!(identifiers.len(), 1);
    assert_eq!(identifiers[0]["name"].as_str().unwrap(), "users");

    let lance_app = lance_app(store);
    let lance_response = lance_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/prod/table/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lance_response.status(), StatusCode::OK);
    let json = body_json(lance_response).await;
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["name"].as_str().unwrap(), "users");
}

#[tokio::test]
#[serial]
async fn test_cross_format_load_isolation() {
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

    let ice_app = iceberg_app(store);
    let response = ice_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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
async fn test_cross_format_describe_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let lance_app = lance_app(store);
    let response = lance_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/describe")
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
async fn test_cross_format_drop_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
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
            "users",
            "lance://prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let ice_app = iceberg_app(store.clone());
    let drop = ice_app
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

    let lance_exists = lance_app(store)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lance_exists.status(), StatusCode::OK);
    let json = body_json(lance_exists).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn test_cross_format_rename_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
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
            "users",
            "lance://prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let ice_app = iceberg_app(store.clone());
    let rename = ice_app
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

    let lance_exists = lance_app(store)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/prod%24users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lance_exists.status(), StatusCode::OK);
    let json = body_json(lance_exists).await;
    assert_eq!(json["exists"], true);
}

#[tokio::test]
#[serial]
async fn test_cross_format_exists_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let lance_app = lance_app(store);
    let response = lance_app
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

#[tokio::test]
#[serial]
async fn test_cross_format_commit_isolation() {
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

    let ice_app = iceberg_app(store);
    let response = ice_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"requirements": [{"type": "assert-create"}], "updates": []}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert_eq!(json["error"]["code"], 404);
}
