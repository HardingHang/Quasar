#![cfg(all(feature = "iceberg", feature = "lance"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::{iceberg, lance};
use quasar_core::CatalogStore;
use quasar_core::{NamespaceStore, TabularStore};
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
    store.initialize().await.expect("initialize failed");

    let client = pool.get().await.expect("failed to get client");
    client
        .execute(
            "TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions CASCADE",
            &[],
        )
        .await
        .expect("failed to truncate tables");

    store
}

fn iceberg_app(store: Arc<PgCatalogStore>) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = store;
    let config = iceberg::IcebergConfig {
        default_warehouse: "default".to_string(),
        ..Default::default()
    };
    iceberg::routes().layer(Extension(config)).with_state(store)
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
        .create_namespace("default", name, None, HashMap::new())
        .await
        .unwrap();
}

// V3 schema invariant: `uq_assets_active_name(namespace_id, name)
// WHERE deleted_at IS NULL` makes active asset names unique within a
// namespace regardless of format (`docs/v3/V3_DESIGN.md` §3.3.4 and
// §11.2). The three tests below verify that the protocol adapters
// surface this invariant correctly:
//   - same-name across formats on create → 409
//   - drop / rename through the wrong protocol → 404
// The Phase 2 same-name fixtures that violated the invariant are gone.

#[tokio::test]
#[serial]
async fn test_cross_format_create_conflicts() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let ice_app = iceberg_app(store.clone());
    let create_iceberg = ice_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "users", "location": "s3://bucket/warehouse/prod/users"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_iceberg.status(), StatusCode::OK);

    // Lance declare with the same `domain$namespace$table` id must reject
    // because the V3 active-name index is on (namespace_id, name) WHERE
    // deleted_at IS NULL — format is not part of the uniqueness key.
    let lance_app = lance_app(store);
    let create_lance = lance_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default%24prod%24users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_lance.status(), StatusCode::CONFLICT);
    let json = body_json(create_lance).await;
    assert_eq!(json["error"], "TableAlreadyExists");
    assert_eq!(json["code"], 409);
}

#[tokio::test]
#[serial]
async fn test_cross_format_load_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "lance",
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
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
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
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
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
                .uri("/lance/v1/table/default%24prod%24users/describe")
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
async fn test_lance_drop_cannot_target_iceberg_asset() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let lance_app = lance_app(store.clone());
    let drop = lance_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default%24prod%24users/drop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(drop.status(), StatusCode::NOT_FOUND);
    let json = body_json(drop).await;
    assert_eq!(json["error"], "TableNotFound");

    // Iceberg asset is still intact.
    let ice_app = iceberg_app(store);
    let load = ice_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(load.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_lance_rename_cannot_target_iceberg_asset() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let lance_app = lance_app(store.clone());
    let rename = lance_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default%24prod%24users/rename")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"new_table_name":"customers"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rename.status(), StatusCode::NOT_FOUND);
    let json = body_json(rename).await;
    assert_eq!(json["error"], "TableNotFound");

    // Iceberg asset is unchanged.
    let ice_app = iceberg_app(store);
    let load = ice_app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(load.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_cross_format_exists_isolation() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
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
                .uri("/lance/v1/table/default%24prod%24users/exists")
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
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "lance",
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
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
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

#[tokio::test]
#[serial]
async fn test_standard_protocol_errors_do_not_use_unified_problem_details() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let ice_response = iceberg_app(store.clone())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables/missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ice_response.status(), StatusCode::NOT_FOUND);
    let ice_content_type = ice_response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_ne!(ice_content_type, "application/problem+json");
    let json = body_json(ice_response).await;
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert!(json["request_id"].is_null());

    let lance_response = lance_app(store)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default%24prod%24missing/describe")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lance_response.status(), StatusCode::NOT_FOUND);
    let lance_content_type = lance_response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap();
    assert_ne!(lance_content_type, "application/problem+json");
    let json = body_json(lance_response).await;
    assert_eq!(json["error"], "TableNotFound");
    assert!(json["request_id"].is_null());
}
