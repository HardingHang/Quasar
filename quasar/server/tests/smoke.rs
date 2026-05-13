#![cfg(all(feature = "unified", feature = "lance", feature = "iceberg"))]
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
                    .create_database("quasar_test_smoke")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_smoke");

                let pool = test_pool(&url);
                let store = PgCatalogStore::new(pool.clone());
                store.initialize().await.expect("initialize failed");

                let client = pool.get().await.expect("failed to get client");
                client
                    .execute(
                        "TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions CASCADE",
                        &[],
                    )
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
        .execute(
            "TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions CASCADE",
            &[],
        )
        .await
        .expect("failed to truncate tables");

    pool
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("JSON parse failed: {} | body: {}", e, text))
}

/// Two server instances sharing the same PostgreSQL must see consistent data.
#[tokio::test]
#[cfg(all(feature = "unified", feature = "lance", feature = "iceberg"))]
async fn test_dual_instance_stateless_smoke() {
    let pool = setup().await;

    // Create two app instances sharing the same database.
    let app1 = create_app(pool.clone());
    let app2 = create_app(pool.clone());

    // 1. App1: Unified API - create namespace.
    let response = app1
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/unified/v1/domains/default/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name":"prod","properties":{"team":"data"}}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // 2. App2: Unified API - read namespace.
    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/domains/default/namespaces/prod")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], "prod");
    assert_eq!(json["properties"]["team"], "data");

    // 3. App1: Lance API - declare table.
    let response = app1
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default$prod$users/declare")
                .header("Content-Type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 4. App2: Lance API - list tables.
    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/lance/v1/namespace/default$prod/table/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let tables = json["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0]["name"], "users");

    // 5. App1: Iceberg API - create namespace.
    let response = app1
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"namespace":["staging"]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // 6. App2: Iceberg API - list namespaces.
    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let namespaces = json["namespaces"].as_array().unwrap();
    assert!(namespaces.iter().any(|n| n[0] == "staging"));

    // 7. App2: Unified API - list assets in prod (cross-format visibility).
    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/unified/v1/namespaces/prod/assets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let assets = json["assets"].as_array().unwrap();
    assert_eq!(assets.len(), 1);
    assert_eq!(assets[0]["name"], "users");
    assert_eq!(assets[0]["format"], "lance");

    // 8. App1: Unified API - delete asset.
    let response = app1
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/unified/v1/namespaces/prod/assets/users?format=lance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // 9. App2: verify asset deleted via Lance API.
    let response = app2
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/lance/v1/table/default$prod$users/exists")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["exists"], false);
}
