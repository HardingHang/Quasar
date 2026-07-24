//! Unified API — asset / version / tag / registry / discovery integration tests.
//!
//! Assets and versions are read-only in the Unified API; restore and tags
//! are the only write exceptions (governance operations). Fixtures are
//! created through the store traits directly (the role of native adapters).

#![cfg(feature = "unified")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::response::Response;
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::{PostgreSQL, Settings};
use quasar_adapter::unified;
use quasar_core::{
    AssetStore, CatalogStore, CreateAsset, CreateVersion, TabularStore, VersionStore,
};
use quasar_storage::PgCatalogStore;
use serde_json::{json, Value};
use serial_test::serial;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tower::ServiceExt;

// ── Embedded PostgreSQL bootstrap (per test binary) ─────────

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

const ZONKY_RELEASES_URL: &str = "https://github.com/zonkyio/embedded-postgres-binaries";

struct PgInstance {
    #[allow(dead_code)]
    postgresql: PostgreSQL,
    url: String,
}

impl PgInstance {
    async fn get() -> &'static Self {
        PG_INSTANCE
            .get_or_init(|| async {
                let settings = Settings {
                    releases_url: ZONKY_RELEASES_URL.to_string(),
                    ..Default::default()
                };
                let mut postgresql = PostgreSQL::new(settings);
                postgresql.setup().await.expect("PostgreSQL setup failed");
                postgresql.start().await.expect("PostgreSQL start failed");
                postgresql
                    .create_database("quasar_test_unified_asset")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test_unified_asset");
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

fn test_app(store: &Arc<PgCatalogStore>) -> axum::Router {
    let store: Arc<dyn CatalogStore> = store.clone();
    unified::routes()
        .layer(axum::Extension(unified::UnifiedConfig::default()))
        .with_state(store)
}

// ── HTTP helpers ────────────────────────────────────────────

async fn body_json(response: Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn request(app: &axum::Router, method: &str, uri: &str, body: Option<String>) -> Response {
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

async fn get(app: &axum::Router, uri: &str) -> Response {
    request(app, "GET", uri, None).await
}

async fn post(app: &axum::Router, uri: &str, body: &str) -> Response {
    request(app, "POST", uri, Some(body.to_string())).await
}

async fn delete(app: &axum::Router, uri: &str) -> Response {
    request(app, "DELETE", uri, None).await
}

async fn assert_problem(response: Response, status: StatusCode, code: &str) -> Value {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );
    let body = body_json(response).await;
    assert_eq!(body["status"], status.as_u16());
    assert_eq!(body["code"], code);
    body
}

// ── Fixtures via store traits (native adapter role) ─────────

async fn make_namespace(store: &Arc<PgCatalogStore>, domain: &str, path: &str) {
    use quasar_core::{CatalogError, CreateNamespace, NamespaceStore};
    match store
        .create_namespace(
            domain,
            path,
            CreateNamespace {
                comment: None,
                properties: None,
            },
        )
        .await
    {
        Ok(_) | Err(CatalogError::AlreadyExists(_)) => {}
        Err(e) => panic!("create namespace failed: {e:?}"),
    }
}

/// Create a tabular asset; returns its id.
async fn make_table(
    store: &Arc<PgCatalogStore>,
    namespace: &str,
    name: &str,
    format: &str,
) -> String {
    make_namespace(store, "default", namespace).await;
    let pair = store
        .create_tabular_asset(
            CreateAsset {
                domain: "default".to_string(),
                namespace: namespace.to_string(),
                name: name.to_string(),
                asset_type: "table".to_string(),
                format: Some(format.to_string()),
                comment: None,
                properties: None,
            },
            &format!("s3://bucket/{namespace}/{name}"),
            Some(&format!(
                "s3://bucket/{namespace}/{name}/metadata/00001-x.metadata.json"
            )),
        )
        .await
        .expect("create tabular asset failed");
    pair.asset.id.to_string()
}

async fn make_version(store: &Arc<PgCatalogStore>, asset_id: &str, key: &str) {
    let id: uuid::Uuid = asset_id.parse().unwrap();
    // Link to the current tip as predecessor; the first version is the root.
    let previous_version_id = store.get_latest_version(id).await.ok().map(|v| v.id);
    store
        .create_version(CreateVersion {
            asset_id: id,
            version_key: key.to_string(),
            version_properties: None,
            content_inline: None,
            content_pointer: Some(format!("s3://bucket/m/{key}.metadata.json")),
            previous_version_id,
        })
        .await
        .expect("create version failed");
}

// ── Asset read-only tests ───────────────────────────────────

#[tokio::test]
#[serial]
async fn asset_list_and_get_readonly() {
    let store = setup().await;
    let app = test_app(&store);
    let id = make_table(&store, "ns1", "events", "iceberg").await;
    make_table(&store, "ns1", "logs", "lance").await;
    make_version(&store, &id, "00001").await;

    // List in namespace, unfiltered.
    let resp = get(&app, "/unified/v1/domains/default/namespaces/ns1/assets").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 2);

    // Filter by format.
    let resp = get(
        &app,
        "/unified/v1/domains/default/namespaces/ns1/assets?format=lance",
    )
    .await;
    let body = body_json(resp).await;
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "logs");

    // Get by name (hierarchical path addressing).
    let resp = get(
        &app,
        "/unified/v1/domains/default/namespaces/ns1/assets/events",
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["id"], id);
    assert_eq!(body["format"], "iceberg");

    // Get by id embeds the current version from the DB (no object store).
    let resp = get(&app, &format!("/unified/v1/assets/{id}")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["current_version"]["version_key"], "00001");
    assert_eq!(
        body["current_version"]["content_pointer"],
        "s3://bucket/m/00001.metadata.json"
    );

    // Lifecycle endpoints are not offered (assets are read-only here).
    let resp = post(
        &app,
        "/unified/v1/domains/default/namespaces/ns1/assets",
        &json!({"name": "new", "asset_type": "table"}).to_string(),
    )
    .await;
    assert!(
        resp.status() == StatusCode::METHOD_NOT_ALLOWED || resp.status() == StatusCode::NOT_FOUND
    );
}

#[tokio::test]
#[serial]
async fn asset_restore_flow() {
    let store = setup().await;
    let app = test_app(&store);
    let id = make_table(&store, "ns2", "tbl", "iceberg").await;

    // Soft-delete via the store (native protocol role), then restore via API.
    store
        .soft_delete_asset(id.parse().unwrap())
        .await
        .expect("soft delete failed");

    let resp = post(&app, &format!("/unified/v1/assets/{id}/restore"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_json(resp).await["deleted_at"].is_null());

    // Restoring an active asset conflicts.
    let resp = post(&app, &format!("/unified/v1/assets/{id}/restore"), "").await;
    assert_problem(resp, StatusCode::CONFLICT, "CONFLICT").await;

    // Same-name conflict on restore: delete tbl, recreate tbl, restore old.
    store
        .soft_delete_asset(id.parse().unwrap())
        .await
        .expect("soft delete failed");
    make_table(&store, "ns2", "tbl", "lance").await;
    let resp = post(&app, &format!("/unified/v1/assets/{id}/restore"), "").await;
    assert_problem(resp, StatusCode::CONFLICT, "CONFLICT").await;
}

#[tokio::test]
#[serial]
async fn tag_lifecycle() {
    let store = setup().await;
    let app = test_app(&store);
    let id = make_table(&store, "ns3", "tagged", "iceberg").await;

    let resp = post(
        &app,
        &format!("/unified/v1/assets/{id}/tags"),
        &json!({"tag": "pii"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(body_json(resp).await["tags"]
        .as_array()
        .unwrap()
        .contains(&json!("pii")));

    let resp = get(&app, &format!("/unified/v1/assets/{id}/tags")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["tags"], json!(["pii"]));

    let resp = delete(&app, &format!("/unified/v1/assets/{id}/tags/pii")).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = get(&app, &format!("/unified/v1/assets/{id}/tags")).await;
    assert_eq!(body_json(resp).await["tags"], json!([]));
}

#[tokio::test]
#[serial]
async fn version_readonly_endpoints() {
    let store = setup().await;
    let app = test_app(&store);
    let id = make_table(&store, "ns4", "versioned", "iceberg").await;
    make_version(&store, &id, "00001").await;
    make_version(&store, &id, "00002").await;

    let resp = get(&app, &format!("/unified/v1/assets/{id}/versions")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["items"].as_array().unwrap().len(), 2);

    let resp = get(&app, &format!("/unified/v1/assets/{id}/versions/00002")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["version_key"], "00002");

    // Version writes are not offered.
    let resp = delete(&app, &format!("/unified/v1/assets/{id}/versions/00002")).await;
    assert!(
        resp.status() == StatusCode::METHOD_NOT_ALLOWED || resp.status() == StatusCode::NOT_FOUND
    );
}

// ── Registry tests ──────────────────────────────────────────

#[tokio::test]
#[serial]
async fn registry_asset_types_and_formats() {
    let store = setup().await;
    let app = test_app(&store);

    // Seeds are visible.
    let resp = get(&app, "/unified/v1/asset-types").await;
    let body = body_json(resp).await;
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"table") && names.contains(&"view"));

    let resp = get(&app, "/unified/v1/asset-types/table").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["category"], "tabular");

    // Register a new type; duplicate conflicts; bad category rejected.
    let resp = post(
        &app,
        "/unified/v1/asset-types",
        &json!({"name": "model", "category": "model", "extension_strategy": "jsonb"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp = post(
        &app,
        "/unified/v1/asset-types",
        &json!({"name": "model", "category": "model", "extension_strategy": "jsonb"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::CONFLICT, "ALREADY_EXISTS").await;
    let resp = post(
        &app,
        "/unified/v1/asset-types",
        &json!({"name": "bogus", "category": "nope", "extension_strategy": "jsonb"}).to_string(),
    )
    .await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;

    // Formats: seeds visible; register + duplicate.
    let resp = get(&app, "/unified/v1/formats").await;
    let body = body_json(resp).await;
    let names: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"iceberg") && names.contains(&"lance"));

    let resp = post(
        &app,
        "/unified/v1/formats",
        &json!({"name": "onnx", "mime_type": "application/octet-stream"}).to_string(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp = get(&app, "/unified/v1/formats/onnx").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = get(&app, "/unified/v1/formats/ghost").await;
    assert_problem(resp, StatusCode::NOT_FOUND, "NOT_FOUND").await;
}

// ── Discovery tests ─────────────────────────────────────────

#[tokio::test]
#[serial]
async fn discovery_filters() {
    let store = setup().await;
    let app = test_app(&store);
    let iceberg_id = make_table(&store, "team/a", "events", "iceberg").await;
    make_table(&store, "team/b", "logs", "lance").await;
    post(
        &app,
        &format!("/unified/v1/assets/{iceberg_id}/tags"),
        &json!({"tag": "core"}).to_string(),
    )
    .await;

    // domain is required.
    let resp = get(&app, "/unified/v1/assets").await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;

    // Filter by format.
    let resp = get(&app, "/unified/v1/assets?domain=default&format=lance").await;
    let items = body_json(resp).await["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "logs");

    // Filter by namespace prefix (hierarchical).
    let resp = get(
        &app,
        "/unified/v1/assets?domain=default&namespace_prefix=team",
    )
    .await;
    assert_eq!(body_json(resp).await["items"].as_array().unwrap().len(), 2);
    let resp = get(
        &app,
        "/unified/v1/assets?domain=default&namespace_prefix=team/a",
    )
    .await;
    assert_eq!(body_json(resp).await["items"].as_array().unwrap().len(), 1);

    // Filter by tag.
    let resp = get(&app, "/unified/v1/assets?domain=default&tag=core").await;
    let items = body_json(resp).await["items"].as_array().unwrap().clone();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["name"], "events");

    // Unregistered format is a validation error.
    let resp = get(&app, "/unified/v1/assets?domain=default&format=nope").await;
    assert_problem(resp, StatusCode::BAD_REQUEST, "VALIDATION_FAILED").await;
}
