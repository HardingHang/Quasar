#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use object_store::memory::InMemory;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::iceberg;
use quasar_core::CatalogStore;
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use serial_test::serial;
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

fn test_app_with_store(
    store: PgCatalogStore,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    let config = iceberg::IcebergConfig {
        warehouse_path: Some("s3://warehouse/".to_string()),
        object_store: Some(object_store),
        s3_bucket: Some("warehouse".to_string()),
    };
    iceberg::routes().layer(Extension(config)).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store
        .create_namespace(name, None, std::collections::HashMap::new())
        .await
        .unwrap();
}

/// Convert an S3 URL to a relative path for InMemory store.
fn s3_to_relative(location: &str) -> String {
    location
        .strip_prefix("s3://warehouse/")
        .unwrap_or(location)
        .to_string()
}

#[tokio::test]
#[serial]
async fn test_create_table_writes_metadata_to_object_store() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let mem_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let app = test_app_with_store(store, mem_store.clone());

    // Create table
    let create = app
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
    assert_eq!(create.status(), StatusCode::OK);

    let create_json = body_json(create).await;
    let metadata_location = create_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();

    // Verify metadata.json was written to object store
    let path = object_store::path::Path::from(s3_to_relative(&metadata_location));
    let result = mem_store.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();
    let metadata: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(metadata["format-version"], 2);
    assert_eq!(
        metadata["table-uuid"],
        create_json["metadata"]["table-uuid"]
    );
    assert_eq!(metadata["location"], "s3://warehouse/prod/users");
}

#[tokio::test]
#[serial]
async fn test_commit_table_writes_new_metadata_to_object_store() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let mem_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let app = test_app_with_store(store, mem_store.clone());

    // Create table
    let create = app
        .clone()
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
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let original_location = create_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();

    // Commit an update
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}
                        ],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {
                                "snapshot-id": 1,
                                "sequence-number": 1,
                                "timestamp-ms": 1234567890,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {"operation": "append"},
                                "schema-id": 0
                            }},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}
                        ]
                    }"#
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::OK);
    let commit_json = body_json(commit).await;
    let new_location = commit_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(new_location, original_location);
    assert!(new_location.contains("00002-"));

    // Verify old metadata still exists in object store
    let old_path = object_store::path::Path::from(s3_to_relative(&original_location));
    let old_result = mem_store.get(&old_path).await.unwrap();
    let old_bytes = old_result.bytes().await.unwrap();
    let old_metadata: Value = serde_json::from_slice(&old_bytes).unwrap();
    assert_eq!(old_metadata["format-version"], 2);

    // Verify new metadata was written to object store
    let new_path = object_store::path::Path::from(s3_to_relative(&new_location));
    let new_result = mem_store.get(&new_path).await.unwrap();
    let new_bytes = new_result.bytes().await.unwrap();
    let new_metadata: Value = serde_json::from_slice(&new_bytes).unwrap();

    assert_eq!(new_metadata["current-snapshot-id"], 1);
    assert_eq!(new_metadata["snapshots"][0]["snapshot-id"], 1);
    assert_eq!(new_metadata["refs"]["main"]["snapshot-id"], 1);
}

#[tokio::test]
#[serial]
async fn test_load_table_reads_from_object_store() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let mem_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let app = test_app_with_store(store, mem_store.clone());

    // Create table
    let create = app
        .clone()
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
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let metadata_location = create_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();

    // Overwrite metadata in object store with a modified version
    let modified_metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": create_json["metadata"]["table-uuid"],
        "location": "s3://warehouse/prod/users",
        "last-sequence-number": 42,
        "last-updated-ms": 9999999999_i64,
        "last-column-id": 0,
        "schemas": [],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {}
    });
    let path = object_store::path::Path::from(s3_to_relative(&metadata_location));
    mem_store
        .put(
            &path,
            object_store::PutPayload::from(modified_metadata.to_string()),
        )
        .await
        .unwrap();

    // Load table should read the modified metadata from object store
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
    let load_json = body_json(load).await;
    assert_eq!(load_json["metadata"]["last-sequence-number"], 42);
    assert_eq!(load_json["metadata"]["last-updated-ms"], 9999999999_i64);
}

#[tokio::test]
#[serial]
async fn test_load_table_metadata_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let mem_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let app = test_app_with_store(store, mem_store.clone());

    // Create table - this writes metadata.json
    let create = app
        .clone()
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
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let metadata_location = create_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete the metadata.json from object store
    let path = object_store::path::Path::from(s3_to_relative(&metadata_location));
    mem_store.delete(&path).await.unwrap();

    // Load table should return 404 MetadataNotFoundException
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
    let json = body_json(load).await;
    assert_eq!(json["error"]["type"], "MetadataNotFoundException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_commit_table_metadata_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    let mem_store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
    let app = test_app_with_store(store, mem_store.clone());

    // Create table - this writes metadata.json
    let create = app
        .clone()
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
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let metadata_location = create_json["metadata-location"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete the metadata.json from object store
    let path = object_store::path::Path::from(s3_to_relative(&metadata_location));
    mem_store.delete(&path).await.unwrap();

    // Commit should return 409 CommitFailedException
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}
                        ],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {
                                "snapshot-id": 1,
                                "sequence-number": 1,
                                "timestamp-ms": 1234567890,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {"operation": "append"},
                                "schema-id": 0
                            }},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::CONFLICT);
    let json = body_json(commit).await;
    assert_eq!(json["error"]["type"], "CommitFailedException");
    assert_eq!(json["error"]["code"], 409);
}
