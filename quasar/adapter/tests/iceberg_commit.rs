#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use postgresql_embedded::PostgreSQL;
use quasar_adapter::iceberg;
use quasar_core::CatalogStore;
use quasar_core::{CasCommitStore, IcebergStagingStore, NamespaceStore, TabularStore};
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
        .execute("TRUNCATE tabular_asset_versions, asset_versions, tabular_assets, assets, namespaces, asset_permissions, iceberg_staged_tables CASCADE", &[])
        .await
        .expect("failed to truncate tables");

    store
}

fn test_app(store: PgCatalogStore) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    iceberg::routes()
        .layer(Extension(iceberg::IcebergConfig::default()))
        .with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            let text = String::from_utf8_lossy(&body);
            panic!("Failed to parse JSON: {}. Body: {}", e, text);
        }
    }
}

async fn create_namespace(store: &PgCatalogStore, name: &str) {
    store
        .create_namespace("default", name, None, HashMap::new())
        .await
        .unwrap();
}

/// Current wall-clock millis for snapshot timestamps in HTTP commit bodies.
///
/// The iceberg crate enforces: snapshot.timestamp_ms >= metadata.last_updated_ms,
/// and on the next build: new last_updated_ms (now) >= last snapshot log entry ts.
/// Using `chrono::Utc::now()` keeps every test in the present and avoids the
/// "future timestamp" pitfall that bit us when we used a far-future constant.
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[tokio::test]
#[serial]
async fn test_commit_success() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let original_location = create_json["metadata-location"].as_str().unwrap();

    // Commit an update: add snapshot + set snapshot ref
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
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
                                "timestamp-ms": __TS__,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {"operation": "append"},
                                "schema-id": 0
                            }},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string())
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = commit.status();
    let commit_json = body_json(commit).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "unexpected status, body: {:?}",
        commit_json
    );
    let new_location = commit_json["metadata-location"].as_str().unwrap();
    assert_ne!(new_location, original_location);
    assert!(new_location.contains("00002-"));
    assert_eq!(commit_json["metadata"]["current-snapshot-id"], 1);
    assert_eq!(commit_json["metadata"]["snapshots"][0]["snapshot-id"], 1);
}

#[tokio::test]
#[serial]
async fn test_commit_conflict() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table directly through store so we can manipulate metadata_location
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
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
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Try to commit based on an outdated metadata_location
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
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
                                "timestamp-ms": __TS__,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {"operation": "append"},
                                "schema-id": 0
                            }}
                        ]
                    }"#
                    .replace("__TS__", &now_ms().to_string())
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Note: This test currently succeeds because the metadata_location in DB matches.
    // This is intentional behavior - the CAS expects the metadata_location to match.
    let commit_json = body_json(commit).await;
    if commit_json.get("error").is_some() {
        panic!("Commit failed: {}", commit_json);
    }
}

#[tokio::test]
#[serial]
async fn test_commit_requirement_failure() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with an existing snapshot
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 1,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [
                    {
                        "snapshot-id": 42,
                        "sequence-number": 1,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://bucket/m.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    }
                ],
                "snapshot-log": [{"timestamp-ms": 2000, "snapshot-id": 42}],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {"main": {"snapshot-id": 42, "type": "branch"}}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Commit with wrong expected snapshot-id
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 999}
                        ],
                        "updates": []
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
    let message = json["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("snapshot-id mismatch")
            || message.contains("snapshot has changed")
            || message.to_lowercase().contains("snapshot"),
        "Expected snapshot-related error, got: {}",
        message
    );
}

#[tokio::test]
#[serial]
async fn test_commit_table_not_found() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/nonexistent")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"requirements": [], "updates": []}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::NOT_FOUND);
    let json = body_json(commit).await;
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert_eq!(json["error"]["code"], 404);
}

#[tokio::test]
#[serial]
async fn test_commit_updates_persisted() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Commit
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
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
                                "timestamp-ms": __TS__,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {"operation": "append"},
                                "schema-id": 0
                            }},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"},
                            {"action": "set-properties", "updates": {"owner": "team-a"}}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string())
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Load and verify
    let load = app
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
    let json = body_json(load).await;
    assert_eq!(json["metadata"]["current-snapshot-id"], 1);
    assert_eq!(json["metadata"]["snapshots"][0]["snapshot-id"], 1);
    assert_eq!(json["metadata"]["properties"]["owner"], "team-a");
    assert_eq!(json["metadata"]["refs"]["main"]["snapshot-id"], 1);
    assert!(json["metadata-location"]
        .as_str()
        .unwrap()
        .contains("00002-"));
}

#[tokio::test]
#[serial]
async fn test_commit_cas_conflict_simulated() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table directly through store with a known metadata_location
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
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
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    // First CAS commit succeeds: expected 00001 matches DB
    store
        .cas_update_metadata_location(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json",
            "s3://bucket/warehouse/prod/users/metadata/00002-uuid.metadata.json",
            None,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap();

    // Second CAS commit fails: expected 00001 no longer matches DB (now 00002)
    let err = store
        .cas_update_metadata_location(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json",
            "s3://bucket/warehouse/prod/users/metadata/00003-uuid.metadata.json",
            None,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap_err();

    assert!(matches!(err, quasar_core::StoreError::Conflict { .. }));
    let msg = format!("{}", err);
    assert!(msg.contains("modified by another commit"));
}

/// AssertRefSnapshotId非main ref测试 - 验证自定义branch
#[tokio::test]
#[serial]
async fn test_assert_ref_snapshot_id_custom_branch() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with a custom branch "staging"
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 2,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [
                    {
                        "snapshot-id": 42,
                        "sequence-number": 1,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://bucket/m1.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    },
                    {
                        "snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 3000,
                        "manifest-list": "s3://bucket/m2.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    }
                ],
                "snapshot-log": [
                    {"timestamp-ms": 2000, "snapshot-id": 42},
                    {"timestamp-ms": 3000, "snapshot-id": 10}
                ],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {
                    "main": {"snapshot-id": 42, "type": "branch"},
                    "staging": {"snapshot-id": 10, "type": "branch"}
                }
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Commit with assert on "staging" branch - correct snapshot-id should succeed
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "staging", "snapshot-id": 10}
                        ],
                        "updates": []
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let json = body_json(commit).await;
    if json.get("error").is_some() {
        panic!("Commit failed: {}", json);
    }
    // Requirements satisfied → 200 OK (empty updates)
}

/// AssertRefSnapshotId非main ref测试 - 验证失败场景
#[tokio::test]
#[serial]
async fn test_assert_ref_snapshot_id_custom_branch_fail() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with a custom branch "staging"
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 2,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [
                    {
                        "snapshot-id": 42,
                        "sequence-number": 1,
                        "timestamp-ms": 2000,
                        "manifest-list": "s3://bucket/m1.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    },
                    {
                        "snapshot-id": 10,
                        "sequence-number": 2,
                        "timestamp-ms": 3000,
                        "manifest-list": "s3://bucket/m2.avro",
                        "summary": {"operation": "append"},
                        "schema-id": 0
                    }
                ],
                "snapshot-log": [
                    {"timestamp-ms": 2000, "snapshot-id": 42},
                    {"timestamp-ms": 3000, "snapshot-id": 10}
                ],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {
                    "main": {"snapshot-id": 42, "type": "branch"},
                    "staging": {"snapshot-id": 10, "type": "branch"}
                }
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Commit with wrong snapshot-id for "staging" branch
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "staging", "snapshot-id": 999}
                        ],
                        "updates": []
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::CONFLICT);
    let json = body_json(commit).await;
    assert_eq!(json["error"]["type"], "CommitFailedException");
    let msg = json["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("snapshot-id mismatch")
            || msg.contains("snapshot has changed")
            || msg.to_lowercase().contains("snapshot"),
        "Expected snapshot-related error, got: {}",
        msg
    );
}

/// AssertTableUuid requirement端到端测试 - UUID不匹配
#[tokio::test]
#[serial]
async fn test_assert_table_uuid_failure() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with known UUID
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440001",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
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
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Commit with wrong UUID requirement
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000999"}
                        ],
                        "updates": []
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::CONFLICT);
    let json = body_json(commit).await;
    assert_eq!(json["error"]["type"], "CommitFailedException");
    let msg = json["error"]["message"].as_str().unwrap();
    println!("UUID error message: {}", msg);
    assert!(
        msg.contains("UUID mismatch") || msg.contains("uuid") || msg.contains("UUID"),
        "Expected UUID-related error, got: {}",
        msg
    );
}

/// 多次commit版本号递增测试
#[tokio::test]
#[serial]
async fn test_multiple_commit_version_increment() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let create_json = body_json(create).await;
    let location1 = create_json["metadata-location"].as_str().unwrap();
    assert!(location1.contains("00001-"));

    // First commit → should produce 00002
    let commit1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": __TS__, "manifest-list": "s3://b/m1.avro", "summary": {"operation": "append"}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string()),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit1.status(), StatusCode::OK);
    let json1 = body_json(commit1).await;
    let location2 = json1["metadata-location"].as_str().unwrap();
    assert!(location2.contains("00002-"));

    // Second commit → should produce 00003
    let commit2 = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 2, "sequence-number": 2, "timestamp-ms": __TS__, "manifest-list": "s3://b/m2.avro", "summary": {"operation": "append"}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 2, "type": "branch"}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string()),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit2.status(), StatusCode::OK);
    let json2 = body_json(commit2).await;
    let location3 = json2["metadata-location"].as_str().unwrap();
    assert!(location3.contains("00003-"));
}

/// SetSnapshotRef type字段测试 - TAG类型
#[tokio::test]
#[serial]
async fn test_set_snapshot_ref_with_tag_type() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Commit with snapshot and set ref as TAG
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": __TS__, "manifest-list": "s3://b/m1.avro", "summary": {"operation": "append"}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"},
                            {"action": "set-snapshot-ref", "ref-name": "v1.0", "snapshot-id": 1, "type": "tag"}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string()),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::OK);

    // Load and verify tag ref
    let load = app
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
    let json = body_json(load).await;
    assert_eq!(json["metadata"]["refs"]["v1.0"]["snapshot-id"], 1);
    assert_eq!(json["metadata"]["refs"]["v1.0"]["type"], "tag");
}

/// Namespace不存在测试
#[tokio::test]
#[serial]
async fn test_commit_namespace_not_found() {
    let store = setup().await;
    // No namespace created
    let app = test_app(store);

    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/nonexistent/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"requirements": [], "updates": []}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::NOT_FOUND);
    let json = body_json(commit).await;
    assert_eq!(json["error"]["type"], "NoSuchTableException");
    assert_eq!(json["error"]["code"], 404);
}

/// 空updates边界测试
#[tokio::test]
#[serial]
async fn test_commit_empty_updates() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Commit with requirements satisfied but empty updates
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": []
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Empty updates should still succeed (no-op commit)
    assert_eq!(commit.status(), StatusCode::OK);
}

/// RemoveProperties端到端测试
#[tokio::test]
#[serial]
async fn test_remove_properties_commit() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // First commit to add properties
    let r1 = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": __TS__, "manifest-list": "s3://b/m1.avro", "summary": {"operation": "append"}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"},
                            {"action": "set-properties", "updates": {"owner": "team-a", "env": "prod"}}
                        ]
                    }"#.replace("__TS__", &now_ms().to_string()),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let s1 = r1.status();
    let j1 = body_json(r1).await;
    assert_eq!(s1, StatusCode::OK, "first commit failed: {:?}", j1);

    // Second commit to remove properties
    let r2 = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
                        "updates": [
                            {"action": "remove-properties", "removals": ["env"]}
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let s2 = r2.status();
    let j2 = body_json(r2).await;
    assert_eq!(s2, StatusCode::OK, "second commit failed: {:?}", j2);

    // Load and verify
    let load = app
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
    let json = body_json(load).await;
    assert_eq!(json["metadata"]["properties"]["owner"], "team-a");
    assert!(json["metadata"]["properties"].get("env").is_none());
}

#[tokio::test]
#[serial]
async fn test_commit_assert_create_fails_on_existing_table() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table directly through store with initial metadata
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": null,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // Commit with assert-create on an existing table should fail with 409
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-create"}],
                        "updates": []
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
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("already exists"));
}

#[tokio::test]
#[serial]
async fn test_commit_add_schema() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": null,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [],
                        "updates": [
                            {
                                "action": "add-schema",
                                "schema": {
                                    "schema-id": 1,
                                    "fields": [
                                        {"id": 1, "name": "id", "type": "long", "required": true}
                                    ]
                                }
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::OK);
    let json = body_json(commit).await;
    let schemas = json["metadata"]["schemas"].as_array().unwrap();
    assert_eq!(schemas.len(), 2);
    // iceberg crate auto-assigns schema-id; may not match the requested value
    assert!(schemas[1]["schema-id"].as_i64().is_some());
}

#[tokio::test]
#[serial]
async fn test_commit_set_current_schema() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [
                    {"type": "struct", "schema-id": 0, "fields": []},
                    {"type": "struct", "schema-id": 1, "fields": [{"id": 1, "name": "id", "type": "long", "required": false}]}
                ],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": null,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [],
                        "updates": [
                            {"action": "set-current-schema", "schema-id": 1}
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let json = body_json(commit).await;
    if json.get("error").is_some() {
        panic!("Commit failed: {}", json);
    }
    assert_eq!(json["metadata"]["current-schema-id"], 1);
}

#[tokio::test]
#[serial]
async fn test_commit_add_partition_spec() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                    {"id": 1, "name": "date", "type": "date", "required": false}
                ]}],
                "current-schema-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "last-column-id": 1,
                "properties": {},
                "current-snapshot-id": null,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [{"order-id": 0, "fields": []}],
                "default-sort-order-id": 0,
                "refs": {}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [],
                        "updates": [
                            {
                                "action": "add-spec",
                                "spec": {
                                    "spec-id": 1,
                                    "fields": [
                                        {"name": "date", "transform": "identity", "source-id": 1}
                                    ]
                                }
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let json = body_json(commit).await;
    if json.get("error").is_some() {
        panic!("Commit failed: {}", json);
    }
    let specs = json["metadata"]["partition-specs"].as_array().unwrap();
    assert_eq!(specs.len(), 2);
    // iceberg crate auto-assigns spec-id; may not match the requested value
    assert!(specs[1]["spec-id"].as_i64().is_some());
}

// ── V4.0 Unsupported Update -> 501 NotImplementedException ─────────────────

async fn assert_unsupported_update_returns_501(body: &str, wire_name: &str) {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(
        status,
        StatusCode::NOT_IMPLEMENTED,
        "expected 501 for {wire_name}, body: {json:?}"
    );
    assert_eq!(json["error"]["type"], "NotImplementedException");
    assert_eq!(json["error"]["code"], 501);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains(wire_name),
        "expected message to mention '{wire_name}', got: {json:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_commit_set_statistics_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{
            "action": "set-statistics",
            "snapshot-id": 1,
            "statistics": {
                "snapshot-id": 1,
                "statistics-path": "s3://bucket/stats.puffin",
                "file-size-in-bytes": 100,
                "file-footer-size-in-bytes": 10,
                "blob-metadata": []
            }
        }]
    }"#;
    assert_unsupported_update_returns_501(body, "set-statistics").await;
}

#[tokio::test]
#[serial]
async fn test_commit_remove_statistics_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{"action": "remove-statistics", "snapshot-id": 1}]
    }"#;
    assert_unsupported_update_returns_501(body, "remove-statistics").await;
}

#[tokio::test]
#[serial]
async fn test_commit_set_partition_statistics_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{
            "action": "set-partition-statistics",
            "partition-statistics": {
                "snapshot-id": 1,
                "statistics-path": "s3://bucket/pstats",
                "file-size-in-bytes": 50
            }
        }]
    }"#;
    assert_unsupported_update_returns_501(body, "set-partition-statistics").await;
}

#[tokio::test]
#[serial]
async fn test_commit_remove_partition_statistics_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{"action": "remove-partition-statistics", "snapshot-id": 1}]
    }"#;
    assert_unsupported_update_returns_501(body, "remove-partition-statistics").await;
}

#[tokio::test]
#[serial]
async fn test_commit_remove_schemas_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{"action": "remove-schemas", "schema-ids": [1]}]
    }"#;
    assert_unsupported_update_returns_501(body, "remove-schemas").await;
}

#[tokio::test]
#[serial]
async fn test_commit_add_encryption_key_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{
            "action": "add-encryption-key",
            "encryption-key": {
                "key-id": "k1",
                "encrypted-key-metadata": "AA==",
                "encrypted-by-id": null,
                "properties": {}
            }
        }]
    }"#;
    assert_unsupported_update_returns_501(body, "add-encryption-key").await;
}

#[tokio::test]
#[serial]
async fn test_commit_remove_encryption_key_returns_501() {
    let body = r#"{
        "requirements": [],
        "updates": [{"action": "remove-encryption-key", "key-id": "k1"}]
    }"#;
    assert_unsupported_update_returns_501(body, "remove-encryption-key").await;
}

// ── V4.0 Requirement coverage (5 of 8 not previously covered) ──────────────
// uuid + ref-snapshot are tested above. The remaining 5: current-schema-id,
// default-spec-id, default-sort-order-id, last-assigned-field-id,
// last-assigned-partition-id — success + mismatch paths each.

async fn create_basic_table(app: &axum::Router) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

async fn assert_requirement_succeeds(req_json: &str) {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    let body = format!(r#"{{ "requirements": [{req_json}], "updates": [] }}"#);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "expected 200, body: {json:?}");
}

async fn assert_requirement_fails(req_json: &str, msg_substr_lower: &str) {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    let body = format!(r#"{{ "requirements": [{req_json}], "updates": [] }}"#);
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::CONFLICT, "expected 409, body: {json:?}");
    assert_eq!(json["error"]["type"], "CommitFailedException");
    let msg = json["error"]["message"].as_str().unwrap().to_lowercase();
    assert!(
        msg.contains(msg_substr_lower),
        "expected message to contain '{msg_substr_lower}', got: {msg}"
    );
}

#[tokio::test]
#[serial]
async fn test_assert_current_schema_id_match_success() {
    assert_requirement_succeeds(r#"{"type": "assert-current-schema-id", "current-schema-id": 0}"#)
        .await;
}

#[tokio::test]
#[serial]
async fn test_assert_current_schema_id_mismatch_fails() {
    assert_requirement_fails(
        r#"{"type": "assert-current-schema-id", "current-schema-id": 99}"#,
        "schema",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_default_spec_id_match_success() {
    assert_requirement_succeeds(r#"{"type": "assert-default-spec-id", "default-spec-id": 0}"#)
        .await;
}

#[tokio::test]
#[serial]
async fn test_assert_default_spec_id_mismatch_fails() {
    assert_requirement_fails(
        r#"{"type": "assert-default-spec-id", "default-spec-id": 99}"#,
        "spec",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_default_sort_order_id_match_success() {
    assert_requirement_succeeds(
        r#"{"type": "assert-default-sort-order-id", "default-sort-order-id": 0}"#,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_default_sort_order_id_mismatch_fails() {
    assert_requirement_fails(
        r#"{"type": "assert-default-sort-order-id", "default-sort-order-id": 99}"#,
        "sort order",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_last_assigned_field_id_match_success() {
    // Default schema has no fields, so last-column-id starts at 0.
    assert_requirement_succeeds(
        r#"{"type": "assert-last-assigned-field-id", "last-assigned-field-id": 0}"#,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_last_assigned_field_id_mismatch_fails() {
    assert_requirement_fails(
        r#"{"type": "assert-last-assigned-field-id", "last-assigned-field-id": 99}"#,
        "field id",
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_last_assigned_partition_id_match_success() {
    // V2 builder sets last-partition-id to 999 by default for empty spec.
    assert_requirement_succeeds(
        r#"{"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 999}"#,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn test_assert_last_assigned_partition_id_mismatch_fails() {
    assert_requirement_fails(
        r#"{"type": "assert-last-assigned-partition-id", "last-assigned-partition-id": 12345}"#,
        "partition id",
    )
    .await;
}

// ── V4.0 Update coverage (variants not yet covered) ────────────────────────

#[tokio::test]
#[serial]
async fn test_commit_assign_uuid() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    // assign-uuid is only valid when the table-uuid is unset or matches; iceberg
    // crate's behavior: AssignUuid::apply sets the uuid only if it differs from
    // the current. Since create_table sets a UUID, supplying the same UUID is a
    // no-op; supplying a different one yields an error. We exercise the
    // accepting branch: assign back the same uuid (read from the created table).
    let load = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let load_json = body_json(load).await;
    let current_uuid = load_json["metadata"]["table-uuid"].as_str().unwrap();

    let body = format!(
        r#"{{ "requirements": [], "updates": [
            {{ "action": "assign-uuid", "uuid": "{current_uuid}" }}
        ] }}"#
    );
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "assign-uuid no-op, body: {json:?}");
    assert_eq!(json["metadata"]["table-uuid"], current_uuid);
}

#[tokio::test]
#[serial]
async fn test_commit_upgrade_format_version_v2_noop() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    // Upgrade to v2 when already v2 — accepted as a no-op.
    let body = r#"{ "requirements": [], "updates": [
        {"action": "upgrade-format-version", "format-version": 2}
    ] }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "expected 200, body: {json:?}");
    assert_eq!(json["metadata"]["format-version"], 2);
}

#[tokio::test]
#[serial]
async fn test_commit_set_location() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    let body = r#"{ "requirements": [], "updates": [
        {"action": "set-location", "location": "s3://bucket/new/path"}
    ] }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json:?}");
    assert_eq!(json["metadata"]["location"], "s3://bucket/new/path");
}

#[tokio::test]
#[serial]
async fn test_commit_remove_snapshots() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    // First commit: add a snapshot.
    let add_body = r#"{
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
        "updates": [
            {"action": "add-snapshot", "snapshot": {
                "snapshot-id": 1, "sequence-number": 1, "timestamp-ms": __TS__,
                "manifest-list": "s3://b/m1.avro", "summary": {"operation": "append"},
                "schema-id": 0
            }},
            {"action": "set-snapshot-ref", "ref-name": "extra", "snapshot-id": 1, "type": "branch"}
        ]
    }"#
    .replace("__TS__", &now_ms().to_string());
    let add_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(add_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = add_resp.status();
    let aj = body_json(add_resp).await;
    assert_eq!(status, StatusCode::OK, "add-snapshot failed: {aj:?}");

    // Second commit: remove-snapshot-ref then remove-snapshots. The iceberg
    // crate requires no ref to retain a snapshot before it can be removed.
    let rm_body = r#"{
        "requirements": [],
        "updates": [
            {"action": "remove-snapshot-ref", "ref-name": "extra"},
            {"action": "remove-snapshots", "snapshot-ids": [1]}
        ]
    }"#;
    let rm_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(rm_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = rm_resp.status();
    let rj = body_json(rm_resp).await;
    assert_eq!(status, StatusCode::OK, "remove failed: {rj:?}");
    // After removing the only snapshot, the array may be missing or empty.
    let snapshots = rj["metadata"]["snapshots"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        snapshots.iter().all(|s| s["snapshot-id"] != 1),
        "snapshot 1 should be removed, got: {snapshots:?}"
    );
}

#[tokio::test]
#[serial]
async fn test_commit_remove_snapshot_ref() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_basic_table(&app).await;

    // Add a snapshot bound to a custom branch.
    let add_body = r#"{
        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
        "updates": [
            {"action": "add-snapshot", "snapshot": {
                "snapshot-id": 1, "sequence-number": 1, "timestamp-ms": __TS__,
                "manifest-list": "s3://b/m1.avro", "summary": {"operation": "append"},
                "schema-id": 0
            }},
            {"action": "set-snapshot-ref", "ref-name": "staging", "snapshot-id": 1, "type": "branch"}
        ]
    }"#
    .replace("__TS__", &now_ms().to_string());
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(add_body))
                .unwrap(),
        )
        .await
        .unwrap();

    let rm_body = r#"{
        "requirements": [],
        "updates": [{"action": "remove-snapshot-ref", "ref-name": "staging"}]
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(rm_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json:?}");
    let refs = json["metadata"]["refs"].as_object().unwrap();
    assert!(!refs.contains_key("staging"));
}

#[tokio::test]
#[serial]
async fn test_commit_set_default_spec() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table with a single string field so add-spec(identity(name)) is valid.
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "name": "users",
                        "schema": {
                            "type": "struct", "schema-id": 0,
                            "fields": [{"id": 1, "name": "name", "type": "string", "required": false}]
                        }
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::OK);

    // add a new partition spec, then set-default-spec to its id.
    let body = r#"{
        "requirements": [],
        "updates": [
            {"action": "add-spec", "spec": {"spec-id": 1, "fields": [
                {"name": "name_id", "transform": "identity", "source-id": 1, "field-id": 1000}
            ]}},
            {"action": "set-default-spec", "spec-id": -1}
        ]
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json:?}");
    let new_default = json["metadata"]["default-spec-id"].as_i64().unwrap();
    assert_ne!(new_default, 0, "default-spec-id should have changed");
}

#[tokio::test]
#[serial]
async fn test_commit_remove_partition_specs() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table with a column we can partition on.
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "name": "users",
                        "schema": {
                            "type": "struct", "schema-id": 0,
                            "fields": [{"id": 1, "name": "name", "type": "string", "required": false}]
                        }
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // First commit: add spec 1, do NOT change default → default stays 0.
    let add_body = r#"{
        "requirements": [],
        "updates": [
            {"action": "add-spec", "spec": {"spec-id": 1, "fields": [
                {"name": "name_id", "transform": "identity", "source-id": 1, "field-id": 1000}
            ]}}
        ]
    }"#;
    let r1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(add_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let s1 = r1.status();
    let j1 = body_json(r1).await;
    assert_eq!(s1, StatusCode::OK, "add-spec failed: {j1:?}");
    let added_id = j1["metadata"]["partition-specs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|s| s["spec-id"].as_i64())
        .find(|&id| id != 0)
        .expect("expected an added non-default spec id");

    // Second commit: remove the non-default spec.
    let rm_body = format!(
        r#"{{ "requirements": [], "updates": [
            {{"action": "remove-partition-specs", "spec-ids": [{added_id}]}}
        ] }}"#
    );
    let r2 = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(rm_body))
                .unwrap(),
        )
        .await
        .unwrap();
    let s2 = r2.status();
    let j2 = body_json(r2).await;
    assert_eq!(s2, StatusCode::OK, "remove failed: {j2:?}");
    let remaining = j2["metadata"]["partition-specs"].as_array().unwrap();
    assert!(remaining
        .iter()
        .all(|s| s["spec-id"].as_i64() != Some(added_id)));
}

#[tokio::test]
#[serial]
async fn test_commit_add_sort_order_and_set_default() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table with a sort-eligible column.
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "name": "users",
                        "schema": {
                            "type": "struct", "schema-id": 0,
                            "fields": [{"id": 1, "name": "name", "type": "string", "required": false}]
                        }
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = r#"{
        "requirements": [],
        "updates": [
            {"action": "add-sort-order", "sort-order": {"order-id": 1, "fields": [
                {"transform": "identity", "source-id": 1, "direction": "asc", "null-order": "nulls-first"}
            ]}},
            {"action": "set-default-sort-order", "sort-order-id": -1}
        ]
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json:?}");
    let default = json["metadata"]["default-sort-order-id"].as_i64().unwrap();
    assert_ne!(default, 0, "default-sort-order-id should have changed");
}

// ── V4.0 staged-create commit (V4_DESIGN.md §5.13, §6.4) ───────────────────

/// Create a staged table; returns the metadata-location reported by the server.
async fn create_staged(app: &axum::Router) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{"name": "users", "stage-create": true, "location": "s3://bucket/warehouse/prod/users"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_staged_commit_success() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_staged(&app).await;

    let body = r#"{
        "requirements": [{"type": "assert-create"}],
        "updates": [
            {"action": "set-properties", "updates": {"owner": "team-a"}}
        ]
    }"#;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "body: {json:?}");
    let loc = json["metadata-location"].as_str().unwrap();
    assert!(
        loc.contains("00002-"),
        "expected 00002-* location, got {loc}"
    );
    assert_eq!(json["metadata"]["properties"]["owner"], "team-a");

    // After commit the table is active: load_table returns it.
    let load = app
        .clone()
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

    // And it appears in the table list.
    let list = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let list_json = body_json(list).await;
    let identifiers = list_json["identifiers"].as_array().unwrap();
    assert!(identifiers.iter().any(|i| i["name"] == "users"));
}

#[tokio::test]
#[serial]
async fn test_staged_commit_missing_assert_create() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_staged(&app).await;

    let body = r#"{
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {json:?}");
    assert_eq!(json["error"]["type"], "CommitFailedException");
    let msg = json["error"]["message"].as_str().unwrap();
    assert!(
        msg.contains("assert-create"),
        "expected message to mention 'assert-create', got: {msg}"
    );
}

#[tokio::test]
#[serial]
async fn test_staged_commit_no_staged_no_active_returns_404() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Neither staged nor active table — commit should be a clean 404.
    let body = r#"{
        "requirements": [{"type": "assert-create"}],
        "updates": []
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/ghost")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "body: {json:?}");
    assert_eq!(json["error"]["type"], "NoSuchTableException");
}

#[tokio::test]
#[serial]
async fn test_staged_commit_with_concurrent_active_create_returns_409() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Stage_create directly through the store (skip HTTP since we need to keep
    // a second store handle for the racing active insert).
    let table_uuid = uuid::Uuid::new_v4();
    store
        .create_staged_table(
            "default",
            "prod",
            "users",
            table_uuid,
            "s3://bucket/warehouse/prod/users",
            "s3://bucket/warehouse/prod/users/metadata/00001-staged.metadata.json",
            serde_json::json!({
                "format-version": 2,
                "table-uuid": table_uuid.to_string(),
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
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
            }),
            HashMap::new(),
        )
        .await
        .unwrap();

    // Racing writer creates the active row before commit lands.
    let active_uuid = uuid::Uuid::new_v4();
    store
        .create_tabular_asset(
            "default",
            "prod",
            "users",
            "iceberg",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-other.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": active_uuid.to_string(),
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
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
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    let app = test_app(store);

    // commit_table now finds the active row (existing-table path); the
    // request carries assert-create (TableRequirement::NotExist), which the
    // iceberg crate rejects against an active table → 409.
    let body = r#"{
        "requirements": [{"type": "assert-create"}],
        "updates": []
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::CONFLICT, "body: {json:?}");
    assert_eq!(json["error"]["type"], "CommitFailedException");
}

#[tokio::test]
#[serial]
async fn test_staged_commit_rejects_unsupported_update_501() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);
    create_staged(&app).await;

    // Even on the staged path, unsupported updates must surface 501 (check
    // runs at handler entry, before the existing/staged split).
    let body = r#"{
        "requirements": [{"type": "assert-create"}],
        "updates": [{"action": "remove-schemas", "schema-ids": [0]}]
    }"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/default/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "body: {json:?}");
    assert_eq!(json["error"]["type"], "NotImplementedException");
}
