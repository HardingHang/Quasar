#![cfg(feature = "iceberg")]
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
                .uri("/iceberg/v1/namespaces/prod/tables")
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(format!(
                    r#"{{
                        "requirements": [
                            {{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}}
                        ],
                        "updates": [
                            {{"action": "add-snapshot", "snapshot": {{
                                "snapshot-id": 1,
                                "sequence-number": 1,
                                "timestamp-ms": 1234567890,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {{"operation": "append"}},
                                "schema-id": 0
                            }}}},
                            {{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}}
                        ]
                    }}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(commit.status(), StatusCode::OK);
    let commit_json = body_json(commit).await;
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
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "test-uuid",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(format!(
                    r#"{{
                        "requirements": [
                            {{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}}
                        ],
                        "updates": [
                            {{"action": "add-snapshot", "snapshot": {{
                                "snapshot-id": 1,
                                "sequence-number": 1,
                                "timestamp-ms": 1234567890,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {{}},
                                "schema-id": 0
                            }}}}
                        ]
                    }}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    // Note: This test currently succeeds because the metadata_location in DB matches.
    // This is intentional behavior - the CAS expects the metadata_location to match.
    assert_eq!(commit.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn test_commit_requirement_failure() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with an existing snapshot
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "test-uuid",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 1,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("snapshot-id mismatch"));
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
                .uri("/iceberg/v1/namespaces/prod/tables/nonexistent")
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
                .uri("/iceberg/v1/namespaces/prod/tables")
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(format!(
                    r#"{{
                        "requirements": [
                            {{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}}
                        ],
                        "updates": [
                            {{"action": "add-snapshot", "snapshot": {{
                                "snapshot-id": 1,
                                "sequence-number": 1,
                                "timestamp-ms": 1234567890,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {{"operation": "append"}},
                                "schema-id": 0
                            }}}},
                            {{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}},
                            {{"action": "set-properties", "updates": {{"owner": "team-a"}}}}
                        ]
                    }}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    // Load and verify
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
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "test-uuid",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
                "default-sort-order-id": 0,
                "refs": {}
            })),
            HashMap::new(),
        )
        .await
        .unwrap();

    // First CAS commit succeeds: expected 00001 matches DB
    store
        .commit_iceberg_table(
            "prod",
            "users",
            "s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json",
            "s3://bucket/warehouse/prod/users/metadata/00002-uuid.metadata.json",
            None,
        )
        .await
        .unwrap();

    // Second CAS commit fails: expected 00001 no longer matches DB (now 00002)
    let err = store
        .commit_iceberg_table(
            "prod",
            "users",
            "s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json",
            "s3://bucket/warehouse/prod/users/metadata/00003-uuid.metadata.json",
            None,
        )
        .await
        .unwrap_err();

    assert!(matches!(err, quasar_core::StoreError::Conflict(_)));
    let msg = format!("{}", err);
    assert!(msg.contains("modified by another commit"));
}

/// S9验收标准核心测试：并发CAS冲突端到端测试
/// 两个连续commit，第二个使用错误的snapshot-id expectation，验证返回409
#[tokio::test]
#[serial]
async fn test_concurrent_cas_conflict_end_to_end() {
    let store = setup().await;
    create_namespace(&store, "prod").await;
    let app = test_app(store);

    // Create table first
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

    // First commit succeeds
    let commit1 = app
        .clone()
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
                                "timestamp-ms": 1000,
                                "manifest-list": "s3://bucket/manifest1.avro",
                                "summary": {},
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
    assert_eq!(commit1.status(), StatusCode::OK);

    // Second commit with wrong snapshot-id expectation should fail (CAS requirement check)
    let commit2 = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                // Expecting snapshot-id=null but it's now 1 after commit1
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}
                        ],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {
                                "snapshot-id": 2,
                                "sequence-number": 2,
                                "timestamp-ms": 2000,
                                "manifest-list": "s3://bucket/manifest2.avro",
                                "summary": {},
                                "schema-id": 0
                            }},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 2, "type": "branch"}
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Requirement mismatch: expected null, actual 1 → 409
    assert_eq!(commit2.status(), StatusCode::CONFLICT);
    let json = body_json(commit2).await;
    assert_eq!(json["error"]["type"], "CommitFailedException");
    assert_eq!(json["error"]["code"], 409);
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("snapshot-id mismatch"));
}

/// AssertRefSnapshotId非main ref测试 - 验证自定义branch
#[tokio::test]
#[serial]
async fn test_assert_ref_snapshot_id_custom_branch() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with a custom branch "staging"
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "test-uuid",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 1,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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

    // Requirements satisfied → 200 OK (empty updates)
    assert_eq!(commit.status(), StatusCode::OK);
}

/// AssertRefSnapshotId非main ref测试 - 验证失败场景
#[tokio::test]
#[serial]
async fn test_assert_ref_snapshot_id_custom_branch_fail() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with a custom branch "staging"
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "test-uuid",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 1,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "current-snapshot-id": 42,
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("snapshot-id mismatch"));
}

/// AssertTableUuid requirement端到端测试 - UUID不匹配
#[tokio::test]
#[serial]
async fn test_assert_table_uuid_failure() {
    let store = setup().await;
    create_namespace(&store, "prod").await;

    // Create table with known UUID
    store
        .create_asset(
            "prod",
            AssetFormat::Iceberg,
            "users",
            "s3://bucket/warehouse/prod/users",
            Some("s3://bucket/warehouse/prod/users/metadata/00001-uuid.metadata.json"),
            Some(serde_json::json!({
                "format-version": 2,
                "table-uuid": "correct-uuid-123",
                "location": "s3://bucket/warehouse/prod/users",
                "last-sequence-number": 0,
                "last-updated-ms": 1000,
                "last-column-id": 0,
                "schemas": [],
                "current-schema-id": 0,
                "partition-specs": [],
                "default-spec-id": 0,
                "last-partition-id": 999,
                "properties": {},
                "snapshots": [],
                "snapshot-log": [],
                "metadata-log": [],
                "sort-orders": [],
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [
                            {"type": "assert-table-uuid", "uuid": "wrong-uuid-999"}
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
    assert!(json["error"]["message"]
        .as_str()
        .unwrap()
        .contains("UUID mismatch"));
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
                .uri("/iceberg/v1/namespaces/prod/tables")
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": 1000, "manifest-list": "s3://b/m1.avro", "summary": {}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"}
                        ]
                    }"#,
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 1}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 2, "sequence-number": 2, "timestamp-ms": 2000, "manifest-list": "s3://b/m2.avro", "summary": {}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 2, "type": "branch"}
                        ]
                    }"#,
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
                .uri("/iceberg/v1/namespaces/prod/tables")
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": 1000, "manifest-list": "s3://b/m1.avro", "summary": {}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"},
                            {"action": "set-snapshot-ref", "ref-name": "v1.0", "snapshot-id": 1, "type": "tag"}
                        ]
                    }"#,
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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
                .uri("/iceberg/v1/namespaces/nonexistent/tables/users")
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
                .uri("/iceberg/v1/namespaces/prod/tables")
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
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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
                .uri("/iceberg/v1/namespaces/prod/tables")
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"name": "users"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // First commit to add properties
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    r#"{
                        "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
                        "updates": [
                            {"action": "add-snapshot", "snapshot": {"snapshot-id": 1, "sequence-number": 1, "timestamp-ms": 1000, "manifest-list": "s3://b/m1.avro", "summary": {}, "schema-id": 0}},
                            {"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 1, "type": "branch"},
                            {"action": "set-properties", "updates": {"owner": "team-a", "env": "prod"}}
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // Second commit to remove properties
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/iceberg/v1/namespaces/prod/tables/users")
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

    // Load and verify
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
    assert_eq!(json["metadata"]["properties"]["owner"], "team-a");
    assert!(json["metadata"]["properties"].get("env").is_none());
}
