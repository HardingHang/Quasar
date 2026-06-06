//! Real concurrent CAS-conflict tests against an isolated PostgreSQL.
//!
//! Existing `postgresql_embedded + serial_test` integration tests are
//! sequential by design (a single shared embedded PG, serialized via
//! `#[serial]`), so they cannot exercise true parallel writers.
//!
//! These tests spin up an isolated PostgreSQL container per test via
//! `testcontainers`, then drive two `commit_table` calls through
//! `tokio::join!` against the same expected `metadata_location`. Only one
//! is allowed to win the row-level CAS in storage; the other must surface
//! `409 CommitFailedException`. Requires a reachable Docker daemon.

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use axum::body::Body;
use axum::http::{Request, StatusCode};
use deadpool_postgres::{Pool, Runtime};
use http_body_util::BodyExt;
use quasar_adapter::iceberg;
use quasar_core::{CatalogStore, IcebergStagingStore, NamespaceStore};
use quasar_storage::PgCatalogStore;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use tower::ServiceExt;

/// Boot a fresh PostgreSQL container, initialize schema, return the
/// container guard (kept alive by the caller) plus a configured store.
///
/// Pins the image to `postgres:16` (rather than the crate's default
/// `postgres:11-alpine`) because `postgres:16` is the version commonly
/// available locally and avoids hitting the public registry on each run.
async fn boot_isolated_pg() -> (ContainerAsync<Postgres>, PgCatalogStore) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("start postgres container");
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let config = url
        .parse::<tokio_postgres::Config>()
        .expect("invalid database URL");
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    let pool = Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool");
    let store = PgCatalogStore::new(pool);
    store.initialize().await.expect("initialize schema");
    (container, store)
}

fn make_app(store: PgCatalogStore) -> axum::Router {
    use axum::Extension;
    let store: Arc<dyn CatalogStore> = Arc::new(store);
    let config = iceberg::IcebergConfig {
        default_warehouse: "default".to_string(),
        ..Default::default()
    };
    iceberg::routes().layer(Extension(config)).with_state(store)
}

async fn body_json(response: axum::response::Response) -> Value {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap_or_else(|_| {
        let text = String::from_utf8_lossy(&body).to_string();
        panic!("non-JSON response body: {text}")
    })
}

async fn post_json(app: axum::Router, path: &str, body: String) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri(path)
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Two concurrent existing-table commits MUST result in exactly one 200 and
/// one 409. They share the same expected pre-commit `metadata_location` —
/// the row-level CAS in storage breaks the tie.
#[tokio::test]
async fn test_concurrent_commit_one_wins_one_409() {
    let (_pg, store) = boot_isolated_pg().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    let app = make_app(store);

    // Create table once.
    let create = post_json(
        app.clone(),
        "/iceberg/v1/default/namespaces/prod/tables",
        r#"{"name": "users"}"#.to_string(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);

    // Two commits, each sets a different property. Both pass requirements
    // (none specified); only one can win the CAS row lock.
    let body_a = r#"{
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"writer": "a"}}]
    }"#
    .to_string();
    let body_b = r#"{
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"writer": "b"}}]
    }"#
    .to_string();

    let (resp_a, resp_b) = tokio::join!(
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body_a,
        ),
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body_b,
        ),
    );
    let status_a = resp_a.status();
    let status_b = resp_b.status();
    let body_a = body_json(resp_a).await;
    let body_b = body_json(resp_b).await;

    let mut statuses = [status_a, status_b];
    statuses.sort_by_key(|s| s.as_u16());
    assert_eq!(
        statuses,
        [StatusCode::OK, StatusCode::CONFLICT],
        "expected exactly one 200 and one 409; got A={status_a} ({body_a:?}), B={status_b} ({body_b:?})"
    );

    let loser = if status_a == StatusCode::CONFLICT {
        &body_a
    } else {
        &body_b
    };
    assert_eq!(loser["error"]["type"], "CommitFailedException");
}

/// Two concurrent staged-create commits for the same table MUST yield
/// exactly one 200 and one 409. The storage-layer transaction in
/// `commit_staged_table` is the serialization point.
#[tokio::test]
async fn test_concurrent_staged_commit_one_wins() {
    let (_pg, store) = boot_isolated_pg().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();

    // stage-create directly through the store so we don't need to write to
    // an object store before commit.
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

    let app = make_app(store);

    let body = r#"{
        "requirements": [{"type": "assert-create"}],
        "updates": [{"action": "set-properties", "updates": {"k": "v"}}]
    }"#;

    let (resp_a, resp_b) = tokio::join!(
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body.to_string(),
        ),
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body.to_string(),
        ),
    );
    let status_a = resp_a.status();
    let status_b = resp_b.status();
    let body_a = body_json(resp_a).await;
    let body_b = body_json(resp_b).await;

    let mut statuses = [status_a, status_b];
    statuses.sort_by_key(|s| s.as_u16());
    assert_eq!(
        statuses,
        [StatusCode::OK, StatusCode::CONFLICT],
        "expected one staged commit to win; got A={status_a} ({body_a:?}), B={status_b} ({body_b:?})"
    );
}

/// Sanity check that after the racing pair, the table is in a consistent
/// state: load_table returns 200 and the property reflects whichever
/// writer won.
#[tokio::test]
async fn test_concurrent_commit_leaves_consistent_state() {
    let (_pg, store) = boot_isolated_pg().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();
    let app = make_app(store);

    let create = post_json(
        app.clone(),
        "/iceberg/v1/default/namespaces/prod/tables",
        r#"{"name": "users"}"#.to_string(),
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);

    let body_a = r#"{
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"writer": "alpha"}}]
    }"#
    .to_string();
    let body_b = r#"{
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"writer": "beta"}}]
    }"#
    .to_string();

    let _ = tokio::join!(
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body_a,
        ),
        post_json(
            app.clone(),
            "/iceberg/v1/default/namespaces/prod/tables/users",
            body_b,
        ),
    );

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
    let writer = json["metadata"]["properties"]["writer"]
        .as_str()
        .expect("writer property set");
    assert!(
        writer == "alpha" || writer == "beta",
        "expected one writer to have won; got {writer:?}"
    );
}

/// Two concurrent multi-table transactions updating the same table MUST
/// result in exactly one 204 and one 409.
#[tokio::test]
async fn test_concurrent_transaction_one_wins_one_409() {
    let (_pg, store) = boot_isolated_pg().await;
    store
        .create_namespace("default", "prod", None, HashMap::new())
        .await
        .unwrap();
    let app = make_app(store);

    // Create two tables.
    let create_users = post_json(
        app.clone(),
        "/iceberg/v1/default/namespaces/prod/tables",
        r#"{"name": "users"}"#.to_string(),
    )
    .await;
    assert_eq!(create_users.status(), StatusCode::OK);

    let create_orders = post_json(
        app.clone(),
        "/iceberg/v1/default/namespaces/prod/tables",
        r#"{"name": "orders"}"#.to_string(),
    )
    .await;
    assert_eq!(create_orders.status(), StatusCode::OK);

    // Two concurrent transactions, each updates both tables.
    let body_a = r#"{
        "table-changes": [
            {
                "identifier": {"namespace": ["prod"], "name": "users"},
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"txn": "a"}}]
            },
            {
                "identifier": {"namespace": ["prod"], "name": "orders"},
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"txn": "a"}}]
            }
        ]
    }"#;

    let body_b = r#"{
        "table-changes": [
            {
                "identifier": {"namespace": ["prod"], "name": "users"},
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"txn": "b"}}]
            },
            {
                "identifier": {"namespace": ["prod"], "name": "orders"},
                "requirements": [],
                "updates": [{"action": "set-properties", "updates": {"txn": "b"}}]
            }
        ]
    }"#;

    let (resp_a, resp_b) = tokio::join!(
        post_json(
            app.clone(),
            "/iceberg/v1/default/transactions/commit",
            body_a.to_string(),
        ),
        post_json(
            app.clone(),
            "/iceberg/v1/default/transactions/commit",
            body_b.to_string(),
        ),
    );
    let status_a = resp_a.status();
    let status_b = resp_b.status();

    let mut statuses = [status_a, status_b];
    statuses.sort_by_key(|s| s.as_u16());
    assert_eq!(
        statuses,
        [StatusCode::NO_CONTENT, StatusCode::CONFLICT],
        "expected exactly one 204 and one 409; got A={status_a}, B={status_b}"
    );

    let loser_resp = if status_a == StatusCode::CONFLICT {
        resp_a
    } else {
        resp_b
    };
    let loser_json = body_json(loser_resp).await;
    assert_eq!(loser_json["error"]["type"], "CommitFailedException");

    // Verify both tables are in a consistent state (same txn value on both).
    let load_users = app
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
    let load_orders = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/iceberg/v1/default/namespaces/prod/tables/orders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let json_users = body_json(load_users).await;
    let json_orders = body_json(load_orders).await;
    let txn_users = json_users["metadata"]["properties"]["txn"].as_str();
    let txn_orders = json_orders["metadata"]["properties"]["txn"].as_str();

    // The winner should have updated both tables atomically.
    assert_eq!(
        txn_users, txn_orders,
        "both tables should have the same txn value after atomic commit; users={txn_users:?}, orders={txn_orders:?}"
    );
    assert!(
        txn_users == Some("a") || txn_users == Some("b"),
        "expected one transaction to have won; got {txn_users:?}"
    );
}
