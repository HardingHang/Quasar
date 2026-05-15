#![allow(clippy::unwrap_used, clippy::expect_used)]

use deadpool_postgres::{Pool, Runtime};
use postgresql_embedded::PostgreSQL;
use quasar_storage::{
    AssetStore, CasCommitStore, DomainPatch, DomainStore, NamespaceStore, PatchField,
    PgCatalogStore, StoreError, TabularStore, TabularVersionStore, UnifiedQueryStore,
};
use serial_test::serial;
use std::collections::HashMap;
use tokio::sync::OnceCell;

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

/// Phase 2 default domain. The V2 trait surface is gone, but most legacy
/// behavioral tests still operate inside the seeded `default` domain — the
/// adapter shim does the same in Phase 2.
const DEFAULT: &str = "default";

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
                    .create_database("quasar_test_v2")
                    .await
                    .expect("create database failed");

                let url = postgresql.settings().url("quasar_test_v2");
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
    let (store, _) = setup_with_pool().await;
    store
}

async fn setup_with_pool() -> (PgCatalogStore, Pool) {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    let store = PgCatalogStore::new(pool.clone());

    let client = pool.get().await.expect("failed to get client");

    // Clean slate: drop any existing V1, V2, or V3 catalog objects from the
    // shared embedded PostgreSQL instance (OnceCell-cached across runs) so
    // `initialize()` can rebuild a fresh V3 schema.
    let _ = client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS
                refinery_schema_history,
                asset_permissions,
                tabular_asset_versions,
                asset_versions,
                tabular_assets,
                assets,
                namespaces,
                domains,
                tabular_formats,
                asset_types
            CASCADE;
            DROP FUNCTION IF EXISTS ensure_tabular_asset_type() CASCADE;
            DROP FUNCTION IF EXISTS ensure_previous_version_same_asset() CASCADE;
            "#,
        )
        .await;

    store.initialize().await.expect("initialize failed");

    (store, pool)
}

// ── Namespace ──────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_namespace_crud() {
    let store = setup().await;

    let ns = store
        .create_namespace(DEFAULT, "test_ns", None, HashMap::new())
        .await
        .unwrap();
    assert_eq!(ns.name, "test_ns");

    let list = store.list_namespaces(DEFAULT, 0, 100).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "test_ns");

    let got = store.get_namespace(DEFAULT, "test_ns").await.unwrap();
    assert_eq!(got.id, ns.id);

    assert!(store.namespace_exists(DEFAULT, "test_ns").await.unwrap());
    assert!(!store.namespace_exists(DEFAULT, "missing").await.unwrap());

    let err = store
        .create_namespace(DEFAULT, "test_ns", None, HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store.drop_namespace(DEFAULT, "test_ns").await.unwrap();
    assert!(!store.namespace_exists(DEFAULT, "test_ns").await.unwrap());

    let err = store.drop_namespace(DEFAULT, "test_ns").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}

// ── Asset ──────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_asset_crud() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();

    let (asset, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "asset_a",
            "lance",
            "s3://bucket/data/asset_a",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(asset.name, "asset_a");

    let list = store
        .list_tabular_assets(DEFAULT, "ns1", Some("lance"), 0, 1000)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].0.name, "asset_a");

    let (got, _) = store
        .get_tabular_asset(DEFAULT, "ns1", "lance", "asset_a")
        .await
        .unwrap();
    assert_eq!(got.id, asset.id);

    assert!(store.asset_exists(DEFAULT, "ns1", "asset_a").await.unwrap());
    assert!(!store.asset_exists(DEFAULT, "ns1", "missing").await.unwrap());

    let err = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "asset_a",
            "lance",
            "s3://bucket/data/asset_a_dup",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store
        .rename_asset(DEFAULT, "ns1", "asset_a", "asset_b", None)
        .await
        .unwrap();
    assert!(store.asset_exists(DEFAULT, "ns1", "asset_b").await.unwrap());
    assert!(!store.asset_exists(DEFAULT, "ns1", "asset_a").await.unwrap());

    let (_, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "asset_c",
            "lance",
            "s3://bucket/data/asset_c",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let err = store
        .rename_asset(DEFAULT, "ns1", "asset_b", "asset_c", None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store.drop_asset(DEFAULT, "ns1", "asset_b").await.unwrap();
    store.drop_asset(DEFAULT, "ns1", "asset_c").await.unwrap();

    let list = store
        .list_tabular_assets(DEFAULT, "ns1", Some("lance"), 0, 1000)
        .await
        .unwrap();
    assert!(list.is_empty());
}

#[tokio::test]
#[serial]
async fn test_rename_asset_cross_namespace() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_namespace(DEFAULT, "ns2", None, HashMap::new())
        .await
        .unwrap();

    let (_, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "asset_x",
            "lance",
            "s3://bucket/data/asset_x",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    // Cross-namespace rename: ns1 -> ns2.
    store
        .rename_asset(DEFAULT, "ns1", "asset_x", "asset_x", Some("ns2"))
        .await
        .unwrap();

    assert!(!store.asset_exists(DEFAULT, "ns1", "asset_x").await.unwrap());
    assert!(store.asset_exists(DEFAULT, "ns2", "asset_x").await.unwrap());

    // Target namespace does not exist.
    let err = store
        .rename_asset(DEFAULT, "ns2", "asset_x", "asset_x", Some("missing_ns"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // Target namespace already has an asset with the same name.
    let (_, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "asset_y",
            "lance",
            "s3://bucket/data/asset_y",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let err = store
        .rename_asset(DEFAULT, "ns2", "asset_x", "asset_y", Some("ns1"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));
}

// ── Versions ───────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_version_commit_and_load() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();

    let (asset, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "tbl",
            "lance",
            "s3://bucket/data/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let (v1, tav1) = store
        .create_tabular_version(
            asset.id,
            "1",
            Some(1),
            None,
            "s3://bucket/data/tbl/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(v1.version_order, Some(1));
    assert_eq!(tav1.metadata_location, "s3://bucket/data/tbl/_v1.manifest");

    let (v2, _) = store
        .create_tabular_version(
            asset.id,
            "2",
            Some(2),
            Some(v1.id),
            "s3://bucket/data/tbl/_v2.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(v2.version_order, Some(2));
    assert_eq!(v2.previous_version_id, Some(v1.id));

    // Latest tabular version
    let latest = store.get_latest_tabular_version(asset.id).await.unwrap();
    let (latest_v, _) = latest.expect("expected at least one version");
    assert_eq!(latest_v.id, v2.id);

    // Specific by key
    let (loaded, _) = store.get_tabular_version(asset.id, "1").await.unwrap();
    assert_eq!(loaded.id, v1.id);

    // List in ascending order
    let list = store.list_tabular_versions(asset.id).await.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].0.id, v1.id);
    assert_eq!(list[1].0.id, v2.id);
}

#[tokio::test]
#[serial]
async fn test_version_conflict() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    let (asset, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "tbl",
            "lance",
            "s3://bucket/data/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    store
        .create_tabular_version(
            asset.id,
            "1",
            Some(1),
            None,
            "s3://bucket/data/tbl/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    // Duplicate version_key triggers `UNIQUE(asset_id, version_key)`
    let err = store
        .create_tabular_version(
            asset.id,
            "1",
            Some(1),
            None,
            "s3://bucket/data/tbl/_v1b.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));
}

#[tokio::test]
#[serial]
async fn test_previous_version_must_belong_to_same_asset() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    let (asset_a, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "a",
            "lance",
            "s3://bucket/data/a",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let (asset_b, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "b",
            "lance",
            "s3://bucket/data/b",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let (v_a, _) = store
        .create_tabular_version(
            asset_a.id,
            "1",
            Some(1),
            None,
            "s3://bucket/data/a/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    // Referencing a version owned by asset_a from asset_b must conflict via
    // the V3 `trg_asset_versions_previous_same_asset` trigger.
    let err = store
        .create_tabular_version(
            asset_b.id,
            "1",
            Some(1),
            Some(v_a.id),
            "s3://bucket/data/b/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }));
}

#[tokio::test]
#[serial]
async fn test_drop_asset_cascades_tabular_and_versions() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    let (asset, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "tbl",
            "lance",
            "s3://bucket/data/tbl",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let (v1, _) = store
        .create_tabular_version(
            asset.id,
            "1",
            Some(1),
            None,
            "s3://bucket/data/tbl/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    store.drop_asset(DEFAULT, "ns1", "tbl").await.unwrap();

    let err = store.get_tabular_version(asset.id, "1").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
    let _ = v1;
}

#[tokio::test]
#[serial]
async fn test_not_found_errors() {
    let store = setup().await;

    let err = store.get_namespace(DEFAULT, "missing").await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();

    let err = store
        .get_tabular_asset(DEFAULT, "ns1", "lance", "missing")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[tokio::test]
#[serial]
async fn test_update_namespace_properties() {
    let store = setup().await;

    let mut initial_props = HashMap::new();
    initial_props.insert("env".to_string(), "prod".to_string());
    initial_props.insert("team".to_string(), "data".to_string());
    store
        .create_namespace(DEFAULT, "ns1", Some("initial".into()), initial_props)
        .await
        .unwrap();

    let mut updates = HashMap::new();
    updates.insert("env".to_string(), "staging".to_string());
    updates.insert("region".to_string(), "us-east-1".to_string());
    let removals = vec!["team".to_string(), "missing-key".to_string()];

    let updated = store
        .update_namespace(
            DEFAULT,
            "ns1",
            PatchField::Value("updated".into()),
            &removals,
            &updates,
        )
        .await
        .unwrap();
    assert_eq!(updated.comment.as_deref(), Some("updated"));
    assert_eq!(
        updated.properties.get("env").map(String::as_str),
        Some("staging")
    );
    assert_eq!(
        updated.properties.get("region").map(String::as_str),
        Some("us-east-1")
    );
    assert!(!updated.properties.contains_key("team"));

    // Missing comment → no change.
    let again = store
        .update_namespace(DEFAULT, "ns1", PatchField::Missing, &[], &HashMap::new())
        .await
        .unwrap();
    assert_eq!(again.comment.as_deref(), Some("updated"));

    // Null comment → cleared.
    let cleared = store
        .update_namespace(DEFAULT, "ns1", PatchField::Null, &[], &HashMap::new())
        .await
        .unwrap();
    assert!(cleared.comment.is_none());

    let err = store
        .update_namespace(
            DEFAULT,
            "missing",
            PatchField::Missing,
            &[],
            &HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
}

// ── New V3 P0 tests (V3_DESIGN §11.2) ──────────────────────────────────────

#[tokio::test]
#[serial]
async fn test_domain_crud_complete_flow() {
    let store = setup().await;

    // The default seed is already present, so a fresh create should add a
    // second row.
    let created = store
        .create_domain(
            "prod",
            Some("production catalog".into()),
            HashMap::from([("env".into(), "prod".into())]),
            Some("s3".into()),
            serde_json::json!({"bucket": "prod-warehouse"}),
            Some("s3://prod-warehouse/".into()),
            Some("data-platform".into()),
        )
        .await
        .unwrap();
    assert_eq!(created.name, "prod");
    assert_eq!(created.comment.as_deref(), Some("production catalog"));
    assert!(
        created
            .storage_config
            .get("bucket")
            .and_then(|v| v.as_str())
            == Some("prod-warehouse")
    );

    let listed = store.list_domains(0, 100).await.unwrap();
    let names: Vec<&str> = listed.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"prod"));

    let got = store.get_domain("prod").await.unwrap();
    assert_eq!(got.id, created.id);
    assert!(store.domain_exists("prod").await.unwrap());
    assert!(!store.domain_exists("nope").await.unwrap());

    // Update: drop env, add owner-of-record, change comment.
    let patched = store
        .update_domain(
            "prod",
            DomainPatch {
                comment: PatchField::Value("updated production".into()),
                property_removals: vec!["env".into()],
                property_updates: HashMap::from([("owner".into(), "team-a".into())]),
                storage_type: PatchField::Missing,
                storage_config: PatchField::Missing,
                warehouse: PatchField::Missing,
                owner: PatchField::Missing,
            },
        )
        .await
        .unwrap();
    assert_eq!(patched.comment.as_deref(), Some("updated production"));
    assert!(!patched.properties.contains_key("env"));
    assert_eq!(
        patched.properties.get("owner").map(String::as_str),
        Some("team-a")
    );

    // Duplicate create → AlreadyExists.
    let err = store
        .create_domain(
            "prod",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    store.drop_domain("prod").await.unwrap();
    assert!(!store.domain_exists("prod").await.unwrap());
}

#[tokio::test]
#[serial]
async fn test_namespace_same_name_across_domains() {
    let store = setup().await;

    store
        .create_domain(
            "prod",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap();
    store
        .create_domain(
            "staging",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap();

    store
        .create_namespace("prod", "analytics", None, HashMap::new())
        .await
        .unwrap();
    // Same namespace name under a different domain must succeed (V3
    // namespaces are unique on `(domain_id, name)`, not on `name` globally).
    store
        .create_namespace("staging", "analytics", None, HashMap::new())
        .await
        .unwrap();

    let prod_ns = store.get_namespace("prod", "analytics").await.unwrap();
    let stg_ns = store.get_namespace("staging", "analytics").await.unwrap();
    assert_ne!(prod_ns.id, stg_ns.id);
    assert_ne!(prod_ns.domain_id, stg_ns.domain_id);
}

#[tokio::test]
#[serial]
async fn test_non_empty_domain_delete_returns_conflict() {
    let store = setup().await;

    store
        .create_domain(
            "biz",
            None,
            HashMap::new(),
            None,
            serde_json::json!({}),
            None,
            None,
        )
        .await
        .unwrap();
    store
        .create_namespace("biz", "ledger", None, HashMap::new())
        .await
        .unwrap();

    let err = store.drop_domain("biz").await.unwrap_err();
    match err {
        StoreError::DomainNotEmpty { domain } => assert_eq!(domain, "biz"),
        other => panic!("expected DomainNotEmpty, got {:?}", other),
    }

    // Non-empty Namespace delete returns NamespaceNotEmpty.
    store
        .create_tabular_asset(
            "biz",
            "ledger",
            "events",
            "lance",
            "s3://bucket/biz/ledger/events",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let err = store.drop_namespace("biz", "ledger").await.unwrap_err();
    match err {
        StoreError::NamespaceNotEmpty { namespace } => assert_eq!(namespace, "ledger"),
        other => panic!("expected NamespaceNotEmpty, got {:?}", other),
    }

    // After clearing children, the deletes succeed.
    store.drop_asset("biz", "ledger", "events").await.unwrap();
    store.drop_namespace("biz", "ledger").await.unwrap();
    store.drop_domain("biz").await.unwrap();
    assert!(!store.domain_exists("biz").await.unwrap());
}

#[tokio::test]
#[serial]
async fn test_asset_active_uniqueness_within_namespace() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "users",
            "iceberg",
            "s3://bucket/ns1/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    // V3 enforces active asset name uniqueness within a namespace
    // regardless of format — even when the second create requests a
    // different format.
    let err = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "users",
            "lance",
            "lance://ns1/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyExists(_)));

    // Same name in a different namespace is fine.
    store
        .create_namespace(DEFAULT, "ns2", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_tabular_asset(
            DEFAULT,
            "ns2",
            "users",
            "lance",
            "lance://ns2/users",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[serial]
async fn test_get_latest_version_uses_version_order() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    let (asset, _) = store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "events",
            "lance",
            "s3://bucket/ns1/events",
            None,
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    // Insert versions out of order to make sure the "latest" query relies
    // on `version_order DESC` rather than on version_key lexicographic
    // sort or insertion order.
    let (v1, _) = store
        .create_tabular_version(
            asset.id,
            "1",
            Some(1),
            None,
            "s3://bucket/ns1/events/_v1.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let (v10, _) = store
        .create_tabular_version(
            asset.id,
            "10",
            Some(10),
            None,
            "s3://bucket/ns1/events/_v10.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();
    let (v2, _) = store
        .create_tabular_version(
            asset.id,
            "2",
            Some(2),
            None,
            "s3://bucket/ns1/events/_v2.manifest",
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let latest = store
        .get_latest_tabular_version(asset.id)
        .await
        .unwrap()
        .expect("expected at least one version");
    let (latest_version, _) = latest;
    assert_eq!(latest_version.id, v10.id);
    assert_eq!(latest_version.version_order, Some(10));
    let _ = (v1, v2);
}

// ── Unified + CAS smoke tests (V3 traits) ──────────────────────────────────

#[tokio::test]
#[serial]
async fn test_unified_query_pairs_asset_with_tabular() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "users",
            "iceberg",
            "s3://bucket/ns1/users",
            Some("s3://bucket/ns1/users/metadata/00000.metadata.json"),
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let list = store
        .list_assets_unified(DEFAULT, "ns1", Some("iceberg"), None, 0, 100)
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    let (asset, tabular) = &list[0];
    assert_eq!(asset.name, "users");
    let tabular = tabular.as_ref().expect("expected tabular extension");
    assert_eq!(tabular.format, "iceberg");

    let (asset2, tabular2) = store
        .get_asset_unified(DEFAULT, "ns1", "users")
        .await
        .unwrap();
    assert_eq!(asset2.id, asset.id);
    assert!(tabular2.is_some());
}

#[tokio::test]
#[serial]
async fn test_cas_commit_optimistic_concurrency() {
    let store = setup().await;

    store
        .create_namespace(DEFAULT, "ns1", None, HashMap::new())
        .await
        .unwrap();
    let initial = "s3://bucket/ns1/orders/metadata/00000.metadata.json";
    store
        .create_tabular_asset(
            DEFAULT,
            "ns1",
            "orders",
            "iceberg",
            "s3://bucket/ns1/orders",
            Some(initial),
            None,
            HashMap::new(),
        )
        .await
        .unwrap();

    let next = "s3://bucket/ns1/orders/metadata/00001.metadata.json";
    let removals: Vec<String> = vec![];
    let updates: HashMap<String, String> = HashMap::from([("phase".into(), "2".into())]);

    store
        .cas_update_metadata_location(
            DEFAULT, "ns1", "orders", "iceberg", initial, next, None, &removals, &updates,
        )
        .await
        .unwrap();

    // Stale expected_location should now conflict.
    let err = store
        .cas_update_metadata_location(
            DEFAULT,
            "ns1",
            "orders",
            "iceberg",
            initial,
            "s3://bucket/ns1/orders/metadata/00002.metadata.json",
            None,
            &removals,
            &HashMap::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }));

    // Property delta from the successful CAS landed on the asset row.
    let (asset, _) = store
        .get_tabular_asset(DEFAULT, "ns1", "iceberg", "orders")
        .await
        .unwrap();
    assert_eq!(asset.properties.get("phase").map(String::as_str), Some("2"));
}
