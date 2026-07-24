//! Integration tests for the generic store traits (DESIGN §4.3):
//! Domain / Namespace / Asset / Version / Tag / Registry / Tabular / CAS /
//! UnifiedQuery, plus soft-delete lifecycle and CAS concurrency.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use quasar_storage::{
    AssetFilter, AssetPatch, AssetQuery, AssetStore, AssetTypeStore, CasCommitStore, CatalogError,
    CreateAsset, CreateDomain, CreateNamespace, CreateVersion, DomainPatch, DomainStore,
    NamespacePatch, NamespaceStore, PatchField, RegisterAssetType, RegisterFormat, TabularStore,
    TagStore, UnifiedQueryStore, VersionStore,
};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const DEFAULT: &str = "default";

// ── DomainStore ────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn domain_crud_and_pagination() {
    let store = common::store().await;

    let created = store
        .create_domain(CreateDomain {
            name: "alpha".to_string(),
            comment: Some("first".to_string()),
            properties: Some(serde_json::json!({"tier": "gold"})),
            storage_type: Some("s3".to_string()),
            storage_config: Some(serde_json::json!({"bucket": "b1"})),
            warehouse: Some("s3://b1/wh".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(created.name, "alpha");
    assert_eq!(created.storage_type.as_deref(), Some("s3"));

    let got = store.get_domain("alpha").await.unwrap();
    assert_eq!(got.id, created.id);
    assert_eq!(got.warehouse.as_deref(), Some("s3://b1/wh"));

    // Duplicate name -> AlreadyExists.
    let dup = store
        .create_domain(CreateDomain {
            name: "alpha".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Unknown domain -> NotFound.
    let missing = store.get_domain("nope").await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));

    // Pagination: ordered by name, seeded `default` participates.
    store
        .create_domain(CreateDomain {
            name: "zeta".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    let page1 = store.list_domains(0, 2).await.unwrap();
    let page2 = store.list_domains(2, 2).await.unwrap();
    let names: Vec<&str> = page1
        .iter()
        .chain(page2.iter())
        .map(|d| d.name.as_str())
        .collect();
    assert_eq!(names, vec!["alpha", "default", "zeta"]);

    // Invalid storage_type -> CHECK violation -> Validation.
    let bad = store
        .create_domain(CreateDomain {
            name: "badtype".to_string(),
            storage_type: Some("ftp".to_string()),
            ..Default::default()
        })
        .await;
    assert!(matches!(bad, Err(CatalogError::Validation(_))));

    // Delete an empty domain, then delete again -> NotFound.
    store.delete_domain("zeta").await.unwrap();
    let gone = store.delete_domain("zeta").await;
    assert!(matches!(gone, Err(CatalogError::NotFound(_))));
}

#[tokio::test]
#[serial]
async fn domain_patch_three_states() {
    let store = common::store().await;
    store
        .create_domain(CreateDomain {
            name: "patchme".to_string(),
            comment: Some("keep".to_string()),
            warehouse: Some("s3://b/wh".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

    // Set: comment overwritten; NoChange: warehouse kept.
    let updated = store
        .update_domain(
            "patchme",
            DomainPatch {
                comment: PatchField::Set("changed".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.comment.as_deref(), Some("changed"));
    assert_eq!(updated.warehouse.as_deref(), Some("s3://b/wh"));

    // Unset: comment cleared to NULL.
    let cleared = store
        .update_domain(
            "patchme",
            DomainPatch {
                comment: PatchField::Unset,
                warehouse: PatchField::Unset,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(cleared.comment.is_none());
    assert!(cleared.warehouse.is_none());

    // Set properties.
    let with_props = store
        .update_domain(
            "patchme",
            DomainPatch {
                properties: PatchField::Set(serde_json::json!({"k": "v"})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(with_props.properties.unwrap()["k"], "v");

    // Patching a missing domain -> NotFound.
    let missing = store.update_domain("nope", DomainPatch::default()).await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));
}

#[tokio::test]
#[serial]
async fn delete_non_empty_domain_conflicts() {
    let store = common::store().await;
    store
        .create_domain(CreateDomain {
            name: "occupied".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    common::make_namespace(&store, "occupied", "ns").await;

    let err = store.delete_domain("occupied").await;
    assert!(matches!(err, Err(CatalogError::Conflict(_))));
}

// ── NamespaceStore ─────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn namespace_hierarchy_and_prefix_listing() {
    let store = common::store().await;

    // Intermediate nodes are created implicitly (FR-N2).
    let leaf = store
        .create_namespace(
            DEFAULT,
            "analytics/teams/finance",
            CreateNamespace::default(),
        )
        .await
        .unwrap();
    assert_eq!(leaf.path, "analytics/teams/finance");
    assert_eq!(leaf.depth, 3);

    let mid = store
        .get_namespace(DEFAULT, "analytics/teams")
        .await
        .unwrap();
    assert_eq!(mid.depth, 2);
    store.get_namespace(DEFAULT, "analytics").await.unwrap();

    // resolve_path maps the protocol path to the same entity.
    let resolved = store
        .resolve_path(DEFAULT, "analytics/teams")
        .await
        .unwrap();
    assert_eq!(resolved.id, mid.id);

    // Prefix listing: Some("analytics") covers the whole subtree.
    let subtree = store
        .list_namespaces(DEFAULT, Some("analytics"), 0, 100)
        .await
        .unwrap();
    let paths: Vec<&str> = subtree.iter().map(|n| n.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["analytics", "analytics/teams", "analytics/teams/finance"]
    );

    // Prefix that matches no path yields an empty page.
    let empty = store
        .list_namespaces(DEFAULT, Some("missing"), 0, 100)
        .await
        .unwrap();
    assert!(empty.is_empty());

    // Duplicate path -> AlreadyExists.
    let dup = store
        .create_namespace(DEFAULT, "analytics/teams", CreateNamespace::default())
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Missing domain -> NotFound.
    let no_domain = store
        .create_namespace("ghost", "ns", CreateNamespace::default())
        .await;
    assert!(matches!(no_domain, Err(CatalogError::NotFound(_))));

    // Three-state patch.
    let patched = store
        .update_namespace(
            DEFAULT,
            "analytics",
            NamespacePatch {
                comment: PatchField::Set("root".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(patched.comment.as_deref(), Some("root"));
    let cleared = store
        .update_namespace(
            DEFAULT,
            "analytics",
            NamespacePatch {
                comment: PatchField::Unset,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(cleared.comment.is_none());
}

#[tokio::test]
#[serial]
async fn namespace_delete_rules() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "parent/child").await;

    // Non-empty (child namespace) -> Conflict.
    let err = store.delete_namespace(DEFAULT, "parent").await;
    assert!(matches!(err, Err(CatalogError::Conflict(_))));

    // Non-empty (holds an asset) -> Conflict.
    store
        .create_asset(common::asset_input(
            DEFAULT,
            "parent/child",
            "t1",
            "table",
            None,
        ))
        .await
        .unwrap();
    let err = store.delete_namespace(DEFAULT, "parent/child").await;
    assert!(matches!(err, Err(CatalogError::Conflict(_))));

    // Deleting the asset frees the namespace; deleting the leaf frees the parent.
    let asset = store
        .get_asset_by_name(DEFAULT, "parent/child", "t1")
        .await
        .unwrap();
    store.hard_delete_asset(asset.id).await.unwrap();
    store
        .delete_namespace(DEFAULT, "parent/child")
        .await
        .unwrap();
    store.delete_namespace(DEFAULT, "parent").await.unwrap();

    let gone = store.delete_namespace(DEFAULT, "parent").await;
    assert!(matches!(gone, Err(CatalogError::NotFound(_))));
}

// ── AssetStore ─────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn asset_create_get_and_validation() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;

    let asset = store
        .create_asset(CreateAsset {
            properties: Some(serde_json::json!({"owner": "alice"})),
            comment: Some("c".to_string()),
            ..common::asset_input(DEFAULT, "ns", "users", "table", Some("iceberg"))
        })
        .await
        .unwrap();
    assert_eq!(asset.format.as_deref(), Some("iceberg"));
    assert!(asset.current_version_key.is_none());
    assert!(asset.deleted_at.is_none());

    // get by id / by name return the same row.
    let by_id = store.get_asset(asset.id).await.unwrap();
    let by_name = store
        .get_asset_by_name(DEFAULT, "ns", "users")
        .await
        .unwrap();
    assert_eq!(by_id.id, by_name.id);

    // Duplicate active name in the same namespace -> AlreadyExists.
    let dup = store
        .create_asset(common::asset_input(DEFAULT, "ns", "users", "table", None))
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Unregistered asset type -> Validation (FK mapped by call site).
    let bad_type = store
        .create_asset(common::asset_input(DEFAULT, "ns", "x1", "model", None))
        .await;
    assert!(matches!(bad_type, Err(CatalogError::Validation(_))));

    // Unregistered format -> Validation.
    let bad_format = store
        .create_asset(common::asset_input(
            DEFAULT,
            "ns",
            "x2",
            "table",
            Some("parquet"),
        ))
        .await;
    assert!(matches!(bad_format, Err(CatalogError::Validation(_))));

    // Missing namespace -> NotFound.
    let no_ns = store
        .create_asset(common::asset_input(DEFAULT, "ghost", "x3", "table", None))
        .await;
    assert!(matches!(no_ns, Err(CatalogError::NotFound(_))));

    let missing = store.get_asset(Uuid::new_v4()).await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));
    let missing_name = store.get_asset_by_name(DEFAULT, "ns", "ghost").await;
    assert!(matches!(missing_name, Err(CatalogError::NotFound(_))));
}

#[tokio::test]
#[serial]
async fn asset_list_filters() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;

    let alice = store
        .create_asset(CreateAsset {
            properties: Some(serde_json::json!({"owner": "alice", "env": "prod"})),
            ..common::asset_input(DEFAULT, "ns", "t_alice", "table", Some("iceberg"))
        })
        .await
        .unwrap();
    store
        .create_asset(CreateAsset {
            properties: Some(serde_json::json!({"owner": "bob"})),
            ..common::asset_input(DEFAULT, "ns", "t_bob", "table", Some("lance"))
        })
        .await
        .unwrap();
    store
        .create_asset(common::asset_input(
            DEFAULT,
            "ns",
            "v1",
            "view",
            Some("iceberg"),
        ))
        .await
        .unwrap();

    let in_ns = |extra: AssetFilter| AssetFilter {
        domain: Some(DEFAULT.to_string()),
        namespace: Some("ns".to_string()),
        ..extra
    };

    // No filter -> all three.
    let all = store
        .list_assets(in_ns(AssetFilter::default()), 0, 100)
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // asset_type filter.
    let tables = store
        .list_assets(
            in_ns(AssetFilter {
                asset_type: Some("table".to_string()),
                ..Default::default()
            }),
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(tables.len(), 2);

    // format filter.
    let lance = store
        .list_assets(
            in_ns(AssetFilter {
                format: Some("lance".to_string()),
                ..Default::default()
            }),
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(lance.len(), 1);
    assert_eq!(lance[0].name, "t_bob");

    // properties exact-match filter (AND across keys).
    let prod_alice = store
        .list_assets(
            in_ns(AssetFilter {
                properties: HashMap::from([
                    ("owner".to_string(), "alice".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ]),
                ..Default::default()
            }),
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(prod_alice.len(), 1);
    assert_eq!(prod_alice[0].id, alice.id);

    // include_deleted=false hides soft-deleted rows (default).
    store.soft_delete_asset(alice.id).await.unwrap();
    let active = store
        .list_assets(in_ns(AssetFilter::default()), 0, 100)
        .await
        .unwrap();
    assert_eq!(active.len(), 2);
    let with_deleted = store
        .list_assets(
            in_ns(AssetFilter {
                include_deleted: true,
                ..Default::default()
            }),
            0,
            100,
        )
        .await
        .unwrap();
    assert_eq!(with_deleted.len(), 3);
}

#[tokio::test]
#[serial]
async fn asset_update_patch_and_rename() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "src").await;
    common::make_namespace(&store, DEFAULT, "dst").await;

    let asset = store
        .create_asset(CreateAsset {
            comment: Some("orig".to_string()),
            ..common::asset_input(DEFAULT, "src", "t", "table", None)
        })
        .await
        .unwrap();

    // Three-state update: Set comment, keep properties absent, then Unset.
    let updated = store
        .update_asset(
            asset.id,
            AssetPatch {
                comment: PatchField::Set("new".to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.comment.as_deref(), Some("new"));
    let cleared = store
        .update_asset(
            asset.id,
            AssetPatch {
                comment: PatchField::Unset,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(cleared.comment.is_none());

    // In-place rename.
    let renamed = store.rename_asset(asset.id, "t2", None).await.unwrap();
    assert_eq!(renamed.name, "t2");
    assert_eq!(renamed.namespace_id, asset.namespace_id);

    // Cross-namespace move with unchanged name.
    let dst_ns = store.get_namespace(DEFAULT, "dst").await.unwrap();
    let moved = store
        .rename_asset(asset.id, "t2", Some("dst"))
        .await
        .unwrap();
    assert_eq!(moved.namespace_id, dst_ns.id);
    store.get_asset_by_name(DEFAULT, "dst", "t2").await.unwrap();
    let old = store.get_asset_by_name(DEFAULT, "src", "t2").await;
    assert!(matches!(old, Err(CatalogError::NotFound(_))));

    // Move to a missing namespace -> NotFound.
    let no_ns = store.rename_asset(asset.id, "t2", Some("ghost")).await;
    assert!(matches!(no_ns, Err(CatalogError::NotFound(_))));

    // Name conflict in the destination namespace -> AlreadyExists.
    store
        .create_asset(common::asset_input(DEFAULT, "src", "clash", "table", None))
        .await
        .unwrap();
    store
        .create_asset(common::asset_input(DEFAULT, "dst", "clash", "table", None))
        .await
        .unwrap();
    let src_clash = store
        .get_asset_by_name(DEFAULT, "src", "clash")
        .await
        .unwrap();
    let conflict = store.rename_asset(src_clash.id, "clash", Some("dst")).await;
    assert!(matches!(conflict, Err(CatalogError::AlreadyExists(_))));

    // Renaming a soft-deleted asset -> NotFound.
    store.soft_delete_asset(src_clash.id).await.unwrap();
    let deleted = store.rename_asset(src_clash.id, "zzz", None).await;
    assert!(matches!(deleted, Err(CatalogError::NotFound(_))));
}

// ── Soft delete / restore / hard delete ────────────────────────────────────

#[tokio::test]
#[serial]
async fn soft_delete_restore_and_name_reuse() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;

    let asset = store
        .create_asset(common::asset_input(DEFAULT, "ns", "t", "table", None))
        .await
        .unwrap();

    store.soft_delete_asset(asset.id).await.unwrap();

    // Soft-deleted: by-name lookup misses, get-by-id still returns the row.
    let by_name = store.get_asset_by_name(DEFAULT, "ns", "t").await;
    assert!(matches!(by_name, Err(CatalogError::NotFound(_))));
    let by_id = store.get_asset(asset.id).await.unwrap();
    assert!(by_id.deleted_at.is_some());

    // Double soft-delete -> NotFound.
    let twice = store.soft_delete_asset(asset.id).await;
    assert!(matches!(twice, Err(CatalogError::NotFound(_))));

    // The name is free for a new active asset.
    let replacement = store
        .create_asset(common::asset_input(DEFAULT, "ns", "t", "table", None))
        .await
        .unwrap();

    // Restoring while the name is taken -> Conflict.
    let conflict = store.restore_asset(asset.id).await;
    assert!(matches!(conflict, Err(CatalogError::Conflict(_))));

    // Free the name, restore succeeds.
    store.hard_delete_asset(replacement.id).await.unwrap();
    let restored = store.restore_asset(asset.id).await.unwrap();
    assert!(restored.deleted_at.is_none());
    store.get_asset_by_name(DEFAULT, "ns", "t").await.unwrap();

    // Restoring an active asset -> Conflict.
    let active = store.restore_asset(asset.id).await;
    assert!(matches!(active, Err(CatalogError::Conflict(_))));
}

#[tokio::test]
#[serial]
async fn hard_delete_cascades_extensions_versions_and_tags() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;

    let with_tabular = common::make_tabular_asset(
        &store,
        DEFAULT,
        "ns",
        "t",
        Some("iceberg"),
        "s3://wh/ns/t",
        Some("s3://wh/ns/t/metadata/00001.metadata.json"),
    )
    .await;
    let id = with_tabular.asset.id;

    store
        .create_version(CreateVersion {
            asset_id: id,
            version_key: "00001".to_string(),
            content_pointer: Some("s3://wh/ns/t/metadata/00001.metadata.json".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    store.add_tag(id, "hot").await.unwrap();

    store.soft_delete_asset(id).await.unwrap();
    store.hard_delete_asset(id).await.unwrap();

    // Everything reachable through the asset is gone.
    let asset = store.get_asset(id).await;
    assert!(matches!(asset, Err(CatalogError::NotFound(_))));
    let tabular = store.get_tabular_asset(id).await;
    assert!(matches!(tabular, Err(CatalogError::NotFound(_))));
    assert!(store.list_versions(id, 0, 100).await.unwrap().is_empty());
    assert!(store.list_tags(id).await.unwrap().is_empty());

    // Deleting again -> NotFound.
    let again = store.hard_delete_asset(id).await;
    assert!(matches!(again, Err(CatalogError::NotFound(_))));
}

// ── VersionStore ───────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn version_chain_and_immutability() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;
    let t = common::make_tabular_asset(&store, DEFAULT, "ns", "t", None, "s3://wh/t", None).await;
    let other =
        common::make_tabular_asset(&store, DEFAULT, "ns", "u", None, "s3://wh/u", None).await;

    let v1 = store
        .create_version(CreateVersion {
            asset_id: t.asset.id,
            version_key: "00001".to_string(),
            version_properties: Some(serde_json::json!({"op": "create"})),
            content_pointer: Some("s3://wh/t/metadata/00001.metadata.json".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(v1.previous_version_id.is_none());

    // create_version mirrors the key onto assets.current_version_key.
    let asset = store.get_asset(t.asset.id).await.unwrap();
    assert_eq!(asset.current_version_key.as_deref(), Some("00001"));

    let v2 = store
        .create_version(CreateVersion {
            asset_id: t.asset.id,
            version_key: "00002".to_string(),
            previous_version_id: Some(v1.id),
            content_pointer: Some("s3://wh/t/metadata/00002.metadata.json".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(v2.previous_version_id, Some(v1.id));

    let latest = store.get_latest_version(t.asset.id).await.unwrap();
    assert_eq!(latest.version_key, "00002");

    let got = store.get_version(t.asset.id, "00001").await.unwrap();
    assert_eq!(got.id, v1.id);
    assert_eq!(got.version_properties.unwrap()["op"], "create");

    let listed = store.list_versions(t.asset.id, 0, 100).await.unwrap();
    assert_eq!(listed.len(), 2);

    // Version rows are immutable: re-inserting the same key is rejected.
    let dup = store
        .create_version(CreateVersion {
            asset_id: t.asset.id,
            version_key: "00001".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // A second root version violates the single-root constraint.
    let second_root = store
        .create_version(CreateVersion {
            asset_id: t.asset.id,
            version_key: "00003".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(second_root, Err(CatalogError::AlreadyExists(_))));

    // Cross-asset predecessor -> Validation (application-layer check).
    let cross = store
        .create_version(CreateVersion {
            asset_id: t.asset.id,
            version_key: "00004".to_string(),
            previous_version_id: Some(
                store
                    .create_version(CreateVersion {
                        asset_id: other.asset.id,
                        version_key: "00001".to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .id,
            ),
            ..Default::default()
        })
        .await;
    assert!(matches!(cross, Err(CatalogError::Validation(_))));

    // Missing asset / missing version / soft-deleted asset -> NotFound.
    let no_asset = store
        .create_version(CreateVersion {
            asset_id: Uuid::new_v4(),
            version_key: "00001".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(no_asset, Err(CatalogError::NotFound(_))));
    let no_version = store.get_version(t.asset.id, "99999").await;
    assert!(matches!(no_version, Err(CatalogError::NotFound(_))));

    store.soft_delete_asset(other.asset.id).await.unwrap();
    let deleted_asset = store
        .create_version(CreateVersion {
            asset_id: other.asset.id,
            version_key: "00002".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(deleted_asset, Err(CatalogError::NotFound(_))));
}

// ── CasCommitStore ─────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn cas_commit_success_and_failures() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;
    let m1 = "s3://wh/ns/t/metadata/00001.metadata.json";
    let t = common::make_tabular_asset(
        &store,
        DEFAULT,
        "ns",
        "t",
        Some("iceberg"),
        "s3://wh/ns/t",
        Some(m1),
    )
    .await;
    let id = t.asset.id;

    // Successful CAS: mirrored version inserted (root, since the asset had
    // no version yet), current_version_key and tabular cache updated.
    let m2 = "s3://wh/ns/t/metadata/00002.metadata.json";
    let v1 = store
        .compare_and_swap_pointer(
            id,
            m1,
            CreateVersion {
                asset_id: id,
                version_key: "00002".to_string(),
                content_pointer: Some(m2.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(v1.previous_version_id.is_none());
    assert_eq!(v1.content_pointer.as_deref(), Some(m2));

    let asset = store.get_asset(id).await.unwrap();
    assert_eq!(asset.current_version_key.as_deref(), Some("00002"));
    let tabular = store.get_tabular_asset(id).await.unwrap();
    assert_eq!(tabular.metadata_location.as_deref(), Some(m2));

    // Second CAS auto-links the previous version via current_version_key.
    let m3 = "s3://wh/ns/t/metadata/00003.metadata.json";
    let v2 = store
        .compare_and_swap_pointer(
            id,
            m2,
            CreateVersion {
                asset_id: id,
                version_key: "00003".to_string(),
                content_pointer: Some(m3.to_string()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(v2.previous_version_id, Some(v1.id));

    // Stale expected pointer -> Conflict.
    let stale = store
        .compare_and_swap_pointer(
            id,
            m2,
            CreateVersion {
                asset_id: id,
                version_key: "00004".to_string(),
                content_pointer: Some("s3://wh/ns/t/metadata/00004.metadata.json".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(stale, Err(CatalogError::Conflict(_))));

    // new_version.asset_id mismatch -> Validation.
    let mismatched = store
        .compare_and_swap_pointer(
            id,
            m3,
            CreateVersion {
                asset_id: Uuid::new_v4(),
                version_key: "00004".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(mismatched, Err(CatalogError::Validation(_))));

    // Nonexistent asset -> NotFound.
    let ghost_id = Uuid::new_v4();
    let ghost = store
        .compare_and_swap_pointer(
            ghost_id,
            m3,
            CreateVersion {
                asset_id: ghost_id,
                version_key: "00004".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(ghost, Err(CatalogError::NotFound(_))));

    // Soft-deleted asset -> NotFound.
    store.soft_delete_asset(id).await.unwrap();
    let deleted = store
        .compare_and_swap_pointer(
            id,
            m3,
            CreateVersion {
                asset_id: id,
                version_key: "00004".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(deleted, Err(CatalogError::NotFound(_))));

    // Asset without a tabular extension -> NotFound (no CAS anchor).
    let plain = store
        .create_asset(common::asset_input(DEFAULT, "ns", "plain", "view", None))
        .await
        .unwrap();
    let no_tabular = store
        .compare_and_swap_pointer(
            plain.id,
            "whatever",
            CreateVersion {
                asset_id: plain.id,
                version_key: "00001".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(no_tabular, Err(CatalogError::NotFound(_))));
}

// ── TabularStore ───────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn tabular_store_double_write() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;

    let created = store
        .create_tabular_asset(
            common::asset_input(DEFAULT, "ns", "t", "table", Some("iceberg")),
            "s3://wh/ns/t",
            Some("s3://wh/ns/t/metadata/00001.metadata.json"),
        )
        .await
        .unwrap();
    assert_eq!(created.tabular.asset_id, created.asset.id);
    assert_eq!(created.tabular.location, "s3://wh/ns/t");

    // Same transaction wrote both rows: the extension is readable.
    let tabular = store.get_tabular_asset(created.asset.id).await.unwrap();
    assert_eq!(
        tabular.metadata_location.as_deref(),
        Some("s3://wh/ns/t/metadata/00001.metadata.json")
    );

    // Non-table asset type -> Validation (application-layer check).
    let wrong_type = store
        .create_tabular_asset(
            common::asset_input(DEFAULT, "ns", "v", "view", Some("iceberg")),
            "s3://wh/ns/v",
            None,
        )
        .await;
    assert!(matches!(wrong_type, Err(CatalogError::Validation(_))));

    let missing = store.get_tabular_asset(Uuid::new_v4()).await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));
}

// ── TagStore ───────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn tag_lifecycle() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "ns").await;
    let a = store
        .create_asset(common::asset_input(DEFAULT, "ns", "a", "table", None))
        .await
        .unwrap();
    let b = store
        .create_asset(common::asset_input(DEFAULT, "ns", "b", "table", None))
        .await
        .unwrap();

    store.add_tag(a.id, "pii").await.unwrap();
    store.add_tag(a.id, "hot").await.unwrap();
    store.add_tag(b.id, "hot").await.unwrap();

    // Tags are listed in sorted order.
    assert_eq!(store.list_tags(a.id).await.unwrap(), vec!["hot", "pii"]);

    // Duplicate add -> AlreadyExists (per implementation semantics).
    let dup = store.add_tag(a.id, "hot").await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Tagging a missing asset -> NotFound.
    let ghost = store.add_tag(Uuid::new_v4(), "x").await;
    assert!(matches!(ghost, Err(CatalogError::NotFound(_))));

    // list_assets_by_tag returns active assets of the domain only.
    let hot = store
        .list_assets_by_tag(DEFAULT, "hot", 0, 100)
        .await
        .unwrap();
    assert_eq!(hot.len(), 2);
    store.soft_delete_asset(b.id).await.unwrap();
    let hot_active = store
        .list_assets_by_tag(DEFAULT, "hot", 0, 100)
        .await
        .unwrap();
    assert_eq!(hot_active.len(), 1);
    assert_eq!(hot_active[0].id, a.id);

    // remove is idempotent.
    store.remove_tag(a.id, "pii").await.unwrap();
    store.remove_tag(a.id, "pii").await.unwrap();
    assert_eq!(store.list_tags(a.id).await.unwrap(), vec!["hot"]);
}

// ── AssetTypeStore (registry) ──────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn registry_register_list_and_conflicts() {
    let store = common::store().await;

    let model = store
        .register_asset_type(RegisterAssetType {
            name: "model".to_string(),
            description: Some("ML model".to_string()),
            category: "model".to_string(),
            validation_schema: None,
            extension_strategy: "jsonb".to_string(),
            supports_native_protocol: false,
        })
        .await
        .unwrap();
    assert_eq!(model.category, "model");

    let got = store.get_asset_type("model").await.unwrap();
    assert!(!got.supports_native_protocol);

    // Category filter: `tabular` only holds the seeded `table` type.
    let tabular = store
        .list_asset_types(Some("tabular"), 0, 100)
        .await
        .unwrap();
    assert_eq!(tabular.len(), 1);
    assert_eq!(tabular[0].name, "table");
    let all = store.list_asset_types(None, 0, 100).await.unwrap();
    assert_eq!(all.len(), 3); // table, view, model

    // Duplicate type -> AlreadyExists.
    let dup = store
        .register_asset_type(RegisterAssetType {
            name: "model".to_string(),
            description: None,
            category: "model".to_string(),
            validation_schema: None,
            extension_strategy: "jsonb".to_string(),
            supports_native_protocol: false,
        })
        .await;
    assert!(matches!(dup, Err(CatalogError::AlreadyExists(_))));

    // Invalid category / strategy -> CHECK violation -> Validation.
    let bad_category = store
        .register_asset_type(RegisterAssetType {
            name: "widget".to_string(),
            description: None,
            category: "nope".to_string(),
            validation_schema: None,
            extension_strategy: "jsonb".to_string(),
            supports_native_protocol: false,
        })
        .await;
    assert!(matches!(bad_category, Err(CatalogError::Validation(_))));

    // Formats.
    let parquet = store
        .register_format(RegisterFormat {
            name: "parquet".to_string(),
            description: Some("Apache Parquet".to_string()),
            mime_type: None,
            serialization_hint: None,
        })
        .await
        .unwrap();
    assert_eq!(parquet.name, "parquet");
    assert_eq!(store.list_formats(0, 100).await.unwrap().len(), 3);

    let dup_format = store
        .register_format(RegisterFormat {
            name: "parquet".to_string(),
            ..Default::default()
        })
        .await;
    assert!(matches!(dup_format, Err(CatalogError::AlreadyExists(_))));

    let missing = store.get_format("avro").await;
    assert!(matches!(missing, Err(CatalogError::NotFound(_))));
}

// ── UnifiedQueryStore ──────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn unified_query_discovers_across_namespaces() {
    let store = common::store().await;
    common::make_namespace(&store, DEFAULT, "a/b").await;
    common::make_namespace(&store, DEFAULT, "a/c").await;
    common::make_namespace(&store, DEFAULT, "z").await;

    let in_b = store
        .create_asset(common::asset_input(
            DEFAULT,
            "a/b",
            "t1",
            "table",
            Some("iceberg"),
        ))
        .await
        .unwrap();
    store.add_tag(in_b.id, "hot").await.unwrap();
    store
        .create_asset(common::asset_input(
            DEFAULT,
            "a/c",
            "t2",
            "table",
            Some("lance"),
        ))
        .await
        .unwrap();
    store
        .create_asset(common::asset_input(
            DEFAULT,
            "z",
            "t3",
            "table",
            Some("iceberg"),
        ))
        .await
        .unwrap();

    let base = AssetQuery {
        domain: DEFAULT.to_string(),
        limit: 100,
        ..Default::default()
    };

    // Prefix query covers the subtree but not sibling roots.
    let subtree = store
        .query_assets(AssetQuery {
            namespace_prefix: Some("a".to_string()),
            ..base.clone()
        })
        .await
        .unwrap();
    assert_eq!(subtree.len(), 2);

    // Tag + format combination.
    let hot_iceberg = store
        .query_assets(AssetQuery {
            namespace_prefix: Some("a".to_string()),
            format: Some("iceberg".to_string()),
            tags: vec!["hot".to_string()],
            ..base.clone()
        })
        .await
        .unwrap();
    assert_eq!(hot_iceberg.len(), 1);
    assert_eq!(hot_iceberg[0].id, in_b.id);

    // Soft-deleted rows are hidden unless include_deleted is set.
    store.soft_delete_asset(in_b.id).await.unwrap();
    let active = store.query_assets(base.clone()).await.unwrap();
    assert_eq!(active.len(), 2);
    let with_deleted = store
        .query_assets(AssetQuery {
            include_deleted: true,
            ..base
        })
        .await
        .unwrap();
    assert_eq!(with_deleted.len(), 3);
}

// ── CAS concurrency ────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn concurrent_cas_exactly_one_wins() {
    let store = Arc::new(common::store().await);
    common::make_namespace(&store, DEFAULT, "ns").await;
    let m1 = "s3://wh/ns/t/metadata/00001.metadata.json";
    let t = common::make_tabular_asset(
        &store,
        DEFAULT,
        "ns",
        "t",
        Some("iceberg"),
        "s3://wh/ns/t",
        Some(m1),
    )
    .await;
    let id = t.asset.id;

    let attempt = |key: &'static str, pointer: &'static str| {
        let store = Arc::clone(&store);
        async move {
            store
                .compare_and_swap_pointer(
                    id,
                    m1,
                    CreateVersion {
                        asset_id: id,
                        version_key: key.to_string(),
                        content_pointer: Some(pointer.to_string()),
                        ..Default::default()
                    },
                )
                .await
        }
    };

    let (r1, r2) = tokio::join!(
        attempt("00002", "s3://wh/ns/t/metadata/00002-a.metadata.json"),
        attempt("00003", "s3://wh/ns/t/metadata/00003-b.metadata.json"),
    );

    // Exactly one commit wins; the loser sees a pointer-mismatch Conflict.
    let results = [&r1, &r2];
    let wins = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(CatalogError::Conflict(_))))
        .count();
    assert_eq!((wins, conflicts), (1, 1), "r1={r1:?} r2={r2:?}");

    // The mirrored history holds exactly one new version and the pointer
    // matches the winner.
    let versions = store.list_versions(id, 0, 100).await.unwrap();
    assert_eq!(versions.len(), 1);
    let tabular = store.get_tabular_asset(id).await.unwrap();
    assert_eq!(
        tabular.metadata_location, versions[0].content_pointer,
        "tabular cache must point at the winning commit's metadata"
    );
}
