//! V3 storage traits: identity-layer + extension-layer + read-projection
//! split.
//!
//! Phase 2 replaces the V2 monolithic `CatalogStore` trait with eight
//! focused traits (`DomainStore`, `NamespaceStore`, `AssetStore`,
//! `TabularStore`, `VersionStore`, `TabularVersionStore`, `CasCommitStore`,
//! `UnifiedQueryStore`). Each adapter route ultimately bounds its state on
//! the narrow trait it needs; `PgCatalogStore` implements all eight.
//!
//! For backward compatibility with axum's single-state model and to keep
//! Phase 2 adapter shims terse, `CatalogStore` remains as a *marker* super-
//! trait composed of the eight sub-traits, with a blanket impl. Routes that
//! want to narrow their bound can use the sub-traits directly; routes that
//! prefer a single `Arc<dyn CatalogStore>` still work unchanged.

use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::StoreError;
use crate::models::{
    Asset, AssetVersion, Domain, Namespace, PatchField, TabularAsset, TabularAssetVersion,
};

/// PATCH descriptor for `DomainStore::update_domain`. Each `PatchField` is
/// `Missing` to leave the field untouched, `Null` to clear, or `Value(_)`
/// to overwrite. `property_removals` and `property_updates` apply as a
/// delta to the existing properties map.
#[derive(Debug, Clone, Default)]
pub struct DomainPatch {
    pub comment: PatchField<String>,
    pub property_removals: Vec<String>,
    pub property_updates: HashMap<String, String>,
    pub storage_type: PatchField<String>,
    pub storage_config: PatchField<serde_json::Value>,
    pub warehouse: PatchField<String>,
    pub owner: PatchField<String>,
}

/// Domain identity operations (V3 §5.1).
#[async_trait]
pub trait DomainStore: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    async fn create_domain(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
        storage_type: Option<String>,
        storage_config: serde_json::Value,
        warehouse: Option<String>,
        owner: Option<String>,
    ) -> Result<Domain, StoreError>;

    async fn list_domains(&self, offset: i64, limit: i32) -> Result<Vec<Domain>, StoreError>;

    async fn get_domain(&self, name: &str) -> Result<Domain, StoreError>;

    async fn domain_exists(&self, name: &str) -> Result<bool, StoreError>;

    /// Hard delete an empty domain. Returns `DomainNotEmpty` if the domain
    /// still has namespaces (FK RESTRICT).
    async fn drop_domain(&self, name: &str) -> Result<(), StoreError>;

    async fn update_domain(&self, name: &str, patch: DomainPatch) -> Result<Domain, StoreError>;
}

/// Namespace identity operations within a domain (V3 §5.1).
#[async_trait]
pub trait NamespaceStore: Send + Sync {
    async fn create_namespace(
        &self,
        domain_name: &str,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self,
        domain_name: &str,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, domain_name: &str, name: &str) -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, domain_name: &str, name: &str) -> Result<bool, StoreError>;

    /// Hard delete an empty namespace. Returns `NamespaceNotEmpty` if any
    /// active asset remains (FK RESTRICT).
    async fn drop_namespace(&self, domain_name: &str, name: &str) -> Result<(), StoreError>;

    async fn update_namespace(
        &self,
        domain_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;
}

/// Format-agnostic asset identity operations (V3 §5.1).
///
/// Identity operations do not take a `format` parameter because V3 enforces
/// active asset name uniqueness within a namespace regardless of format
/// (`uq_assets_active_name` partial unique index).
#[async_trait]
pub trait AssetStore: Send + Sync {
    async fn create_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        asset_type: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError>;

    async fn get_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<Asset, StoreError>;

    async fn asset_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(), StoreError>;

    async fn rename_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        new_name: &str,
        new_namespace_name: Option<&str>,
    ) -> Result<(), StoreError>;

    async fn update_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError>;
}

/// Tabular asset operations (V3 §5.1). Extends [`AssetStore`] with
/// format-aware reads and the dual-row `(assets, tabular_assets)` create.
#[async_trait]
pub trait TabularStore: AssetStore {
    #[allow(clippy::too_many_arguments)]
    async fn create_tabular_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        format: &str,
        location: &str,
        metadata_location: Option<&str>,
        schema_snapshot: Option<serde_json::Value>,
        properties: HashMap<String, String>,
    ) -> Result<(Asset, TabularAsset), StoreError>;

    async fn list_tabular_assets(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError>;

    async fn get_tabular_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError>;

    async fn get_tabular_asset_with_current_version(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<
        (
            Asset,
            TabularAsset,
            Option<(AssetVersion, TabularAssetVersion)>,
        ),
        StoreError,
    >;
}

/// Version identity operations (V3 §5.1). Uses `asset_id` directly so
/// callers that already hold an asset reference do not re-resolve through
/// the `(domain, namespace, name)` triple.
#[async_trait]
pub trait VersionStore: Send + Sync {
    async fn create_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<AssetVersion, StoreError>;

    async fn get_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<AssetVersion, StoreError>;

    async fn list_versions(&self, asset_id: Uuid) -> Result<Vec<AssetVersion>, StoreError>;

    /// Returns the highest `version_order` row for the asset, or `None` if
    /// no version has a numeric order.
    async fn get_latest_version(&self, asset_id: Uuid) -> Result<Option<AssetVersion>, StoreError>;
}

/// Tabular version operations (V3 §5.1). Extends [`VersionStore`] with the
/// dual-row `(asset_versions, tabular_asset_versions)` create and the
/// joined reads that hand back the version's extension columns.
#[async_trait]
pub trait TabularVersionStore: VersionStore {
    #[allow(clippy::too_many_arguments)]
    async fn create_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        metadata_location: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError>;

    async fn get_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError>;

    async fn list_tabular_versions(
        &self,
        asset_id: Uuid,
    ) -> Result<Vec<(AssetVersion, TabularAssetVersion)>, StoreError>;

    async fn get_latest_tabular_version(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<(AssetVersion, TabularAssetVersion)>, StoreError>;
}

/// Iceberg-style compare-and-swap commit (V3 §5.1). Extends
/// [`TabularStore`] because the CAS read/write pair operates on a single
/// tabular asset.
#[async_trait]
pub trait CasCommitStore: TabularStore {
    /// Update `tabular_assets.metadata_location` only when the current
    /// value matches `expected_location`; otherwise return
    /// [`StoreError::Conflict`]. The property delta is applied to the
    /// underlying asset in the same transaction.
    #[allow(clippy::too_many_arguments)]
    async fn cas_update_metadata_location(
        &self,
        domain_name: &str,
        namespace_name: &str,
        asset_name: &str,
        format: &str,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
        property_removals: &[String],
        property_updates: &HashMap<String, String>,
    ) -> Result<(), StoreError>;
}

/// Cross-format aggregate queries for the Unified API surface (V3 §5.1).
/// Returns `Option<TabularAsset>` so non-tabular asset types (added in
/// future versions) surface naturally as `None`.
#[async_trait]
pub trait UnifiedQueryStore: Send + Sync {
    async fn list_assets_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
        name_filter: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, Option<TabularAsset>)>, StoreError>;

    async fn get_asset_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(Asset, Option<TabularAsset>), StoreError>;
}

/// Marker super-trait that composes the eight V3 store traits. Holds no
/// methods of its own; the blanket impl makes any type satisfying the
/// constituent bounds satisfy `CatalogStore` as well.
///
/// Phase 2 keeps the `CatalogStore` name so adapter handlers can continue
/// to type their state as `Arc<dyn CatalogStore>` without churn. Phase 3
/// may narrow individual route bounds to the smallest sub-trait that fits.
pub trait CatalogStore:
    DomainStore
    + NamespaceStore
    + AssetStore
    + TabularStore
    + VersionStore
    + TabularVersionStore
    + CasCommitStore
    + UnifiedQueryStore
    + Send
    + Sync
{
}

impl<T> CatalogStore for T where
    T: DomainStore
        + NamespaceStore
        + AssetStore
        + TabularStore
        + VersionStore
        + TabularVersionStore
        + CasCommitStore
        + UnifiedQueryStore
        + Send
        + Sync
        + ?Sized
{
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time assertion that the marker super-trait composes the
    /// eight sub-traits and that the blanket impl reaches it. If the
    /// constraint list drifts away from V3_DESIGN §5.1, this stops
    /// compiling.
    #[allow(dead_code)]
    fn _catalog_store_marker_composes_all_eight<T>()
    where
        T: DomainStore
            + NamespaceStore
            + AssetStore
            + TabularStore
            + VersionStore
            + TabularVersionStore
            + CasCommitStore
            + UnifiedQueryStore,
    {
        fn _assert_catalog_store<S: CatalogStore + ?Sized>() {}
        _assert_catalog_store::<T>();
    }

    #[test]
    fn domain_patch_default_is_all_missing() {
        let patch = DomainPatch::default();
        assert!(matches!(patch.comment, PatchField::Missing));
        assert!(matches!(patch.storage_type, PatchField::Missing));
        assert!(matches!(patch.storage_config, PatchField::Missing));
        assert!(matches!(patch.warehouse, PatchField::Missing));
        assert!(matches!(patch.owner, PatchField::Missing));
        assert!(patch.property_removals.is_empty());
        assert!(patch.property_updates.is_empty());
    }
}
