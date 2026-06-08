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
        offset: i64,
        limit: i32,
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
    + IcebergStagingStore
    + IcebergRegisterStore
    + IcebergMetricsStore
    + IcebergPurgeStore
    + IcebergTransactionStore
    + IcebergViewStore
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
        + IcebergStagingStore
        + IcebergRegisterStore
        + IcebergMetricsStore
        + IcebergPurgeStore
        + IcebergTransactionStore
        + IcebergViewStore
        + Send
        + Sync
        + ?Sized
{
}

/// Staged table lifecycle for Iceberg staged-create commit flow.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait IcebergStagingStore: TabularStore {
    /// Create a staged table record. Before insertion, deletes any expired
    /// staged record with the same (domain, namespace, name). Returns
    /// `AlreadyExists` if an active table or non-expired staged record exists.
    async fn create_staged_table(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
        table_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: HashMap<String, String>,
    ) -> Result<(), StoreError>;

    /// Read a non-expired staged table record.
    async fn get_staged_table(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, StoreError>;

    /// Delete a staged table record (e.g., after successful commit or explicit cancel).
    async fn delete_staged_table(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<(), StoreError>;

    /// Commit a staged table: in a single transaction, verify active table
    /// does not exist, lock the staged record, insert assets + tabular_assets,
    /// and delete the staged record. Returns the created (Asset, TabularAsset).
    #[allow(clippy::too_many_arguments)]
    async fn commit_staged_table(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: HashMap<String, String>,
    ) -> Result<(Asset, TabularAsset), StoreError>;
}

/// Register an externally-managed Iceberg table into the catalog.
#[async_trait]
pub trait IcebergRegisterStore: TabularStore {
    /// In a single transaction, insert assets + tabular_assets pointing to
    /// an external metadata location. Does not write object store.
    #[allow(clippy::too_many_arguments)]
    async fn register_iceberg_table(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: HashMap<String, String>,
    ) -> Result<(Asset, TabularAsset), StoreError>;
}

/// Persist Iceberg scan metrics reports.
#[async_trait]
pub trait IcebergMetricsStore: Send + Sync {
    /// Record a raw scan metrics report JSON. The caller has already verified
    /// that the table exists and is an Iceberg table.
    async fn record_scan_metrics_report(
        &self,
        asset_id: Option<Uuid>,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
        report: serde_json::Value,
        user_agent: Option<&str>,
    ) -> Result<(), StoreError>;
}

/// Iceberg purge (DROP TABLE PURGE) operation tracking.
#[async_trait]
pub trait IcebergPurgeStore: TabularStore {
    /// In a single transaction: read the table location, create a purge
    /// operation record with status `catalog_dropped`, delete the catalog
    /// records (assets + tabular via FK cascade), and return the operation id
    /// along with the table location and metadata location needed for object
    /// store cleanup.
    async fn begin_iceberg_purge_and_drop_catalog(
        &self,
        domain_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<(Uuid, String, Option<String>), StoreError>;

    /// Update the status of a purge operation.
    async fn update_purge_operation(
        &self,
        operation_id: Uuid,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<(), StoreError>;
}

/// Multi-table transaction commit (V4.1).
/// Executes CAS updates for multiple tables within a single PostgreSQL
/// transaction, guaranteeing atomicity.
#[async_trait]
pub trait IcebergTransactionStore: TabularStore {
    /// In a single DB transaction, execute CAS metadata_location updates
    /// for all tables in `table_updates`.
    ///
    /// Each tuple is `(domain, namespace, table, expected_location, new_location,
    /// schema_snapshot)`. Tables must be sorted by `(domain, namespace, table)`
    /// before calling. If any CAS fails, the entire transaction rolls back.
    async fn commit_transaction_tables(
        &self,
        table_updates: Vec<(
            String,
            String,
            String,
            String,
            String,
            Option<serde_json::Value>,
        )>,
    ) -> Result<(), StoreError>;
}

/// Iceberg-specific super-trait composing all Iceberg capabilities.
/// Present as a marker for documentation and future state splitting;
/// C2 keeps all handlers on `Arc<dyn CatalogStore>` due to Rust trait-object
/// upcast limitations in Axum's `Router::merge`.
/// Iceberg View lifecycle management (V4.2).
///
/// View requirement validation is performed at the adapter layer (consistent
/// with Table requirement handling); the Store layer is only responsible for
/// View identity management and CAS metadata_location updates.
#[async_trait]
pub trait IcebergViewStore: AssetStore {
    /// Create a View (dual-row insert into assets + view_assets).
    ///
    /// `view_uuid` is generated by the adapter layer and passed in.
    /// `current_version_id` is initialized to 1 by the adapter layer.
    #[allow(clippy::too_many_arguments)]
    async fn create_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        view_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        current_version_id: i32,
        properties: serde_json::Value,
    ) -> Result<crate::models::View, StoreError>;

    /// Load a View record (read assets + view_assets joined row).
    async fn get_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<crate::models::View, StoreError>;

    /// CAS update of View metadata_location.
    ///
    /// Requirement validation is already done at the adapter layer; the Store
    /// layer only executes the CAS update.
    /// **This method does NOT update `view_assets.current_version_id` or
    /// `view_assets.properties`**: View Load always reads full metadata from
    /// object store (no DB snapshot fallback), so these DB fields only record
    /// the initial creation state.
    async fn commit_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        expected_location: &str,
        new_location: &str,
    ) -> Result<(), StoreError>;

    /// Drop a View (FK cascade automatically deletes view_assets row).
    async fn drop_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<(), StoreError>;

    /// List all Views in a namespace.
    async fn list_views(
        &self,
        domain_name: &str,
        namespace_name: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<crate::models::ViewIdentifier>, StoreError>;

    /// Rename a View (update assets row + sync view_assets).
    #[allow(clippy::too_many_arguments)]
    async fn rename_view(
        &self,
        source_domain: &str,
        source_namespace: &str,
        source_name: &str,
        dest_domain: &str,
        dest_namespace: &str,
        dest_name: &str,
    ) -> Result<(), StoreError>;

    /// Check if a View exists (query assets table where asset_type='view').
    async fn view_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<bool, StoreError>;
}

/// Iceberg-specific super-trait composing all Iceberg capabilities.
/// Present as a marker for documentation and future state splitting;
/// C2 keeps all handlers on `Arc<dyn CatalogStore>` due to Rust trait-object
/// upcast limitations in Axum's `Router::merge`.
pub trait IcebergCatalogStore:
    CatalogStore
    + CasCommitStore
    + IcebergStagingStore
    + IcebergRegisterStore
    + IcebergMetricsStore
    + IcebergPurgeStore
    + IcebergTransactionStore
    + IcebergViewStore
    + Send
    + Sync
{
}

impl<T> IcebergCatalogStore for T where
    T: CatalogStore
        + CasCommitStore
        + IcebergStagingStore
        + IcebergRegisterStore
        + IcebergMetricsStore
        + IcebergPurgeStore
        + IcebergTransactionStore
        + IcebergViewStore
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
    fn _catalog_store_marker_composes_all<T>()
    where
        T: DomainStore
            + NamespaceStore
            + AssetStore
            + TabularStore
            + VersionStore
            + TabularVersionStore
            + CasCommitStore
            + UnifiedQueryStore
            + IcebergStagingStore
            + IcebergRegisterStore
            + IcebergMetricsStore
            + IcebergPurgeStore
            + IcebergTransactionStore
            + IcebergViewStore,
    {
        fn _assert_catalog_store<S: CatalogStore + ?Sized>() {}
        _assert_catalog_store::<T>();
    }

    /// Compile-time assertion that the IcebergCatalogStore marker composes
    /// all Iceberg-specific traits and the underlying CatalogStore.
    #[allow(dead_code)]
    fn _iceberg_catalog_store_marker_composes_all<T>()
    where
        T: CatalogStore + CasCommitStore + IcebergViewStore,
    {
        fn _assert_iceberg_store<S: IcebergCatalogStore + ?Sized>() {}
        _assert_iceberg_store::<T>();
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
