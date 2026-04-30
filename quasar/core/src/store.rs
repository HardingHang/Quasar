use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::StoreError;
use crate::models::{
    Asset, AssetFormat, AssetVersionWithTabular, AssetWithTabular, Namespace, PatchField,
    TabularAsset,
};

#[async_trait]
pub trait CatalogStore: Send + Sync {
    // ── Namespace (format-agnostic, V2 adds comment) ───────────────

    async fn create_namespace(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(&self, offset: i64, limit: i32) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, name: &str) -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError>;

    async fn update_namespace(
        &self,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    // ── Asset (table assets, enforced format isolation) ─────────────

    /// Create a table asset. Writes both `assets` and `tabular_assets` in a transaction.
    ///
    /// Note: standard protocol (Iceberg/Lance) table creation requests typically
    /// do not include a comment field, so comment is NULL at creation time.
    /// To set comment after creation, use `update_asset_properties`.
    #[allow(clippy::too_many_arguments)]
    async fn create_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        location: &str,
        metadata_location: Option<&str>,
        schema_snapshot: Option<serde_json::Value>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError>;

    /// List table assets in a namespace. Returns Asset with its tabular detail.
    /// SQL must inject `a.asset_type = 'table' AND a.asset_subtype = $format`.
    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<AssetWithTabular>, StoreError>;

    async fn get_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<Asset, StoreError>;

    /// Get Asset with its tabular detail (joined with tabular_assets).
    async fn get_asset_with_tabular(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError>;

    /// Get asset with its tabular fields and current version in a single query.
    async fn get_asset_with_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset, Option<AssetVersionWithTabular>), StoreError>;

    async fn asset_exists(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError>;

    async fn rename_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError>;

    /// Update Asset's catalog-level comment and properties
    /// (used by Unified API PATCH /assets, does not touch format-internal state).
    async fn update_asset_properties(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError>;

    // ── Version (Lance-specific) ────────────────────────────────────

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersionWithTabular, StoreError>;

    async fn load_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Option<AssetVersionWithTabular>, StoreError>;

    async fn list_versions(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Vec<AssetVersionWithTabular>, StoreError>;

    /// Create a version record.
    /// Writes `asset_versions` then `tabular_asset_versions` in a transaction.
    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersionWithTabular, StoreError>;

    // ── Iceberg CAS ─────────────────────────────────────────────────

    /// Atomically update tabular_assets.metadata_location if it matches.
    /// SQL must constrain target via `assets.asset_type = 'table' AND assets.asset_subtype = $format`.
    #[allow(clippy::too_many_arguments)]
    async fn cas_update_metadata_location(
        &self,
        namespace_name: &str,
        asset_name: &str,
        format: AssetFormat,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
        property_removals: &[String],
        property_updates: &HashMap<String, String>,
    ) -> Result<(), StoreError>;

    // ── Unified API helpers ─────────────────────────────────────────

    /// Cross-format asset listing (used by Unified API).
    /// `format` = None returns all table assets; Some filters by subtype.
    /// `name` = Some filters by exact name match.
    async fn list_assets_unified(
        &self,
        namespace_name: &str,
        format: Option<AssetFormat>,
        name: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError>;

    /// Get Asset with its tabular detail (Unified API use, not bound to format).
    async fn get_asset_unified(
        &self,
        namespace_name: &str,
        name: &str,
        format: AssetFormat,
    ) -> Result<(Asset, TabularAsset), StoreError>;
}
