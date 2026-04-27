use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::StoreError;
use crate::models::{Asset, AssetFormat, AssetVersion, Namespace};

#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn create_namespace(
        &self,
        name: &str,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, name: &str) -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError>;

    async fn update_namespace_properties(
        &self,
        name: &str,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

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

    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<Asset>, StoreError>;

    async fn get_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<Asset, StoreError>;

    /// Get asset with its current version in a single query.
    /// Returns (Asset, Option<AssetVersion>) - version may be None if no versions exist.
    async fn get_asset_with_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, Option<AssetVersion>), StoreError>;

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

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersion, StoreError>;

    async fn load_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<AssetVersion, StoreError>;

    async fn list_versions(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Vec<AssetVersion>, StoreError>;

    /// Create a version record.
    /// If `previous_version_id` is Some, performs CAS check against the current
    /// latest version before inserting. Returns Conflict if the check fails.
    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersion, StoreError>;

    /// Atomically update metadata_location if it matches the expected value.
    /// Returns Conflict if the current location does not match.
    async fn cas_update_metadata_location(
        &self,
        namespace_name: &str,
        asset_name: &str,
        format: AssetFormat,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
    ) -> Result<(), StoreError>;
}
