use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::StoreError;
use crate::models::{Asset, AssetCommitUpdate, AssetFormat, AssetVersion, Namespace};

#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn create_namespace(
        &self,
        name: &str,
        format: AssetFormat,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self,
        format: AssetFormat,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, name: &str, format: AssetFormat)
        -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, name: &str, format: AssetFormat) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, name: &str, format: AssetFormat) -> Result<(), StoreError>;

    async fn update_namespace_properties(
        &self,
        name: &str,
        format: AssetFormat,
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

    async fn commit_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        update: AssetCommitUpdate,
    ) -> Result<AssetVersion, StoreError>;

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

    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
    ) -> Result<AssetVersion, StoreError>;

    async fn commit_iceberg_table(
        &self,
        namespace_name: &str,
        asset_name: &str,
        expected_metadata_location: &str,
        new_metadata_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
    ) -> Result<(), StoreError>;
}
