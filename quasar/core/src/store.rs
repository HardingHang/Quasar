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

    async fn get_namespace(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<Namespace, StoreError>;

    async fn namespace_exists(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<bool, StoreError>;

    async fn drop_namespace(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<(), StoreError>;

    async fn create_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
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
}
