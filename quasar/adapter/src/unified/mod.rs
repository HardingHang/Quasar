pub mod asset;
pub mod dto;
pub mod error;
pub mod namespace;
pub mod version;

use axum::{routing::get, routing::post, Router};
use std::sync::Arc;

use quasar_core::CatalogStore;

/// Unified API configuration.
#[derive(Clone, Default)]
pub struct UnifiedConfig {
    #[cfg(feature = "iceberg")]
    pub object_store: Option<Arc<dyn object_store::ObjectStore>>,
    #[cfg(feature = "iceberg")]
    pub s3_bucket: Option<String>,
}

/// Register Unified API routes.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        .route(
            "/unified/v1/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/unified/v1/namespaces/{ns}",
            get(namespace::get_namespace)
                .delete(namespace::drop_namespace)
                .patch(namespace::update_namespace),
        )
        .route(
            "/unified/v1/namespaces/{ns}/assets",
            get(asset::list_assets).post(asset::create_asset_not_allowed),
        )
        .route(
            "/unified/v1/namespaces/{ns}/assets/{name}",
            get(asset::get_asset)
                .delete(asset::drop_asset)
                .patch(asset::update_asset),
        )
        .route(
            "/unified/v1/namespaces/{ns}/assets/{name}/rename",
            post(asset::rename_asset),
        )
}
