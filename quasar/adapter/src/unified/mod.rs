pub mod asset;
pub mod domain;
pub mod dto;
pub mod error;
pub mod namespace;
pub mod version;

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use quasar_core::CatalogStore;

/// Unified API configuration.
#[derive(Clone, Default)]
pub struct UnifiedConfig {
    pub object_store: Option<Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
}

/// Register Unified API routes.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        // Domain management (V3 §4.1.3)
        .route(
            "/unified/v1/domains",
            get(domain::list_domains).post(domain::create_domain),
        )
        .route(
            "/unified/v1/domains/{domain}",
            get(domain::get_domain)
                .patch(domain::update_domain)
                .delete(domain::drop_domain),
        )
        // Namespace routes scoped under a Domain
        .route(
            "/unified/v1/domains/{domain}/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/unified/v1/domains/{domain}/namespaces/{ns}",
            get(namespace::get_namespace)
                .delete(namespace::drop_namespace)
                .patch(namespace::update_namespace),
        )
        // Asset routes (still on the Phase 2 path until C7 lands the
        // domain-scoped form. Adding the `/domains/{domain}` prefix is
        // C7 because asset.rs still uses the DEFAULT_DOMAIN shim.)
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
