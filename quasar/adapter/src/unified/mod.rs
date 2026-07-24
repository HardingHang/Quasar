//! Unified API adapter (`/unified/v1/...`).
//!
//! Format-agnostic management and discovery API (REQUIREMENTS §6.2,
//! DESIGN §5.3). Assets and versions are read-only here; the only write
//! exceptions are soft-delete restore and tag management (governance
//! operations). Errors are RFC-7807 Problem Details with a closed set of
//! six machine codes (DESIGN §5.5 / §7.5).

pub mod asset;
pub mod discovery;
pub mod domain;
pub mod dto;
pub mod error;
pub mod namespace;
pub mod registry;
pub mod tag;
pub mod version;

use axum::{
    extract::Extension,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use quasar_core::CatalogStore;

/// Unified API configuration.
///
/// Retained for server assembly compatibility (`Extension<UnifiedConfig>`
/// is layered onto the router); the current handlers only read from the
/// catalog store and do not touch the object store.
#[derive(Clone, Default)]
pub struct UnifiedConfig {
    pub object_store: Option<Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
}

/// Extract the request id injected by the server middleware as
/// `Extension<String>`; generate a fresh UUID when the extension is
/// absent (e.g. in tests).
pub(crate) fn request_id_of(extension: Option<Extension<String>>) -> String {
    match extension {
        Some(Extension(id)) => id,
        None => uuid::Uuid::new_v4().to_string(),
    }
}

/// Register Unified API routes (REQUIREMENTS §6.2).
///
/// Namespace paths are hierarchical, so they are captured with the axum
/// wildcard `{*path}`; the wildcard handler also dispatches the
/// path-addressed asset endpoints via the `/assets` suffix (DESIGN §5.3).
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        // Domain lifecycle
        .route(
            "/unified/v1/domains",
            get(domain::list_domains).post(domain::create_domain),
        )
        .route(
            "/unified/v1/domains/{domain}",
            get(domain::get_domain)
                .patch(domain::update_domain)
                .delete(domain::delete_domain),
        )
        // Namespace lifecycle (hierarchical path via wildcard)
        .route(
            "/unified/v1/domains/{domain}/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/unified/v1/domains/{domain}/namespaces/{*path}",
            get(namespace::get_path)
                .patch(namespace::update_path)
                .delete(namespace::delete_path),
        )
        // Discovery collection (cross-namespace, domain-scoped)
        .route("/unified/v1/assets", get(discovery::query_assets))
        // Single-asset read + restore, addressed by id
        .route("/unified/v1/assets/{asset_id}", get(asset::get_asset_by_id))
        .route(
            "/unified/v1/assets/{asset_id}/restore",
            post(asset::restore_asset),
        )
        // Tag management
        .route(
            "/unified/v1/assets/{asset_id}/tags",
            get(tag::list_tags).post(tag::add_tag),
        )
        .route(
            "/unified/v1/assets/{asset_id}/tags/{tag}",
            axum::routing::delete(tag::remove_tag),
        )
        // Version history (read-only)
        .route(
            "/unified/v1/assets/{asset_id}/versions",
            get(version::list_versions),
        )
        .route(
            "/unified/v1/assets/{asset_id}/versions/{version_key}",
            get(version::get_version),
        )
        // AssetType / Format registries
        .route(
            "/unified/v1/asset-types",
            get(registry::list_asset_types).post(registry::register_asset_type),
        )
        .route(
            "/unified/v1/asset-types/{name}",
            get(registry::get_asset_type),
        )
        .route(
            "/unified/v1/formats",
            get(registry::list_formats).post(registry::register_format),
        )
        .route("/unified/v1/formats/{name}", get(registry::get_format))
}
