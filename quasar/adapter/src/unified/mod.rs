pub mod dto;
pub mod error;
pub mod namespace;

use axum::{routing::get, Router};
use std::sync::Arc;

use quasar_core::CatalogStore;

/// Unified API configuration (empty for now, reserved for future extensions).
#[derive(Clone, Default)]
pub struct UnifiedConfig;

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
}
