pub mod error;
pub mod namespace;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

/// Create Lance REST Namespace routes mounted at `/lance/v1/...`.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        .route("/lance/v1/namespace/{id}/create", post(namespace::create_namespace))
        .route("/lance/v1/namespace/{id}/list", get(namespace::list_namespaces))
        .route("/lance/v1/namespace/{id}/describe", post(namespace::describe_namespace))
        .route("/lance/v1/namespace/{id}/drop", post(namespace::drop_namespace))
        .route("/lance/v1/namespace/{id}/exists", post(namespace::namespace_exists))
}
