pub mod error;
pub mod namespace;
pub mod table;
pub mod version;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

/// Create Lance REST Namespace routes mounted at `/lance/v1/...`.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        // Namespace routes
        .route("/lance/v1/namespace/{id}/create", post(namespace::create_namespace))
        .route("/lance/v1/namespace/{id}/list", get(namespace::list_namespaces))
        .route("/lance/v1/namespace/{id}/describe", post(namespace::describe_namespace))
        .route("/lance/v1/namespace/{id}/drop", post(namespace::drop_namespace))
        .route("/lance/v1/namespace/{id}/exists", post(namespace::namespace_exists))
        // Table list route (nested under namespace path)
        .route("/lance/v1/namespace/{id}/table/list", get(namespace::list_tables))
        // Table routes
        .route("/lance/v1/table/{id}/declare", post(table::declare_table))
        .route("/lance/v1/table/{id}/describe", post(table::describe_table))
        .route("/lance/v1/table/{id}/register", post(table::register_table))
        .route("/lance/v1/table/{id}/deregister", post(table::deregister_table))
        .route("/lance/v1/table/{id}/drop", post(table::drop_table))
        .route("/lance/v1/table/{id}/exists", post(table::table_exists))
        .route("/lance/v1/table/{id}/rename", post(table::rename_table))
        // Version routes
        .route("/lance/v1/table/{id}/version/create", post(version::create_version))
        .route("/lance/v1/table/{id}/version/list", get(version::list_versions))
        .route("/lance/v1/table/{id}/version/describe", post(version::describe_version))
}
