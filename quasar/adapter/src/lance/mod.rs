pub mod error;
pub mod namespace;
pub mod table;
pub mod version;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for Lance REST Namespace endpoints.
#[derive(Clone, Default)]
pub struct LanceConfig {
    /// Base warehouse path for table location allocation.
    /// e.g. `s3://bucket/warehouse/` or `file:///tmp/warehouse/`
    pub warehouse_path: Option<String>,
    /// Storage options passed back to clients for object store access.
    pub storage_options: HashMap<String, String>,
}

static LANCE_CONFIG: std::sync::OnceLock<LanceConfig> = std::sync::OnceLock::new();

pub(crate) fn lance_config() -> &'static LanceConfig {
    LANCE_CONFIG.get().unwrap_or_else(|| {
        static DEFAULT: std::sync::OnceLock<LanceConfig> = std::sync::OnceLock::new();
        DEFAULT.get_or_init(LanceConfig::default)
    })
}

/// Create Lance REST Namespace routes mounted at `/lance/v1/...`.
pub fn routes(config: LanceConfig) -> Router<Arc<dyn CatalogStore>> {
    let _ = LANCE_CONFIG.set(config);
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
