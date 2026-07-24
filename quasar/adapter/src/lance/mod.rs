pub mod error;
pub(crate) mod id;
pub mod namespace;
pub mod table;
pub mod version;

use axum::{
    extract::Extension,
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::collections::HashMap;
use std::sync::Arc;

/// Extract the request id injected by the server middleware as
/// `Extension<String>`; generate a fresh UUID when the extension is
/// absent (e.g. in tests).
pub(crate) fn request_id_of(extension: Option<Extension<String>>) -> String {
    match extension {
        Some(Extension(id)) => id,
        None => uuid::Uuid::new_v4().to_string(),
    }
}

/// Convert core JSON properties into the Lance string-map shape. The Lance
/// write path only accepts string values, so non-string values cannot
/// appear through this API and are dropped defensively.
pub(crate) fn properties_to_string_map(
    properties: Option<serde_json::Value>,
) -> HashMap<String, String> {
    match properties {
        Some(serde_json::Value::Object(map)) => map
            .into_iter()
            .filter_map(|(key, value)| value.as_str().map(|s| (key, s.to_string())))
            .collect(),
        _ => HashMap::new(),
    }
}

/// Convert a Lance string-map into core JSON properties; an empty map is
/// stored as `None` (no properties).
pub(crate) fn string_map_to_properties(map: HashMap<String, String>) -> Option<serde_json::Value> {
    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, serde_json::Value::String(value)))
                .collect(),
        ))
    }
}

/// Configuration for Lance REST Namespace endpoints.
#[derive(Clone, Default)]
pub struct LanceConfig {
    /// Base warehouse path for table location allocation.
    /// e.g. `s3://bucket/warehouse/` or `file:///tmp/warehouse/`
    pub warehouse_path: Option<String>,
    /// Storage options passed back to clients for object store access.
    pub storage_options: HashMap<String, String>,
}

/// Create Lance REST Namespace routes mounted at `/lance/v1/...`.
/// Configuration is passed via Extension layer.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        // Namespace routes
        .route(
            "/lance/v1/namespace/{id}/create",
            post(namespace::create_namespace),
        )
        .route(
            "/lance/v1/namespace/{id}/list",
            get(namespace::list_namespaces),
        )
        .route(
            "/lance/v1/namespace/{id}/describe",
            post(namespace::describe_namespace),
        )
        .route(
            "/lance/v1/namespace/{id}/drop",
            post(namespace::drop_namespace),
        )
        .route(
            "/lance/v1/namespace/{id}/exists",
            post(namespace::namespace_exists),
        )
        // Table list route (nested under namespace path)
        .route(
            "/lance/v1/namespace/{id}/table/list",
            get(namespace::list_tables),
        )
        // Table routes
        .route("/lance/v1/table/{id}/declare", post(table::declare_table))
        .route("/lance/v1/table/{id}/describe", post(table::describe_table))
        .route("/lance/v1/table/{id}/register", post(table::register_table))
        .route(
            "/lance/v1/table/{id}/deregister",
            post(table::deregister_table),
        )
        .route("/lance/v1/table/{id}/drop", post(table::drop_table))
        .route("/lance/v1/table/{id}/exists", post(table::table_exists))
        .route("/lance/v1/table/{id}/rename", post(table::rename_table))
        // Version routes
        .route(
            "/lance/v1/table/{id}/version/create",
            post(version::create_version),
        )
        .route(
            "/lance/v1/table/{id}/version/list",
            get(version::list_versions),
        )
        .route(
            "/lance/v1/table/{id}/version/describe",
            post(version::describe_version),
        )
}
