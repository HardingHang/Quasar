pub mod dispatch;
pub mod dto;
pub mod error;
pub mod metadata;
pub mod namespace;
pub mod table;
pub mod view;
pub mod view_metadata;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::IcebergCatalogStore;
use std::collections::HashMap;
use std::sync::Arc;

use crate::iceberg::error::IcebergError;

/// Convert core JSON properties into the protocol string-map shape. The
/// Iceberg write path only accepts string values, so non-string values are
/// dropped defensively.
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

/// Convert a protocol string-map into core JSON properties; an empty map
/// is stored as `None` (no properties).
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

/// Configuration for Iceberg REST Catalog endpoints.
#[derive(Clone, Default)]
pub struct IcebergConfig {
    pub warehouse_path: Option<String>,
    pub object_store: Option<std::sync::Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
    pub default_warehouse: String,
}

/// Commit outcome metrics sink for the Iceberg adapter.
///
/// Metrics are a server-side observability concern, but the adapter cannot
/// depend on the server crate (layering rule). The server therefore adapts
/// its registry into this lightweight handle and injects it via an
/// `Extension`; handlers extract it as `Option<Extension<CommitMetrics>>`
/// and fall back to the no-op `Default` when it is absent, so the adapter
/// also works standalone (tests, embedded use).
#[derive(Clone, Default)]
pub struct CommitMetrics {
    on_success: Option<Arc<dyn Fn() + Send + Sync>>,
    on_conflict: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl CommitMetrics {
    /// Build a sink from two callbacks (typically closures incrementing
    /// the server registry's commit counters).
    pub fn new(
        on_success: impl Fn() + Send + Sync + 'static,
        on_conflict: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            on_success: Some(Arc::new(on_success)),
            on_conflict: Some(Arc::new(on_conflict)),
        }
    }

    /// Record a successful commit. No-op when no sink is installed.
    pub fn record_success(&self) {
        if let Some(f) = &self.on_success {
            f();
        }
    }

    /// Record a commit conflict (CAS failure). No-op when no sink is installed.
    pub fn record_conflict(&self) {
        if let Some(f) = &self.on_conflict {
            f();
        }
    }
}

/// Create Iceberg REST Catalog routes mounted at `/iceberg/v1/...`.
/// Configuration is passed via Extension layer.
///
/// Namespace paths are hierarchical (D3): everything under
/// `/namespaces/{*path}` is dispatched by [`dispatch`], which splits the
/// reserved trailing segments (`tables`, `views`, `properties`,
/// `register`, `metrics`) from the namespace path.
pub fn routes() -> Router<Arc<dyn IcebergCatalogStore>> {
    Router::new()
        .route("/iceberg/v1/config", get(config::get_config))
        .route(
            "/iceberg/v1/{prefix}/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{*path}",
            get(dispatch::get)
                .head(dispatch::head)
                .delete(dispatch::delete)
                .post(dispatch::post),
        )
        .route(
            "/iceberg/v1/{prefix}/tables/rename",
            post(table::rename_table),
        )
        .route(
            "/iceberg/v1/{prefix}/transactions/commit",
            post(table::commit_transaction),
        )
        .route("/iceberg/v1/{prefix}/views/rename", post(view::rename_view))
}

/// Validate warehouse query parameter against the configured default warehouse.
/// Returns Ok if no warehouse is specified or it matches the default.
pub fn validate_warehouse(
    query_warehouse: Option<&str>,
    config: &IcebergConfig,
) -> Result<(), IcebergError> {
    match query_warehouse {
        None => Ok(()),
        Some(name) if name == config.default_warehouse => Ok(()),
        Some(name) => Err(IcebergError::NoSuchWarehouseException {
            message: format!("Warehouse does not exist: {}", name),
        }),
    }
}

pub mod config {
    use axum::{
        extract::{Extension, Query},
        http::StatusCode,
        response::IntoResponse,
        Json,
    };
    use serde::Serialize;
    use std::collections::HashMap;

    use super::dto::ConfigQuery;
    use super::error::IcebergError;
    use super::{validate_warehouse, IcebergConfig};

    #[derive(Serialize)]
    pub struct ConfigResponse {
        pub defaults: HashMap<String, String>,
        pub overrides: HashMap<String, String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub endpoints: Vec<String>,
    }

    fn supported_endpoints() -> Vec<String> {
        vec![
            "GET /v1/config".to_string(),
            "GET /v1/{prefix}/namespaces".to_string(),
            "POST /v1/{prefix}/namespaces".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}".to_string(),
            "DELETE /v1/{prefix}/namespaces/{namespace}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/properties".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/tables".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/register".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics".to_string(),
            "POST /v1/{prefix}/tables/rename".to_string(),
            "POST /v1/{prefix}/transactions/commit".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/views".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/views".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "POST /v1/{prefix}/views/rename".to_string(),
        ]
    }

    pub async fn get_config(
        Extension(config): Extension<IcebergConfig>,
        Query(query): Query<ConfigQuery>,
    ) -> Result<impl IntoResponse, IcebergError> {
        validate_warehouse(query.warehouse.as_deref(), &config)?;

        let mut defaults = HashMap::new();
        let mut overrides = HashMap::new();

        if let Some(ref warehouse) = config.warehouse_path {
            defaults.insert("warehouse".to_string(), warehouse.clone());
        }

        if let Some(ref warehouse) = query.warehouse {
            overrides.insert("warehouse".to_string(), warehouse.clone());
        }

        Ok((
            StatusCode::OK,
            Json(ConfigResponse {
                defaults,
                overrides,
                endpoints: supported_endpoints(),
            }),
        ))
    }
}
