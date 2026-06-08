#[cfg(test)]
pub mod crate_validation;
pub mod dto;
pub mod error;
pub mod metadata;
pub mod namespace;
pub mod scan_planning;
pub mod table;
pub mod table_metadata;
pub mod view;
pub mod view_metadata;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

use crate::iceberg::error::IcebergError;

/// Configuration for Iceberg REST Catalog endpoints.
#[derive(Clone, Default)]
pub struct IcebergConfig {
    pub warehouse_path: Option<String>,
    pub object_store: Option<std::sync::Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
    pub default_warehouse: String,
}

/// Create Iceberg REST Catalog routes mounted at `/iceberg/v1/...`.
/// Configuration is passed via Extension layer.
pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        .route("/iceberg/v1/config", get(config::get_config))
        .route(
            "/iceberg/v1/{prefix}/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}",
            get(namespace::get_namespace)
                .head(namespace::namespace_exists)
                .delete(namespace::drop_namespace),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/properties",
            post(namespace::update_namespace_properties),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables",
            get(table::list_tables).post(table::create_table),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/register",
            post(table::register_table),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}",
            get(table::load_table)
                .post(table::commit_table)
                .delete(table::drop_table)
                .head(table::table_exists),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/metrics",
            post(table::report_metrics),
        )
        .route(
            "/iceberg/v1/{prefix}/tables/rename",
            post(table::rename_table),
        )
        .route(
            "/iceberg/v1/{prefix}/transactions/commit",
            post(table::commit_transaction),
        )
        // View routes (V4.2)
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/views",
            get(view::list_views).post(view::create_view),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/views/{view}",
            get(view::load_view)
                .post(view::replace_view)
                .delete(view::drop_view)
                .head(view::view_exists),
        )
        .route("/iceberg/v1/{prefix}/views/rename", post(view::rename_view))
        // Scan Planning routes (V4.2) - OpenAPI primary paths
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan",
            post(scan_planning::submit_plan),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan/{plan_id}",
            get(scan_planning::fetch_plan).delete(scan_planning::cancel_plan),
        )
        .route(
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/tasks",
            post(scan_planning::fetch_tasks),
        )
        // Scan Planning alias paths (Java ResourcePaths)
        .route(
            "/iceberg/v1/{prefix}/tables/{table}/plan",
            post(scan_planning::submit_plan_alias),
        )
        .route(
            "/iceberg/v1/{prefix}/tables/{table}/plan/{plan_id}",
            get(scan_planning::fetch_plan_alias).delete(scan_planning::cancel_plan_alias),
        )
        .route(
            "/iceberg/v1/{prefix}/tables/{table}/tasks",
            post(scan_planning::fetch_tasks_alias),
        )
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
            // V4.2 View endpoints
            "GET /v1/{prefix}/namespaces/{namespace}/views".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/views".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
            "POST /v1/{prefix}/views/rename".to_string(),
            // V4.2 Scan Planning endpoints
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan".to_string(),
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}".to_string(),
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}".to_string(),
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks".to_string(),
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
