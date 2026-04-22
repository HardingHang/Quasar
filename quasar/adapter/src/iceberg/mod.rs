pub mod dto;
pub mod error;
pub mod namespace;
pub mod table;
pub mod table_metadata;

use axum::{
    routing::{get, post},
    Router,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct IcebergConfig {
    pub warehouse_path: Option<String>,
    pub object_store: Option<std::sync::Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
}

static ICEBERG_CONFIG: std::sync::Mutex<Option<IcebergConfig>> = std::sync::Mutex::new(None);

pub(crate) fn iceberg_config() -> IcebergConfig {
    ICEBERG_CONFIG
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_default()
}

pub fn routes(config: IcebergConfig) -> Router<Arc<dyn CatalogStore>> {
    *ICEBERG_CONFIG.lock().unwrap_or_else(|e| e.into_inner()) = Some(config);
    Router::new()
        .route("/iceberg/v1/config", get(config::get_config))
        .route(
            "/iceberg/v1/namespaces",
            get(namespace::list_namespaces).post(namespace::create_namespace),
        )
        .route(
            "/iceberg/v1/namespaces/{ns}",
            get(namespace::get_namespace).delete(namespace::drop_namespace),
        )
        .route(
            "/iceberg/v1/namespaces/{ns}/properties",
            post(namespace::update_namespace_properties),
        )
        .route(
            "/iceberg/v1/namespaces/{ns}/tables",
            get(table::list_tables).post(table::create_table),
        )
        .route(
            "/iceberg/v1/namespaces/{ns}/tables/{table}",
            get(table::load_table)
                .post(table::commit_table)
                .delete(table::drop_table)
                .head(table::table_exists),
        )
        .route("/iceberg/v1/tables/rename", post(table::rename_table))
}

pub mod config {
    use axum::{http::StatusCode, response::IntoResponse, Json};
    use serde::Serialize;
    use std::collections::HashMap;

    use super::error::IcebergError;
    use super::iceberg_config;

    #[derive(Serialize)]
    pub struct ConfigResponse {
        pub defaults: HashMap<String, String>,
        pub overrides: HashMap<String, String>,
    }

    pub async fn get_config() -> Result<impl IntoResponse, IcebergError> {
        let config = iceberg_config();
        let mut defaults = HashMap::new();
        let overrides = HashMap::new();

        if let Some(ref warehouse) = config.warehouse_path {
            defaults.insert("warehouse".to_string(), warehouse.clone());
        }

        Ok((StatusCode::OK, Json(ConfigResponse { defaults, overrides })))
    }
}
