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

/// Configuration for Iceberg REST Catalog endpoints.
#[derive(Clone, Default)]
pub struct IcebergConfig {
    pub warehouse_path: Option<String>,
    pub object_store: Option<std::sync::Arc<dyn object_store::ObjectStore>>,
    pub s3_bucket: Option<String>,
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
            get(namespace::get_namespace).delete(namespace::drop_namespace),
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
            "/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}",
            get(table::load_table)
                .post(table::commit_table)
                .delete(table::drop_table)
                .head(table::table_exists),
        )
        .route(
            "/iceberg/v1/{prefix}/tables/rename",
            post(table::rename_table),
        )
}

pub mod config {
    use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
    use serde::Serialize;
    use std::collections::HashMap;

    use super::error::IcebergError;
    use super::IcebergConfig;

    #[derive(Serialize)]
    pub struct ConfigResponse {
        pub defaults: HashMap<String, String>,
        pub overrides: HashMap<String, String>,
    }

    pub async fn get_config(
        Extension(config): Extension<IcebergConfig>,
    ) -> Result<impl IntoResponse, IcebergError> {
        let mut defaults = HashMap::new();
        let overrides = HashMap::new();

        if let Some(ref warehouse) = config.warehouse_path {
            defaults.insert("warehouse".to_string(), warehouse.clone());
        }

        Ok((
            StatusCode::OK,
            Json(ConfigResponse {
                defaults,
                overrides,
            }),
        ))
    }
}
