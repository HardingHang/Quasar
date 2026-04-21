use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{AssetFormat, CatalogStore, StoreError};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CreateTableRequest, ListTablesQuery, ListTablesResponse, LoadTableResponse,
    RenameTableRequest, TableIdentifier,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
use super::iceberg_config;

// ── Helpers ────────────────────────────────────────────────

fn build_initial_metadata(
    table_uuid: Uuid,
    _table_name: &str,
    location: &str,
    schema: Option<&serde_json::Value>,
) -> serde_json::Value {
    let schema = schema.cloned().unwrap_or_else(|| {
        json!({
            "type": "struct",
            "schema-id": 0,
            "fields": []
        })
    });

    json!({
        "format-version": 2,
        "table-uuid": table_uuid.to_string(),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": chrono::Utc::now().timestamp_millis(),
        "last-column-id": 0,
        "schemas": [schema],
        "current-schema-id": 0,
        "partition-specs": [],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "sort-orders": [],
        "default-sort-order-id": 0,
        "refs": {}
    })
}

// ── Handlers ───────────────────────────────────────────────

/// GET /iceberg/v1/namespaces/{ns}/tables
pub async fn list_tables(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(ns): Path<String>,
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    let _ = query; // pagination placeholder for MVP

    let assets = store
        .list_assets(&ns, AssetFormat::Iceberg)
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok((
        StatusCode::OK,
        Json(ListTablesResponse {
            identifiers: assets
                .into_iter()
                .map(|a| TableIdentifier {
                    namespace: vec![ns.clone()],
                    name: a.name,
                })
                .collect(),
            next_page_token: None,
        }),
    ))
}

/// POST /iceberg/v1/namespaces/{ns}/tables
pub async fn create_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(ns): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let location = req.location.unwrap_or_else(|| {
        let config = iceberg_config();
        if let Some(ref wp) = config.warehouse_path {
            format!("{}/{}/{}", wp.trim_end_matches('/'), ns, req.name)
        } else {
            format!("iceberg://{}/{}", ns, req.name)
        }
    });

    let table_uuid = Uuid::new_v4();
    let metadata = build_initial_metadata(table_uuid, &req.name, &location, req.schema.as_ref());

    let metadata_location = format!("{}/metadata/00001-{}.metadata.json", location, table_uuid);

    let mut properties = req.properties;
    properties.insert("table-uuid".to_string(), table_uuid.to_string());

    let _asset = store
        .create_asset(
            &ns,
            AssetFormat::Iceberg,
            &req.name,
            &location,
            Some(&metadata_location),
            properties,
        )
        .await
        .map_err(|e| match e {
            StoreError::NotFound(ref msg) if msg.starts_with("namespace") => {
                IcebergError::NoSuchNamespaceException {
                    message: msg.clone(),
                }
            }
            other => store_error_to_iceberg_table(other),
        })?;

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(metadata_location),
            metadata,
        }),
    ))
}

/// GET /iceberg/v1/namespaces/{ns}/tables/{table}
pub async fn load_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let asset = store
        .get_asset(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    let metadata = asset.schema_snapshot.unwrap_or_else(|| {
        build_initial_metadata(asset.id, &asset.name, &asset.location, None)
    });

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: asset.metadata_location,
            metadata,
        }),
    ))
}

/// DELETE /iceberg/v1/namespaces/{ns}/tables/{table}
pub async fn drop_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    store
        .drop_asset(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/namespaces/{ns}/tables/{table}
pub async fn table_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let exists = store
        .asset_exists(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    if exists {
        Ok(StatusCode::OK)
    } else {
        Err(IcebergError::NoSuchTableException {
            message: format!("Table '{}.{}' not found", ns, table),
        })
    }
}

/// POST /iceberg/v1/tables/rename
pub async fn rename_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Json(req): Json<RenameTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let src_ns = req.source.namespace.first().ok_or_else(|| {
        IcebergError::BadRequestException {
            message: "source namespace must not be empty".to_string(),
        }
    })?;
    let dst_ns = req.destination.namespace.first().ok_or_else(|| {
        IcebergError::BadRequestException {
            message: "destination namespace must not be empty".to_string(),
        }
    })?;

    if src_ns != dst_ns {
        return Err(IcebergError::BadRequestException {
            message: "cross-namespace rename not supported".to_string(),
        });
    }

    store
        .rename_asset(src_ns, AssetFormat::Iceberg, &req.source.name, &req.destination.name)
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok(StatusCode::OK)
}
