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
    CommitTableRequest, CreateTableRequest, ListTablesQuery, ListTablesResponse,
    LoadTableResponse, RenameTableRequest, TableIdentifier,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
use super::iceberg_config;
use super::table_metadata::TableMetadata;

// ── Helpers ────────────────────────────────────────────────

/// Generate the next metadata location by incrementing the sequence number.
/// Expected format: `{...}/metadata/{NNNNN}-{uuid}.metadata.json`
fn next_metadata_location(current: &str) -> String {
    if let Some(metadata_idx) = current.rfind("/metadata/") {
        let prefix = &current[..metadata_idx + 10];
        let rest = &current[metadata_idx + 10..];
        if let Some(dash_idx) = rest.find('-') {
            let seq_str = &rest[..dash_idx];
            if let Ok(seq) = seq_str.parse::<u32>() {
                let suffix = &rest[dash_idx..];
                let new_seq_str = format!("{:0width$}", seq + 1, width = seq_str.len());
                return format!("{}{}{}", prefix, new_seq_str, suffix);
            }
        }
    }
    current.to_string()
}

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

    let metadata_json = metadata.clone();

    let _asset = store
        .create_asset(
            &ns,
            AssetFormat::Iceberg,
            &req.name,
            &location,
            Some(&metadata_location),
            Some(metadata_json),
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

/// POST /iceberg/v1/namespaces/{ns}/tables/{table}
/// Commit table updates (CAS).
pub async fn commit_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((ns, table)): Path<(String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    // 1. Load the current asset
    let asset = store
        .get_asset(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    let metadata_location = asset.metadata_location.ok_or_else(|| {
        IcebergError::CommitFailedException {
            message: format!("Table '{}.{}' has no metadata location", ns, table),
        }
    })?;

    // 2. Parse current metadata from schema_snapshot
    let mut table_metadata = if let Some(ref snapshot) = asset.schema_snapshot {
        serde_json::from_value::<TableMetadata>(snapshot.clone()).map_err(|e| {
            IcebergError::InternalServerError {
                message: format!("Failed to parse table metadata: {}", e),
            }
        })?
    } else {
        return Err(IcebergError::CommitFailedException {
            message: format!(
                "Table '{}.{}' has no metadata snapshot to commit against",
                ns, table
            ),
        });
    };

    // 3. Check requirements
    if let Err(msg) = table_metadata.check_requirements(&req.requirements) {
        return Err(IcebergError::CommitFailedException { message: msg });
    }

    // 4. Apply updates
    table_metadata.apply_updates(&req.updates);

    // 5. Generate new metadata location
    let new_metadata_location = next_metadata_location(&metadata_location);

    // 6. Serialize new metadata
    let new_schema_snapshot = serde_json::to_value(&table_metadata).map_err(|e| {
        IcebergError::InternalServerError {
            message: format!("Failed to serialize table metadata: {}", e),
        }
    })?;

    // 7. CAS update via storage layer
    store
        .commit_iceberg_table(
            &ns,
            &table,
            &metadata_location,
            &new_metadata_location,
            Some(new_schema_snapshot.clone()),
        )
        .await
        .map_err(|e| match e {
            StoreError::Conflict(msg) => IcebergError::CommitFailedException { message: msg },
            StoreError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
            other => store_error_to_iceberg_table(other),
        })?;

    // 8. Return updated table
    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_schema_snapshot,
        }),
    ))
}
