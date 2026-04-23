use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{AssetFormat, CatalogStore, MetricsState, StoreError};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitTableRequest, CreateTableRequest, ListTablesQuery, ListTablesResponse, LoadTableResponse,
    RenameTableRequest, TableIdentifier,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
use super::iceberg_config;
use super::table_metadata::TableMetadata;
use quasar_core::validate_name;

// ── Object Store Helpers ───────────────────────────────────

/// Convert an S3 URL to an object store relative Path.
fn s3_url_to_object_path(location: &str, bucket: &str) -> Option<object_store::path::Path> {
    let prefix = format!("s3://{}/", bucket);
    location
        .strip_prefix(&prefix)
        .map(object_store::path::Path::from)
}

/// Write metadata JSON to object store if configured.
async fn write_metadata_to_store(
    location: &str,
    content: &serde_json::Value,
) -> Result<(), IcebergError> {
    let config = iceberg_config();
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        let path = s3_url_to_object_path(location, bucket).ok_or_else(|| {
            IcebergError::InternalServerError {
                message: format!("invalid metadata location: {}", location),
            }
        })?;
        let payload = object_store::PutPayload::from(content.to_string());
        store
            .put(&path, payload)
            .await
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to write metadata to object store: {}", e),
            })?;
    }
    Ok(())
}

/// Read metadata JSON from object store if configured, otherwise fallback.
async fn read_metadata_from_store(
    location: &str,
    fallback: Option<serde_json::Value>,
) -> Result<serde_json::Value, IcebergError> {
    let config = iceberg_config();
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        let path = s3_url_to_object_path(location, bucket).ok_or_else(|| {
            IcebergError::InternalServerError {
                message: format!("invalid metadata location: {}", location),
            }
        })?;
        let result = store
            .get(&path)
            .await
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to read metadata from object store: {}", e),
            })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to read metadata bytes: {}", e),
            })?;
        serde_json::from_slice(&bytes).map_err(|e| IcebergError::InternalServerError {
            message: format!("failed to parse metadata JSON: {}", e),
        })
    } else if let Some(fb) = fallback {
        Ok(fb)
    } else {
        Err(IcebergError::InternalServerError {
            message: "no metadata available".to_string(),
        })
    }
}

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

    // Compute last-column-id from schema fields
    let last_column_id = schema
        .get("fields")
        .and_then(|f| f.as_array())
        .map(|fields| {
            fields
                .iter()
                .filter_map(|f| f.get("id").and_then(|id| id.as_i64()))
                .max()
                .unwrap_or(0) as i32
        })
        .unwrap_or(0);

    json!({
        "format-version": 2,
        "table-uuid": table_uuid.to_string(),
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": chrono::Utc::now().timestamp_millis(),
        "last-column-id": last_column_id,
        "schemas": [schema],
        "current-schema-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "default-spec-id": 0,
        "last-partition-id": 999,
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "sort-orders": [{"order-id": 0, "fields": []}],
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
    validate_name(&ns).map_err(store_error_to_iceberg_table)?;
    validate_name(&req.name).map_err(store_error_to_iceberg_table)?;

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

    // Write initial metadata.json to object store
    write_metadata_to_store(&metadata_location, &metadata_json).await?;

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

    let metadata = if let Some(ref ml) = asset.metadata_location {
        read_metadata_from_store(ml, asset.schema_snapshot.clone()).await?
    } else {
        asset
            .schema_snapshot
            .unwrap_or_else(|| build_initial_metadata(asset.id, &asset.name, &asset.location, None))
    };

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
    validate_name(&req.source.name).map_err(store_error_to_iceberg_table)?;
    validate_name(&req.destination.name).map_err(store_error_to_iceberg_table)?;

    let src_ns = req
        .source
        .namespace
        .first()
        .ok_or_else(|| IcebergError::BadRequestException {
            message: "source namespace must not be empty".to_string(),
        })?;
    let dst_ns =
        req.destination
            .namespace
            .first()
            .ok_or_else(|| IcebergError::BadRequestException {
                message: "destination namespace must not be empty".to_string(),
            })?;

    if src_ns != dst_ns {
        return Err(IcebergError::BadRequestException {
            message: "cross-namespace rename not supported".to_string(),
        });
    }

    store
        .rename_asset(
            src_ns,
            AssetFormat::Iceberg,
            &req.source.name,
            &req.destination.name,
        )
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok(StatusCode::OK)
}

/// POST /iceberg/v1/namespaces/{ns}/tables/{table}
/// Commit table updates (CAS).
pub async fn commit_table(
    State(store): State<Arc<dyn CatalogStore>>,
    metrics: Option<Extension<MetricsState>>,
    Path((ns, table)): Path<(String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let metrics = metrics.map(|e| e.0);
    // 1. Load the current asset
    let asset = store
        .get_asset(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    let metadata_location =
        asset
            .metadata_location
            .ok_or_else(|| IcebergError::CommitFailedException {
                message: format!("Table '{}.{}' has no metadata location", ns, table),
            })?;

    // 2. Load current metadata from object store (or fallback to schema_snapshot)
    let current_metadata_json =
        read_metadata_from_store(&metadata_location, asset.schema_snapshot.clone()).await?;
    let mut table_metadata = serde_json::from_value::<TableMetadata>(current_metadata_json)
        .map_err(|e| IcebergError::InternalServerError {
            message: format!("Failed to parse table metadata: {}", e),
        })?;

    // 3. Check requirements
    if let Err(msg) = table_metadata.check_requirements(&req.requirements) {
        return Err(IcebergError::CommitFailedException { message: msg });
    }

    // 4. Apply updates
    table_metadata.apply_updates(&req.updates);

    // 5. Generate new metadata location
    let new_metadata_location = next_metadata_location(&metadata_location);

    // 6. Serialize new metadata
    let new_schema_snapshot =
        serde_json::to_value(&table_metadata).map_err(|e| IcebergError::InternalServerError {
            message: format!("Failed to serialize table metadata: {}", e),
        })?;

    // 7. Write new metadata.json to object store
    write_metadata_to_store(&new_metadata_location, &new_schema_snapshot).await?;

    // 8. CAS update via storage layer
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
            StoreError::Conflict(msg) => {
                if let Some(ref m) = metrics {
                    m.registry.record_iceberg_commit_conflict();
                }
                IcebergError::CommitFailedException { message: msg }
            }
            StoreError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
            other => store_error_to_iceberg_table(other),
        })?;

    if let Some(ref m) = metrics {
        m.registry.record_iceberg_commit_success();
    }

    // 9. Return updated table
    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_schema_snapshot,
        }),
    ))
}
