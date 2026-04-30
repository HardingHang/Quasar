use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{AssetFormat, CatalogStore, MetricsState, StoreError};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitTableRequest, CreateTableRequest, ListTablesQuery, ListTablesResponse, LoadTableResponse,
    RenameTableRequest, TableIdentifier,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
use super::table_metadata::TableMetadata;
use super::IcebergConfig;
use crate::object_store_util::{object_exists, read_json, s3_url_to_path, write_json};
use quasar_core::validate_name;

// ── Object Store Helpers ───────────────────────────────────

/// Write metadata JSON to object store if configured.
/// After writing, verifies the file exists using HEAD operation.
async fn write_metadata_to_store(
    location: &str,
    content: &serde_json::Value,
    config: &IcebergConfig,
) -> Result<(), IcebergError> {
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        let path =
            s3_url_to_path(location, bucket).ok_or_else(|| IcebergError::InternalServerError {
                message: format!("invalid metadata location: {}", location),
            })?;

        write_json(&**store, &path, content).await.map_err(|e| {
            IcebergError::InternalServerError {
                message: format!("failed to write metadata to object store: {}", e),
            }
        })?;

        if !object_exists(&**store, &path).await {
            return Err(IcebergError::InternalServerError {
                message: format!("write verification failed for {}", location),
            });
        }
    }
    Ok(())
}

/// Read metadata JSON from object store if configured, otherwise fallback.
async fn read_metadata_from_store(
    location: &str,
    fallback: Option<serde_json::Value>,
    config: &IcebergConfig,
) -> Result<serde_json::Value, IcebergError> {
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        let path =
            s3_url_to_path(location, bucket).ok_or_else(|| IcebergError::InternalServerError {
                message: format!("invalid metadata location: {}", location),
            })?;
        read_json(&**store, &path)
            .await
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to read metadata from object store: {}", e),
            })
    } else if let Some(fb) = fallback {
        Ok(fb)
    } else {
        Err(IcebergError::InternalServerError {
            message: "no metadata available".to_string(),
        })
    }
}

/// Check if metadata.json exists in object store using HEAD operation.
/// Returns true if file exists, false if it doesn't.
/// If object_store is not configured, returns true (assumes fallback is available).
async fn check_metadata_exists(location: &str, config: &IcebergConfig) -> bool {
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        if let Some(path) = s3_url_to_path(location, bucket) {
            object_exists(&**store, &path).await
        } else {
            false
        }
    } else {
        true
    }
}

// ── Helpers ────────────────────────────────────────────────

/// Generate the next metadata location by incrementing the sequence number.
/// Expected format: `{...}/metadata/{NNNNN}-{uuid}.metadata.json`
/// Handles overflow by extending width when sequence exceeds current width.
fn next_metadata_location(current: &str) -> String {
    if let Some(metadata_idx) = current.rfind("/metadata/") {
        let prefix = &current[..metadata_idx + 10];
        let rest = &current[metadata_idx + 10..];
        if let Some(dash_idx) = rest.find('-') {
            let seq_str = &rest[..dash_idx];
            if let Ok(seq) = seq_str.parse::<u64>() {
                let suffix = &rest[dash_idx..];
                let new_seq = seq + 1;
                // Handle overflow: if new_seq needs more digits, extend width
                let new_width = seq_str.len().max(new_seq.to_string().len());
                let new_seq_str = format!("{:0width$}", new_seq, width = new_width);
                return format!("{}{}{}", prefix, new_seq_str, suffix);
            }
        }
    }
    // Fallback: append timestamp-based sequence if format is unrecognized
    let timestamp = chrono::Utc::now().timestamp_millis();
    format!(
        "{}-{}.metadata.json",
        current.trim_end_matches(".metadata.json"),
        timestamp
    )
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
                    name: a.asset.name,
                })
                .collect(),
            next_page_token: None,
        }),
    ))
}

/// POST /iceberg/v1/namespaces/{ns}/tables
pub async fn create_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path(ns): Path<String>,
    Json(req): Json<CreateTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_name(&ns).map_err(store_error_to_iceberg_table)?;
    validate_name(&req.name).map_err(store_error_to_iceberg_table)?;

    let location = req.location.unwrap_or_else(|| {
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
    write_metadata_to_store(&metadata_location, &metadata_json, &config).await?;

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
    Extension(config): Extension<IcebergConfig>,
    Path((ns, table)): Path<(String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let (asset, tabular) = store
        .get_asset_with_tabular(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    // Check metadata.json exists before loading
    if let Some(ref ml) = tabular.metadata_location {
        if !check_metadata_exists(ml, &config).await {
            return Err(IcebergError::MetadataNotFoundException {
                message: format!("metadata.json not found at {}", ml),
            });
        }
    }

    let metadata = if let Some(ref ml) = tabular.metadata_location {
        read_metadata_from_store(ml, tabular.schema_snapshot.clone(), &config).await?
    } else {
        tabular.schema_snapshot.unwrap_or_else(|| {
            build_initial_metadata(asset.id, &asset.name, &tabular.location, None)
        })
    };

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: tabular.metadata_location,
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
    Extension(config): Extension<IcebergConfig>,
    metrics: Option<Extension<MetricsState>>,
    Path((ns, table)): Path<(String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let metrics = metrics.map(|e| e.0);
    // 1. Load the current asset with tabular detail
    let (_asset, tabular) = store
        .get_asset_with_tabular(&ns, AssetFormat::Iceberg, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    let metadata_location =
        tabular
            .metadata_location
            .ok_or_else(|| IcebergError::CommitFailedException {
                message: format!("Table '{}.{}' has no metadata location", ns, table),
            })?;

    // 2. Check metadata.json exists before committing (CAS requires current metadata)
    if !check_metadata_exists(&metadata_location, &config).await {
        return Err(IcebergError::CommitFailedException {
            message: format!(
                "Cannot commit: metadata.json not found at {}",
                metadata_location
            ),
        });
    }

    // 3. Load current metadata from object store (or fallback to schema_snapshot)
    let current_metadata_json =
        read_metadata_from_store(&metadata_location, tabular.schema_snapshot.clone(), &config)
            .await?;
    let mut table_metadata = serde_json::from_value::<TableMetadata>(current_metadata_json)
        .map_err(|e| IcebergError::InternalServerError {
            message: format!("Failed to parse table metadata: {}", e),
        })?;

    // 4. Check requirements
    if let Err(msg) = table_metadata.check_requirements(&req.requirements) {
        return Err(IcebergError::CommitFailedException { message: msg });
    }

    // 5. Save current properties before applying updates
    let old_properties = table_metadata.properties.clone();

    // 6. Apply updates
    table_metadata.apply_updates(&req.updates);

    // 7. Compute property changes
    let property_removals: Vec<String> = old_properties
        .keys()
        .filter(|k| !table_metadata.properties.contains_key(*k))
        .cloned()
        .collect();
    let property_updates: HashMap<String, String> = table_metadata
        .properties
        .iter()
        .filter(|(k, v)| old_properties.get(*k) != Some(v))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // 8. Generate new metadata location
    let new_metadata_location = next_metadata_location(&metadata_location);

    // 9. Serialize new metadata
    let new_schema_snapshot =
        serde_json::to_value(&table_metadata).map_err(|e| IcebergError::InternalServerError {
            message: format!("Failed to serialize table metadata: {}", e),
        })?;

    // 10. Write new metadata.json to object store
    write_metadata_to_store(&new_metadata_location, &new_schema_snapshot, &config).await?;

    // 11. CAS update via storage layer (atomically updates metadata_location, schema_snapshot, and properties)
    store
        .cas_update_metadata_location(
            &ns,
            &table,
            AssetFormat::Iceberg,
            &metadata_location,
            &new_metadata_location,
            Some(new_schema_snapshot.clone()),
            &property_removals,
            &property_updates,
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

    // 10. Return updated table
    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_schema_snapshot,
        }),
    ))
}
