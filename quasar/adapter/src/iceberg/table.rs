use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{CatalogStore, MetricsState, StoreError};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitTableRequest, CreateTableRequest, ListTablesQuery, ListTablesResponse, LoadTableResponse,
    RegisterTableRequest, RenameTableRequest, TableIdentifier,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
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

/// Fallback metadata builder for load_table when schema_snapshot is missing.
/// Uses hand-rolled JSON instead of the iceberg crate wrapper to avoid
/// error handling in the fallback path.
fn _build_fallback_metadata(
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

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/tables
pub async fn list_tables(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    let limit = query.page_size.unwrap_or(100).clamp(1, 1000);
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let assets = store
        .list_tabular_assets(&prefix, &ns, Some("iceberg"), offset, limit)
        .await
        .map_err(store_error_to_iceberg_table)?;

    let next_page_token = if assets.len() as i32 >= limit {
        Some((offset + assets.len() as i64).to_string())
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListTablesResponse {
            identifiers: assets
                .into_iter()
                .map(|(asset, _)| TableIdentifier {
                    namespace: vec![ns.clone()],
                    name: asset.name,
                })
                .collect(),
            next_page_token,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables
pub async fn create_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns)): Path<(String, String)>,
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
    let mut properties = req.properties;
    properties.insert("table-uuid".to_string(), table_uuid.to_string());

    let metadata = super::metadata::build_initial_metadata(
        table_uuid,
        &location,
        req.schema.as_ref(),
        properties.clone(),
    )
    .map_err(|msg| IcebergError::InternalServerError { message: msg })?;

    let metadata_location = format!("{}/metadata/00001-{}.metadata.json", location, table_uuid);

    // Write initial metadata.json to object store
    write_metadata_to_store(&metadata_location, &metadata, &config).await?;

    if req.stage_create == Some(true) {
        // Staged create: only insert staged record, no active catalog entry
        store
            .create_staged_table(
                &prefix,
                &ns,
                &req.name,
                table_uuid,
                &location,
                &metadata_location,
                metadata.clone(),
                properties,
            )
            .await
            .map_err(store_error_to_iceberg_table)?;
    } else {
        // Non-staged create: insert active catalog entry
        let _ = store
            .create_tabular_asset(
                &prefix,
                &ns,
                &req.name,
                "iceberg",
                &location,
                Some(&metadata_location),
                Some(metadata.clone()),
                properties,
            )
            .await
            .map_err(store_error_to_iceberg_table)?;
    }

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(metadata_location),
            metadata,
        }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}
pub async fn load_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns, table)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let (asset, tabular) = store
        .get_tabular_asset(&prefix, &ns, "iceberg", &table)
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
            _build_fallback_metadata(asset.id, &asset.name, &tabular.location, None)
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

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}
pub async fn drop_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns, table)): Path<(String, String, String)>,
    Query(query): Query<super::dto::DropTableQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    // V3: purgeRequested data cleanup is not implemented; reject if requested
    if query.purge_requested == Some(true) {
        return Err(IcebergError::NotImplementedException {
            message: "purgeRequested=true is not supported in V3".to_string(),
        });
    }

    // V3 endpoint-level isolation: only Iceberg-format assets are
    // visible to the Iceberg drop endpoint. An asset with the same
    // name in another format must surface as NoSuchTableException.
    store
        .get_tabular_asset(&prefix, &ns, "iceberg", &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    store
        .drop_asset(&prefix, &ns, &table)
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}
pub async fn table_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns, table)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let exists = match store
        .get_tabular_asset(&prefix, &ns, "iceberg", &table)
        .await
    {
        Ok(_) => true,
        Err(StoreError::NotFound(_)) => false,
        Err(e) => return Err(store_error_to_iceberg_table(e)),
    };

    if exists {
        Ok(StatusCode::OK)
    } else {
        Err(IcebergError::NoSuchTableException {
            message: format!("Table '{}.{}' not found", ns, table),
        })
    }
}

/// POST /iceberg/v1/{prefix}/tables/rename
pub async fn rename_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(prefix): Path<String>,
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

    let new_namespace = if src_ns == dst_ns {
        None
    } else {
        Some(dst_ns.as_str())
    };

    // V3 endpoint-level isolation: same rationale as drop_table —
    // Iceberg rename cannot operate on a Lance-format source asset.
    store
        .get_tabular_asset(&prefix, src_ns, "iceberg", &req.source.name)
        .await
        .map_err(store_error_to_iceberg_table)?;

    store
        .rename_asset(
            &prefix,
            src_ns,
            &req.source.name,
            &req.destination.name,
            new_namespace,
        )
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok(StatusCode::OK)
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/register
/// Register an externally-managed Iceberg table into the catalog.
pub async fn register_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns)): Path<(String, String)>,
    Json(req): Json<RegisterTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_name(&ns).map_err(store_error_to_iceberg_table)?;
    validate_name(&req.name).map_err(store_error_to_iceberg_table)?;

    // 1. Read metadata from object store
    let metadata_json =
        if let (Some(ref store_os), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
            let path = s3_url_to_path(&req.metadata_location, bucket).ok_or_else(|| {
                IcebergError::InternalServerError {
                    message: format!("invalid metadata location: {}", req.metadata_location),
                }
            })?;
            read_json(&**store_os, &path).await.map_err(|e| match e {
                object_store::Error::NotFound { .. } => IcebergError::MetadataNotFoundException {
                    message: format!("metadata not found at {}", req.metadata_location),
                },
                _ => IcebergError::InternalServerError {
                    message: format!("failed to read metadata: {}", e),
                },
            })?
        } else {
            return Err(IcebergError::InternalServerError {
                message: "object store not configured".to_string(),
            });
        };

    // 2. Validate metadata format using iceberg crate
    let metadata = super::metadata::parse_metadata(&metadata_json)
        .map_err(|msg| IcebergError::BadRequestException { message: msg })?;

    let location = metadata.location().to_string();

    // 3. Register catalog record (no object store write)
    store
        .register_iceberg_table(
            &prefix,
            &ns,
            &req.name,
            &location,
            &req.metadata_location,
            metadata_json.clone(),
            HashMap::new(),
        )
        .await
        .map_err(store_error_to_iceberg_table)?;

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(req.metadata_location),
            metadata: metadata_json,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}
/// Commit table updates (CAS).
///
/// Two paths:
/// - existing active table → CAS update via `cas_update_metadata_location`.
/// - active table absent but a non-expired staged record exists → finalize
///   the staged-create commit via `commit_staged_table`. The request must
///   carry `assert-create` (`TableRequirement::NotExist`).
///
/// V4_DESIGN.md §5.13, §6.4.
pub async fn commit_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    metrics: Option<Extension<MetricsState>>,
    Path((prefix, ns, table)): Path<(String, String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let metrics = metrics.map(|e| e.0);

    // 0. Reject any V4.0-unsupported official TableUpdate variant before IO.
    super::metadata::check_supported_updates(&req.updates).map_err(|u| {
        IcebergError::NotImplementedException {
            message: format!("update '{}' is not supported in V4.0", u.0),
        }
    })?;

    // 1. Look up the active table; on NotFound, fall through to staged commit.
    match store
        .get_tabular_asset(&prefix, &ns, "iceberg", &table)
        .await
    {
        Ok((_asset, tabular)) => {
            commit_existing_table(
                store.clone(),
                config,
                metrics,
                prefix,
                ns,
                table,
                tabular,
                req,
            )
            .await
        }
        Err(StoreError::NotFound(_)) => {
            commit_staged_table(store, config, metrics, prefix, ns, table, req).await
        }
        Err(e) => Err(store_error_to_iceberg_table(e)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_existing_table(
    store: Arc<dyn CatalogStore>,
    config: IcebergConfig,
    metrics: Option<MetricsState>,
    prefix: String,
    ns: String,
    table: String,
    tabular: quasar_core::TabularAsset,
    req: CommitTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
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

    // 4. Extract old properties for delta computation
    let old_properties = current_metadata_json
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    // 5. Apply requirements and updates via metadata wrapper
    let new_metadata_json =
        super::metadata::apply_commit(&current_metadata_json, &req.requirements, &req.updates)
            .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;

    // 6. Compute property changes
    let new_properties = new_metadata_json
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default();

    let property_removals: Vec<String> = old_properties
        .keys()
        .filter(|k| !new_properties.contains_key(*k))
        .cloned()
        .collect();
    let property_updates: HashMap<String, String> = new_properties
        .iter()
        .filter(|(k, v)| old_properties.get(*k) != Some(v))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // 7. Generate new metadata location
    let new_metadata_location = super::metadata::next_metadata_location(&metadata_location)
        .map_err(|msg| IcebergError::InternalServerError { message: msg })?;

    // 8. Write new metadata.json to object store
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, &config).await?;

    // 9. CAS update via storage layer (atomically updates metadata_location, schema_snapshot, and properties)
    store
        .cas_update_metadata_location(
            &prefix,
            &ns,
            &table,
            "iceberg",
            &metadata_location,
            &new_metadata_location,
            Some(new_metadata_json.clone()),
            &property_removals,
            &property_updates,
        )
        .await
        .map_err(|e| match e {
            StoreError::Conflict { msg } => {
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
            metadata: new_metadata_json,
        }),
    ))
}

/// Finalize a staged-create commit (V4_DESIGN.md §6.4).
async fn commit_staged_table(
    store: Arc<dyn CatalogStore>,
    config: IcebergConfig,
    metrics: Option<MetricsState>,
    prefix: String,
    ns: String,
    table: String,
    req: CommitTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    // 1. Look up the staged record. None → truly missing.
    let staged_metadata = store
        .get_staged_table(&prefix, &ns, &table)
        .await
        .map_err(store_error_to_iceberg_table)?
        .ok_or_else(|| IcebergError::NoSuchTableException {
            message: format!("Table '{}.{}' not found", ns, table),
        })?;

    // 2. Require assert-create. Iceberg crate's check(None) only accepts
    // NotExist, but we want a cleaner client-facing error if it's missing.
    if !req
        .requirements
        .iter()
        .any(|r| matches!(r, iceberg::TableRequirement::NotExist))
    {
        if let Some(ref m) = metrics {
            m.registry.record_iceberg_commit_conflict();
        }
        return Err(IcebergError::CommitFailedException {
            message: "staged commit requires the assert-create requirement".to_string(),
        });
    }

    // 3. Check requirements against `None` (NotExist passes, others fail), then
    // apply updates starting from the staged metadata.
    let new_metadata_json =
        super::metadata::apply_commit_for_staged(&staged_metadata, &req.requirements, &req.updates)
            .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;

    // 4. Build the new metadata location. The staged record holds 00001-*;
    // the active commit writes a new 00002-* file with the same UUID/location.
    let location = new_metadata_json
        .get("location")
        .and_then(|v| v.as_str())
        .ok_or_else(|| IcebergError::InternalServerError {
            message: "staged metadata missing 'location' field".to_string(),
        })?
        .to_string();
    let table_uuid = new_metadata_json
        .get("table-uuid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| IcebergError::InternalServerError {
            message: "staged metadata missing 'table-uuid' field".to_string(),
        })?;
    let new_metadata_location = format!("{}/metadata/00002-{}.metadata.json", location, table_uuid);

    // 5. Write the new metadata to object store.
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, &config).await?;

    // 6. Extract properties for the catalog row.
    let properties: HashMap<String, String> = new_metadata_json
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // 7. Single-transaction: verify active absent, lock staged, insert
    // assets+tabular_assets, delete staged record.
    let (_asset, _tabular) = store
        .commit_staged_table(
            &prefix,
            &ns,
            &table,
            &location,
            &new_metadata_location,
            new_metadata_json.clone(),
            properties,
        )
        .await
        .map_err(|e| match e {
            StoreError::AlreadyExists(msg) => {
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

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_metadata_json,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_fallback_metadata_empty_schema() {
        let uuid = Uuid::new_v4();
        let meta = _build_fallback_metadata(uuid, "test", "s3://bucket/test", None);
        assert_eq!(meta["format-version"], 2);
        assert_eq!(meta["table-uuid"], uuid.to_string());
        assert_eq!(meta["location"], "s3://bucket/test");
    }

    #[test]
    fn test_build_fallback_metadata_with_schema() {
        let uuid = Uuid::new_v4();
        let schema = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "type": "long", "required": true}]
        });
        let meta = _build_fallback_metadata(uuid, "test", "s3://bucket/test", Some(&schema));
        assert_eq!(meta["last-column-id"], 1);
    }
}
