use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{
    validate_name, validate_namespace_path, AssetFilter, AssetPatch, CatalogError, CreateVersion,
    IcebergCatalogStore, IcebergTableCommit, PatchField, TabularAsset,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitTableRequest, CreateTableRequest, ListTablesQuery, ListTablesResponse, LoadTableResponse,
    RegisterTableRequest, RenameTableRequest, TableIdentifier, WarehouseQuery,
};
use super::error::{catalog_error_to_iceberg_table, IcebergError};
use super::{string_map_to_properties, validate_warehouse, CommitMetrics, IcebergConfig};
use crate::object_store_util::{
    delete_prefix, object_exists, read_json, s3_url_to_path, write_json,
};

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

/// Protocol isolation (DESIGN §4.2): an asset is only visible through the
/// Iceberg API when its format is `iceberg`; anything else reads as
/// "table not found".
fn ensure_iceberg_format(
    asset: &quasar_core::Asset,
    ns: &str,
    name: &str,
) -> Result<(), IcebergError> {
    if asset.format.as_deref() == Some("iceberg") {
        Ok(())
    } else {
        Err(IcebergError::NoSuchTableException {
            message: format!("Table '{}.{}' not found", ns, name),
        })
    }
}

/// Extract the string-valued `properties` object from a metadata document.
fn metadata_properties(metadata: &serde_json::Value) -> HashMap<String, String> {
    metadata
        .get("properties")
        .and_then(|p| p.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

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

/// Validate that a table location is safe to purge.
/// Returns the bucket-relative path on success.
fn validate_purge_location(
    table_location: &str,
    metadata_location: Option<&str>,
    config: &IcebergConfig,
) -> Result<object_store::path::Path, IcebergError> {
    let bucket = config
        .s3_bucket
        .as_ref()
        .ok_or_else(|| IcebergError::InternalServerError {
            message: "object store bucket not configured".to_string(),
        })?;

    // 1. table_location must convert to a bucket-relative path
    let table_path = s3_url_to_path(table_location, bucket).ok_or_else(|| {
        IcebergError::BadRequestException {
            message: format!(
                "table location '{}' is not in configured bucket '{}'",
                table_location, bucket
            ),
        }
    })?;

    let path_str = table_path.as_ref();

    // 2. path must not be bucket root or empty
    if path_str.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: "cannot purge bucket root".to_string(),
        });
    }

    // 3. path must be under the configured warehouse prefix
    if let Some(ref wp) = config.warehouse_path {
        let warehouse_prefix = s3_url_to_path(wp, bucket);
        if let Some(prefix) = warehouse_prefix {
            let prefix_str = prefix.as_ref().trim_end_matches('/');
            if !prefix_str.is_empty() && !path_str.starts_with(prefix_str) {
                return Err(IcebergError::BadRequestException {
                    message: "table location is outside warehouse prefix".to_string(),
                });
            }
        }
    }

    // 4. metadata_location must be in the same bucket
    if let Some(ml) = metadata_location {
        if s3_url_to_path(ml, bucket).is_none() {
            return Err(IcebergError::BadRequestException {
                message: "metadata location is not in configured bucket".to_string(),
            });
        }
    }

    Ok(table_path)
}

// ── Handlers ───────────────────────────────────────────────

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/tables (dispatched)
pub async fn list_tables(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    query: ListTablesQuery,
) -> Result<(StatusCode, Json<ListTablesResponse>), IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), config)?;

    let limit = query.page_size.unwrap_or(100).clamp(1, 1000) as u64;
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(0);

    // Endpoint-level protocol isolation: only `iceberg`-format tables are
    // visible through the Iceberg API.
    let filter = AssetFilter {
        domain: Some(prefix.to_string()),
        namespace: Some(ns.to_string()),
        asset_type: Some("table".to_string()),
        format: Some("iceberg".to_string()),
        ..AssetFilter::default()
    };
    let assets = store
        .list_assets(filter, offset, limit)
        .await
        .map_err(catalog_error_to_iceberg_table)?;

    let next_page_token = if assets.len() as u64 >= limit {
        Some((offset + assets.len() as u64).to_string())
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListTablesResponse {
            identifiers: assets
                .into_iter()
                .map(|asset| TableIdentifier {
                    namespace: ns.split('/').map(str::to_string).collect(),
                    name: asset.name,
                })
                .collect(),
            next_page_token,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables (dispatched)
pub async fn create_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
    req: CreateTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;
    validate_namespace_path(ns).map_err(catalog_error_to_iceberg_table)?;
    validate_name(&req.name).map_err(catalog_error_to_iceberg_table)?;

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
    write_metadata_to_store(&metadata_location, &metadata, config).await?;

    let properties_value = string_map_to_properties(properties).unwrap_or_else(|| json!({}));

    if req.stage_create == Some(true) {
        // Staged create: only insert staged record, no active catalog entry
        store
            .create_staged_table(
                prefix,
                ns,
                &req.name,
                table_uuid,
                &location,
                &metadata_location,
                metadata.clone(),
                properties_value,
            )
            .await
            .map_err(catalog_error_to_iceberg_table)?;
    } else {
        // Non-staged create: register the catalog entry. The store creates
        // the asset + tabular extension + first mirrored version in one
        // transaction (DESIGN §6.1).
        let _ = store
            .register_iceberg_table(
                prefix,
                ns,
                &req.name,
                &location,
                &metadata_location,
                metadata.clone(),
                properties_value,
            )
            .await
            .map_err(catalog_error_to_iceberg_table)?;
    }

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(metadata_location),
            metadata,
        }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table} (dispatched)
pub async fn load_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    table: &str,
    warehouse: Option<&str>,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;

    let asset = store
        .get_asset_by_name(prefix, ns, table)
        .await
        .map_err(catalog_error_to_iceberg_table)?;
    ensure_iceberg_format(&asset, ns, table)?;
    let tabular = store
        .get_tabular_asset(asset.id)
        .await
        .map_err(catalog_error_to_iceberg_table)?;

    // Check metadata.json exists before loading
    if let Some(ref ml) = tabular.metadata_location {
        if !check_metadata_exists(ml, config).await {
            return Err(IcebergError::MetadataNotFoundException {
                message: format!("metadata.json not found at {}", ml),
            });
        }
    }

    let metadata = if let Some(ref ml) = tabular.metadata_location {
        read_metadata_from_store(ml, tabular.schema_snapshot.clone(), config).await?
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

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table} (dispatched)
pub async fn drop_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    table: &str,
    warehouse: Option<&str>,
    purge_requested: Option<bool>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    let asset = store
        .get_asset_by_name(prefix, ns, table)
        .await
        .map_err(catalog_error_to_iceberg_table)?;
    ensure_iceberg_format(&asset, ns, table)?;

    if purge_requested == Some(true) {
        // 1. Read the tabular extension for the location.
        let tabular = store
            .get_tabular_asset(asset.id)
            .await
            .map_err(catalog_error_to_iceberg_table)?;

        // 2. Safety boundary check before any destructive operation.
        let table_path = validate_purge_location(
            &tabular.location,
            tabular.metadata_location.as_deref(),
            config,
        )?;

        // 3. Transaction: create purge operation + drop catalog records.
        let (operation_id, table_location, _metadata_location) = store
            .begin_iceberg_purge_and_drop_catalog(prefix, ns, table)
            .await
            .map_err(|e| match e {
                CatalogError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
                CatalogError::Internal(msg) => {
                    tracing::error!(error = %msg, "begin purge failed");
                    IcebergError::InternalServerError {
                        message: "An internal error occurred".to_string(),
                    }
                }
                other => catalog_error_to_iceberg_table(other),
            })?;

        // 4. Delete objects from object store.
        if let Some(ref obj_store) = config.object_store {
            match delete_prefix(&**obj_store, &table_path).await {
                Ok(()) => {
                    store
                        .update_purge_operation(operation_id, "completed", None)
                        .await
                        .map_err(|e| {
                            tracing::error!(error = %e, "failed to mark purge completed");
                            IcebergError::InternalServerError {
                                message: "An internal error occurred".to_string(),
                            }
                        })?;
                }
                Err(e) => {
                    let sanitized = "object store cleanup failed".to_string();
                    tracing::error!(
                        error = %e,
                        table_location = %table_location,
                        "object store purge failed"
                    );
                    store
                        .update_purge_operation(operation_id, "failed", Some(&sanitized))
                        .await
                        .map_err(|inner| {
                            tracing::error!(
                                error = %inner,
                                "failed to mark purge failed"
                            );
                            IcebergError::InternalServerError {
                                message: "An internal error occurred".to_string(),
                            }
                        })?;
                    return Err(IcebergError::InternalServerError {
                        message: "object store cleanup failed".to_string(),
                    });
                }
            }
        }

        Ok(StatusCode::NO_CONTENT)
    } else {
        // purgeRequested=false or absent: soft-delete the catalog record;
        // restore is a governance operation through the Unified API
        // (docs/DESIGN.md §6.3).
        store
            .soft_delete_asset(asset.id)
            .await
            .map_err(catalog_error_to_iceberg_table)?;

        Ok(StatusCode::NO_CONTENT)
    }
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/metrics (dispatched)
#[allow(clippy::too_many_arguments)]
pub async fn report_metrics(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    table: &str,
    warehouse: Option<&str>,
    user_agent: Option<&str>,
    report: serde_json::Value,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    let asset = store
        .get_asset_by_name(prefix, ns, table)
        .await
        .map_err(catalog_error_to_iceberg_table)?;
    ensure_iceberg_format(&asset, ns, table)?;

    store
        .record_scan_metrics_report(Some(asset.id), prefix, ns, table, report, user_agent)
        .await
        .map_err(|e| match e {
            CatalogError::Internal(msg) => {
                tracing::error!(error = %msg, "failed to record scan metrics");
                IcebergError::InternalServerError {
                    message: "An internal error occurred".to_string(),
                }
            }
            other => catalog_error_to_iceberg_table(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table} (dispatched)
pub async fn table_exists(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    table: &str,
    warehouse: Option<&str>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    match store.get_asset_by_name(prefix, ns, table).await {
        Ok(asset) if asset.format.as_deref() == Some("iceberg") => Ok(StatusCode::NO_CONTENT),
        Ok(_) | Err(CatalogError::NotFound(_)) => Err(IcebergError::NoSuchTableException {
            message: format!("Table '{}.{}' not found", ns, table),
        }),
        Err(e) => Err(catalog_error_to_iceberg_table(e)),
    }
}

/// POST /iceberg/v1/{prefix}/tables/rename
pub async fn rename_table(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<RenameTableRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    validate_name(&req.source.name).map_err(catalog_error_to_iceberg_table)?;
    validate_name(&req.destination.name).map_err(catalog_error_to_iceberg_table)?;

    if req.source.namespace.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: "source namespace must not be empty".to_string(),
        });
    }
    if req.destination.namespace.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: "destination namespace must not be empty".to_string(),
        });
    }

    let src_ns = req.source.namespace.join("/");
    let dst_ns = req.destination.namespace.join("/");

    // Protocol isolation: the Iceberg rename cannot operate on a
    // non-Iceberg source asset.
    let asset = store
        .get_asset_by_name(&prefix, &src_ns, &req.source.name)
        .await
        .map_err(catalog_error_to_iceberg_table)?;
    ensure_iceberg_format(&asset, &src_ns, &req.source.name)?;

    // Cross-namespace move (Iceberg REST spec): source and destination
    // share the URL `{prefix}`, so the move always stays inside the same
    // Domain; the destination namespace must exist.
    let dest_namespace = if src_ns == dst_ns {
        None
    } else {
        store
            .get_namespace(&prefix, &dst_ns)
            .await
            .map_err(|e| match e {
                CatalogError::NotFound(_) => IcebergError::NoSuchNamespaceException {
                    message: format!("Namespace '{}' not found", dst_ns),
                },
                other => catalog_error_to_iceberg_table(other),
            })?;
        Some(dst_ns.as_str())
    };

    store
        .rename_asset(asset.id, &req.destination.name, dest_namespace)
        .await
        .map_err(catalog_error_to_iceberg_table)?;

    Ok(StatusCode::NO_CONTENT)
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/register (dispatched)
/// Register an externally-managed Iceberg table into the catalog.
pub async fn register_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
    req: RegisterTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;
    validate_namespace_path(ns).map_err(catalog_error_to_iceberg_table)?;
    validate_name(&req.name).map_err(catalog_error_to_iceberg_table)?;

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

    // 3. Register catalog record (no object store write). The store
    // creates the asset + tabular extension + first mirrored version.
    store
        .register_iceberg_table(
            prefix,
            ns,
            &req.name,
            &location,
            &req.metadata_location,
            metadata_json.clone(),
            json!({}),
        )
        .await
        .map_err(catalog_error_to_iceberg_table)?;

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(req.metadata_location),
            metadata: metadata_json,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table} (dispatched)
/// Commit table updates (CAS).
///
/// Two paths:
/// - existing active table → CAS commit via `compare_and_swap_pointer`;
///   the store locks the asset row, compares the pointer, inserts the
///   mirrored version and updates the caches (docs/DESIGN.md
///   §6.4, S1–S6).
/// - active table absent but a non-expired staged record exists → finalize
///   the staged-create commit via `commit_staged_table`. The request must
///   carry `assert-create` (`TableRequirement::NotExist`).
#[allow(clippy::too_many_arguments)]
pub async fn commit_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    metrics: CommitMetrics,
    prefix: &str,
    ns: &str,
    table: &str,
    warehouse: Option<&str>,
    req: CommitTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;

    // 0. Reject unsupported official TableUpdate variants before IO.
    super::metadata::check_supported_updates(&req.updates).map_err(|u| {
        IcebergError::NotImplementedException {
            message: format!("update '{}' is not supported", u.0),
        }
    })?;

    // 1. Look up the active table; on NotFound, fall through to staged commit.
    match store.get_asset_by_name(prefix, ns, table).await {
        Ok(asset) => {
            ensure_iceberg_format(&asset, ns, table)?;
            let tabular = store
                .get_tabular_asset(asset.id)
                .await
                .map_err(catalog_error_to_iceberg_table)?;
            commit_existing_table(store, config, metrics, ns, table, asset, tabular, req).await
        }
        Err(CatalogError::NotFound(_)) => {
            commit_staged_table(store, config, metrics, prefix, ns, table, req).await
        }
        Err(e) => Err(catalog_error_to_iceberg_table(e)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_existing_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    metrics: CommitMetrics,
    ns: &str,
    table: &str,
    asset: quasar_core::Asset,
    tabular: TabularAsset,
    req: CommitTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    let metadata_location =
        tabular
            .metadata_location
            .ok_or_else(|| IcebergError::CommitFailedException {
                message: format!("Table '{}.{}' has no metadata location", ns, table),
            })?;

    // 2. Check metadata.json exists before committing (CAS requires current metadata)
    if !check_metadata_exists(&metadata_location, config).await {
        return Err(IcebergError::CommitFailedException {
            message: format!(
                "Cannot commit: metadata.json not found at {}",
                metadata_location
            ),
        });
    }

    // 3. Load current metadata from object store (or fallback to schema_snapshot)
    let current_metadata_json =
        read_metadata_from_store(&metadata_location, tabular.schema_snapshot.clone(), config)
            .await?;

    // 4. Validate requirements and apply updates via the metadata wrapper
    //    (outside the DB lock, against the immutable L1 document; DESIGN
    //    §6.4 "锁外校验 + 锁内 CAS").
    let new_metadata_json =
        super::metadata::apply_commit(&current_metadata_json, &req.requirements, &req.updates)
            .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;

    // 5. Generate new metadata location and write it to the object store.
    let new_metadata_location = super::metadata::next_metadata_location(&metadata_location)
        .map_err(|msg| IcebergError::InternalServerError { message: msg })?;
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, config).await?;

    // 6. CAS commit: the store locks the asset row, compares the pointer,
    //    inserts the mirrored version (previous version auto-linked) and
    //    updates current_version_key + the tabular cache (S1–S6).
    let new_version = CreateVersion {
        asset_id: asset.id,
        version_key: super::metadata::version_key_from_location(&new_metadata_location),
        version_properties: None,
        content_inline: None,
        content_pointer: Some(new_metadata_location.clone()),
        previous_version_id: None,
    };
    store
        .compare_and_swap_pointer(asset.id, &metadata_location, new_version)
        .await
        .map_err(|e| match e {
            CatalogError::Conflict(msg) => {
                metrics.record_conflict();
                IcebergError::CommitFailedException { message: msg }
            }
            CatalogError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
            other => catalog_error_to_iceberg_table(other),
        })?;
    metrics.record_success();

    // 7. Best-effort mirror of the protocol-truth properties into the
    //    catalog row; intentionally non-atomic (DESIGN §6.1).
    let patch = AssetPatch {
        comment: PatchField::NoChange,
        properties: match string_map_to_properties(metadata_properties(&new_metadata_json)) {
            Some(value) => PatchField::Set(value),
            None => PatchField::Unset,
        },
    };
    if let Err(e) = store.update_asset(asset.id, patch).await {
        tracing::warn!(error = %e, asset_id = %asset.id, "failed to mirror table properties");
    }

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_metadata_json,
        }),
    ))
}

/// Finalize a staged-create commit (docs/DESIGN.md §6.4).
async fn commit_staged_table(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    metrics: CommitMetrics,
    prefix: &str,
    ns: &str,
    table: &str,
    req: CommitTableRequest,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergError> {
    // 1. Look up the staged record. None → truly missing.
    let staged_metadata = store
        .get_staged_table(prefix, ns, table)
        .await
        .map_err(catalog_error_to_iceberg_table)?
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
        metrics.record_conflict();
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
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, config).await?;

    // 6. Extract properties for the catalog row.
    let properties_value = string_map_to_properties(metadata_properties(&new_metadata_json))
        .unwrap_or_else(|| json!({}));

    // 7. Single-transaction: verify active absent, lock staged, insert
    // assets + tabular_assets + first mirrored version, delete staged.
    let _ = store
        .commit_staged_table(
            prefix,
            ns,
            table,
            &location,
            &new_metadata_location,
            new_metadata_json.clone(),
            properties_value,
        )
        .await
        .map_err(|e| match e {
            CatalogError::AlreadyExists(msg) => {
                metrics.record_conflict();
                IcebergError::CommitFailedException { message: msg }
            }
            // The staged record is gone — either it expired between the
            // earlier get_staged_table() read and now (TTL race), or another
            // writer just consumed it. Either way the client should treat
            // this as a commit conflict and retry, not as "table missing".
            CatalogError::NotFound(msg) => {
                metrics.record_conflict();
                IcebergError::CommitFailedException { message: msg }
            }
            other => catalog_error_to_iceberg_table(other),
        })?;
    metrics.record_success();

    Ok((
        StatusCode::OK,
        Json(LoadTableResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_metadata_json,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/transactions/commit
/// Multi-table atomic commit.
pub async fn commit_transaction(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path(prefix): Path<String>,
    Query(query): Query<WarehouseQuery>,
    Json(request): Json<super::dto::CommitTransactionRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    // 0. Reject unsupported update actions before any IO.
    for change in &request.table_changes {
        super::metadata::check_supported_updates(&change.updates).map_err(|u| {
            IcebergError::NotImplementedException {
                message: format!("update '{}' is not supported", u.0),
            }
        })?;
    }

    if request.table_changes.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    // Sort by (namespace, table) for consistent lock ordering (deadlock prevention).
    let mut sorted_changes = request.table_changes;
    sorted_changes.sort_by(|a, b| {
        let a_ns = a.identifier.namespace.join("/");
        let b_ns = b.identifier.namespace.join("/");
        a_ns.cmp(&b_ns)
            .then_with(|| a.identifier.name.cmp(&b.identifier.name))
    });

    // Phase 1: Prepare — validate all tables exist, apply updates, write to object store.
    let mut commits: Vec<IcebergTableCommit> = Vec::with_capacity(sorted_changes.len());

    for change in &sorted_changes {
        if change.identifier.namespace.is_empty() {
            return Err(IcebergError::BadRequestException {
                message: "namespace array must not be empty".to_string(),
            });
        }
        let ns = change.identifier.namespace.join("/");

        // Verify table exists and read current metadata.
        let asset = store
            .get_asset_by_name(&prefix, &ns, &change.identifier.name)
            .await
            .map_err(|e| match e {
                CatalogError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
                other => catalog_error_to_iceberg_table(other),
            })?;
        ensure_iceberg_format(&asset, &ns, &change.identifier.name)?;
        let tabular = store
            .get_tabular_asset(asset.id)
            .await
            .map_err(catalog_error_to_iceberg_table)?;

        let metadata_location =
            tabular
                .metadata_location
                .ok_or_else(|| IcebergError::CommitFailedException {
                    message: format!(
                        "Table '{}.{}' has no metadata location",
                        ns, change.identifier.name
                    ),
                })?;

        // Check metadata.json exists.
        if !check_metadata_exists(&metadata_location, &config).await {
            return Err(IcebergError::CommitFailedException {
                message: format!(
                    "Cannot commit: metadata.json not found at {}",
                    metadata_location
                ),
            });
        }

        // Load current metadata.
        let current_metadata_json =
            read_metadata_from_store(&metadata_location, tabular.schema_snapshot, &config).await?;

        // Apply requirements and updates.
        let new_metadata_json = super::metadata::apply_commit(
            &current_metadata_json,
            &change.requirements,
            &change.updates,
        )
        .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;

        // Generate new metadata location.
        let new_metadata_location = super::metadata::next_metadata_location(&metadata_location)
            .map_err(|msg| IcebergError::InternalServerError { message: msg })?;

        // Write to object store. Failure here aborts the entire transaction.
        write_metadata_to_store(&new_metadata_location, &new_metadata_json, &config).await?;

        commits.push(IcebergTableCommit {
            domain: prefix.clone(),
            namespace_path: ns,
            table: change.identifier.name.clone(),
            expected_pointer: metadata_location,
            new_version_key: super::metadata::version_key_from_location(&new_metadata_location),
            new_location: new_metadata_location,
            schema_snapshot: Some(new_metadata_json),
        });
    }

    // Phase 2: DB commit — atomic CAS updates + version mirroring for all
    // tables within a single PostgreSQL transaction (DESIGN §6.4).
    store
        .commit_transaction_tables(commits)
        .await
        .map_err(|e| match e {
            CatalogError::Conflict(msg) => IcebergError::CommitFailedException { message: msg },
            CatalogError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
            other => catalog_error_to_iceberg_table(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
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
