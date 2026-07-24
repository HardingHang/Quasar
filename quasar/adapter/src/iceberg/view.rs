use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{validate_name, validate_namespace_path, CatalogError, IcebergCatalogStore};
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitViewRequest, CreateViewRequest, GetViewResponse, ListViewsQuery, ListViewsResponse,
    RenameViewRequest,
};
use super::error::{catalog_error_to_iceberg_view, IcebergError};
use super::{validate_warehouse, CommitMetrics, IcebergConfig};
use crate::object_store_util::{object_exists, read_json, s3_url_to_path, write_json};

/// Write metadata JSON to object store if configured.
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

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/views (dispatched)
pub async fn list_views(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    query: ListViewsQuery,
) -> Result<(StatusCode, Json<ListViewsResponse>), IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), config)?;

    let limit = query.page_size.unwrap_or(100).clamp(1, 1000) as u64;
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(0);

    let views = store
        .list_views(prefix, ns, offset, limit)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    let next_page_token = if views.len() as u64 >= limit {
        Some((offset + views.len() as u64).to_string())
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListViewsResponse {
            identifiers: views,
            next_page_token,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/views (dispatched)
pub async fn create_view(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
    req: CreateViewRequest,
) -> Result<(StatusCode, Json<GetViewResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;
    validate_namespace_path(ns).map_err(catalog_error_to_iceberg_view)?;
    validate_name(&req.name).map_err(catalog_error_to_iceberg_view)?;

    // Check for same-name asset conflict (e.g. an existing table).
    match store.get_asset_by_name(prefix, ns, &req.name).await {
        Ok(_) => {
            return Err(IcebergError::AlreadyExistsException {
                message: format!("Asset with same name already exists: {}.{}", ns, req.name),
            });
        }
        Err(CatalogError::NotFound(_)) => {}
        Err(e) => return Err(catalog_error_to_iceberg_view(e)),
    }

    let location = req.location.unwrap_or_else(|| {
        if let Some(ref wp) = config.warehouse_path {
            format!("{}/{}/{}", wp.trim_end_matches('/'), ns, req.name)
        } else {
            format!("iceberg://{}/{}", ns, req.name)
        }
    });

    let view_uuid = Uuid::new_v4();

    // Build representations from request
    let representations = if let Some(reps) = req.representations {
        reps.into_iter()
            .map(|r| {
                serde_json::from_value::<iceberg::spec::ViewRepresentation>(r)
                    .map_err(|e| format!("invalid representation: {e}"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|msg| IcebergError::BadRequestException { message: msg })?
    } else if let Some(sql) = req.sql {
        vec![iceberg::spec::ViewRepresentation::Sql(
            iceberg::spec::SqlViewRepresentation {
                sql,
                dialect: "spark".to_string(),
            },
        )]
    } else {
        return Err(IcebergError::BadRequestException {
            message: "either 'sql' or 'representations' must be provided".to_string(),
        });
    };

    let metadata = super::view_metadata::build_initial_view_metadata(
        view_uuid,
        &location,
        &req.schema,
        representations,
        req.default_namespace.unwrap_or_default(),
        req.default_catalog,
        req.properties,
    )
    .map_err(|msg| IcebergError::InternalServerError { message: msg })?;

    let metadata_location = format!("{}/metadata/00001-{}.metadata.json", location, view_uuid);

    // Write initial metadata.json to object store
    write_metadata_to_store(&metadata_location, &metadata, config).await?;

    // When no object store is configured, store metadata in properties as fallback
    let properties = if config.object_store.is_none() {
        metadata.clone()
    } else {
        serde_json::json!({})
    };

    let _view = store
        .create_view(
            prefix,
            ns,
            &req.name,
            view_uuid,
            &location,
            &metadata_location,
            properties,
        )
        .await
        .map_err(|e| match e {
            CatalogError::AlreadyExists(msg) => {
                IcebergError::ViewAlreadyExistsException { message: msg }
            }
            other => catalog_error_to_iceberg_view(other),
        })?;

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location: Some(metadata_location),
            metadata,
        }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/views/{view} (dispatched)
pub async fn load_view(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    view: &str,
    warehouse: Option<&str>,
) -> Result<(StatusCode, Json<GetViewResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;

    let view_record = store
        .get_view(prefix, ns, view)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    // Use properties as fallback when object store is not configured
    let fallback = if config.object_store.is_none() {
        view_record.asset.properties.clone()
    } else {
        None
    };

    let metadata_location = match view_record.view.metadata_location {
        Some(ref ml) => {
            // Check metadata.json exists before loading
            if !check_metadata_exists(ml, config).await {
                return Err(IcebergError::MetadataNotFoundException {
                    message: format!("metadata.json not found at {}", ml),
                });
            }
            Some(ml.clone())
        }
        None => None,
    };

    let metadata = match metadata_location {
        Some(ref ml) => read_metadata_from_store(ml, fallback, config).await?,
        None => fallback.ok_or_else(|| IcebergError::InternalServerError {
            message: "no metadata available".to_string(),
        })?,
    };

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location,
            metadata,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/views/{view} (dispatched)
/// Replace view (CAS commit).
#[allow(clippy::too_many_arguments)]
pub async fn replace_view(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    metrics: CommitMetrics,
    prefix: &str,
    ns: &str,
    view: &str,
    warehouse: Option<&str>,
    req: CommitViewRequest,
) -> Result<(StatusCode, Json<GetViewResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;

    let view_record = store
        .get_view(prefix, ns, view)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    let metadata_location =
        view_record
            .view
            .metadata_location
            .ok_or_else(|| IcebergError::CommitFailedException {
                message: format!("View '{}.{}' has no metadata location", ns, view),
            })?;

    // Check metadata.json exists before committing
    if !check_metadata_exists(&metadata_location, config).await {
        return Err(IcebergError::CommitFailedException {
            message: format!(
                "Cannot commit: metadata.json not found at {}",
                metadata_location
            ),
        });
    }

    // Load current metadata from object store
    let current_metadata_json = read_metadata_from_store(&metadata_location, None, config).await?;

    // Apply requirements and updates
    let new_metadata_json = super::view_metadata::apply_view_commit(
        &current_metadata_json,
        &req.requirements,
        &req.updates,
    )
    .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;

    // Generate new metadata location
    let new_metadata_location =
        super::view_metadata::next_view_metadata_location(&metadata_location)
            .map_err(|msg| IcebergError::InternalServerError { message: msg })?;

    // Write new metadata.json to object store
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, config).await?;

    // CAS commit: the store locks the asset row, compares the pointer,
    // inserts the mirrored version and updates current_version_key + the
    // view pointer cache (same S1–S6 discipline as tables, DESIGN §6.4).
    let new_version_key = super::metadata::version_key_from_location(&new_metadata_location);
    store
        .commit_view(
            prefix,
            ns,
            view,
            &metadata_location,
            &new_metadata_location,
            &new_version_key,
        )
        .await
        .map_err(|e| match e {
            CatalogError::Conflict(msg) => {
                metrics.record_conflict();
                IcebergError::CommitFailedException { message: msg }
            }
            CatalogError::NotFound(msg) => IcebergError::NoSuchViewException { message: msg },
            other => catalog_error_to_iceberg_view(other),
        })?;
    metrics.record_success();

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_metadata_json,
        }),
    ))
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}/views/{view} (dispatched)
pub async fn drop_view(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    view: &str,
    warehouse: Option<&str>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    store
        .drop_view(prefix, ns, view)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns}/views/{view} (dispatched)
pub async fn view_exists(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    view: &str,
    warehouse: Option<&str>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    let exists = store
        .view_exists(prefix, ns, view)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    if exists {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(IcebergError::NoSuchViewException {
            message: format!("View '{}.{}' not found", ns, view),
        })
    }
}

/// POST /iceberg/v1/{prefix}/views/rename
pub async fn rename_view(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<super::dto::WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<RenameViewRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    validate_name(&req.source.name).map_err(catalog_error_to_iceberg_view)?;
    validate_name(&req.destination.name).map_err(catalog_error_to_iceberg_view)?;

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

    // Verify destination namespace exists
    store
        .get_namespace(&prefix, &dst_ns)
        .await
        .map_err(|e| match e {
            CatalogError::NotFound(_) => IcebergError::NoSuchNamespaceException {
                message: format!("Namespace '{}' not found", dst_ns),
            },
            other => catalog_error_to_iceberg_view(other),
        })?;

    // Verify source view exists
    store
        .get_view(&prefix, &src_ns, &req.source.name)
        .await
        .map_err(catalog_error_to_iceberg_view)?;

    store
        .rename_view(
            &prefix,
            &src_ns,
            &req.source.name,
            &prefix,
            &dst_ns,
            &req.destination.name,
        )
        .await
        .map_err(|e| match e {
            CatalogError::AlreadyExists(msg) => {
                IcebergError::ViewAlreadyExistsException { message: msg }
            }
            other => catalog_error_to_iceberg_view(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
}
