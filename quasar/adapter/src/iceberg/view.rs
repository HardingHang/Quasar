use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{CatalogStore, MetricsState, StoreError};
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    CommitViewRequest, CreateViewRequest, GetViewResponse, ListViewsQuery, ListViewsResponse,
    RenameViewRequest, WarehouseQuery,
};
use super::error::{store_error_to_iceberg_view, IcebergError};
use super::{validate_warehouse, IcebergConfig};
use crate::object_store_util::{object_exists, read_json, s3_url_to_path, write_json};
use quasar_core::validate_name;

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

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/views
pub async fn list_views(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<ListViewsQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    let limit = query.page_size.unwrap_or(100).clamp(1, 1000) as i64;
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let views = store
        .list_views(&prefix, &ns, offset, limit)
        .await
        .map_err(store_error_to_iceberg_view)?;

    let next_page_token = if views.len() as i64 >= limit {
        Some((offset + views.len() as i64).to_string())
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

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/views
pub async fn create_view(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Json(req): Json<CreateViewRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    validate_name(&ns).map_err(store_error_to_iceberg_view)?;
    validate_name(&req.name).map_err(store_error_to_iceberg_view)?;

    // Check for same-name table conflict
    let table_exists = store
        .asset_exists(&prefix, &ns, &req.name)
        .await
        .map_err(store_error_to_iceberg_view)?;
    if table_exists {
        return Err(IcebergError::AlreadyExistsException {
            message: format!("Table with same name already exists: {}.{}", ns, req.name),
        });
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
    write_metadata_to_store(&metadata_location, &metadata, &config).await?;

    // When no object store is configured, store metadata in properties as fallback
    let properties = if config.object_store.is_none() {
        metadata.clone()
    } else {
        serde_json::json!({})
    };

    let _view = store
        .create_view(
            &prefix,
            &ns,
            &req.name,
            view_uuid,
            &location,
            &metadata_location,
            1,
            properties,
        )
        .await
        .map_err(|e| match e {
            StoreError::AlreadyExists(msg) => {
                IcebergError::ViewAlreadyExistsException { message: msg }
            }
            other => store_error_to_iceberg_view(other),
        })?;

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location: Some(metadata_location),
            metadata,
        }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/views/{view}
pub async fn load_view(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns, view)): Path<(String, String, String)>,
    Query(query): Query<WarehouseQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let view_record = store
        .get_view(&prefix, &ns, &view)
        .await
        .map_err(store_error_to_iceberg_view)?;

    // Check metadata.json exists before loading
    if !check_metadata_exists(&view_record.view.metadata_location, &config).await {
        return Err(IcebergError::MetadataNotFoundException {
            message: format!(
                "metadata.json not found at {}",
                view_record.view.metadata_location
            ),
        });
    }

    // Use properties as fallback when object store is not configured
    let fallback = if config.object_store.is_none() {
        Some(view_record.view.properties.clone())
    } else {
        None
    };

    let metadata =
        read_metadata_from_store(&view_record.view.metadata_location, fallback, &config).await?;

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location: Some(view_record.view.metadata_location),
            metadata,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/views/{view}
/// Replace view (CAS commit).
pub async fn replace_view(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    metrics: Option<Extension<MetricsState>>,
    Path((prefix, ns, view)): Path<(String, String, String)>,
    Query(query): Query<WarehouseQuery>,
    Json(req): Json<CommitViewRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let metrics = metrics.map(|e| e.0);

    let view_record = store
        .get_view(&prefix, &ns, &view)
        .await
        .map_err(store_error_to_iceberg_view)?;

    let metadata_location = view_record.view.metadata_location;

    // Check metadata.json exists before committing
    if !check_metadata_exists(&metadata_location, &config).await {
        return Err(IcebergError::CommitFailedException {
            message: format!(
                "Cannot commit: metadata.json not found at {}",
                metadata_location
            ),
        });
    }

    // Load current metadata from object store
    let current_metadata_json = read_metadata_from_store(&metadata_location, None, &config).await?;

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
    write_metadata_to_store(&new_metadata_location, &new_metadata_json, &config).await?;

    // CAS update via storage layer
    store
        .commit_view(
            &prefix,
            &ns,
            &view,
            &metadata_location,
            &new_metadata_location,
        )
        .await
        .map_err(|e| match e {
            StoreError::Conflict { msg } => {
                if let Some(ref m) = metrics {
                    m.registry.record_iceberg_commit_conflict();
                }
                IcebergError::CommitFailedException { message: msg }
            }
            StoreError::NotFound(msg) => IcebergError::NoSuchViewException { message: msg },
            other => store_error_to_iceberg_view(other),
        })?;

    if let Some(ref m) = metrics {
        m.registry.record_iceberg_commit_success();
    }

    Ok((
        StatusCode::OK,
        Json(GetViewResponse {
            metadata_location: Some(new_metadata_location),
            metadata: new_metadata_json,
        }),
    ))
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}/views/{view}
pub async fn drop_view(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns, view)): Path<(String, String, String)>,
    Query(query): Query<WarehouseQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    store
        .drop_view(&prefix, &ns, &view)
        .await
        .map_err(store_error_to_iceberg_view)?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns}/views/{view}
pub async fn view_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns, view)): Path<(String, String, String)>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let exists = match store.view_exists(&prefix, &ns, &view).await {
        Ok(e) => e,
        Err(StoreError::NotFound(_)) => false,
        Err(e) => return Err(store_error_to_iceberg_view(e)),
    };

    if exists {
        Ok(StatusCode::OK)
    } else {
        Err(IcebergError::NoSuchViewException {
            message: format!("View '{}.{}' not found", ns, view),
        })
    }
}

/// POST /iceberg/v1/{prefix}/views/rename
pub async fn rename_view(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<RenameViewRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    validate_name(&req.source.name).map_err(store_error_to_iceberg_view)?;
    validate_name(&req.destination.name).map_err(store_error_to_iceberg_view)?;

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

    // Verify destination namespace exists
    let dest_exists = store
        .namespace_exists(&prefix, dst_ns)
        .await
        .map_err(store_error_to_iceberg_view)?;
    if !dest_exists {
        return Err(IcebergError::NoSuchNamespaceException {
            message: format!("Namespace '{}' not found", dst_ns),
        });
    }

    // Verify source view exists
    store
        .get_view(&prefix, src_ns, &req.source.name)
        .await
        .map_err(store_error_to_iceberg_view)?;

    store
        .rename_view(
            &prefix,
            src_ns,
            &req.source.name,
            &prefix,
            dst_ns,
            &req.destination.name,
        )
        .await
        .map_err(|e| match e {
            StoreError::AlreadyExists(msg) => {
                IcebergError::ViewAlreadyExistsException { message: msg }
            }
            other => store_error_to_iceberg_view(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
}
