//! AssetType / Format registry handlers (`/unified/v1/asset-types*`,
//! `/unified/v1/formats*`).
//!
//! Both are global registries (REQUIREMENTS §4.5). Core `AssetType` and
//! `Format` models carry no sensitive fields and are serialized directly
//! as responses.

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_name, CatalogError, CatalogStore, RegisterAssetType, RegisterFormat};
use std::sync::Arc;

use super::dto::{
    next_page_token, AssetTypeListQuery, ListResponse, PaginationQuery, RegisterAssetTypeRequest,
    RegisterFormatRequest,
};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::request_id_of;

const ASSET_TYPES_INSTANCE: &str = "/unified/v1/asset-types";
const FORMATS_INSTANCE: &str = "/unified/v1/formats";

/// Known AssetType categories (DESIGN §3.2 `asset_types` CHECK constraint).
const VALID_CATEGORIES: [&str; 9] = [
    "tabular",
    "view",
    "model",
    "agent",
    "tool",
    "mcp_server",
    "fileset",
    "topic",
    "generic",
];

/// Known extension strategies (DESIGN §3.2 `asset_types` CHECK constraint).
const VALID_EXTENSION_STRATEGIES: [&str; 3] = ["jsonb", "dedicated_table", "reference_only"];

/// Reject an unregistered asset type filter with `VALIDATION_FAILED`
/// (FR-Q3); a `None` filter passes through.
pub(crate) async fn ensure_asset_type_registered(
    store: &Arc<dyn CatalogStore>,
    asset_type: Option<&str>,
    instance: &str,
    request_id: &str,
) -> Result<(), UnifiedError> {
    let Some(name) = asset_type else {
        return Ok(());
    };
    store
        .get_asset_type(name)
        .await
        .map(|_| ())
        .map_err(|e| match e {
            CatalogError::NotFound(_) => UnifiedError::new(
                UnifiedErrorCode::ValidationFailed,
                format!("asset type '{}' is not registered", name),
                instance,
                request_id,
            ),
            other => map_catalog_error(other, instance, request_id),
        })
}

/// Reject an unregistered format filter with `VALIDATION_FAILED` (FR-Q4);
/// a `None` filter passes through.
pub(crate) async fn ensure_format_registered(
    store: &Arc<dyn CatalogStore>,
    format: Option<&str>,
    instance: &str,
    request_id: &str,
) -> Result<(), UnifiedError> {
    let Some(name) = format else {
        return Ok(());
    };
    store
        .get_format(name)
        .await
        .map(|_| ())
        .map_err(|e| match e {
            CatalogError::NotFound(_) => UnifiedError::new(
                UnifiedErrorCode::ValidationFailed,
                format!("format '{}' is not registered", name),
                instance,
                request_id,
            ),
            other => map_catalog_error(other, instance, request_id),
        })
}

/// GET /unified/v1/asset-types
pub async fn list_asset_types(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Query(query): Query<AssetTypeListQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let (offset, limit) = query
        .pagination
        .resolve()
        .map_err(|e| map_catalog_error(e, ASSET_TYPES_INSTANCE, &request_id))?;

    let asset_types = store
        .list_asset_types(query.category.as_deref(), offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, ASSET_TYPES_INSTANCE, &request_id))?;

    let token = next_page_token(offset, &asset_types, limit);
    Ok(Json(ListResponse::new(asset_types, token)))
}

/// POST /unified/v1/asset-types
pub async fn register_asset_type(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Json(req): Json<RegisterAssetTypeRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    validate_name(&req.name)
        .map_err(|e| map_catalog_error(e, ASSET_TYPES_INSTANCE, &request_id))?;
    if !VALID_CATEGORIES.contains(&req.category.as_str()) {
        return Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            format!(
                "invalid category '{}', expected one of: {}",
                req.category,
                VALID_CATEGORIES.join(", ")
            ),
            ASSET_TYPES_INSTANCE,
            request_id,
        ));
    }
    if !VALID_EXTENSION_STRATEGIES.contains(&req.extension_strategy.as_str()) {
        return Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            format!(
                "invalid extension_strategy '{}', expected one of: {}",
                req.extension_strategy,
                VALID_EXTENSION_STRATEGIES.join(", ")
            ),
            ASSET_TYPES_INSTANCE,
            request_id,
        ));
    }

    let input = RegisterAssetType {
        name: req.name,
        description: req.description,
        category: req.category,
        validation_schema: req.validation_schema,
        extension_strategy: req.extension_strategy,
        supports_native_protocol: req.supports_native_protocol,
    };

    let asset_type = store
        .register_asset_type(input)
        .await
        .map_err(|e| map_catalog_error(e, ASSET_TYPES_INSTANCE, &request_id))?;

    Ok((StatusCode::CREATED, Json(asset_type)))
}

/// GET /unified/v1/asset-types/{name}
pub async fn get_asset_type(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = format!("{}/{}", ASSET_TYPES_INSTANCE, name);
    validate_name(&name).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let asset_type = store
        .get_asset_type(&name)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(asset_type))
}

/// GET /unified/v1/formats
pub async fn list_formats(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let (offset, limit) = query
        .resolve()
        .map_err(|e| map_catalog_error(e, FORMATS_INSTANCE, &request_id))?;

    let formats = store
        .list_formats(offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, FORMATS_INSTANCE, &request_id))?;

    let token = next_page_token(offset, &formats, limit);
    Ok(Json(ListResponse::new(formats, token)))
}

/// POST /unified/v1/formats
pub async fn register_format(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Json(req): Json<RegisterFormatRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    validate_name(&req.name).map_err(|e| map_catalog_error(e, FORMATS_INSTANCE, &request_id))?;

    let input = RegisterFormat {
        name: req.name,
        description: req.description,
        mime_type: req.mime_type,
        serialization_hint: req.serialization_hint,
    };

    let format = store
        .register_format(input)
        .await
        .map_err(|e| map_catalog_error(e, FORMATS_INSTANCE, &request_id))?;

    Ok((StatusCode::CREATED, Json(format)))
}

/// GET /unified/v1/formats/{name}
pub async fn get_format(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = format!("{}/{}", FORMATS_INSTANCE, name);
    validate_name(&name).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let format = store
        .get_format(&name)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(format))
}
