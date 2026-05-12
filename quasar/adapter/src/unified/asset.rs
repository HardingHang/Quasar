use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_name, AssetFormat, CatalogStore};
use std::str::FromStr;
use std::sync::Arc;

use super::dto::{
    AssetDetailQuery, AssetListItem, AssetListQuery, AssetResponse, CurrentVersionResponse,
    ListAssetsResponse, PageSizeError, PaginationQuery, RenameAssetRequest, UpdateAssetRequest,
};
use super::error::{map_asset_error, UnifiedError, UnifiedErrorCode};
use crate::DEFAULT_DOMAIN;

/// Parse and validate the `format` query parameter.
fn parse_format(
    format: Option<String>,
    instance: &str,
    request_id: &str,
) -> Result<AssetFormat, UnifiedError> {
    match format {
        None => Err(UnifiedError::new(
            UnifiedErrorCode::InvalidInput,
            "format query parameter is required",
            instance,
            request_id,
        )),
        Some(ref s) if s.is_empty() => Err(UnifiedError::new(
            UnifiedErrorCode::InvalidFormat,
            "format query parameter cannot be empty",
            instance,
            request_id,
        )),
        Some(s) => AssetFormat::from_str(&s).map_err(|_| {
            UnifiedError::new(
                UnifiedErrorCode::InvalidFormat,
                format!("invalid format '{}', expected 'iceberg' or 'lance'", s),
                instance,
                request_id,
            )
        }),
    }
}

/// Parse and validate the optional `format` query parameter for list endpoints.
fn parse_optional_format(
    format: Option<String>,
    instance: &str,
    request_id: &str,
) -> Result<Option<AssetFormat>, UnifiedError> {
    match format {
        None => Ok(None),
        Some(ref s) if s.is_empty() => Err(UnifiedError::new(
            UnifiedErrorCode::InvalidFormat,
            "format query parameter cannot be empty",
            instance,
            request_id,
        )),
        Some(s) => AssetFormat::from_str(&s).map(Some).map_err(|_| {
            UnifiedError::new(
                UnifiedErrorCode::InvalidFormat,
                format!("invalid format '{}', expected 'iceberg' or 'lance'", s),
                instance,
                request_id,
            )
        }),
    }
}

/// Convert (Asset, TabularAsset) to AssetListItem.
fn asset_pair_to_list_item(
    (asset, tabular): (quasar_core::Asset, quasar_core::TabularAsset),
) -> AssetListItem {
    AssetListItem {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type,
        format: tabular.format,
        location: tabular.location,
        metadata_location: tabular.metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// Convert (Asset, TabularAsset) to AssetResponse with current version.
fn asset_pair_to_response(
    (asset, tabular): (quasar_core::Asset, quasar_core::TabularAsset),
    current_version: Option<CurrentVersionResponse>,
) -> AssetResponse {
    AssetResponse {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type,
        format: tabular.format,
        location: tabular.location,
        metadata_location: tabular.metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        current_version,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// Unwrap the tabular extension from a unified read. V3 unified reads
/// return `(Asset, Option<TabularAsset>)`; today every asset is tabular,
/// but the V2 trait surface here promises a non-optional pair, so we
/// surface a clear NotFound when an asset has no extension row.
fn require_tabular(
    pair: (quasar_core::Asset, Option<quasar_core::TabularAsset>),
    name: &str,
    instance: &str,
    request_id: &str,
) -> Result<(quasar_core::Asset, quasar_core::TabularAsset), UnifiedError> {
    match pair {
        (asset, Some(tabular)) => Ok((asset, tabular)),
        (_, None) => Err(UnifiedError::new(
            UnifiedErrorCode::AssetNotFound,
            format!("asset '{}' has no tabular extension", name),
            instance,
            request_id,
        )),
    }
}

/// GET /unified/v1/namespaces/{ns}/assets
pub async fn list_assets(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
    Query(query): Query<AssetListQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets", ns);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let page_size = query
        .resolved_page_size()
        .map_err(|e| map_page_size_error(e, &instance, request_id.as_str()))?;
    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            instance.clone(),
            request_id.clone(),
        )
    })?;

    let format = parse_optional_format(query.format, &instance, &request_id)?;
    let format_str = format.map(|f| f.as_str());

    let name_filter = match query.name {
        Some(name) => {
            validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
            Some(name)
        }
        None => None,
    };

    let rows = store
        .list_assets_unified(
            DEFAULT_DOMAIN,
            &ns,
            format_str,
            name_filter.as_deref(),
            offset,
            page_size,
        )
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let next_page_token = if rows.len() as i32 >= page_size {
        Some(super::dto::PaginationQuery::encode_token(
            offset + rows.len() as i64,
        ))
    } else {
        None
    };

    // Phase 2 keeps the V2 wire shape, which only surfaces tabular assets;
    // non-tabular rows from the V3 LEFT JOIN are filtered out here. Phase 3
    // can promote them once `AssetListItem` learns to encode non-tabular
    // assets.
    let assets: Vec<AssetListItem> = rows
        .into_iter()
        .filter_map(|(asset, tabular)| tabular.map(|t| asset_pair_to_list_item((asset, t))))
        .collect();

    Ok((
        StatusCode::OK,
        Json(ListAssetsResponse {
            assets,
            next_page_token,
        }),
    ))
}

/// GET /unified/v1/namespaces/{ns}/assets/{name}
pub async fn get_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((ns, name)): Path<(String, String)>,
    Query(query): Query<AssetDetailQuery>,
    Extension(config): Extension<super::UnifiedConfig>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    let format = parse_format(query.format, &instance, &request_id)?;

    let raw = store
        .get_asset_unified(DEFAULT_DOMAIN, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;
    let pair = require_tabular(raw, &name, &instance, &request_id)?;

    let current_version = super::version::get_current_version(
        store.as_ref(),
        &config,
        &ns,
        &name,
        format,
        pair.1.metadata_location.as_deref(),
        &instance,
        &request_id,
    )
    .await?;

    Ok((
        StatusCode::OK,
        Json(asset_pair_to_response(pair, current_version)),
    ))
}

/// DELETE /unified/v1/namespaces/{ns}/assets/{name}
pub async fn drop_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((ns, name)): Path<(String, String)>,
    Query(query): Query<AssetDetailQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    // Phase 2 still requires the `format` query parameter for wire
    // compatibility; the storage drop is format-agnostic, so we validate
    // and discard. Phase 3 will drop the parameter entirely.
    let _ = parse_format(query.format, &instance, &request_id)?;

    store
        .drop_asset(DEFAULT_DOMAIN, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /unified/v1/namespaces/{ns}/assets/{name}
pub async fn update_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((ns, name)): Path<(String, String)>,
    Query(query): Query<AssetDetailQuery>,
    Extension(config): Extension<super::UnifiedConfig>,
    Json(req): Json<UpdateAssetRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    let format = parse_format(query.format, &instance, &request_id)?;

    store
        .update_asset(
            DEFAULT_DOMAIN,
            &ns,
            &name,
            req.comment,
            &req.removals,
            &req.updates,
        )
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    // Re-fetch to get full asset + tabular for response
    let raw = store
        .get_asset_unified(DEFAULT_DOMAIN, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;
    let pair = require_tabular(raw, &name, &instance, &request_id)?;

    let current_version = super::version::get_current_version(
        store.as_ref(),
        &config,
        &ns,
        &name,
        format,
        pair.1.metadata_location.as_deref(),
        &instance,
        &request_id,
    )
    .await?;

    Ok((
        StatusCode::OK,
        Json(asset_pair_to_response(pair, current_version)),
    ))
}

/// POST /unified/v1/namespaces/{ns}/assets/{name}/rename
pub async fn rename_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((ns, name)): Path<(String, String)>,
    Query(query): Query<AssetDetailQuery>,
    Json(req): Json<RenameAssetRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}/rename", ns, name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&req.new_name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    // V2 wire shape requires format; storage rename is format-agnostic.
    let _ = parse_format(query.format, &instance, &request_id)?;

    store
        .rename_asset(DEFAULT_DOMAIN, &ns, &name, &req.new_name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok(StatusCode::OK)
}

/// POST /unified/v1/namespaces/{ns}/assets is intentionally not supported in V2.
pub async fn create_asset_not_allowed(
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
) -> Result<StatusCode, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets", ns);
    Err(UnifiedError::new(
        UnifiedErrorCode::MethodNotAllowed,
        "Unified Asset creation is not supported in V2",
        instance,
        request_id,
    ))
}

fn map_page_size_error(err: PageSizeError, instance: &str, request_id: &str) -> UnifiedError {
    match err {
        PageSizeError::Invalid => UnifiedError::new(
            UnifiedErrorCode::InvalidInput,
            "pageSize must be greater than 0",
            instance,
            request_id,
        ),
        PageSizeError::TooLarge => UnifiedError::new(
            UnifiedErrorCode::PageSizeTooLarge,
            format!(
                "pageSize must not exceed {}",
                PaginationQuery::MAX_PAGE_SIZE
            ),
            instance,
            request_id,
        ),
    }
}
