use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{AssetFormat, CatalogStore};
use std::str::FromStr;
use std::sync::Arc;

use super::dto::{
    AssetDetailQuery, AssetListItem, AssetListQuery, AssetResponse, ListAssetsResponse,
    RenameAssetRequest, UpdateAssetRequest,
};
use super::error::{map_asset_error, UnifiedError, UnifiedErrorCode};

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

/// Convert (Asset, TabularAsset) to AssetListItem.
fn asset_pair_to_list_item(
    (asset, tabular): (quasar_core::Asset, quasar_core::TabularAsset),
) -> AssetListItem {
    AssetListItem {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type.as_str().to_string(),
        format: asset.asset_subtype,
        location: tabular.location,
        metadata_location: tabular.metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// Convert (Asset, TabularAsset) to AssetResponse.
/// S5: current_version is always None; V2-S6 will populate it.
fn asset_pair_to_response(
    (asset, tabular): (quasar_core::Asset, quasar_core::TabularAsset),
) -> AssetResponse {
    AssetResponse {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type.as_str().to_string(),
        format: asset.asset_subtype,
        location: tabular.location,
        metadata_location: tabular.metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        current_version: None,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// GET /unified/v1/namespaces/{ns}/assets
pub async fn list_assets(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
    Query(query): Query<AssetListQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let page_size = query.resolved_page_size();
    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            format!("/unified/v1/namespaces/{}/assets", ns),
            request_id.clone(),
        )
    })?;

    let format = query
        .format
        .and_then(|s| if s.is_empty() { None } else { Some(s) })
        .map(|s| {
            AssetFormat::from_str(&s).map_err(|_| {
                UnifiedError::new(
                    UnifiedErrorCode::InvalidFormat,
                    format!("invalid format '{}', expected 'iceberg' or 'lance'", s),
                    format!("/unified/v1/namespaces/{}/assets", ns),
                    request_id.clone(),
                )
            })
        })
        .transpose()?;

    let name_filter = query
        .name
        .and_then(|s| if s.is_empty() { None } else { Some(s) });

    let assets = store
        .list_assets_unified(&ns, format, name_filter.as_deref(), offset, page_size)
        .await
        .map_err(|e| {
            map_asset_error(
                e,
                &format!("/unified/v1/namespaces/{}/assets", ns),
                &request_id,
            )
        })?;

    let next_page_token = if assets.len() as i32 >= page_size {
        Some(super::dto::PaginationQuery::encode_token(
            offset + assets.len() as i64,
        ))
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListAssetsResponse {
            assets: assets.into_iter().map(asset_pair_to_list_item).collect(),
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
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    let format = parse_format(query.format, &instance, &request_id)?;

    let pair = store
        .get_asset_unified(&ns, &name, format)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(asset_pair_to_response(pair))))
}

/// DELETE /unified/v1/namespaces/{ns}/assets/{name}
pub async fn drop_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((ns, name)): Path<(String, String)>,
    Query(query): Query<AssetDetailQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    let format = parse_format(query.format, &instance, &request_id)?;

    store
        .drop_asset(&ns, format, &name)
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
    Json(req): Json<UpdateAssetRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = format!("/unified/v1/namespaces/{}/assets/{}", ns, name);
    let format = parse_format(query.format, &instance, &request_id)?;

    store
        .update_asset_properties(&ns, format, &name, req.comment, &req.removals, &req.updates)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    // Re-fetch to get full asset + tabular for response
    let pair = store
        .get_asset_unified(&ns, &name, format)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(asset_pair_to_response(pair))))
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
    let format = parse_format(query.format, &instance, &request_id)?;

    store
        .rename_asset(&ns, format, &name, &req.new_name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok(StatusCode::OK)
}
