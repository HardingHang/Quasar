//! Asset tag management handlers (`/unified/v1/assets/{asset_id}/tags*`).
//!
//! Tags are governance annotations stored in `asset_tags`; writing them
//! does not touch native protocol state, so they are open for every asset
//! including native protocol assets (FR-A3, DESIGN §3.2).

use axum::{
    extract::{Extension, Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

use super::dto::{AddTagRequest, TagsResponse};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::{asset, request_id_of};

fn tags_instance(asset_id: &str) -> String {
    format!("/unified/v1/assets/{}/tags", asset_id)
}

fn validate_tag(tag: &str, instance: &str, request_id: &str) -> Result<(), UnifiedError> {
    if tag.is_empty() {
        return Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            "tag must not be empty",
            instance,
            request_id,
        ));
    }
    Ok(())
}

/// GET /unified/v1/assets/{asset_id}/tags
pub async fn list_tags(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(asset_id): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = tags_instance(&asset_id);
    let id = asset::parse_asset_id(&asset_id, &instance, &request_id)?;

    let tags = store
        .list_tags(id)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(TagsResponse { tags }))
}

/// POST /unified/v1/assets/{asset_id}/tags
pub async fn add_tag(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(asset_id): Path<String>,
    Json(req): Json<AddTagRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = tags_instance(&asset_id);
    let id = asset::parse_asset_id(&asset_id, &instance, &request_id)?;
    validate_tag(&req.tag, &instance, &request_id)?;

    store
        .add_tag(id, &req.tag)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let tags = store
        .list_tags(id)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok((StatusCode::CREATED, Json(TagsResponse { tags })))
}

/// DELETE /unified/v1/assets/{asset_id}/tags/{tag}
pub async fn remove_tag(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path((asset_id, tag)): Path<(String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = format!("{}/{}", tags_instance(&asset_id), tag);
    let id = asset::parse_asset_id(&asset_id, &instance, &request_id)?;
    validate_tag(&tag, &instance, &request_id)?;

    store
        .remove_tag(id, &tag)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}
