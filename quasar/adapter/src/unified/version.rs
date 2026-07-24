//! Asset version read-only handlers
//! (`/unified/v1/assets/{asset_id}/versions*`).
//!
//! Versions are mirrored into `asset_versions` by native protocol
//! adapters on commit (FR-V2); the Unified API exposes list/get only —
//! the baseline provides no version write endpoints (REQUIREMENTS §4.4).

use axum::{
    extract::{Extension, Path, Query, State},
    response::IntoResponse,
    Json,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

use super::dto::{next_page_token, ListResponse, PaginationQuery, VersionResponse};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::{asset, request_id_of};

fn versions_instance(asset_id: &str) -> String {
    format!("/unified/v1/assets/{}/versions", asset_id)
}

/// GET /unified/v1/assets/{asset_id}/versions
pub async fn list_versions(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(asset_id): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = versions_instance(&asset_id);
    let id = asset::parse_asset_id(&asset_id, &instance, &request_id)?;
    let (offset, limit) = query
        .resolve()
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let versions = store
        .list_versions(id, offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let token = next_page_token(offset, &versions, limit);
    Ok(Json(ListResponse::new(
        versions.into_iter().map(VersionResponse::from).collect(),
        token,
    )))
}

/// GET /unified/v1/assets/{asset_id}/versions/{version_key}
pub async fn get_version(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path((asset_id, version_key)): Path<(String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = format!("{}/{}", versions_instance(&asset_id), version_key);
    let id = asset::parse_asset_id(&asset_id, &instance, &request_id)?;
    if version_key.is_empty() {
        return Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            "version key must not be empty",
            &instance,
            &request_id,
        ));
    }

    let version = store
        .get_version(id, &version_key)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(VersionResponse::from(version)))
}
