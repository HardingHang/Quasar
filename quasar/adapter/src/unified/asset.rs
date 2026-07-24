//! Asset read-only handlers plus the soft-delete restore governance
//! endpoint (REQUIREMENTS §4.3 / §6.2).
//!
//! Asset lifecycle (create/update/rename/delete) belongs to the native
//! protocol adapters; the Unified API only reads assets, restores
//! soft-deleted ones, and manages tags. Single-asset operations are
//! addressed by id (`/unified/v1/assets/{asset_id}`); the path-addressed
//! read endpoints (`.../namespaces/{*path}/assets[/{asset}]`) are
//! dispatched here from the namespace wildcard route (DESIGN §5.3).

use axum::{
    extract::{Extension, Path, State},
    response::{IntoResponse, Response},
    Json,
};
use quasar_core::{validate_name, validate_namespace_path, Asset, AssetFilter, CatalogStore};
use std::sync::Arc;
use uuid::Uuid;

use super::dto::{
    next_page_token, AssetListItem, AssetResponse, NamespacePathQuery, VersionResponse,
};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::{registry, request_id_of};

fn asset_instance(id: &str) -> String {
    format!("/unified/v1/assets/{}", id)
}

/// Parse a path segment as an asset UUID.
pub(crate) fn parse_asset_id(
    raw: &str,
    instance: &str,
    request_id: &str,
) -> Result<Uuid, UnifiedError> {
    Uuid::parse_str(raw).map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            format!("invalid asset id '{}', expected a UUID", raw),
            instance,
            request_id,
        )
    })
}

/// Fetch the current version of an asset from the database via
/// `VersionStore::get_latest_version` (never from the object store).
/// Assets without a `current_version_key` have no current version.
async fn current_version_for(
    store: &Arc<dyn CatalogStore>,
    asset: &Asset,
    instance: &str,
    request_id: &str,
) -> Result<Option<VersionResponse>, UnifiedError> {
    if asset.current_version_key.is_none() {
        return Ok(None);
    }
    let version = store
        .get_latest_version(asset.id)
        .await
        .map_err(|e| map_catalog_error(e, instance, request_id))?;
    Ok(Some(VersionResponse::from(version)))
}

/// GET /unified/v1/assets/{asset_id}
pub async fn get_asset_by_id(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(asset_id): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = asset_instance(&asset_id);
    let id = parse_asset_id(&asset_id, &instance, &request_id)?;

    let asset = store
        .get_asset(id)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let current_version = current_version_for(&store, &asset, &instance, &request_id).await?;
    Ok(Json(AssetResponse::from_asset(asset, current_version)))
}

/// POST /unified/v1/assets/{asset_id}/restore
///
/// Governance exception to the read-only rule (FR-A9): clears
/// `deleted_at`. A name clash with an active asset surfaces as the
/// storage-provided `409 Conflict`.
pub async fn restore_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(asset_id): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = format!("/unified/v1/assets/{}/restore", asset_id);
    let id = parse_asset_id(&asset_id, &instance, &request_id)?;

    let asset = store
        .restore_asset(id)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let current_version = current_version_for(&store, &asset, &instance, &request_id).await?;
    Ok(Json(AssetResponse::from_asset(asset, current_version)))
}

/// GET /unified/v1/domains/{domain}/namespaces/{namespace}/assets
/// (reached through the namespace wildcard route).
///
/// Supports `asset_type` / `format` filters; unregistered values are
/// rejected with `VALIDATION_FAILED` (consistent with FR-Q3/Q4).
pub(crate) async fn list_assets_in_namespace(
    store: Arc<dyn CatalogStore>,
    domain: &str,
    namespace: &str,
    query: &NamespacePathQuery,
    instance: &str,
    request_id: &str,
) -> Result<Response, UnifiedError> {
    validate_name(domain).map_err(|e| map_catalog_error(e, instance, request_id))?;
    validate_namespace_path(namespace).map_err(|e| map_catalog_error(e, instance, request_id))?;
    let (offset, limit) = query
        .pagination
        .resolve()
        .map_err(|e| map_catalog_error(e, instance, request_id))?;
    registry::ensure_asset_type_registered(
        &store,
        query.asset_type.as_deref(),
        instance,
        request_id,
    )
    .await?;
    registry::ensure_format_registered(&store, query.format.as_deref(), instance, request_id)
        .await?;

    let filter = AssetFilter {
        domain: Some(domain.to_string()),
        namespace: Some(namespace.to_string()),
        asset_type: query.asset_type.clone(),
        format: query.format.clone(),
        ..Default::default()
    };

    let assets = store
        .list_assets(filter, offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, instance, request_id))?;

    let token = next_page_token(offset, &assets, limit);
    Ok(Json(super::dto::ListResponse::new(
        assets.into_iter().map(AssetListItem::from).collect(),
        token,
    ))
    .into_response())
}

/// GET /unified/v1/domains/{domain}/namespaces/{namespace}/assets/{asset}
/// (reached through the namespace wildcard route).
pub(crate) async fn get_asset_by_name(
    store: Arc<dyn CatalogStore>,
    domain: &str,
    namespace: &str,
    name: &str,
    instance: &str,
    request_id: &str,
) -> Result<Response, UnifiedError> {
    validate_name(domain).map_err(|e| map_catalog_error(e, instance, request_id))?;
    validate_namespace_path(namespace).map_err(|e| map_catalog_error(e, instance, request_id))?;
    validate_name(name).map_err(|e| map_catalog_error(e, instance, request_id))?;

    let asset = store
        .get_asset_by_name(domain, namespace, name)
        .await
        .map_err(|e| map_catalog_error(e, instance, request_id))?;

    let current_version = current_version_for(&store, &asset, instance, request_id).await?;
    Ok(Json(AssetResponse::from_asset(asset, current_version)).into_response())
}
