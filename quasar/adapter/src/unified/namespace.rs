//! Namespace management handlers (`/unified/v1/domains/{domain}/namespaces*`).
//!
//! Namespaces are addressed by hierarchical path (`analytics/teams/finance`),
//! captured with the axum wildcard `{*path}` (REQUIREMENTS §6.2). The same
//! wildcard route also serves the path-addressed asset endpoints: a path
//! ending in `/assets` is the asset collection of the namespace, and
//! `.../assets/{asset}` addresses a single asset by name (DESIGN §5.3).

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use quasar_core::{
    validate_name, validate_namespace_path, CatalogStore, CreateNamespace, NamespacePatch,
};
use std::sync::Arc;

use super::dto::{
    next_page_token, CreateNamespaceRequest, ListResponse, NamespaceListQuery, NamespacePathQuery,
    NamespaceResponse, UpdateNamespaceRequest,
};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::{asset, request_id_of};

fn namespaces_instance(domain: &str) -> String {
    format!("/unified/v1/domains/{}/namespaces", domain)
}

fn path_instance(domain: &str, path: &str) -> String {
    format!("/unified/v1/domains/{}/namespaces/{}", domain, path)
}

/// What a wildcard namespace path actually targets (DESIGN §5.3).
pub(crate) enum PathTarget<'a> {
    /// `.../assets`: the asset collection of the namespace.
    AssetList(&'a str),
    /// `.../assets/{asset}`: a single asset addressed by name.
    AssetGet(&'a str, &'a str),
    /// Anything else: the namespace itself.
    Namespace(&'a str),
}

/// Split a wildcard path into its target. The split happens at the *last*
/// `/assets` separator so namespaces containing a segment literally named
/// `assets` still resolve (e.g. `a/assets/b/assets` lists assets of
/// `a/assets/b`). A trailing multi-segment remainder after `/assets/`
/// cannot be an asset name and falls back to the namespace itself.
pub(crate) fn asset_path_target(path: &str) -> PathTarget<'_> {
    if let Some(ns) = path.strip_suffix("/assets") {
        PathTarget::AssetList(ns)
    } else if let Some((ns, name)) = path.rsplit_once("/assets/") {
        if name.is_empty() || name.contains('/') {
            PathTarget::Namespace(path)
        } else {
            PathTarget::AssetGet(ns, name)
        }
    } else {
        PathTarget::Namespace(path)
    }
}

/// GET /unified/v1/domains/{domain}/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(domain): Path<String>,
    Query(query): Query<NamespaceListQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = namespaces_instance(&domain);
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    let (offset, limit) = query
        .pagination
        .resolve()
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    if let Some(ref prefix) = query.prefix {
        validate_namespace_path(prefix)
            .map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    }

    let namespaces = store
        .list_namespaces(&domain, query.prefix.as_deref(), offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let token = next_page_token(offset, &namespaces, limit);
    Ok(Json(ListResponse::new(
        namespaces
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        token,
    )))
}

/// POST /unified/v1/domains/{domain}/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(domain): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = namespaces_instance(&domain);
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    validate_namespace_path(&req.path).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let input = CreateNamespace {
        comment: req.comment,
        properties: req.properties,
    };

    let ns = store
        .create_namespace(&domain, &req.path, input)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok((StatusCode::CREATED, Json(NamespaceResponse::from(ns))))
}

/// GET /unified/v1/domains/{domain}/namespaces/{*path}
///
/// Dispatches between namespace get, asset list and asset get-by-name
/// based on the `/assets` suffix (see `asset_path_target`).
pub async fn get_path(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path((domain, path)): Path<(String, String)>,
    Query(query): Query<NamespacePathQuery>,
) -> Result<Response, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = path_instance(&domain, &path);
    match asset_path_target(&path) {
        PathTarget::AssetList(ns) => {
            asset::list_assets_in_namespace(store, &domain, ns, &query, &instance, &request_id)
                .await
        }
        PathTarget::AssetGet(ns, name) => {
            asset::get_asset_by_name(store, &domain, ns, name, &instance, &request_id).await
        }
        PathTarget::Namespace(ns) => {
            validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
            validate_namespace_path(ns)
                .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

            let namespace = store
                .get_namespace(&domain, ns)
                .await
                .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

            Ok(Json(NamespaceResponse::from(namespace)).into_response())
        }
    }
}

/// PATCH /unified/v1/domains/{domain}/namespaces/{*path}
///
/// Assets are read-only in the Unified API (REQUIREMENTS §4.3); a path
/// targeting an asset is rejected as a validation error.
pub async fn update_path(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path((domain, path)): Path<(String, String)>,
    Json(req): Json<UpdateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = path_instance(&domain, &path);
    let ns = writable_namespace_path(&path, &instance, &request_id)?;
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    validate_namespace_path(ns).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let patch = NamespacePatch {
        comment: req.comment,
        properties: req.properties,
    };

    let updated = store
        .update_namespace(&domain, ns, patch)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(NamespaceResponse::from(updated)))
}

/// DELETE /unified/v1/domains/{domain}/namespaces/{*path}
pub async fn delete_path(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path((domain, path)): Path<(String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = path_instance(&domain, &path);
    let ns = writable_namespace_path(&path, &instance, &request_id)?;
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    validate_namespace_path(ns).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    store
        .delete_namespace(&domain, ns)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}

/// Reject write attempts against asset paths: asset lifecycle operations
/// belong to the native protocol adapters (REQUIREMENTS §4.3).
fn writable_namespace_path<'a>(
    path: &'a str,
    instance: &str,
    request_id: &str,
) -> Result<&'a str, UnifiedError> {
    match asset_path_target(path) {
        PathTarget::Namespace(ns) => Ok(ns),
        _ => Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            "asset write endpoints are not provided by the Unified API; \
             asset lifecycle operations are managed by native protocol adapters",
            instance,
            request_id,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_target_dispatch() {
        assert!(matches!(
            asset_path_target("analytics/teams/finance"),
            PathTarget::Namespace("analytics/teams/finance")
        ));
        assert!(matches!(
            asset_path_target("analytics/teams/assets"),
            PathTarget::AssetList("analytics/teams")
        ));
        assert!(matches!(
            asset_path_target("analytics/teams/assets/orders"),
            PathTarget::AssetGet("analytics/teams", "orders")
        ));
        // A namespace may itself contain an `assets` segment.
        assert!(matches!(
            asset_path_target("a/assets/b/assets"),
            PathTarget::AssetList("a/assets/b")
        ));
        assert!(matches!(
            asset_path_target("a/assets/b/assets/t1"),
            PathTarget::AssetGet("a/assets/b", "t1")
        ));
        // Multi-segment remainder after the last `/assets/` is a namespace.
        assert!(matches!(
            asset_path_target("a/assets/b/c"),
            PathTarget::Namespace("a/assets/b/c")
        ));
        assert!(matches!(
            asset_path_target("assets"),
            PathTarget::Namespace("assets")
        ));
    }
}
