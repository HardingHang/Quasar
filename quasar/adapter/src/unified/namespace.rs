use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

use super::dto::{
    CreateNamespaceRequest, ListNamespacesResponse, NamespaceResponse, PaginationQuery,
    UpdateNamespaceRequest,
};
use super::error::{map_namespace_error, UnifiedError, UnifiedErrorCode};

fn namespace_to_response(ns: quasar_core::Namespace) -> NamespaceResponse {
    NamespaceResponse {
        id: ns.id.to_string(),
        name: ns.name,
        comment: ns.comment,
        properties: ns.properties,
        created_at: ns.created_at.to_rfc3339(),
    }
}

/// GET /unified/v1/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let page_size = query.resolved_page_size();

    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            "/unified/v1/namespaces",
            request_id.clone(),
        )
    })?;

    let namespaces = store
        .list_namespaces(offset, page_size)
        .await
        .map_err(|e| map_namespace_error(e, "/unified/v1/namespaces", &request_id))?;

    let next_page_token = if namespaces.len() as i32 >= page_size {
        Some(PaginationQuery::encode_token(
            offset + namespaces.len() as i64,
        ))
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListNamespacesResponse {
            namespaces: namespaces.into_iter().map(namespace_to_response).collect(),
            next_page_token,
        }),
    ))
}

/// POST /unified/v1/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let ns = store
        .create_namespace(&req.name, req.comment, req.properties)
        .await
        .map_err(|e| map_namespace_error(e, "/unified/v1/namespaces", &request_id))?;

    Ok((StatusCode::CREATED, Json(namespace_to_response(ns))))
}

/// GET /unified/v1/namespaces/{ns}
pub async fn get_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let namespace = store.get_namespace(&ns).await.map_err(|e| {
        map_namespace_error(e, &format!("/unified/v1/namespaces/{}", ns), &request_id)
    })?;

    Ok((StatusCode::OK, Json(namespace_to_response(namespace))))
}

/// DELETE /unified/v1/namespaces/{ns}
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    store.drop_namespace(&ns).await.map_err(|e| {
        map_namespace_error(e, &format!("/unified/v1/namespaces/{}", ns), &request_id)
    })?;

    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /unified/v1/namespaces/{ns}
pub async fn update_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(ns): Path<String>,
    Json(req): Json<UpdateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let updated = store
        .update_namespace(&ns, req.comment, &req.removals, &req.updates)
        .await
        .map_err(|e| {
            map_namespace_error(e, &format!("/unified/v1/namespaces/{}", ns), &request_id)
        })?;

    Ok((StatusCode::OK, Json(namespace_to_response(updated))))
}
