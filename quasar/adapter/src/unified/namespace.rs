use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::CatalogStore;
use std::sync::Arc;

use super::dto::{
    CreateNamespaceRequest, ListNamespacesResponse, NamespaceResponse, PageSizeError,
    PaginationQuery, UpdateNamespaceRequest,
};
use super::error::{map_namespace_error, UnifiedError, UnifiedErrorCode};
use quasar_core::validate_name;

fn namespace_to_response(ns: quasar_core::Namespace) -> NamespaceResponse {
    NamespaceResponse {
        id: ns.id.to_string(),
        name: ns.name,
        comment: ns.comment,
        properties: ns.properties,
        created_at: ns.created_at.to_rfc3339(),
    }
}

fn namespaces_instance(domain: &str) -> String {
    format!("/unified/v1/domains/{}/namespaces", domain)
}

fn namespace_instance(domain: &str, ns: &str) -> String {
    format!("/unified/v1/domains/{}/namespaces/{}", domain, ns)
}

/// GET /unified/v1/domains/{domain}/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(domain): Path<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = namespaces_instance(&domain);
    let page_size = query
        .resolved_page_size()
        .map_err(|e| map_page_size_error(e, &instance, request_id.as_str()))?;

    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            &instance,
            request_id.clone(),
        )
    })?;

    let namespaces = store
        .list_namespaces(&domain, offset, page_size)
        .await
        .map_err(|e| map_namespace_error(e, &instance, &request_id))?;

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

/// POST /unified/v1/domains/{domain}/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(domain): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = namespaces_instance(&domain);
    validate_name(&req.name).map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    let ns = store
        .create_namespace(&domain, &req.name, req.comment, req.properties)
        .await
        .map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    Ok((StatusCode::CREATED, Json(namespace_to_response(ns))))
}

/// GET /unified/v1/domains/{domain}/namespaces/{ns}
pub async fn get_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns)): Path<(String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = namespace_instance(&domain, &ns);
    validate_name(&ns).map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    let namespace = store
        .get_namespace(&domain, &ns)
        .await
        .map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(namespace_to_response(namespace))))
}

/// DELETE /unified/v1/domains/{domain}/namespaces/{ns}
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns)): Path<(String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = namespace_instance(&domain, &ns);
    validate_name(&ns).map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    store
        .drop_namespace(&domain, &ns)
        .await
        .map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /unified/v1/domains/{domain}/namespaces/{ns}
pub async fn update_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns)): Path<(String, String)>,
    Json(req): Json<UpdateNamespaceRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = namespace_instance(&domain, &ns);
    validate_name(&ns).map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    let updated = store
        .update_namespace(&domain, &ns, req.comment, &req.removals, &req.updates)
        .await
        .map_err(|e| map_namespace_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(namespace_to_response(updated))))
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
