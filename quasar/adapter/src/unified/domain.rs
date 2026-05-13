//! V3 Unified Domain management handlers (`/unified/v1/domains*`).
//!
//! Domain management is Quasar-specific: protocol adapters such as
//! Iceberg and Lance cannot create or modify Domains. Only the Unified
//! API exposes Domain CRUD per `docs/v3/V3_DESIGN.md` §4.1.3 and §8.3.
//!
//! Responses redact `storage_config` at the structural level (see
//! `DomainResponse`): the field is never serialized because it may
//! contain credentials or secret references (§3.1 invariant 8).

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{CatalogStore, DomainPatch};
use std::sync::Arc;

use super::dto::{
    CreateDomainRequest, DomainResponse, ListDomainsResponse, PageSizeError, PaginationQuery,
    UpdateDomainRequest,
};
use super::error::{map_domain_error, UnifiedError, UnifiedErrorCode};
use quasar_core::validate_name;

const BASE_INSTANCE: &str = "/unified/v1/domains";

fn domain_to_response(domain: quasar_core::Domain) -> DomainResponse {
    DomainResponse {
        id: domain.id.to_string(),
        name: domain.name,
        comment: domain.comment,
        properties: domain.properties,
        storage_type: domain.storage_type,
        warehouse: domain.warehouse,
        owner: domain.owner,
        created_at: domain.created_at.to_rfc3339(),
        updated_at: domain.updated_at.to_rfc3339(),
    }
}

fn instance_for(name: &str) -> String {
    format!("{}/{}", BASE_INSTANCE, name)
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

/// GET /unified/v1/domains
pub async fn list_domains(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let page_size = query
        .resolved_page_size()
        .map_err(|e| map_page_size_error(e, BASE_INSTANCE, request_id.as_str()))?;

    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            BASE_INSTANCE,
            request_id.clone(),
        )
    })?;

    let domains = store
        .list_domains(offset, page_size)
        .await
        .map_err(|e| map_domain_error(e, BASE_INSTANCE, &request_id))?;

    let next_page_token = if domains.len() as i32 >= page_size {
        Some(PaginationQuery::encode_token(offset + domains.len() as i64))
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListDomainsResponse {
            domains: domains.into_iter().map(domain_to_response).collect(),
            next_page_token,
        }),
    ))
}

/// POST /unified/v1/domains
pub async fn create_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Json(req): Json<CreateDomainRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    validate_name(&req.name).map_err(|e| map_domain_error(e, BASE_INSTANCE, &request_id))?;

    let domain = store
        .create_domain(
            &req.name,
            req.comment,
            req.properties,
            req.storage_type,
            req.storage_config.unwrap_or(serde_json::Value::Null),
            req.warehouse,
            req.owner,
        )
        .await
        .map_err(|e| map_domain_error(e, BASE_INSTANCE, &request_id))?;

    Ok((StatusCode::CREATED, Json(domain_to_response(domain))))
}

/// GET /unified/v1/domains/{domain}
pub async fn get_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(domain): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_domain_error(e, &instance, &request_id))?;

    let result = store
        .get_domain(&domain)
        .await
        .map_err(|e| map_domain_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(domain_to_response(result))))
}

/// DELETE /unified/v1/domains/{domain}
pub async fn drop_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(domain): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_domain_error(e, &instance, &request_id))?;

    store
        .drop_domain(&domain)
        .await
        .map_err(|e| map_domain_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /unified/v1/domains/{domain}
pub async fn update_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path(domain): Path<String>,
    Json(req): Json<UpdateDomainRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_domain_error(e, &instance, &request_id))?;

    let patch = DomainPatch {
        comment: req.comment,
        property_removals: req.removals,
        property_updates: req.updates,
        storage_type: req.storage_type,
        storage_config: req.storage_config,
        warehouse: req.warehouse,
        owner: req.owner,
    };

    let updated = store
        .update_domain(&domain, patch)
        .await
        .map_err(|e| map_domain_error(e, &instance, &request_id))?;

    Ok((StatusCode::OK, Json(domain_to_response(updated))))
}
