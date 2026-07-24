//! Domain management handlers (`/unified/v1/domains*`).
//!
//! Domains have no native protocol; the Unified API owns their full
//! lifecycle (REQUIREMENTS §4.1). Responses structurally redact
//! `storage_config` (see `DomainResponse`).

use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_name, CatalogStore, CreateDomain, DomainPatch, PatchField};
use std::sync::Arc;

use super::dto::{
    next_page_token, CreateDomainRequest, DomainResponse, ListResponse, PaginationQuery,
    UpdateDomainRequest,
};
use super::error::{map_catalog_error, UnifiedError, UnifiedErrorCode};
use super::request_id_of;

const BASE_INSTANCE: &str = "/unified/v1/domains";

/// Allowed `storage_type` values (DESIGN §3.2 `domains` CHECK constraint).
const VALID_STORAGE_TYPES: [&str; 4] = ["s3", "minio", "hdfs", "local"];

fn instance_for(name: &str) -> String {
    format!("{}/{}", BASE_INSTANCE, name)
}

fn validate_storage_type(
    storage_type: &str,
    instance: &str,
    request_id: &str,
) -> Result<(), UnifiedError> {
    if VALID_STORAGE_TYPES.contains(&storage_type) {
        Ok(())
    } else {
        Err(UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            format!(
                "invalid storage_type '{}', expected one of: {}",
                storage_type,
                VALID_STORAGE_TYPES.join(", ")
            ),
            instance,
            request_id,
        ))
    }
}

/// GET /unified/v1/domains
pub async fn list_domains(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Query(query): Query<PaginationQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let (offset, limit) = query
        .resolve()
        .map_err(|e| map_catalog_error(e, BASE_INSTANCE, &request_id))?;

    let domains = store
        .list_domains(offset, limit)
        .await
        .map_err(|e| map_catalog_error(e, BASE_INSTANCE, &request_id))?;

    let token = next_page_token(offset, &domains, limit);
    Ok(Json(ListResponse::new(
        domains.into_iter().map(DomainResponse::from).collect(),
        token,
    )))
}

/// POST /unified/v1/domains
pub async fn create_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Json(req): Json<CreateDomainRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    validate_name(&req.name).map_err(|e| map_catalog_error(e, BASE_INSTANCE, &request_id))?;
    if let Some(ref storage_type) = req.storage_type {
        validate_storage_type(storage_type, BASE_INSTANCE, &request_id)?;
    }

    let input = CreateDomain {
        name: req.name,
        comment: req.comment,
        properties: req.properties,
        storage_type: req.storage_type,
        storage_config: req.storage_config,
        warehouse: req.warehouse,
    };

    let domain = store
        .create_domain(input)
        .await
        .map_err(|e| map_catalog_error(e, BASE_INSTANCE, &request_id))?;

    Ok((StatusCode::CREATED, Json(DomainResponse::from(domain))))
}

/// GET /unified/v1/domains/{domain}
pub async fn get_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(domain): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    let domain = store
        .get_domain(&domain)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(DomainResponse::from(domain)))
}

/// PATCH /unified/v1/domains/{domain}
pub async fn update_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(domain): Path<String>,
    Json(req): Json<UpdateDomainRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;
    if let PatchField::Set(ref storage_type) = req.storage_type {
        validate_storage_type(storage_type, &instance, &request_id)?;
    }

    let patch = DomainPatch {
        comment: req.comment,
        properties: req.properties,
        storage_type: req.storage_type,
        storage_config: req.storage_config,
        warehouse: req.warehouse,
    };

    let updated = store
        .update_domain(&domain, patch)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(Json(DomainResponse::from(updated)))
}

/// DELETE /unified/v1/domains/{domain}
pub async fn delete_domain(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(domain): Path<String>,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let instance = instance_for(&domain);
    validate_name(&domain).map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    store
        .delete_domain(&domain)
        .await
        .map_err(|e| map_catalog_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}
