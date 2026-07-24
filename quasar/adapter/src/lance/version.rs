use axum::{
    extract::{Extension, Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{CatalogError, CatalogStore, CreateVersion};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::error::{catalog_error_to_lance_table, LanceError, ProblemDetails};
use super::id::parse_table_id;
use super::request_id_of;
use super::table::{lance_table_by_name, parse_version_key};

/// Upper bound for the unpaginated version list; fits the storage layer's
/// `u64 -> i64` limit conversion.
const VERSION_LIST_LIMIT: u64 = i64::MAX as u64;

// ── Request DTOs ───────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateVersionRequest {
    pub version: i64,
    pub manifest_path: String,
}

#[derive(Deserialize)]
pub struct DescribeVersionRequest {
    pub version: i64,
}

// ── Response DTOs ──────────────────────────────────────────

#[derive(Serialize)]
pub struct VersionResponse {
    pub version: i64,
    pub manifest_path: String,
}

#[derive(Serialize)]
pub struct ListVersionsResponse {
    pub versions: Vec<i64>,
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/table/{id}/version/create
pub async fn create_version(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Json(req): Json<CreateVersionRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/version/create", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    // Version mirroring is keyed on asset_id; resolve the Lance table first.
    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    // Lance does not go through CAS (DESIGN §6.4): the store inserts the
    // mirrored version and updates assets.current_version_key in one
    // transaction; a duplicate native version number violates
    // UNIQUE(asset_id, version_key) and surfaces as 409. Link the version
    // chain to the current version so the single-root constraint holds.
    let previous_version_id = match store.get_latest_version(asset.id).await {
        Ok(current) => Some(current.id),
        Err(CatalogError::NotFound(_)) => None,
        Err(e) => {
            return Err(catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id));
        }
    };

    let input = CreateVersion {
        asset_id: asset.id,
        version_key: req.version.to_string(),
        version_properties: None,
        content_inline: None,
        content_pointer: Some(req.manifest_path.clone()),
        previous_version_id,
    };
    store
        .create_version(input)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: req.version,
            manifest_path: req.manifest_path,
        }),
    ))
}

/// GET /lance/v1/table/{id}/version/list
pub async fn list_versions(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/version/list", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    // The endpoint predates token pagination; fetch the full history and
    // order by the native version number (storage orders by created_at).
    let versions = store
        .list_versions(asset.id, 0, VERSION_LIST_LIMIT)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    let mut version_ids = versions
        .iter()
        .map(|v| parse_version_key(&v.version_key, &asset, &instance))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_problem_details(&request_id))?;
    version_ids.sort_unstable();

    Ok((
        StatusCode::OK,
        Json(ListVersionsResponse {
            versions: version_ids,
        }),
    ))
}

/// POST /lance/v1/table/{id}/version/describe
pub async fn describe_version(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Json(req): Json<DescribeVersionRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/version/describe", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    let version = store
        .get_version(asset.id, &req.version.to_string())
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    // The manifest path is mirrored as content_pointer; Lance versions
    // always carry one.
    let manifest_path = version.content_pointer.ok_or_else(|| {
        tracing::error!(asset_id = %asset.id, version_key = %version.version_key,
            "lance version missing content_pointer");
        LanceError::InternalError {
            detail: "An internal error occurred".to_string(),
            instance: instance.clone(),
        }
        .to_problem_details(&request_id)
    })?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: req.version,
            manifest_path,
        }),
    ))
}
