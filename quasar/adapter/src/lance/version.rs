use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::CatalogStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance_version, LanceError, ProblemDetails};
use super::id::parse_table_id;

// ── Request DTOs ───────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateVersionRequest {
    pub version: i64,
    pub manifest_path: String,
    #[serde(default)]
    pub naming_scheme: Option<String>,
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
    Path(id): Path<String>,
    Json(req): Json<CreateVersionRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/version/create", id);
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    // Resolve asset id first; V3 version operations are keyed on asset_id.
    let (asset, _) = store
        .get_tabular_asset(&parsed.domain, &parsed.namespace, "lance", &parsed.table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_key = req.version.to_string();
    let (version, tabular_version) = store
        .create_tabular_version(
            asset.id,
            &version_key,
            Some(req.version),
            None,
            &req.manifest_path,
            None,
            HashMap::new(),
        )
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_id = version.version_order.ok_or_else(|| {
        tracing::error!(asset_id = %asset.id, version_key = %version.version_key,
            "lance create_version returned version without version_order");
        LanceError::InternalError {
            detail: "An internal error occurred".to_string(),
            instance: instance.clone(),
        }
        .to_problem_details()
    })?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version_id,
            manifest_path: tabular_version.metadata_location,
        }),
    ))
}

/// GET /lance/v1/table/{id}/version/list
pub async fn list_versions(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/version/list", id);
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    let (asset, _) = store
        .get_tabular_asset(&parsed.domain, &parsed.namespace, "lance", &parsed.table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let versions = store
        .list_tabular_versions(asset.id)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_ids = versions
        .into_iter()
        .map(|(v, _)| {
            v.version_order.ok_or_else(|| {
                tracing::error!(asset_id = %asset.id, version_key = %v.version_key,
                    "lance list_versions encountered version without version_order");
                LanceError::InternalError {
                    detail: "An internal error occurred".to_string(),
                    instance: instance.clone(),
                }
                .to_problem_details()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

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
    Path(id): Path<String>,
    Json(req): Json<DescribeVersionRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/version/describe", id);
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    let (asset, _) = store
        .get_tabular_asset(&parsed.domain, &parsed.namespace, "lance", &parsed.table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_key = req.version.to_string();
    let (version, tabular_version) = store
        .get_tabular_version(asset.id, &version_key)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_id = version.version_order.ok_or_else(|| {
        tracing::error!(asset_id = %asset.id, version_key = %version.version_key,
            "lance describe_version returned version without version_order");
        LanceError::InternalError {
            detail: "An internal error occurred".to_string(),
            instance: instance.clone(),
        }
        .to_problem_details()
    })?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version_id,
            manifest_path: tabular_version.metadata_location,
        }),
    ))
}
