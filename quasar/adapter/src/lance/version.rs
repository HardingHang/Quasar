use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::CatalogStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance_version, ProblemDetails};
use super::table::parse_table_id;
use crate::DEFAULT_DOMAIN;

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
    let (namespace, table) = parse_table_id(&id)?;

    // Resolve asset id first; V3 version operations are keyed on asset_id.
    let (asset, _) = store
        .get_tabular_asset(DEFAULT_DOMAIN, namespace, "lance", table)
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

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version.version_order.unwrap_or(0),
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
    let (namespace, table) = parse_table_id(&id)?;

    let (asset, _) = store
        .get_tabular_asset(DEFAULT_DOMAIN, namespace, "lance", table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let versions = store
        .list_tabular_versions(asset.id)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(ListVersionsResponse {
            versions: versions
                .into_iter()
                .map(|(v, _)| v.version_order.unwrap_or(0))
                .collect(),
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
    let (namespace, table) = parse_table_id(&id)?;

    let (asset, _) = store
        .get_tabular_asset(DEFAULT_DOMAIN, namespace, "lance", table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    let version_key = req.version.to_string();
    let (version, tabular_version) = store
        .get_tabular_version(asset.id, &version_key)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version.version_order.unwrap_or(0),
            manifest_path: tabular_version.metadata_location,
        }),
    ))
}
