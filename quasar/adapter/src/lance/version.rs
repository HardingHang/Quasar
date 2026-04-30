use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{AssetFormat, CatalogStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::error::{store_error_to_lance_version, ProblemDetails};
use super::table::parse_table_id;

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

    let version = store
        .create_version(
            namespace,
            AssetFormat::Lance,
            table,
            req.version,
            req.manifest_path.clone(),
            None,
        )
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version.version.version_order.unwrap_or(0),
            manifest_path: version.tabular_version.metadata_location,
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

    let versions = store
        .list_versions(namespace, AssetFormat::Lance, table)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(ListVersionsResponse {
            versions: versions
                .into_iter()
                .map(|v| v.version.version_order.unwrap_or(0))
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

    let version = store
        .load_version(namespace, AssetFormat::Lance, table, req.version)
        .await
        .map_err(|e| store_error_to_lance_version(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(VersionResponse {
            version: version.version.version_order.unwrap_or(0),
            manifest_path: version.tabular_version.metadata_location,
        }),
    ))
}
