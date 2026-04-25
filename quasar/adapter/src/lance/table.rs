use axum::{
    extract::{Extension, Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{AssetFormat, CatalogStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance_table, LanceError, ProblemDetails};
use super::LanceConfig;
use quasar_core::validate_name;

// ── Path Parsing ───────────────────────────────────────────

pub(crate) fn parse_table_id(id: &str) -> Result<(&str, &str), LanceError> {
    id.rsplit_once('$').ok_or_else(|| LanceError::InvalidInput {
        detail: format!(
            "invalid table id '{}': expected '{{namespace}}${{table}}'",
            id
        ),
        instance: format!("/lance/v1/table/{}", id),
    })
}

// ── Request DTOs ───────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct DeclareTableRequest {
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Deserialize)]
pub struct RegisterTableRequest {
    pub location: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

#[derive(Deserialize)]
pub struct RenameTableRequest {
    pub new_name: String,
}

// ── Response DTOs ──────────────────────────────────────────

#[derive(Serialize)]
pub struct DeclareTableResponse {
    pub name: String,
    pub location: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub storage_options: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct DescribeTableResponse {
    pub name: String,
    pub location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<i64>,
    pub created_at: String,
}

#[derive(Serialize)]
pub struct TableExistsResponse {
    pub exists: bool,
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/table/{id}/declare
pub async fn declare_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<LanceConfig>,
    Path(id): Path<String>,
    Json(req): Json<DeclareTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/declare", id);
    let (namespace, table) = parse_table_id(&id)?;
    validate_name(namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    let location = if let Some(ref wp) = config.warehouse_path {
        format!("{}/{}/{}/", wp.trim_end_matches('/'), namespace, table)
    } else {
        format!("lance://{}/{}", namespace, table)
    };

    // Persist storage options into properties with prefix for later retrieval.
    let mut properties = req.options;
    for (key, value) in &config.storage_options {
        properties.insert(format!("storage_{}", key), value.clone());
    }

    let asset = store
        .create_asset(
            namespace,
            AssetFormat::Lance,
            table,
            &location,
            None,
            None,
            properties,
        )
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(DeclareTableResponse {
            name: asset.name,
            location,
            storage_options: config.storage_options.clone(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/describe
pub async fn describe_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/describe", id);
    let (namespace, table) = parse_table_id(&id)?;

    // Use single query to get asset and current version
    let (asset, current_version) = store
        .get_asset_with_current_version(namespace, AssetFormat::Lance, table)
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(DescribeTableResponse {
            name: asset.name,
            location: asset.location,
            current_version: current_version.map(|v| v.version_id),
            created_at: asset.created_at.to_rfc3339(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/register
pub async fn register_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<RegisterTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/register", id);
    let (namespace, table) = parse_table_id(&id)?;
    validate_name(namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    let properties = req.options;

    let asset = store
        .create_asset(
            namespace,
            AssetFormat::Lance,
            table,
            &req.location,
            None,
            None,
            properties,
        )
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(DeclareTableResponse {
            name: asset.name,
            location: req.location,
            storage_options: HashMap::new(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/deregister
pub async fn deregister_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/deregister", id);
    let (namespace, table) = parse_table_id(&id)?;

    store
        .drop_asset(namespace, AssetFormat::Lance, table)
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok(StatusCode::OK)
}

/// POST /lance/v1/table/{id}/drop
pub async fn drop_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/drop", id);
    let (namespace, table) = parse_table_id(&id)?;

    store
        .drop_asset(namespace, AssetFormat::Lance, table)
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok(StatusCode::OK)
}

/// POST /lance/v1/table/{id}/exists
pub async fn table_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/exists", id);
    let (namespace, table) = parse_table_id(&id)?;

    let exists = store
        .asset_exists(namespace, AssetFormat::Lance, table)
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(TableExistsResponse { exists })))
}

/// POST /lance/v1/table/{id}/rename
pub async fn rename_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<RenameTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/rename", id);
    let (namespace, table) = parse_table_id(&id)?;
    validate_name(namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(&req.new_name)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    store
        .rename_asset(namespace, AssetFormat::Lance, table, &req.new_name)
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok(StatusCode::OK)
}
