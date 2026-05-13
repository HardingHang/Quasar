use axum::{
    extract::{Extension, Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{CatalogStore, StoreError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance_table, LanceError, ProblemDetails};
use super::id::parse_table_id;
use super::LanceConfig;
use quasar_core::validate_name;

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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    validate_name(&parsed.namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(&parsed.table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    let location = if let Some(ref wp) = config.warehouse_path {
        format!(
            "{}/{}/{}/",
            wp.trim_end_matches('/'),
            parsed.namespace,
            parsed.table
        )
    } else {
        format!("lance://{}/{}", parsed.namespace, parsed.table)
    };

    // Persist storage options into properties with prefix for later retrieval.
    let mut properties = req.options;
    for (key, value) in &config.storage_options {
        properties.insert(format!("storage_{}", key), value.clone());
    }

    let (asset, _tabular) = store
        .create_tabular_asset(
            &parsed.domain,
            &parsed.namespace,
            &parsed.table,
            "lance",
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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    // Use single query to get asset and current version
    let (asset, tabular, current_version) = store
        .get_tabular_asset_with_current_version(
            &parsed.domain,
            &parsed.namespace,
            "lance",
            &parsed.table,
        )
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok((
        StatusCode::OK,
        Json(DescribeTableResponse {
            name: asset.name,
            location: tabular.location,
            current_version: current_version.and_then(|(v, _)| v.version_order),
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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    validate_name(&parsed.namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(&parsed.table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    let properties = req.options;

    let (asset, _tabular) = store
        .create_tabular_asset(
            &parsed.domain,
            &parsed.namespace,
            &parsed.table,
            "lance",
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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    store
        .drop_asset(&parsed.domain, &parsed.namespace, &parsed.table)
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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    store
        .drop_asset(&parsed.domain, &parsed.namespace, &parsed.table)
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
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    // V3 stores the format on `tabular_assets.format`; an Iceberg-format
    // asset under the same name still satisfies `asset_exists`, so we
    // ask the format-filtered lookup instead.
    let exists = match store
        .get_tabular_asset(&parsed.domain, &parsed.namespace, "lance", &parsed.table)
        .await
    {
        Ok(_) => true,
        Err(StoreError::NotFound(_)) => false,
        Err(e) => return Err(store_error_to_lance_table(e, &instance).to_problem_details()),
    };

    Ok((StatusCode::OK, Json(TableExistsResponse { exists })))
}

/// POST /lance/v1/table/{id}/rename
pub async fn rename_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<RenameTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/table/{}/rename", id);
    let parsed = parse_table_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    validate_name(&parsed.namespace)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(&parsed.table)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;
    validate_name(&req.new_name)
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    store
        .rename_asset(
            &parsed.domain,
            &parsed.namespace,
            &parsed.table,
            &req.new_name,
        )
        .await
        .map_err(|e| store_error_to_lance_table(e, &instance).to_problem_details())?;

    Ok(StatusCode::OK)
}
