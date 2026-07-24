use axum::{
    extract::{Extension, Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use quasar_core::{
    validate_name, validate_namespace_path, Asset, CatalogError, CatalogStore, CreateAsset,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{catalog_error_to_lance_table, LanceError, ProblemDetails};
use super::id::{parse_table_id, LanceTableId};
use super::{request_id_of, string_map_to_properties, LanceConfig};

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
    pub new_table_name: String,
    pub new_namespace_id: Option<Vec<String>>,
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

// ── Helpers ────────────────────────────────────────────────

/// Resolve a Lance table by protocol id. Endpoint-level protocol
/// isolation (DESIGN §5.2): a same-named asset without the `lance` format
/// (e.g. an Iceberg table) is invisible through Lance endpoints and must
/// surface as TableNotFound.
pub(crate) async fn lance_table_by_name(
    store: &Arc<dyn CatalogStore>,
    parsed: &LanceTableId,
    instance: &str,
) -> Result<Asset, LanceError> {
    let asset = store
        .get_asset_by_name(&parsed.domain, &parsed.namespace, &parsed.table)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, instance))?;
    if asset.format.as_deref() != Some("lance") {
        return Err(LanceError::TableNotFound {
            name: parsed.table.clone(),
            instance: instance.to_string(),
        });
    }
    Ok(asset)
}

/// Parse a Lance native version key back into its numeric form; version
/// keys written by this adapter are always numeric, so a parse failure is
/// an internal inconsistency.
pub(crate) fn parse_version_key(
    version_key: &str,
    asset: &Asset,
    instance: &str,
) -> Result<i64, LanceError> {
    version_key.parse::<i64>().map_err(|_| {
        tracing::error!(asset_id = %asset.id, %version_key,
            "lance version key is not numeric");
        LanceError::InternalError {
            detail: "An internal error occurred".to_string(),
            instance: instance.to_string(),
        }
    })
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/table/{id}/declare
pub async fn declare_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Extension(config): Extension<LanceConfig>,
    Path(id): Path<String>,
    Json(req): Json<DeclareTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/declare", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    validate_namespace_path(&parsed.namespace)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;
    validate_name(&parsed.table)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

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

    let input = CreateAsset {
        domain: parsed.domain,
        namespace: parsed.namespace,
        name: parsed.table,
        asset_type: "table".to_string(),
        format: Some("lance".to_string()),
        comment: None,
        properties: string_map_to_properties(properties),
    };
    // A declared table has no committed manifest yet: metadata_location
    // stays None until the first mirrored version.
    let created = store
        .create_tabular_asset(input, &location, None)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok((
        StatusCode::OK,
        Json(DeclareTableResponse {
            name: created.asset.name,
            location,
            storage_options: config.storage_options.clone(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/describe
pub async fn describe_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/describe", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    // The location lives on the tabular extension row; a Lance table
    // without one is an internal inconsistency (the asset itself exists).
    let tabular = store
        .get_tabular_asset(asset.id)
        .await
        .map_err(|e| match e {
            CatalogError::NotFound(_) => {
                tracing::error!(asset_id = %asset.id,
                    "lance table asset missing tabular extension row");
                LanceError::InternalError {
                    detail: "An internal error occurred".to_string(),
                    instance: instance.clone(),
                }
            }
            other => catalog_error_to_lance_table(other, &instance),
        })
        .map_err(|e| e.to_problem_details(&request_id))?;

    // The current version is tracked via assets.current_version_key; a
    // table without any mirrored version has no current version.
    let current_version = match store.get_latest_version(asset.id).await {
        Ok(version) => Some(
            parse_version_key(&version.version_key, &asset, &instance)
                .map_err(|e| e.to_problem_details(&request_id))?,
        ),
        Err(CatalogError::NotFound(_)) => None,
        Err(e) => {
            return Err(catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id));
        }
    };

    Ok((
        StatusCode::OK,
        Json(DescribeTableResponse {
            name: asset.name,
            location: tabular.location,
            current_version,
            created_at: asset.created_at.to_rfc3339(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/register
pub async fn register_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Json(req): Json<RegisterTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/register", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    validate_namespace_path(&parsed.namespace)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;
    validate_name(&parsed.table)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    let input = CreateAsset {
        domain: parsed.domain,
        namespace: parsed.namespace,
        name: parsed.table,
        asset_type: "table".to_string(),
        format: Some("lance".to_string()),
        comment: None,
        properties: string_map_to_properties(req.options),
    };
    // A registered table already exists externally; its location also
    // serves as the initial metadata_location pointer.
    let created = store
        .create_tabular_asset(input, &req.location, Some(&req.location))
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok((
        StatusCode::OK,
        Json(DeclareTableResponse {
            name: created.asset.name,
            location: req.location,
            storage_options: HashMap::new(),
        }),
    ))
}

/// POST /lance/v1/table/{id}/deregister
pub async fn deregister_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/deregister", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    store
        .soft_delete_asset(asset.id)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok(StatusCode::OK)
}

/// POST /lance/v1/table/{id}/drop
pub async fn drop_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/drop", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    store
        .soft_delete_asset(asset.id)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok(StatusCode::OK)
}

/// POST /lance/v1/table/{id}/exists
pub async fn table_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/exists", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    // Endpoint-level protocol isolation: a same-named asset with another
    // format does not count as an existing Lance table.
    let exists = match store
        .get_asset_by_name(&parsed.domain, &parsed.namespace, &parsed.table)
        .await
    {
        Ok(asset) => asset.format.as_deref() == Some("lance"),
        Err(CatalogError::NotFound(_)) => false,
        Err(e) => {
            return Err(catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id));
        }
    };

    Ok((StatusCode::OK, Json(TableExistsResponse { exists })))
}

/// POST /lance/v1/table/{id}/rename
pub async fn rename_table(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Json(req): Json<RenameTableRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/table/{}/rename", id);
    let parsed = parse_table_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    validate_namespace_path(&parsed.namespace)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;
    validate_name(&parsed.table)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;
    validate_name(&req.new_table_name)
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    // Rename is id-addressed in the baseline contract and cannot move a
    // table across namespaces; a new_namespace_id is accepted only when it
    // names the current namespace.
    if let Some(ref segments) = req.new_namespace_id {
        if segments.len() < 2 {
            return Err(LanceError::InvalidInput {
                detail: format!(
                    "new_namespace_id must start with the domain followed by the \
                     namespace path segments, got {} segment(s)",
                    segments.len()
                ),
                instance: instance.clone(),
            }
            .to_problem_details(&request_id));
        }
        if segments[0] != parsed.domain {
            return Err(LanceError::InvalidInput {
                detail: "cross-domain rename not supported".to_string(),
                instance: instance.clone(),
            }
            .to_problem_details(&request_id));
        }
        let new_namespace = segments[1..].join("/");
        validate_namespace_path(&new_namespace).map_err(|e| {
            catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id)
        })?;
        if new_namespace != parsed.namespace {
            return Err(LanceError::InvalidInput {
                detail: "cross-namespace rename not supported".to_string(),
                instance: instance.clone(),
            }
            .to_problem_details(&request_id));
        }
    }

    // Endpoint-level protocol isolation: only Lance-format assets are
    // visible through the Lance rename endpoint.
    let asset = lance_table_by_name(&store, &parsed, &instance)
        .await
        .map_err(|e| e.to_problem_details(&request_id))?;

    store
        .rename_asset(asset.id, &req.new_table_name, None)
        .await
        .map_err(|e| catalog_error_to_lance_table(e, &instance).to_problem_details(&request_id))?;

    Ok(StatusCode::OK)
}
