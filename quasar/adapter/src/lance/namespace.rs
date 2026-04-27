use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{AssetFormat, CatalogStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance, ProblemDetails};
use quasar_core::validate_name;

// ── Request DTOs ───────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct CreateNamespaceRequest {
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Deserialize)]
pub struct DescribeNamespaceRequest {
    #[serde(default)]
    pub properties: Option<Vec<String>>,
}

#[derive(Deserialize, Default)]
pub struct ListNamespacesQuery {
    pub page_token: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Deserialize, Default)]
pub struct ListTablesQuery {
    pub page_token: Option<String>,
    pub limit: Option<i32>,
}

// ── Response DTOs ──────────────────────────────────────────

#[derive(Serialize)]
pub struct NamespaceResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    pub created_at: String,
}

impl From<quasar_core::Namespace> for NamespaceResponse {
    fn from(ns: quasar_core::Namespace) -> Self {
        Self {
            id: ns.id.to_string(),
            name: ns.name,
            properties: ns.properties,
            created_at: ns.created_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Serialize)]
pub struct ExistsResponse {
    pub exists: bool,
}

#[derive(Serialize)]
pub struct TableResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
    pub created_at: String,
}

impl From<quasar_core::Asset> for TableResponse {
    fn from(asset: quasar_core::Asset) -> Self {
        Self {
            id: asset.id.to_string(),
            name: asset.name,
            properties: asset.properties,
            created_at: asset.created_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/namespace/{id}/create
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/create", id);
    validate_name(&id).map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;
    let ns = store
        .create_namespace(&id, req.properties)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(NamespaceResponse::from(ns))))
}

/// GET /lance/v1/namespace/{id}/list
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(_id): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/list", _id);
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let namespaces = store
        .list_namespaces(offset, limit)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    let next_page_token = if namespaces.len() as i32 >= limit {
        Some((offset + namespaces.len() as i64).to_string())
    } else {
        None
    };

    let response = ListNamespacesResponse {
        namespaces: namespaces
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        next_page_token,
    };

    Ok((StatusCode::OK, Json(response)))
}

/// POST /lance/v1/namespace/{id}/describe
pub async fn describe_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/describe", id);
    let ns = store
        .get_namespace(&id)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(NamespaceResponse::from(ns))))
}

/// POST /lance/v1/namespace/{id}/drop
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/drop", id);

    // Check if namespace is empty before dropping
    let assets = store
        .list_assets(&id, AssetFormat::Lance)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    if !assets.is_empty() {
        return Err(ProblemDetails {
            error: "NamespaceNotEmpty".to_string(),
            code: 409,
            detail: format!(
                "Namespace '{}' is not empty (contains {} tables)",
                id,
                assets.len()
            ),
            instance,
        });
    }

    store
        .drop_namespace(&id)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok(StatusCode::OK)
}

/// GET /lance/v1/namespace/{id}/table/list
pub async fn list_tables(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/table/list", id);
    let _limit = query.limit.unwrap_or(100).clamp(1, 1000);

    let assets = store
        .list_assets(&id, AssetFormat::Lance)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    let response = ListTablesResponse {
        tables: assets.into_iter().map(TableResponse::from).collect(),
        next_page_token: None,
    };

    Ok((StatusCode::OK, Json(response)))
}

/// POST /lance/v1/namespace/{id}/exists
pub async fn namespace_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/exists", id);
    let exists = store
        .namespace_exists(&id)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(ExistsResponse { exists })))
}
