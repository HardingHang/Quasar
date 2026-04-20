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

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/namespace/{id}/create
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/create", id);
    let ns = store
        .create_namespace(&id, AssetFormat::Lance, req.properties)
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
        .list_namespaces(AssetFormat::Lance, offset, limit)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    let next_page_token = if namespaces.len() as i32 >= limit {
        Some((offset + namespaces.len() as i64).to_string())
    } else {
        None
    };

    let response = ListNamespacesResponse {
        namespaces: namespaces.into_iter().map(NamespaceResponse::from).collect(),
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
        .get_namespace(&id, AssetFormat::Lance)
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
    store
        .drop_namespace(&id, AssetFormat::Lance)
        .await
        .map_err(|e| {
            let lance_err = store_error_to_lance(e, &instance);
            lance_err.to_problem_details()
        })?;

    Ok(StatusCode::OK)
}

/// POST /lance/v1/namespace/{id}/exists
pub async fn namespace_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/exists", id);
    let exists = store
        .namespace_exists(&id, AssetFormat::Lance)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(ExistsResponse { exists })))
}
