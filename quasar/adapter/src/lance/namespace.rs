use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::CatalogStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{store_error_to_lance, LanceError, ProblemDetails};
use super::id::{parse_namespace_id, LanceNamespaceId};
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

impl From<quasar_core::Domain> for NamespaceResponse {
    fn from(domain: quasar_core::Domain) -> Self {
        Self {
            id: domain.id.to_string(),
            name: domain.name,
            properties: domain.properties,
            created_at: domain.created_at.to_rfc3339(),
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

impl From<(quasar_core::Asset, quasar_core::TabularAsset)> for TableResponse {
    fn from(pair: (quasar_core::Asset, quasar_core::TabularAsset)) -> Self {
        Self::from(pair.0)
    }
}

#[derive(Serialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

// ── Helpers ────────────────────────────────────────────────

/// Build a `LanceError::InvalidInput` problem when an endpoint requires a
/// Namespace-shaped id but received Root or a single-segment Domain id.
fn require_namespace_id_error(id: &str, op: &str, instance: &str) -> ProblemDetails {
    LanceError::InvalidInput {
        detail: format!(
            "operation '{op}' on namespace id '{id}' requires a \
             '{{domain}}${{namespace}}' shape; Domain management goes through the Unified API"
        ),
        instance: instance.to_string(),
    }
    .to_problem_details()
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/namespace/{id}/create
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/create", id);
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;

    let (domain, namespace) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(&id, "create", &instance));
        }
    };

    validate_name(&namespace)
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;
    let ns = store
        .create_namespace(&domain, &namespace, None, req.properties)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(NamespaceResponse::from(ns))))
}

/// GET /lance/v1/namespace/{id}/list
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(id): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let instance = format!("/lance/v1/namespace/{}/list", id);
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let namespaces: Vec<NamespaceResponse> = match parsed {
        LanceNamespaceId::Root => store
            .list_domains(offset, limit)
            .await
            .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        LanceNamespaceId::Domain(domain) => store
            .list_namespaces(&domain, offset, limit)
            .await
            .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        LanceNamespaceId::Namespace { .. } => Vec::new(),
    };

    let next_page_token = if namespaces.len() as i32 >= limit {
        Some((offset + namespaces.len() as i64).to_string())
    } else {
        None
    };

    let response = ListNamespacesResponse {
        namespaces,
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
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    let (domain, namespace) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(&id, "describe", &instance));
        }
    };

    let ns = store
        .get_namespace(&domain, &namespace)
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
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    let (domain, namespace) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(&id, "drop", &instance));
        }
    };

    store
        .drop_namespace(&domain, &namespace)
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
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    let (domain, namespace) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(&id, "list_tables", &instance));
        }
    };
    let _limit = query.limit.unwrap_or(100).clamp(1, 1000);

    let assets = store
        .list_tabular_assets(&domain, &namespace, Some("lance"))
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
    let parsed = parse_namespace_id(&id, &instance).map_err(LanceError::to_problem_details)?;
    let (domain, namespace) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(&id, "exists", &instance));
        }
    };

    let exists = store
        .namespace_exists(&domain, &namespace)
        .await
        .map_err(|e| store_error_to_lance(e, &instance).to_problem_details())?;

    Ok((StatusCode::OK, Json(ExistsResponse { exists })))
}
