use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_namespace_path, AssetFilter, CatalogStore, CreateNamespace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::error::{catalog_error_to_lance, LanceError, ProblemDetails};
use super::id::{parse_namespace_id, LanceNamespaceId};
use super::{properties_to_string_map, request_id_of, string_map_to_properties};

// ── Request DTOs ───────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct CreateNamespaceRequest {
    #[serde(default)]
    pub properties: HashMap<String, String>,
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
            // Hierarchical namespaces are identified by their full path.
            name: ns.path,
            properties: properties_to_string_map(ns.properties),
            created_at: ns.created_at.to_rfc3339(),
        }
    }
}

impl From<quasar_core::Domain> for NamespaceResponse {
    fn from(domain: quasar_core::Domain) -> Self {
        Self {
            id: domain.id.to_string(),
            name: domain.name,
            properties: properties_to_string_map(domain.properties),
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
            properties: properties_to_string_map(asset.properties),
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

// ── Helpers ────────────────────────────────────────────────

/// Build a `LanceError::InvalidInput` problem when an endpoint requires a
/// Namespace-shaped id but received Root or a single-segment Domain id.
fn require_namespace_id_error(
    id: &str,
    op: &str,
    instance: &str,
    request_id: &str,
) -> ProblemDetails {
    LanceError::InvalidInput {
        detail: format!(
            "operation '{op}' on namespace id '{id}' requires a \
             '{{domain}}${{namespace}}' shape; Domain management goes through the Unified API"
        ),
        instance: instance.to_string(),
    }
    .to_problem_details(request_id)
}

/// Resolve `(offset, limit)` from the Lance list query parameters.
fn pagination(query_limit: Option<i32>, page_token: Option<&String>) -> (u64, u64) {
    let limit = query_limit.unwrap_or(100).clamp(1, 1000) as u64;
    let offset = page_token.and_then(|t| t.parse::<u64>().ok()).unwrap_or(0);
    (offset, limit)
}

/// Next page token: produced only when the page is full (DESIGN §5.4).
fn next_page_token<T>(offset: u64, page: &[T], limit: u64) -> Option<String> {
    if page.len() as u64 >= limit {
        Some((offset + page.len() as u64).to_string())
    } else {
        None
    }
}

// ── Handlers ───────────────────────────────────────────────

/// POST /lance/v1/namespace/{id}/create
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/create", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;

    let (domain, path) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(
                &id,
                "create",
                &instance,
                &request_id,
            ));
        }
    };

    validate_namespace_path(&path)
        .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?;
    // Missing intermediate nodes are created implicitly by the store.
    let input = CreateNamespace {
        comment: None,
        properties: string_map_to_properties(req.properties),
    };
    let ns = store
        .create_namespace(&domain, &path, input)
        .await
        .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?;

    Ok((StatusCode::OK, Json(NamespaceResponse::from(ns))))
}

/// GET /lance/v1/namespace/{id}/list
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/list", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    let (offset, limit) = pagination(query.limit, query.page_token.as_ref());

    let namespaces: Vec<NamespaceResponse> = match parsed {
        LanceNamespaceId::Root => store
            .list_domains(offset, limit)
            .await
            .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        LanceNamespaceId::Domain(domain) => store
            .list_namespaces(&domain, None, offset, limit)
            .await
            .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?
            .into_iter()
            .map(NamespaceResponse::from)
            .collect(),
        LanceNamespaceId::Namespace { .. } => Vec::new(),
    };

    let token = next_page_token(offset, &namespaces, limit);
    let response = ListNamespacesResponse {
        namespaces,
        next_page_token: token,
    };

    Ok((StatusCode::OK, Json(response)))
}

/// POST /lance/v1/namespace/{id}/describe
pub async fn describe_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/describe", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    let (domain, path) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(
                &id,
                "describe",
                &instance,
                &request_id,
            ));
        }
    };

    let ns = store
        .get_namespace(&domain, &path)
        .await
        .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?;

    Ok((StatusCode::OK, Json(NamespaceResponse::from(ns))))
}

/// POST /lance/v1/namespace/{id}/drop
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/drop", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    let (domain, path) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(
                &id,
                "drop",
                &instance,
                &request_id,
            ));
        }
    };

    store
        .delete_namespace(&domain, &path)
        .await
        .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?;

    Ok(StatusCode::OK)
}

/// GET /lance/v1/namespace/{id}/table/list
pub async fn list_tables(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/table/list", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    let (domain, path) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(
                &id,
                "list_tables",
                &instance,
                &request_id,
            ));
        }
    };
    let (offset, limit) = pagination(query.limit, query.page_token.as_ref());

    // Endpoint-level protocol isolation: only `lance`-format tables are
    // visible through the Lance API.
    let filter = AssetFilter {
        domain: Some(domain),
        namespace: Some(path),
        asset_type: Some("table".to_string()),
        format: Some("lance".to_string()),
        ..AssetFilter::default()
    };
    let assets = store
        .list_assets(filter, offset, limit)
        .await
        .map_err(|e| catalog_error_to_lance(e, &instance).to_problem_details(&request_id))?;

    let token = next_page_token(offset, &assets, limit);
    let response = ListTablesResponse {
        tables: assets.into_iter().map(TableResponse::from).collect(),
        next_page_token: token,
    };

    Ok((StatusCode::OK, Json(response)))
}

/// POST /lance/v1/namespace/{id}/exists
pub async fn namespace_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ProblemDetails> {
    let request_id = request_id_of(request_id);
    let instance = format!("/lance/v1/namespace/{}/exists", id);
    let parsed =
        parse_namespace_id(&id, &instance).map_err(|e| e.to_problem_details(&request_id))?;
    let (domain, path) = match parsed {
        LanceNamespaceId::Namespace { domain, namespace } => (domain, namespace),
        LanceNamespaceId::Root | LanceNamespaceId::Domain(_) => {
            return Err(require_namespace_id_error(
                &id,
                "exists",
                &instance,
                &request_id,
            ));
        }
    };

    let exists = match store.get_namespace(&domain, &path).await {
        Ok(_) => true,
        Err(quasar_core::CatalogError::NotFound(_)) => false,
        Err(e) => {
            return Err(catalog_error_to_lance(e, &instance).to_problem_details(&request_id));
        }
    };

    Ok((StatusCode::OK, Json(ExistsResponse { exists })))
}
