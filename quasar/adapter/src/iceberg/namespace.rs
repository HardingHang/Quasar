use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{
    validate_namespace_path, CatalogError, CreateNamespace, IcebergCatalogStore, NamespacePatch,
    PatchField,
};
use std::sync::Arc;

use super::dto::{
    CreateNamespaceRequest, ListNamespacesQuery, ListNamespacesResponse, NamespaceResponse,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use super::error::{catalog_error_to_iceberg_namespace, IcebergError};
use super::{
    properties_to_string_map, string_map_to_properties, validate_warehouse, IcebergConfig,
};
use axum::extract::Extension;

/// Convert a core `Namespace` into the protocol response shape: the
/// hierarchical path is split back into the protocol's `Vec<String>`.
fn namespace_response(ns: quasar_core::Namespace) -> NamespaceResponse {
    NamespaceResponse {
        namespace: ns.path.split('/').map(str::to_string).collect(),
        properties: properties_to_string_map(ns.properties),
    }
}

/// GET /iceberg/v1/{prefix}/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    // Hierarchical namespaces (D3): `parent` filters by path prefix. The
    // spec encodes nested parents with the \x1F separator; accept it and
    // normalize to the internal `/` path separator.
    let parent = query.parent.map(|p| p.replace('\u{1f}', "/"));
    if let Some(ref p) = parent {
        validate_namespace_path(p).map_err(catalog_error_to_iceberg_namespace)?;
    }

    let limit = query.page_size.unwrap_or(100).clamp(1, 1000) as u64;
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(0);

    let page = store
        .list_namespaces(&prefix, parent.as_deref(), offset, limit)
        .await
        .map_err(catalog_error_to_iceberg_namespace)?;

    let next_page_token = if page.len() as u64 >= limit {
        Some((offset + page.len() as u64).to_string())
    } else {
        None
    };

    // The prefix query includes the parent itself; list_namespaces returns
    // only its descendants.
    let namespaces = page
        .into_iter()
        .filter(|ns| Some(ns.path.as_str()) != parent.as_deref())
        .map(|ns| ns.path.split('/').map(str::to_string).collect())
        .collect();

    Ok((
        StatusCode::OK,
        Json(ListNamespacesResponse {
            namespaces,
            next_page_token,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<super::dto::WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    if req.namespace.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: "namespace array must not be empty".to_string(),
        });
    }

    // Multi-level create: intermediate nodes are created implicitly by the
    // store (DESIGN §3.2 namespaces).
    let path = req.namespace.join("/");
    validate_namespace_path(&path).map_err(catalog_error_to_iceberg_namespace)?;

    let input = CreateNamespace {
        comment: None,
        properties: string_map_to_properties(req.properties),
    };
    let ns = store
        .create_namespace(&prefix, &path, input)
        .await
        .map_err(catalog_error_to_iceberg_namespace)?;

    Ok((StatusCode::OK, Json(namespace_response(ns))))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns} (dispatched; `ns` may be hierarchical)
pub async fn get_namespace(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
) -> Result<(StatusCode, Json<NamespaceResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;
    let namespace = store
        .get_namespace(prefix, ns)
        .await
        .map_err(catalog_error_to_iceberg_namespace)?;

    Ok((StatusCode::OK, Json(namespace_response(namespace))))
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns} (dispatched)
pub async fn drop_namespace(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;

    // The store rejects non-empty namespaces (child namespaces or assets)
    // with Conflict; FR-N7 surfaces that as 409 NamespaceNotEmptyException.
    store
        .delete_namespace(prefix, ns)
        .await
        .map_err(|e| match e {
            CatalogError::Conflict(msg) => {
                IcebergError::NamespaceNotEmptyException { message: msg }
            }
            other => catalog_error_to_iceberg_namespace(other),
        })?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns} (dispatched)
pub async fn namespace_exists(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
) -> Result<StatusCode, IcebergError> {
    validate_warehouse(warehouse, config)?;
    match store.get_namespace(prefix, ns).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(CatalogError::NotFound(_)) => Err(IcebergError::NoSuchNamespaceException {
            message: format!("Namespace '{}' not found", ns),
        }),
        Err(e) => Err(catalog_error_to_iceberg_namespace(e)),
    }
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/properties (dispatched)
pub async fn update_namespace_properties(
    store: &Arc<dyn IcebergCatalogStore>,
    config: &IcebergConfig,
    prefix: &str,
    ns: &str,
    warehouse: Option<&str>,
    req: UpdateNamespacePropertiesRequest,
) -> Result<(StatusCode, Json<UpdateNamespacePropertiesResponse>), IcebergError> {
    validate_warehouse(warehouse, config)?;

    // Read pre-update namespace to distinguish removed vs missing keys and
    // to compute the merged property set (NamespacePatch replaces wholesale).
    let before = store
        .get_namespace(prefix, ns)
        .await
        .map_err(catalog_error_to_iceberg_namespace)?;
    let before_props = properties_to_string_map(before.properties);

    let mut merged = before_props.clone();
    for key in &req.removals {
        merged.remove(key);
    }
    for (key, value) in &req.updates {
        merged.insert(key.clone(), value.clone());
    }

    let patch = NamespacePatch {
        comment: PatchField::NoChange,
        properties: match string_map_to_properties(merged) {
            Some(value) => PatchField::Set(value),
            None => PatchField::Unset,
        },
    };
    let updated_ns = store
        .update_namespace(prefix, ns, patch)
        .await
        .map_err(catalog_error_to_iceberg_namespace)?;
    let after_props = properties_to_string_map(updated_ns.properties);

    let missing: Vec<String> = req
        .removals
        .iter()
        .filter(|key| !before_props.contains_key(*key))
        .cloned()
        .collect();

    let removed: Vec<String> = req
        .removals
        .into_iter()
        .filter(|key| before_props.contains_key(key) && !after_props.contains_key(key))
        .collect();

    Ok((
        StatusCode::OK,
        Json(UpdateNamespacePropertiesResponse {
            removed,
            updated: req.updates.keys().cloned().collect(),
            missing,
        }),
    ))
}
