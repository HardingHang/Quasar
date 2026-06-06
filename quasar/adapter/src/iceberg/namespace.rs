use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{CatalogStore, PatchField};
use std::sync::Arc;

use super::dto::{
    CreateNamespaceRequest, ListNamespacesQuery, ListNamespacesResponse, NamespaceResponse,
    UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse, WarehouseQuery,
};
use super::error::{store_error_to_iceberg_namespace, IcebergError};
use super::{validate_warehouse, IcebergConfig};
use axum::extract::Extension;
use quasar_core::validate_name;

/// GET /iceberg/v1/{prefix}/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<ListNamespacesQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    // V3 does not support nested namespaces; reject parent query parameter
    if query.parent.is_some() {
        return Err(IcebergError::BadRequestException {
            message: "Nested namespace is not supported in V3".to_string(),
        });
    }

    let limit = query.page_size.unwrap_or(100).clamp(1, 1000);
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let namespaces = store
        .list_namespaces(&prefix, offset, limit)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    let next_page_token = if namespaces.len() as i32 >= limit {
        Some((offset + namespaces.len() as i64).to_string())
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(ListNamespacesResponse {
            namespaces: namespaces.into_iter().map(|ns| vec![ns.name]).collect(),
            next_page_token,
        }),
    ))
}

/// POST /iceberg/v1/{prefix}/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(prefix): Path<String>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let name = req
        .namespace
        .first()
        .ok_or_else(|| IcebergError::BadRequestException {
            message: "namespace array must not be empty".to_string(),
        })?;

    validate_name(name).map_err(store_error_to_iceberg_namespace)?;

    let ns = store
        .create_namespace(&prefix, name, None, req.properties)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    Ok((
        StatusCode::OK,
        Json(NamespaceResponse {
            namespace: vec![ns.name],
            properties: ns.properties,
        }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}
pub async fn get_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let namespace = store
        .get_namespace(&prefix, &ns)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    Ok((
        StatusCode::OK,
        Json(NamespaceResponse {
            namespace: vec![namespace.name],
            properties: namespace.properties,
        }),
    ))
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let assets = store
        .list_tabular_assets(&prefix, &ns, Some("iceberg"), 0, 1000)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    if !assets.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: format!("Namespace '{}' is not empty", ns),
        });
    }

    store
        .drop_namespace(&prefix, &ns)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{ns}
/// Check if a namespace exists.
pub async fn namespace_exists(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    let exists = match store.get_namespace(&prefix, &ns).await {
        Ok(_) => true,
        Err(quasar_core::StoreError::NotFound(_)) => false,
        Err(e) => return Err(store_error_to_iceberg_namespace(e)),
    };

    if exists {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(IcebergError::NoSuchNamespaceException {
            message: format!("Namespace '{}' not found", ns),
        })
    }
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/properties
pub async fn update_namespace_properties(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, ns)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
    Json(req): Json<UpdateNamespacePropertiesRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;
    // Read pre-update namespace to distinguish removed vs missing keys.
    let before = store
        .get_namespace(&prefix, &ns)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    let updated_ns = store
        .update_namespace(
            &prefix,
            &ns,
            PatchField::Missing,
            &req.removals,
            &req.updates,
        )
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    let missing: Vec<String> = req
        .removals
        .iter()
        .filter(|key| !before.properties.contains_key(*key))
        .cloned()
        .collect();

    let removed: Vec<String> = req
        .removals
        .into_iter()
        .filter(|key| {
            before.properties.contains_key(key) && !updated_ns.properties.contains_key(key)
        })
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
