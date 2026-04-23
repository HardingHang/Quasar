use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{AssetFormat, CatalogStore};
use std::sync::Arc;

use super::dto::{
    CreateNamespaceRequest, ListNamespacesQuery, ListNamespacesResponse,
    NamespaceResponse, UpdateNamespacePropertiesRequest, UpdateNamespacePropertiesResponse,
};
use super::error::{store_error_to_iceberg_namespace, IcebergError};
use quasar_core::validate_name;

/// GET /iceberg/v1/namespaces
pub async fn list_namespaces(
    State(store): State<Arc<dyn CatalogStore>>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    let limit = query.page_size.unwrap_or(100).clamp(1, 1000);
    let offset = query
        .page_token
        .as_ref()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0);

    let namespaces = store
        .list_namespaces(AssetFormat::Iceberg, offset, limit)
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

/// POST /iceberg/v1/namespaces
pub async fn create_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Json(req): Json<CreateNamespaceRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let name = req.namespace.first().ok_or_else(|| {
        IcebergError::BadRequestException {
            message: "namespace array must not be empty".to_string(),
        }
    })?;

    validate_name(name).map_err(store_error_to_iceberg_namespace)?;

    let ns = store
        .create_namespace(name, AssetFormat::Iceberg, req.properties)
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

/// GET /iceberg/v1/namespaces/{ns}
pub async fn get_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(ns): Path<String>,
) -> Result<impl IntoResponse, IcebergError> {
    let namespace = store
        .get_namespace(&ns, AssetFormat::Iceberg)
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

/// DELETE /iceberg/v1/namespaces/{ns}
pub async fn drop_namespace(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(ns): Path<String>,
) -> Result<impl IntoResponse, IcebergError> {
    let assets = store
        .list_assets(&ns, AssetFormat::Iceberg)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    if !assets.is_empty() {
        return Err(IcebergError::BadRequestException {
            message: format!("Namespace '{}' is not empty", ns),
        });
    }

    store
        .drop_namespace(&ns, AssetFormat::Iceberg)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    Ok(StatusCode::NO_CONTENT)
}

/// POST /iceberg/v1/namespaces/{ns}/properties
pub async fn update_namespace_properties(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(ns): Path<String>,
    Json(req): Json<UpdateNamespacePropertiesRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    // Read pre-update namespace to distinguish removed vs missing keys.
    let before = store
        .get_namespace(&ns, AssetFormat::Iceberg)
        .await
        .map_err(store_error_to_iceberg_namespace)?;

    let updated_ns = store
        .update_namespace_properties(&ns, AssetFormat::Iceberg, &req.removals, &req.updates)
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
        .filter(|key| before.properties.contains_key(key) && !updated_ns.properties.contains_key(key))
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
