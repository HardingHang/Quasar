use axum::{
    extract::{Extension, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_name, AssetFormat, CatalogStore};
use std::str::FromStr;
use std::sync::Arc;

use super::dto::{
    AssetListItem, AssetListQuery, AssetResponse, CurrentVersionResponse, ListAssetsResponse,
    PageSizeError, PaginationQuery, RenameAssetRequest, UpdateAssetRequest,
};
use super::error::{map_asset_error, UnifiedError, UnifiedErrorCode};

const NAMESPACES_PREFIX: &str = "/unified/v1/domains";

fn list_assets_instance(domain: &str, ns: &str) -> String {
    format!("{}/{}/namespaces/{}/assets", NAMESPACES_PREFIX, domain, ns)
}

fn asset_instance(domain: &str, ns: &str, name: &str) -> String {
    format!(
        "{}/{}/namespaces/{}/assets/{}",
        NAMESPACES_PREFIX, domain, ns, name
    )
}

fn rename_instance(domain: &str, ns: &str, name: &str) -> String {
    format!(
        "{}/{}/namespaces/{}/assets/{}/rename",
        NAMESPACES_PREFIX, domain, ns, name
    )
}

/// Parse and validate the optional `format` query parameter for list endpoints.
///
/// V3 list endpoints continue to accept `?format=` as an optional filter
/// (`docs/v3/V3_DESIGN.md` §4.4). Single-asset endpoints (GET/DELETE/PATCH/
/// rename) no longer require or accept the parameter — the unique active
/// asset name within a namespace makes format redundant for identity.
fn parse_optional_format(
    format: Option<String>,
    instance: &str,
    request_id: &str,
) -> Result<Option<AssetFormat>, UnifiedError> {
    match format {
        None => Ok(None),
        Some(ref s) if s.is_empty() => Err(UnifiedError::new(
            UnifiedErrorCode::InvalidFormat,
            "format query parameter cannot be empty",
            instance,
            request_id,
        )),
        Some(s) => AssetFormat::from_str(&s).map(Some).map_err(|_| {
            UnifiedError::new(
                UnifiedErrorCode::InvalidFormat,
                format!("invalid format '{}', expected 'iceberg' or 'lance'", s),
                instance,
                request_id,
            )
        }),
    }
}

/// Convert a stored `tabular.format` string into the typed `AssetFormat`
/// enum that the version helper expects. An unrecognised format from
/// storage is an internal data integrity issue; surface it as 500.
fn tabular_format_for_version(
    tabular_format: &str,
    instance: &str,
    request_id: &str,
) -> Result<AssetFormat, UnifiedError> {
    AssetFormat::from_str(tabular_format).map_err(|_| {
        tracing::error!(
            tabular_format = %tabular_format,
            "unified asset has unrecognised tabular format"
        );
        UnifiedError::new(
            UnifiedErrorCode::InternalError,
            "An internal error occurred",
            instance,
            request_id,
        )
    })
}

/// Convert (Asset, Option<TabularAsset>) to AssetListItem.
/// Non-tabular assets surface as `format: None, location: None,
/// metadata_location: None` (V3_DESIGN §4.4 / §6).
fn asset_pair_to_list_item(
    (asset, tabular): (quasar_core::Asset, Option<quasar_core::TabularAsset>),
) -> AssetListItem {
    let (format, location, metadata_location) = match tabular {
        Some(t) => (Some(t.format), Some(t.location), t.metadata_location),
        None => (None, None, None),
    };
    AssetListItem {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type,
        format,
        location,
        metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// Convert (Asset, Option<TabularAsset>) to AssetResponse with current version.
fn asset_pair_to_response(
    (asset, tabular): (quasar_core::Asset, Option<quasar_core::TabularAsset>),
    current_version: Option<CurrentVersionResponse>,
) -> AssetResponse {
    let (format, location, metadata_location) = match tabular {
        Some(t) => (Some(t.format), Some(t.location), t.metadata_location),
        None => (None, None, None),
    };
    AssetResponse {
        id: asset.id.to_string(),
        name: asset.name,
        asset_type: asset.asset_type,
        format,
        location,
        metadata_location,
        comment: asset.comment,
        properties: asset.properties,
        current_version,
        created_at: asset.created_at.to_rfc3339(),
    }
}

/// GET /unified/v1/domains/{domain}/namespaces/{ns}/assets
pub async fn list_assets(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns)): Path<(String, String)>,
    Query(query): Query<AssetListQuery>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = list_assets_instance(&domain, &ns);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let page_size = query
        .resolved_page_size()
        .map_err(|e| map_page_size_error(e, &instance, request_id.as_str()))?;
    let offset = query.resolved_offset().map_err(|_| {
        UnifiedError::new(
            UnifiedErrorCode::InvalidPageToken,
            "invalid page token",
            instance.clone(),
            request_id.clone(),
        )
    })?;

    let format = parse_optional_format(query.format, &instance, &request_id)?;
    let format_str = format.map(|f| f.as_str());

    let name_filter = match query.name {
        Some(name) => {
            validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
            Some(name)
        }
        None => None,
    };

    let rows = store
        .list_assets_unified(
            &domain,
            &ns,
            format_str,
            name_filter.as_deref(),
            offset,
            page_size,
        )
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let next_page_token = if rows.len() as i32 >= page_size {
        Some(super::dto::PaginationQuery::encode_token(
            offset + rows.len() as i64,
        ))
    } else {
        None
    };

    let assets: Vec<AssetListItem> = rows
        .into_iter()
        .map(|(asset, tabular)| asset_pair_to_list_item((asset, tabular)))
        .collect();

    Ok((
        StatusCode::OK,
        Json(ListAssetsResponse {
            assets,
            next_page_token,
        }),
    ))
}

/// GET /unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}
pub async fn get_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns, name)): Path<(String, String, String)>,
    Extension(config): Extension<super::UnifiedConfig>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = asset_instance(&domain, &ns, &name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let raw = store
        .get_asset_unified(&domain, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let (asset, tabular) = raw;
    let current_version = if let Some(ref t) = tabular {
        let format = tabular_format_for_version(&t.format, &instance, &request_id)?;
        super::version::get_current_version(
            store.as_ref(),
            &config,
            &domain,
            &ns,
            &name,
            format,
            t.metadata_location.as_deref(),
            &instance,
            &request_id,
        )
        .await?
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(asset_pair_to_response((asset, tabular), current_version)),
    ))
}

/// DELETE /unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}
pub async fn drop_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns, name)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = asset_instance(&domain, &ns, &name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    store
        .drop_asset(&domain, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok(StatusCode::NO_CONTENT)
}

/// PATCH /unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}
pub async fn update_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns, name)): Path<(String, String, String)>,
    Extension(config): Extension<super::UnifiedConfig>,
    Json(req): Json<UpdateAssetRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = asset_instance(&domain, &ns, &name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    store
        .update_asset(
            &domain,
            &ns,
            &name,
            req.comment,
            &req.removals,
            &req.updates,
        )
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    // Re-fetch to get full asset + tabular for response
    let raw = store
        .get_asset_unified(&domain, &ns, &name)
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    let (asset, tabular) = raw;
    let current_version = if let Some(ref t) = tabular {
        let format = tabular_format_for_version(&t.format, &instance, &request_id)?;
        super::version::get_current_version(
            store.as_ref(),
            &config,
            &domain,
            &ns,
            &name,
            format,
            t.metadata_location.as_deref(),
            &instance,
            &request_id,
        )
        .await?
    } else {
        None
    };

    Ok((
        StatusCode::OK,
        Json(asset_pair_to_response((asset, tabular), current_version)),
    ))
}

/// POST /unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}/rename
pub async fn rename_asset(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(request_id): Extension<String>,
    Path((domain, ns, name)): Path<(String, String, String)>,
    Json(req): Json<RenameAssetRequest>,
) -> Result<impl IntoResponse, UnifiedError> {
    let instance = rename_instance(&domain, &ns, &name);
    validate_name(&ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&name).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    validate_name(&req.new_name).map_err(|e| map_asset_error(e, &instance, &request_id))?;

    if let Some(ref new_ns) = req.new_namespace {
        validate_name(new_ns).map_err(|e| map_asset_error(e, &instance, &request_id))?;
    }

    store
        .rename_asset(
            &domain,
            &ns,
            &name,
            &req.new_name,
            req.new_namespace.as_deref(),
        )
        .await
        .map_err(|e| map_asset_error(e, &instance, &request_id))?;

    Ok(StatusCode::OK)
}

/// POST /unified/v1/domains/{domain}/namespaces/{ns}/assets is intentionally
/// not supported in V3.
pub async fn create_asset_not_allowed(
    Extension(request_id): Extension<String>,
    Path((domain, ns)): Path<(String, String)>,
) -> Result<StatusCode, UnifiedError> {
    let instance = list_assets_instance(&domain, &ns);
    Err(UnifiedError::new(
        UnifiedErrorCode::MethodNotAllowed,
        "Unified Asset creation is not supported in V3",
        instance,
        request_id,
    ))
}

fn map_page_size_error(err: PageSizeError, instance: &str, request_id: &str) -> UnifiedError {
    match err {
        PageSizeError::Invalid => UnifiedError::new(
            UnifiedErrorCode::InvalidInput,
            "pageSize must be greater than 0",
            instance,
            request_id,
        ),
        PageSizeError::TooLarge => UnifiedError::new(
            UnifiedErrorCode::PageSizeTooLarge,
            format!(
                "pageSize must not exceed {}",
                PaginationQuery::MAX_PAGE_SIZE
            ),
            instance,
            request_id,
        ),
    }
}
