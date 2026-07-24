//! Discovery handler (`GET /unified/v1/assets`): cross-namespace asset
//! discovery within a Domain (REQUIREMENTS §4.6, DESIGN §5.3).
//!
//! Query parameters: `domain` (required), `namespace_prefix`,
//! `asset_type`, `format`, `tag` (repeatable, AND semantics), `property`
//! (repeatable `key=value`, AND semantics), `include_deleted`,
//! `pageToken`, `pageSize`.
//!
//! `tag` and `property` are repeatable parameters, which
//! `serde_urlencoded` cannot represent; the raw query string is parsed
//! manually instead of using the `Query` extractor.

use axum::{
    extract::{Extension, RawQuery, State},
    response::IntoResponse,
    Json,
};
use quasar_core::{validate_name, validate_namespace_path, AssetQuery, CatalogError, CatalogStore};
use std::collections::HashMap;
use std::sync::Arc;

use super::dto::{next_page_token, AssetListItem, ListResponse, PaginationQuery};
use super::error::{map_catalog_error, UnifiedError};
use super::{registry, request_id_of};

const INSTANCE: &str = "/unified/v1/assets";

/// Parsed discovery query parameters.
#[derive(Debug, Default)]
struct DiscoveryParams {
    domain: Option<String>,
    namespace_prefix: Option<String>,
    asset_type: Option<String>,
    format: Option<String>,
    tags: Vec<String>,
    properties: HashMap<String, String>,
    include_deleted: bool,
    page_token: Option<String>,
    page_size: Option<i64>,
}

impl DiscoveryParams {
    fn parse(raw: &str) -> Result<Self, CatalogError> {
        let mut params = DiscoveryParams::default();
        for pair in raw.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode(key)?;
            let value = percent_decode(value)?;
            match key.as_str() {
                "domain" => params.domain = Some(value),
                "namespace_prefix" => params.namespace_prefix = Some(value),
                "asset_type" => params.asset_type = Some(value),
                "format" => params.format = Some(value),
                "tag" => params.tags.push(value),
                "property" => {
                    let (k, v) = value.split_once('=').ok_or_else(|| {
                        CatalogError::Validation(
                            "property parameter must be in key=value form".to_string(),
                        )
                    })?;
                    if k.is_empty() {
                        return Err(CatalogError::Validation(
                            "property parameter key must not be empty".to_string(),
                        ));
                    }
                    params.properties.insert(k.to_string(), v.to_string());
                }
                "include_deleted" => {
                    params.include_deleted = match value.as_str() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(CatalogError::Validation(format!(
                                "invalid include_deleted value '{}', expected true or false",
                                value
                            )));
                        }
                    };
                }
                "pageToken" => params.page_token = Some(value),
                "pageSize" => {
                    params.page_size = Some(value.parse::<i64>().map_err(|_| {
                        CatalogError::Validation(format!(
                            "invalid pageSize value '{}', expected an integer",
                            value
                        ))
                    })?);
                }
                // Unknown parameters are ignored.
                _ => {}
            }
        }
        Ok(params)
    }
}

/// Decode a percent-encoded query component (`+` maps to space).
fn percent_decode(input: &str) -> Result<String, CatalogError> {
    if !input.contains('%') && !input.contains('+') {
        return Ok(input.to_string());
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = input.get(i + 1..i + 3).ok_or_else(|| {
                    CatalogError::Validation("invalid percent-encoding in query string".to_string())
                })?;
                let byte = u8::from_str_radix(hex, 16).map_err(|_| {
                    CatalogError::Validation("invalid percent-encoding in query string".to_string())
                })?;
                out.push(byte);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| CatalogError::Validation("query string is not valid UTF-8".to_string()))
}

/// GET /unified/v1/assets
pub async fn query_assets(
    State(store): State<Arc<dyn CatalogStore>>,
    request_id: Option<Extension<String>>,
    RawQuery(raw): RawQuery,
) -> Result<impl IntoResponse, UnifiedError> {
    let request_id = request_id_of(request_id);
    let params = DiscoveryParams::parse(raw.as_deref().unwrap_or_default())
        .map_err(|e| map_catalog_error(e, INSTANCE, &request_id))?;

    // `domain` is required for discovery (REQUIREMENTS §4.6).
    let domain = params.domain.filter(|d| !d.is_empty()).ok_or_else(|| {
        map_catalog_error(
            CatalogError::Validation("domain query parameter is required".to_string()),
            INSTANCE,
            &request_id,
        )
    })?;
    validate_name(&domain).map_err(|e| map_catalog_error(e, INSTANCE, &request_id))?;
    if let Some(ref prefix) = params.namespace_prefix {
        validate_namespace_path(prefix).map_err(|e| map_catalog_error(e, INSTANCE, &request_id))?;
    }
    registry::ensure_asset_type_registered(
        &store,
        params.asset_type.as_deref(),
        INSTANCE,
        &request_id,
    )
    .await?;
    registry::ensure_format_registered(&store, params.format.as_deref(), INSTANCE, &request_id)
        .await?;

    let pagination = PaginationQuery {
        page_token: params.page_token,
        page_size: params.page_size,
    };
    let (offset, limit) = pagination
        .resolve()
        .map_err(|e| map_catalog_error(e, INSTANCE, &request_id))?;

    let query = AssetQuery {
        domain,
        namespace_prefix: params.namespace_prefix,
        asset_type: params.asset_type,
        format: params.format,
        tags: params.tags,
        properties: params.properties,
        include_deleted: params.include_deleted,
        offset,
        limit,
    };

    let assets = store
        .query_assets(query)
        .await
        .map_err(|e| map_catalog_error(e, INSTANCE, &request_id))?;

    let token = next_page_token(offset, &assets, limit);
    Ok(Json(ListResponse::new(
        assets.into_iter().map(AssetListItem::from).collect(),
        token,
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_repeatable_tag_and_property_params() {
        let params = DiscoveryParams::parse(
            "domain=prod&tag=gold&tag=pi&property=owner%3Dalice&property=team=ml&include_deleted=true&pageSize=50",
        )
        .unwrap();
        assert_eq!(params.domain.as_deref(), Some("prod"));
        assert_eq!(params.tags, vec!["gold".to_string(), "pi".to_string()]);
        // Percent-encoded `=` inside a property value is preserved.
        assert_eq!(params.properties.get("owner"), Some(&"alice".to_string()));
        assert_eq!(params.properties.get("team"), Some(&"ml".to_string()));
        assert!(params.include_deleted);
        assert_eq!(params.page_size, Some(50));
    }

    #[test]
    fn property_without_equals_is_validation_error() {
        assert!(matches!(
            DiscoveryParams::parse("domain=prod&property=justkey"),
            Err(CatalogError::Validation(_))
        ));
    }

    #[test]
    fn invalid_include_deleted_is_validation_error() {
        assert!(matches!(
            DiscoveryParams::parse("domain=prod&include_deleted=maybe"),
            Err(CatalogError::Validation(_))
        ));
    }

    #[test]
    fn unknown_params_are_ignored() {
        let params = DiscoveryParams::parse("domain=prod&future_param=x").unwrap();
        assert_eq!(params.domain.as_deref(), Some("prod"));
    }

    #[test]
    fn percent_decoding_handles_plus_and_hex() {
        assert_eq!(percent_decode("a+b").unwrap(), "a b");
        assert_eq!(percent_decode("a%2Fb").unwrap(), "a/b");
        assert!(percent_decode("%zz").is_err());
        assert!(percent_decode("%1").is_err());
    }
}
