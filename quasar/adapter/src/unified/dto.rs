//! Request/response DTOs and token pagination for the Unified API
//! (REQUIREMENTS §6.2, DESIGN §5.3 / §5.4).

use quasar_core::{Asset, AssetVersion, CatalogError, Domain, Namespace, PatchField};
use serde::{Deserialize, Serialize};

/// Default page size when `pageSize` is absent (DESIGN §5.4).
pub const DEFAULT_PAGE_SIZE: u64 = 100;
/// Server-side upper bound; larger `pageSize` values are clamped, not rejected.
pub const MAX_PAGE_SIZE: u64 = 1000;

/// `?pageToken=&pageSize=` query parameters shared by all list endpoints.
#[derive(Debug, Deserialize, Default)]
pub struct PaginationQuery {
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i64>,
}

impl PaginationQuery {
    /// Resolve `(offset, limit)`: `pageSize` defaults to 100, clamps to
    /// 1000, and must be positive (`VALIDATION_FAILED` otherwise); the
    /// token is an offset encoding.
    pub fn resolve(&self) -> Result<(u64, u64), CatalogError> {
        let limit = match self.page_size {
            None => DEFAULT_PAGE_SIZE,
            Some(size) if size <= 0 => {
                return Err(CatalogError::Validation(
                    "pageSize must be greater than 0".to_string(),
                ));
            }
            Some(size) => (size as u64).min(MAX_PAGE_SIZE),
        };
        let offset = match &self.page_token {
            None => 0,
            Some(token) => token
                .parse::<u64>()
                .map_err(|_| CatalogError::Validation("invalid page token".to_string()))?,
        };
        Ok((offset, limit))
    }
}

/// Compute the next page token (`offset + page count`), produced only when
/// the returned page is full (DESIGN §5.4); otherwise there is no next page.
pub fn next_page_token<T>(offset: u64, page: &[T], limit: u64) -> Option<String> {
    if page.len() as u64 == limit {
        Some((offset + page.len() as u64).to_string())
    } else {
        None
    }
}

/// Generic list envelope: `{ "items": [...], "next_page_token": ... }`;
/// `next_page_token` is omitted when there is no next page.
#[derive(Debug, Serialize)]
pub struct ListResponse<T: Serialize> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl<T: Serialize> ListResponse<T> {
    pub fn new(items: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            items,
            next_page_token,
        }
    }
}

// ── Domain DTOs ─────────────────────────────────────────────────

/// POST /unified/v1/domains
#[derive(Debug, Deserialize)]
pub struct CreateDomainRequest {
    pub name: String,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: Option<serde_json::Value>,
    #[serde(default)]
    pub storage_type: Option<String>,
    /// May carry secret references; never echoed back (see `DomainResponse`).
    #[serde(default)]
    pub storage_config: Option<serde_json::Value>,
    #[serde(default)]
    pub warehouse: Option<String>,
}

/// PATCH /unified/v1/domains/{domain}
#[derive(Debug, Deserialize, Default)]
pub struct UpdateDomainRequest {
    #[serde(default)]
    pub comment: PatchField<String>,
    #[serde(default)]
    pub properties: PatchField<serde_json::Value>,
    #[serde(default)]
    pub storage_type: PatchField<String>,
    #[serde(default)]
    pub storage_config: PatchField<serde_json::Value>,
    #[serde(default)]
    pub warehouse: PatchField<String>,
}

/// Domain response body. `storage_config` is structurally redacted: the
/// field is never serialized because it may contain credentials or secret
/// references (sensitive-field red line).
#[derive(Debug, Serialize)]
pub struct DomainResponse {
    pub id: String,
    pub name: String,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub storage_type: Option<String>,
    pub warehouse: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Domain> for DomainResponse {
    fn from(domain: Domain) -> Self {
        Self {
            id: domain.id.to_string(),
            name: domain.name,
            comment: domain.comment,
            properties: domain.properties,
            storage_type: domain.storage_type,
            warehouse: domain.warehouse,
            created_at: domain.created_at.to_rfc3339(),
            updated_at: domain.updated_at.to_rfc3339(),
        }
    }
}

// ── Namespace DTOs ──────────────────────────────────────────────

/// POST /unified/v1/domains/{domain}/namespaces
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    /// Hierarchical path, e.g. `analytics/teams/finance`; missing
    /// intermediate nodes are created implicitly (FR-N2).
    pub path: String,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: Option<serde_json::Value>,
}

/// PATCH /unified/v1/domains/{domain}/namespaces/{namespace}
#[derive(Debug, Deserialize, Default)]
pub struct UpdateNamespaceRequest {
    #[serde(default)]
    pub comment: PatchField<String>,
    #[serde(default)]
    pub properties: PatchField<serde_json::Value>,
}

/// Namespace response body.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    pub id: String,
    pub domain_id: String,
    pub path: String,
    pub depth: i32,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Namespace> for NamespaceResponse {
    fn from(ns: Namespace) -> Self {
        Self {
            id: ns.id.to_string(),
            domain_id: ns.domain_id.to_string(),
            path: ns.path,
            depth: ns.depth,
            comment: ns.comment,
            properties: ns.properties,
            created_at: ns.created_at.to_rfc3339(),
            updated_at: ns.updated_at.to_rfc3339(),
        }
    }
}

/// Query parameters for `GET /unified/v1/domains/{domain}/namespaces`:
/// pagination plus an optional hierarchical path prefix filter (FR-N5).
#[derive(Debug, Deserialize, Default)]
pub struct NamespaceListQuery {
    #[serde(flatten)]
    pub pagination: PaginationQuery,
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Query parameters for the namespace wildcard route
/// (`GET .../namespaces/{*path}`): pagination plus asset list filters
/// (`asset_type` / `format`), used only when the path targets the assets
/// collection.
#[derive(Debug, Deserialize, Default)]
pub struct NamespacePathQuery {
    #[serde(flatten)]
    pub pagination: PaginationQuery,
    #[serde(default)]
    pub asset_type: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
}

// ── Asset DTOs ──────────────────────────────────────────────────

/// Asset list item; the current version is not embedded in list views.
#[derive(Debug, Serialize)]
pub struct AssetListItem {
    pub id: String,
    pub namespace_id: String,
    pub name: String,
    pub asset_type: String,
    pub format: Option<String>,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub current_version_key: Option<String>,
    pub deleted_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Asset> for AssetListItem {
    fn from(asset: Asset) -> Self {
        Self {
            id: asset.id.to_string(),
            namespace_id: asset.namespace_id.to_string(),
            name: asset.name,
            asset_type: asset.asset_type,
            format: asset.format,
            comment: asset.comment,
            properties: asset.properties,
            current_version_key: asset.current_version_key,
            deleted_at: asset.deleted_at.map(|ts| ts.to_rfc3339()),
            created_at: asset.created_at.to_rfc3339(),
            updated_at: asset.updated_at.to_rfc3339(),
        }
    }
}

/// Asset detail response; embeds the current version fetched from the
/// database via `VersionStore::get_latest_version` (never from the object
/// store).
#[derive(Debug, Serialize)]
pub struct AssetResponse {
    pub id: String,
    pub namespace_id: String,
    pub name: String,
    pub asset_type: String,
    pub format: Option<String>,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub current_version_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_version: Option<VersionResponse>,
    pub deleted_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl AssetResponse {
    pub fn from_asset(asset: Asset, current_version: Option<VersionResponse>) -> Self {
        Self {
            id: asset.id.to_string(),
            namespace_id: asset.namespace_id.to_string(),
            name: asset.name,
            asset_type: asset.asset_type,
            format: asset.format,
            comment: asset.comment,
            properties: asset.properties,
            current_version_key: asset.current_version_key,
            current_version,
            deleted_at: asset.deleted_at.map(|ts| ts.to_rfc3339()),
            created_at: asset.created_at.to_rfc3339(),
            updated_at: asset.updated_at.to_rfc3339(),
        }
    }
}

// ── Version DTOs ────────────────────────────────────────────────

/// Version response body (read-only; versions are mirrored by native
/// protocol adapters).
#[derive(Debug, Serialize)]
pub struct VersionResponse {
    pub id: String,
    pub asset_id: String,
    pub version_key: String,
    pub version_properties: Option<serde_json::Value>,
    pub content_inline: Option<serde_json::Value>,
    pub content_pointer: Option<String>,
    pub previous_version_id: Option<String>,
    pub created_at: String,
}

impl From<AssetVersion> for VersionResponse {
    fn from(version: AssetVersion) -> Self {
        Self {
            id: version.id.to_string(),
            asset_id: version.asset_id.to_string(),
            version_key: version.version_key,
            version_properties: version.version_properties,
            content_inline: version.content_inline,
            content_pointer: version.content_pointer,
            previous_version_id: version.previous_version_id.map(|id| id.to_string()),
            created_at: version.created_at.to_rfc3339(),
        }
    }
}

// ── Tag DTOs ────────────────────────────────────────────────────

/// POST /unified/v1/assets/{asset_id}/tags
#[derive(Debug, Deserialize)]
pub struct AddTagRequest {
    pub tag: String,
}

/// Tag list response body.
#[derive(Debug, Serialize)]
pub struct TagsResponse {
    pub tags: Vec<String>,
}

// ── AssetType / Format DTOs ─────────────────────────────────────

/// POST /unified/v1/asset-types
#[derive(Debug, Deserialize)]
pub struct RegisterAssetTypeRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Logical grouping; must be one of the known categories.
    pub category: String,
    #[serde(default)]
    pub validation_schema: Option<serde_json::Value>,
    /// `jsonb` / `dedicated_table` / `reference_only`.
    pub extension_strategy: String,
    #[serde(default)]
    pub supports_native_protocol: bool,
}

/// POST /unified/v1/formats
#[derive(Debug, Deserialize)]
pub struct RegisterFormatRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub serialization_hint: Option<String>,
}

/// Query parameters for `GET /unified/v1/asset-types`: pagination plus an
/// optional category filter (FR-T5).
#[derive(Debug, Deserialize, Default)]
pub struct AssetTypeListQuery {
    #[serde(flatten)]
    pub pagination: PaginationQuery,
    #[serde(default)]
    pub category: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn page_size_defaults_and_clamps() {
        let q = PaginationQuery::default();
        assert_eq!(q.resolve().unwrap(), (0, DEFAULT_PAGE_SIZE));

        let q = PaginationQuery {
            page_token: None,
            page_size: Some(5000),
        };
        assert_eq!(q.resolve().unwrap(), (0, MAX_PAGE_SIZE));

        let q = PaginationQuery {
            page_token: Some("40".to_string()),
            page_size: Some(20),
        };
        assert_eq!(q.resolve().unwrap(), (40, 20));
    }

    #[test]
    fn non_positive_page_size_is_validation_error() {
        for size in [0, -1] {
            let q = PaginationQuery {
                page_token: None,
                page_size: Some(size),
            };
            assert!(matches!(q.resolve(), Err(CatalogError::Validation(_))));
        }
    }

    #[test]
    fn malformed_token_is_validation_error() {
        let q = PaginationQuery {
            page_token: Some("abc".to_string()),
            page_size: None,
        };
        assert!(matches!(q.resolve(), Err(CatalogError::Validation(_))));
    }

    #[test]
    fn next_token_only_when_page_is_full() {
        let full = vec![1, 2, 3];
        assert_eq!(next_page_token(6, &full, 3), Some("9".to_string()));
        let partial = vec![1, 2];
        assert_eq!(next_page_token(6, &partial, 3), None);
    }
}
