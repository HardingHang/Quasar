use quasar_core::PatchField;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Asset DTOs ──────────────────────────────────────────────────

// ── Namespace DTOs ──────────────────────────────────────────────

/// POST /unified/v1/namespaces
#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    pub name: String,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// PATCH /unified/v1/namespaces/{ns}
#[derive(Debug, Deserialize)]
pub struct UpdateNamespaceRequest {
    #[serde(default)]
    pub comment: PatchField<String>,
    #[serde(default)]
    pub removals: Vec<String>,
    #[serde(default)]
    pub updates: HashMap<String, String>,
}

/// Namespace response body.
#[derive(Debug, Serialize)]
pub struct NamespaceResponse {
    pub id: String,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: String,
}

/// List namespaces response body.
#[derive(Debug, Serialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<NamespaceResponse>,
    pub next_page_token: Option<String>,
}

// ── Pagination ──────────────────────────────────────────────────

/// Pagination query parameters for list endpoints.
#[derive(Debug, Deserialize)]
pub struct PaginationQuery {
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i32>,
}

impl PaginationQuery {
    pub const DEFAULT_PAGE_SIZE: i32 = 100;
    pub const MAX_PAGE_SIZE: i32 = 1000;

    /// Resolve page_size with default and validation.
    pub fn resolved_page_size(&self) -> Result<i32, PageSizeError> {
        resolve_page_size(self.page_size)
    }

    /// Decode page_token as offset (S4 MVP: offset encoded in token).
    /// Returns Err if token is present but malformed.
    pub fn resolved_offset(&self) -> Result<i64, &'static str> {
        match &self.page_token {
            None => Ok(0),
            Some(token) => token.parse::<i64>().map_err(|_| "invalid page token"),
        }
    }

    /// Encode offset into page_token for next page.
    pub fn encode_token(offset: i64) -> String {
        offset.to_string()
    }
}

/// Current version response (tagged union by format).
#[derive(Debug, Serialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum CurrentVersionResponse {
    Iceberg {
        sequence_number: i64,
        snapshot_id: Option<i64>,
        timestamp_ms: Option<i64>,
    },
    Lance {
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
        timestamp: String,
    },
}

/// Asset list item (without current_version).
#[derive(Debug, Serialize)]
pub struct AssetListItem {
    pub id: String,
    pub name: String,
    pub asset_type: String,
    pub format: Option<String>,
    pub location: Option<String>,
    pub metadata_location: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: String,
}

/// Asset detail response (with current_version).
#[derive(Debug, Serialize)]
pub struct AssetResponse {
    pub id: String,
    pub name: String,
    pub asset_type: String,
    pub format: Option<String>,
    pub location: Option<String>,
    pub metadata_location: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub current_version: Option<CurrentVersionResponse>,
    pub created_at: String,
}

/// PATCH /unified/v1/namespaces/{ns}/assets/{name}
#[derive(Debug, Deserialize)]
pub struct UpdateAssetRequest {
    #[serde(default)]
    pub comment: PatchField<String>,
    #[serde(default)]
    pub removals: Vec<String>,
    #[serde(default)]
    pub updates: HashMap<String, String>,
}

/// POST /unified/v1/namespaces/{ns}/assets/{name}/rename
#[derive(Debug, Deserialize)]
pub struct RenameAssetRequest {
    pub new_name: String,
    pub new_namespace: Option<String>,
}

/// List assets response body.
#[derive(Debug, Serialize)]
pub struct ListAssetsResponse {
    pub assets: Vec<AssetListItem>,
    pub next_page_token: Option<String>,
}

/// Query parameters for asset list endpoint.
#[derive(Debug, Deserialize)]
pub struct AssetListQuery {
    pub format: Option<String>,
    pub name: Option<String>,
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i32>,
}

impl AssetListQuery {
    pub fn resolved_page_size(&self) -> Result<i32, PageSizeError> {
        resolve_page_size(self.page_size)
    }

    pub fn resolved_offset(&self) -> Result<i64, &'static str> {
        match &self.page_token {
            None => Ok(0),
            Some(token) => token.parse::<i64>().map_err(|_| "invalid page token"),
        }
    }
}

/// Page size validation error for Unified list endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageSizeError {
    Invalid,
    TooLarge,
}

fn resolve_page_size(page_size: Option<i32>) -> Result<i32, PageSizeError> {
    match page_size.unwrap_or(PaginationQuery::DEFAULT_PAGE_SIZE) {
        size if size <= 0 => Err(PageSizeError::Invalid),
        size if size > PaginationQuery::MAX_PAGE_SIZE => Err(PageSizeError::TooLarge),
        size => Ok(size),
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
    pub properties: HashMap<String, String>,
    #[serde(default)]
    pub storage_type: Option<String>,
    /// JSON storage configuration. May contain secret references or
    /// pre-encrypted credentials; the server must not echo this field
    /// back in responses (see `DomainResponse`).
    #[serde(default)]
    pub storage_config: Option<serde_json::Value>,
    #[serde(default)]
    pub warehouse: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
}

/// PATCH /unified/v1/domains/{domain}
#[derive(Debug, Deserialize, Default)]
pub struct UpdateDomainRequest {
    #[serde(default)]
    pub comment: PatchField<String>,
    #[serde(default)]
    pub removals: Vec<String>,
    #[serde(default)]
    pub updates: HashMap<String, String>,
    #[serde(default)]
    pub storage_type: PatchField<String>,
    #[serde(default)]
    pub storage_config: PatchField<serde_json::Value>,
    #[serde(default)]
    pub warehouse: PatchField<String>,
    #[serde(default)]
    pub owner: PatchField<String>,
}

/// Domain response body. **Does not** include `storage_config` because the
/// field may contain credentials or secret references; V3 §3.1 invariant 8
/// requires structural redaction of sensitive storage configuration.
#[derive(Debug, Serialize)]
pub struct DomainResponse {
    pub id: String,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub storage_type: Option<String>,
    pub warehouse: Option<String>,
    pub owner: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// List domains response body.
#[derive(Debug, Serialize)]
pub struct ListDomainsResponse {
    pub domains: Vec<DomainResponse>,
    pub next_page_token: Option<String>,
}
