use quasar_core::PatchField;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

    /// Resolve page_size with default and clamp.
    pub fn resolved_page_size(&self) -> i32 {
        self.page_size
            .unwrap_or(Self::DEFAULT_PAGE_SIZE)
            .clamp(1, Self::MAX_PAGE_SIZE)
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
