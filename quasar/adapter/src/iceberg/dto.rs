use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── Namespace DTOs ─────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateNamespaceRequest {
    pub namespace: Vec<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct NamespaceResponse {
    pub namespace: Vec<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Deserialize, Default)]
pub struct ListNamespacesQuery {
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i32>,
}

#[derive(Serialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "nextPageToken")]
    pub next_page_token: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateNamespacePropertiesRequest {
    #[serde(default)]
    pub removals: Vec<String>,
    #[serde(default)]
    pub updates: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct UpdateNamespacePropertiesResponse {
    pub removed: Vec<String>,
    pub updated: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
}

// ── Table DTOs ─────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Serialize)]
pub struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: Option<String>,
    pub metadata: serde_json::Value,
}

#[derive(Deserialize, Default)]
pub struct ListTablesQuery {
    #[serde(rename = "pageToken")]
    pub page_token: Option<String>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i32>,
}

#[derive(Serialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "nextPageToken")]
    pub next_page_token: Option<String>,
}

#[derive(Serialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Deserialize)]
pub struct RenameTableRequest {
    pub source: TableIdentifierInput,
    pub destination: TableIdentifierInput,
}

#[derive(Deserialize)]
pub struct TableIdentifierInput {
    pub namespace: Vec<String>,
    pub name: String,
}
