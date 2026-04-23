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

#[derive(Serialize, Deserialize)]
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

// ── Commit Table DTOs ──────────────────────────────────────

#[derive(Deserialize)]
pub struct CommitTableRequest {
    pub identifier: Option<TableIdentifier>,
    pub requirements: Vec<super::table_metadata::TableRequirement>,
    pub updates: Vec<super::table_metadata::TableUpdate>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_create_namespace_request_serde() {
        let json = r#"{"namespace": ["prod"], "properties": {"owner": "team-a"}}"#;
        let req: CreateNamespaceRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.namespace, vec!["prod"]);
        assert_eq!(req.properties.get("owner").unwrap(), "team-a");

        let json_no_props = r#"{"namespace": ["prod"]}"#;
        let req_no_props: CreateNamespaceRequest = serde_json::from_str(json_no_props).unwrap();
        assert!(req_no_props.properties.is_empty());
    }

    #[test]
    fn test_namespace_response_serde() {
        let resp = NamespaceResponse {
            namespace: vec!["prod".to_string()],
            properties: HashMap::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["namespace"], serde_json::json!(["prod"]));
        assert!(!decoded.as_object().unwrap().contains_key("properties"));

        let resp_with_props = NamespaceResponse {
            namespace: vec!["prod".to_string()],
            properties: HashMap::from([("owner".to_string(), "team-a".to_string())]),
        };
        let json_with_props = serde_json::to_string(&resp_with_props).unwrap();
        let decoded_with_props: serde_json::Value = serde_json::from_str(&json_with_props).unwrap();
        assert_eq!(decoded_with_props["properties"]["owner"], "team-a");
    }

    #[test]
    fn test_list_namespaces_query_serde() {
        let json = r#"{"pageToken": "100", "pageSize": 50}"#;
        let query: ListNamespacesQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.page_token, Some("100".to_string()));
        assert_eq!(query.page_size, Some(50));

        let empty_json = "{}";
        let empty_query: ListNamespacesQuery = serde_json::from_str(empty_json).unwrap();
        assert!(empty_query.page_token.is_none());
        assert!(empty_query.page_size.is_none());
    }

    #[test]
    fn test_list_namespaces_response_serde() {
        let resp = ListNamespacesResponse {
            namespaces: vec![vec!["prod".to_string()], vec!["staging".to_string()]],
            next_page_token: Some("100".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded["namespaces"],
            serde_json::json!([["prod"], ["staging"]])
        );
        assert_eq!(decoded["nextPageToken"], "100");

        let resp_no_token = ListNamespacesResponse {
            namespaces: vec![vec!["prod".to_string()]],
            next_page_token: None,
        };
        let json_no_token = serde_json::to_string(&resp_no_token).unwrap();
        let decoded_no_token: serde_json::Value = serde_json::from_str(&json_no_token).unwrap();
        assert!(!decoded_no_token
            .as_object()
            .unwrap()
            .contains_key("nextPageToken"));
    }

    #[test]
    fn test_update_namespace_properties_request_serde() {
        let json = r#"{"removals": ["env"], "updates": {"owner": "team-b"}}"#;
        let req: UpdateNamespacePropertiesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.removals, vec!["env"]);
        assert_eq!(req.updates.get("owner").unwrap(), "team-b");

        let empty_json = "{}";
        let empty_req: UpdateNamespacePropertiesRequest = serde_json::from_str(empty_json).unwrap();
        assert!(empty_req.removals.is_empty());
        assert!(empty_req.updates.is_empty());
    }

    #[test]
    fn test_update_namespace_properties_response_serde() {
        let resp = UpdateNamespacePropertiesResponse {
            removed: vec!["env".to_string()],
            updated: vec!["owner".to_string()],
            missing: vec!["nonexistent".to_string()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["removed"], serde_json::json!(["env"]));
        assert_eq!(decoded["updated"], serde_json::json!(["owner"]));
        assert_eq!(decoded["missing"], serde_json::json!(["nonexistent"]));

        let resp_no_missing = UpdateNamespacePropertiesResponse {
            removed: vec!["env".to_string()],
            updated: vec!["owner".to_string()],
            missing: vec![],
        };
        let json_no_missing = serde_json::to_string(&resp_no_missing).unwrap();
        let decoded_no_missing: serde_json::Value = serde_json::from_str(&json_no_missing).unwrap();
        assert!(!decoded_no_missing
            .as_object()
            .unwrap()
            .contains_key("missing"));
    }

    #[test]
    fn test_create_table_request_serde() {
        let json = r#"{"name": "users", "location": "s3://bucket/users", "properties": {"format": "parquet"}}"#;
        let req: CreateTableRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "users");
        assert_eq!(req.location, Some("s3://bucket/users".to_string()));
        assert_eq!(req.properties.get("format").unwrap(), "parquet");

        let minimal_json = r#"{"name": "users"}"#;
        let minimal_req: CreateTableRequest = serde_json::from_str(minimal_json).unwrap();
        assert_eq!(minimal_req.name, "users");
        assert!(minimal_req.location.is_none());
        assert!(minimal_req.properties.is_empty());
        assert!(minimal_req.schema.is_none());
    }

    #[test]
    fn test_load_table_response_serde() {
        let resp = LoadTableResponse {
            metadata_location: Some("s3://bucket/metadata.json".to_string()),
            metadata: serde_json::json!({"format-version": 2}),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["metadata-location"], "s3://bucket/metadata.json");
        assert_eq!(decoded["metadata"]["format-version"], 2);
    }

    #[test]
    fn test_list_tables_response_serde() {
        let resp = ListTablesResponse {
            identifiers: vec![TableIdentifier {
                namespace: vec!["prod".to_string()],
                name: "users".to_string(),
            }],
            next_page_token: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded["identifiers"][0]["namespace"],
            serde_json::json!(["prod"])
        );
        assert_eq!(decoded["identifiers"][0]["name"], "users");
    }

    #[test]
    fn test_rename_table_request_serde() {
        let json = r#"{"source": {"namespace": ["prod"], "name": "users"}, "destination": {"namespace": ["prod"], "name": "customers"}}"#;
        let req: RenameTableRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.source.namespace, vec!["prod"]);
        assert_eq!(req.source.name, "users");
        assert_eq!(req.destination.namespace, vec!["prod"]);
        assert_eq!(req.destination.name, "customers");
    }

    #[test]
    fn test_table_identifier_serde() {
        let identifier = TableIdentifier {
            namespace: vec!["prod".to_string()],
            name: "users".to_string(),
        };
        let json = serde_json::to_string(&identifier).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["namespace"], serde_json::json!(["prod"]));
        assert_eq!(decoded["name"], "users");
    }
}
