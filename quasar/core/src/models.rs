//! Core domain models aligned with the baseline physical data model
//! (DESIGN §3.2 / §3.3). Each entity maps one-to-one to its table.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

/// Three-state PATCH field: distinguishes "leave unchanged" from
/// "clear to NULL", avoiding the ambiguity of `Option<Option<T>>`.
///
/// - `NoChange`: field not present in the request body, keep the value.
/// - `Unset`: field explicitly set to null, clear the value.
/// - `Set(T)`: field carries a value, set/overwrite.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PatchField<T> {
    #[default]
    NoChange,
    Unset,
    Set(T),
}

impl<'de, T> Deserialize<'de> for PatchField<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(|value| match value {
            Some(v) => Self::Set(v),
            None => Self::Unset,
        })
    }
}

/// Asset type registry entry (`asset_types` table).
///
/// Global registry of asset types. `category` groups types for browsing;
/// `validation_schema` optionally constrains asset properties;
/// `extension_strategy` decides how type-specific fields are stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetType {
    pub name: String,
    pub description: Option<String>,
    /// Logical grouping: `tabular`, `view`, `model`, `agent`, `tool`,
    /// `mcp_server`, `fileset`, `topic`, `generic`.
    pub category: String,
    /// Optional JSON Schema used to validate asset properties.
    pub validation_schema: Option<serde_json::Value>,
    /// Storage strategy for type-specific fields:
    /// `jsonb` / `dedicated_table` / `reference_only`.
    pub extension_strategy: String,
    /// Whether the type is expected to have a native protocol adapter.
    pub supports_native_protocol: bool,
}

/// Format registry entry (`formats` table).
///
/// Formats are orthogonal to asset types: the same type may have several
/// formats (e.g. a table may be `iceberg` or `lance`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Format {
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    /// Optional serialization hint, e.g. `json`, `protobuf`.
    pub serialization_hint: Option<String>,
}

/// Domain: top-level organizational boundary (`domains` table).
///
/// The name is globally unique and used in API paths. Storage settings
/// override the global environment defaults when present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub id: Uuid,
    /// Globally unique, URL-safe slug: `^[a-z0-9][a-z0-9_-]{0,62}$`.
    pub name: String,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    /// Object storage backend: `s3` / `minio` / `hdfs` / `local`.
    pub storage_type: Option<String>,
    /// Backend connection config; must not store credentials in plaintext.
    pub storage_config: Option<serde_json::Value>,
    /// Domain-level default warehouse root path.
    pub warehouse: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Namespace: hierarchical container of assets inside a Domain
/// (`namespaces` table).
///
/// `path` materializes the full hierarchy with `/`-separated slug
/// segments (e.g. `analytics/teams/finance`); `depth` is the redundant
/// segment count supporting prefix queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    pub domain_id: Uuid,
    /// Hierarchical path; each segment is a URL-safe slug.
    pub path: String,
    /// Number of path segments.
    pub depth: i32,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Asset: unified identity for every asset (`assets` table).
///
/// Governance operations reference `id` so renames do not break
/// references. `format` lives on this table so protocol isolation filters
/// without joining extension tables. `current_version_key` is updated as
/// a result of a successful CAS commit, not a validation anchor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    /// Active asset names are unique within a namespace; URL-safe slug.
    pub name: String,
    /// Registered asset type (references `asset_types.name`).
    pub asset_type: String,
    /// Optional format (references `formats.name`).
    pub format: Option<String>,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    /// Native version key of the current version; joins to
    /// `asset_versions` via `asset_id = id AND version_key = current_version_key`.
    pub current_version_key: Option<String>,
    /// Soft-delete timestamp; `None` means active.
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// AssetVersion: immutable version history entry (`asset_versions` table).
///
/// `version_key` is the format-native identifier (Iceberg metadata
/// sequence like `00001`, Lance native version number). `content_pointer`
/// references the metadata file in object storage; `previous_version_id`
/// forms the version chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    /// Format-native version identifier.
    pub version_key: String,
    /// Format-agnostic version info: commit message, author, operation
    /// type, custom attributes. Not the native metadata file.
    pub version_properties: Option<serde_json::Value>,
    /// Optional small inline content (e.g. a small JSON/YAML config).
    pub content_inline: Option<serde_json::Value>,
    /// Optional pointer to the actual artifact file in object storage.
    pub content_pointer: Option<String>,
    /// Predecessor version; `None` for the root version (at most one per
    /// asset, enforced by a partial unique index).
    pub previous_version_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

/// TabularAsset: extension row for `table` assets (`tabular_assets` table).
///
/// `metadata_location` is the hot-path cache of the current content
/// pointer and the CAS validation anchor; `schema_snapshot` caches the
/// current schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAsset {
    pub asset_id: Uuid,
    /// Table root path.
    pub location: String,
    /// Current metadata.json pointer.
    pub metadata_location: Option<String>,
    /// Current schema cache.
    pub schema_snapshot: Option<serde_json::Value>,
}

/// ViewAsset: extension row for `view` assets (`view_assets` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewAsset {
    pub asset_id: Uuid,
    /// Iceberg view uuid (checked against `assert-view-uuid` on commit).
    pub view_uuid: Option<Uuid>,
    pub location: Option<String>,
    /// Current content pointer cache.
    pub metadata_location: Option<String>,
}

/// Asset + TabularAsset combination for scenarios needing both the
/// generic identity fields and the table extension fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetWithTabular {
    pub asset: Asset,
    pub tabular: TabularAsset,
}

/// View: Asset + ViewAsset combination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub asset: Asset,
    pub view: ViewAsset,
}

/// ViewIdentifier: view identifier for list responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewIdentifier {
    /// Hierarchical namespace path of the view.
    pub namespace_path: String,
    pub name: String,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestPatch {
        #[serde(default)]
        field: PatchField<String>,
    }

    #[test]
    fn test_patch_field_deserialize() {
        // Missing field: defaults to PatchField::NoChange
        let missing: TestPatch = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.field, PatchField::NoChange);

        // Explicit null
        let null: PatchField<String> = serde_json::from_str("null").unwrap();
        assert_eq!(null, PatchField::Unset);

        // Explicit value
        let value: PatchField<String> = serde_json::from_str("\"hello\"").unwrap();
        assert_eq!(value, PatchField::Set("hello".to_string()));
    }

    #[test]
    fn test_patch_field_default() {
        let pf: PatchField<String> = Default::default();
        assert_eq!(pf, PatchField::NoChange);
    }

    #[test]
    fn test_asset_type_serde_roundtrip() {
        let asset_type = AssetType {
            name: "table".to_string(),
            description: Some("tabular data".to_string()),
            category: "tabular".to_string(),
            validation_schema: None,
            extension_strategy: "dedicated_table".to_string(),
            supports_native_protocol: true,
        };

        let json = serde_json::to_string(&asset_type).unwrap();
        let decoded: AssetType = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, "table");
        assert_eq!(decoded.category, "tabular");
        assert_eq!(decoded.extension_strategy, "dedicated_table");
        assert!(decoded.supports_native_protocol);
        assert!(decoded.validation_schema.is_none());
    }

    #[test]
    fn test_format_serde_roundtrip() {
        let format = Format {
            name: "iceberg".to_string(),
            description: None,
            mime_type: Some("application/json".to_string()),
            serialization_hint: Some("json".to_string()),
        };

        let json = serde_json::to_string(&format).unwrap();
        let decoded: Format = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, "iceberg");
        assert_eq!(decoded.mime_type.as_deref(), Some("application/json"));
        assert!(decoded.description.is_none());
    }

    #[test]
    fn test_domain_serde_roundtrip() {
        let now = Utc::now();
        let domain = Domain {
            id: Uuid::new_v4(),
            name: "prod".to_string(),
            comment: Some("production tenant".to_string()),
            properties: Some(serde_json::json!({"team": "platform"})),
            storage_type: Some("s3".to_string()),
            storage_config: Some(serde_json::json!({
                "bucket": "quasar-prod",
                "prefix": "warehouse",
            })),
            warehouse: Some("s3://quasar-prod/warehouse".to_string()),
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&domain).unwrap();
        let decoded: Domain = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, "prod");
        assert_eq!(decoded.storage_type.as_deref(), Some("s3"));
        assert_eq!(
            decoded.warehouse.as_deref(),
            Some("s3://quasar-prod/warehouse")
        );
        assert_eq!(decoded.properties.unwrap()["team"], "platform");
        assert_eq!(decoded.storage_config.unwrap()["bucket"], "quasar-prod");
    }

    #[test]
    fn test_namespace_serde_roundtrip() {
        let now = Utc::now();
        let ns = Namespace {
            id: Uuid::new_v4(),
            domain_id: Uuid::new_v4(),
            path: "analytics/teams/finance".to_string(),
            depth: 3,
            comment: Some("finance analytics".to_string()),
            properties: None,
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&ns).unwrap();
        let decoded: Namespace = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.path, "analytics/teams/finance");
        assert_eq!(decoded.depth, 3);
        assert!(decoded.properties.is_none());
    }

    #[test]
    fn test_asset_serde_roundtrip() {
        let now = Utc::now();
        let ns_id = Uuid::new_v4();
        let asset = Asset {
            id: Uuid::new_v4(),
            namespace_id: ns_id,
            name: "users".to_string(),
            asset_type: "table".to_string(),
            format: Some("iceberg".to_string()),
            comment: Some("user data".to_string()),
            properties: Some(serde_json::json!({"owner": "alice"})),
            current_version_key: Some("00001".to_string()),
            deleted_at: None,
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&asset).unwrap();
        let decoded: Asset = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.namespace_id, ns_id);
        assert_eq!(decoded.format.as_deref(), Some("iceberg"));
        assert_eq!(decoded.current_version_key.as_deref(), Some("00001"));
        assert!(decoded.deleted_at.is_none());
    }

    #[test]
    fn test_asset_version_serde_roundtrip() {
        let now = Utc::now();
        let asset_id = Uuid::new_v4();
        let prev_id = Uuid::new_v4();
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id,
            version_key: "00002".to_string(),
            version_properties: Some(serde_json::json!({"commit": "daily load"})),
            content_inline: None,
            content_pointer: Some(
                "s3://bucket/warehouse/prod/users/metadata/00002.metadata.json".to_string(),
            ),
            previous_version_id: Some(prev_id),
            created_at: now,
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.version_key, "00002");
        assert_eq!(decoded.previous_version_id, Some(prev_id));
        assert_eq!(decoded.version_properties.unwrap()["commit"], "daily load");
        assert!(decoded.content_inline.is_none());
        assert!(decoded.content_pointer.is_some());
    }

    #[test]
    fn test_tabular_asset_serde_roundtrip() {
        let tab = TabularAsset {
            asset_id: Uuid::new_v4(),
            location: "s3://bucket/warehouse/prod/users".to_string(),
            metadata_location: Some(
                "s3://bucket/warehouse/prod/users/metadata/00001.metadata.json".to_string(),
            ),
            schema_snapshot: None,
        };

        let json = serde_json::to_string(&tab).unwrap();
        let decoded: TabularAsset = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.location, "s3://bucket/warehouse/prod/users");
        assert!(decoded.metadata_location.is_some());
        assert!(decoded.schema_snapshot.is_none());
    }

    #[test]
    fn test_view_asset_serde_roundtrip() {
        let view = ViewAsset {
            asset_id: Uuid::new_v4(),
            view_uuid: Some(Uuid::new_v4()),
            location: Some("s3://bucket/warehouse/prod/v1".to_string()),
            metadata_location: None,
        };

        let json = serde_json::to_string(&view).unwrap();
        let decoded: ViewAsset = serde_json::from_str(&json).unwrap();
        assert!(decoded.view_uuid.is_some());
        assert!(decoded.location.is_some());
        assert!(decoded.metadata_location.is_none());
    }

    #[test]
    fn test_asset_with_tabular_serde_roundtrip() {
        let now = Utc::now();
        let asset = Asset {
            id: Uuid::new_v4(),
            namespace_id: Uuid::new_v4(),
            name: "orders".to_string(),
            asset_type: "table".to_string(),
            format: Some("lance".to_string()),
            comment: None,
            properties: None,
            current_version_key: None,
            deleted_at: None,
            created_at: now,
            updated_at: now,
        };
        let tabular = TabularAsset {
            asset_id: asset.id,
            location: "s3://bucket/orders.lance".to_string(),
            metadata_location: None,
            schema_snapshot: None,
        };

        let combined = AssetWithTabular { asset, tabular };
        let json = serde_json::to_string(&combined).unwrap();
        let decoded: AssetWithTabular = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.asset.name, "orders");
        assert_eq!(decoded.tabular.location, "s3://bucket/orders.lance");
    }

    #[test]
    fn test_view_identifier_serde_roundtrip() {
        let ident = ViewIdentifier {
            namespace_path: "analytics/teams".to_string(),
            name: "monthly_report".to_string(),
        };

        let json = serde_json::to_string(&ident).unwrap();
        let decoded: ViewIdentifier = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.namespace_path, "analytics/teams");
        assert_eq!(decoded.name, "monthly_report");
    }
}
