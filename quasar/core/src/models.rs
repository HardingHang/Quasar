use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use strum::{Display, EnumString, IntoStaticStr};
use uuid::Uuid;

/// Asset format enum for standard protocol adapter format isolation.
///
/// V3 stores formats as free-form strings in the database; this enum is
/// retained for protocol adapters that route by a closed format set.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AssetFormat {
    Iceberg,
    Lance,
}

impl AssetFormat {
    pub fn as_str(&self) -> &'static str {
        (*self).into()
    }
}

/// Generic asset type enum.
///
/// V3 stores `asset_type` as a free-form string referencing the
/// `asset_types` registry; this enum is retained for protocol adapters
/// that only deal with the table type today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum AssetType {
    Table,
}

impl AssetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetType::Table => "table",
        }
    }
}

/// PATCH field three-state:
/// - Missing: field not present in request body, no change
/// - Null: field explicitly set to null, clear the value
/// - Value(T): field set to a specific value, set/overwrite
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum PatchField<T> {
    #[default]
    Missing,
    Null,
    Value(T),
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
            Some(v) => Self::Value(v),
            None => Self::Null,
        })
    }
}

/// Domain: top-level storage and governance container introduced in V3.
///
/// A Domain is **not** bound to any tabular format. Format isolation
/// lives at the REST endpoint, not at the domain level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub id: Uuid,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub storage_type: Option<String>,
    /// Storage configuration (e.g. S3 bucket, prefix). Credentials must
    /// be stored as secret references; API responses must redact.
    pub storage_config: serde_json::Value,
    pub warehouse: Option<String>,
    pub owner: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Default for Domain {
    fn default() -> Self {
        let now = DateTime::<Utc>::MIN_UTC;
        Self {
            id: Uuid::nil(),
            name: String::new(),
            comment: None,
            properties: HashMap::new(),
            storage_type: None,
            storage_config: serde_json::Value::Object(serde_json::Map::new()),
            warehouse: None,
            owner: None,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Namespace: business organization unit inside a Domain.
///
/// V3 only models a single level. Nested namespaces are deferred to a
/// later version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    /// Owning Domain. V3 wires this through the `namespaces.domain_id`
    /// column; Phase 1 storage bridges still populate `Uuid::nil()` until
    /// Phase 2 switches to the new schema.
    pub domain_id: Uuid,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Asset: shared identity layer for every type of asset (table, model, ...).
///
/// V3 drops `asset_subtype` and stores the format on the type-specific
/// extension table (`tabular_assets.format` for tabular assets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    /// Free-form asset type name referencing the `asset_types` registry,
    /// e.g. `"table"`, `"model"`, `"fileset"`.
    pub asset_type: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    /// Soft-delete timestamp. `None` means active.
    pub deleted_at: Option<DateTime<Utc>>,
    pub created_by: Option<String>,
    pub updated_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// TabularAsset: tabular-asset extension (Iceberg, Lance, ...).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAsset {
    pub asset_id: Uuid,
    /// Tabular format name referencing the `tabular_formats` registry,
    /// e.g. `"iceberg"`, `"lance"`.
    pub format: String,
    pub location: String,
    pub metadata_location: Option<String>,
    pub schema_snapshot: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Asset + TabularAsset combination for scenarios needing both generic and table fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetWithTabular {
    pub asset: Asset,
    pub tabular: TabularAsset,
}

/// AssetVersion: generic version registry entity.
///
/// V3 adds `previous_version_id` to express the version chain and
/// `comment` for human-readable change notes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    /// Format-native version identifier (e.g. Lance `"42"`, model `"v1.2.0"`).
    pub version_key: String,
    /// Comparable ordering. `None` for formats without a stable numeric
    /// ordering; latest queries must use this column and never parse
    /// `version_key`.
    pub version_order: Option<i64>,
    /// Predecessor version id within the same asset. Database trigger
    /// `trg_asset_versions_previous_same_asset` enforces cross-asset
    /// references are rejected.
    pub previous_version_id: Option<Uuid>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// TabularAssetVersion: tabular-asset version extension.
///
/// V3 renames the primary key from `asset_version_id` to `version_id`
/// and drops the `previous_*` columns (the version chain lives on
/// `AssetVersion.previous_version_id`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAssetVersion {
    pub version_id: Uuid,
    pub metadata_location: String,
    pub created_at: DateTime<Utc>,
}

/// AssetVersion + TabularAssetVersion combination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersionWithTabular {
    pub version: AssetVersion,
    pub tabular_version: TabularAssetVersion,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json;
    use std::collections::HashMap;

    #[test]
    fn test_asset_format_as_str() {
        assert_eq!(AssetFormat::Iceberg.as_str(), "iceberg");
        assert_eq!(AssetFormat::Lance.as_str(), "lance");
    }

    #[test]
    fn test_asset_format_serde_roundtrip() {
        let lance = AssetFormat::Lance;
        let json = serde_json::to_string(&lance).unwrap();
        assert_eq!(json, "\"lance\"");

        let iceberg = AssetFormat::Iceberg;
        let json = serde_json::to_string(&iceberg).unwrap();
        assert_eq!(json, "\"iceberg\"");

        let decoded: AssetFormat = serde_json::from_str("\"lance\"").unwrap();
        assert_eq!(decoded, AssetFormat::Lance);

        let decoded: AssetFormat = serde_json::from_str("\"iceberg\"").unwrap();
        assert_eq!(decoded, AssetFormat::Iceberg);
    }

    #[test]
    fn test_asset_type_as_str() {
        assert_eq!(AssetType::Table.as_str(), "table");
    }

    #[test]
    fn test_asset_type_serde_roundtrip() {
        let table = AssetType::Table;
        let json = serde_json::to_string(&table).unwrap();
        assert_eq!(json, "\"table\"");

        let decoded: AssetType = serde_json::from_str("\"table\"").unwrap();
        assert_eq!(decoded, AssetType::Table);
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestPatch {
        #[serde(default)]
        field: PatchField<String>,
    }

    #[test]
    fn test_patch_field_deserialize() {
        // Missing field: defaults to PatchField::Missing
        let missing: TestPatch = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.field, PatchField::Missing);

        // Explicit null
        let null: PatchField<String> = serde_json::from_str("null").unwrap();
        assert_eq!(null, PatchField::Null);

        // Explicit value
        let value: PatchField<String> = serde_json::from_str("\"hello\"").unwrap();
        assert_eq!(value, PatchField::Value("hello".to_string()));
    }

    #[test]
    fn test_patch_field_default() {
        let pf: PatchField<String> = Default::default();
        assert_eq!(pf, PatchField::Missing);
    }

    #[test]
    fn test_domain_default() {
        let d = Domain::default();
        assert_eq!(d.id, Uuid::nil());
        assert_eq!(d.name, "");
        assert!(d.comment.is_none());
        assert!(d.properties.is_empty());
        assert!(d.storage_type.is_none());
        assert_eq!(
            d.storage_config,
            serde_json::Value::Object(serde_json::Map::new())
        );
        assert!(d.warehouse.is_none());
        assert!(d.owner.is_none());
    }

    #[test]
    fn test_domain_serde_roundtrip() {
        let now = Utc::now();
        let mut props = HashMap::new();
        props.insert("team".to_string(), "platform".to_string());

        let domain = Domain {
            id: Uuid::new_v4(),
            name: "prod".to_string(),
            comment: Some("production tenant".to_string()),
            properties: props,
            storage_type: Some("s3".to_string()),
            storage_config: serde_json::json!({
                "bucket": "quasar-prod",
                "prefix": "warehouse",
            }),
            warehouse: Some("s3://quasar-prod/warehouse".to_string()),
            owner: Some("data-platform".to_string()),
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
        assert_eq!(decoded.storage_config["bucket"], "quasar-prod");
    }

    #[test]
    fn test_namespace_serde_roundtrip() {
        let now = Utc::now();
        let mut props = HashMap::new();
        props.insert("team".to_string(), "data".to_string());

        let ns = Namespace {
            id: Uuid::new_v4(),
            domain_id: Uuid::new_v4(),
            name: "prod".to_string(),
            comment: Some("production environment".to_string()),
            properties: props.clone(),
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&ns).unwrap();
        let decoded: Namespace = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.name, "prod");
        assert_eq!(decoded.comment, Some("production environment".to_string()));
        assert_eq!(decoded.properties.get("team"), Some(&"data".to_string()));
    }

    #[test]
    fn test_namespace_without_comment() {
        let ns = Namespace {
            id: Uuid::new_v4(),
            domain_id: Uuid::new_v4(),
            name: "dev".to_string(),
            comment: None,
            properties: HashMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&ns).unwrap();
        let decoded: Namespace = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.comment, None);
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
            comment: Some("user data".to_string()),
            properties: HashMap::new(),
            deleted_at: None,
            created_by: Some("alice".to_string()),
            updated_by: None,
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&asset).unwrap();
        let decoded: Asset = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.namespace_id, ns_id);
        assert_eq!(decoded.asset_type, "table");
        assert_eq!(decoded.comment, Some("user data".to_string()));
        assert_eq!(decoded.created_by.as_deref(), Some("alice"));
        assert!(decoded.deleted_at.is_none());
    }

    #[test]
    fn test_tabular_asset_serde_roundtrip() {
        let now = Utc::now();
        let tab = TabularAsset {
            asset_id: Uuid::new_v4(),
            format: "iceberg".to_string(),
            location: "s3://bucket/warehouse/prod/users".to_string(),
            metadata_location: Some(
                "s3://bucket/warehouse/prod/users/metadata/00001.metadata.json".to_string(),
            ),
            schema_snapshot: None,
            created_at: now,
            updated_at: now,
        };

        let json = serde_json::to_string(&tab).unwrap();
        let decoded: TabularAsset = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.format, "iceberg");
        assert_eq!(decoded.location, "s3://bucket/warehouse/prod/users");
        assert_eq!(
            decoded.metadata_location,
            Some("s3://bucket/warehouse/prod/users/metadata/00001.metadata.json".to_string())
        );
    }

    #[test]
    fn test_asset_with_tabular_serde_roundtrip() {
        let now = Utc::now();
        let asset = Asset {
            id: Uuid::new_v4(),
            namespace_id: Uuid::new_v4(),
            name: "orders".to_string(),
            asset_type: "table".to_string(),
            comment: None,
            properties: HashMap::new(),
            deleted_at: None,
            created_by: None,
            updated_by: None,
            created_at: now,
            updated_at: now,
        };
        let tabular = TabularAsset {
            asset_id: asset.id,
            format: "lance".to_string(),
            location: "s3://bucket/orders.lance".to_string(),
            metadata_location: None,
            schema_snapshot: None,
            created_at: now,
            updated_at: now,
        };

        let combined = AssetWithTabular { asset, tabular };
        let json = serde_json::to_string(&combined).unwrap();
        let decoded: AssetWithTabular = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.asset.name, "orders");
        assert_eq!(decoded.tabular.format, "lance");
        assert_eq!(decoded.tabular.location, "s3://bucket/orders.lance");
    }

    #[test]
    fn test_asset_version_serde_roundtrip() {
        let now = Utc::now();
        let asset_id = Uuid::new_v4();
        let prev_id = Uuid::new_v4();

        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id,
            version_key: "1".to_string(),
            version_order: Some(1),
            previous_version_id: Some(prev_id),
            comment: Some("initial load".to_string()),
            properties: HashMap::new(),
            created_at: now,
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.version_key, "1");
        assert_eq!(decoded.version_order, Some(1));
        assert_eq!(decoded.previous_version_id, Some(prev_id));
        assert_eq!(decoded.comment.as_deref(), Some("initial load"));
    }

    #[test]
    fn test_asset_version_without_order() {
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            version_key: "abc".to_string(),
            version_order: None,
            previous_version_id: None,
            comment: None,
            properties: HashMap::new(),
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.version_order, None);
        assert!(decoded.previous_version_id.is_none());
    }

    #[test]
    fn test_tabular_asset_version_serde_roundtrip() {
        let now = Utc::now();
        let tv = TabularAssetVersion {
            version_id: Uuid::new_v4(),
            metadata_location: "s3://bucket/v42.manifest".to_string(),
            created_at: now,
        };

        let json = serde_json::to_string(&tv).unwrap();
        let decoded: TabularAssetVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.metadata_location, "s3://bucket/v42.manifest");
    }

    #[test]
    fn test_asset_version_with_tabular_serde_roundtrip() {
        let now = Utc::now();
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            version_key: "3".to_string(),
            version_order: Some(3),
            previous_version_id: None,
            comment: None,
            properties: HashMap::new(),
            created_at: now,
        };
        let tabular_version = TabularAssetVersion {
            version_id: version.id,
            metadata_location: "s3://bucket/v3.manifest".to_string(),
            created_at: now,
        };

        let combined = AssetVersionWithTabular {
            version,
            tabular_version,
        };
        let json = serde_json::to_string(&combined).unwrap();
        let decoded: AssetVersionWithTabular = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.version.version_key, "3");
        assert_eq!(
            decoded.tabular_version.metadata_location,
            "s3://bucket/v3.manifest"
        );
    }
}
