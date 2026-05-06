use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use strum::{Display, EnumString, IntoStaticStr};
use uuid::Uuid;

/// Asset format enum for standard protocol adapter format isolation.
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

/// Namespace: format-agnostic organization unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// Asset: generic asset registry entity.
/// V2 only contains identity, type, comment, properties, and other generic fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub asset_type: AssetType,
    pub asset_subtype: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// TabularAsset: table asset detail.
/// Holds table-specific location, metadata_location, schema_snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAsset {
    pub asset_id: Uuid,
    pub location: String,
    pub metadata_location: Option<String>,
    pub schema_snapshot: Option<serde_json::Value>,
}

/// Asset + TabularAsset combination for scenarios needing both generic and table fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetWithTabular {
    pub asset: Asset,
    pub tabular: TabularAsset,
}

/// AssetVersion: generic version registry entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub version_key: String,
    pub version_order: Option<i64>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// TabularAssetVersion: table asset version detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAssetVersion {
    pub asset_version_id: Uuid,
    pub metadata_location: String,
    pub previous_asset_version_id: Option<Uuid>,
    pub previous_version_order: Option<i64>,
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
    fn test_namespace_serde_roundtrip() {
        let now = Utc::now();
        let mut props = HashMap::new();
        props.insert("team".to_string(), "data".to_string());

        let ns = Namespace {
            id: Uuid::new_v4(),
            name: "prod".to_string(),
            comment: Some("production environment".to_string()),
            properties: props.clone(),
            created_at: now,
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
            name: "dev".to_string(),
            comment: None,
            properties: HashMap::new(),
            created_at: Utc::now(),
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
            asset_type: AssetType::Table,
            asset_subtype: "iceberg".to_string(),
            comment: Some("user data".to_string()),
            properties: HashMap::new(),
            created_at: now,
        };

        let json = serde_json::to_string(&asset).unwrap();
        let decoded: Asset = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.namespace_id, ns_id);
        assert_eq!(decoded.asset_type, AssetType::Table);
        assert_eq!(decoded.asset_subtype, "iceberg");
        assert_eq!(decoded.comment, Some("user data".to_string()));
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
        assert_eq!(
            decoded.metadata_location,
            Some("s3://bucket/warehouse/prod/users/metadata/00001.metadata.json".to_string())
        );
    }

    #[test]
    fn test_asset_with_tabular_serde_roundtrip() {
        let asset = Asset {
            id: Uuid::new_v4(),
            namespace_id: Uuid::new_v4(),
            name: "orders".to_string(),
            asset_type: AssetType::Table,
            asset_subtype: "lance".to_string(),
            comment: None,
            properties: HashMap::new(),
            created_at: Utc::now(),
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
    fn test_asset_version_serde_roundtrip() {
        let now = Utc::now();
        let asset_id = Uuid::new_v4();

        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id,
            version_key: "1".to_string(),
            version_order: Some(1),
            properties: HashMap::new(),
            created_at: now,
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.version_key, "1");
        assert_eq!(decoded.version_order, Some(1));
    }

    #[test]
    fn test_asset_version_without_order() {
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            version_key: "abc".to_string(),
            version_order: None,
            properties: HashMap::new(),
            created_at: Utc::now(),
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.version_order, None);
    }

    #[test]
    fn test_tabular_asset_version_serde_roundtrip() {
        let tv = TabularAssetVersion {
            asset_version_id: Uuid::new_v4(),
            metadata_location: "s3://bucket/v42.manifest".to_string(),
            previous_asset_version_id: Some(Uuid::new_v4()),
            previous_version_order: Some(1),
        };

        let json = serde_json::to_string(&tv).unwrap();
        let decoded: TabularAssetVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.metadata_location, "s3://bucket/v42.manifest");
        assert!(decoded.previous_asset_version_id.is_some());
        assert_eq!(decoded.previous_version_order, Some(1));
    }

    #[test]
    fn test_tabular_asset_version_without_previous() {
        let tv = TabularAssetVersion {
            asset_version_id: Uuid::new_v4(),
            metadata_location: "s3://bucket/first.manifest".to_string(),
            previous_asset_version_id: None,
            previous_version_order: None,
        };

        let json = serde_json::to_string(&tv).unwrap();
        let decoded: TabularAssetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.previous_asset_version_id, None);
        assert_eq!(decoded.previous_version_order, None);
    }

    #[test]
    fn test_asset_version_with_tabular_serde_roundtrip() {
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            version_key: "3".to_string(),
            version_order: Some(3),
            properties: HashMap::new(),
            created_at: Utc::now(),
        };
        let tabular_version = TabularAssetVersion {
            asset_version_id: version.id,
            metadata_location: "s3://bucket/v3.manifest".to_string(),
            previous_asset_version_id: None,
            previous_version_order: None,
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
