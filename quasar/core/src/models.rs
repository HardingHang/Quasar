use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetFormat {
    Iceberg,
    Lance,
}

impl AssetFormat {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetFormat::Iceberg => "iceberg",
            AssetFormat::Lance => "lance",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub format: AssetFormat,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub version_id: i64,
    pub metadata_location: String,
    pub previous_version_id: Option<i64>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetCommitUpdate {
    pub metadata_location: String,
    pub previous_version_id: Option<i64>,
}

#[cfg(test)]
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
    fn test_namespace_serde_roundtrip() {
        let now = Utc::now();
        let mut props = HashMap::new();
        props.insert("team".to_string(), "data".to_string());

        let ns = Namespace {
            id: Uuid::new_v4(),
            name: "prod".to_string(),
            format: AssetFormat::Lance,
            properties: props.clone(),
            created_at: now,
        };

        let json = serde_json::to_string(&ns).unwrap();
        let decoded: Namespace = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.name, "prod");
        assert_eq!(decoded.format, AssetFormat::Lance);
        assert_eq!(decoded.properties.get("team"), Some(&"data".to_string()));
    }

    #[test]
    fn test_asset_serde_roundtrip() {
        let now = Utc::now();
        let ns_id = Uuid::new_v4();

        let asset = Asset {
            id: Uuid::new_v4(),
            namespace_id: ns_id,
            name: "users".to_string(),
            properties: HashMap::new(),
            created_at: now,
        };

        let json = serde_json::to_string(&asset).unwrap();
        let decoded: Asset = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.name, "users");
        assert_eq!(decoded.namespace_id, ns_id);
    }

    #[test]
    fn test_asset_version_serde_roundtrip() {
        let now = Utc::now();
        let asset_id = Uuid::new_v4();

        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id,
            version_id: 42,
            metadata_location: "s3://bucket/v42.manifest".to_string(),
            previous_version_id: Some(41),
            timestamp: now,
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.version_id, 42);
        assert_eq!(decoded.metadata_location, "s3://bucket/v42.manifest");
        assert_eq!(decoded.previous_version_id, Some(41));
    }

    #[test]
    fn test_asset_version_without_previous() {
        let version = AssetVersion {
            id: Uuid::new_v4(),
            asset_id: Uuid::new_v4(),
            version_id: 1,
            metadata_location: "s3://bucket/v1.manifest".to_string(),
            previous_version_id: None,
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&version).unwrap();
        let decoded: AssetVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.previous_version_id, None);
    }

    #[test]
    fn test_asset_commit_update_serde() {
        let update = AssetCommitUpdate {
            metadata_location: "s3://bucket/new.manifest".to_string(),
            previous_version_id: Some(3),
        };

        let json = serde_json::to_string(&update).unwrap();
        let decoded: AssetCommitUpdate = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.metadata_location, "s3://bucket/new.manifest");
        assert_eq!(decoded.previous_version_id, Some(3));
    }

    #[test]
    fn test_asset_commit_update_without_previous() {
        let update = AssetCommitUpdate {
            metadata_location: "s3://bucket/first.manifest".to_string(),
            previous_version_id: None,
        };

        let json = serde_json::to_string(&update).unwrap();
        let decoded: AssetCommitUpdate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.previous_version_id, None);
    }
}
