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
