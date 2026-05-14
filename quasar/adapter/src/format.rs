//! Format and type enums for protocol adapter routing.
//!
//! V3 stores formats as free-form strings in the database; these enums are
//! adapter-internal newtypes used for protocol routing dispatch. They should
//! not be exported to core layer which uses `String` for format/type fields.

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

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
        match self {
            AssetFormat::Iceberg => "iceberg",
            AssetFormat::Lance => "lance",
        }
    }

    /// Parse a format string into AssetFormat.
    /// Returns None if the format is unrecognized.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "iceberg" => Some(AssetFormat::Iceberg),
            "lance" => Some(AssetFormat::Lance),
            _ => None,
        }
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
}
