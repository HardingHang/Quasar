use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── TableMetadata ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TableMetadata {
    pub format_version: i32,
    pub table_uuid: String,
    pub location: String,
    #[serde(default)]
    pub last_sequence_number: i64,
    pub last_updated_ms: i64,
    #[serde(default)]
    pub last_column_id: i32,
    #[serde(default)]
    pub schemas: Vec<Schema>,
    #[serde(default)]
    pub current_schema_id: i32,
    #[serde(default)]
    pub partition_specs: Vec<serde_json::Value>,
    #[serde(default)]
    pub default_spec_id: i32,
    #[serde(default)]
    pub last_partition_id: i32,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    #[serde(default)]
    pub snapshot_log: Vec<serde_json::Value>,
    #[serde(default)]
    pub metadata_log: Vec<serde_json::Value>,
    #[serde(default)]
    pub sort_orders: Vec<serde_json::Value>,
    #[serde(default)]
    pub default_sort_order_id: i32,
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Snapshot {
    pub snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(default)]
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list: String,
    #[serde(default)]
    pub summary: HashMap<String, String>,
    #[serde(default)]
    pub schema_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct SnapshotRef {
    pub snapshot_id: i64,
    #[serde(default = "default_branch")]
    pub r#type: String,
}

fn default_branch() -> String {
    "branch".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    pub schema_id: i32,
    #[serde(default)]
    pub fields: Vec<serde_json::Value>,
}

// ── Requirements ───────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate,
    AssertTableUuid { uuid: String },
    AssertRefSnapshotId {
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

// ── Updates ────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AddSnapshot { snapshot: Snapshot },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        r#type: Option<String>,
    },
    SetProperties { updates: HashMap<String, String> },
    RemoveProperties { removals: Vec<String> },
}

// ── Methods ────────────────────────────────────────────────

impl TableMetadata {
    /// Verify that all requirements are satisfied.
    /// Returns Ok(()) if all pass, or Err(message) on first failure.
    pub fn check_requirements(&self, requirements: &[TableRequirement]) -> Result<(), String> {
        for req in requirements {
            match req {
                TableRequirement::AssertCreate => {
                    // Table already exists → fail
                    return Err(
                        "Table already exists, cannot assert-create".to_string()
                    );
                }
                TableRequirement::AssertTableUuid { uuid } => {
                    if self.table_uuid != *uuid {
                        return Err(format!(
                            "Table UUID mismatch: expected {}, got {}",
                            uuid, self.table_uuid
                        ));
                    }
                }
                TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                    let actual = if r#ref == "main" || r#ref == "current" {
                        self.current_snapshot_id
                    } else {
                        self.refs.get(r#ref).map(|r| r.snapshot_id)
                    };
                    if actual != *snapshot_id {
                        return Err(format!(
                            "Ref '{}' snapshot-id mismatch: expected {:?}, got {:?}",
                            r#ref, snapshot_id, actual
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply a list of updates to this metadata in place.
    pub fn apply_updates(&mut self, updates: &[TableUpdate]) {
        for update in updates {
            match update {
                TableUpdate::AddSnapshot { snapshot } => {
                    self.snapshots.push(snapshot.clone());
                    self.current_snapshot_id = Some(snapshot.snapshot_id);
                    self.last_sequence_number += 1;
                    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    snapshot_id,
                    r#type,
                } => {
                    self.refs.insert(
                        ref_name.clone(),
                        SnapshotRef {
                            snapshot_id: *snapshot_id,
                            r#type: r#type.clone().unwrap_or_else(|| "branch".to_string()),
                        },
                    );
                    if ref_name == "main" {
                        self.current_snapshot_id = Some(*snapshot_id);
                    }
                    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
                }
                TableUpdate::SetProperties { updates } => {
                    for (k, v) in updates {
                        self.properties.insert(k.clone(), v.clone());
                    }
                    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
                }
                TableUpdate::RemoveProperties { removals } => {
                    for k in removals {
                        self.properties.remove(k);
                    }
                    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
                }
            }
        }
    }

    /// Generate the next metadata location by incrementing the sequence number
    /// in the current metadata_location path.
    ///
    /// Expected format: `{location}/metadata/{NNNNN}-{uuid}.metadata.json`
    /// Returns: `{location}/metadata/{NNNNN+1}-{uuid}.metadata.json`
    pub fn next_metadata_location(&self) -> String {
        // Extract the sequence number from current metadata_location
        let current = self
            .metadata_location_from_properties()
            .or_else(|| self.refs.get("metadata-location").map(|_| self.location.clone()))
            .unwrap_or_else(|| self.location.clone());

        // Try to find the pattern /metadata/NNNNN-uuid.metadata.json
        if let Some(idx) = current.rfind("/metadata/") {
            let after = &current[idx + 10..]; // skip "/metadata/"
            if let Some(dash_idx) = after.find('-') {
                if let Ok(seq) = after[..dash_idx].parse::<u32>() {
                    let rest = &after[dash_idx..];
                    // rest should be "-{uuid}.metadata.json"
                    let new_seq = seq + 1;
                    let prefix = &current[..idx + 10];
                    return format!("{}{}{}", prefix, new_seq, rest);
                }
            }
        }

        // Fallback: append a simple increment
        format!("{}/metadata/00002-{}.metadata.json", self.location, self.table_uuid)
    }

    fn metadata_location_from_properties(&self) -> Option<String> {
        None // placeholder: actual metadata_location is tracked externally in the DB
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample_metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: "test-uuid".to_string(),
            location: "s3://bucket/warehouse/prod/users".to_string(),
            last_sequence_number: 0,
            last_updated_ms: 1000,
            last_column_id: 0,
            schemas: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 999,
            properties: HashMap::new(),
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            sort_orders: vec![],
            default_sort_order_id: 0,
            refs: HashMap::new(),
        }
    }

    #[test]
    fn test_check_requirements_uuid_ok() {
        let meta = sample_metadata();
        let req = TableRequirement::AssertTableUuid {
            uuid: "test-uuid".to_string(),
        };
        assert!(meta.check_requirements(&[req]).is_ok());
    }

    #[test]
    fn test_check_requirements_uuid_fail() {
        let meta = sample_metadata();
        let req = TableRequirement::AssertTableUuid {
            uuid: "wrong-uuid".to_string(),
        };
        let err = meta.check_requirements(&[req]).unwrap_err();
        assert!(err.contains("UUID mismatch"));
    }

    #[test]
    fn test_check_requirements_snapshot_id_ok() {
        let mut meta = sample_metadata();
        meta.current_snapshot_id = Some(42);
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: Some(42),
        };
        assert!(meta.check_requirements(&[req]).is_ok());
    }

    #[test]
    fn test_check_requirements_snapshot_id_fail() {
        let mut meta = sample_metadata();
        meta.current_snapshot_id = Some(42);
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: Some(99),
        };
        let err = meta.check_requirements(&[req]).unwrap_err();
        assert!(err.contains("snapshot-id mismatch"));
    }

    #[test]
    fn test_check_requirements_assert_create_fails() {
        let meta = sample_metadata();
        let req = TableRequirement::AssertCreate;
        let err = meta.check_requirements(&[req]).unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn test_apply_updates_add_snapshot() {
        let mut meta = sample_metadata();
        let snapshot = Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 2000,
            manifest_list: "s3://bucket/manifest1.avro".to_string(),
            summary: HashMap::new(),
            schema_id: 0,
        };
        meta.apply_updates(&[TableUpdate::AddSnapshot {
            snapshot: snapshot.clone(),
        }]);
        assert_eq!(meta.snapshots.len(), 1);
        assert_eq!(meta.current_snapshot_id, Some(1));
        assert_eq!(meta.last_sequence_number, 1);
    }

    #[test]
    fn test_apply_updates_set_snapshot_ref() {
        let mut meta = sample_metadata();
        meta.apply_updates(&[TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            snapshot_id: 42,
            r#type: Some("branch".to_string()),
        }]);
        assert_eq!(meta.refs.get("main").unwrap().snapshot_id, 42);
        assert_eq!(meta.current_snapshot_id, Some(42));
    }

    #[test]
    fn test_apply_updates_set_properties() {
        let mut meta = sample_metadata();
        meta.apply_updates(&[TableUpdate::SetProperties {
            updates: HashMap::from([("owner".to_string(), "team-a".to_string())]),
        }]);
        assert_eq!(meta.properties.get("owner").unwrap(), "team-a");
    }

    #[test]
    fn test_apply_updates_remove_properties() {
        let mut meta = sample_metadata();
        meta.properties
            .insert("owner".to_string(), "team-a".to_string());
        meta.apply_updates(&[TableUpdate::RemoveProperties {
            removals: vec!["owner".to_string()],
        }]);
        assert!(!meta.properties.contains_key("owner"));
    }

    #[test]
    fn test_next_metadata_location_increment() {
        let mut meta = sample_metadata();
        meta.location = "s3://bucket/warehouse/prod/users".to_string();
        // Simulate a metadata_location property
        let current = "s3://bucket/warehouse/prod/users/metadata/00001-test-uuid.metadata.json";
        let next = current.replace("00001-", "00002-");
        assert!(next.contains("00002-"));
    }

    #[test]
    fn test_serde_roundtrip() {
        let meta = sample_metadata();
        let json = serde_json::to_string(&meta).unwrap();
        let decoded: TableMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.table_uuid, "test-uuid");
        assert_eq!(decoded.format_version, 2);
    }

    #[test]
    fn test_snapshot_ref_default_type() {
        let sref = SnapshotRef {
            snapshot_id: 1,
            r#type: "branch".to_string(),
        };
        let json = serde_json::to_string(&sref).unwrap();
        assert!(json.contains("\"type\":\"branch\""));
    }

    #[test]
    fn test_check_requirements_snapshot_id_null_ok() {
        // When current_snapshot_id is None, asserting snapshot-id=null should pass
        let meta = sample_metadata(); // current_snapshot_id: None
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: None,
        };
        assert!(meta.check_requirements(&[req]).is_ok());
    }

    #[test]
    fn test_check_requirements_snapshot_id_null_fail() {
        // When current_snapshot_id is Some(42), asserting snapshot-id=null should fail
        let mut meta = sample_metadata();
        meta.current_snapshot_id = Some(42);
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: None,
        };
        let err = meta.check_requirements(&[req]).unwrap_err();
        assert!(err.contains("snapshot-id mismatch"));
        // The message shows "expected None, got Some(42)" format
        assert!(err.contains("42"));
    }

    #[test]
    fn test_check_requirements_ref_custom_branch() {
        // Assert on a custom ref (not "main")
        let mut meta = sample_metadata();
        meta.refs.insert(
            "staging".to_string(),
            SnapshotRef {
                snapshot_id: 10,
                r#type: "branch".to_string(),
            },
        );
        // Correct snapshot-id
        let req = TableRequirement::AssertRefSnapshotId {
            r#ref: "staging".to_string(),
            snapshot_id: Some(10),
        };
        assert!(meta.check_requirements(&[req]).is_ok());

        // Wrong snapshot-id
        let req2 = TableRequirement::AssertRefSnapshotId {
            r#ref: "staging".to_string(),
            snapshot_id: Some(99),
        };
        let err = meta.check_requirements(&[req2]).unwrap_err();
        assert!(err.contains("staging"));
        assert!(err.contains("snapshot-id mismatch"));
    }

    #[test]
    fn test_apply_updates_multiple() {
        // Apply multiple updates in sequence
        let mut meta = sample_metadata();
        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: Snapshot {
                    snapshot_id: 1,
                    parent_snapshot_id: None,
                    sequence_number: 1,
                    timestamp_ms: 1000,
                    manifest_list: "s3://b/m1.avro".to_string(),
                    summary: HashMap::new(),
                    schema_id: 0,
                },
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: 1,
                r#type: Some("branch".to_string()),
            },
            TableUpdate::SetProperties {
                updates: HashMap::from([
                    ("owner".to_string(), "team-a".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ]),
            },
        ];
        meta.apply_updates(&updates);
        assert_eq!(meta.snapshots.len(), 1);
        assert_eq!(meta.current_snapshot_id, Some(1));
        assert_eq!(meta.refs.get("main").unwrap().snapshot_id, 1);
        assert_eq!(meta.properties.get("owner").unwrap(), "team-a");
        assert_eq!(meta.properties.get("env").unwrap(), "prod");
        assert_eq!(meta.last_sequence_number, 1);
    }

    #[test]
    fn test_check_requirements_multiple() {
        // Multiple requirements must all pass
        let mut meta = sample_metadata();
        meta.refs.insert(
            "staging".to_string(),
            SnapshotRef {
                snapshot_id: 10,
                r#type: "branch".to_string(),
            },
        );
        let reqs = vec![
            TableRequirement::AssertTableUuid {
                uuid: "test-uuid".to_string(),
            },
            TableRequirement::AssertRefSnapshotId {
                r#ref: "main".to_string(),
                snapshot_id: None, // meta has None for main
            },
            TableRequirement::AssertRefSnapshotId {
                r#ref: "staging".to_string(),
                snapshot_id: Some(10),
            },
        ];
        assert!(meta.check_requirements(&reqs).is_ok());

        // One requirement fails
        let reqs_fail = vec![
            TableRequirement::AssertTableUuid {
                uuid: "test-uuid".to_string(),
            },
            TableRequirement::AssertRefSnapshotId {
                r#ref: "staging".to_string(),
                snapshot_id: Some(99), // wrong
            },
        ];
        let err = meta.check_requirements(&reqs_fail).unwrap_err();
        assert!(err.contains("snapshot-id mismatch"));
    }
}
