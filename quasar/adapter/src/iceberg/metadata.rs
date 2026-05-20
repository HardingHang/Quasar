//! Iceberg metadata parse/build/serialize wrapper.
//!
//! Thin wrapper around `iceberg` crate types for metadata operations.
//! REST request/response field names remain controlled by Quasar DTOs,
//! while internal metadata structures are converted to/from `iceberg` crate types.

use std::collections::HashMap;
use uuid::Uuid;

/// Parse metadata JSON into `iceberg` crate `TableMetadata`.
pub fn parse_metadata(json: &serde_json::Value) -> Result<iceberg::spec::TableMetadata, String> {
    serde_json::from_value(json.clone()).map_err(|e| format!("parse metadata failed: {e}"))
}

/// Build initial V2 metadata from schema JSON, location, and properties.
///
/// If `schema` is `None`, creates an empty schema (no fields).
/// Generates `table-uuid` from the provided `Uuid`.
pub fn build_initial_metadata(
    table_uuid: Uuid,
    location: &str,
    schema: Option<&serde_json::Value>,
    properties: HashMap<String, String>,
) -> Result<serde_json::Value, String> {
    let schema: iceberg::spec::Schema = if let Some(s) = schema {
        serde_json::from_value(s.clone()).map_err(|e| format!("parse schema: {e}"))?
    } else {
        iceberg::spec::Schema::builder()
            .with_fields(vec![])
            .build()
            .map_err(|e| format!("build empty schema: {e}"))?
    };

    let spec = iceberg::spec::UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields(vec![])
        .map_err(|e| format!("partition fields: {e}"))?
        .build();

    let sort_order = iceberg::spec::SortOrder::builder()
        .with_order_id(0)
        .build(&schema)
        .map_err(|e| format!("build sort order: {e}"))?;

    let builder = iceberg::spec::TableMetadataBuilder::new(
        schema,
        spec,
        sort_order,
        location.to_string(),
        iceberg::spec::FormatVersion::V2,
        properties,
    )
    .map_err(|e| format!("TableMetadataBuilder::new: {e}"))?;

    let result = builder.build().map_err(|e| format!("build: {e}"))?;
    let metadata = result.metadata;

    // The crate generates its own UUID; we need to override with our provided one
    // Build a fresh metadata with the correct UUID by cloning and re-serializing
    let mut json = serde_json::to_value(&metadata).map_err(|e| format!("serialize: {e}"))?;
    json["table-uuid"] = serde_json::Value::String(table_uuid.to_string());

    Ok(json)
}

/// Apply requirements and updates to existing metadata, returning the new metadata JSON.
///
/// 1. Parse current metadata from JSON.
/// 2. Check all requirements against current metadata.
/// 3. Apply all updates to build new metadata.
/// 4. Serialize and return new metadata JSON.
pub fn apply_commit(
    current_metadata: &serde_json::Value,
    requirements: &[iceberg::TableRequirement],
    updates: &[iceberg::TableUpdate],
) -> Result<serde_json::Value, String> {
    let metadata: iceberg::spec::TableMetadata =
        serde_json::from_value(current_metadata.clone())
            .map_err(|e| format!("parse current metadata: {e}"))?;

    // Check requirements
    for req in requirements {
        req.check(Some(&metadata))
            .map_err(|e| format!("requirement failed: {e}"))?;
    }

    // Apply updates
    let builder = metadata.into_builder(None);
    let builder = updates.iter().try_fold(builder, |b, update| {
        update
            .clone()
            .apply(b)
            .map_err(|e| format!("apply update failed: {e}"))
    })?;

    let result = builder
        .build()
        .map_err(|e| format!("build after updates: {e}"))?;

    serde_json::to_value(&result.metadata).map_err(|e| format!("serialize: {e}"))
}

/// Generate the next metadata file location by incrementing the sequence number.
///
/// Expected format: `{...}/metadata/{NNNNN}-{uuid}.metadata.json`
/// Returns an error if the current location format is unrecognized.
pub fn next_metadata_location(current: &str) -> Result<String, String> {
    if let Some(metadata_idx) = current.rfind("/metadata/") {
        let prefix = &current[..metadata_idx + 10];
        let rest = &current[metadata_idx + 10..];
        if let Some(dash_idx) = rest.find('-') {
            let seq_str = &rest[..dash_idx];
            if let Ok(seq) = seq_str.parse::<u64>() {
                let suffix = &rest[dash_idx..];
                let new_seq = seq + 1;
                let new_width = seq_str.len().max(new_seq.to_string().len());
                let new_seq_str = format!("{:0width$}", new_seq, width = new_width);
                return Ok(format!("{prefix}{new_seq_str}{suffix}"));
            }
        }
    }
    Err(format!("unrecognized metadata location format: {current}"))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metadata_valid_v2() {
        let json = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://bucket/warehouse/test",
            "last-sequence-number": 0,
            "last-updated-ms": 1000,
            "last-column-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {}
        });

        let metadata = parse_metadata(&json).expect("parse should succeed");
        assert_eq!(metadata.format_version(), iceberg::spec::FormatVersion::V2);
        assert_eq!(metadata.location(), "s3://bucket/warehouse/test");
    }

    #[test]
    fn test_build_initial_metadata_v2() {
        let uuid = Uuid::new_v4();
        let location = "s3://bucket/warehouse/db/table";
        let properties = HashMap::new();

        let json =
            build_initial_metadata(uuid, location, None, properties).expect("build should succeed");

        assert_eq!(json["format-version"], 2);
        assert_eq!(json["location"], location);
        assert_eq!(json["table-uuid"], uuid.to_string());
        assert!(!json["schemas"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_build_initial_metadata_with_schema() {
        let uuid = Uuid::new_v4();
        let schema_json = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "type": "long", "required": true}
            ]
        });
        let properties = HashMap::new();

        let json = build_initial_metadata(
            uuid,
            "s3://bucket/warehouse/db/table",
            Some(&schema_json),
            properties,
        )
        .expect("build should succeed");

        assert_eq!(json["format-version"], 2);
        let schemas = json["schemas"].as_array().unwrap();
        assert_eq!(schemas.len(), 1);
    }

    #[test]
    fn test_apply_commit_set_properties() {
        let current = serde_json::json!({
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://bucket/warehouse/test",
            "last-sequence-number": 0,
            "last-updated-ms": 1000,
            "last-column-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {}
        });

        let mut updates = HashMap::new();
        updates.insert("owner".to_string(), "team-a".to_string());

        let new_metadata = apply_commit(
            &current,
            &[],
            &[iceberg::TableUpdate::SetProperties { updates }],
        )
        .expect("commit should succeed");

        let props = new_metadata["properties"].as_object().unwrap();
        assert_eq!(props.get("owner").unwrap().as_str().unwrap(), "team-a");
    }

    #[test]
    fn test_apply_commit_requirement_uuid_match() {
        let table_uuid = uuid::Uuid::new_v4();
        let current = serde_json::json!({
            "format-version": 2,
            "table-uuid": table_uuid.to_string(),
            "location": "s3://bucket/warehouse/test",
            "last-sequence-number": 0,
            "last-updated-ms": 1000,
            "last-column-id": 0,
            "schemas": [{"type": "struct", "schema-id": 0, "fields": []}],
            "current-schema-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 999,
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {}
        });

        // Matching UUID should pass
        let result = apply_commit(
            &current,
            &[iceberg::TableRequirement::UuidMatch { uuid: table_uuid }],
            &[],
        );
        assert!(result.is_ok(), "UUID match should pass: {:?}", result.err());

        // Non-matching UUID should fail
        let result2 = apply_commit(
            &current,
            &[iceberg::TableRequirement::UuidMatch {
                uuid: uuid::Uuid::new_v4(),
            }],
            &[],
        );
        assert!(result2.is_err(), "UUID mismatch should fail");
    }

    #[test]
    fn test_next_metadata_location_basic() {
        let current = "s3://bucket/warehouse/db/table/metadata/00001-abc.metadata.json";
        assert_eq!(
            next_metadata_location(current).unwrap(),
            "s3://bucket/warehouse/db/table/metadata/00002-abc.metadata.json"
        );
    }

    #[test]
    fn test_next_metadata_location_width_overflow() {
        let current = "s3://bucket/metadata/00099-abc.metadata.json";
        assert_eq!(
            next_metadata_location(current).unwrap(),
            "s3://bucket/metadata/00100-abc.metadata.json"
        );
    }

    #[test]
    fn test_next_metadata_location_unrecognized() {
        let result = next_metadata_location("s3://bucket/unusual/location.json");
        assert!(result.is_err());
    }

    #[test]
    fn test_wire_name_add_spec_deserializes() {
        // Verify the official wire name "add-spec" deserializes correctly
        let json = serde_json::json!({
            "action": "add-spec",
            "spec": {"spec-id": 1, "fields": []}
        });
        let update: iceberg::TableUpdate =
            serde_json::from_value(json).expect("deserialize add-spec");
        match update {
            iceberg::TableUpdate::AddSpec { .. } => {}
            other => panic!("expected AddSpec, got {:?}", other),
        }
    }

    #[test]
    fn test_wire_name_assert_create_deserializes() {
        let json = serde_json::json!({"type": "assert-create"});
        let req: iceberg::TableRequirement =
            serde_json::from_value(json).expect("deserialize assert-create");
        match req {
            iceberg::TableRequirement::NotExist => {}
            other => panic!("expected NotExist, got {:?}", other),
        }
    }
}
