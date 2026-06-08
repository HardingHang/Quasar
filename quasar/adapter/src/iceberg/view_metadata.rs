//! Iceberg view metadata parse/build/serialize wrapper.
//!
//! Thin wrapper around `iceberg` crate types for view metadata operations.
//! REST request/response field names remain controlled by Quasar DTOs,
//! while internal metadata structures are converted to/from `iceberg` crate types.

use serde::Deserialize;
use std::collections::HashMap;
use uuid::Uuid;

/// View commit requirement (Iceberg 1.10.x).
/// The iceberg crate 0.9.1 does not provide this type, so it is defined
/// in the adapter layer.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRequirement {
    /// View must not exist for the requirement to pass.
    AssertCreate,
    /// View UUID must match the requested value.
    AssertViewUuid {
        #[serde(rename = "uuid")]
        uuid: Uuid,
    },
}

/// Check view requirements against current metadata.
///
/// Returns `Err(message)` if any requirement fails.
pub fn check_view_requirements(
    requirements: &[ViewRequirement],
    current_metadata: Option<&iceberg::spec::ViewMetadata>,
) -> Result<(), String> {
    for req in requirements {
        match req {
            ViewRequirement::AssertCreate => {
                if current_metadata.is_some() {
                    return Err("View already exists, cannot assert-create".to_string());
                }
            }
            ViewRequirement::AssertViewUuid { uuid } => {
                let metadata =
                    current_metadata.ok_or("View does not exist, cannot assert-view-uuid")?;
                if metadata.uuid() != *uuid {
                    return Err(format!(
                        "View UUID mismatch: expected {}, found {}",
                        uuid,
                        metadata.uuid()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Build initial view metadata as JSON directly.
///
/// The iceberg crate 0.9.1's `ViewRepresentations` and `ViewVersion` types
/// have `pub(crate)` constructors, so we build the metadata JSON by hand
/// and then validate it by parsing through the crate.
pub fn build_initial_view_metadata(
    view_uuid: Uuid,
    location: &str,
    schema: &serde_json::Value,
    _representations: Vec<iceberg::spec::ViewRepresentation>,
    default_namespace: Vec<String>,
    default_catalog: Option<String>,
    properties: HashMap<String, String>,
) -> Result<serde_json::Value, String> {
    let now = chrono::Utc::now().timestamp_millis();

    // Build the initial metadata JSON manually to avoid pub(crate) constructors
    let metadata = serde_json::json!({
        "format-version": 1,
        "view-uuid": view_uuid.to_string(),
        "location": location,
        "current-version-id": 1,
        "properties": properties,
        "schemas": [schema],
        "versions": [{
            "version-id": 1,
            "schema-id": 0,
            "timestamp-ms": now,
            "summary": {},
            "representations": build_representations_json(
                _representations,
                default_catalog.clone(),
                default_namespace.clone()
            )?,
            "default-catalog": default_catalog,
            "default-namespace": default_namespace
        }],
        "version-log": [{
            "version-id": 1,
            "timestamp-ms": now
        }]
    });

    // Validate by parsing through the iceberg crate
    let _: iceberg::spec::ViewMetadata = serde_json::from_value(metadata.clone())
        .map_err(|e| format!("view metadata validation failed: {e}"))?;

    Ok(metadata)
}

/// Convert ViewRepresentation list to JSON array.
fn build_representations_json(
    representations: Vec<iceberg::spec::ViewRepresentation>,
    _default_catalog: Option<String>,
    _default_namespace: Vec<String>,
) -> Result<Vec<serde_json::Value>, String> {
    representations
        .into_iter()
        .map(|r| match r {
            iceberg::spec::ViewRepresentation::Sql(sql) => Ok(serde_json::json!({
                "type": "sql",
                "sql": sql.sql,
                "dialect": sql.dialect
            })),
        })
        .collect()
}

/// Apply view requirements and updates to existing metadata, returning the new metadata JSON.
///
/// 1. Parse current metadata from JSON.
/// 2. Check all requirements against current metadata.
/// 3. Apply all updates by matching ViewUpdate variants to builder methods.
/// 4. Serialize and return new metadata JSON.
pub fn apply_view_commit(
    current_metadata: &serde_json::Value,
    requirements: &[ViewRequirement],
    updates: &[iceberg::ViewUpdate],
) -> Result<serde_json::Value, String> {
    let metadata: iceberg::spec::ViewMetadata = serde_json::from_value(current_metadata.clone())
        .map_err(|e| format!("parse view metadata: {e}"))?;

    // Check requirements (adapter-level manual implementation)
    check_view_requirements(requirements, Some(&metadata))?;

    // Apply updates by matching each ViewUpdate variant
    let mut builder = iceberg::spec::ViewMetadataBuilder::new_from_metadata(metadata);

    for update in updates {
        match update {
            iceberg::ViewUpdate::AssignUuid { uuid } => {
                builder = builder.assign_uuid(*uuid);
            }
            iceberg::ViewUpdate::UpgradeFormatVersion { format_version } => {
                builder = builder
                    .upgrade_format_version(*format_version)
                    .map_err(|e| format!("upgrade format version: {e}"))?;
            }
            iceberg::ViewUpdate::AddSchema {
                schema,
                last_column_id,
            } => {
                builder = builder.add_schema(schema.clone());
                let _ = last_column_id;
            }
            iceberg::ViewUpdate::SetLocation { location } => {
                builder = builder.set_location(location.clone());
            }
            iceberg::ViewUpdate::SetProperties { updates } => {
                builder = builder
                    .set_properties(updates.clone())
                    .map_err(|e| format!("set properties: {e}"))?;
            }
            iceberg::ViewUpdate::RemoveProperties { removals } => {
                builder = builder.remove_properties(removals);
            }
            iceberg::ViewUpdate::AddViewVersion { view_version } => {
                builder = builder
                    .add_version(view_version.clone())
                    .map_err(|e| format!("add version: {e}"))?;
            }
            iceberg::ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                builder = builder
                    .set_current_version_id(*view_version_id)
                    .map_err(|e| format!("set current version: {e}"))?;
            }
        }
    }

    let result = builder
        .build()
        .map_err(|e| format!("build view metadata: {e}"))?;
    serde_json::to_value(&result.metadata).map_err(|e| format!("serialize: {e}"))
}

/// Generate the next metadata file location by incrementing the sequence number.
///
/// Expected format: `{...}/metadata/{NNNNN}-{uuid}.metadata.json`
/// Returns an error if the current location format is unrecognized.
pub fn next_view_metadata_location(current: &str) -> Result<String, String> {
    // Reuse the same logic as table metadata location
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_view_requirement_assert_create_passes_when_none() {
        assert!(check_view_requirements(&[ViewRequirement::AssertCreate], None).is_ok());
    }

    #[test]
    fn test_view_requirement_assert_create_fails_when_some() {
        let metadata = create_test_metadata();
        let result = check_view_requirements(&[ViewRequirement::AssertCreate], Some(&metadata));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_view_requirement_assert_uuid_passes() {
        let uuid = Uuid::new_v4();
        let metadata = create_test_metadata_with_uuid(uuid);
        assert!(check_view_requirements(
            &[ViewRequirement::AssertViewUuid { uuid }],
            Some(&metadata),
        )
        .is_ok());
    }

    #[test]
    fn test_view_requirement_assert_uuid_fails() {
        let metadata = create_test_metadata_with_uuid(Uuid::new_v4());
        let wrong_uuid = Uuid::new_v4();
        let result = check_view_requirements(
            &[ViewRequirement::AssertViewUuid { uuid: wrong_uuid }],
            Some(&metadata),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("mismatch"));
    }

    #[test]
    fn test_next_view_metadata_location_basic() {
        let current = "s3://bucket/warehouse/db/view/metadata/00001-abc.metadata.json";
        assert_eq!(
            next_view_metadata_location(current).unwrap(),
            "s3://bucket/warehouse/db/view/metadata/00002-abc.metadata.json"
        );
    }

    #[test]
    fn test_next_view_metadata_location_width_overflow() {
        let current = "s3://bucket/metadata/00099-abc.metadata.json";
        assert_eq!(
            next_view_metadata_location(current).unwrap(),
            "s3://bucket/metadata/00100-abc.metadata.json"
        );
    }

    #[test]
    fn test_build_initial_view_metadata() {
        let schema = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": []
        });

        let reps = vec![iceberg::spec::ViewRepresentation::Sql(
            iceberg::spec::SqlViewRepresentation {
                sql: "SELECT 1".to_string(),
                dialect: "spark".to_string(),
            },
        )];

        let result = build_initial_view_metadata(
            Uuid::new_v4(),
            "s3://bucket/view",
            &schema,
            reps,
            vec!["default".to_string()],
            None,
            HashMap::new(),
        );

        assert!(result.is_ok(), "build failed: {:?}", result.err());
        let metadata = result.unwrap();
        assert_eq!(metadata["format-version"], 1);
        assert_eq!(metadata["current-version-id"], 1);
    }

    fn create_test_metadata() -> iceberg::spec::ViewMetadata {
        create_test_metadata_with_uuid(Uuid::new_v4())
    }

    fn create_test_metadata_with_uuid(view_uuid: Uuid) -> iceberg::spec::ViewMetadata {
        let schema = serde_json::json!({
            "type": "struct",
            "schema-id": 0,
            "fields": []
        });

        let reps = vec![iceberg::spec::ViewRepresentation::Sql(
            iceberg::spec::SqlViewRepresentation {
                sql: "SELECT 1".to_string(),
                dialect: "spark".to_string(),
            },
        )];

        let metadata = build_initial_view_metadata(
            view_uuid,
            "s3://bucket/view",
            &schema,
            reps,
            vec!["default".to_string()],
            None,
            HashMap::new(),
        )
        .unwrap();

        serde_json::from_value(metadata).unwrap()
    }
}
