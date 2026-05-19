//! Temporary validation module for iceberg crate capabilities.
//! This module verifies the 3 critical capabilities required by V4_DESIGN §3.1.1.
//! After validation, this file may be deleted or converted to proper tests.

use serde_json;

/// Validation 1: V2 format support
/// Build TableMetadataBuilder with FormatVersion::V2 and verify output.
#[cfg(test)]
mod validation_tests {
    use super::*;

    fn build_empty_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_fields(vec![])
            .build()
            .expect("schema build failed")
    }

    fn build_empty_spec() -> iceberg::spec::UnboundPartitionSpec {
        iceberg::spec::UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_fields(vec![])
            .expect("partition fields")
            .build()
    }

    fn build_empty_sort_order(schema: &iceberg::spec::Schema) -> iceberg::spec::SortOrder {
        iceberg::spec::SortOrder::builder()
            .with_order_id(0)
            .build(schema)
            .expect("sort order build failed")
    }

    #[test]
    fn test_v2_format_support() {
        let schema = build_empty_schema();
        let spec = build_empty_spec();
        let sort_order = build_empty_sort_order(&schema);
        let properties = std::collections::HashMap::new();

        let builder = iceberg::spec::TableMetadataBuilder::new(
            schema,
            spec,
            sort_order,
            "s3://bucket/warehouse/test".to_string(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .expect("TableMetadataBuilder::new failed");

        let build_result = builder.build();
        assert!(
            build_result.is_ok(),
            "build() failed: {:?}",
            build_result.err()
        );

        let metadata = build_result.unwrap().metadata;
        let json = serde_json::to_value(&metadata).expect("serialize failed");
        assert_eq!(json["format-version"], 2, "format-version should be 2");
        println!("V2 format support: OK");
    }

    #[test]
    fn test_table_update_serde_wire_names() {
        // Test what serde name the crate uses for TableUpdate variants
        let update = iceberg::TableUpdate::SetProperties {
            updates: std::collections::HashMap::new(),
        };
        let json = serde_json::to_value(&update).expect("serialize failed");
        println!("TableUpdate::SetProperties serialized as: {}", json);

        // Test AddSpec (the critical one)
        let _schema = build_empty_schema();
        let unbound_spec = iceberg::spec::UnboundPartitionSpec::builder()
            .with_spec_id(0)
            .add_partition_fields(vec![])
            .unwrap()
            .build();
        let update2 = iceberg::TableUpdate::AddSpec { spec: unbound_spec };
        let json2 = serde_json::to_value(&update2).expect("serialize failed");
        println!("TableUpdate::AddSpec serialized as: {}", json2);

        // The discriminator key should tell us the wire name
        if let Some(obj) = json2.as_object() {
            for (k, _v) in obj {
                println!("  key: {}", k);
            }
        }
    }

    #[test]
    fn test_table_requirement_serde_wire_names() {
        let req = iceberg::TableRequirement::NotExist;
        let json = serde_json::to_value(&req).expect("serialize failed");
        println!("TableRequirement::NotExist serialized as: {}", json);

        let req2 = iceberg::TableRequirement::UuidMatch {
            uuid: uuid::Uuid::new_v4(),
        };
        let json2 = serde_json::to_value(&req2).expect("serialize failed");
        println!("TableRequirement::UuidMatch serialized as: {}", json2);
    }

    #[test]
    fn test_metadata_builder_from_existing() {
        let schema = build_empty_schema();
        let spec = build_empty_spec();
        let sort_order = build_empty_sort_order(&schema);
        let properties = std::collections::HashMap::new();

        let builder = iceberg::spec::TableMetadataBuilder::new(
            schema.clone(),
            spec,
            sort_order,
            "s3://bucket/warehouse/test".to_string(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap();

        let metadata = builder.build().unwrap().metadata;

        // Test into_builder
        let builder2 = metadata.into_builder(Some("test-location".to_string()));
        let new_metadata = builder2.build().unwrap().metadata;
        let json = serde_json::to_value(&new_metadata).unwrap();
        assert_eq!(json["format-version"], 2);
        println!("into_builder works: OK");
    }

    #[test]
    fn test_table_update_apply() {
        let schema = build_empty_schema();
        let spec = build_empty_spec();
        let sort_order = build_empty_sort_order(&schema);
        let properties = std::collections::HashMap::new();

        let builder = iceberg::spec::TableMetadataBuilder::new(
            schema.clone(),
            spec,
            sort_order,
            "s3://bucket/warehouse/test".to_string(),
            iceberg::spec::FormatVersion::V2,
            properties,
        )
        .unwrap();

        let metadata = builder.build().unwrap().metadata;
        let builder2 = metadata.into_builder(None);

        // Apply a SetProperties update
        let mut updates = std::collections::HashMap::new();
        updates.insert("owner".to_string(), "team-a".to_string());
        let update = iceberg::TableUpdate::SetProperties { updates };
        let builder3 = update.apply(builder2).expect("apply failed");

        let new_metadata = builder3.build().unwrap().metadata;
        let props = new_metadata.properties();
        assert_eq!(props.get("owner"), Some(&"team-a".to_string()));
        println!("TableUpdate::apply works: OK");
    }
}
