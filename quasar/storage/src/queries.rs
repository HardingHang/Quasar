//! Centralized SQL constants for V3 Data Core Model queries.
//!
//! These constants are defined in Phase 1 of the V3 implementation as the
//! single place where SQL strings live. `PgCatalogStore` switches its inline
//! V1 SQL to these constants in Phase 2 once the V3 schema is wired through
//! `crate::schema`.
//!
//! Conventions:
//! - constant names use `SCREAMING_SNAKE_CASE`;
//! - multi-line SQL uses `r#"..."#` raw strings, SQL keywords are uppercase;
//! - placeholders are positional `$1`, `$2`, ...;
//! - lookups by `(domain, namespace, asset)` join through the registry tables
//!   so callers do not have to thread UUIDs in handler code;
//! - statements that mutate state always `RETURNING` the canonical column set
//!   so callers can rebuild the model without an extra round-trip.
//!
//! Phase 2 C3 keeps the V2 trait surface, so the domain-, version-,
//! and identity-level constants (Domain* CRUD, version::GET_LATEST,
//! version::LIST_BY_ASSET, asset::LOOKUP_ID_BY_NAME) are not yet referenced.
//! C4 lights them up when `DomainStore`, `VersionStore`, and the V3-shaped
//! `AssetStore` methods land. The file-level `dead_code` allow keeps clippy
//! quiet until then and is removed in C4.
#![allow(dead_code)]

/// Queries against the `domains` table (V3 §3.3.2, §5.3).
pub mod domain {
    /// Insert a new domain. Parameters: $1 name, $2 comment, $3 properties,
    /// $4 storage_type, $5 storage_config, $6 warehouse, $7 owner.
    pub const CREATE: &str = r#"
        INSERT INTO domains (name, comment, properties, storage_type, storage_config, warehouse, owner)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
    "#;

    /// List domains in name order. Parameters: $1 limit, $2 offset.
    pub const LIST: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
        FROM domains
        ORDER BY name
        LIMIT $1 OFFSET $2
    "#;

    /// Look up a domain by name. Parameter: $1 name.
    pub const GET_BY_NAME: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
        FROM domains
        WHERE name = $1
    "#;

    /// Check whether a domain exists by name. Parameter: $1 name.
    pub const EXISTS: &str = r#"
        SELECT EXISTS(SELECT 1 FROM domains WHERE name = $1)
    "#;

    /// Delete an empty domain. Fails with FK RESTRICT if namespaces remain.
    /// Parameter: $1 name.
    pub const DELETE: &str = r#"
        DELETE FROM domains WHERE name = $1
    "#;

    /// Update mutable fields of a domain. Parameters: $1 comment,
    /// $2 properties, $3 storage_type, $4 storage_config, $5 warehouse,
    /// $6 owner, $7 name (target).
    pub const UPDATE: &str = r#"
        UPDATE domains
        SET comment = $1,
            properties = $2,
            storage_type = $3,
            storage_config = $4,
            warehouse = $5,
            owner = $6,
            updated_at = NOW()
        WHERE name = $7
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
    "#;
}

/// Queries against the `namespaces` table (V3 §3.3.3, §5.3).
pub mod namespace {
    /// Insert a namespace under an existing domain (resolved by name). The
    /// SELECT-source subquery returns zero rows when the domain does not
    /// exist, so the INSERT inserts zero rows and the caller can detect that
    /// case via `RETURNING`. Parameters: $1 domain_name, $2 name,
    /// $3 comment, $4 properties.
    pub const CREATE: &str = r#"
        INSERT INTO namespaces (domain_id, name, comment, properties)
        SELECT d.id, $2, $3, $4
        FROM domains d
        WHERE d.name = $1
        RETURNING id, domain_id, name, comment, properties, created_at, updated_at
    "#;

    /// List namespaces under a domain (resolved by domain name).
    /// Parameters: $1 domain_name, $2 limit, $3 offset.
    pub const LIST_BY_DOMAIN: &str = r#"
        SELECT n.id, n.domain_id, n.name, n.comment, n.properties, n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON n.domain_id = d.id
        WHERE d.name = $1
        ORDER BY n.name
        LIMIT $2 OFFSET $3
    "#;

    /// Look up a namespace by (domain_name, namespace_name).
    /// Parameters: $1 domain_name, $2 namespace_name.
    pub const GET_BY_NAME: &str = r#"
        SELECT n.id, n.domain_id, n.name, n.comment, n.properties, n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON n.domain_id = d.id
        WHERE d.name = $1 AND n.name = $2
    "#;

    /// Check whether a namespace exists. Parameters: $1 domain_name, $2 name.
    pub const EXISTS: &str = r#"
        SELECT EXISTS(
            SELECT 1
            FROM namespaces n
            JOIN domains d ON n.domain_id = d.id
            WHERE d.name = $1 AND n.name = $2
        )
    "#;

    /// Delete an empty namespace. Fails with FK RESTRICT if assets remain.
    /// Parameters: $1 domain_name, $2 namespace_name.
    pub const DELETE: &str = r#"
        DELETE FROM namespaces n
        USING domains d
        WHERE n.domain_id = d.id AND d.name = $1 AND n.name = $2
    "#;

    /// Update a namespace's comment and properties.
    /// Parameters: $1 comment, $2 properties, $3 domain_name, $4 namespace_name.
    pub const UPDATE: &str = r#"
        UPDATE namespaces n
        SET comment = $1,
            properties = $2,
            updated_at = NOW()
        FROM domains d
        WHERE n.domain_id = d.id AND d.name = $3 AND n.name = $4
        RETURNING n.id, n.domain_id, n.name, n.comment, n.properties, n.created_at, n.updated_at
    "#;
}

/// Queries against `assets` and `tabular_assets` (V3 §3.3.4-5, §5.3).
pub mod asset {
    /// Insert a generic asset under `(domain, namespace)` resolved by name.
    /// The SELECT-source returns zero rows when the namespace does not exist,
    /// so the INSERT inserts zero rows and the caller can detect that case via
    /// `RETURNING`. Parameters: $1 domain_name, $2 namespace_name, $3 name,
    /// $4 asset_type, $5 comment, $6 properties.
    pub const CREATE: &str = r#"
        INSERT INTO assets (namespace_id, name, asset_type, comment, properties)
        SELECT ns.id, $3, $4, $5, $6
        FROM namespaces ns
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2
        RETURNING id, namespace_id, name, asset_type, comment, properties, deleted_at,
                  created_by, updated_by, created_at, updated_at
    "#;

    /// Look up an active asset by (domain_name, namespace_name, asset_name).
    /// Active means `deleted_at IS NULL`. Parameters: $1 domain_name,
    /// $2 namespace_name, $3 asset_name.
    pub const GET_BY_NAME: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties,
               a.deleted_at, a.created_by, a.updated_by, a.created_at, a.updated_at
        FROM assets a
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3 AND a.deleted_at IS NULL
    "#;

    /// Join an active asset with its tabular extension filtered by format.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 asset_name, $4 format.
    pub const GET_TABULAR_BY_NAME: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties,
               a.deleted_at, a.created_by, a.updated_by, a.created_at, a.updated_at,
               ta.asset_id, ta.format, ta.location, ta.metadata_location, ta.schema_snapshot,
               ta.created_at AS tabular_created_at, ta.updated_at AS tabular_updated_at
        FROM assets a
        JOIN tabular_assets ta ON a.id = ta.asset_id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL AND a.asset_type = 'table' AND ta.format = $4
    "#;

    /// List active tabular assets in a namespace, optionally filtered by format.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 format filter
    /// (TEXT, NULL = no filter), $4 limit, $5 offset.
    pub const LIST_TABULAR_BY_NAMESPACE: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties,
               a.deleted_at, a.created_by, a.updated_by, a.created_at, a.updated_at,
               ta.asset_id, ta.format, ta.location, ta.metadata_location, ta.schema_snapshot,
               ta.created_at AS tabular_created_at, ta.updated_at AS tabular_updated_at
        FROM assets a
        JOIN tabular_assets ta ON a.id = ta.asset_id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2
          AND a.deleted_at IS NULL AND a.asset_type = 'table'
          AND ($3::TEXT IS NULL OR ta.format = $3)
        ORDER BY a.name
        LIMIT $4 OFFSET $5
    "#;

    /// Insert a tabular asset extension row. Parameters: $1 asset_id, $2 format,
    /// $3 location, $4 metadata_location, $5 schema_snapshot.
    pub const CREATE_TABULAR: &str = r#"
        INSERT INTO tabular_assets (asset_id, format, location, metadata_location, schema_snapshot)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING asset_id, format, location, metadata_location, schema_snapshot,
                  created_at AS tabular_created_at, updated_at AS tabular_updated_at
    "#;

    /// Update comment and properties on the active asset, returning the new row.
    /// Parameters: $1 comment, $2 properties, $3 asset_id.
    pub const UPDATE_PROPERTIES: &str = r#"
        UPDATE assets
        SET comment = $1,
            properties = $2,
            updated_at = NOW()
        WHERE id = $3 AND deleted_at IS NULL
        RETURNING id, namespace_id, name, asset_type, comment, properties, deleted_at,
                  created_by, updated_by, created_at, updated_at
    "#;

    /// Rename an active asset within the same namespace.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 current_name,
    /// $4 new_name.
    pub const RENAME: &str = r#"
        UPDATE assets a
        SET name = $4,
            updated_at = NOW()
        FROM namespaces ns, domains d
        WHERE ns.id = a.namespace_id
          AND d.id = ns.domain_id
          AND d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL
    "#;

    /// Rename an active asset and move it to a different namespace
    /// within the same domain. Parameters: $1 domain_name, $2 src_namespace_name,
    /// $3 current_name, $4 new_name, $5 new_namespace_id.
    pub const RENAME_WITH_NAMESPACE: &str = r#"
        UPDATE assets a
        SET name = $4,
            namespace_id = $5,
            updated_at = NOW()
        FROM namespaces ns, domains d
        WHERE ns.id = a.namespace_id
          AND d.id = ns.domain_id
          AND d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL
    "#;

    /// Hard delete an asset; cascades to extension and version rows via FK.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 asset_name.
    pub const DELETE: &str = r#"
        DELETE FROM assets a
        USING namespaces ns, domains d
        WHERE ns.id = a.namespace_id
          AND d.id = ns.domain_id
          AND d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL
    "#;

    /// Check whether a tabular asset of a specific format exists with the
    /// given (domain, namespace, name) tuple.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 asset_name, $4 format.
    pub const EXISTS_TABULAR: &str = r#"
        SELECT EXISTS(
            SELECT 1
            FROM assets a
            JOIN tabular_assets ta ON a.id = ta.asset_id
            JOIN namespaces ns ON a.namespace_id = ns.id
            JOIN domains d ON ns.domain_id = d.id
            WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
              AND a.deleted_at IS NULL AND a.asset_type = 'table' AND ta.format = $4
        )
    "#;

    /// Compare-and-swap update of a tabular asset's `metadata_location`. The
    /// row is only updated when its current `metadata_location` equals the
    /// caller-supplied expected value, giving Iceberg-style optimistic
    /// concurrency. Callers detect mismatch by checking that the statement
    /// returned a row (0 rows → `Conflict`).
    ///
    /// Parameters: $1 new_location, $2 new_schema_snapshot (JSONB, nullable),
    /// $3 domain_name, $4 namespace_name, $5 asset_name, $6 format,
    /// $7 expected_location.
    pub const CAS_UPDATE_METADATA_LOCATION: &str = r#"
        UPDATE tabular_assets ta
        SET metadata_location = $1,
            schema_snapshot = COALESCE($2, ta.schema_snapshot),
            updated_at = NOW()
        FROM assets a
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE ta.asset_id = a.id
          AND d.name = $3 AND ns.name = $4 AND a.name = $5 AND ta.format = $6
          AND a.deleted_at IS NULL AND a.asset_type = 'table'
          AND ta.metadata_location IS NOT DISTINCT FROM $7
        RETURNING ta.asset_id
    "#;

    /// Update an active asset's properties in place. Used as the second
    /// statement of a CAS commit transaction so the property delta lands
    /// atomically with the metadata_location swap. Parameters: $1 properties
    /// (JSONB), $2 asset_id.
    pub const UPDATE_PROPERTIES_BY_ID: &str = r#"
        UPDATE assets
        SET properties = $1,
            updated_at = NOW()
        WHERE id = $2 AND deleted_at IS NULL
    "#;

    /// Unified list of active assets in a namespace. `LEFT JOIN tabular_assets`
    /// lets future non-tabular asset types appear as `(Asset, None)`. Today
    /// every row pairs with a tabular extension. Parameters: $1 domain_name,
    /// $2 namespace_name, $3 format filter (TEXT, NULL = no filter),
    /// $4 name filter (TEXT, NULL = no filter), $5 limit, $6 offset.
    pub const LIST_UNIFIED: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties,
               a.deleted_at, a.created_by, a.updated_by, a.created_at, a.updated_at,
               ta.asset_id, ta.format, ta.location, ta.metadata_location, ta.schema_snapshot,
               ta.created_at AS tabular_created_at, ta.updated_at AS tabular_updated_at
        FROM assets a
        LEFT JOIN tabular_assets ta ON a.id = ta.asset_id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2
          AND a.deleted_at IS NULL
          AND ($3::TEXT IS NULL OR ta.format = $3)
          AND ($4::TEXT IS NULL OR a.name = $4)
        ORDER BY a.name, ta.format NULLS LAST
        LIMIT $5 OFFSET $6
    "#;

    /// Unified single-asset lookup. Returns the asset with its optional
    /// tabular extension; non-tabular asset types map to a `None` extension.
    /// Parameters: $1 domain_name, $2 namespace_name, $3 asset_name.
    pub const GET_UNIFIED: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties,
               a.deleted_at, a.created_by, a.updated_by, a.created_at, a.updated_at,
               ta.asset_id, ta.format, ta.location, ta.metadata_location, ta.schema_snapshot,
               ta.created_at AS tabular_created_at, ta.updated_at AS tabular_updated_at
        FROM assets a
        LEFT JOIN tabular_assets ta ON a.id = ta.asset_id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL
    "#;

    /// Resolve the active asset id for `(domain, namespace, name)`. Used by
    /// version operations that need an `asset_id` before issuing the version
    /// query. Parameters: $1 domain_name, $2 namespace_name, $3 asset_name.
    pub const LOOKUP_ID_BY_NAME: &str = r#"
        SELECT a.id
        FROM assets a
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL
    "#;

    /// Check whether an active asset exists by (domain, namespace, name).
    /// Parameters: $1 domain_name, $2 namespace_name, $3 asset_name.
    pub const EXISTS: &str = r#"
        SELECT EXISTS(
            SELECT 1 FROM assets a
            JOIN namespaces ns ON a.namespace_id = ns.id
            JOIN domains d ON ns.domain_id = d.id
            WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
              AND a.deleted_at IS NULL
        )
    "#;
}

/// Queries against `asset_versions` and `tabular_asset_versions` (V3 §3.3.6-7).
pub mod version {
    /// Insert a generic version row. Parameters: $1 asset_id, $2 version_key,
    /// $3 version_order, $4 previous_version_id, $5 comment, $6 properties.
    pub const CREATE: &str = r#"
        INSERT INTO asset_versions (asset_id, version_key, version_order, previous_version_id, comment, properties)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
    "#;

    /// Insert the tabular extension for a version. Parameters: $1 version_id,
    /// $2 metadata_location.
    pub const CREATE_TABULAR: &str = r#"
        INSERT INTO tabular_asset_versions (version_id, metadata_location)
        VALUES ($1, $2)
        RETURNING version_id, metadata_location, created_at AS tabular_created_at
    "#;

    /// Get the highest version_order for an asset. Returns NULL if the asset
    /// has only versions without `version_order`. Parameter: $1 asset_id.
    pub const GET_LATEST: &str = r#"
        SELECT id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
        FROM asset_versions
        WHERE asset_id = $1 AND version_order IS NOT NULL
        ORDER BY version_order DESC
        LIMIT 1
    "#;

    /// Get the highest version_order joined with its tabular extension.
    /// Parameter: $1 asset_id.
    pub const GET_LATEST_TABULAR: &str = r#"
        SELECT av.id, av.asset_id, av.version_key, av.version_order, av.previous_version_id,
               av.comment, av.properties, av.created_at,
               tav.version_id, tav.metadata_location, tav.created_at AS tabular_created_at
        FROM asset_versions av
        JOIN tabular_asset_versions tav ON av.id = tav.version_id
        WHERE av.asset_id = $1 AND av.version_order IS NOT NULL
        ORDER BY av.version_order DESC
        LIMIT 1
    "#;

    /// Look up a version by its native key. Parameters: $1 asset_id, $2 version_key.
    pub const GET_BY_KEY: &str = r#"
        SELECT id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
        FROM asset_versions
        WHERE asset_id = $1 AND version_key = $2
    "#;

    /// List all versions of an asset in ascending order (NULL orders last).
    /// Parameter: $1 asset_id.
    pub const LIST_BY_ASSET: &str = r#"
        SELECT id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
        FROM asset_versions
        WHERE asset_id = $1
        ORDER BY version_order ASC NULLS LAST
    "#;

    /// Look up a tabular version by (asset_id, version_key) joined with its
    /// tabular extension. Parameters: $1 asset_id, $2 version_key.
    pub const GET_TABULAR_BY_KEY: &str = r#"
        SELECT av.id, av.asset_id, av.version_key, av.version_order, av.previous_version_id,
               av.comment, av.properties, av.created_at,
               tav.version_id, tav.metadata_location,
               tav.created_at AS tabular_created_at
        FROM asset_versions av
        JOIN tabular_asset_versions tav ON av.id = tav.version_id
        WHERE av.asset_id = $1 AND av.version_key = $2
    "#;

    /// List all tabular versions of an asset in ascending order (NULL orders last).
    /// Parameter: $1 asset_id.
    pub const LIST_TABULAR_BY_ASSET: &str = r#"
        SELECT av.id, av.asset_id, av.version_key, av.version_order, av.previous_version_id,
               av.comment, av.properties, av.created_at,
               tav.version_id, tav.metadata_location,
               tav.created_at AS tabular_created_at
        FROM asset_versions av
        JOIN tabular_asset_versions tav ON av.id = tav.version_id
        WHERE av.asset_id = $1
        ORDER BY av.version_order ASC NULLS LAST
    "#;
}

/// Queries against `iceberg_staged_tables` (V4.0 C2).
pub mod iceberg_staged {
    /// Insert a staged table record. Parameters: $1 domain_name, $2 namespace_name,
    /// $3 table_name, $4 table_uuid, $5 location, $6 metadata_location,
    /// $7 metadata_json, $8 properties, $9 expires_at.
    pub const CREATE: &str = r#"
        INSERT INTO iceberg_staged_tables
            (domain_name, namespace_name, table_name, table_uuid, location,
             metadata_location, metadata_json, properties, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
    "#;

    /// Look up a non-expired staged table. Parameters: $1 domain_name,
    /// $2 namespace_name, $3 table_name.
    pub const GET: &str = r#"
        SELECT metadata_json
        FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_name = $2 AND table_name = $3
          AND expires_at > NOW()
    "#;

    /// Delete a staged table record. Parameters: $1 domain_name,
    /// $2 namespace_name, $3 table_name.
    pub const DELETE: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_name = $2 AND table_name = $3
    "#;

    /// Delete expired staged records for a given (domain, namespace, name).
    /// Parameters: $1 domain_name, $2 namespace_name, $3 table_name.
    pub const DELETE_EXPIRED: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_name = $2 AND table_name = $3
          AND expires_at <= NOW()
    "#;

    /// Commit staged table: delete staged record and return metadata needed
    /// for catalog insertion. Parameters: $1 domain_name, $2 namespace_name,
    /// $3 table_name.
    pub const DELETE_FOR_COMMIT: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_name = $2 AND table_name = $3
          AND expires_at > NOW()
        RETURNING table_uuid, location, metadata_location, metadata_json, properties
    "#;
}

/// Queries against `iceberg_scan_metrics_reports` (V4.0 C2).
pub mod iceberg_metrics {
    /// Insert a scan metrics report. Parameters: $1 asset_id, $2 domain_name,
    /// $3 namespace_name, $4 table_name, $5 report, $6 user_agent.
    pub const CREATE: &str = r#"
        INSERT INTO iceberg_scan_metrics_reports
            (asset_id, domain_name, namespace_name, table_name, report, user_agent)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
    "#;
}

/// Queries against `iceberg_purge_operations` (V4.0 C2).
pub mod iceberg_purge {
    /// Insert a purge operation record. Parameters: $1 domain_name,
    /// $2 namespace_name, $3 table_name, $4 table_location, $5 metadata_location,
    /// $6 status.
    pub const CREATE: &str = r#"
        INSERT INTO iceberg_purge_operations
            (domain_name, namespace_name, table_name, table_location,
             metadata_location, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
    "#;

    /// Update purge operation status. Parameters: $1 status, $2 error_message,
    /// $3 completed_at, $4 id.
    pub const UPDATE_STATUS: &str = r#"
        UPDATE iceberg_purge_operations
        SET status = $1, error_message = $2, completed_at = $3
        WHERE id = $4
    "#;

    /// Read table location and metadata_location for purge, joining through
    /// catalog tables. Parameters: $1 domain_name, $2 namespace_name, $3 table_name.
    pub const GET_TABLE_LOCATION: &str = r#"
        SELECT ta.location, ta.metadata_location
        FROM tabular_assets ta
        JOIN assets a ON ta.asset_id = a.id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
          AND a.deleted_at IS NULL AND a.asset_type = 'table' AND ta.format = 'iceberg'
    "#;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every SQL constant must be non-empty and reference at least one
    /// positional parameter; otherwise it is suspicious enough to fail the
    /// build during refactors.
    fn assert_constant(name: &str, sql: &str) {
        assert!(!sql.trim().is_empty(), "{} must not be empty", name);
        assert!(
            sql.contains("$1"),
            "{} should reference at least placeholder $1",
            name
        );
    }

    #[test]
    fn domain_constants_are_well_formed() {
        assert_constant("domain::CREATE", domain::CREATE);
        assert_constant("domain::LIST", domain::LIST);
        assert_constant("domain::GET_BY_NAME", domain::GET_BY_NAME);
        assert_constant("domain::EXISTS", domain::EXISTS);
        assert_constant("domain::DELETE", domain::DELETE);
        assert_constant("domain::UPDATE", domain::UPDATE);
    }

    #[test]
    fn namespace_constants_are_well_formed() {
        assert_constant("namespace::CREATE", namespace::CREATE);
        assert_constant("namespace::LIST_BY_DOMAIN", namespace::LIST_BY_DOMAIN);
        assert_constant("namespace::GET_BY_NAME", namespace::GET_BY_NAME);
        assert_constant("namespace::EXISTS", namespace::EXISTS);
        assert_constant("namespace::DELETE", namespace::DELETE);
        assert_constant("namespace::UPDATE", namespace::UPDATE);
    }

    #[test]
    fn asset_constants_are_well_formed() {
        assert_constant("asset::CREATE", asset::CREATE);
        assert_constant("asset::GET_BY_NAME", asset::GET_BY_NAME);
        assert_constant("asset::GET_TABULAR_BY_NAME", asset::GET_TABULAR_BY_NAME);
        assert_constant(
            "asset::LIST_TABULAR_BY_NAMESPACE",
            asset::LIST_TABULAR_BY_NAMESPACE,
        );
        assert_constant("asset::CREATE_TABULAR", asset::CREATE_TABULAR);
        assert_constant("asset::UPDATE_PROPERTIES", asset::UPDATE_PROPERTIES);
        assert_constant("asset::RENAME", asset::RENAME);
        assert_constant("asset::RENAME_WITH_NAMESPACE", asset::RENAME_WITH_NAMESPACE);
        assert_constant("asset::DELETE", asset::DELETE);
        assert_constant("asset::EXISTS_TABULAR", asset::EXISTS_TABULAR);
        assert_constant(
            "asset::CAS_UPDATE_METADATA_LOCATION",
            asset::CAS_UPDATE_METADATA_LOCATION,
        );
        assert_constant(
            "asset::UPDATE_PROPERTIES_BY_ID",
            asset::UPDATE_PROPERTIES_BY_ID,
        );
        assert_constant("asset::LIST_UNIFIED", asset::LIST_UNIFIED);
        assert_constant("asset::GET_UNIFIED", asset::GET_UNIFIED);
        assert_constant("asset::LOOKUP_ID_BY_NAME", asset::LOOKUP_ID_BY_NAME);
        assert_constant("asset::EXISTS", asset::EXISTS);
    }

    #[test]
    fn version_constants_are_well_formed() {
        assert_constant("version::CREATE", version::CREATE);
        assert_constant("version::CREATE_TABULAR", version::CREATE_TABULAR);
        assert_constant("version::GET_LATEST", version::GET_LATEST);
        assert_constant("version::GET_LATEST_TABULAR", version::GET_LATEST_TABULAR);
        assert_constant("version::GET_BY_KEY", version::GET_BY_KEY);
        assert_constant("version::LIST_BY_ASSET", version::LIST_BY_ASSET);
        assert_constant("version::GET_TABULAR_BY_KEY", version::GET_TABULAR_BY_KEY);
        assert_constant(
            "version::LIST_TABULAR_BY_ASSET",
            version::LIST_TABULAR_BY_ASSET,
        );
    }

    #[test]
    fn asset_get_uses_active_filter() {
        assert!(
            asset::GET_BY_NAME.contains("deleted_at IS NULL"),
            "asset GET_BY_NAME must restrict to active assets"
        );
        assert!(
            asset::GET_TABULAR_BY_NAME.contains("deleted_at IS NULL"),
            "asset GET_TABULAR_BY_NAME must restrict to active assets"
        );
    }

    #[test]
    fn version_latest_excludes_nulls() {
        assert!(
            version::GET_LATEST.contains("version_order IS NOT NULL"),
            "latest version query must ignore rows without a numeric order"
        );
        assert!(
            version::GET_LATEST_TABULAR.contains("version_order IS NOT NULL"),
            "latest tabular version query must ignore rows without a numeric order"
        );
    }

    #[test]
    fn cas_update_uses_optimistic_predicate() {
        assert!(
            asset::CAS_UPDATE_METADATA_LOCATION.contains("IS NOT DISTINCT FROM"),
            "CAS update must check the prior metadata_location with IS NOT DISTINCT FROM"
        );
        assert!(
            asset::CAS_UPDATE_METADATA_LOCATION.contains("RETURNING"),
            "CAS update must RETURN affected rows for the caller to detect Conflict"
        );
    }

    #[test]
    fn unified_queries_use_left_join() {
        assert!(
            asset::LIST_UNIFIED.contains("LEFT JOIN tabular_assets"),
            "LIST_UNIFIED must LEFT JOIN tabular_assets so non-table assets surface as None"
        );
        assert!(
            asset::GET_UNIFIED.contains("LEFT JOIN tabular_assets"),
            "GET_UNIFIED must LEFT JOIN tabular_assets so non-table assets surface as None"
        );
    }

    #[test]
    fn iceberg_staged_constants_are_well_formed() {
        assert_constant("iceberg_staged::CREATE", iceberg_staged::CREATE);
        assert_constant("iceberg_staged::GET", iceberg_staged::GET);
        assert_constant("iceberg_staged::DELETE", iceberg_staged::DELETE);
        assert_constant(
            "iceberg_staged::DELETE_EXPIRED",
            iceberg_staged::DELETE_EXPIRED,
        );
        assert_constant(
            "iceberg_staged::DELETE_FOR_COMMIT",
            iceberg_staged::DELETE_FOR_COMMIT,
        );
    }

    #[test]
    fn iceberg_metrics_constants_are_well_formed() {
        assert_constant("iceberg_metrics::CREATE", iceberg_metrics::CREATE);
    }

    #[test]
    fn iceberg_purge_constants_are_well_formed() {
        assert_constant("iceberg_purge::CREATE", iceberg_purge::CREATE);
        assert_constant("iceberg_purge::UPDATE_STATUS", iceberg_purge::UPDATE_STATUS);
        assert_constant(
            "iceberg_purge::GET_TABLE_LOCATION",
            iceberg_purge::GET_TABLE_LOCATION,
        );
    }
}
