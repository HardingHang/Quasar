//! Centralized SQL constants for V3 Data Core Model queries.
//!
//! These constants are defined in Phase 1 of the V3 implementation as the
//! single place where SQL strings live. `PgCatalogStore` will switch from its
//! inline V1 SQL to these constants in Phase 2 once the V3 schema is wired
//! through `crate::schema`. They are intentionally unused right now; the
//! `dead_code` attribute keeps clippy quiet without hiding the symbols from
//! Phase 2 implementations.
//!
//! Conventions:
//! - constant names use `SCREAMING_SNAKE_CASE`;
//! - multi-line SQL uses `r#"..."#` raw strings, SQL keywords are uppercase;
//! - placeholders are positional `$1`, `$2`, ...;
//! - lookups by `(domain, namespace, asset)` join through the registry tables
//!   so callers do not have to thread UUIDs in handler code;
//! - statements that mutate state always `RETURNING` the canonical column set
//!   so callers can rebuild the model without an extra round-trip.

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
    /// Insert a namespace under an existing domain. Parameters: $1 domain_id,
    /// $2 name, $3 comment, $4 properties.
    pub const CREATE: &str = r#"
        INSERT INTO namespaces (domain_id, name, comment, properties)
        VALUES ($1, $2, $3, $4)
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
    /// Insert a generic asset under a namespace. Parameters: $1 namespace_id,
    /// $2 name, $3 asset_type, $4 comment, $5 properties.
    pub const CREATE: &str = r#"
        INSERT INTO assets (namespace_id, name, asset_type, comment, properties)
        VALUES ($1, $2, $3, $4, $5)
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
    /// (TEXT, NULL = no filter).
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
    "#;

    /// Insert a tabular asset extension row. Parameters: $1 asset_id, $2 format,
    /// $3 location, $4 metadata_location, $5 schema_snapshot.
    pub const CREATE_TABULAR: &str = r#"
        INSERT INTO tabular_assets (asset_id, format, location, metadata_location, schema_snapshot)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING asset_id, format, location, metadata_location, schema_snapshot, created_at, updated_at
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
    /// Parameters: $1 new_name, $2 asset_id.
    pub const RENAME: &str = r#"
        UPDATE assets
        SET name = $1,
            updated_at = NOW()
        WHERE id = $2 AND deleted_at IS NULL
    "#;

    /// Hard delete an asset; cascades to extension and version rows via FK.
    /// Parameter: $1 asset_id.
    pub const DELETE: &str = r#"
        DELETE FROM assets WHERE id = $1
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
        RETURNING version_id, metadata_location, created_at
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
        assert_constant("asset::DELETE", asset::DELETE);
        assert_constant("asset::EXISTS_TABULAR", asset::EXISTS_TABULAR);
    }

    #[test]
    fn version_constants_are_well_formed() {
        assert_constant("version::CREATE", version::CREATE);
        assert_constant("version::CREATE_TABULAR", version::CREATE_TABULAR);
        assert_constant("version::GET_LATEST", version::GET_LATEST);
        assert_constant("version::GET_LATEST_TABULAR", version::GET_LATEST_TABULAR);
        assert_constant("version::GET_BY_KEY", version::GET_BY_KEY);
        assert_constant("version::LIST_BY_ASSET", version::LIST_BY_ASSET);
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
}
