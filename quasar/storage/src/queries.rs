//! SQL statements for the PostgreSQL storage implementation.
//!
//! Conventions (CODING_CONVENTION.md): keywords upper-case, every value
//! parameterized (`$1`, `$2`, ...), no string-built SQL. Optional filters
//! use the `($n::type IS NULL OR ...)` sentinel pattern; three-state patch
//! fields use `CASE $action::int WHEN 1 THEN $value WHEN 2 THEN NULL ELSE col`
//! (action: 0 = NoChange, 1 = Set, 2 = Unset; the cast pins the parameter
//! type — PostgreSQL otherwise infers the CASE operand as text). Prefix matching uses
//! `starts_with(path, $n || '/')` instead of LIKE so slug characters such
//! as `_` are not treated as wildcards.

pub mod domain {
    pub const CREATE: &str = r#"
        INSERT INTO domains (name, comment, properties, storage_type, storage_config, warehouse)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse,
                  created_at, updated_at
    "#;

    pub const GET_BY_NAME: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse,
               created_at, updated_at
        FROM domains
        WHERE name = $1
    "#;

    pub const GET_ID_BY_NAME: &str = r#"
        SELECT id FROM domains WHERE name = $1
    "#;

    pub const LIST: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse,
               created_at, updated_at
        FROM domains
        ORDER BY name
        OFFSET $1 LIMIT $2
    "#;

    /// Three-state patch: per field `$action` (0/1/2) + `$value`.
    pub const UPDATE: &str = r#"
        UPDATE domains SET
            comment        = CASE $2::int WHEN 1 THEN $3 WHEN 2 THEN NULL ELSE comment END,
            properties     = CASE $4::int WHEN 1 THEN $5 WHEN 2 THEN NULL ELSE properties END,
            storage_type   = CASE $6::int WHEN 1 THEN $7 WHEN 2 THEN NULL ELSE storage_type END,
            storage_config = CASE $8::int WHEN 1 THEN $9 WHEN 2 THEN NULL ELSE storage_config END,
            warehouse      = CASE $10::int WHEN 1 THEN $11 WHEN 2 THEN NULL ELSE warehouse END,
            updated_at     = now()
        WHERE name = $1
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse,
                  created_at, updated_at
    "#;

    pub const DELETE: &str = r#"
        DELETE FROM domains WHERE name = $1
    "#;
}

pub mod namespace {
    /// Implicit intermediate node insert (FR-N2); conflicts are ignored.
    pub const CREATE_PREFIX: &str = r#"
        INSERT INTO namespaces (domain_id, path, depth)
        VALUES ($1, $2, $3)
        ON CONFLICT (domain_id, path) DO NOTHING
    "#;

    /// Final node insert; no RETURNING row means the path already exists.
    pub const CREATE_FINAL: &str = r#"
        INSERT INTO namespaces (domain_id, path, depth, comment, properties)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (domain_id, path) DO NOTHING
        RETURNING id, domain_id, path, depth, comment, properties, created_at, updated_at
    "#;

    pub const GET_BY_PATH: &str = r#"
        SELECT n.id, n.domain_id, n.path, n.depth, n.comment, n.properties,
               n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2
    "#;

    pub const GET_ID_BY_PATH: &str = r#"
        SELECT n.id
        FROM namespaces n
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2
    "#;

    pub const LIST: &str = r#"
        SELECT n.id, n.domain_id, n.path, n.depth, n.comment, n.properties,
               n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1
          AND ($2::text IS NULL OR n.path = $2 OR starts_with(n.path, $2 || '/'))
        ORDER BY n.path
        OFFSET $3 LIMIT $4
    "#;

    pub const UPDATE: &str = r#"
        UPDATE namespaces n SET
            comment    = CASE $3::int WHEN 1 THEN $4 WHEN 2 THEN NULL ELSE n.comment END,
            properties = CASE $5::int WHEN 1 THEN $6 WHEN 2 THEN NULL ELSE n.properties END,
            updated_at = now()
        FROM domains d
        WHERE n.domain_id = d.id AND d.name = $1 AND n.path = $2
        RETURNING n.id, n.domain_id, n.path, n.depth, n.comment, n.properties,
                  n.created_at, n.updated_at
    "#;

    /// Child namespaces of the given path within the same domain.
    pub const COUNT_CHILDREN: &str = r#"
        SELECT COUNT(*)
        FROM namespaces
        WHERE domain_id = $1 AND starts_with(path, $2 || '/')
    "#;

    pub const COUNT_ASSETS: &str = r#"
        SELECT COUNT(*) FROM assets WHERE namespace_id = $1
    "#;

    pub const DELETE_BY_ID: &str = r#"
        DELETE FROM namespaces WHERE id = $1
    "#;
}

pub mod registry {
    pub const CREATE_ASSET_TYPE: &str = r#"
        INSERT INTO asset_types
            (name, description, category, validation_schema, extension_strategy,
             supports_native_protocol)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING name, description, category, validation_schema, extension_strategy,
                  supports_native_protocol
    "#;

    pub const GET_ASSET_TYPE: &str = r#"
        SELECT name, description, category, validation_schema, extension_strategy,
               supports_native_protocol
        FROM asset_types
        WHERE name = $1
    "#;

    pub const LIST_ASSET_TYPES: &str = r#"
        SELECT name, description, category, validation_schema, extension_strategy,
               supports_native_protocol
        FROM asset_types
        WHERE ($1::text IS NULL OR category = $1)
        ORDER BY name
        OFFSET $2 LIMIT $3
    "#;

    pub const CREATE_FORMAT: &str = r#"
        INSERT INTO formats (name, description, mime_type, serialization_hint)
        VALUES ($1, $2, $3, $4)
        RETURNING name, description, mime_type, serialization_hint
    "#;

    pub const GET_FORMAT: &str = r#"
        SELECT name, description, mime_type, serialization_hint
        FROM formats
        WHERE name = $1
    "#;

    pub const LIST_FORMATS: &str = r#"
        SELECT name, description, mime_type, serialization_hint
        FROM formats
        ORDER BY name
        OFFSET $1 LIMIT $2
    "#;
}

pub mod asset {
    pub const CREATE: &str = r#"
        INSERT INTO assets (namespace_id, name, asset_type, format, comment, properties)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, namespace_id, name, asset_type, format, comment, properties,
                  current_version_key, deleted_at, created_at, updated_at
    "#;

    pub const GET_BY_ID: &str = r#"
        SELECT id, namespace_id, name, asset_type, format, comment, properties,
               current_version_key, deleted_at, created_at, updated_at
        FROM assets
        WHERE id = $1
    "#;

    pub const GET_BY_NAME: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.format, a.comment, a.properties,
               a.current_version_key, a.deleted_at, a.created_at, a.updated_at
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2 AND a.name = $3 AND a.deleted_at IS NULL
    "#;

    /// Filtered listing. $5 include_deleted, $6 tags (NULL = no tag
    /// filter; every listed tag must be present), $7 properties
    /// exact-match object (`@>` containment).
    pub const LIST: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.format, a.comment, a.properties,
               a.current_version_key, a.deleted_at, a.created_at, a.updated_at
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE ($1::text IS NULL OR d.name = $1)
          AND ($2::text IS NULL OR n.path = $2)
          AND ($3::text IS NULL OR a.asset_type = $3)
          AND ($4::text IS NULL OR a.format = $4)
          AND ($5::bool OR a.deleted_at IS NULL)
          AND ($6::text[] IS NULL OR (
                  SELECT COUNT(DISTINCT t.tag) FROM asset_tags t
                  WHERE t.asset_id = a.id AND t.tag = ANY($6)
              ) = (SELECT COUNT(DISTINCT u) FROM unnest($6) AS u))
          AND (COALESCE(a.properties, '{}'::jsonb) @> $7::jsonb)
        ORDER BY a.created_at, a.id
        OFFSET $8 LIMIT $9
    "#;

    pub const UPDATE: &str = r#"
        UPDATE assets SET
            comment    = CASE $2::int WHEN 1 THEN $3 WHEN 2 THEN NULL ELSE comment END,
            properties = CASE $4::int WHEN 1 THEN $5 WHEN 2 THEN NULL ELSE properties END,
            updated_at = now()
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id, namespace_id, name, asset_type, format, comment, properties,
                  current_version_key, deleted_at, created_at, updated_at
    "#;

    pub const RENAME: &str = r#"
        UPDATE assets SET name = $2, namespace_id = $3, updated_at = now()
        WHERE id = $1 AND deleted_at IS NULL
        RETURNING id, namespace_id, name, asset_type, format, comment, properties,
                  current_version_key, deleted_at, created_at, updated_at
    "#;

    /// Resolve the destination namespace of a cross-namespace rename: the
    /// path is looked up in the Domain that owns the asset, so a move can
    /// never cross the Domain boundary.
    pub const RESOLVE_RENAME_TARGET_NAMESPACE: &str = r#"
        SELECT n.id
        FROM namespaces n
        JOIN namespaces src ON src.domain_id = n.domain_id
        JOIN assets a ON a.namespace_id = src.id
        WHERE a.id = $1 AND n.path = $2
    "#;

    pub const SOFT_DELETE: &str = r#"
        UPDATE assets SET deleted_at = now(), updated_at = now()
        WHERE id = $1 AND deleted_at IS NULL
    "#;

    /// Lock the asset row for restore validation.
    pub const LOCK_BY_ID: &str = r#"
        SELECT id, namespace_id, name, asset_type, format, comment, properties,
               current_version_key, deleted_at, created_at, updated_at
        FROM assets
        WHERE id = $1
        FOR UPDATE
    "#;

    pub const EXISTS_ACTIVE_NAME: &str = r#"
        SELECT EXISTS(
            SELECT 1 FROM assets
            WHERE namespace_id = $1 AND name = $2 AND deleted_at IS NULL
        )
    "#;

    pub const RESTORE: &str = r#"
        UPDATE assets SET deleted_at = NULL, updated_at = now()
        WHERE id = $1
        RETURNING id, namespace_id, name, asset_type, format, comment, properties,
                  current_version_key, deleted_at, created_at, updated_at
    "#;

    /// Delete the current "leaf" versions of an asset: versions no other
    /// version of the same asset references as predecessor. The
    /// self-referencing `previous_version_id ... ON DELETE RESTRICT`
    /// rejects deleting a referenced version even when the referencing
    /// row is deleted by the same statement, so hard deletes run this in a
    /// loop (tip-to-root, one round per chain depth level) before the
    /// assets row itself is removed.
    pub const DELETE_VERSION_LEAVES: &str = r#"
        DELETE FROM asset_versions v
        WHERE v.asset_id = $1
          AND NOT EXISTS (
              SELECT 1 FROM asset_versions c
              WHERE c.asset_id = $1 AND c.previous_version_id = v.id
          )
    "#;

    pub const HARD_DELETE: &str = r#"
        DELETE FROM assets WHERE id = $1
    "#;

    pub const SET_CURRENT_VERSION_KEY: &str = r#"
        UPDATE assets SET current_version_key = $2, updated_at = now()
        WHERE id = $1 AND deleted_at IS NULL
    "#;

    /// Insert the `tabular_assets` extension row for a table asset.
    pub const CREATE_TABULAR: &str = r#"
        INSERT INTO tabular_assets (asset_id, location, metadata_location, schema_snapshot)
        VALUES ($1, $2, $3, $4)
        RETURNING asset_id, location, metadata_location, schema_snapshot
    "#;

    /// Read the `tabular_assets` extension row by asset id.
    pub const GET_TABULAR: &str = r#"
        SELECT asset_id, location, metadata_location, schema_snapshot
        FROM tabular_assets
        WHERE asset_id = $1
    "#;
}

pub mod version {
    pub const CREATE: &str = r#"
        INSERT INTO asset_versions
            (asset_id, version_key, version_properties, content_inline, content_pointer,
             previous_version_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, asset_id, version_key, version_properties, content_inline,
                  content_pointer, previous_version_id, created_at
    "#;

    pub const GET_BY_KEY: &str = r#"
        SELECT id, asset_id, version_key, version_properties, content_inline,
               content_pointer, previous_version_id, created_at
        FROM asset_versions
        WHERE asset_id = $1 AND version_key = $2
    "#;

    pub const LIST: &str = r#"
        SELECT id, asset_id, version_key, version_properties, content_inline,
               content_pointer, previous_version_id, created_at
        FROM asset_versions
        WHERE asset_id = $1
        ORDER BY created_at DESC, id DESC
        OFFSET $2 LIMIT $3
    "#;

    pub const GET_LATEST: &str = r#"
        SELECT v.id, v.asset_id, v.version_key, v.version_properties, v.content_inline,
               v.content_pointer, v.previous_version_id, v.created_at
        FROM asset_versions v
        JOIN assets a ON a.id = v.asset_id AND a.current_version_key = v.version_key
        WHERE a.id = $1 AND a.deleted_at IS NULL
    "#;

    pub const PREV_BELONGS_TO_ASSET: &str = r#"
        SELECT EXISTS(
            SELECT 1 FROM asset_versions WHERE id = $1 AND asset_id = $2
        )
    "#;

    pub const ASSET_ACTIVE_EXISTS: &str = r#"
        SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND deleted_at IS NULL)
    "#;
}

pub mod cas {
    /// S1: lock the asset row and read the current version key.
    pub const LOCK_ASSET: &str = r#"
        SELECT id, current_version_key
        FROM assets
        WHERE id = $1 AND deleted_at IS NULL
        FOR UPDATE
    "#;

    /// S2: read the current pointer from the tabular hot-path cache (the
    /// assets row lock is already held, so no extra lock is needed).
    pub const READ_TABULAR_POINTER: &str = r#"
        SELECT metadata_location FROM tabular_assets WHERE asset_id = $1
    "#;

    /// S4: insert the mirrored version, auto-linking the previous version
    /// via the current version key captured in S1 (NULL key -> root).
    pub const INSERT_MIRRORED_VERSION: &str = r#"
        INSERT INTO asset_versions
            (asset_id, version_key, version_properties, content_inline, content_pointer,
             previous_version_id)
        VALUES (
            $1, $2, $3, $4, $5,
            (SELECT id FROM asset_versions WHERE asset_id = $1 AND version_key = $6)
        )
        RETURNING id, asset_id, version_key, version_properties, content_inline,
                  content_pointer, previous_version_id, created_at
    "#;

    /// S5: update the current version key (result of the CAS, not an anchor).
    pub const UPDATE_CURRENT_VERSION_KEY: &str = r#"
        UPDATE assets SET current_version_key = $2, updated_at = now() WHERE id = $1
    "#;

    /// S6: update the tabular hot-path cache. schema_snapshot is left
    /// untouched: `CreateVersion` carries no schema, so there is nothing
    /// to cache here.
    pub const UPDATE_TABULAR_POINTER: &str = r#"
        UPDATE tabular_assets SET metadata_location = $2 WHERE asset_id = $1
    "#;
}

pub mod tag {
    pub const ADD: &str = r#"
        INSERT INTO asset_tags (asset_id, tag) VALUES ($1, $2)
    "#;

    pub const REMOVE: &str = r#"
        DELETE FROM asset_tags WHERE asset_id = $1 AND tag = $2
    "#;

    pub const LIST: &str = r#"
        SELECT tag FROM asset_tags WHERE asset_id = $1 ORDER BY tag
    "#;

    pub const LIST_ASSETS_BY_TAG: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.format, a.comment, a.properties,
               a.current_version_key, a.deleted_at, a.created_at, a.updated_at
        FROM assets a
        JOIN asset_tags t ON t.asset_id = a.id
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND t.tag = $2 AND a.deleted_at IS NULL
        ORDER BY a.created_at, a.id
        OFFSET $3 LIMIT $4
    "#;
}

pub mod unified {
    /// Cross-namespace discovery query (UnifiedQueryStore). Domain is
    /// mandatory; $2 is the namespace path prefix filter.
    pub const QUERY_ASSETS: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.format, a.comment, a.properties,
               a.current_version_key, a.deleted_at, a.created_at, a.updated_at
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1
          AND ($2::text IS NULL OR n.path = $2 OR starts_with(n.path, $2 || '/'))
          AND ($3::text IS NULL OR a.asset_type = $3)
          AND ($4::text IS NULL OR a.format = $4)
          AND ($5::bool OR a.deleted_at IS NULL)
          AND ($6::text[] IS NULL OR (
                  SELECT COUNT(DISTINCT t.tag) FROM asset_tags t
                  WHERE t.asset_id = a.id AND t.tag = ANY($6)
              ) = (SELECT COUNT(DISTINCT u) FROM unnest($6) AS u))
          AND (COALESCE(a.properties, '{}'::jsonb) @> $7::jsonb)
        ORDER BY a.created_at, a.id
        OFFSET $8 LIMIT $9
    "#;
}

pub mod iceberg {
    /// Active asset (any type) with the same name blocks stage-create.
    pub const ACTIVE_ASSET_EXISTS: &str = r#"
        SELECT EXISTS(
            SELECT 1
            FROM assets a
            JOIN namespaces n ON n.id = a.namespace_id
            JOIN domains d ON d.id = n.domain_id
            WHERE d.name = $1 AND n.path = $2 AND a.name = $3 AND a.deleted_at IS NULL
        )
    "#;

    // ── Staged tables (24h TTL) ─────────────────────────────────────────

    pub const DELETE_EXPIRED_STAGED: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_path = $2 AND table_name = $3
          AND expires_at <= now()
    "#;

    pub const CREATE_STAGED: &str = r#"
        INSERT INTO iceberg_staged_tables
            (domain_name, namespace_path, table_name, table_uuid, location,
             metadata_location, metadata_json, properties, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + interval '24 hours')
    "#;

    pub const GET_STAGED: &str = r#"
        SELECT metadata_json
        FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_path = $2 AND table_name = $3
          AND expires_at > now()
    "#;

    pub const DELETE_STAGED: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_path = $2 AND table_name = $3
    "#;

    /// Atomic "take" of a non-expired staged record during commit.
    pub const DELETE_STAGED_FOR_COMMIT: &str = r#"
        DELETE FROM iceberg_staged_tables
        WHERE domain_name = $1 AND namespace_path = $2 AND table_name = $3
          AND expires_at > now()
        RETURNING id
    "#;

    // ── Tabular / view extension rows ───────────────────────────────────

    pub use super::asset::CREATE_TABULAR;

    pub const CREATE_VIEW_ASSET: &str = r#"
        INSERT INTO view_assets (asset_id, view_uuid, location, metadata_location)
        VALUES ($1, $2, $3, $4)
        RETURNING asset_id, view_uuid, location, metadata_location
    "#;

    // ── View queries ────────────────────────────────────────────────────

    pub const GET_VIEW: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.format, a.comment, a.properties,
               a.current_version_key, a.deleted_at, a.created_at, a.updated_at,
               va.asset_id AS view_asset_id, va.view_uuid, va.location AS view_location,
               va.metadata_location AS view_metadata_location
        FROM assets a
        JOIN view_assets va ON va.asset_id = a.id
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2 AND a.name = $3
          AND a.asset_type = 'view' AND a.deleted_at IS NULL
    "#;

    pub const GET_VIEW_ID: &str = r#"
        SELECT a.id
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2 AND a.name = $3
          AND a.asset_type = 'view' AND a.deleted_at IS NULL
    "#;

    pub const READ_VIEW_POINTER: &str = r#"
        SELECT metadata_location FROM view_assets WHERE asset_id = $1
    "#;

    pub const UPDATE_VIEW_POINTER: &str = r#"
        UPDATE view_assets SET metadata_location = $2 WHERE asset_id = $1
    "#;

    pub const LIST_VIEWS: &str = r#"
        SELECT n.path, a.name
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2
          AND a.asset_type = 'view' AND a.deleted_at IS NULL
        ORDER BY a.name
        OFFSET $3 LIMIT $4
    "#;

    pub const MOVE_VIEW: &str = r#"
        UPDATE assets SET name = $2, namespace_id = $3, updated_at = now()
        WHERE id = $1 AND deleted_at IS NULL
    "#;

    pub const VIEW_EXISTS: &str = r#"
        SELECT EXISTS(
            SELECT 1
            FROM assets a
            JOIN namespaces n ON n.id = a.namespace_id
            JOIN domains d ON d.id = n.domain_id
            WHERE d.name = $1 AND n.path = $2 AND a.name = $3
              AND a.asset_type = 'view' AND a.deleted_at IS NULL
        )
    "#;

    // ── Multi-table transaction ─────────────────────────────────────────

    /// Resolve a table's asset id by name (active tables only).
    pub const RESOLVE_TABLE_ID: &str = r#"
        SELECT a.id
        FROM assets a
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2 AND a.name = $3
          AND a.asset_type = 'table' AND a.deleted_at IS NULL
    "#;

    /// Pointer update inside a multi-table transaction. schema_snapshot is
    /// only overwritten when the commit carries one.
    pub const TX_UPDATE_TABULAR: &str = r#"
        UPDATE tabular_assets
        SET metadata_location = $2,
            schema_snapshot = COALESCE($3, schema_snapshot)
        WHERE asset_id = $1
    "#;

    // ── Metrics ─────────────────────────────────────────────────────────

    pub const CREATE_METRICS_REPORT: &str = r#"
        INSERT INTO iceberg_scan_metrics_reports
            (asset_id, domain_name, namespace_path, table_name, report, user_agent)
        VALUES ($1, $2, $3, $4, $5, $6)
    "#;

    // ── Purge ───────────────────────────────────────────────────────────

    /// Read and lock the table row plus its tabular extension for purge.
    pub const LOCK_TABLE_FOR_PURGE: &str = r#"
        SELECT a.id, t.location, t.metadata_location
        FROM assets a
        JOIN tabular_assets t ON t.asset_id = a.id
        JOIN namespaces n ON n.id = a.namespace_id
        JOIN domains d ON d.id = n.domain_id
        WHERE d.name = $1 AND n.path = $2 AND a.name = $3
          AND a.asset_type = 'table' AND a.deleted_at IS NULL
        FOR UPDATE OF a
    "#;

    pub const CREATE_PURGE_OPERATION: &str = r#"
        INSERT INTO iceberg_purge_operations
            (domain_name, namespace_path, table_name, table_location, metadata_location, status)
        VALUES ($1, $2, $3, $4, $5, 'catalog_dropped')
        RETURNING id
    "#;

    pub const UPDATE_PURGE_OPERATION: &str = r#"
        UPDATE iceberg_purge_operations
        SET status = $2,
            error_message = $3,
            completed_at = CASE WHEN $2 IN ('completed', 'failed') THEN now() ELSE completed_at END
        WHERE id = $1
    "#;
}
