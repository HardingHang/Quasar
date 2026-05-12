//! V3 initial database schema loader.
//!
//! `init.sql` is the single source of truth for V3 catalog tables. It is NOT
//! a migration script: callers (test harnesses, fresh-deployment bootstraps)
//! invoke [`initialize`] explicitly. Phase 1 ships the SQL plus loader; Phase
//! 2 will wire it into `PgCatalogStore::new` once the V1 migration runner is
//! retired.

use quasar_core::StoreError;

/// V3 initial DDL. Idempotent: re-executing on an already-initialized
/// database succeeds without changes.
pub const INIT_SQL: &str = include_str!("init.sql");

/// Execute [`INIT_SQL`] against the supplied client.
///
/// Each statement in `init.sql` is idempotent, so this function can be
/// invoked repeatedly during tests or fresh deployments.
pub async fn initialize(client: &tokio_postgres::Client) -> Result<(), StoreError> {
    client
        .batch_execute(INIT_SQL)
        .await
        .map_err(|e| StoreError::Internal {
            msg: format!("schema init failed: {}", &e),
            source: Some(Box::new(e)),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tables that V3 considers part of the Data Core Model.
    const EXPECTED_TABLES: &[&str] = &[
        "asset_types",
        "tabular_formats",
        "domains",
        "namespaces",
        "assets",
        "tabular_assets",
        "asset_versions",
        "tabular_asset_versions",
        "asset_permissions",
    ];

    /// Triggers that enforce cross-row invariants the application also checks.
    const EXPECTED_TRIGGERS: &[&str] = &[
        "trg_tabular_assets_asset_type",
        "trg_asset_versions_previous_same_asset",
    ];

    /// Partial-unique / latest-version indexes that V3 explicitly requires.
    const EXPECTED_INDEXES: &[&str] = &[
        "uq_assets_active_name",
        "uq_asset_versions_order",
        "idx_asset_versions_latest",
        "idx_tabular_assets_format_asset",
    ];

    #[test]
    fn init_sql_is_non_empty() {
        assert!(
            !INIT_SQL.trim().is_empty(),
            "INIT_SQL must not be empty after include_str!"
        );
    }

    #[test]
    fn init_sql_contains_expected_tables() {
        for table in EXPECTED_TABLES {
            let needle = format!("CREATE TABLE IF NOT EXISTS {}", table);
            assert!(
                INIT_SQL.contains(&needle),
                "INIT_SQL is missing table declaration `{}`",
                needle
            );
        }
    }

    #[test]
    fn init_sql_contains_expected_triggers() {
        for trigger in EXPECTED_TRIGGERS {
            assert!(
                INIT_SQL.contains(&format!("CREATE TRIGGER {}", trigger)),
                "INIT_SQL is missing CREATE TRIGGER for `{}`",
                trigger
            );
            assert!(
                INIT_SQL.contains(&format!("DROP TRIGGER IF EXISTS {}", trigger)),
                "INIT_SQL must DROP TRIGGER IF EXISTS for `{}` before CREATE",
                trigger
            );
        }
    }

    #[test]
    fn init_sql_contains_expected_indexes() {
        for index in EXPECTED_INDEXES {
            assert!(
                INIT_SQL.contains(index),
                "INIT_SQL is missing index `{}`",
                index
            );
        }
    }

    #[test]
    fn init_sql_seeds_registries() {
        assert!(
            INIT_SQL.contains("INSERT INTO asset_types"),
            "INIT_SQL must seed asset_types registry"
        );
        assert!(
            INIT_SQL.contains("INSERT INTO tabular_formats"),
            "INIT_SQL must seed tabular_formats registry"
        );
        assert!(
            INIT_SQL.contains("ON CONFLICT DO NOTHING"),
            "registry seed must be idempotent (ON CONFLICT DO NOTHING)"
        );
    }

    #[test]
    fn init_sql_uses_partial_unique_for_active_assets() {
        assert!(
            INIT_SQL.contains("WHERE deleted_at IS NULL"),
            "active asset uniqueness must use partial unique index on deleted_at IS NULL"
        );
    }
}
