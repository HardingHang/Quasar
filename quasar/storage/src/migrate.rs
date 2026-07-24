//! Lightweight schema migration runner (DESIGN §8.2 / §8.4).
//!
//! Migrations are embedded at compile time via `include_str!` and applied
//! in ascending version order at service startup. Each migration runs in
//! its own transaction together with its `schema_migrations` bookkeeping
//! row, so a failed migration rolls back cleanly and can be retried on the
//! next startup. Any failure aborts startup (`initialize` returns `Err`).
//!
//! `.down.sql` files are for manual development rollback only and are
//! never executed here.

use deadpool_postgres::Client;
use quasar_core::CatalogError;
use tokio_postgres::error::SqlState;

/// One embedded migration script.
struct Migration {
    /// Monotonic version number recorded in `schema_migrations`.
    version: i64,
    description: &'static str,
    up_sql: &'static str,
}

/// All migrations, in ascending version order. New migrations are appended
/// as `NNNN_name.up.sql` files plus one entry here.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    description: "init",
    up_sql: include_str!("../migrations/0001_init.up.sql"),
}];

/// Apply every migration not yet recorded in `schema_migrations`.
///
/// The bookkeeping table is created by the first migration itself; on a
/// fresh database the applied-versions query fails with `UNDEFINED_TABLE`,
/// which is treated as "nothing applied yet".
pub async fn run(client: &mut Client) -> Result<(), CatalogError> {
    let applied = load_applied_versions(client).await?;

    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        apply_migration(client, migration).await?;
        tracing::info!(
            version = migration.version,
            description = migration.description,
            "applied schema migration"
        );
    }
    Ok(())
}

/// Read the set of already-applied migration versions.
async fn load_applied_versions(client: &Client) -> Result<Vec<i64>, CatalogError> {
    match client
        .query("SELECT version FROM schema_migrations", &[])
        .await
    {
        Ok(rows) => Ok(rows.iter().map(|row| row.get(0)).collect()),
        Err(e) => {
            let fresh_database = e
                .as_db_error()
                .is_some_and(|db| db.code() == &SqlState::UNDEFINED_TABLE);
            if fresh_database {
                Ok(Vec::new())
            } else {
                Err(CatalogError::Internal(format!(
                    "load applied migrations failed: {}",
                    &e
                )))
            }
        }
    }
}

/// Apply a single migration in its own transaction, including the
/// `schema_migrations` record.
async fn apply_migration(client: &mut Client, migration: &Migration) -> Result<(), CatalogError> {
    let tx = client
        .transaction()
        .await
        .map_err(|e| CatalogError::Internal(format!("migration tx start failed: {}", &e)))?;

    tx.batch_execute(migration.up_sql).await.map_err(|e| {
        CatalogError::Internal(format!(
            "migration {} ({}) failed: {}",
            migration.version, migration.description, &e
        ))
    })?;

    tx.execute(
        "INSERT INTO schema_migrations (version, description) VALUES ($1, $2)",
        &[&migration.version, &migration.description],
    )
    .await
    .map_err(|e| {
        CatalogError::Internal(format!(
            "migration {} bookkeeping failed: {}",
            migration.version, &e
        ))
    })?;

    tx.commit()
        .await
        .map_err(|e| CatalogError::Internal(format!("migration tx commit failed: {}", &e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_ordered_and_unique() {
        let mut versions = MIGRATIONS.iter().map(|m| m.version);
        let mut prev = versions.next();
        for version in versions {
            assert!(
                Some(version) > prev,
                "migration versions must be strictly ascending"
            );
            prev = Some(version);
        }
    }

    #[test]
    fn migrations_have_non_empty_sql_and_description() {
        for migration in MIGRATIONS {
            assert!(!migration.up_sql.trim().is_empty());
            assert!(!migration.description.is_empty());
        }
    }

    #[test]
    fn first_migration_creates_bookkeeping_table() {
        let first = &MIGRATIONS[0];
        assert!(
            first.up_sql.contains("CREATE TABLE schema_migrations"),
            "the first migration must create the schema_migrations table"
        );
    }

    #[test]
    fn seeds_are_idempotent() {
        let first = &MIGRATIONS[0];
        assert!(
            first.up_sql.contains("ON CONFLICT (name) DO NOTHING"),
            "registry seeds must use ON CONFLICT DO NOTHING"
        );
    }
}
