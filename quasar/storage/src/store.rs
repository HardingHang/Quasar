use async_trait::async_trait;
use deadpool_postgres::Pool;
use quasar_core::{Asset, AssetFormat, AssetVersion, CatalogStore, Namespace, StoreError};
use serde_json;
use std::collections::HashMap;
use std::str::FromStr;
use strum::ParseError;
use tokio_postgres::Row;

pub struct PgCatalogStore {
    pool: Pool,
}

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./src/migrations");
}

impl PgCatalogStore {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    async fn get_client(&self) -> Result<deadpool_postgres::Client, StoreError> {
        self.pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(format!("connection pool error: {}", e)))
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        let mut client = self.get_client().await?;

        let report = embedded::migrations::runner()
            .run_async(&mut **client)
            .await
            .map_err(|e| StoreError::Internal(format!("migration failed: {}", e)))?;

        for migration in report.applied_migrations() {
            tracing::info!("applied migration: {}", migration.name());
        }

        Ok(())
    }
}

macro_rules! try_get {
    ($row:expr, $col:expr) => {
        $row.try_get($col)
            .map_err(|e| StoreError::Internal(format!("column '{}': {}", $col, e)))?
    };
}

fn row_to_namespace(row: &Row) -> Result<Namespace, StoreError> {
    let format_str: String = try_get!(row, "format");
    let format = AssetFormat::from_str(&format_str).map_err(|e: ParseError| {
        StoreError::Internal(format!("unknown asset format in DB: {}", e))
    })?;

    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    Ok(Namespace {
        id: try_get!(row, "id"),
        name: try_get!(row, "name"),
        format,
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

fn row_to_asset(row: &Row) -> Result<Asset, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    let schema_snapshot: Option<serde_json::Value> = row.try_get("schema_snapshot").ok();

    Ok(Asset {
        id: try_get!(row, "id"),
        namespace_id: try_get!(row, "namespace_id"),
        name: try_get!(row, "name"),
        location: try_get!(row, "location"),
        metadata_location: row.try_get("metadata_location").ok(),
        schema_snapshot,
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

fn row_to_version(row: &Row) -> Result<AssetVersion, StoreError> {
    Ok(AssetVersion {
        id: try_get!(row, "id"),
        asset_id: try_get!(row, "asset_id"),
        version_id: try_get!(row, "version_id"),
        metadata_location: try_get!(row, "metadata_location"),
        previous_version_id: try_get!(row, "previous_version_id"),
        timestamp: try_get!(row, "timestamp"),
    })
}

fn props_to_json(props: &HashMap<String, String>) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(props)
        .map_err(|e| StoreError::Internal(format!("properties serialization: {}", e)))
}

#[async_trait]
impl CatalogStore for PgCatalogStore {
    async fn create_namespace(
        &self,
        name: &str,
        format: AssetFormat,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;

        let props_json = props_to_json(&properties)?;
        let row = client
            .query_one(
                "INSERT INTO namespaces (name, format, properties) VALUES ($1, $2, $3) RETURNING *",
                &[&name, &format.as_str(), &props_json],
            )
            .await
            .map_err(|e| match e.code() {
                Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                    StoreError::AlreadyExists(format!("namespace '{}'", name))
                }
                _ => StoreError::Internal(e.to_string()),
            })?;

        row_to_namespace(&row)
    }

    async fn list_namespaces(
        &self,
        format: AssetFormat,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError> {
        let client = self.get_client().await?;

        let rows = client
            .query(
                "SELECT * FROM namespaces WHERE format = $1 ORDER BY created_at LIMIT $2 OFFSET $3",
                &[&format.as_str(), &(limit as i64), &offset],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.iter().map(row_to_namespace).collect()
    }

    async fn get_namespace(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_opt(
                "SELECT * FROM namespaces WHERE name = $1 AND format = $2",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_namespace(&r),
            None => Err(StoreError::NotFound(format!("namespace '{}'", name))),
        }
    }

    async fn namespace_exists(&self, name: &str, format: AssetFormat) -> Result<bool, StoreError> {
        let client = self.get_client().await?;

        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM namespaces WHERE name = $1 AND format = $2)",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.get(0))
    }

    async fn drop_namespace(&self, name: &str, format: AssetFormat) -> Result<(), StoreError> {
        let client = self.get_client().await?;

        let deleted = client
            .execute(
                "DELETE FROM namespaces WHERE name = $1 AND format = $2",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| match e.code() {
                Some(&tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION) => {
                    StoreError::Conflict(format!("namespace '{}' is not empty", name))
                }
                _ => StoreError::Internal(e.to_string()),
            })?;

        if deleted == 0 {
            return Err(StoreError::NotFound(format!("namespace '{}'", name)));
        }

        Ok(())
    }

    async fn update_namespace_properties(
        &self,
        name: &str,
        format: AssetFormat,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;

        let props_json = props_to_json(updates)?;

        // Use RETURNING * to get updated row in single query
        let row = client
            .query_opt(
                "UPDATE namespaces
                 SET properties = (properties - $1::text[]) || $2::jsonb
                 WHERE name = $3 AND format = $4
                 RETURNING *",
                &[&removals, &props_json, &name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_namespace(&r),
            None => Err(StoreError::NotFound(format!("namespace '{}'", name))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        location: &str,
        metadata_location: Option<&str>,
        schema_snapshot: Option<serde_json::Value>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        // Use single query with subquery to avoid separate get_namespace call
        // This prevents race condition where namespace could be deleted between calls
        let row = client
            .query_opt(
                "INSERT INTO assets (namespace_id, name, location, metadata_location, schema_snapshot, properties)
                 SELECT id, $2, $3, $4, $5, $6 FROM namespaces WHERE name = $1 AND format = $7
                 RETURNING *",
                &[&namespace_name, &name, &location, &metadata_location, &schema_snapshot, &props_json, &format.as_str()],
            )
            .await
            .map_err(|e| match e.code() {
                Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                    StoreError::AlreadyExists(format!(
                        "asset '{}' in namespace '{}'",
                        name, namespace_name
                    ))
                }
                _ => StoreError::Internal(e.to_string()),
            })?;

        match row {
            Some(r) => row_to_asset(&r),
            None => Err(StoreError::NotFound(format!(
                "namespace '{}'",
                namespace_name
            ))),
        }
    }

    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<Asset>, StoreError> {
        let client = self.get_client().await?;

        // Use JOIN to avoid N+1 query pattern
        let rows = client
            .query(
                "SELECT a.id, a.namespace_id, a.name, a.location, a.metadata_location, a.schema_snapshot, a.properties, a.created_at
                 FROM assets a
                 JOIN namespaces n ON a.namespace_id = n.id
                 WHERE n.name = $1 AND n.format = $2
                 ORDER BY a.created_at",
                &[&namespace_name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.iter().map(row_to_asset).collect()
    }

    async fn get_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;

        // Use JOIN to avoid separate namespace query
        let row = client
            .query_opt(
                "SELECT a.* FROM assets a
                 JOIN namespaces n ON a.namespace_id = n.id
                 WHERE n.name = $1 AND n.format = $2 AND a.name = $3",
                &[&namespace_name, &format.as_str(), &name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_asset(&r),
            None => Err(StoreError::NotFound(format!(
                "asset '{}' in namespace '{}'",
                name, namespace_name
            ))),
        }
    }

    async fn get_asset_with_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, Option<AssetVersion>), StoreError> {
        let client = self.get_client().await?;

        // Single query: get asset and its latest version using lateral join
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.location, a.metadata_location,
                        a.schema_snapshot, a.properties, a.created_at,
                        av.id as version_id_col, av.asset_id as version_asset_id,
                        av.version_id, av.metadata_location as version_metadata_location,
                        av.previous_version_id, av.timestamp as version_timestamp
                 FROM assets a
                 JOIN namespaces n ON a.namespace_id = n.id
                 LEFT JOIN LATERAL (
                     SELECT * FROM asset_versions
                     WHERE asset_id = a.id
                     ORDER BY version_id DESC LIMIT 1
                 ) av ON true
                 WHERE n.name = $1 AND n.format = $2 AND a.name = $3",
                &[&namespace_name, &format.as_str(), &name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => {
                let asset = row_to_asset(&r)?;
                // Check if version columns are present (not null)
                let version_id: Option<i64> = r.try_get("version_id").ok();
                let version = if version_id.is_some() {
                    Some(AssetVersion {
                        id: try_get!(r, "version_id_col"),
                        asset_id: try_get!(r, "version_asset_id"),
                        version_id: try_get!(r, "version_id"),
                        metadata_location: try_get!(r, "version_metadata_location"),
                        previous_version_id: r.try_get("previous_version_id").ok(),
                        timestamp: try_get!(r, "version_timestamp"),
                    })
                } else {
                    None
                };
                Ok((asset, version))
            }
            None => Err(StoreError::NotFound(format!(
                "asset '{}' in namespace '{}'",
                name, namespace_name
            ))),
        }
    }

    async fn asset_exists(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<bool, StoreError> {
        let client = self.get_client().await?;

        // Single query using CASE to distinguish namespace vs asset existence
        // Returns: 'namespace_not_found', 'asset_not_found', or 'asset_found'
        let row = client
            .query_one(
                "SELECT CASE
                    WHEN n.id IS NULL THEN 'namespace_not_found'
                    WHEN a.id IS NULL THEN 'asset_not_found'
                    ELSE 'asset_found'
                 END as status
                 FROM (SELECT 1) dummy
                 LEFT JOIN namespaces n ON n.name = $1 AND n.format = $2
                 LEFT JOIN assets a ON a.namespace_id = n.id AND a.name = $3",
                &[&namespace_name, &format.as_str(), &name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let status: String = row.get(0);
        match status.as_str() {
            "namespace_not_found" => Err(StoreError::NotFound(format!(
                "namespace '{}'",
                namespace_name
            ))),
            "asset_not_found" => Ok(false),
            "asset_found" => Ok(true),
            other => Err(StoreError::Internal(format!(
                "unexpected status from asset_exists query: {}",
                other
            ))),
        }
    }

    async fn drop_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;

        // Use DELETE with JOIN to avoid separate namespace query
        let deleted = client
            .execute(
                "DELETE FROM assets
                 WHERE namespace_id = (SELECT id FROM namespaces WHERE name = $1 AND format = $2)
                 AND name = $3",
                &[&namespace_name, &format.as_str(), &name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if deleted == 0 {
            return Err(StoreError::NotFound(format!(
                "asset '{}' in namespace '{}'",
                name, namespace_name
            )));
        }

        Ok(())
    }

    async fn rename_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;

        // Use single UPDATE with subquery to avoid separate namespace query
        let updated = client
            .execute(
                "UPDATE assets SET name = $1
                 WHERE namespace_id = (SELECT id FROM namespaces WHERE name = $2 AND format = $3)
                 AND name = $4",
                &[&new_name, &namespace_name, &format.as_str(), &name],
            )
            .await
            .map_err(|e| match e.code() {
                Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                    StoreError::AlreadyExists(format!(
                        "asset '{}' in namespace '{}'",
                        new_name, namespace_name
                    ))
                }
                _ => StoreError::Internal(e.to_string()),
            })?;

        if updated == 0 {
            return Err(StoreError::NotFound(format!(
                "asset '{}' in namespace '{}'",
                name, namespace_name
            )));
        }

        Ok(())
    }

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersion, StoreError> {
        let client = self.get_client().await?;

        // Use JOIN to avoid separate asset query
        let row = client
            .query_opt(
                "SELECT av.* FROM asset_versions av
                 JOIN assets a ON av.asset_id = a.id
                 JOIN namespaces n ON a.namespace_id = n.id
                 WHERE n.name = $1 AND n.format = $2 AND a.name = $3 AND av.version_id = $4",
                &[&namespace_name, &format.as_str(), &asset_name, &version_id],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_version(&r),
            None => Err(StoreError::NotFound(format!(
                "version {} for asset '{}'",
                version_id, asset_name
            ))),
        }
    }

    async fn load_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<AssetVersion, StoreError> {
        let client = self.get_client().await?;

        // Use JOIN to avoid separate asset query
        let row = client
            .query_opt(
                "SELECT av.* FROM asset_versions av
                 JOIN assets a ON av.asset_id = a.id
                 JOIN namespaces n ON a.namespace_id = n.id
                 WHERE n.name = $1 AND n.format = $2 AND a.name = $3
                 ORDER BY av.version_id DESC LIMIT 1",
                &[&namespace_name, &format.as_str(), &asset_name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_version(&r),
            None => Err(StoreError::NotFound(format!(
                "no versions for asset '{}'",
                asset_name
            ))),
        }
    }

    async fn list_versions(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Vec<AssetVersion>, StoreError> {
        let client = self.get_client().await?;

        // Use JOIN to avoid separate asset query
        let rows = client
            .query(
                "SELECT av.* FROM asset_versions av
                 JOIN assets a ON av.asset_id = a.id
                 JOIN namespaces n ON a.namespace_id = n.id
                 WHERE n.name = $1 AND n.format = $2 AND a.name = $3
                 ORDER BY av.version_id ASC",
                &[&namespace_name, &format.as_str(), &asset_name],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        rows.iter().map(row_to_version).collect()
    }

    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersion, StoreError> {
        if let Some(expected_prev) = previous_version_id {
            // CAS mode: transaction + previous_version_id check
            let mut client = self.get_client().await?;
            let tx = client
                .transaction()
                .await
                .map_err(|e| StoreError::Internal(format!("transaction start: {}", e)))?;

            let asset_row = tx
                .query_opt(
                    "SELECT a.* FROM assets a
                     JOIN namespaces n ON a.namespace_id = n.id
                     WHERE n.name = $1 AND n.format = $2 AND a.name = $3
                     FOR UPDATE OF a",
                    &[&namespace_name, &format.as_str(), &asset_name],
                )
                .await
                .map_err(|e| StoreError::Internal(format!("asset query: {}", e)))?;

            let asset = match asset_row {
                Some(r) => row_to_asset(&r)?,
                None => {
                    let _ = tx.rollback().await;
                    return Err(StoreError::NotFound(format!(
                        "asset '{}' in namespace '{}'",
                        asset_name, namespace_name
                    )));
                }
            };

            let current_row = tx
                .query_opt(
                    "SELECT version_id FROM asset_versions
                     WHERE asset_id = $1
                     ORDER BY version_id DESC LIMIT 1",
                    &[&asset.id],
                )
                .await
                .map_err(|e| StoreError::Internal(format!("version query: {}", e)))?;

            let current_version_id = current_row.map(|r: Row| r.get::<_, i64>(0));

            if current_version_id != Some(expected_prev) {
                let _ = tx.rollback().await;
                return Err(StoreError::Conflict(format!(
                    "version conflict: expected {:?}, got {:?}",
                    expected_prev, current_version_id
                )));
            }

            let row = tx
                .query_one(
                    "INSERT INTO asset_versions (asset_id, version_id, metadata_location, previous_version_id)
                     VALUES ($1, $2, $3, $4) RETURNING *",
                    &[
                        &asset.id,
                        &version_id,
                        &metadata_location,
                        &expected_prev,
                    ],
                )
                .await
                .map_err(|e| match e.code() {
                    Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                        StoreError::Conflict(format!(
                            "version {} for asset '{}' already exists",
                            version_id, asset_name
                        ))
                    }
                    _ => StoreError::Internal(format!("failed to create version: {}", e)),
                })?;

            tx.commit()
                .await
                .map_err(|e| StoreError::Internal(format!("transaction commit: {}", e)))?;

            row_to_version(&row)
        } else {
            // Non-CAS mode: direct INSERT
            let client = self.get_client().await?;
            let row = client
                .query_opt(
                    "INSERT INTO asset_versions (asset_id, version_id, metadata_location)
                     SELECT a.id, $4, $5 FROM assets a
                     JOIN namespaces n ON a.namespace_id = n.id
                     WHERE n.name = $1 AND n.format = $2 AND a.name = $3
                     RETURNING *",
                    &[
                        &namespace_name,
                        &format.as_str(),
                        &asset_name,
                        &version_id,
                        &metadata_location,
                    ],
                )
                .await
                .map_err(|e| match e.code() {
                    Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
                        StoreError::AlreadyExists(format!(
                            "version {} for asset '{}'",
                            version_id, asset_name
                        ))
                    }
                    _ => StoreError::Internal(e.to_string()),
                })?;

            match row {
                Some(r) => row_to_version(&r),
                None => Err(StoreError::NotFound(format!(
                    "asset '{}' in namespace '{}'",
                    asset_name, namespace_name
                ))),
            }
        }
    }

    async fn cas_update_metadata_location(
        &self,
        namespace_name: &str,
        asset_name: &str,
        format: AssetFormat,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;

        let updated = client
            .execute(
                "UPDATE assets
                 SET metadata_location = $1,
                     schema_snapshot = COALESCE($2, schema_snapshot)
                 WHERE namespace_id = (SELECT id FROM namespaces WHERE name = $3 AND format = $4)
                   AND name = $5
                   AND metadata_location = $6",
                &[
                    &new_location,
                    &new_schema_snapshot,
                    &namespace_name,
                    &format.as_str(),
                    &asset_name,
                    &expected_location,
                ],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if updated == 0 {
            return Err(StoreError::Conflict(format!(
                "metadata_location has been modified by another commit for table '{}.{}'",
                namespace_name, asset_name
            )));
        }

        Ok(())
    }
}
