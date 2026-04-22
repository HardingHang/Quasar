use async_trait::async_trait;
use deadpool_postgres::Pool;
use quasar_core::{
    Asset, AssetCommitUpdate, AssetFormat, AssetVersion, CatalogStore, Namespace, StoreError,
};
use serde_json;
use std::collections::HashMap;
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

    pub async fn migrate(&self) -> Result<(), StoreError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

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

fn row_to_namespace(row: &Row) -> Result<Namespace, StoreError> {
    let format_str: String = row.try_get("format").map_err(|e| StoreError::Internal(e.to_string()))?;
    let format = match format_str.as_str() {
        "iceberg" => AssetFormat::Iceberg,
        "lance" => AssetFormat::Lance,
        other => {
            return Err(StoreError::Internal(format!(
                "unknown asset format in DB: {}",
                other
            )))
        }
    };

    let props: serde_json::Value = row
        .try_get("properties")
        .map_err(|e| StoreError::Internal(e.to_string()))?;
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal(e.to_string()))?;

    Ok(Namespace {
        id: row.try_get("id").map_err(|e| StoreError::Internal(e.to_string()))?,
        name: row.try_get("name").map_err(|e| StoreError::Internal(e.to_string()))?,
        format,
        properties,
        created_at: row
            .try_get("created_at")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
    })
}

fn row_to_asset(row: &Row) -> Result<Asset, StoreError> {
    let props: serde_json::Value = row
        .try_get("properties")
        .map_err(|e| StoreError::Internal(e.to_string()))?;
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal(e.to_string()))?;

    let schema_snapshot: Option<serde_json::Value> = row
        .try_get("schema_snapshot")
        .ok();

    Ok(Asset {
        id: row.try_get("id").map_err(|e| StoreError::Internal(e.to_string()))?,
        namespace_id: row
            .try_get("namespace_id")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
        name: row.try_get("name").map_err(|e| StoreError::Internal(e.to_string()))?,
        location: row.try_get("location").map_err(|e| StoreError::Internal(e.to_string()))?,
        metadata_location: row.try_get("metadata_location").ok(),
        schema_snapshot,
        properties,
        created_at: row
            .try_get("created_at")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
    })
}

fn row_to_version(row: &Row) -> Result<AssetVersion, StoreError> {
    Ok(AssetVersion {
        id: row.try_get("id").map_err(|e| StoreError::Internal(e.to_string()))?,
        asset_id: row
            .try_get("asset_id")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
        version_id: row
            .try_get("version_id")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
        metadata_location: row
            .try_get("metadata_location")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
        previous_version_id: row
            .try_get("previous_version_id")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
        timestamp: row
            .try_get("timestamp")
            .map_err(|e| StoreError::Internal(e.to_string()))?,
    })
}

fn props_to_json(
    props: &HashMap<String, String>,
) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(props).map_err(|e| StoreError::Internal(e.to_string()))
}

#[async_trait]
impl CatalogStore for PgCatalogStore {
    async fn create_namespace(
        &self,
        name: &str,
        format: AssetFormat,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let props_json = props_to_json(&properties)?;
        let row = client
            .query_one(
                "INSERT INTO namespaces (name, format, properties) VALUES ($1, $2, $3) RETURNING *",
                &[
                    &name,
                    &format.as_str(),
                    &props_json,
                ],
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let row = client
            .query_opt(
                "SELECT * FROM namespaces WHERE name = $1 AND format = $2",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        match row {
            Some(r) => row_to_namespace(&r),
            None => Err(StoreError::NotFound(format!(
                "namespace '{}'",
                name
            ))),
        }
    }

    async fn namespace_exists(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<bool, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM namespaces WHERE name = $1 AND format = $2)",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.get(0))
    }

    async fn drop_namespace(
        &self,
        name: &str,
        format: AssetFormat,
    ) -> Result<(), StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let deleted = client
            .execute(
                "DELETE FROM namespaces WHERE name = $1 AND format = $2",
                &[&name, &format.as_str()],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if deleted == 0 {
            return Err(StoreError::NotFound(format!(
                "namespace '{}'",
                name
            )));
        }

        Ok(())
    }

    async fn update_namespace_properties(
        &self,
        name: &str,
        format: AssetFormat,
        removals: &[ String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let props_json = props_to_json(updates)?;

        let updated = client
            .execute(
                "UPDATE namespaces
                 SET properties = (properties - $1::text[]) || $2::jsonb
                 WHERE name = $3 AND format = $4",
                &[&removals,
                    &props_json,
                    &name,
                    &format.as_str(),
                ],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        if updated == 0 {
            return Err(StoreError::NotFound(format!(
                "namespace '{}'",
                name
            )));
        }

        self.get_namespace(name, format).await
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_one(
                "INSERT INTO assets (namespace_id, name, location, metadata_location, schema_snapshot, properties) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
                &[&ns.id, &name, &location, &metadata_location, &schema_snapshot, &props_json],
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

        row_to_asset(&row)
    }

    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<Asset>, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;

        let rows = client
            .query(
                "SELECT * FROM assets WHERE namespace_id = $1 ORDER BY created_at",
                &[&ns.id,
                ],
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;

        let row = client
            .query_opt(
                "SELECT * FROM assets WHERE namespace_id = $1 AND name = $2",
                &[&ns.id,&name,
                ],
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

    async fn asset_exists(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<bool, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;

        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM assets WHERE namespace_id = $1 AND name = $2)",
                &[&ns.id,&name,
                ],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        Ok(row.get(0))
    }

    async fn drop_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;

        let deleted = client
            .execute(
                "DELETE FROM assets WHERE namespace_id = $1 AND name = $2",
                &[&ns.id,&name,
                ],
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let ns = self.get_namespace(namespace_name, format).await?;

        let updated = client
            .execute(
                "UPDATE assets SET name = $1 WHERE namespace_id = $2 AND name = $3",
                &[&new_name,&ns.id,&name,
                ],
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

    async fn commit_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        update: AssetCommitUpdate,
    ) -> Result<AssetVersion, StoreError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let asset = self.get_asset(namespace_name, format, asset_name).await?;

        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let current_row = tx
            .query_opt(
                "SELECT version_id FROM asset_versions WHERE asset_id = $1 ORDER BY version_id DESC LIMIT 1",
                &[&asset.id,
                ],
            )
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let current_version_id = current_row.map(|r: Row| r.get::<_, i64>(0));

        if current_version_id != update.previous_version_id {
            let _ = tx.rollback().await;
            return Err(StoreError::Conflict(format!(
                "version conflict: expected {:?}, got {:?}",
                update.previous_version_id, current_version_id
            )));
        }

        let new_version_id = current_version_id.map(|v| v + 1).unwrap_or(1);

        let row = tx
            .query_one(
                "INSERT INTO asset_versions (asset_id, version_id, metadata_location, previous_version_id) VALUES ($1, $2, $3, $4) RETURNING *",
                &[
                    &asset.id,
                    &new_version_id,
                    &update.metadata_location,
                    &update.previous_version_id,
                ],
            )
            .await
            .map_err(|e| {
                StoreError::Internal(format!("failed to commit version: {}", e))
            })?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        row_to_version(&row)
    }

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersion, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let asset = self.get_asset(namespace_name, format, asset_name).await?;

        let row = client
            .query_opt(
                "SELECT * FROM asset_versions WHERE asset_id = $1 AND version_id = $2",
                &[&asset.id,&version_id,
                ],
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let asset = self.get_asset(namespace_name, format, asset_name).await?;

        let row = client
            .query_opt(
                "SELECT * FROM asset_versions WHERE asset_id = $1 ORDER BY version_id DESC LIMIT 1",
                &[&asset.id,
                ],
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
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let asset = self.get_asset(namespace_name, format, asset_name).await?;

        let rows = client
            .query(
                "SELECT * FROM asset_versions WHERE asset_id = $1 ORDER BY version_id ASC",
                &[&asset.id,
                ],
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
    ) -> Result<AssetVersion, StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let asset = self.get_asset(namespace_name, format, asset_name).await?;

        let row = client
            .query_one(
                "INSERT INTO asset_versions (asset_id, version_id, metadata_location) VALUES ($1, $2, $3) RETURNING *",
                &[&asset.id, &version_id, &metadata_location],
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

        row_to_version(&row)
    }

    async fn commit_iceberg_table(
        &self,
        namespace_name: &str,
        asset_name: &str,
        expected_metadata_location: &str,
        new_metadata_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
    ) -> Result<(), StoreError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|e| StoreError::Internal(e.to_string()))?;

        let updated = client
            .execute(
                "UPDATE assets
                 SET metadata_location = $1,
                     schema_snapshot = COALESCE($2, schema_snapshot)
                 WHERE namespace_id = (SELECT id FROM namespaces WHERE name = $3 AND format = 'iceberg')
                   AND name = $4
                   AND metadata_location = $5",
                &[
                    &new_metadata_location,
                    &new_schema_snapshot,
                    &namespace_name,
                    &asset_name,
                    &expected_metadata_location,
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
