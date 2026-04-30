use async_trait::async_trait;
use deadpool_postgres::Pool;
use quasar_core::{
    Asset, AssetFormat, AssetType, AssetVersion, AssetVersionWithTabular, AssetWithTabular,
    CatalogStore, Namespace, PatchField, StoreError, TabularAsset, TabularAssetVersion,
};
use serde_json;
use std::collections::HashMap;
use tokio_postgres::error::SqlState;
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
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    Ok(Namespace {
        id: try_get!(row, "id"),
        name: try_get!(row, "name"),
        comment: row.try_get("comment").ok(),
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

#[allow(dead_code)]
fn row_to_asset(row: &Row) -> Result<Asset, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    let asset_type_str: String = try_get!(row, "asset_type");
    let asset_type = match asset_type_str.as_str() {
        "table" => AssetType::Table,
        _ => {
            return Err(StoreError::Internal(format!(
                "unknown asset_type: {}",
                asset_type_str
            )))
        }
    };

    Ok(Asset {
        id: try_get!(row, "id"),
        namespace_id: try_get!(row, "namespace_id"),
        name: try_get!(row, "name"),
        asset_type,
        asset_subtype: try_get!(row, "asset_subtype"),
        comment: row.try_get("comment").ok(),
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

#[allow(dead_code)]
fn row_to_tabular_asset(row: &Row) -> Result<TabularAsset, StoreError> {
    let schema_snapshot: Option<serde_json::Value> = row.try_get("schema_snapshot").ok();

    Ok(TabularAsset {
        asset_id: try_get!(row, "asset_id"),
        location: try_get!(row, "location"),
        metadata_location: row.try_get("metadata_location").ok(),
        schema_snapshot,
    })
}

#[allow(dead_code)]
fn row_to_asset_version(row: &Row) -> Result<AssetVersion, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    Ok(AssetVersion {
        id: try_get!(row, "id"),
        asset_id: try_get!(row, "asset_id"),
        version_key: try_get!(row, "version_key"),
        version_order: row.try_get("version_order").ok(),
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

#[allow(dead_code)]
fn row_to_tabular_version(row: &Row) -> Result<TabularAssetVersion, StoreError> {
    Ok(TabularAssetVersion {
        asset_version_id: try_get!(row, "asset_version_id"),
        metadata_location: try_get!(row, "metadata_location"),
        previous_asset_version_id: row.try_get("previous_asset_version_id").ok(),
    })
}

#[allow(dead_code)]
fn props_to_json(props: &HashMap<String, String>) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(props)
        .map_err(|e| StoreError::Internal(format!("properties serialization: {}", e)))
}

#[async_trait]
impl CatalogStore for PgCatalogStore {
    async fn create_namespace(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_one(
                "INSERT INTO namespaces (name, comment, properties) VALUES ($1, $2, $3) RETURNING id, name, comment, properties, created_at",
                &[&name, &comment, &props_json],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!("namespace '{}'", name));
                    }
                }
                StoreError::Internal(format!("create_namespace failed: {}", e))
            })?;

        row_to_namespace(&row)
    }

    async fn list_namespaces(&self, offset: i64, limit: i32) -> Result<Vec<Namespace>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                "SELECT id, name, comment, properties, created_at FROM namespaces ORDER BY name LIMIT $1 OFFSET $2",
                &[&(limit as i64), &offset],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("list_namespaces failed: {}", e)))?;

        rows.iter().map(row_to_namespace).collect()
    }

    async fn get_namespace(&self, name: &str) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                "SELECT id, name, comment, properties, created_at FROM namespaces WHERE name = $1",
                &[&name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("get_namespace failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", name)))?;

        row_to_namespace(&row)
    }

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM namespaces WHERE name = $1)",
                &[&name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("namespace_exists failed: {}", e)))?;

        Ok(row.get(0))
    }

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute("DELETE FROM namespaces WHERE name = $1", &[&name])
            .await
            .map_err(|e| StoreError::Internal(format!("drop_namespace failed: {}", e)))?;

        if n == 0 {
            return Err(StoreError::NotFound(format!("namespace '{}'", name)));
        }
        Ok(())
    }

    async fn update_namespace(
        &self,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;

        // Fetch existing
        let row = client
            .query_opt(
                "SELECT id, name, comment, properties, created_at FROM namespaces WHERE name = $1",
                &[&name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("update_namespace failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", name)))?;

        let mut namespace = row_to_namespace(&row)?;

        // Apply comment patch
        match comment {
            PatchField::Missing => {}
            PatchField::Null => namespace.comment = None,
            PatchField::Value(c) => namespace.comment = Some(c),
        }

        // Apply property removals
        for key in removals {
            namespace.properties.remove(key);
        }

        // Apply property updates
        for (key, value) in updates {
            namespace.properties.insert(key.clone(), value.clone());
        }

        // Write back
        let props_json = props_to_json(&namespace.properties)?;
        let row = client
            .query_one(
                "UPDATE namespaces SET comment = $1, properties = $2 WHERE name = $3 RETURNING id, name, comment, properties, created_at",
                &[&namespace.comment, &props_json, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("update_namespace write failed: {}", e)))?;

        row_to_namespace(&row)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_asset(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
        _location: &str,
        _metadata_location: Option<&str>,
        _schema_snapshot: Option<serde_json::Value>,
        _properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        todo!()
    }

    async fn list_assets(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
    ) -> Result<Vec<AssetWithTabular>, StoreError> {
        todo!()
    }

    async fn get_asset(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
    ) -> Result<Asset, StoreError> {
        todo!()
    }

    async fn get_asset_with_tabular(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        todo!()
    }

    async fn get_asset_with_current_version(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
    ) -> Result<(Asset, TabularAsset, Option<AssetVersionWithTabular>), StoreError> {
        todo!()
    }

    async fn asset_exists(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
    ) -> Result<bool, StoreError> {
        todo!()
    }

    async fn drop_asset(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
    ) -> Result<(), StoreError> {
        todo!()
    }

    async fn rename_asset(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
        _new_name: &str,
    ) -> Result<(), StoreError> {
        todo!()
    }

    async fn update_asset_properties(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _name: &str,
        _comment: PatchField<String>,
        _removals: &[String],
        _updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        todo!()
    }

    async fn load_version(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _asset_name: &str,
        _version_id: i64,
    ) -> Result<AssetVersionWithTabular, StoreError> {
        todo!()
    }

    async fn load_current_version(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _asset_name: &str,
    ) -> Result<Option<AssetVersionWithTabular>, StoreError> {
        todo!()
    }

    async fn list_versions(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _asset_name: &str,
    ) -> Result<Vec<AssetVersionWithTabular>, StoreError> {
        todo!()
    }

    async fn create_version(
        &self,
        _namespace_name: &str,
        _format: AssetFormat,
        _asset_name: &str,
        _version_id: i64,
        _metadata_location: String,
        _previous_version_id: Option<i64>,
    ) -> Result<AssetVersionWithTabular, StoreError> {
        todo!()
    }

    #[allow(clippy::too_many_arguments)]
    async fn cas_update_metadata_location(
        &self,
        _namespace_name: &str,
        _asset_name: &str,
        _format: AssetFormat,
        _expected_location: &str,
        _new_location: &str,
        _new_schema_snapshot: Option<serde_json::Value>,
        _property_removals: &[String],
        _property_updates: &HashMap<String, String>,
    ) -> Result<(), StoreError> {
        todo!()
    }

    async fn list_assets_unified(
        &self,
        _namespace_name: &str,
        _format: Option<AssetFormat>,
        _name: Option<&str>,
        _offset: i64,
        _limit: i32,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError> {
        todo!()
    }

    async fn get_asset_unified(
        &self,
        _namespace_name: &str,
        _name: &str,
        _format: AssetFormat,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        todo!()
    }
}
