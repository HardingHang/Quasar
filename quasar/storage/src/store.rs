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

fn row_to_tabular_asset(row: &Row) -> Result<TabularAsset, StoreError> {
    let schema_snapshot: Option<serde_json::Value> = row.try_get("schema_snapshot").ok();

    Ok(TabularAsset {
        asset_id: try_get!(row, "asset_id"),
        location: try_get!(row, "location"),
        metadata_location: row.try_get("metadata_location").ok(),
        schema_snapshot,
    })
}

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
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        location: &str,
        metadata_location: Option<&str>,
        schema_snapshot: Option<serde_json::Value>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        let mut client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;
        let format_str = format.as_str();

        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Internal(format!("transaction start failed: {}", e)))?;

        // Resolve namespace_id
        let ns_row = tx
            .query_opt(
                "SELECT id FROM namespaces WHERE name = $1",
                &[&namespace_name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("namespace lookup failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", namespace_name)))?;
        let ns_id: uuid::Uuid = try_get!(ns_row, "id");

        // Insert asset
        let asset_row = tx
            .query_one(
                "INSERT INTO assets (namespace_id, name, asset_type, asset_subtype, properties) VALUES ($1, $2, 'table', $3, $4) RETURNING id, namespace_id, name, asset_type, asset_subtype, comment, properties, created_at",
                &[&ns_id, &name, &format_str, &props_json],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "asset '{}' in namespace '{}'",
                            name, namespace_name
                        ));
                    }
                }
                StoreError::Internal(format!("create_asset failed: {}", e))
            })?;
        let asset_id: uuid::Uuid = try_get!(asset_row, "id");

        // Insert tabular_asset
        tx.execute(
            "INSERT INTO tabular_assets (asset_id, location, metadata_location, schema_snapshot) VALUES ($1, $2, $3, $4)",
            &[&asset_id, &location, &metadata_location, &schema_snapshot],
        )
        .await
        .map_err(|e| StoreError::Internal(format!("create tabular_asset failed: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| StoreError::Internal(format!("transaction commit failed: {}", e)))?;

        row_to_asset(&asset_row)
    }

    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<AssetWithTabular>, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let rows = client
            .query(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 ORDER BY a.name",
                &[&namespace_name, &format_str],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("list_assets failed: {}", e)))?;

        rows.iter()
            .map(|row| {
                let asset = row_to_asset(row)?;
                let tabular = row_to_tabular_asset(row)?;
                Ok(AssetWithTabular { asset, tabular })
            })
            .collect()
    }

    async fn get_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("get_asset failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        row_to_asset(&row)
    }

    async fn get_asset_with_tabular(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("get_asset_with_tabular failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;
        Ok((asset, tabular))
    }

    async fn get_asset_with_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset, Option<AssetVersionWithTabular>), StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();

        // Fetch asset + tabular
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("get_asset_with_current_version failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;

        // Fetch latest version
        let version_row = client
            .query_opt(
                "SELECT av.id, av.asset_id, av.version_key, av.version_order, av.properties, av.created_at, tav.asset_version_id, tav.metadata_location, tav.previous_asset_version_id FROM asset_versions av JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id WHERE av.asset_id = $1 ORDER BY av.version_order DESC NULLS LAST LIMIT 1",
                &[&asset.id],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("load current version failed: {}", e)))?;

        let current_version = match version_row {
            Some(row) => {
                let version = row_to_asset_version(&row)?;
                let tabular_version = row_to_tabular_version(&row)?;
                Some(AssetVersionWithTabular {
                    version,
                    tabular_version,
                })
            }
            None => None,
        };

        Ok((asset, tabular, current_version))
    }

    async fn asset_exists(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3)",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("asset_exists failed: {}", e)))?;

        Ok(row.get(0))
    }

    async fn drop_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let n = client
            .execute(
                "DELETE FROM assets a USING namespaces n WHERE a.namespace_id = n.id AND n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("drop_asset failed: {}", e)))?;

        if n == 0 {
            return Err(StoreError::NotFound(format!("asset '{}'", name)));
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
        let format_str = format.as_str();
        let n = client
            .execute(
                "UPDATE assets a SET name = $4 FROM namespaces n WHERE a.namespace_id = n.id AND n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name, &new_name],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "asset '{}' in namespace '{}'",
                            new_name, namespace_name
                        ));
                    }
                }
                StoreError::Internal(format!("rename_asset failed: {}", e))
            })?;

        if n == 0 {
            return Err(StoreError::NotFound(format!("asset '{}'", name)));
        }
        Ok(())
    }

    async fn update_asset_properties(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();

        // Fetch existing asset
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &name],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("update_asset_properties failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let mut asset = row_to_asset(&row)?;

        // Apply comment patch
        match comment {
            PatchField::Missing => {}
            PatchField::Null => asset.comment = None,
            PatchField::Value(c) => asset.comment = Some(c),
        }

        // Apply property removals
        for key in removals {
            asset.properties.remove(key);
        }

        // Apply property updates
        for (key, value) in updates {
            asset.properties.insert(key.clone(), value.clone());
        }

        // Write back
        let props_json = props_to_json(&asset.properties)?;
        let row = client
            .query_one(
                "UPDATE assets SET comment = $1, properties = $2 WHERE id = $3 RETURNING id, namespace_id, name, asset_type, asset_subtype, comment, properties, created_at",
                &[&asset.comment, &props_json, &asset.id],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("update_asset_properties write failed: {}", e)))?;

        row_to_asset(&row)
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
        namespace_name: &str,
        format: Option<AssetFormat>,
        name: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.map(|f| f.as_str());
        let rows = client
            .query(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND ($2::TEXT IS NULL OR a.asset_subtype = $2) AND ($3::TEXT IS NULL OR a.name = $3) ORDER BY a.name LIMIT $4 OFFSET $5",
                &[&namespace_name, &format_str, &name, &(limit as i64), &offset],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("list_assets_unified failed: {}", e)))?;

        rows.iter()
            .map(|row| {
                let asset = row_to_asset(row)?;
                let tabular = row_to_tabular_asset(row)?;
                Ok((asset, tabular))
            })
            .collect()
    }

    async fn get_asset_unified(
        &self,
        namespace_name: &str,
        name: &str,
        format: AssetFormat,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let row = client
            .query_opt(
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.name = $2 AND a.asset_type = 'table' AND a.asset_subtype = $3",
                &[&namespace_name, &name, &format_str],
            )
            .await
            .map_err(|e| StoreError::Internal(format!("get_asset_unified failed: {}", e)))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;
        Ok((asset, tabular))
    }
}
