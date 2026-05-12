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
        self.pool.get().await.map_err(|e| StoreError::Internal {
            msg: format!("connection pool error: {}", &e),
            source: Some(Box::new(e)),
        })
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        let mut client = self.get_client().await?;

        let report = embedded::migrations::runner()
            .run_async(&mut **client)
            .await
            .map_err(|e| StoreError::Internal {
                msg: format!("migration failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        for migration in report.applied_migrations() {
            tracing::info!("applied migration: {}", migration.name());
        }

        Ok(())
    }
}

macro_rules! try_get {
    ($row:expr, $col:expr) => {
        $row.try_get($col).map_err(|e| StoreError::Internal {
            msg: format!("column '{}': {}", $col, &e),
            source: Some(Box::new(e)),
        })?
    };
}

fn row_to_namespace(row: &Row) -> Result<Namespace, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal {
            msg: format!("properties JSON: {}", &e),
            source: Some(Box::new(e)),
        })?;

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
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal {
            msg: format!("properties JSON: {}", &e),
            source: Some(Box::new(e)),
        })?;

    let asset_type_str: String = try_get!(row, "asset_type");
    let asset_type = match asset_type_str.as_str() {
        "table" => AssetType::Table,
        _ => {
            return Err(StoreError::Internal {
                msg: format!("unknown asset_type: {}", asset_type_str),
                source: None,
            })
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
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal {
            msg: format!("properties JSON: {}", &e),
            source: Some(Box::new(e)),
        })?;

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
        previous_version_order: row.try_get("previous_version_order").ok(),
    })
}

fn props_to_json(props: &HashMap<String, String>) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(props).map_err(|e| StoreError::Internal {
        msg: format!("properties serialization: {}", &e),
        source: Some(Box::new(e)),
    })
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
                StoreError::Internal { msg: format!("create_namespace failed: {}", &e), source: Some(Box::new(e)) }
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
            .map_err(|e| StoreError::Internal { msg: format!("list_namespaces failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal {
                msg: format!("get_namespace failed: {}", &e),
                source: Some(Box::new(e)),
            })?
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
            .map_err(|e| StoreError::Internal {
                msg: format!("namespace_exists failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        Ok(row.get(0))
    }

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute("DELETE FROM namespaces WHERE name = $1", &[&name])
            .await
            .map_err(|e| match e.code() {
                Some(code)
                    if code == &tokio_postgres::error::SqlState::RESTRICT_VIOLATION
                        || code == &tokio_postgres::error::SqlState::FOREIGN_KEY_VIOLATION =>
                {
                    StoreError::NamespaceNotEmpty {
                        namespace: name.to_string(),
                    }
                }
                Some(code) => StoreError::Internal {
                    msg: format!("drop_namespace failed: {} (sqlstate: {})", &e, code.code()),
                    source: Some(Box::new(e)),
                },
                None => StoreError::Internal {
                    msg: format!("drop_namespace failed: {}", &e),
                    source: Some(Box::new(e)),
                },
            })?;

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
            .map_err(|e| StoreError::Internal {
                msg: format!("update_namespace failed: {}", &e),
                source: Some(Box::new(e)),
            })?
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
            .map_err(|e| StoreError::Internal { msg: format!("update_namespace write failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal {
                msg: format!("transaction start failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        // Resolve namespace_id
        let ns_row = tx
            .query_opt(
                "SELECT id FROM namespaces WHERE name = $1",
                &[&namespace_name],
            )
            .await
            .map_err(|e| StoreError::Internal {
                msg: format!("namespace lookup failed: {}", &e),
                source: Some(Box::new(e)),
            })?
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
                StoreError::Internal { msg: format!("create_asset failed: {}", &e), source: Some(Box::new(e)) }
            })?;
        let asset_id: uuid::Uuid = try_get!(asset_row, "id");

        // Insert tabular_asset
        tx.execute(
            "INSERT INTO tabular_assets (asset_id, location, metadata_location, schema_snapshot) VALUES ($1, $2, $3, $4)",
            &[&asset_id, &location, &metadata_location, &schema_snapshot],
        )
        .await
        .map_err(|e| StoreError::Internal { msg: format!("create tabular_asset failed: {}", &e), source: Some(Box::new(e)) })?;

        tx.commit().await.map_err(|e| StoreError::Internal {
            msg: format!("transaction commit failed: {}", &e),
            source: Some(Box::new(e)),
        })?;

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
            .map_err(|e| StoreError::Internal { msg: format!("list_assets failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal { msg: format!("get_asset failed: {}", &e), source: Some(Box::new(e)) })?
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
            .map_err(|e| StoreError::Internal { msg: format!("get_asset_with_tabular failed: {}", &e), source: Some(Box::new(e)) })?
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
            .map_err(|e| StoreError::Internal { msg: format!("get_asset_with_current_version failed: {}", &e), source: Some(Box::new(e)) })?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;

        // Fetch latest version
        let version_row = client
            .query_opt(
                "SELECT av.id, av.asset_id, av.version_key, av.version_order, av.properties, av.created_at, tav.asset_version_id, tav.metadata_location, tav.previous_asset_version_id, prev.version_order as previous_version_order FROM asset_versions av JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id LEFT JOIN asset_versions prev ON tav.previous_asset_version_id = prev.id WHERE av.asset_id = $1 ORDER BY av.version_order DESC NULLS LAST LIMIT 1",
                &[&asset.id],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("load current version failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal { msg: format!("asset_exists failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal { msg: format!("drop_asset failed: {}", &e), source: Some(Box::new(e)) })?;

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
                StoreError::Internal { msg: format!("rename_asset failed: {}", &e), source: Some(Box::new(e)) }
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
            .map_err(|e| StoreError::Internal { msg: format!("update_asset_properties failed: {}", &e), source: Some(Box::new(e)) })?
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
            .map_err(|e| StoreError::Internal { msg: format!("update_asset_properties write failed: {}", &e), source: Some(Box::new(e)) })?;

        row_to_asset(&row)
    }

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersionWithTabular, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();
        let version_key = version_id.to_string();

        let row = client
            .query_opt(
                "SELECT av.id, av.asset_id, av.version_key, av.version_order, av.properties, av.created_at, tav.asset_version_id, tav.metadata_location, tav.previous_asset_version_id FROM asset_versions av JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id JOIN assets a ON av.asset_id = a.id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3 AND av.version_key = $4",
                &[&namespace_name, &format_str, &asset_name, &version_key],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("load_version failed: {}", &e), source: Some(Box::new(e)) })?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", asset_name)))?;

        let version = row_to_asset_version(&row)?;
        let tabular_version = row_to_tabular_version(&row)?;
        Ok(AssetVersionWithTabular {
            version,
            tabular_version,
        })
    }

    async fn load_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Option<AssetVersionWithTabular>, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();

        // Resolve asset_id
        let asset_row = client
            .query_opt(
                "SELECT a.id FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &asset_name],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("load_current_version failed: {}", &e), source: Some(Box::new(e)) })?;

        let asset_id: uuid::Uuid = match asset_row {
            Some(row) => try_get!(row, "id"),
            None => return Ok(None),
        };

        // Fetch latest version
        let version_row = client
            .query_opt(
                "SELECT av.id, av.asset_id, av.version_key, av.version_order, av.properties, av.created_at, tav.asset_version_id, tav.metadata_location, tav.previous_asset_version_id, prev.version_order as previous_version_order FROM asset_versions av JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id LEFT JOIN asset_versions prev ON tav.previous_asset_version_id = prev.id WHERE av.asset_id = $1 ORDER BY av.version_order DESC NULLS LAST LIMIT 1",
                &[&asset_id],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("load_current_version query failed: {}", &e), source: Some(Box::new(e)) })?;

        match version_row {
            Some(row) => {
                let version = row_to_asset_version(&row)?;
                let tabular_version = row_to_tabular_version(&row)?;
                Ok(Some(AssetVersionWithTabular {
                    version,
                    tabular_version,
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_versions(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Vec<AssetVersionWithTabular>, StoreError> {
        let client = self.get_client().await?;
        let format_str = format.as_str();

        // Resolve asset_id
        let asset_row = client
            .query_opt(
                "SELECT a.id FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &asset_name],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("list_versions failed: {}", &e), source: Some(Box::new(e)) })?;

        let asset_id: uuid::Uuid = match asset_row {
            Some(row) => try_get!(row, "id"),
            None => return Err(StoreError::NotFound(format!("asset '{}'", asset_name))),
        };

        // Fetch all versions
        let rows = client
            .query(
                "SELECT av.id, av.asset_id, av.version_key, av.version_order, av.properties, av.created_at, tav.asset_version_id, tav.metadata_location, tav.previous_asset_version_id FROM asset_versions av JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id WHERE av.asset_id = $1 ORDER BY av.version_order ASC NULLS LAST",
                &[&asset_id],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("list_versions query failed: {}", &e), source: Some(Box::new(e)) })?;

        rows.iter()
            .map(|row| {
                let version = row_to_asset_version(row)?;
                let tabular_version = row_to_tabular_version(row)?;
                Ok(AssetVersionWithTabular {
                    version,
                    tabular_version,
                })
            })
            .collect()
    }

    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersionWithTabular, StoreError> {
        let mut client = self.get_client().await?;
        let format_str = format.as_str();
        let version_key = version_id.to_string();
        let empty_props = serde_json::json!({});

        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Internal {
                msg: format!("transaction start failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        // Step 1: resolve asset_id
        let asset_row = tx
            .query_opt(
                "SELECT a.id FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.asset_type = 'table' AND a.asset_subtype = $2 AND a.name = $3",
                &[&namespace_name, &format_str, &asset_name],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("asset lookup failed: {}", &e), source: Some(Box::new(e)) })?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", asset_name)))?;
        let asset_id: uuid::Uuid = try_get!(asset_row, "id");

        // Step 2: optionally resolve previous_version_id
        let previous_version_uuid: Option<uuid::Uuid> = match previous_version_id {
            Some(prev_id) => {
                let prev_row = tx
                    .query_opt(
                        "SELECT id FROM asset_versions WHERE asset_id = $1 AND version_order = $2",
                        &[&asset_id, &prev_id],
                    )
                    .await
                    .map_err(|e| StoreError::Internal {
                        msg: format!("previous version lookup failed: {}", &e),
                        source: Some(Box::new(e)),
                    })?
                    .ok_or_else(|| StoreError::Conflict {
                        msg: format!("previous version {} not found", prev_id),
                    })?;
                Some(try_get!(prev_row, "id"))
            }
            None => None,
        };

        // Step 3: insert asset_versions
        let version_row = tx
            .query_one(
                "INSERT INTO asset_versions (asset_id, version_key, version_order, properties) VALUES ($1, $2, $3, $4) RETURNING id, asset_id, version_key, version_order, properties, created_at",
                &[&asset_id, &version_key, &version_id, &empty_props],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!("version {} already exists", version_id));
                    }
                }
                StoreError::Internal { msg: format!("create_version failed: {}", &e), source: Some(Box::new(e)) }
            })?;
        let version_uuid: uuid::Uuid = try_get!(version_row, "id");

        // Step 4: insert tabular_asset_versions
        tx.execute(
            "INSERT INTO tabular_asset_versions (asset_version_id, metadata_location, previous_asset_version_id) VALUES ($1, $2, $3)",
            &[&version_uuid, &metadata_location, &previous_version_uuid],
        )
        .await
        .map_err(|e| StoreError::Internal { msg: format!("create tabular_asset_version failed: {}", &e), source: Some(Box::new(e)) })?;

        tx.commit().await.map_err(|e| StoreError::Internal {
            msg: format!("transaction commit failed: {}", &e),
            source: Some(Box::new(e)),
        })?;

        let version = row_to_asset_version(&version_row)?;
        let tabular_version = TabularAssetVersion {
            asset_version_id: version_uuid,
            metadata_location,
            previous_asset_version_id: previous_version_uuid,
            previous_version_order: previous_version_id,
        };
        Ok(AssetVersionWithTabular {
            version,
            tabular_version,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn cas_update_metadata_location(
        &self,
        namespace_name: &str,
        asset_name: &str,
        format: AssetFormat,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
        property_removals: &[String],
        property_updates: &HashMap<String, String>,
    ) -> Result<(), StoreError> {
        let mut client = self.get_client().await?;
        let format_str = format.as_str();

        let tx = client
            .transaction()
            .await
            .map_err(|e| StoreError::Internal {
                msg: format!("transaction start failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        // Step 1: Lock and fetch asset
        let asset_row = tx
            .query_opt(
                "SELECT a.id, a.properties FROM assets a JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND a.name = $2 AND a.asset_type = 'table' AND a.asset_subtype = $3 FOR UPDATE",
                &[&namespace_name, &asset_name, &format_str],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("cas lock asset failed: {}", &e), source: Some(Box::new(e)) })?;

        let (asset_id, mut properties): (uuid::Uuid, HashMap<String, String>) = match asset_row {
            Some(row) => {
                let id: uuid::Uuid = try_get!(row, "id");
                let props_json: serde_json::Value = try_get!(row, "properties");
                let props: HashMap<String, String> =
                    serde_json::from_value(props_json).map_err(|e| StoreError::Internal {
                        msg: format!("properties JSON: {}", &e),
                        source: Some(Box::new(e)),
                    })?;
                (id, props)
            }
            None => return Err(StoreError::NotFound(format!("asset '{}'", asset_name))),
        };

        // Step 2: Fetch current metadata_location
        let tabular_row = tx
            .query_opt(
                "SELECT metadata_location FROM tabular_assets WHERE asset_id = $1",
                &[&asset_id],
            )
            .await
            .map_err(|e| StoreError::Internal {
                msg: format!("cas fetch metadata_location failed: {}", &e),
                source: Some(Box::new(e)),
            })?;

        let current_location: Option<String> = match tabular_row {
            Some(row) => row.try_get("metadata_location").ok(),
            None => {
                return Err(StoreError::Internal {
                    msg: "tabular asset missing".to_string(),
                    source: None,
                })
            }
        };

        // Step 3: CAS check
        if current_location.as_deref() != Some(expected_location) {
            return Err(StoreError::Conflict {
                msg: format!(
                    "metadata location has been modified by another commit (expected '{}', found '{:?}')",
                    expected_location, current_location
                ),
            });
        }

        // Step 4: Apply property changes
        for key in property_removals {
            properties.remove(key);
        }
        for (key, value) in property_updates {
            properties.insert(key.clone(), value.clone());
        }

        // Step 5: Update tabular_assets
        tx.execute(
            "UPDATE tabular_assets SET metadata_location = $1, schema_snapshot = $2 WHERE asset_id = $3",
            &[&new_location, &new_schema_snapshot, &asset_id],
        )
        .await
        .map_err(|e| StoreError::Internal { msg: format!("cas update tabular_assets failed: {}", &e), source: Some(Box::new(e)) })?;

        // Step 6: Update assets properties
        let props_json = props_to_json(&properties)?;
        tx.execute(
            "UPDATE assets SET properties = $1 WHERE id = $2",
            &[&props_json, &asset_id],
        )
        .await
        .map_err(|e| StoreError::Internal {
            msg: format!("cas update assets properties failed: {}", &e),
            source: Some(Box::new(e)),
        })?;

        tx.commit().await.map_err(|e| StoreError::Internal {
            msg: format!("transaction commit failed: {}", &e),
            source: Some(Box::new(e)),
        })?;

        Ok(())
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
                "SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype, a.comment, a.properties, a.created_at, ta.asset_id, ta.location, ta.metadata_location, ta.schema_snapshot FROM assets a JOIN tabular_assets ta ON a.id = ta.asset_id JOIN namespaces n ON a.namespace_id = n.id WHERE n.name = $1 AND ($2::TEXT IS NULL OR a.asset_subtype = $2) AND ($3::TEXT IS NULL OR a.name = $3) ORDER BY a.name, a.asset_subtype LIMIT $4 OFFSET $5",
                &[&namespace_name, &format_str, &name, &(limit as i64), &offset],
            )
            .await
            .map_err(|e| StoreError::Internal { msg: format!("list_assets_unified failed: {}", &e), source: Some(Box::new(e)) })?;

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
            .map_err(|e| StoreError::Internal { msg: format!("get_asset_unified failed: {}", &e), source: Some(Box::new(e)) })?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;
        Ok((asset, tabular))
    }
}
