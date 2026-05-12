use async_trait::async_trait;
use deadpool_postgres::Pool;
use quasar_core::{
    Asset, AssetFormat, AssetVersion, AssetVersionWithTabular, AssetWithTabular, CatalogStore,
    Namespace, PatchField, StoreError, TabularAsset, TabularAssetVersion,
};
use std::collections::HashMap;
use tokio_postgres::error::SqlState;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::queries;
use crate::schema;

pub struct PgCatalogStore {
    pool: Pool,
}

/// Phase 2 transitional binding. The V2 `CatalogStore` trait surface has no
/// domain parameter, so storage scopes every call to the `default` domain
/// seeded by `schema/init.sql`. Phase 3 lifts domain into the trait surface
/// and removes this constant along with the seed.
// TODO(v3-phase3): replace hardcoded "default" with adapter-supplied domain_name.
const DEFAULT_DOMAIN: &str = "default";

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

    /// Bootstrap (or repair) the V3 catalog schema for this pool. Replaces
    /// V2's refinery-based `migrate()`: V3 ships a single idempotent
    /// `schema/init.sql` that creates every catalog object, seeds the
    /// registry tables, and seeds the transitional `default` domain.
    pub async fn initialize(&self) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        schema::initialize(&client).await
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
        domain_id: try_get!(row, "domain_id"),
        name: try_get!(row, "name"),
        comment: row.try_get("comment").ok(),
        properties,
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
}

fn row_to_asset(row: &Row) -> Result<Asset, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal {
            msg: format!("properties JSON: {}", &e),
            source: Some(Box::new(e)),
        })?;

    Ok(Asset {
        id: try_get!(row, "id"),
        namespace_id: try_get!(row, "namespace_id"),
        name: try_get!(row, "name"),
        asset_type: try_get!(row, "asset_type"),
        comment: row.try_get("comment").ok(),
        properties,
        deleted_at: row.try_get("deleted_at").ok().flatten(),
        created_by: row.try_get("created_by").ok().flatten(),
        updated_by: row.try_get("updated_by").ok().flatten(),
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
}

fn row_to_tabular_asset(row: &Row) -> Result<TabularAsset, StoreError> {
    let schema_snapshot: Option<serde_json::Value> = row.try_get("schema_snapshot").ok().flatten();
    Ok(TabularAsset {
        asset_id: try_get!(row, "asset_id"),
        format: try_get!(row, "format"),
        location: try_get!(row, "location"),
        metadata_location: row.try_get("metadata_location").ok().flatten(),
        schema_snapshot,
        created_at: try_get!(row, "tabular_created_at"),
        updated_at: try_get!(row, "tabular_updated_at"),
    })
}

/// Variant of `row_to_tabular_asset` that returns `None` when the row's
/// tabular extension columns are all NULL (i.e., a `LEFT JOIN` row for a
/// non-tabular asset). Used by the unified read queries.
fn row_to_tabular_asset_optional(row: &Row) -> Result<Option<TabularAsset>, StoreError> {
    let asset_id: Option<Uuid> = row.try_get("asset_id").ok().flatten();
    match asset_id {
        None => Ok(None),
        Some(_) => row_to_tabular_asset(row).map(Some),
    }
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
        version_order: row.try_get("version_order").ok().flatten(),
        previous_version_id: row.try_get("previous_version_id").ok().flatten(),
        comment: row.try_get("comment").ok().flatten(),
        properties,
        created_at: try_get!(row, "created_at"),
    })
}

fn row_to_tabular_version(row: &Row) -> Result<TabularAssetVersion, StoreError> {
    Ok(TabularAssetVersion {
        version_id: try_get!(row, "version_id"),
        metadata_location: try_get!(row, "metadata_location"),
        created_at: try_get!(row, "tabular_created_at"),
    })
}

fn props_to_json(props: &HashMap<String, String>) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(props).map_err(|e| StoreError::Internal {
        msg: format!("properties serialization: {}", &e),
        source: Some(Box::new(e)),
    })
}

fn internal_err<E>(op: &'static str) -> impl FnOnce(E) -> StoreError
where
    E: std::error::Error + Send + Sync + 'static,
{
    move |e| StoreError::Internal {
        msg: format!("{} failed: {}", op, &e),
        source: Some(Box::new(e)),
    }
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
            .query_opt(
                queries::namespace::CREATE,
                &[&DEFAULT_DOMAIN, &name, &comment, &props_json],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!("namespace '{}'", name));
                    }
                }
                StoreError::Internal {
                    msg: format!("create_namespace failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?
            .ok_or_else(|| StoreError::NotFound(format!("domain '{}'", DEFAULT_DOMAIN)))?;

        row_to_namespace(&row)
    }

    async fn list_namespaces(&self, offset: i64, limit: i32) -> Result<Vec<Namespace>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::namespace::LIST_BY_DOMAIN,
                &[&DEFAULT_DOMAIN, &(limit as i64), &offset],
            )
            .await
            .map_err(internal_err("list_namespaces"))?;

        rows.iter().map(row_to_namespace).collect()
    }

    async fn get_namespace(&self, name: &str) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::namespace::GET_BY_NAME, &[&DEFAULT_DOMAIN, &name])
            .await
            .map_err(internal_err("get_namespace"))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", name)))?;

        row_to_namespace(&row)
    }

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(queries::namespace::EXISTS, &[&DEFAULT_DOMAIN, &name])
            .await
            .map_err(internal_err("namespace_exists"))?;

        Ok(row.get(0))
    }

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute(queries::namespace::DELETE, &[&DEFAULT_DOMAIN, &name])
            .await
            .map_err(|e| match e.code() {
                Some(code)
                    if code == &SqlState::RESTRICT_VIOLATION
                        || code == &SqlState::FOREIGN_KEY_VIOLATION =>
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

        let existing = client
            .query_opt(queries::namespace::GET_BY_NAME, &[&DEFAULT_DOMAIN, &name])
            .await
            .map_err(internal_err("update_namespace"))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", name)))?;

        let mut namespace = row_to_namespace(&existing)?;

        match comment {
            PatchField::Missing => {}
            PatchField::Null => namespace.comment = None,
            PatchField::Value(c) => namespace.comment = Some(c),
        }

        for key in removals {
            namespace.properties.remove(key);
        }
        for (key, value) in updates {
            namespace.properties.insert(key.clone(), value.clone());
        }

        let props_json = props_to_json(&namespace.properties)?;
        let row = client
            .query_one(
                queries::namespace::UPDATE,
                &[&namespace.comment, &props_json, &DEFAULT_DOMAIN, &name],
            )
            .await
            .map_err(internal_err("update_namespace write"))?;

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
        let comment: Option<String> = None;
        let asset_type = "table";

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let asset_row = tx
            .query_opt(
                queries::asset::CREATE,
                &[
                    &DEFAULT_DOMAIN,
                    &namespace_name,
                    &name,
                    &asset_type,
                    &comment,
                    &props_json,
                ],
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
                StoreError::Internal {
                    msg: format!("create_asset failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", namespace_name)))?;

        let asset_id: Uuid = try_get!(asset_row, "id");

        tx.execute(
            queries::asset::CREATE_TABULAR,
            &[
                &asset_id,
                &format_str,
                &location,
                &metadata_location,
                &schema_snapshot,
            ],
        )
        .await
        .map_err(internal_err("create tabular_asset"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

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
                queries::asset::LIST_TABULAR_BY_NAMESPACE,
                &[&DEFAULT_DOMAIN, &namespace_name, &format_str],
            )
            .await
            .map_err(internal_err("list_assets"))?;

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
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &format_str],
            )
            .await
            .map_err(internal_err("get_asset"))?
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
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &format_str],
            )
            .await
            .map_err(internal_err("get_asset_with_tabular"))?
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

        let asset_row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &format_str],
            )
            .await
            .map_err(internal_err("get_asset_with_current_version"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&asset_row)?;
        let tabular = row_to_tabular_asset(&asset_row)?;

        let version_row = client
            .query_opt(queries::version::GET_LATEST_TABULAR, &[&asset.id])
            .await
            .map_err(internal_err("load current version"))?;

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
                queries::asset::EXISTS_TABULAR,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &format_str],
            )
            .await
            .map_err(internal_err("asset_exists"))?;

        Ok(row.get(0))
    }

    async fn drop_asset(
        &self,
        namespace_name: &str,
        _format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError> {
        // Phase 2: V3 schema enforces active-name uniqueness within a
        // namespace regardless of format, so the V2 trait's `format` filter
        // is ignored here. Format-aware drop semantics return in Phase 3 at
        // the adapter layer (404 if asset has a different format).
        let client = self.get_client().await?;
        let n = client
            .execute(
                queries::asset::DELETE,
                &[&DEFAULT_DOMAIN, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("drop_asset"))?;

        if n == 0 {
            return Err(StoreError::NotFound(format!("asset '{}'", name)));
        }
        Ok(())
    }

    async fn rename_asset(
        &self,
        namespace_name: &str,
        _format: AssetFormat,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute(
                queries::asset::RENAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &new_name],
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
                StoreError::Internal {
                    msg: format!("rename_asset failed: {}", &e),
                    source: Some(Box::new(e)),
                }
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

        let existing = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &name, &format_str],
            )
            .await
            .map_err(internal_err("update_asset_properties"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let mut asset = row_to_asset(&existing)?;

        match comment {
            PatchField::Missing => {}
            PatchField::Null => asset.comment = None,
            PatchField::Value(c) => asset.comment = Some(c),
        }

        for key in removals {
            asset.properties.remove(key);
        }
        for (key, value) in updates {
            asset.properties.insert(key.clone(), value.clone());
        }

        let props_json = props_to_json(&asset.properties)?;
        let row = client
            .query_one(
                queries::asset::UPDATE_PROPERTIES,
                &[&asset.comment, &props_json, &asset.id],
            )
            .await
            .map_err(internal_err("update_asset_properties write"))?;

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

        let asset_row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &asset_name, &format_str],
            )
            .await
            .map_err(internal_err("load_version asset lookup"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", asset_name)))?;
        let asset_id: Uuid = try_get!(asset_row, "id");

        let version_row = client
            .query_opt(queries::version::GET_BY_KEY, &[&asset_id, &version_key])
            .await
            .map_err(internal_err("load_version"))?
            .ok_or_else(|| StoreError::NotFound(format!("version {}", version_id)))?;

        let version = row_to_asset_version(&version_row)?;

        // Need the tabular extension too. We have version.id; fetch the row
        // directly rather than re-joining through the asset path.
        let tav_row = client
            .query_one(
                r#"
                SELECT version_id, metadata_location, created_at AS tabular_created_at
                FROM tabular_asset_versions
                WHERE version_id = $1
                "#,
                &[&version.id],
            )
            .await
            .map_err(internal_err("load_version tabular"))?;
        let tabular_version = row_to_tabular_version(&tav_row)?;

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

        let asset_row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &asset_name, &format_str],
            )
            .await
            .map_err(internal_err("load_current_version asset lookup"))?;
        let asset_id: Uuid = match asset_row {
            Some(row) => try_get!(row, "id"),
            None => return Ok(None),
        };

        let version_row = client
            .query_opt(queries::version::GET_LATEST_TABULAR, &[&asset_id])
            .await
            .map_err(internal_err("load_current_version"))?;

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

        let asset_row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &asset_name, &format_str],
            )
            .await
            .map_err(internal_err("list_versions asset lookup"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", asset_name)))?;
        let asset_id: Uuid = try_get!(asset_row, "id");

        // Join in the tabular extension columns so we can return paired rows.
        let rows = client
            .query(
                r#"
                SELECT av.id, av.asset_id, av.version_key, av.version_order, av.previous_version_id,
                       av.comment, av.properties, av.created_at,
                       tav.version_id, tav.metadata_location,
                       tav.created_at AS tabular_created_at
                FROM asset_versions av
                JOIN tabular_asset_versions tav ON av.id = tav.version_id
                WHERE av.asset_id = $1
                ORDER BY av.version_order ASC NULLS LAST
                "#,
                &[&asset_id],
            )
            .await
            .map_err(internal_err("list_versions"))?;

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
        let comment: Option<String> = None;

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let asset_row = tx
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&DEFAULT_DOMAIN, &namespace_name, &asset_name, &format_str],
            )
            .await
            .map_err(internal_err("asset lookup"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", asset_name)))?;
        let asset_id: Uuid = try_get!(asset_row, "id");

        let previous_version_uuid: Option<Uuid> = match previous_version_id {
            Some(prev_order) => {
                let prev_row = tx
                    .query_opt(
                        "SELECT id FROM asset_versions WHERE asset_id = $1 AND version_order = $2",
                        &[&asset_id, &prev_order],
                    )
                    .await
                    .map_err(internal_err("previous version lookup"))?
                    .ok_or_else(|| StoreError::Conflict {
                        msg: format!("previous version {} not found", prev_order),
                    })?;
                Some(try_get!(prev_row, "id"))
            }
            None => None,
        };

        let version_row = tx
            .query_one(
                queries::version::CREATE,
                &[
                    &asset_id,
                    &version_key,
                    &version_id,
                    &previous_version_uuid,
                    &comment,
                    &empty_props,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "version {} already exists",
                            version_id
                        ));
                    }
                    if db_err.code() == &SqlState::CHECK_VIOLATION {
                        return StoreError::Conflict {
                            msg: db_err.message().to_string(),
                        };
                    }
                }
                StoreError::Internal {
                    msg: format!("create_version failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?;

        let version = row_to_asset_version(&version_row)?;

        let tav_row = tx
            .query_one(
                queries::version::CREATE_TABULAR,
                &[&version.id, &metadata_location],
            )
            .await
            .map_err(internal_err("create tabular_asset_version"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        let tabular_version = row_to_tabular_version(&tav_row)?;

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
            .map_err(internal_err("transaction start"))?;

        // Step 1: CAS update tabular_assets.metadata_location. Zero rows
        // affected ⇒ optimistic concurrency conflict (Iceberg CAS commit).
        let cas_row = tx
            .query_opt(
                queries::asset::CAS_UPDATE_METADATA_LOCATION,
                &[
                    &new_location,
                    &new_schema_snapshot,
                    &DEFAULT_DOMAIN,
                    &namespace_name,
                    &asset_name,
                    &format_str,
                    &expected_location,
                ],
            )
            .await
            .map_err(internal_err("cas update"))?;

        let asset_id: Uuid = match cas_row {
            Some(row) => try_get!(row, "asset_id"),
            None => {
                // Either the asset does not exist or the metadata_location
                // does not match `expected_location`. The Iceberg adapter
                // interprets Conflict as a CommitFailedException; NotFound
                // semantics could be distinguished in a follow-up if needed.
                return Err(StoreError::Conflict {
                    msg: format!(
                        "metadata location for '{}' has been modified by another commit (expected '{}')",
                        asset_name, expected_location
                    ),
                });
            }
        };

        // Step 2: merge property delta into assets row.
        let existing_props_row = tx
            .query_one(
                "SELECT properties FROM assets WHERE id = $1 AND deleted_at IS NULL",
                &[&asset_id],
            )
            .await
            .map_err(internal_err("cas fetch properties"))?;
        let props_json: serde_json::Value = try_get!(existing_props_row, "properties");
        let mut properties: HashMap<String, String> =
            serde_json::from_value(props_json).map_err(|e| StoreError::Internal {
                msg: format!("properties JSON: {}", &e),
                source: Some(Box::new(e)),
            })?;

        for key in property_removals {
            properties.remove(key);
        }
        for (key, value) in property_updates {
            properties.insert(key.clone(), value.clone());
        }
        let merged = props_to_json(&properties)?;

        tx.execute(
            queries::asset::UPDATE_PROPERTIES_BY_ID,
            &[&merged, &asset_id],
        )
        .await
        .map_err(internal_err("cas update properties"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

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
                queries::asset::LIST_UNIFIED,
                &[
                    &DEFAULT_DOMAIN,
                    &namespace_name,
                    &format_str,
                    &name,
                    &(limit as i64),
                    &offset,
                ],
            )
            .await
            .map_err(internal_err("list_assets_unified"))?;

        // Phase 2: the V2 trait surface returns `Vec<(Asset, TabularAsset)>`;
        // LEFT JOIN rows for non-tabular assets surface as `None` and are
        // skipped. Today every active asset is tabular so this filter is a
        // no-op in practice, but it preserves V2 semantics if a future asset
        // type slips into the namespace before Phase 3 lands the
        // `Option<TabularAsset>` trait return shape.
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let asset = row_to_asset(row)?;
            if let Some(tabular) = row_to_tabular_asset_optional(row)? {
                out.push((asset, tabular));
            }
        }
        Ok(out)
    }

    async fn get_asset_unified(
        &self,
        namespace_name: &str,
        name: &str,
        _format: AssetFormat,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        // Phase 2: `_format` is ignored — V3 active asset names are unique
        // within a namespace, so a single name resolves to at most one row
        // regardless of format. Endpoint-level format filtering is the
        // adapter's job (Phase 3).
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::asset::GET_UNIFIED,
                &[&DEFAULT_DOMAIN, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("get_asset_unified"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset_optional(&row)?
            .ok_or_else(|| StoreError::NotFound(format!("tabular asset '{}'", name)))?;
        Ok((asset, tabular))
    }
}
