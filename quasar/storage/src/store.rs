use async_trait::async_trait;
use deadpool_postgres::Pool;
use quasar_core::{
    Asset, AssetStore, AssetVersion, CasCommitStore, Domain, DomainPatch, DomainStore, Namespace,
    NamespaceStore, PatchField, StoreError, TabularAsset, TabularAssetVersion, TabularStore,
    TabularVersionStore, UnifiedQueryStore, VersionStore,
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

// ── Row mappers ────────────────────────────────────────────────────────────

fn row_to_domain(row: &Row) -> Result<Domain, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> =
        serde_json::from_value(props).map_err(|e| StoreError::Internal {
            msg: format!("properties JSON: {}", &e),
            source: Some(Box::new(e)),
        })?;

    Ok(Domain {
        id: try_get!(row, "id"),
        name: try_get!(row, "name"),
        comment: row.try_get("comment").ok().flatten(),
        properties,
        storage_type: row.try_get("storage_type").ok().flatten(),
        storage_config: try_get!(row, "storage_config"),
        warehouse: row.try_get("warehouse").ok().flatten(),
        owner: row.try_get("owner").ok().flatten(),
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
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

/// `LEFT JOIN`-friendly variant of [`row_to_tabular_asset`]. Returns `None`
/// when the row has no tabular extension (e.g., a future non-tabular asset
/// type whose `LEFT JOIN tabular_assets` row has all-NULL extension
/// columns).
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

/// Apply a `PatchField` to an existing optional value.
fn apply_patch<T>(current: Option<T>, patch: PatchField<T>) -> Option<T> {
    match patch {
        PatchField::Missing => current,
        PatchField::Null => None,
        PatchField::Value(v) => Some(v),
    }
}

// ── DomainStore ────────────────────────────────────────────────────────────

#[async_trait]
impl DomainStore for PgCatalogStore {
    async fn create_domain(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
        storage_type: Option<String>,
        storage_config: serde_json::Value,
        warehouse: Option<String>,
        owner: Option<String>,
    ) -> Result<Domain, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_one(
                queries::domain::CREATE,
                &[
                    &name,
                    &comment,
                    &props_json,
                    &storage_type,
                    &storage_config,
                    &warehouse,
                    &owner,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!("domain '{}'", name));
                    }
                }
                StoreError::Internal {
                    msg: format!("create_domain failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?;

        row_to_domain(&row)
    }

    async fn list_domains(&self, offset: i64, limit: i32) -> Result<Vec<Domain>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(queries::domain::LIST, &[&(limit as i64), &offset])
            .await
            .map_err(internal_err("list_domains"))?;

        rows.iter().map(row_to_domain).collect()
    }

    async fn get_domain(&self, name: &str) -> Result<Domain, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::domain::GET_BY_NAME, &[&name])
            .await
            .map_err(internal_err("get_domain"))?
            .ok_or_else(|| StoreError::NotFound(format!("domain '{}'", name)))?;

        row_to_domain(&row)
    }

    async fn domain_exists(&self, name: &str) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(queries::domain::EXISTS, &[&name])
            .await
            .map_err(internal_err("domain_exists"))?;

        Ok(row.get(0))
    }

    async fn drop_domain(&self, name: &str) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute(queries::domain::DELETE, &[&name])
            .await
            .map_err(|e| match e.code() {
                Some(code)
                    if code == &SqlState::RESTRICT_VIOLATION
                        || code == &SqlState::FOREIGN_KEY_VIOLATION =>
                {
                    StoreError::DomainNotEmpty {
                        domain: name.to_string(),
                    }
                }
                Some(code) => StoreError::Internal {
                    msg: format!("drop_domain failed: {} (sqlstate: {})", &e, code.code()),
                    source: Some(Box::new(e)),
                },
                None => StoreError::Internal {
                    msg: format!("drop_domain failed: {}", &e),
                    source: Some(Box::new(e)),
                },
            })?;

        if n == 0 {
            return Err(StoreError::NotFound(format!("domain '{}'", name)));
        }
        Ok(())
    }

    async fn update_domain(&self, name: &str, patch: DomainPatch) -> Result<Domain, StoreError> {
        let client = self.get_client().await?;

        let existing = client
            .query_opt(queries::domain::GET_BY_NAME, &[&name])
            .await
            .map_err(internal_err("update_domain"))?
            .ok_or_else(|| StoreError::NotFound(format!("domain '{}'", name)))?;

        let mut domain = row_to_domain(&existing)?;
        domain.comment = apply_patch(domain.comment, patch.comment);
        domain.storage_type = apply_patch(domain.storage_type, patch.storage_type);
        domain.warehouse = apply_patch(domain.warehouse, patch.warehouse);
        domain.owner = apply_patch(domain.owner, patch.owner);
        match patch.storage_config {
            PatchField::Missing => {}
            PatchField::Null => {
                domain.storage_config = serde_json::Value::Object(Default::default());
            }
            PatchField::Value(v) => domain.storage_config = v,
        }
        for key in &patch.property_removals {
            domain.properties.remove(key);
        }
        for (key, value) in patch.property_updates {
            domain.properties.insert(key, value);
        }
        let props_json = props_to_json(&domain.properties)?;

        let row = client
            .query_one(
                queries::domain::UPDATE,
                &[
                    &domain.comment,
                    &props_json,
                    &domain.storage_type,
                    &domain.storage_config,
                    &domain.warehouse,
                    &domain.owner,
                    &name,
                ],
            )
            .await
            .map_err(internal_err("update_domain write"))?;

        row_to_domain(&row)
    }
}

// ── NamespaceStore ─────────────────────────────────────────────────────────

#[async_trait]
impl NamespaceStore for PgCatalogStore {
    async fn create_namespace(
        &self,
        domain_name: &str,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_opt(
                queries::namespace::CREATE,
                &[&domain_name, &name, &comment, &props_json],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "namespace '{}' in domain '{}'",
                            name, domain_name
                        ));
                    }
                }
                StoreError::Internal {
                    msg: format!("create_namespace failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?
            .ok_or_else(|| StoreError::NotFound(format!("domain '{}'", domain_name)))?;

        row_to_namespace(&row)
    }

    async fn list_namespaces(
        &self,
        domain_name: &str,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::namespace::LIST_BY_DOMAIN,
                &[&domain_name, &(limit as i64), &offset],
            )
            .await
            .map_err(internal_err("list_namespaces"))?;

        rows.iter().map(row_to_namespace).collect()
    }

    async fn get_namespace(&self, domain_name: &str, name: &str) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::namespace::GET_BY_NAME, &[&domain_name, &name])
            .await
            .map_err(internal_err("get_namespace"))?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", name)))?;

        row_to_namespace(&row)
    }

    async fn namespace_exists(&self, domain_name: &str, name: &str) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(queries::namespace::EXISTS, &[&domain_name, &name])
            .await
            .map_err(internal_err("namespace_exists"))?;

        Ok(row.get(0))
    }

    async fn drop_namespace(&self, domain_name: &str, name: &str) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute(queries::namespace::DELETE, &[&domain_name, &name])
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
        domain_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError> {
        let client = self.get_client().await?;

        let existing = client
            .query_opt(queries::namespace::GET_BY_NAME, &[&domain_name, &name])
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
                &[&namespace.comment, &props_json, &domain_name, &name],
            )
            .await
            .map_err(internal_err("update_namespace write"))?;

        row_to_namespace(&row)
    }
}

// ── AssetStore ─────────────────────────────────────────────────────────────

#[async_trait]
impl AssetStore for PgCatalogStore {
    async fn create_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        asset_type: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_opt(
                queries::asset::CREATE,
                &[
                    &domain_name,
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

        row_to_asset(&row)
    }

    async fn get_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::asset::GET_BY_NAME,
                &[&domain_name, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("get_asset"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        row_to_asset(&row)
    }

    async fn asset_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<bool, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM assets a
                    JOIN namespaces ns ON a.namespace_id = ns.id
                    JOIN domains d ON ns.domain_id = d.id
                    WHERE d.name = $1 AND ns.name = $2 AND a.name = $3
                      AND a.deleted_at IS NULL
                )
                "#,
                &[&domain_name, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("asset_exists"))?;

        Ok(row.get(0))
    }

    async fn drop_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;
        let n = client
            .execute(
                queries::asset::DELETE,
                &[&domain_name, &namespace_name, &name],
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
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        new_name: &str,
        new_namespace_name: Option<&str>,
    ) -> Result<(), StoreError> {
        let client = self.get_client().await?;

        let map_err = |e: tokio_postgres::Error| -> StoreError {
            if let Some(db_err) = e.as_db_error() {
                if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                    let target_ns = new_namespace_name.unwrap_or(namespace_name);
                    return StoreError::AlreadyExists(format!(
                        "asset '{}' in namespace '{}'",
                        new_name, target_ns
                    ));
                }
            }
            StoreError::Internal {
                msg: format!("rename_asset failed: {}", &e),
                source: Some(Box::new(e)),
            }
        };

        let n = if let Some(target_ns) = new_namespace_name {
            if target_ns != namespace_name {
                // Cross-namespace rename: look up target namespace id first.
                let target_row = client
                    .query_opt(
                        queries::namespace::GET_BY_NAME,
                        &[&domain_name, &target_ns],
                    )
                    .await
                    .map_err(internal_err("rename_asset lookup target namespace"))?;

                let target_ns_id: Uuid = match target_row {
                    Some(row) => row.get("id"),
                    None => {
                        return Err(StoreError::NotFound(format!(
                            "target namespace '{}'",
                            target_ns
                        )));
                    }
                };

                client
                    .execute(
                        queries::asset::RENAME_WITH_NAMESPACE,
                        &[
                            &domain_name,
                            &namespace_name,
                            &name,
                            &new_name,
                            &target_ns_id,
                        ],
                    )
                    .await
                    .map_err(map_err)?
            } else {
                client
                    .execute(
                        queries::asset::RENAME,
                        &[&domain_name, &namespace_name, &name, &new_name],
                    )
                    .await
                    .map_err(map_err)?
            }
        } else {
            client
                .execute(
                    queries::asset::RENAME,
                    &[&domain_name, &namespace_name, &name, &new_name],
                )
                .await
                .map_err(map_err)?
        };

        if n == 0 {
            return Err(StoreError::NotFound(format!("asset '{}'", name)));
        }
        Ok(())
    }

    async fn update_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError> {
        let client = self.get_client().await?;

        let existing = client
            .query_opt(
                queries::asset::GET_BY_NAME,
                &[&domain_name, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("update_asset"))?
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
            .map_err(internal_err("update_asset write"))?;

        row_to_asset(&row)
    }
}

// ── TabularStore ───────────────────────────────────────────────────────────

#[async_trait]
impl TabularStore for PgCatalogStore {
    async fn create_tabular_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        format: &str,
        location: &str,
        metadata_location: Option<&str>,
        schema_snapshot: Option<serde_json::Value>,
        properties: HashMap<String, String>,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        let mut client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;
        let asset_type = "table";
        let comment: Option<String> = None;

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let asset_row = tx
            .query_opt(
                queries::asset::CREATE,
                &[
                    &domain_name,
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
                    msg: format!("create_tabular_asset failed: {}", &e),
                    source: Some(Box::new(e)),
                }
            })?
            .ok_or_else(|| StoreError::NotFound(format!("namespace '{}'", namespace_name)))?;

        let asset = row_to_asset(&asset_row)?;

        let tabular_row = tx
            .query_one(
                queries::asset::CREATE_TABULAR,
                &[
                    &asset.id,
                    &format,
                    &location,
                    &metadata_location,
                    &schema_snapshot,
                ],
            )
            .await
            .map_err(internal_err("create tabular_asset"))?;

        let tabular = row_to_tabular_asset(&tabular_row)?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok((asset, tabular))
    }

    async fn list_tabular_assets(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::asset::LIST_TABULAR_BY_NAMESPACE,
                &[&domain_name, &namespace_name, &format],
            )
            .await
            .map_err(internal_err("list_tabular_assets"))?;

        rows.iter()
            .map(|row| {
                let asset = row_to_asset(row)?;
                let tabular = row_to_tabular_asset(row)?;
                Ok((asset, tabular))
            })
            .collect()
    }

    async fn get_tabular_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&domain_name, &namespace_name, &name, &format],
            )
            .await
            .map_err(internal_err("get_tabular_asset"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset(&row)?;
        Ok((asset, tabular))
    }

    async fn get_tabular_asset_with_current_version(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<
        (
            Asset,
            TabularAsset,
            Option<(AssetVersion, TabularAssetVersion)>,
        ),
        StoreError,
    > {
        let client = self.get_client().await?;
        let asset_row = client
            .query_opt(
                queries::asset::GET_TABULAR_BY_NAME,
                &[&domain_name, &namespace_name, &name, &format],
            )
            .await
            .map_err(internal_err("get_tabular_asset_with_current_version"))?
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
                Some((version, tabular_version))
            }
            None => None,
        };

        Ok((asset, tabular, current_version))
    }
}

// ── VersionStore ───────────────────────────────────────────────────────────

#[async_trait]
impl VersionStore for PgCatalogStore {
    async fn create_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<AssetVersion, StoreError> {
        let client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let row = client
            .query_one(
                queries::version::CREATE,
                &[
                    &asset_id,
                    &version_key,
                    &version_order,
                    &previous_version_id,
                    &comment,
                    &props_json,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "version '{}' already exists",
                            version_key
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

        row_to_asset_version(&row)
    }

    async fn get_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<AssetVersion, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::version::GET_BY_KEY, &[&asset_id, &version_key])
            .await
            .map_err(internal_err("get_version"))?
            .ok_or_else(|| StoreError::NotFound(format!("version '{}'", version_key)))?;

        row_to_asset_version(&row)
    }

    async fn list_versions(&self, asset_id: Uuid) -> Result<Vec<AssetVersion>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(queries::version::LIST_BY_ASSET, &[&asset_id])
            .await
            .map_err(internal_err("list_versions"))?;

        rows.iter().map(row_to_asset_version).collect()
    }

    async fn get_latest_version(&self, asset_id: Uuid) -> Result<Option<AssetVersion>, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::version::GET_LATEST, &[&asset_id])
            .await
            .map_err(internal_err("get_latest_version"))?;

        row.as_ref().map(row_to_asset_version).transpose()
    }
}

// ── TabularVersionStore ────────────────────────────────────────────────────

#[async_trait]
impl TabularVersionStore for PgCatalogStore {
    async fn create_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        metadata_location: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError> {
        let mut client = self.get_client().await?;
        let props_json = props_to_json(&properties)?;

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let version_row = tx
            .query_one(
                queries::version::CREATE,
                &[
                    &asset_id,
                    &version_key,
                    &version_order,
                    &previous_version_id,
                    &comment,
                    &props_json,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db_err) = e.as_db_error() {
                    if db_err.code() == &SqlState::UNIQUE_VIOLATION {
                        return StoreError::AlreadyExists(format!(
                            "version '{}' already exists",
                            version_key
                        ));
                    }
                    if db_err.code() == &SqlState::CHECK_VIOLATION {
                        return StoreError::Conflict {
                            msg: db_err.message().to_string(),
                        };
                    }
                }
                StoreError::Internal {
                    msg: format!("create_tabular_version failed: {}", &e),
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
        let tabular_version = row_to_tabular_version(&tav_row)?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok((version, tabular_version))
    }

    async fn get_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                r#"
                SELECT av.id, av.asset_id, av.version_key, av.version_order, av.previous_version_id,
                       av.comment, av.properties, av.created_at,
                       tav.version_id, tav.metadata_location,
                       tav.created_at AS tabular_created_at
                FROM asset_versions av
                JOIN tabular_asset_versions tav ON av.id = tav.version_id
                WHERE av.asset_id = $1 AND av.version_key = $2
                "#,
                &[&asset_id, &version_key],
            )
            .await
            .map_err(internal_err("get_tabular_version"))?
            .ok_or_else(|| StoreError::NotFound(format!("version '{}'", version_key)))?;

        let version = row_to_asset_version(&row)?;
        let tabular_version = row_to_tabular_version(&row)?;
        Ok((version, tabular_version))
    }

    async fn list_tabular_versions(
        &self,
        asset_id: Uuid,
    ) -> Result<Vec<(AssetVersion, TabularAssetVersion)>, StoreError> {
        let client = self.get_client().await?;
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
            .map_err(internal_err("list_tabular_versions"))?;

        rows.iter()
            .map(|row| {
                let version = row_to_asset_version(row)?;
                let tabular_version = row_to_tabular_version(row)?;
                Ok((version, tabular_version))
            })
            .collect()
    }

    async fn get_latest_tabular_version(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<(AssetVersion, TabularAssetVersion)>, StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::version::GET_LATEST_TABULAR, &[&asset_id])
            .await
            .map_err(internal_err("get_latest_tabular_version"))?;

        match row {
            Some(r) => {
                let version = row_to_asset_version(&r)?;
                let tabular_version = row_to_tabular_version(&r)?;
                Ok(Some((version, tabular_version)))
            }
            None => Ok(None),
        }
    }
}

// ── CasCommitStore ─────────────────────────────────────────────────────────

#[async_trait]
impl CasCommitStore for PgCatalogStore {
    async fn cas_update_metadata_location(
        &self,
        domain_name: &str,
        namespace_name: &str,
        asset_name: &str,
        format: &str,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
        property_removals: &[String],
        property_updates: &HashMap<String, String>,
    ) -> Result<(), StoreError> {
        let mut client = self.get_client().await?;

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let cas_row = tx
            .query_opt(
                queries::asset::CAS_UPDATE_METADATA_LOCATION,
                &[
                    &new_location,
                    &new_schema_snapshot,
                    &domain_name,
                    &namespace_name,
                    &asset_name,
                    &format,
                    &expected_location,
                ],
            )
            .await
            .map_err(internal_err("cas update"))?;

        let asset_id: Uuid = match cas_row {
            Some(row) => try_get!(row, "asset_id"),
            None => {
                return Err(StoreError::Conflict {
                    msg: format!(
                        "metadata location for '{}' has been modified by another commit (expected '{}')",
                        asset_name, expected_location
                    ),
                });
            }
        };

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
}

// ── UnifiedQueryStore ──────────────────────────────────────────────────────

#[async_trait]
impl UnifiedQueryStore for PgCatalogStore {
    async fn list_assets_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
        name_filter: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, Option<TabularAsset>)>, StoreError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::asset::LIST_UNIFIED,
                &[
                    &domain_name,
                    &namespace_name,
                    &format,
                    &name_filter,
                    &(limit as i64),
                    &offset,
                ],
            )
            .await
            .map_err(internal_err("list_assets_unified"))?;

        rows.iter()
            .map(|row| {
                let asset = row_to_asset(row)?;
                let tabular = row_to_tabular_asset_optional(row)?;
                Ok((asset, tabular))
            })
            .collect()
    }

    async fn get_asset_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(Asset, Option<TabularAsset>), StoreError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::asset::GET_UNIFIED,
                &[&domain_name, &namespace_name, &name],
            )
            .await
            .map_err(internal_err("get_asset_unified"))?
            .ok_or_else(|| StoreError::NotFound(format!("asset '{}'", name)))?;

        let asset = row_to_asset(&row)?;
        let tabular = row_to_tabular_asset_optional(&row)?;
        Ok((asset, tabular))
    }
}
