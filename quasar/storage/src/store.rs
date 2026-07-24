//! PostgreSQL implementation of the `quasar-core` store traits.
//!
//! Layout: pool/bootstrap helpers, row mappers, SQLSTATE -> `CatalogError`
//! classification, small pure helpers, then one section per trait.
//!
//! Concurrency discipline (DESIGN §6.4): READ COMMITTED (PostgreSQL
//! default); every commit path that touches `tabular_assets` or
//! `assets.current_version_key` first locks the `assets` row with
//! `SELECT ... FOR UPDATE`; multi-table transactions lock rows in
//! ascending `asset_id` order.

use async_trait::async_trait;
use deadpool_postgres::{GenericClient, Pool};
use quasar_core::{
    Asset, AssetFilter, AssetPatch, AssetQuery, AssetStore, AssetType, AssetTypeStore,
    AssetVersion, AssetWithTabular, CasCommitStore, CatalogError, CreateAsset, CreateDomain,
    CreateNamespace, CreateVersion, Domain, DomainPatch, DomainStore, Format, IcebergMetricsStore,
    IcebergPurgeStore, IcebergRegisterStore, IcebergStagingStore, IcebergTableCommit,
    IcebergTransactionStore, IcebergViewStore, Namespace, NamespacePatch, NamespaceStore,
    PatchField, RegisterAssetType, RegisterFormat, TabularAsset, TabularStore, TagStore,
    UnifiedQueryStore, VersionStore, View, ViewAsset, ViewIdentifier,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::{migrate, queries};

/// PostgreSQL-backed catalog store implementing every `quasar-core` trait.
pub struct PgCatalogStore {
    pool: Pool,
}

impl PgCatalogStore {
    /// Wrap an existing connection pool.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    async fn get_client(&self) -> Result<deadpool_postgres::Client, CatalogError> {
        self.pool
            .get()
            .await
            .map_err(|e| CatalogError::Transient(format!("connection pool checkout: {}", &e)))
    }

    /// Apply pending schema migrations (DESIGN §8.2). Fails startup on any
    /// migration error.
    pub async fn initialize(&self) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;
        migrate::run(&mut client).await
    }
}

macro_rules! try_get {
    ($row:expr, $col:expr) => {
        $row.try_get($col)
            .map_err(|e| CatalogError::Internal(format!("row column '{}': {}", $col, &e)))?
    };
}

// ── Row mappers ────────────────────────────────────────────────────────────

fn row_to_domain(row: &Row) -> Result<Domain, CatalogError> {
    Ok(Domain {
        id: try_get!(row, "id"),
        name: try_get!(row, "name"),
        comment: try_get!(row, "comment"),
        properties: try_get!(row, "properties"),
        storage_type: try_get!(row, "storage_type"),
        storage_config: try_get!(row, "storage_config"),
        warehouse: try_get!(row, "warehouse"),
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
}

fn row_to_namespace(row: &Row) -> Result<Namespace, CatalogError> {
    Ok(Namespace {
        id: try_get!(row, "id"),
        domain_id: try_get!(row, "domain_id"),
        path: try_get!(row, "path"),
        depth: try_get!(row, "depth"),
        comment: try_get!(row, "comment"),
        properties: try_get!(row, "properties"),
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
}

fn row_to_asset(row: &Row) -> Result<Asset, CatalogError> {
    Ok(Asset {
        id: try_get!(row, "id"),
        namespace_id: try_get!(row, "namespace_id"),
        name: try_get!(row, "name"),
        asset_type: try_get!(row, "asset_type"),
        format: try_get!(row, "format"),
        comment: try_get!(row, "comment"),
        properties: try_get!(row, "properties"),
        current_version_key: try_get!(row, "current_version_key"),
        deleted_at: try_get!(row, "deleted_at"),
        created_at: try_get!(row, "created_at"),
        updated_at: try_get!(row, "updated_at"),
    })
}

fn row_to_asset_version(row: &Row) -> Result<AssetVersion, CatalogError> {
    Ok(AssetVersion {
        id: try_get!(row, "id"),
        asset_id: try_get!(row, "asset_id"),
        version_key: try_get!(row, "version_key"),
        version_properties: try_get!(row, "version_properties"),
        content_inline: try_get!(row, "content_inline"),
        content_pointer: try_get!(row, "content_pointer"),
        previous_version_id: try_get!(row, "previous_version_id"),
        created_at: try_get!(row, "created_at"),
    })
}

fn row_to_tabular_asset(row: &Row) -> Result<TabularAsset, CatalogError> {
    Ok(TabularAsset {
        asset_id: try_get!(row, "asset_id"),
        location: try_get!(row, "location"),
        metadata_location: try_get!(row, "metadata_location"),
        schema_snapshot: try_get!(row, "schema_snapshot"),
    })
}

/// Map the RETURNING columns of `CREATE_VIEW_ASSET`.
fn row_to_view_asset(row: &Row) -> Result<ViewAsset, CatalogError> {
    Ok(ViewAsset {
        asset_id: try_get!(row, "asset_id"),
        view_uuid: try_get!(row, "view_uuid"),
        location: try_get!(row, "location"),
        metadata_location: try_get!(row, "metadata_location"),
    })
}

/// Map a joined `assets + view_assets` row (GET_VIEW aliases the view
/// columns with a `view_` prefix).
fn row_to_view(row: &Row) -> Result<View, CatalogError> {
    let asset = row_to_asset(row)?;
    let view = ViewAsset {
        asset_id: try_get!(row, "view_asset_id"),
        view_uuid: try_get!(row, "view_uuid"),
        location: try_get!(row, "view_location"),
        metadata_location: try_get!(row, "view_metadata_location"),
    };
    Ok(View { asset, view })
}

fn row_to_asset_type(row: &Row) -> Result<AssetType, CatalogError> {
    Ok(AssetType {
        name: try_get!(row, "name"),
        description: try_get!(row, "description"),
        category: try_get!(row, "category"),
        validation_schema: try_get!(row, "validation_schema"),
        extension_strategy: try_get!(row, "extension_strategy"),
        supports_native_protocol: try_get!(row, "supports_native_protocol"),
    })
}

fn row_to_format(row: &Row) -> Result<Format, CatalogError> {
    Ok(Format {
        name: try_get!(row, "name"),
        description: try_get!(row, "description"),
        mime_type: try_get!(row, "mime_type"),
        serialization_hint: try_get!(row, "serialization_hint"),
    })
}

// ── Error classification ───────────────────────────────────────────────────
//
// SQLSTATE -> CatalogError:
//   connection-class / query-canceled / lock / serialization  -> Transient
//   CHECK_VIOLATION (invalid enum-ish values)                 -> Validation
//   UNIQUE_VIOLATION / FK / RESTRICT                          -> decided by
//     the call site (AlreadyExists / Conflict) via the helpers below
//   everything else                                           -> Internal

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgFailureClass {
    Transient,
    Validation,
}

fn classify_sql_state(code: &SqlState) -> Option<PgFailureClass> {
    if code == &SqlState::QUERY_CANCELED || code == &SqlState::LOCK_NOT_AVAILABLE {
        return Some(PgFailureClass::Transient);
    }

    if code == &SqlState::CONNECTION_EXCEPTION
        || code == &SqlState::CONNECTION_DOES_NOT_EXIST
        || code == &SqlState::CONNECTION_FAILURE
        || code == &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION
        || code == &SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION
        || code == &SqlState::TRANSACTION_RESOLUTION_UNKNOWN
        || code == &SqlState::PROTOCOL_VIOLATION
        || code == &SqlState::OPERATOR_INTERVENTION
        || code == &SqlState::ADMIN_SHUTDOWN
        || code == &SqlState::CRASH_SHUTDOWN
        || code == &SqlState::CANNOT_CONNECT_NOW
        || code == &SqlState::DATABASE_DROPPED
    {
        return Some(PgFailureClass::Transient);
    }

    if code == &SqlState::CHECK_VIOLATION {
        return Some(PgFailureClass::Validation);
    }

    None
}

fn classify_db_error(op: &'static str, e: tokio_postgres::Error) -> CatalogError {
    if e.is_closed() {
        return CatalogError::Transient(format!("{}: connection closed", op));
    }

    match e.code().and_then(classify_sql_state) {
        Some(PgFailureClass::Transient) => CatalogError::Transient(format!("{}: {}", op, &e)),
        Some(PgFailureClass::Validation) => CatalogError::Validation(format!("{}: {}", op, &e)),
        None => CatalogError::Internal(format!("{} failed: {}", op, &e)),
    }
}

fn internal_err(op: &'static str) -> impl FnOnce(tokio_postgres::Error) -> CatalogError {
    move |e| classify_db_error(op, e)
}

/// Map UNIQUE_VIOLATION to `AlreadyExists` with a call-site message.
fn already_exists_or(
    op: &'static str,
    msg: String,
) -> impl FnOnce(tokio_postgres::Error) -> CatalogError {
    move |e| {
        if e.as_db_error()
            .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION)
        {
            CatalogError::AlreadyExists(msg)
        } else {
            classify_db_error(op, e)
        }
    }
}

/// Map FK / RESTRICT violations to `Conflict` (used on delete paths where
/// the constraint signals a non-empty parent or a referenced row).
fn conflict_on_fk_or(
    op: &'static str,
    msg: String,
) -> impl FnOnce(tokio_postgres::Error) -> CatalogError {
    move |e| {
        if e.as_db_error().is_some_and(|db| {
            db.code() == &SqlState::FOREIGN_KEY_VIOLATION
                || db.code() == &SqlState::RESTRICT_VIOLATION
        }) {
            CatalogError::Conflict(msg)
        } else {
            classify_db_error(op, e)
        }
    }
}

// ── Pure helpers ───────────────────────────────────────────────────────────

/// Decompose a three-state patch field into the `CASE` action code
/// (0 = NoChange, 1 = Set, 2 = Unset) plus the optional value parameter.
fn patch_action<T>(field: PatchField<T>) -> (i32, Option<T>) {
    match field {
        PatchField::NoChange => (0, None),
        PatchField::Set(value) => (1, Some(value)),
        PatchField::Unset => (2, None),
    }
}

/// Strict prefixes of a namespace path: `"a/b/c"` -> `["a", "a/b"]`.
fn path_prefixes(path: &str) -> Vec<&str> {
    path.match_indices('/').map(|(i, _)| &path[..i]).collect()
}

/// Number of `/`-separated segments.
fn path_depth(path: &str) -> i32 {
    path.bytes().filter(|b| *b == b'/').count() as i32 + 1
}

/// Derive the first mirrored version's `version_key` from a metadata
/// location (Iceberg paths embed the sequence, e.g. `00001-<uuid>.metadata.json`
/// or `v1.metadata.json`). Falls back to the full file stem when no
/// numeric prefix is present.
fn version_key_from_location(location: &str) -> String {
    let file = location.rsplit('/').next().unwrap_or(location);
    let stem = file.strip_suffix(".metadata.json").unwrap_or(file);
    let numeric: String = stem
        .strip_prefix('v')
        .unwrap_or(stem)
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if numeric.is_empty() {
        stem.to_string()
    } else {
        numeric
    }
}

/// `JSON null` -> SQL NULL for optional JSONB columns.
fn non_null_json(value: serde_json::Value) -> Option<serde_json::Value> {
    if value.is_null() {
        None
    } else {
        Some(value)
    }
}

/// Exact-match JSONB object for property filters (`@>` containment).
fn properties_filter_json(
    properties: &std::collections::HashMap<String, String>,
) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = properties
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();
    serde_json::Value::Object(map)
}

/// Resolve `(domain, path)` to a namespace id.
async fn resolve_namespace_id<C>(client: &C, domain: &str, path: &str) -> Result<Uuid, CatalogError>
where
    C: GenericClient + Sync,
{
    client
        .query_opt(queries::namespace::GET_ID_BY_PATH, &[&domain, &path])
        .await
        .map_err(internal_err("resolve namespace"))?
        .map(|row| row.get(0))
        .ok_or_else(|| {
            CatalogError::NotFound(format!("namespace '{}' in domain '{}'", path, domain))
        })
}

/// Shared "create first mirrored version + mark it current" sequence used
/// by the Iceberg create paths (staged commit, register, view create).
/// Returns the derived version key so callers can reflect it on the
/// already-fetched `Asset` row.
async fn insert_first_version<C>(
    client: &C,
    asset_id: Uuid,
    metadata_location: &str,
) -> Result<String, CatalogError>
where
    C: GenericClient + Sync,
{
    let version_key = version_key_from_location(metadata_location);
    let no_json: Option<serde_json::Value> = None;
    let no_uuid: Option<Uuid> = None;
    let pointer = Some(metadata_location.to_string());

    client
        .execute(
            queries::version::CREATE,
            &[
                &asset_id,
                &version_key,
                &no_json,
                &no_json,
                &pointer,
                &no_uuid,
            ],
        )
        .await
        .map_err(already_exists_or(
            "insert first version",
            format!("version '{}' for asset '{}'", version_key, asset_id),
        ))?;

    client
        .execute(
            queries::asset::SET_CURRENT_VERSION_KEY,
            &[&asset_id, &version_key],
        )
        .await
        .map_err(internal_err("set current version key"))?;

    Ok(version_key)
}

/// Remove every version row of an asset, tip-to-root. Required before
/// hard-deleting the assets row: the self-referencing
/// `previous_version_id ... ON DELETE RESTRICT` forbids deleting a
/// referenced version even when the referencing row goes away in the same
/// statement (see `queries::asset::DELETE_VERSION_LEAVES`).
async fn delete_all_versions<C>(client: &C, asset_id: Uuid) -> Result<(), CatalogError>
where
    C: GenericClient + Sync,
{
    loop {
        let deleted = client
            .execute(queries::asset::DELETE_VERSION_LEAVES, &[&asset_id])
            .await
            .map_err(internal_err("delete version leaves"))?;
        if deleted == 0 {
            return Ok(());
        }
    }
}

// ── DomainStore ────────────────────────────────────────────────────────────

#[async_trait]
impl DomainStore for PgCatalogStore {
    async fn create_domain(&self, input: CreateDomain) -> Result<Domain, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                queries::domain::CREATE,
                &[
                    &input.name,
                    &input.comment,
                    &input.properties,
                    &input.storage_type,
                    &input.storage_config,
                    &input.warehouse,
                ],
            )
            .await
            .map_err(already_exists_or(
                "create_domain",
                format!("domain '{}'", input.name),
            ))?;

        row_to_domain(&row)
    }

    async fn get_domain(&self, name: &str) -> Result<Domain, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::domain::GET_BY_NAME, &[&name])
            .await
            .map_err(internal_err("get_domain"))?
            .ok_or_else(|| CatalogError::NotFound(format!("domain '{}'", name)))?;

        row_to_domain(&row)
    }

    async fn list_domains(&self, offset: u64, limit: u64) -> Result<Vec<Domain>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(queries::domain::LIST, &[&(offset as i64), &(limit as i64)])
            .await
            .map_err(internal_err("list_domains"))?;

        rows.iter().map(row_to_domain).collect()
    }

    async fn update_domain(&self, name: &str, patch: DomainPatch) -> Result<Domain, CatalogError> {
        let client = self.get_client().await?;
        let (c_act, c_val) = patch_action(patch.comment);
        let (p_act, p_val) = patch_action(patch.properties);
        let (st_act, st_val) = patch_action(patch.storage_type);
        let (sc_act, sc_val) = patch_action(patch.storage_config);
        let (w_act, w_val) = patch_action(patch.warehouse);

        let row = client
            .query_opt(
                queries::domain::UPDATE,
                &[
                    &name, &c_act, &c_val, &p_act, &p_val, &st_act, &st_val, &sc_act, &sc_val,
                    &w_act, &w_val,
                ],
            )
            .await
            .map_err(internal_err("update_domain"))?
            .ok_or_else(|| CatalogError::NotFound(format!("domain '{}'", name)))?;

        row_to_domain(&row)
    }

    async fn delete_domain(&self, name: &str) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        let deleted = client
            .execute(queries::domain::DELETE, &[&name])
            .await
            .map_err(conflict_on_fk_or(
                "delete_domain",
                format!("domain '{}' is not empty", name),
            ))?;

        if deleted == 0 {
            return Err(CatalogError::NotFound(format!("domain '{}'", name)));
        }
        Ok(())
    }
}

// ── NamespaceStore ─────────────────────────────────────────────────────────

#[async_trait]
impl NamespaceStore for PgCatalogStore {
    async fn create_namespace(
        &self,
        domain: &str,
        path: &str,
        input: CreateNamespace,
    ) -> Result<Namespace, CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let domain_row = tx
            .query_opt(queries::domain::GET_ID_BY_NAME, &[&domain])
            .await
            .map_err(internal_err("resolve domain"))?
            .ok_or_else(|| CatalogError::NotFound(format!("domain '{}'", domain)))?;
        let domain_id: Uuid = domain_row.get(0);

        // FR-N2: implicitly create intermediate nodes.
        for prefix in path_prefixes(path) {
            tx.execute(
                queries::namespace::CREATE_PREFIX,
                &[&domain_id, &prefix, &path_depth(prefix)],
            )
            .await
            .map_err(internal_err("create intermediate namespace"))?;
        }

        let row = tx
            .query_opt(
                queries::namespace::CREATE_FINAL,
                &[
                    &domain_id,
                    &path,
                    &path_depth(path),
                    &input.comment,
                    &input.properties,
                ],
            )
            .await
            .map_err(internal_err("create_namespace"))?
            .ok_or_else(|| {
                CatalogError::AlreadyExists(format!("namespace '{}' in domain '{}'", path, domain))
            })?;

        let namespace = row_to_namespace(&row)?;
        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(namespace)
    }

    async fn get_namespace(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::namespace::GET_BY_PATH, &[&domain, &path])
            .await
            .map_err(internal_err("get_namespace"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("namespace '{}' in domain '{}'", path, domain))
            })?;

        row_to_namespace(&row)
    }

    async fn list_namespaces(
        &self,
        domain: &str,
        prefix: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Namespace>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::namespace::LIST,
                &[&domain, &prefix, &(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_namespaces"))?;

        rows.iter().map(row_to_namespace).collect()
    }

    async fn update_namespace(
        &self,
        domain: &str,
        path: &str,
        patch: NamespacePatch,
    ) -> Result<Namespace, CatalogError> {
        let client = self.get_client().await?;
        let (c_act, c_val) = patch_action(patch.comment);
        let (p_act, p_val) = patch_action(patch.properties);

        let row = client
            .query_opt(
                queries::namespace::UPDATE,
                &[&domain, &path, &c_act, &c_val, &p_act, &p_val],
            )
            .await
            .map_err(internal_err("update_namespace"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("namespace '{}' in domain '{}'", path, domain))
            })?;

        row_to_namespace(&row)
    }

    async fn delete_namespace(&self, domain: &str, path: &str) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let row = tx
            .query_opt(queries::namespace::GET_BY_PATH, &[&domain, &path])
            .await
            .map_err(internal_err("resolve namespace"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("namespace '{}' in domain '{}'", path, domain))
            })?;
        let namespace = row_to_namespace(&row)?;

        // Application-level emptiness checks (FK RESTRICT is the backstop).
        let children: i64 = tx
            .query_one(
                queries::namespace::COUNT_CHILDREN,
                &[&namespace.domain_id, &namespace.path],
            )
            .await
            .map_err(internal_err("count child namespaces"))?
            .get(0);
        if children > 0 {
            return Err(CatalogError::Conflict(format!(
                "namespace '{}' has child namespaces",
                path
            )));
        }

        let assets: i64 = tx
            .query_one(queries::namespace::COUNT_ASSETS, &[&namespace.id])
            .await
            .map_err(internal_err("count namespace assets"))?
            .get(0);
        if assets > 0 {
            return Err(CatalogError::Conflict(format!(
                "namespace '{}' is not empty",
                path
            )));
        }

        tx.execute(queries::namespace::DELETE_BY_ID, &[&namespace.id])
            .await
            .map_err(conflict_on_fk_or(
                "delete_namespace",
                format!("namespace '{}' is not empty", path),
            ))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }

    async fn resolve_path(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError> {
        self.get_namespace(domain, path).await
    }
}

// ── AssetTypeStore ─────────────────────────────────────────────────────────

#[async_trait]
impl AssetTypeStore for PgCatalogStore {
    async fn register_asset_type(
        &self,
        input: RegisterAssetType,
    ) -> Result<AssetType, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                queries::registry::CREATE_ASSET_TYPE,
                &[
                    &input.name,
                    &input.description,
                    &input.category,
                    &input.validation_schema,
                    &input.extension_strategy,
                    &input.supports_native_protocol,
                ],
            )
            .await
            .map_err(already_exists_or(
                "register_asset_type",
                format!("asset type '{}'", input.name),
            ))?;

        row_to_asset_type(&row)
    }

    async fn register_format(&self, input: RegisterFormat) -> Result<Format, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_one(
                queries::registry::CREATE_FORMAT,
                &[
                    &input.name,
                    &input.description,
                    &input.mime_type,
                    &input.serialization_hint,
                ],
            )
            .await
            .map_err(already_exists_or(
                "register_format",
                format!("format '{}'", input.name),
            ))?;

        row_to_format(&row)
    }

    async fn list_asset_types(
        &self,
        category: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<AssetType>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::registry::LIST_ASSET_TYPES,
                &[&category, &(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_asset_types"))?;

        rows.iter().map(row_to_asset_type).collect()
    }

    async fn get_asset_type(&self, name: &str) -> Result<AssetType, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::registry::GET_ASSET_TYPE, &[&name])
            .await
            .map_err(internal_err("get_asset_type"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset type '{}'", name)))?;

        row_to_asset_type(&row)
    }

    async fn get_format(&self, name: &str) -> Result<Format, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::registry::GET_FORMAT, &[&name])
            .await
            .map_err(internal_err("get_format"))?
            .ok_or_else(|| CatalogError::NotFound(format!("format '{}'", name)))?;

        row_to_format(&row)
    }

    async fn list_formats(&self, offset: u64, limit: u64) -> Result<Vec<Format>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::registry::LIST_FORMATS,
                &[&(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_formats"))?;

        rows.iter().map(row_to_format).collect()
    }
}

// ── AssetStore ─────────────────────────────────────────────────────────────

#[async_trait]
impl AssetStore for PgCatalogStore {
    async fn create_asset(&self, input: CreateAsset) -> Result<Asset, CatalogError> {
        let client = self.get_client().await?;
        let namespace_id = resolve_namespace_id(&client, &input.domain, &input.namespace).await?;

        let row = client
            .query_one(
                queries::asset::CREATE,
                &[
                    &namespace_id,
                    &input.name,
                    &input.asset_type,
                    &input.format,
                    &input.comment,
                    &input.properties,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db) = e.as_db_error() {
                    if db.code() == &SqlState::UNIQUE_VIOLATION {
                        return CatalogError::AlreadyExists(format!(
                            "asset '{}' in namespace '{}'",
                            input.name, input.namespace
                        ));
                    }
                    if db.code() == &SqlState::FOREIGN_KEY_VIOLATION {
                        return CatalogError::Validation(format!(
                            "unregistered asset type '{}' or format {:?}",
                            input.asset_type, input.format
                        ));
                    }
                }
                classify_db_error("create_asset", e)
            })?;

        row_to_asset(&row)
    }

    async fn get_asset(&self, id: Uuid) -> Result<Asset, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::asset::GET_BY_ID, &[&id])
            .await
            .map_err(internal_err("get_asset"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", id)))?;

        row_to_asset(&row)
    }

    async fn get_asset_by_name(
        &self,
        domain: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Asset, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::asset::GET_BY_NAME, &[&domain, &namespace, &name])
            .await
            .map_err(internal_err("get_asset_by_name"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "asset '{}' in namespace '{}' of domain '{}'",
                    name, namespace, domain
                ))
            })?;

        row_to_asset(&row)
    }

    async fn list_assets(
        &self,
        filter: AssetFilter,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Asset>, CatalogError> {
        let client = self.get_client().await?;
        let tags: Option<Vec<String>> = if filter.tags.is_empty() {
            None
        } else {
            Some(filter.tags)
        };
        let properties = properties_filter_json(&filter.properties);

        let rows = client
            .query(
                queries::asset::LIST,
                &[
                    &filter.domain,
                    &filter.namespace,
                    &filter.asset_type,
                    &filter.format,
                    &filter.include_deleted,
                    &tags,
                    &properties,
                    &(offset as i64),
                    &(limit as i64),
                ],
            )
            .await
            .map_err(internal_err("list_assets"))?;

        rows.iter().map(row_to_asset).collect()
    }

    async fn update_asset(&self, id: Uuid, patch: AssetPatch) -> Result<Asset, CatalogError> {
        let client = self.get_client().await?;
        let (c_act, c_val) = patch_action(patch.comment);
        let (p_act, p_val) = patch_action(patch.properties);

        let row = client
            .query_opt(
                queries::asset::UPDATE,
                &[&id, &c_act, &c_val, &p_act, &p_val],
            )
            .await
            .map_err(internal_err("update_asset"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", id)))?;

        row_to_asset(&row)
    }

    async fn rename_asset(
        &self,
        id: Uuid,
        new_name: &str,
        new_namespace_path: Option<&str>,
    ) -> Result<Asset, CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // Lock the asset row so a concurrent rename/move/restore cannot
        // interleave with the destination resolution and the update.
        let row = tx
            .query_opt(queries::asset::LOCK_BY_ID, &[&id])
            .await
            .map_err(internal_err("lock asset for rename"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", id)))?;
        let asset = row_to_asset(&row)?;

        if asset.deleted_at.is_some() {
            return Err(CatalogError::NotFound(format!("asset '{}'", id)));
        }

        // None renames in place; Some(path) moves the asset to the
        // namespace at `path` within the asset's own Domain (the SQL join
        // derives the Domain from the asset, so cross-Domain moves are not
        // expressible).
        let target_namespace_id = match new_namespace_path {
            None => asset.namespace_id,
            Some(path) => tx
                .query_opt(
                    queries::asset::RESOLVE_RENAME_TARGET_NAMESPACE,
                    &[&id, &path],
                )
                .await
                .map_err(internal_err("resolve rename target namespace"))?
                .map(|row| row.get(0))
                .ok_or_else(|| {
                    CatalogError::NotFound(format!("namespace '{}' in the asset's domain", path))
                })?,
        };

        // A conflicting active asset name in the destination namespace is
        // caught by the uq_assets_active_name unique index.
        let renamed_row = tx
            .query_opt(
                queries::asset::RENAME,
                &[&id, &new_name, &target_namespace_id],
            )
            .await
            .map_err(already_exists_or(
                "rename_asset",
                format!("asset named '{}'", new_name),
            ))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", id)))?;
        let renamed = row_to_asset(&renamed_row)?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(renamed)
    }

    async fn soft_delete_asset(&self, id: Uuid) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        let updated = client
            .execute(queries::asset::SOFT_DELETE, &[&id])
            .await
            .map_err(internal_err("soft_delete_asset"))?;

        if updated == 0 {
            return Err(CatalogError::NotFound(format!("asset '{}'", id)));
        }
        Ok(())
    }

    async fn restore_asset(&self, id: Uuid) -> Result<Asset, CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // Lock the row before validating so a concurrent restore/create
        // cannot interleave with the name-conflict check.
        let row = tx
            .query_opt(queries::asset::LOCK_BY_ID, &[&id])
            .await
            .map_err(internal_err("lock asset for restore"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", id)))?;
        let asset = row_to_asset(&row)?;

        if asset.deleted_at.is_none() {
            return Err(CatalogError::Conflict(format!(
                "asset '{}' is not soft-deleted",
                id
            )));
        }

        let name_taken: bool = tx
            .query_one(
                queries::asset::EXISTS_ACTIVE_NAME,
                &[&asset.namespace_id, &asset.name],
            )
            .await
            .map_err(internal_err("check restore name conflict"))?
            .get(0);
        if name_taken {
            return Err(CatalogError::Conflict(format!(
                "an active asset named '{}' already exists in the namespace",
                asset.name
            )));
        }

        let restored_row = tx
            .query_one(queries::asset::RESTORE, &[&id])
            .await
            .map_err(internal_err("restore_asset"))?;
        let restored = row_to_asset(&restored_row)?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(restored)
    }

    async fn hard_delete_asset(&self, id: Uuid) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // Remove the version rows first (tip-to-root): the
        // self-referencing RESTRICT chain would otherwise reject the
        // cascaded delete.
        delete_all_versions(&tx, id).await?;

        let deleted = tx
            .execute(queries::asset::HARD_DELETE, &[&id])
            .await
            .map_err(internal_err("hard_delete_asset"))?;

        if deleted == 0 {
            return Err(CatalogError::NotFound(format!("asset '{}'", id)));
        }

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }
}

// ── TabularStore ───────────────────────────────────────────────────────────

#[async_trait]
impl TabularStore for PgCatalogStore {
    async fn create_tabular_asset(
        &self,
        input: CreateAsset,
        location: &str,
        metadata_location: Option<&str>,
    ) -> Result<AssetWithTabular, CatalogError> {
        if input.asset_type != "table" {
            return Err(CatalogError::Validation(format!(
                "tabular extension requires asset_type 'table', got '{}'",
                input.asset_type
            )));
        }

        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let namespace_id = resolve_namespace_id(&tx, &input.domain, &input.namespace).await?;
        let asset_row = tx
            .query_one(
                queries::asset::CREATE,
                &[
                    &namespace_id,
                    &input.name,
                    &input.asset_type,
                    &input.format,
                    &input.comment,
                    &input.properties,
                ],
            )
            .await
            .map_err(|e| {
                if let Some(db) = e.as_db_error() {
                    if db.code() == &SqlState::UNIQUE_VIOLATION {
                        return CatalogError::AlreadyExists(format!(
                            "asset '{}' in namespace '{}'",
                            input.name, input.namespace
                        ));
                    }
                    if db.code() == &SqlState::FOREIGN_KEY_VIOLATION {
                        return CatalogError::Validation(format!(
                            "unregistered asset type '{}' or format {:?}",
                            input.asset_type, input.format
                        ));
                    }
                }
                classify_db_error("create_tabular_asset", e)
            })?;
        let asset = row_to_asset(&asset_row)?;

        let no_schema: Option<serde_json::Value> = None;
        let tabular_row = tx
            .query_one(
                queries::asset::CREATE_TABULAR,
                &[&asset.id, &location, &metadata_location, &no_schema],
            )
            .await
            .map_err(internal_err("create_tabular_asset create extension"))?;
        let tabular = row_to_tabular_asset(&tabular_row)?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(AssetWithTabular { asset, tabular })
    }

    async fn get_tabular_asset(&self, asset_id: Uuid) -> Result<TabularAsset, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::asset::GET_TABULAR, &[&asset_id])
            .await
            .map_err(internal_err("get_tabular_asset"))?
            .ok_or_else(|| CatalogError::NotFound(format!("tabular asset '{}'", asset_id)))?;

        row_to_tabular_asset(&row)
    }
}

// ── VersionStore ───────────────────────────────────────────────────────────

#[async_trait]
impl VersionStore for PgCatalogStore {
    async fn create_version(&self, input: CreateVersion) -> Result<AssetVersion, CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let asset_active: bool = tx
            .query_one(queries::version::ASSET_ACTIVE_EXISTS, &[&input.asset_id])
            .await
            .map_err(internal_err("check asset exists"))?
            .get(0);
        if !asset_active {
            return Err(CatalogError::NotFound(format!(
                "asset '{}'",
                input.asset_id
            )));
        }

        // Version-chain integrity is validated at the application layer:
        // the predecessor must belong to the same asset.
        if let Some(previous) = input.previous_version_id {
            let belongs: bool = tx
                .query_one(
                    queries::version::PREV_BELONGS_TO_ASSET,
                    &[&previous, &input.asset_id],
                )
                .await
                .map_err(internal_err("check previous version"))?
                .get(0);
            if !belongs {
                return Err(CatalogError::Validation(format!(
                    "previous version '{}' does not belong to asset '{}'",
                    previous, input.asset_id
                )));
            }
        }

        let row = tx
            .query_one(
                queries::version::CREATE,
                &[
                    &input.asset_id,
                    &input.version_key,
                    &input.version_properties,
                    &input.content_inline,
                    &input.content_pointer,
                    &input.previous_version_id,
                ],
            )
            .await
            .map_err(already_exists_or(
                "create_version",
                format!(
                    "version '{}' for asset '{}'",
                    input.version_key, input.asset_id
                ),
            ))?;

        tx.execute(
            queries::asset::SET_CURRENT_VERSION_KEY,
            &[&input.asset_id, &input.version_key],
        )
        .await
        .map_err(internal_err("set current version key"))?;

        let version = row_to_asset_version(&row)?;
        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(version)
    }

    async fn get_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<AssetVersion, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::version::GET_BY_KEY, &[&asset_id, &version_key])
            .await
            .map_err(internal_err("get_version"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "version '{}' for asset '{}'",
                    version_key, asset_id
                ))
            })?;

        row_to_asset_version(&row)
    }

    async fn list_versions(
        &self,
        asset_id: Uuid,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<AssetVersion>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::version::LIST,
                &[&asset_id, &(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_versions"))?;

        rows.iter().map(row_to_asset_version).collect()
    }

    async fn get_latest_version(&self, asset_id: Uuid) -> Result<AssetVersion, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(queries::version::GET_LATEST, &[&asset_id])
            .await
            .map_err(internal_err("get_latest_version"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("current version for asset '{}'", asset_id))
            })?;

        row_to_asset_version(&row)
    }
}

// ── TagStore ───────────────────────────────────────────────────────────────

#[async_trait]
impl TagStore for PgCatalogStore {
    async fn add_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError> {
        let client = self.get_client().await?;

        let asset_active: bool = client
            .query_one(queries::version::ASSET_ACTIVE_EXISTS, &[&asset_id])
            .await
            .map_err(internal_err("check asset exists"))?
            .get(0);
        if !asset_active {
            return Err(CatalogError::NotFound(format!("asset '{}'", asset_id)));
        }

        client
            .execute(queries::tag::ADD, &[&asset_id, &tag])
            .await
            .map_err(already_exists_or(
                "add_tag",
                format!("tag '{}' on asset '{}'", tag, asset_id),
            ))?;

        Ok(())
    }

    async fn remove_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        // Idempotent: removing an absent tag is not an error.
        client
            .execute(queries::tag::REMOVE, &[&asset_id, &tag])
            .await
            .map_err(internal_err("remove_tag"))?;

        Ok(())
    }

    async fn list_tags(&self, asset_id: Uuid) -> Result<Vec<String>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(queries::tag::LIST, &[&asset_id])
            .await
            .map_err(internal_err("list_tags"))?;

        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    async fn list_assets_by_tag(
        &self,
        domain: &str,
        tag: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Asset>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::tag::LIST_ASSETS_BY_TAG,
                &[&domain, &tag, &(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_assets_by_tag"))?;

        rows.iter().map(row_to_asset).collect()
    }
}

// ── UnifiedQueryStore ──────────────────────────────────────────────────────

#[async_trait]
impl UnifiedQueryStore for PgCatalogStore {
    async fn query_assets(&self, query: AssetQuery) -> Result<Vec<Asset>, CatalogError> {
        let client = self.get_client().await?;
        let tags: Option<Vec<String>> = if query.tags.is_empty() {
            None
        } else {
            Some(query.tags)
        };
        let properties = properties_filter_json(&query.properties);

        let rows = client
            .query(
                queries::unified::QUERY_ASSETS,
                &[
                    &query.domain,
                    &query.namespace_prefix,
                    &query.asset_type,
                    &query.format,
                    &query.include_deleted,
                    &tags,
                    &properties,
                    &(query.offset as i64),
                    &(query.limit as i64),
                ],
            )
            .await
            .map_err(internal_err("query_assets"))?;

        rows.iter().map(row_to_asset).collect()
    }
}

// ── CasCommitStore ─────────────────────────────────────────────────────────

#[async_trait]
impl CasCommitStore for PgCatalogStore {
    async fn compare_and_swap_pointer(
        &self,
        asset_id: Uuid,
        expected_pointer: &str,
        new_version: CreateVersion,
    ) -> Result<AssetVersion, CatalogError> {
        if new_version.asset_id != asset_id {
            return Err(CatalogError::Validation(format!(
                "new_version.asset_id '{}' does not match asset_id '{}'",
                new_version.asset_id, asset_id
            )));
        }

        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // S1: lock the asset row (the linearization point for all commit
        // paths of this asset) and capture the current version key.
        let s1 = tx
            .query_opt(queries::cas::LOCK_ASSET, &[&asset_id])
            .await
            .map_err(internal_err("cas lock asset"))?
            .ok_or_else(|| CatalogError::NotFound(format!("asset '{}'", asset_id)))?;
        let current_version_key: Option<String> = s1.get("current_version_key");

        // S2: read the current pointer from the tabular hot-path cache.
        let s2 = tx
            .query_opt(queries::cas::READ_TABULAR_POINTER, &[&asset_id])
            .await
            .map_err(internal_err("cas read pointer"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!("tabular extension for asset '{}'", asset_id))
            })?;
        let current_pointer: Option<String> = s2.get(0);

        // S3: application-level pointer comparison.
        if current_pointer.as_deref() != Some(expected_pointer) {
            return Err(CatalogError::Conflict(format!(
                "CAS pointer mismatch for asset '{}': expected '{}', current is {:?}",
                asset_id, expected_pointer, current_pointer
            )));
        }

        // S4: insert the mirrored version, auto-linking the previous
        // version via the current version key from S1.
        let row = tx
            .query_one(
                queries::cas::INSERT_MIRRORED_VERSION,
                &[
                    &asset_id,
                    &new_version.version_key,
                    &new_version.version_properties,
                    &new_version.content_inline,
                    &new_version.content_pointer,
                    &current_version_key,
                ],
            )
            .await
            .map_err(already_exists_or(
                "cas insert version",
                format!(
                    "version '{}' for asset '{}'",
                    new_version.version_key, asset_id
                ),
            ))?;

        // S5: update the current version key (result, not anchor).
        tx.execute(
            queries::cas::UPDATE_CURRENT_VERSION_KEY,
            &[&asset_id, &new_version.version_key],
        )
        .await
        .map_err(internal_err("cas update current version key"))?;

        // S6: update the tabular hot-path cache. schema_snapshot stays as
        // is: CreateVersion carries no schema to cache.
        tx.execute(
            queries::cas::UPDATE_TABULAR_POINTER,
            &[&asset_id, &new_version.content_pointer],
        )
        .await
        .map_err(internal_err("cas update tabular pointer"))?;

        let version = row_to_asset_version(&row)?;
        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(version)
    }
}

// ── IcebergStagingStore ────────────────────────────────────────────────────

#[async_trait]
impl IcebergStagingStore for PgCatalogStore {
    async fn create_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        table_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: serde_json::Value,
    ) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // Expired staged records of the same name are reclaimed first.
        tx.execute(
            queries::iceberg::DELETE_EXPIRED_STAGED,
            &[&domain, &namespace_path, &table_name],
        )
        .await
        .map_err(internal_err("delete expired staged"))?;

        let active_exists: bool = tx
            .query_one(
                queries::iceberg::ACTIVE_ASSET_EXISTS,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("check active table"))?
            .get(0);
        if active_exists {
            return Err(CatalogError::AlreadyExists(format!(
                "table '{}' in namespace '{}'",
                table_name, namespace_path
            )));
        }

        tx.execute(
            queries::iceberg::CREATE_STAGED,
            &[
                &domain,
                &namespace_path,
                &table_name,
                &table_uuid,
                &location,
                &metadata_location,
                &metadata_json,
                &properties,
            ],
        )
        .await
        .map_err(already_exists_or(
            "create_staged_table",
            format!(
                "staged table '{}' in namespace '{}'",
                table_name, namespace_path
            ),
        ))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }

    async fn get_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::iceberg::GET_STAGED,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("get_staged_table"))?;

        Ok(row.and_then(|r| r.get::<usize, Option<serde_json::Value>>(0)))
    }

    async fn delete_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        client
            .execute(
                queries::iceberg::DELETE_STAGED,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("delete_staged_table"))?;

        Ok(())
    }

    async fn commit_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: serde_json::Value,
    ) -> Result<AssetWithTabular, CatalogError> {
        // metadata_json stays in the staged record / object store; the
        // catalog persists pointers, not the native metadata document.
        let _ = &metadata_json;

        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // 1. The active table must not exist.
        let active_exists: bool = tx
            .query_one(
                queries::iceberg::ACTIVE_ASSET_EXISTS,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("check active table"))?
            .get(0);
        if active_exists {
            return Err(CatalogError::AlreadyExists(format!(
                "table '{}' in namespace '{}'",
                table_name, namespace_path
            )));
        }

        // 2. Atomically take the staged record (must be non-expired).
        let staged = tx
            .query_opt(
                queries::iceberg::DELETE_STAGED_FOR_COMMIT,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("take staged record"))?;
        if staged.is_none() {
            return Err(CatalogError::NotFound(format!(
                "staged table '{}' in namespace '{}' (expired or missing)",
                table_name, namespace_path
            )));
        }

        // 3. Create identity + extension + first mirrored version.
        let namespace_id = resolve_namespace_id(&tx, domain, namespace_path).await?;
        let format = Some("iceberg".to_string());
        let comment: Option<String> = None;
        let props = non_null_json(properties);

        let asset_row = tx
            .query_one(
                queries::asset::CREATE,
                &[
                    &namespace_id,
                    &table_name,
                    &"table",
                    &format,
                    &comment,
                    &props,
                ],
            )
            .await
            .map_err(already_exists_or(
                "commit_staged_table create asset",
                format!("table '{}' in namespace '{}'", table_name, namespace_path),
            ))?;
        let mut asset = row_to_asset(&asset_row)?;

        let no_schema: Option<serde_json::Value> = None;
        let tabular_row = tx
            .query_one(
                queries::iceberg::CREATE_TABULAR,
                &[&asset.id, &location, &metadata_location, &no_schema],
            )
            .await
            .map_err(internal_err("commit_staged_table create tabular"))?;
        let tabular = row_to_tabular_asset(&tabular_row)?;

        asset.current_version_key =
            Some(insert_first_version(&tx, asset.id, metadata_location).await?);

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(AssetWithTabular { asset, tabular })
    }
}

// ── IcebergRegisterStore ───────────────────────────────────────────────────

#[async_trait]
impl IcebergRegisterStore for PgCatalogStore {
    async fn register_iceberg_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: serde_json::Value,
    ) -> Result<AssetWithTabular, CatalogError> {
        // See commit_staged_table: the catalog does not persist the
        // native metadata document itself.
        let _ = &metadata_json;

        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let namespace_id = resolve_namespace_id(&tx, domain, namespace_path).await?;
        let format = Some("iceberg".to_string());
        let comment: Option<String> = None;
        let props = non_null_json(properties);

        let asset_row = tx
            .query_one(
                queries::asset::CREATE,
                &[
                    &namespace_id,
                    &table_name,
                    &"table",
                    &format,
                    &comment,
                    &props,
                ],
            )
            .await
            .map_err(already_exists_or(
                "register_iceberg_table create asset",
                format!("table '{}' in namespace '{}'", table_name, namespace_path),
            ))?;
        let mut asset = row_to_asset(&asset_row)?;

        let no_schema: Option<serde_json::Value> = None;
        let tabular_row = tx
            .query_one(
                queries::iceberg::CREATE_TABULAR,
                &[&asset.id, &location, &metadata_location, &no_schema],
            )
            .await
            .map_err(internal_err("register_iceberg_table create tabular"))?;
        let tabular = row_to_tabular_asset(&tabular_row)?;

        asset.current_version_key =
            Some(insert_first_version(&tx, asset.id, metadata_location).await?);

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(AssetWithTabular { asset, tabular })
    }
}

// ── IcebergMetricsStore ────────────────────────────────────────────────────

#[async_trait]
impl IcebergMetricsStore for PgCatalogStore {
    async fn record_scan_metrics_report(
        &self,
        asset_id: Option<Uuid>,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        report: serde_json::Value,
        user_agent: Option<&str>,
    ) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        client
            .execute(
                queries::iceberg::CREATE_METRICS_REPORT,
                &[
                    &asset_id,
                    &domain,
                    &namespace_path,
                    &table_name,
                    &report,
                    &user_agent,
                ],
            )
            .await
            .map_err(internal_err("record_scan_metrics_report"))?;

        Ok(())
    }
}

// ── IcebergPurgeStore ──────────────────────────────────────────────────────

#[async_trait]
impl IcebergPurgeStore for PgCatalogStore {
    async fn begin_iceberg_purge_and_drop_catalog(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<(Uuid, String, Option<String>), CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // Read + lock the table row and its tabular extension.
        let row = tx
            .query_opt(
                queries::iceberg::LOCK_TABLE_FOR_PURGE,
                &[&domain, &namespace_path, &table_name],
            )
            .await
            .map_err(internal_err("lock table for purge"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "table '{}' in namespace '{}'",
                    table_name, namespace_path
                ))
            })?;
        let asset_id: Uuid = row.get("id");
        let location: String = row.get("location");
        let metadata_location: Option<String> = row.get("metadata_location");

        // Record the purge operation before dropping the catalog rows so
        // object-store cleanup can be tracked / compensated.
        let op_row = tx
            .query_one(
                queries::iceberg::CREATE_PURGE_OPERATION,
                &[
                    &domain,
                    &namespace_path,
                    &table_name,
                    &location,
                    &metadata_location,
                ],
            )
            .await
            .map_err(internal_err("create purge operation"))?;
        let operation_id: Uuid = op_row.get("id");

        // Drop the catalog records (extension rows cascade; versions are
        // removed explicitly tip-to-root because of the RESTRICT chain).
        delete_all_versions(&tx, asset_id).await?;
        tx.execute(queries::asset::HARD_DELETE, &[&asset_id])
            .await
            .map_err(internal_err("drop purged table"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok((operation_id, location, metadata_location))
    }

    async fn update_purge_operation(
        &self,
        operation_id: Uuid,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<(), CatalogError> {
        let client = self.get_client().await?;
        let updated = client
            .execute(
                queries::iceberg::UPDATE_PURGE_OPERATION,
                &[&operation_id, &status, &error_message],
            )
            .await
            .map_err(internal_err("update_purge_operation"))?;

        if updated == 0 {
            return Err(CatalogError::NotFound(format!(
                "purge operation '{}'",
                operation_id
            )));
        }
        Ok(())
    }
}

// ── IcebergTransactionStore ────────────────────────────────────────────────

#[async_trait]
impl IcebergTransactionStore for PgCatalogStore {
    async fn commit_transaction_tables(
        &self,
        commits: Vec<IcebergTableCommit>,
    ) -> Result<(), CatalogError> {
        if commits.is_empty() {
            return Ok(());
        }

        let mut client = self.get_client().await?;

        // Resolve every table to its asset id first, then lock in
        // ascending asset_id order (DESIGN §6.4 deadlock avoidance).
        let mut resolved: Vec<(Uuid, &IcebergTableCommit)> = Vec::with_capacity(commits.len());
        for commit in &commits {
            let asset_id: Uuid = client
                .query_opt(
                    queries::iceberg::RESOLVE_TABLE_ID,
                    &[&commit.domain, &commit.namespace_path, &commit.table],
                )
                .await
                .map_err(internal_err("resolve transaction table"))?
                .map(|row| row.get(0))
                .ok_or_else(|| {
                    CatalogError::NotFound(format!(
                        "table '{}' in namespace '{}'",
                        commit.table, commit.namespace_path
                    ))
                })?;
            resolved.push((asset_id, commit));
        }
        resolved.sort_by_key(|(asset_id, _)| *asset_id);

        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        for (asset_id, commit) in &resolved {
            // S1: lock the asset row and capture the current version key.
            let locked = tx
                .query_opt(queries::cas::LOCK_ASSET, &[asset_id])
                .await
                .map_err(internal_err("transaction lock asset"))?
                .ok_or_else(|| CatalogError::NotFound(format!("table '{}'", commit.table)))?;
            let current_version_key: Option<String> = locked.get("current_version_key");

            // S2: read the current pointer.
            let pointer_row = tx
                .query_opt(queries::cas::READ_TABULAR_POINTER, &[asset_id])
                .await
                .map_err(internal_err("transaction read pointer"))?
                .ok_or_else(|| {
                    CatalogError::NotFound(format!("tabular extension for '{}'", commit.table))
                })?;
            let current_pointer: Option<String> = pointer_row.get(0);

            // S3: compare; any mismatch rolls back the whole transaction.
            if current_pointer.as_deref() != Some(commit.expected_pointer.as_str()) {
                return Err(CatalogError::Conflict(format!(
                    "CAS pointer mismatch for table '{}' in namespace '{}': expected '{}'",
                    commit.table, commit.namespace_path, commit.expected_pointer
                )));
            }

            // S4: insert the mirrored version, auto-linking the previous
            // version via the current version key from S1.
            let no_json: Option<serde_json::Value> = None;
            let new_pointer = Some(commit.new_location.clone());
            tx.execute(
                queries::cas::INSERT_MIRRORED_VERSION,
                &[
                    asset_id,
                    &commit.new_version_key,
                    &no_json,
                    &no_json,
                    &new_pointer,
                    &current_version_key,
                ],
            )
            .await
            .map_err(already_exists_or(
                "transaction insert version",
                format!(
                    "version '{}' for table '{}'",
                    commit.new_version_key, commit.table
                ),
            ))?;

            // S5: update the current version key (result, not anchor).
            tx.execute(
                queries::cas::UPDATE_CURRENT_VERSION_KEY,
                &[asset_id, &commit.new_version_key],
            )
            .await
            .map_err(internal_err("transaction update current version key"))?;

            // S6: pointer + schema cache update.
            tx.execute(
                queries::iceberg::TX_UPDATE_TABULAR,
                &[asset_id, &commit.new_location, &commit.schema_snapshot],
            )
            .await
            .map_err(internal_err("transaction update pointer"))?;
        }

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }
}

// ── IcebergViewStore ───────────────────────────────────────────────────────

#[async_trait]
impl IcebergViewStore for PgCatalogStore {
    async fn create_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
        view_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        properties: serde_json::Value,
    ) -> Result<View, CatalogError> {
        let mut client = self.get_client().await?;
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        let namespace_id = resolve_namespace_id(&tx, domain, namespace_path).await?;
        let format = Some("iceberg".to_string());
        let comment: Option<String> = None;
        let props = non_null_json(properties);

        let asset_row = tx
            .query_one(
                queries::asset::CREATE,
                &[
                    &namespace_id,
                    &view_name,
                    &"view",
                    &format,
                    &comment,
                    &props,
                ],
            )
            .await
            .map_err(already_exists_or(
                "create_view create asset",
                format!("view '{}' in namespace '{}'", view_name, namespace_path),
            ))?;
        let mut asset = row_to_asset(&asset_row)?;

        let view_row = tx
            .query_one(
                queries::iceberg::CREATE_VIEW_ASSET,
                &[&asset.id, &view_uuid, &location, &metadata_location],
            )
            .await
            .map_err(internal_err("create_view create view asset"))?;
        let view_asset = row_to_view_asset(&view_row)?;

        asset.current_version_key =
            Some(insert_first_version(&tx, asset.id, metadata_location).await?);

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))?;

        Ok(View {
            asset,
            view: view_asset,
        })
    }

    async fn get_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<View, CatalogError> {
        let client = self.get_client().await?;
        let row = client
            .query_opt(
                queries::iceberg::GET_VIEW,
                &[&domain, &namespace_path, &view_name],
            )
            .await
            .map_err(internal_err("get_view"))?
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "view '{}' in namespace '{}'",
                    view_name, namespace_path
                ))
            })?;

        row_to_view(&row)
    }

    async fn commit_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
        expected_pointer: &str,
        new_location: &str,
        new_version_key: &str,
    ) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;

        let view_id: Uuid = client
            .query_opt(
                queries::iceberg::GET_VIEW_ID,
                &[&domain, &namespace_path, &view_name],
            )
            .await
            .map_err(internal_err("resolve view"))?
            .map(|row| row.get(0))
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "view '{}' in namespace '{}'",
                    view_name, namespace_path
                ))
            })?;

        // Same CAS discipline as compare_and_swap_pointer, anchored on
        // view_assets.metadata_location.
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        // S1: lock the asset row and capture the current version key.
        let locked = tx
            .query_opt(queries::cas::LOCK_ASSET, &[&view_id])
            .await
            .map_err(internal_err("commit_view lock asset"))?
            .ok_or_else(|| CatalogError::NotFound(format!("view '{}'", view_name)))?;
        let current_version_key: Option<String> = locked.get("current_version_key");

        // S2: read the current pointer.
        let pointer_row = tx
            .query_opt(queries::iceberg::READ_VIEW_POINTER, &[&view_id])
            .await
            .map_err(internal_err("commit_view read pointer"))?
            .ok_or_else(|| CatalogError::NotFound(format!("view extension for '{}'", view_name)))?;
        let current_pointer: Option<String> = pointer_row.get(0);

        // S3: application-level pointer comparison.
        if current_pointer.as_deref() != Some(expected_pointer) {
            return Err(CatalogError::Conflict(format!(
                "CAS pointer mismatch for view '{}' in namespace '{}': expected '{}'",
                view_name, namespace_path, expected_pointer
            )));
        }

        // S4: insert the mirrored version, auto-linking the previous
        // version via the current version key from S1.
        let no_json: Option<serde_json::Value> = None;
        let new_pointer = Some(new_location.to_string());
        tx.execute(
            queries::cas::INSERT_MIRRORED_VERSION,
            &[
                &view_id,
                &new_version_key,
                &no_json,
                &no_json,
                &new_pointer,
                &current_version_key,
            ],
        )
        .await
        .map_err(already_exists_or(
            "commit_view insert version",
            format!("version '{}' for view '{}'", new_version_key, view_name),
        ))?;

        // S5: update the current version key (result, not anchor).
        tx.execute(
            queries::cas::UPDATE_CURRENT_VERSION_KEY,
            &[&view_id, &new_version_key],
        )
        .await
        .map_err(internal_err("commit_view update current version key"))?;

        // S6: update the view pointer cache.
        tx.execute(
            queries::iceberg::UPDATE_VIEW_POINTER,
            &[&view_id, &new_location],
        )
        .await
        .map_err(internal_err("commit_view update pointer"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }

    async fn drop_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<(), CatalogError> {
        let mut client = self.get_client().await?;

        let view_id: Uuid = client
            .query_opt(
                queries::iceberg::GET_VIEW_ID,
                &[&domain, &namespace_path, &view_name],
            )
            .await
            .map_err(internal_err("resolve view"))?
            .map(|row| row.get(0))
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "view '{}' in namespace '{}'",
                    view_name, namespace_path
                ))
            })?;

        // FK cascade removes the view_assets row; versions are removed
        // explicitly tip-to-root because of the RESTRICT chain.
        let tx = client
            .transaction()
            .await
            .map_err(internal_err("transaction start"))?;

        delete_all_versions(&tx, view_id).await?;
        tx.execute(queries::asset::HARD_DELETE, &[&view_id])
            .await
            .map_err(internal_err("drop_view"))?;

        tx.commit()
            .await
            .map_err(internal_err("transaction commit"))
    }

    async fn list_views(
        &self,
        domain: &str,
        namespace_path: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ViewIdentifier>, CatalogError> {
        let client = self.get_client().await?;
        let rows = client
            .query(
                queries::iceberg::LIST_VIEWS,
                &[&domain, &namespace_path, &(offset as i64), &(limit as i64)],
            )
            .await
            .map_err(internal_err("list_views"))?;

        Ok(rows
            .iter()
            .map(|row| ViewIdentifier {
                namespace_path: row.get(0),
                name: row.get(1),
            })
            .collect())
    }

    async fn rename_view(
        &self,
        source_domain: &str,
        source_namespace_path: &str,
        source_name: &str,
        dest_domain: &str,
        dest_namespace_path: &str,
        dest_name: &str,
    ) -> Result<(), CatalogError> {
        let client = self.get_client().await?;

        let view_id: Uuid = client
            .query_opt(
                queries::iceberg::GET_VIEW_ID,
                &[&source_domain, &source_namespace_path, &source_name],
            )
            .await
            .map_err(internal_err("resolve source view"))?
            .map(|row| row.get(0))
            .ok_or_else(|| {
                CatalogError::NotFound(format!(
                    "view '{}' in namespace '{}'",
                    source_name, source_namespace_path
                ))
            })?;

        let dest_namespace_id =
            resolve_namespace_id(&client, dest_domain, dest_namespace_path).await?;

        let updated = client
            .execute(
                queries::iceberg::MOVE_VIEW,
                &[&view_id, &dest_name, &dest_namespace_id],
            )
            .await
            .map_err(already_exists_or(
                "rename_view",
                format!(
                    "view '{}' in namespace '{}'",
                    dest_name, dest_namespace_path
                ),
            ))?;

        if updated == 0 {
            return Err(CatalogError::NotFound(format!("view '{}'", source_name)));
        }
        Ok(())
    }

    async fn view_exists(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<bool, CatalogError> {
        let client = self.get_client().await?;
        let exists: bool = client
            .query_one(
                queries::iceberg::VIEW_EXISTS,
                &[&domain, &namespace_path, &view_name],
            )
            .await
            .map_err(internal_err("view_exists"))?
            .get(0);

        Ok(exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_connection_sqlstates_as_transient() {
        for code in [
            SqlState::CONNECTION_EXCEPTION,
            SqlState::CONNECTION_DOES_NOT_EXIST,
            SqlState::CONNECTION_FAILURE,
            SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            SqlState::SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION,
            SqlState::TRANSACTION_RESOLUTION_UNKNOWN,
            SqlState::PROTOCOL_VIOLATION,
            SqlState::OPERATOR_INTERVENTION,
            SqlState::ADMIN_SHUTDOWN,
            SqlState::CRASH_SHUTDOWN,
            SqlState::CANNOT_CONNECT_NOW,
            SqlState::DATABASE_DROPPED,
        ] {
            assert_eq!(classify_sql_state(&code), Some(PgFailureClass::Transient));
        }
    }

    #[test]
    fn classifies_retryable_sqlstates_as_transient() {
        for code in [SqlState::QUERY_CANCELED, SqlState::LOCK_NOT_AVAILABLE] {
            assert_eq!(classify_sql_state(&code), Some(PgFailureClass::Transient));
        }
    }

    #[test]
    fn classifies_check_violation_as_validation() {
        assert_eq!(
            classify_sql_state(&SqlState::CHECK_VIOLATION),
            Some(PgFailureClass::Validation)
        );
    }

    #[test]
    fn leaves_business_sqlstates_to_call_sites() {
        for code in [
            SqlState::UNIQUE_VIOLATION,
            SqlState::FOREIGN_KEY_VIOLATION,
            SqlState::RESTRICT_VIOLATION,
        ] {
            assert_eq!(classify_sql_state(&code), None);
        }
    }

    #[test]
    fn patch_action_encodes_three_states() {
        let (act, val): (i32, Option<String>) = patch_action(PatchField::NoChange);
        assert_eq!((act, val), (0, None));

        let (act, val) = patch_action(PatchField::Set("x".to_string()));
        assert_eq!((act, val.as_deref()), (1, Some("x")));

        let (act, val): (i32, Option<String>) = patch_action(PatchField::Unset);
        assert_eq!((act, val), (2, None));
    }

    #[test]
    fn path_prefixes_excludes_full_path() {
        assert_eq!(path_prefixes("a"), Vec::<&str>::new());
        assert_eq!(path_prefixes("a/b"), vec!["a"]);
        assert_eq!(path_prefixes("a/b/c"), vec!["a", "a/b"]);
    }

    #[test]
    fn path_depth_counts_segments() {
        assert_eq!(path_depth("a"), 1);
        assert_eq!(path_depth("a/b/c"), 3);
    }

    #[test]
    fn version_key_derived_from_iceberg_location() {
        assert_eq!(
            version_key_from_location(
                "s3://bucket/wh/default/ns/t/metadata/00001-a3f2c1.metadata.json"
            ),
            "00001"
        );
        assert_eq!(
            version_key_from_location("s3://bucket/wh/t/metadata/v3.metadata.json"),
            "3"
        );
        assert_eq!(
            version_key_from_location("s3://bucket/wh/t/metadata/snapshot.metadata.json"),
            "snapshot"
        );
    }

    #[test]
    fn non_null_json_maps_json_null_to_sql_null() {
        assert!(non_null_json(serde_json::Value::Null).is_none());
        assert!(non_null_json(serde_json::json!({"a": 1})).is_some());
    }

    #[test]
    fn empty_properties_filter_matches_everything() {
        let filter = properties_filter_json(&std::collections::HashMap::new());
        assert_eq!(filter, serde_json::json!({}));
    }
}
