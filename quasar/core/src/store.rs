//! Store trait definitions: the single contract between `storage`
//! (implementation) and `adapter` (callers), per DESIGN §4.
//!
//! Nine focused traits compose the `CatalogStore` marker super-trait via
//! a blanket impl. Protocol-specific capabilities that cannot be
//! expressed through the generic traits live in extension traits; the
//! only current instance is `IcebergCatalogStore` = `CatalogStore` + six
//! Iceberg extension traits (DESIGN §4.1).
//!
//! Pagination convention (DESIGN §4.3): all list methods take
//! `(offset: u64, limit: u64)` and return the current page. When the
//! returned count equals `limit`, the caller (adapter) generates
//! `next_token = offset + returned count`; otherwise there is no next
//! page. Token encoding happens in the adapter protocol layer; the
//! storage layer runs no COUNT query.

use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::CatalogError;
use crate::models::{
    Asset, AssetType, AssetVersion, AssetWithTabular, Domain, Format, Namespace, PatchField,
    TabularAsset, View, ViewIdentifier,
};

// ---------- 过滤、补丁与创建输入类型（DESIGN §4.3 支撑类型） ----------

/// AssetStore::list_assets 的存储层过滤条件，用于已知 Domain/Namespace 上下文的资产列举。
#[derive(Debug, Clone, Default)]
pub struct AssetFilter {
    /// 按 Domain 名过滤；None 表示不限。
    pub domain: Option<String>,
    /// 按 Namespace 路径精确匹配；None 表示不限。
    pub namespace: Option<String>,
    /// 按资产类型过滤（如 `table`、`view`）。
    pub asset_type: Option<String>,
    /// 按格式过滤（如 `iceberg`、`lance`）。
    pub format: Option<String>,
    /// 按标签过滤；多个标签为 AND 关系。
    pub tags: Vec<String>,
    /// 按 properties 精确匹配过滤；多个条件为 AND 关系。
    pub properties: HashMap<String, String>,
    /// 是否包含软删除资产；默认 false（仅活动资产）。
    pub include_deleted: bool,
}

/// UnifiedQueryStore::query_assets 的发现层查询条件，用于跨 Namespace 资产发现。
#[derive(Debug, Clone, Default)]
pub struct AssetQuery {
    /// 目标 Domain（必填）。
    pub domain: String,
    /// Namespace 路径前缀（层级查询）；None 表示整个 Domain。
    pub namespace_prefix: Option<String>,
    pub asset_type: Option<String>,
    pub format: Option<String>,
    pub tags: Vec<String>,
    pub properties: HashMap<String, String>,
    pub include_deleted: bool,
    /// 分页偏移与页大小（语义同列表方法的分页约定）。
    pub offset: u64,
    pub limit: u64,
}

/// Asset 的可补丁字段（comment、properties）。
#[derive(Debug, Clone, Default)]
pub struct AssetPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

/// 创建 Domain 的输入。未指定 warehouse 时使用全局 QUASAR_WAREHOUSE_PATH。
#[derive(Debug, Clone, Default)]
pub struct CreateDomain {
    /// 全局唯一名称，URL-safe slug。
    pub name: String,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    /// 对象存储后端类型：s3 / minio / hdfs / local。
    pub storage_type: Option<String>,
    /// 后端连接配置；不得明文存 credential（用 secret 引用）。
    pub storage_config: Option<serde_json::Value>,
    /// Domain 级默认对象存储根路径，覆盖全局默认。
    pub warehouse: Option<String>,
}

/// Domain 的可补丁字段。
#[derive(Debug, Clone, Default)]
pub struct DomainPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
    pub storage_type: PatchField<String>,
    pub storage_config: PatchField<serde_json::Value>,
    pub warehouse: PatchField<String>,
}

/// 创建 Namespace 的输入（Domain 与路径在 trait 方法参数中给出）。
#[derive(Debug, Clone, Default)]
pub struct CreateNamespace {
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
}

/// Namespace 的可补丁字段。
#[derive(Debug, Clone, Default)]
pub struct NamespacePatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

/// 创建 Asset 的输入（由原生 adapter 调用）。
#[derive(Debug, Clone, Default)]
pub struct CreateAsset {
    pub domain: String,
    /// Namespace 层级路径。
    pub namespace: String,
    /// 资产名，URL-safe slug；同 Namespace 内活动资产唯一。
    pub name: String,
    /// 已注册的资产类型。
    pub asset_type: String,
    /// 可选格式（如 iceberg、lance）；兼容性由 adapter 校验。
    pub format: Option<String>,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
}

/// 创建版本的输入（由原生 adapter 在 commit 时镜像调用）。
#[derive(Debug, Clone, Default)]
pub struct CreateVersion {
    pub asset_id: Uuid,
    /// 格式原生版本标识：Iceberg 为 metadata 序号（如 `00001`），Lance 为原生版本号。
    pub version_key: String,
    /// 版本属性：commit message、作者、操作类型等格式无关信息。
    pub version_properties: Option<serde_json::Value>,
    /// 可选少量内联内容（小 JSON/YAML 配置）。
    pub content_inline: Option<serde_json::Value>,
    /// 外部内容指针：Iceberg 为完整 metadata_location，Lance 为 manifest 路径。
    pub content_pointer: Option<String>,
    /// 前驱版本；CAS 路径下由存储层自动链接，无需调用方传入（见 §6.4）。
    pub previous_version_id: Option<Uuid>,
}

/// 注册资产类型的输入。
#[derive(Debug, Clone)]
pub struct RegisterAssetType {
    pub name: String,
    pub description: Option<String>,
    /// 分类：tabular / view / model / agent / tool / mcp_server / fileset / topic / generic。
    pub category: String,
    /// 可选 JSON Schema，用于校验资产 properties。
    pub validation_schema: Option<serde_json::Value>,
    /// 类型字段存储策略：jsonb / dedicated_table / reference_only。
    pub extension_strategy: String,
    /// 是否预期有原生协议（影响 Unified API 只读判定）。
    pub supports_native_protocol: bool,
}

/// 注册格式的输入。
#[derive(Debug, Clone, Default)]
pub struct RegisterFormat {
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    /// 序列化提示，如 json、protobuf。
    pub serialization_hint: Option<String>,
}

// ---------- 核心 trait（DESIGN §4.3） ----------

/// Domain 生命周期。Domain 是顶层组织边界，按名称寻址（名称全局唯一）。
#[async_trait]
pub trait DomainStore: Send + Sync {
    async fn create_domain(&self, input: CreateDomain) -> Result<Domain, CatalogError>;
    async fn get_domain(&self, name: &str) -> Result<Domain, CatalogError>;
    async fn list_domains(&self, offset: u64, limit: u64) -> Result<Vec<Domain>, CatalogError>;
    async fn update_domain(&self, name: &str, patch: DomainPatch) -> Result<Domain, CatalogError>;
    /// 仅空 Domain 可删除；非空返回 Conflict。
    async fn delete_domain(&self, name: &str) -> Result<(), CatalogError>;
}

/// 层级 Namespace 生命周期。Namespace 按 (domain, path) 寻址，path 如 `analytics/teams/finance`。
#[async_trait]
pub trait NamespaceStore: Send + Sync {
    /// 创建 path 指向的 Namespace；中间节点不存在时隐式创建。
    async fn create_namespace(
        &self,
        domain: &str,
        path: &str,
        input: CreateNamespace,
    ) -> Result<Namespace, CatalogError>;
    async fn get_namespace(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError>;
    /// prefix 为路径前缀过滤（层级查询）；None 列出 Domain 下全部。
    async fn list_namespaces(
        &self,
        domain: &str,
        prefix: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Namespace>, CatalogError>;
    async fn update_namespace(
        &self,
        domain: &str,
        path: &str,
        patch: NamespacePatch,
    ) -> Result<Namespace, CatalogError>;
    /// 仅空 Namespace（无子节点、无资产）可删除；非空返回 Conflict。
    async fn delete_namespace(&self, domain: &str, path: &str) -> Result<(), CatalogError>;
    /// 解析层级路径到 Namespace 实体；供 adapter 将协议路径换算为内部实体。
    async fn resolve_path(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError>;
}

/// Asset 生命周期。治理操作按 id 寻址（重命名不影响引用）；协议路径按名解析。
/// 基线下所有资产均为原生协议资产：create/update/rename/soft_delete 由原生 adapter 调用，
/// Unified API 仅调用读取方法与 restore（见需求文档 §4.3）。
#[async_trait]
pub trait AssetStore: Send + Sync {
    async fn create_asset(&self, input: CreateAsset) -> Result<Asset, CatalogError>;
    async fn get_asset(&self, id: Uuid) -> Result<Asset, CatalogError>;
    async fn get_asset_by_name(
        &self,
        domain: &str,
        namespace: &str,
        name: &str,
    ) -> Result<Asset, CatalogError>;
    async fn list_assets(
        &self,
        filter: AssetFilter,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Asset>, CatalogError>;
    async fn update_asset(&self, id: Uuid, patch: AssetPatch) -> Result<Asset, CatalogError>;
    /// Rename an asset, optionally moving it to another namespace.
    ///
    /// `new_namespace_path` is `None` for a plain in-place rename. When it
    /// is `Some(path)`, the asset is additionally moved to the namespace at
    /// that hierarchical path, resolved inside the Domain that currently
    /// owns the asset — cross-Domain moves are not expressible through this
    /// signature. A missing destination namespace yields `NotFound`; an
    /// active asset with the same name in the destination namespace yields
    /// `AlreadyExists` (backed by the `uq_assets_active_name` index). A
    /// move with an unchanged name is supported.
    async fn rename_asset(
        &self,
        id: Uuid,
        new_name: &str,
        new_namespace_path: Option<&str>,
    ) -> Result<Asset, CatalogError>;
    async fn soft_delete_asset(&self, id: Uuid) -> Result<(), CatalogError>;
    /// 恢复软删除资产；同 Namespace 同名活动资产已存在时返回 Conflict。
    async fn restore_asset(&self, id: Uuid) -> Result<Asset, CatalogError>;
    /// 级联清理扩展表与版本记录；保留窗口策略见需求文档 §10。
    async fn hard_delete_asset(&self, id: Uuid) -> Result<(), CatalogError>;
}

/// 版本历史。create_version 由原生 adapter 在 commit 时镜像调用，
/// 并自动将 assets.current_version_key 更新为新版本的 version_key。
/// 基线不提供 delete_version（延后，见需求文档 §10）。
#[async_trait]
pub trait VersionStore: Send + Sync {
    async fn create_version(&self, input: CreateVersion) -> Result<AssetVersion, CatalogError>;
    async fn get_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<AssetVersion, CatalogError>;
    async fn list_versions(
        &self,
        asset_id: Uuid,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<AssetVersion>, CatalogError>;
    /// 基于 assets.current_version_key 查询当前版本。
    async fn get_latest_version(&self, asset_id: Uuid) -> Result<AssetVersion, CatalogError>;
}

/// 资产标签（治理标注）。标签写入不触碰原生协议状态，对所有资产开放。
#[async_trait]
pub trait TagStore: Send + Sync {
    async fn add_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn remove_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn list_tags(&self, asset_id: Uuid) -> Result<Vec<String>, CatalogError>;
    async fn list_assets_by_tag(
        &self,
        domain: &str,
        tag: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Asset>, CatalogError>;
}

/// Discovery 查询：跨 Namespace 的资产发现，domain 必填，namespace_prefix 前缀匹配。
#[async_trait]
pub trait UnifiedQueryStore: Send + Sync {
    async fn query_assets(&self, query: AssetQuery) -> Result<Vec<Asset>, CatalogError>;
}

/// CAS 版本提交：以"当前内容指针"为锚点（tabular 资产即 metadata_location）。
/// 事务内锁定 assets 行、校验指针、插入镜像版本并更新 current_version_key（见 §6.4）。
#[async_trait]
pub trait CasCommitStore: Send + Sync {
    async fn compare_and_swap_pointer(
        &self,
        asset_id: Uuid,
        expected_pointer: &str,
        new_version: CreateVersion,
    ) -> Result<AssetVersion, CatalogError>;
}

/// 资产类型与格式注册表（全局资源）。
#[async_trait]
pub trait AssetTypeStore: Send + Sync {
    async fn register_asset_type(
        &self,
        input: RegisterAssetType,
    ) -> Result<AssetType, CatalogError>;
    async fn register_format(&self, input: RegisterFormat) -> Result<Format, CatalogError>;
    async fn list_asset_types(
        &self,
        category: Option<&str>,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<AssetType>, CatalogError>;
    async fn get_asset_type(&self, name: &str) -> Result<AssetType, CatalogError>;
    async fn get_format(&self, name: &str) -> Result<Format, CatalogError>;
    async fn list_formats(&self, offset: u64, limit: u64) -> Result<Vec<Format>, CatalogError>;
}

/// tabular 资产的类型扩展读写（DESIGN §3.3 `tabular_assets`）。
///
/// 通用 `create_asset` 只写 `assets` 行；`table` 类型资产需要在同一事务内
/// 写入 `tabular_assets` 扩展行（location / metadata_location 缓存）。Iceberg
/// 与 Lance adapter 共用此通道创建表资产。
#[async_trait]
pub trait TabularStore: Send + Sync {
    /// 同事务创建 `assets` 行与 `tabular_assets` 扩展行。
    /// `input.asset_type` 必须为 `table`（应用层校验，替代原触发器）。
    async fn create_tabular_asset(
        &self,
        input: CreateAsset,
        location: &str,
        metadata_location: Option<&str>,
    ) -> Result<AssetWithTabular, CatalogError>;

    /// 按资产 id 读取 `tabular_assets` 扩展行。
    async fn get_tabular_asset(&self, asset_id: Uuid) -> Result<TabularAsset, CatalogError>;
}

/// Marker super-trait composing all generic stores (DESIGN §4.3). Holds
/// no methods of its own; the blanket impl makes any type satisfying the
/// constituent bounds satisfy `CatalogStore` as well.
pub trait CatalogStore:
    DomainStore
    + NamespaceStore
    + AssetTypeStore
    + AssetStore
    + VersionStore
    + TagStore
    + UnifiedQueryStore
    + CasCommitStore
    + TabularStore
    + Send
    + Sync
{
}

impl<T> CatalogStore for T where
    T: DomainStore
        + NamespaceStore
        + AssetTypeStore
        + AssetStore
        + VersionStore
        + TagStore
        + UnifiedQueryStore
        + CasCommitStore
        + TabularStore
        + Send
        + Sync
        + ?Sized
{
}

// ---------- Iceberg 协议扩展 trait（DESIGN §4.1 协议扩展 trait） ----------
//
// These capabilities cannot be composed from the generic traits (e.g. a
// multi-table transaction requires all per-table CAS branches to roll
// back together in one DB transaction). Their operations land on the
// Iceberg adapter's private tables (DESIGN §3.5). They follow the same
// error, pagination and supporting-type conventions as the generic
// traits; `namespace_path` is the hierarchical namespace path.

/// Staged table lifecycle for the Iceberg stage-create commit flow.
#[async_trait]
pub trait IcebergStagingStore: Send + Sync {
    /// Create a staged table record. Before insertion, deletes any
    /// expired staged record with the same (domain, namespace_path,
    /// table_name). Returns `AlreadyExists` if an active table or a
    /// non-expired staged record exists.
    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<(), CatalogError>;

    /// Read a non-expired staged table record.
    async fn get_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<Option<serde_json::Value>, CatalogError>;

    /// Delete a staged table record (e.g. after successful commit or
    /// explicit cancel).
    async fn delete_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<(), CatalogError>;

    /// Commit a staged table: in a single transaction, verify the active
    /// table does not exist, lock the staged record, insert assets +
    /// tabular_assets rows plus the first mirrored version, and delete
    /// the staged record.
    #[allow(clippy::too_many_arguments)]
    async fn commit_staged_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: serde_json::Value,
    ) -> Result<AssetWithTabular, CatalogError>;
}

/// Register an externally-managed Iceberg table into the catalog.
#[async_trait]
pub trait IcebergRegisterStore: Send + Sync {
    /// In a single transaction, insert assets + tabular_assets rows
    /// pointing to an external metadata location, plus the first mirrored
    /// version. Does not write to the object store.
    #[allow(clippy::too_many_arguments)]
    async fn register_iceberg_table(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        location: &str,
        metadata_location: &str,
        metadata_json: serde_json::Value,
        properties: serde_json::Value,
    ) -> Result<AssetWithTabular, CatalogError>;
}

/// Persist Iceberg scan metrics reports.
#[async_trait]
pub trait IcebergMetricsStore: Send + Sync {
    /// Record a raw scan metrics report JSON. The caller has already
    /// verified that the table exists and is an Iceberg table.
    async fn record_scan_metrics_report(
        &self,
        asset_id: Option<Uuid>,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
        report: serde_json::Value,
        user_agent: Option<&str>,
    ) -> Result<(), CatalogError>;
}

/// Iceberg purge (DROP TABLE PURGE) operation tracking.
#[async_trait]
pub trait IcebergPurgeStore: Send + Sync {
    /// In a single transaction: read the table location, create a purge
    /// operation record with status `catalog_dropped`, delete the catalog
    /// records (assets + extension rows via FK cascade), and return the
    /// operation id along with the table location and metadata location
    /// needed for object store cleanup.
    async fn begin_iceberg_purge_and_drop_catalog(
        &self,
        domain: &str,
        namespace_path: &str,
        table_name: &str,
    ) -> Result<(Uuid, String, Option<String>), CatalogError>;

    /// Update the status of a purge operation.
    async fn update_purge_operation(
        &self,
        operation_id: Uuid,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<(), CatalogError>;
}

/// One table's CAS branch inside a multi-table transaction.
#[derive(Debug, Clone)]
pub struct IcebergTableCommit {
    pub domain: String,
    /// Hierarchical namespace path of the table.
    pub namespace_path: String,
    pub table: String,
    /// CAS anchor: the pointer the client based its commit on.
    pub expected_pointer: String,
    /// New metadata location produced by the commit.
    pub new_location: String,
    /// Mirrored version key extracted from the new metadata file name
    /// (e.g. `00002`); the store mirrors the commit into `asset_versions`
    /// within the same DB transaction (DESIGN §6.4).
    pub new_version_key: String,
    pub schema_snapshot: Option<serde_json::Value>,
}

/// Multi-table transaction commit. Executes the CAS updates for all
/// tables within a single PostgreSQL transaction, guaranteeing
/// atomicity.
#[async_trait]
pub trait IcebergTransactionStore: Send + Sync {
    /// In a single DB transaction, execute the CAS pointer update for
    /// every commit in `commits`. Tables are locked in ascending
    /// `asset_id` order to avoid deadlocks (DESIGN §6.4). If any CAS
    /// fails, the entire transaction rolls back.
    async fn commit_transaction_tables(
        &self,
        commits: Vec<IcebergTableCommit>,
    ) -> Result<(), CatalogError>;
}

/// Iceberg view lifecycle management.
///
/// View requirement validation happens at the adapter layer (consistent
/// with table requirement handling); the store layer is only responsible
/// for view identity management and CAS metadata_location updates.
#[async_trait]
pub trait IcebergViewStore: Send + Sync {
    /// Create a view (assets + view_assets rows plus the first mirrored
    /// version). `view_uuid` is generated by the adapter layer.
    #[allow(clippy::too_many_arguments)]
    async fn create_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
        view_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        properties: serde_json::Value,
    ) -> Result<View, CatalogError>;

    /// Load a view (assets + view_assets joined row).
    async fn get_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<View, CatalogError>;

    /// CAS update of the view metadata_location. Requirement validation
    /// is already done at the adapter layer; the store layer executes the
    /// CAS update and mirrors the commit into `asset_versions` (version
    /// key extracted by the adapter from the new metadata file name).
    async fn commit_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
        expected_pointer: &str,
        new_location: &str,
        new_version_key: &str,
    ) -> Result<(), CatalogError>;

    /// Drop a view (FK cascade deletes the view_assets row and versions).
    async fn drop_view(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<(), CatalogError>;

    /// List all views in a namespace.
    async fn list_views(
        &self,
        domain: &str,
        namespace_path: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ViewIdentifier>, CatalogError>;

    /// Rename a view (update the assets row + sync view_assets).
    async fn rename_view(
        &self,
        source_domain: &str,
        source_namespace_path: &str,
        source_name: &str,
        dest_domain: &str,
        dest_namespace_path: &str,
        dest_name: &str,
    ) -> Result<(), CatalogError>;

    /// Check whether a view exists (assets row with `asset_type='view'`).
    async fn view_exists(
        &self,
        domain: &str,
        namespace_path: &str,
        view_name: &str,
    ) -> Result<bool, CatalogError>;
}

/// Iceberg-specific marker super-trait composing the generic
/// `CatalogStore` plus the six Iceberg extension traits (DESIGN §4.1).
pub trait IcebergCatalogStore:
    CatalogStore
    + IcebergStagingStore
    + IcebergRegisterStore
    + IcebergMetricsStore
    + IcebergPurgeStore
    + IcebergTransactionStore
    + IcebergViewStore
    + Send
    + Sync
{
}

impl<T> IcebergCatalogStore for T where
    T: CatalogStore
        + IcebergStagingStore
        + IcebergRegisterStore
        + IcebergMetricsStore
        + IcebergPurgeStore
        + IcebergTransactionStore
        + IcebergViewStore
        + Send
        + Sync
        + ?Sized
{
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compile-time assertion that the marker super-trait composes the
    /// nine generic sub-traits and that the blanket impl reaches it. If
    /// the constraint list drifts away from DESIGN §4.3, this stops
    /// compiling.
    #[allow(dead_code)]
    fn _catalog_store_marker_composes_all<T>()
    where
        T: DomainStore
            + NamespaceStore
            + AssetTypeStore
            + AssetStore
            + VersionStore
            + TagStore
            + UnifiedQueryStore
            + CasCommitStore
            + TabularStore,
    {
        fn _assert_catalog_store<S: CatalogStore + ?Sized>() {}
        _assert_catalog_store::<T>();
    }

    /// Compile-time assertion that the IcebergCatalogStore marker
    /// composes all Iceberg extension traits and the underlying
    /// CatalogStore.
    #[allow(dead_code)]
    fn _iceberg_catalog_store_marker_composes_all<T>()
    where
        T: CatalogStore
            + IcebergStagingStore
            + IcebergRegisterStore
            + IcebergMetricsStore
            + IcebergPurgeStore
            + IcebergTransactionStore
            + IcebergViewStore,
    {
        fn _assert_iceberg_store<S: IcebergCatalogStore + ?Sized>() {}
        _assert_iceberg_store::<T>();
    }

    #[test]
    fn domain_patch_default_is_all_no_change() {
        let patch = DomainPatch::default();
        assert!(matches!(patch.comment, PatchField::NoChange));
        assert!(matches!(patch.properties, PatchField::NoChange));
        assert!(matches!(patch.storage_type, PatchField::NoChange));
        assert!(matches!(patch.storage_config, PatchField::NoChange));
        assert!(matches!(patch.warehouse, PatchField::NoChange));
    }

    #[test]
    fn asset_filter_default_is_unrestricted() {
        let filter = AssetFilter::default();
        assert!(filter.domain.is_none());
        assert!(filter.namespace.is_none());
        assert!(filter.asset_type.is_none());
        assert!(filter.format.is_none());
        assert!(filter.tags.is_empty());
        assert!(filter.properties.is_empty());
        assert!(!filter.include_deleted);
    }
}
