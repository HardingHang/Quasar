# Quasar V4.2 设计说明书

> **版本**: V1.1
> **日期**: 2026-06-06
> **状态**: V4.2 设计基线（待评审）
>
> 本文档承接 V4.2 需求文档 (`V4_2_REQUIREMENTS.md`) 的实现设计。
> 官方 REST API 来源、端点全集和小版本范围矩阵以
> `docs/v4/V4_OFFICIAL_REST_API.md` 为准；本文档只描述 Quasar 的 V4.2
> 实现选择。
>
> **前置依赖**: V4.1 已完成并通过验收；本文档经评审批准后方可进入实现阶段。

---

## 目录

- [一、执行摘要](#一执行摘要)
- [二、架构总览](#二架构总览)
- [三、依赖与类型策略（增量）](#三依赖与类型策略增量)
- [四、数据模型设计（增量）](#四数据模型设计增量)
- [五、Iceberg View REST API 设计](#五iceberg-view-rest-api-设计)
- [六、Server-side Scan Planning REST API 设计](#六server-side-scan-planning-rest-api-设计)
- [七、View Commit 与 Metadata 设计](#七view-commit-与-metadata-设计)
- [八、Scan Planning 执行设计](#八scan-planning-执行设计)
- [九、错误处理设计（增量）](#九错误处理设计增量)
- [十、测试策略](#十测试策略)
- [十一、实现顺序](#十一实现顺序)
- [十二、V4.2 不做什么](#十二v42-不做什么)
- [十三、修订记录](#十三修订记录)

---

## 一、执行摘要

### 1.1 V4.2 核心目标

V4.2 在 V4.1 "REST Table Capability Completion" 基础上，补齐 Iceberg REST Catalog 的 **View 生命周期管理** 和 **Server-side Scan Planning** 能力：

- **Iceberg View 生命周期**: 7 个端点（list / create / load / replace / drop / head / rename），支持 Iceberg View Format Spec V1 的 metadata 构建和 CAS commit。
- **Server-side Scan Planning**: 4 个端点（submit plan / fetch plan / cancel plan / fetch tasks），支持 Filter/Split 参数和 OpenAPI/Java ResourcePaths 双路径 alias。

V4.2 明确不做：`register-view` 端点、View Version 历史回滚、Scan Planning 结果持久化、Scan Planning 调度优化、View/Table 跨资源引用验证。

### 1.2 V4.1 -> V4.2 关键设计变更

| 设计域 | V4.1 状态 | V4.2 设计 |
|--------|-----------|-----------|
| Iceberg View | 不实现 | 7 个 View 端点，ViewMetadata 构建与 CAS commit，`view_assets` 扩展表 |
| Scan Planning | 不实现 | 4 个 Scan Planning 端点，自包含 plan-task token，OpenAPI + Java ResourcePaths 双路径 alias |
| `/v1/config` endpoints | 不包含 View/Scan Planning | 添加 7 个 View 端点 + 4 个 Scan Planning 端点（含 alias 路径） |
| Store trait | 不含 View Store | 新增 `IcebergViewStore` trait，super-trait 为 `AssetStore` |
| 数据模型 | 无 `view_assets` | 新增 `view_assets` 扩展表 + `asset_types` 注册 `view` |

### 1.3 V4.2 设计原则

1. **继承 V4.0/V4.1 原则**: 官方协议路径优先、声明即实现、metadata 兼容优先、Catalog 状态由 PostgreSQL 仲裁。
2. **View/Table 身份隔离**: View 和 Table 在同一 `(namespace_id, name)` 下禁止同名（Iceberg 语义），现有 `uq_assets_active_name` 纯唯一索引自动保证此约束。
3. **View metadata 由 iceberg crate 处理**: 与 Table metadata 一样，采用 `iceberg` crate 0.9.1 的 `ViewMetadataBuilder` 构建/校验 metadata，Quasar adapter 仅做 REST DTO wrapper。
4. **Scan Planning 无持久化**: Plan 结果仅在请求会话内有效，`plan-task` token 自包含，无 PostgreSQL 状态表。
5. **路径 alias 优先 OpenAPI**: Scan Planning 主路径使用 OpenAPI 路径，Java `ResourcePaths` 常量路径作为路由层 alias。

---

## 二、架构总览

### 2.1 V4.2 组件边界

```
Spark Iceberg 1.10.x client
        |
        v
  /iceberg/v1/{prefix}/...
        |
        v
quasar-adapter::iceberg
  - View lifecycle handlers (新增: view.rs)
  - Scan Planning handlers (新增: scan_planning.rs)
  - View metadata build/commit wrapper (新增: view_metadata.rs)
  - Scan Planning token encode/decode (新增: scan_plan.rs)
  - Path alias routing (新增: axum route alias)
  - state: Arc<dyn CatalogStore>
        |
        v
quasar-core
  - V4.1 Store trait family (继承)
  - IcebergViewStore (新增)
  - View / ViewAsset / ViewIdentifier models (新增)
  - StoreError semantic errors (继承)
        |
        v
quasar-storage
  - PostgreSQL catalog identity rows (继承)
  - view_assets extension table (新增)
  - View CRUD + CAS commit SQL (新增)
  - asset_types 'view' seed (新增)
        |
        v
PostgreSQL + S3/MinIO object store
```

### 2.2 V4.2 新增端点

| 端点 | 主路径 | Alias 路径（仅 Scan Planning） | V4.2 状态 |
|------|--------|------------------------------|----------|
| List Views | `GET .../namespaces/{namespace}/views` | — | 新增实现 |
| Create View | `POST .../namespaces/{namespace}/views` | — | 新增实现 |
| Load View | `GET .../namespaces/{namespace}/views/{view}` | — | 新增实现 |
| Replace View | `POST .../namespaces/{namespace}/views/{view}` | — | 新增实现 |
| Drop View | `DELETE .../namespaces/{namespace}/views/{view}` | — | 新增实现 |
| Head View | `HEAD .../namespaces/{namespace}/views/{view}` | — | 新增实现 |
| Rename View | `POST .../views/rename` | — | 新增实现 |
| Submit Plan | `POST .../namespaces/{namespace}/tables/{table}/plan` | `POST .../tables/{table}/plan` | 新增实现 |
| Fetch Plan | `GET .../namespaces/{namespace}/tables/{table}/plan/{plan-id}` | `GET .../tables/{table}/plan/{plan-id}` | 新增实现 |
| Cancel Plan | `DELETE .../namespaces/{namespace}/tables/{table}/plan/{plan-id}` | `DELETE .../tables/{table}/plan/{plan-id}` | 新增实现 |
| Fetch Tasks | `POST .../namespaces/{namespace}/tables/{table}/tasks` | `POST .../tables/{table}/tasks` | 新增实现 |

### 2.3 `/v1/config` endpoints 更新

V4.2 实现后，`/v1/config` 的 `endpoints` 字段新增：

```rust
fn supported_endpoints() -> Vec<String> {
    vec![
        // V4.0 + V4.1 endpoints...
        "GET /v1/{prefix}/namespaces/{namespace}/views".to_string(),
        "POST /v1/{prefix}/namespaces/{namespace}/views".to_string(),
        "GET /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
        "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
        "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
        "POST /v1/{prefix}/namespaces/{namespace}/views/{view}".to_string(),
        "POST /v1/{prefix}/views/rename".to_string(),
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan".to_string(),
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}".to_string(),
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}".to_string(),
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks".to_string(),
    ]
}
```

**说明**：
- Scan Planning 的 Java `ResourcePaths` alias 路径（不含 `namespaces/{namespace}`）**不在 `/v1/config` endpoints 中声明**，因为 Iceberg `Endpoint` enum 只定义 OpenAPI 路径版本。alias 仅用于路由层兼容，不在客户端能力发现机制中暴露。
- `POST .../views`（Create View）和 `POST .../views/{view}`（Replace View）使用相同 HTTP method 但路径不同，需分别列出。

---

## 三、依赖与类型策略（增量）

### 3.1 iceberg crate View 能力验证

V4.2 实现前必须在 `quasar/adapter` 中验证 `iceberg` crate 0.9.1 的以下 View 能力：

| 验证项 | 验证方法 | Fallback |
|--------|----------|----------|
| ViewMetadata 解析 | `serde_json::from_value::<ViewMetadata>()` 反序列化 ViewMetadataV1Valid.json | 若不支持，评估是否需要自研 View metadata DTO |
| ViewMetadataBuilder 构建 | `ViewMetadataBuilder::new()` / `from_view_creation()` / `new_from_metadata()` 构建 ViewMetadata | 若缺失关键 builder 方法，在 adapter 中补充 wrapper |
| ViewRequirement 处理 | 检查 crate 是否提供 `ViewRequirement` 类型 | iceberg crate 0.9.1 无 `ViewRequirement` enum，V4.2 在 adapter 层定义兼容枚举 |
| ViewUpdate 应用 | `ViewMetadataBuilder` 的 `add_version` / `set_current_version` / `set_properties` / `remove_properties` / `set_location` / `assign_uuid` 等方法 | 若 crate 方法覆盖不全，补充 adapter wrapper |
| ViewVersion / ViewRepresentation 序列化 | 构建含 SQL representation 的 ViewVersion 并序列化验证 `type: "sql"` discriminator | 若 discriminator 缺失，评估是否需要 serde wrapper 补充 |

### 3.2 View DTO 策略

与 Table 的 wrapper 策略一致：

- REST 请求/响应字段名由 Quasar DTO 控制，保证与 Iceberg REST OpenAPI 对齐。
- View metadata 内部结构优先转换为 `iceberg` crate 类型或由其校验。
- `ViewRequirement` 类型由 Quasar adapter 定义（iceberg crate 0.9.1 不提供此 enum），使用官方 1.10.x wire name。
- `ViewUpdate` 使用 `iceberg` crate 0.9.1 提供的 `ViewUpdate` enum（serde tag = `action`，与 OpenAPI discriminator 对齐）。

### 3.3 ViewRequirement Adapter 定义

iceberg crate 0.9.1 不提供 `ViewRequirement` enum（只有 `TableRequirement`）。V4.2 在 adapter 层定义：

```rust
// quasar/adapter/src/iceberg/view_metadata.rs (新增)

/// View commit requirement（Iceberg 1.10.x）。
/// iceberg crate 0.9.1 不提供此类型，由 adapter 定义。
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ViewRequirement {
    /// View 不存在时才允许创建（与 Table assert-create 语义一致）。
    AssertCreate,
    /// View UUID 必须匹配请求值。
    AssertViewUuid {
        #[serde(rename = "uuid")]
        uuid: Uuid,
    },
}
```

校验逻辑在 adapter 层实现（与 Table requirement 校验模式一致），不委托给 Store 层。

---

## 四、数据模型设计（增量）

### 4.1 现有核心表保持不变

V4.2 继续使用 V3/V4.0/V4.1 的核心目录模型：

```text
domains
  -> namespaces
      -> assets
          -> tabular_assets (table)
          -> view_assets (新增，V4.2)
```

### 4.2 新增 `asset_types` 注册

```sql
INSERT INTO asset_types (name, comment)
VALUES ('view', 'Iceberg View')
ON CONFLICT DO NOTHING;
```

设计规则：

- `assets.asset_type = 'view'` 标识 View 资源身份。
- `view_assets` 扩展表通过 `asset_id` FK 与 `assets` 关联。
- `uq_assets_active_name` partial unique index 自动保证同一 `(namespace_id, name)` 下 Table 和 View 不同名。

### 4.3 新增 `view_assets` 扩展表

```sql
CREATE TABLE IF NOT EXISTS view_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    view_uuid UUID NOT NULL,
    location TEXT NOT NULL,
    current_version_id INT NOT NULL,
    metadata_location TEXT NOT NULL,
    properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_view_assets_uuid ON view_assets(view_uuid);

-- Trigger: 确保 view_assets 对应 asset_type='view'
CREATE OR REPLACE FUNCTION ensure_view_asset_type()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM assets
        WHERE id = NEW.asset_id
          AND asset_type = 'view'
    ) THEN
        RAISE EXCEPTION 'view asset % must reference an asset with asset_type=view', NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_view_assets_type_check ON view_assets;
CREATE TRIGGER trg_view_assets_type_check
BEFORE INSERT OR UPDATE OF asset_id ON view_assets
FOR EACH ROW
EXECUTE FUNCTION ensure_view_asset_type();
```

设计规则：

- `view_assets` 无 `format` 字段（View 只有 Iceberg 支持，与 `tabular_assets` 有 `format` 字段的设计对称性简化）。若后续 Iceberg 1.11+ 或 Lance 引入 View 格式变体，需评估添加 `format` 字段（V4.2 不预先添加，YAGNI 原则）。
- `view_uuid` 存储 Iceberg ViewMetadata 中的 UUID，用于 CAS commit 校验（`assert-view-uuid`）。
- `metadata_location` 指向对象存储中的当前 View metadata 文件。View metadata JSON 不存储在 PostgreSQL（与 Table 一致，仅存 location）。
- `current_version_id` 存储创建时的初始 version ID（值为 1）。此字段为**快照性质**：Replace commit 后的实际 `current-version-id` 从对象存储 ViewMetadata JSON 中读取，Store 层的 `commit_view` 方法**不更新此字段**。这与 Table CAS 中同步更新 `schema_snapshot` 的模式不同——V4.2 选择不更新 `current_version_id` 和 `properties` 的理由是：View metadata 文件数量远少于 Table，Load View 时总是从对象存储读取完整 metadata（不走 DB 快照 fallback），DB 中的 `current_version_id` 和 `properties` 仅作为创建时初始状态记录。
- `properties` 存储 View 属性快照（如 `version.history.num-entries`），**不保证实时与对象存储 metadata 同步**。`commit_view` 方法**不更新 `properties` 字段**，与 `current_version_id` 的设计决策一致。

### 4.4 View/Table 同名冲突

Iceberg 禁止 View 和 Table 在同一 Namespace 下同名。现有 `uq_assets_active_name` partial unique index：

```sql
CREATE UNIQUE INDEX uq_assets_active_name
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;
```

此约束已保证同一 `(namespace_id, name)` 下 `deleted_at IS NULL` 的记录唯一，**无论 `asset_type` 是 `table` 还是 `view`**。无需新增额外约束。

Adapter 层在 Create View 时还需显式检查：若同 Namespace 下存在同名 Table（`asset_type = 'table'`），返回 `409 AlreadyExistsException`。反之，Create Table 时也应检查同名 View 存在性（此检查在 V4.0/V4.1 已通过 `uq_assets_active_name` 级别隐式保证，但 adapter 层可增加语义明确的显式检查）。

### 4.5 Scan Planning 无新增数据库表

Scan Planning 结果不持久化到 PostgreSQL：

- `plan-task` token 自包含（base64 encoded JSON），具体内容结构由 §八选定策略后定义。
- 服务端无需在 PostgreSQL 维护 Plan 状态表。
- 若后续需要 Plan 状态持久化或异步调度，推迟到 V4.3+。

### 4.6 Store trait 增量

V4.2 新增 `IcebergViewStore` trait。super-trait 为 `AssetStore`（与 `IcebergPurgeStore: TabularStore` 的模式对齐，最小化 trait 约束）：

**实现约束说明**：虽然 `IcebergViewStore: AssetStore` 使 View 的身份操作通过 `assets` 表管理，但 Storage 层的 `create_view` 和 `rename_view` 必须使用自定义 SQL 在单个 PostgreSQL 事务内完成 `assets` + `view_assets` 的双行操作，不能拆分为先调用 `AssetStore::create_asset` 再单独插入 `view_assets`（否则事务原子性被破坏）。`AssetStore` super-trait 的作用是让 `IcebergViewStore` 的实现者可以使用 `asset_exists`、`drop_asset` 等不涉及扩展表的方法，而非委托核心 CRUD 给 `AssetStore`。

```rust
// quasar/core/src/store.rs

/// Iceberg View 生命周期管理（V4.2）。
///
/// View requirement 校验在 adapter 层执行（与 Table requirement 一致），
/// Store 层仅负责 View 身份管理和 CAS metadata_location 更新。
#[async_trait]
pub trait IcebergViewStore: AssetStore {
    /// 创建 View（插入 assets + view_assets 双行）。
    ///
    /// `view_uuid` 由 adapter 层生成后传入。
    /// `current_version_id` 初始值为 1（在 adapter 层设置，Store 层仅持久化）。
    #[allow(clippy::too_many_arguments)]
    async fn create_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        view_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        current_version_id: i32,
        properties: serde_json::Value,
    ) -> Result<View, StoreError>;

    /// 加载 View 记录（读取 assets + view_assets 联合行）。
    async fn get_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<View, StoreError>;

    /// CAS 更新 View metadata_location。
    ///
    /// requirement 校验已在 adapter 层完成，Store 层仅执行 CAS 更新。
    /// **此方法不更新 `view_assets.current_version_id` 和 `view_assets.properties`**：
    /// View 的 Load 总是从对象存储读取完整 metadata（不走 DB 快照 fallback），
    /// DB 中的这两个字段仅作为创建时初始状态记录，commit 后不同步更新。
    /// 这与 Table CAS 中同步更新 `schema_snapshot` 的模式不同。
    async fn commit_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        expected_location: &str,
        new_location: &str,
    ) -> Result<(), StoreError>;

    /// 删除 View（依赖 assets FK cascade 自动删除 view_assets）。
    async fn drop_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<(), StoreError>;

    /// 列出 Namespace 下所有 View。
    async fn list_views(
        &self,
        domain_name: &str,
        namespace_name: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<ViewIdentifier>, StoreError>;

    /// 重命名 View（更新 assets 行 + view_assets 同步）。
    #[allow(clippy::too_many_arguments)]
    async fn rename_view(
        &self,
        source_domain: &str,
        source_namespace: &str,
        source_name: &str,
        dest_domain: &str,
        dest_namespace: &str,
        dest_name: &str,
    ) -> Result<(), StoreError>;

    /// 检查 View 是否存在（查询 assets 表 asset_type='view'）。
    async fn view_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<bool, StoreError>;
}

// CatalogStore marker trait 扩展
pub trait CatalogStore:
    DomainStore
    + NamespaceStore
    + AssetStore
    + TabularStore
    + VersionStore
    + TabularVersionStore
    + CasCommitStore
    + UnifiedQueryStore
    + IcebergStagingStore
    + IcebergRegisterStore
    + IcebergMetricsStore
    + IcebergPurgeStore
    + IcebergTransactionStore
    + IcebergViewStore  // V4.2 新增
    + Send
    + Sync
{}
```

### 4.7 新增 Model 类型

在 `quasar/core/src/models.rs` 中新增：

```rust
/// ViewAsset: View 扩展层（Iceberg-only）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewAsset {
    pub asset_id: Uuid,
    pub view_uuid: Uuid,
    pub location: String,
    pub current_version_id: i32,
    pub metadata_location: String,
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// View: Asset + ViewAsset 联合行投影。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub asset: Asset,
    pub view: ViewAsset,
}

/// ViewIdentifier: View 标识符（REST 响应用）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}
```

设计规则：

- `View` 是 `(Asset, ViewAsset)` 联合行的投影，与 `TabularAsset` 是 `(Asset, TabularAsset)` 联合行投影的模式一致。
- `ViewIdentifier` 语义上与 `TableIdentifier` 相同但类型独立，避免 Table/View 标识混用。

Scan Planning 无 Store trait——所有逻辑在 Adapter 层完成（plan-task token 自包含，Adapter handler 直接读取 TableMetadata）。

---

## 五、Iceberg View REST API 设计

本章按 endpoint 逐条描述 adapter → core → storage 的完整调用逻辑。

> **Handler 签名模式**：所有 View 端点 handler 均接收 `Query<WarehouseQuery>` 查询参数（与 Table 端点对齐），从中提取 `warehouse` 并调用 `validate_warehouse()` 校验。以下各端点描述中省略此通用步骤的细节，统一表述为"校验 warehouse 参数"。

### 5.1 `GET /v1/{prefix}/namespaces/{namespace}/views`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 作为 `domain_name`，`{namespace}` 作为 `namespace_name`。
2. 从 `Query<WarehouseQuery>` 提取 `warehouse`，调用 `validate_warehouse()` 校验。
3. 调用 `IcebergViewStore::list_views(domain_name, namespace_name, offset, limit)`。
4. 将 `Vec<ViewIdentifier>` 映射为 Iceberg `ListViewsResponse`（格式与 `ListTablesResponse` 一致：`{"identifiers": [...]}`）。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `IcebergViewStore` | `list_views(domain_name, namespace_name, offset, limit)` | `prefix`, `namespace`, `offset`, `limit` | `Vec<ViewIdentifier>` |

**Storage 事务边界**：单条只读查询；`JOIN assets` 后按 `domain_name` + `namespace_name` + `asset_type = 'view'` 过滤。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |

---

### 5.2 `POST /v1/{prefix}/namespaces/{namespace}/views`

Create View 请求体为 Iceberg OpenAPI 定义的 `CreateViewRequest`（独立结构，与 Replace View 的 `CommitViewRequest` 不同）。

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 和 `{namespace}`。
2. 校验 warehouse 参数。
3. 校验 Domain 和 Namespace 存在。
4. 校验同一 Namespace 下不存在同名 Table（`AssetStore::asset_exists` 检查 `asset_type = 'table'`）。
5. 将 `CreateViewRequest` 转换为 `ViewCreation`（iceberg crate 类型）：
   - `name` → `ViewCreation.name`（仅用于 REST 层面的 View 标识，写入 `assets` 表的 `name` 字段；**不参与 ViewMetadata JSON 构建**——`from_view_creation()` 内部忽略此字段）
   - `schema` → `ViewCreation.schema`
   - `sql` / `representations` → `ViewCreation.representations`（构建 `SqlViewRepresentation` 列表）
   - `default-namespace` → `ViewCreation.default_namespace`
   - `default-catalog` → `ViewCreation.default_catalog`
   - `properties` → `ViewCreation.properties`
   - `location` → `ViewCreation.location`（若缺失，按 Domain warehouse + namespace + view name 规则生成）
6. **iceberg crate**: `ViewMetadataBuilder::from_view_creation(view_creation)` → `build()` → `ViewMetadataBuildResult` → 获取 `metadata` 和 `changes`。
7. 分配 `view-uuid`：使用 `Uuid::now_v7()` 或覆盖 crate 生成的 UUID。
8. 序列化 `metadata` 为 JSON：`serde_json::to_value(&metadata)`。
9. 生成 `metadata_location`：`metadata/00001-{uuid}.metadata.json`（与 Table 命名规则一致）。
10. 写入 View metadata JSON 到对象存储。
11. 调用 `IcebergViewStore::create_view(...)` 插入 `assets` + `view_assets`。
12. 返回 `GetViewResponse`（`metadata-location` + `metadata` JSON）。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `AssetStore` | `asset_exists(domain_name, namespace_name, view_name)` | `prefix`, `namespace`, `view_name` | `bool`（检查同名 Table） |
| 2 | `IcebergViewStore` | `create_view(domain_name, namespace_name, view_name, view_uuid, location, metadata_location, current_version_id, properties)` | 全部参数 | `View` |

**Storage 事务边界**：插入 `assets`（`asset_type = 'view'`）+ `view_assets` 必须在单个 PostgreSQL 事务内完成；`uq_assets_active_name` 约束保证同名冲突。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |
| View 已存在 | `AlreadyExists` | 409 | `ViewAlreadyExistsException` |
| 同名 Table 已存在 | `AlreadyExists` | 409 | `AlreadyExistsException` |
| 对象存储写入失败 | 内部处理 | 500 | `InternalServerError` |

---

### 5.3 `GET /v1/{prefix}/namespaces/{namespace}/views/{view}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{view}`。
2. 校验 warehouse 参数。
3. 调用 `IcebergViewStore::get_view(domain_name, namespace_name, view_name)`。
4. 从返回的 `View.view.metadata_location` 读取对象存储中的 View metadata JSON。
5. 构建并返回 `GetViewResponse`（`metadata-location` + `metadata` JSON）。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `IcebergViewStore` | `get_view(domain_name, namespace_name, view_name)` | `prefix`, `namespace`, `view` | `View` |

**Storage 事务边界**：单条只读 `JOIN` 查询；必须过滤 `asset_type = 'view'` 和 `deleted_at IS NULL`。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| View 不存在 | `NotFound` | 404 | `NoSuchViewException` |

---

### 5.4 `POST /v1/{prefix}/namespaces/{namespace}/views/{view}`

Replace View 请求体为 `CommitViewRequest`，包含 `requirements` 和 `updates`。

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{view}`。
2. 校验 warehouse 参数。
3. 调用 `IcebergViewStore::get_view(...)` 加载 View 记录，获取 `metadata_location`。
4. 从对象存储读取 View metadata JSON。
5. **iceberg crate**: `serde_json::from_value::<ViewMetadata>()` 解析。
6. **adapter 层**: 校验 `ViewRequirement`（`assert-create` 和 `assert-view-uuid`）：
   - `AssertCreate`: View 不存在时才允许。若 View 已存在（当前 Replace View 端点），此 requirement 失败 → `409 CommitFailedException`。
   - `AssertViewUuid`: `metadata.view_uuid` 必须等于请求中的 UUID → 不匹配返回 `409 CommitFailedException`。
7. **iceberg crate**: `ViewMetadataBuilder::new_from_metadata(metadata)` → 应用 `ViewUpdate`（通过 builder 的 `add_version` / `set_current_version_id` / `set_properties` / `remove_properties` / `set_location` / `assign_uuid` / `add_schema` / `upgrade_format_version` 方法）→ `build()` → 获取新 `metadata`。
8. 序列化新 metadata 为 JSON。
9. 生成新 `metadata_location`：`metadata/00002-{uuid}.metadata.json`（版本号递增）。
10. 写入新 View metadata JSON 到对象存储。
11. 调用 `IcebergViewStore::commit_view(...)` CAS 更新 `metadata_location`。
12. 返回 `GetViewResponse`。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `IcebergViewStore` | `get_view(domain_name, namespace_name, view_name)` | `prefix`, `namespace`, `view` | `View` |
| 2 | `IcebergViewStore` | `commit_view(domain_name, namespace_name, view_name, expected_location, new_location)` | `prefix`, `namespace`, `view`, `expected`, `new` | `()` |

**Storage 事务边界**：CAS 更新 `view_assets.metadata_location`，条件为 `expected_location` 匹配。与 Table commit 的 CAS 模式一致。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| View 不存在 | `NotFound` | 404 | `NoSuchViewException` |
| Requirement 不满足 | adapter 层处理 | 409 | `CommitFailedException` |
| View UUID 不匹配 | adapter 层处理 | 409 | `CommitFailedException` |
| CAS 冲突 | `Conflict` | 409 | `CommitFailedException` |
| 对象存储写入失败 | 内部处理 | 500 | `InternalServerError` |

---

### 5.5 `DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{view}`。
2. 校验 warehouse 参数。
3. 调用 `IcebergViewStore::drop_view(domain_name, namespace_name, view_name)`。
4. 返回 `204 No Content`。

**注意**：V4.2 **不清理对象存储 metadata 文件**（与 Table `purgeRequested=false` 行为一致）。View metadata 文件数量远少于 Table，清理推迟到 V4.3。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `IcebergViewStore` | `drop_view(domain_name, namespace_name, view_name)` | `prefix`, `namespace`, `view` | `()` |

**Storage 事务边界**：`drop_view` 单条 delete；FK cascade 删除 `view_assets` 扩展行。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| View 不存在 | `NotFound` | 404 | `NoSuchViewException` |

---

### 5.6 `HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{view}`。
2. 校验 warehouse 参数。
3. 调用 `IcebergViewStore::view_exists(domain_name, namespace_name, view_name)`。
4. 存在返回 `200 OK`（无 body），不存在返回 `404 NoSuchViewException`（无 body）。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `IcebergViewStore` | `view_exists(domain_name, namespace_name, view_name)` | `prefix`, `namespace`, `view` | `bool` |

**Storage 事务边界**：单条只读查询（`SELECT EXISTS`）。

**错误映射**：`false` 直接映射为 404 `NoSuchViewException`。

---

### 5.7 `POST /v1/{prefix}/views/rename`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`。
2. 校验 warehouse 参数。
3. 解析请求体 `source` 和 `destination`（均包含 namespace + view name）。
4. 校验 destination Namespace 存在（调用 `NamespaceStore::namespace_exists`）。
5. 调用 `IcebergViewStore::get_view(...)` 校验 source 是 active View。
6. 调用 `IcebergViewStore::rename_view(source_domain, source_namespace, source_name, dest_domain, dest_namespace, dest_name)`。
7. 返回 `204 No Content`。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `namespace_exists(dest_domain, dest_namespace)` | `prefix`, `dest_ns` | `bool` |
| 2 | `IcebergViewStore` | `get_view(domain_name, src_namespace, src_view)` | `prefix`, `src_ns`, `src_view` | `View` |
| 3 | `IcebergViewStore` | `rename_view(...)` | 全部参数 | `()` |

**Storage 事务边界**：`rename_view` 单条 `UPDATE`；`uq_assets_active_name` 唯一约束保证目标冲突。

**错误映射**：

| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Destination Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |
| Source View 不存在 | `NotFound` | 404 | `NoSuchViewException` |
| Destination 已存在 | `AlreadyExists` | 409 | `ViewAlreadyExistsException` |

---

### 5.8 View Endpoint -> Store 调用矩阵（汇总）

| Endpoint | Adapter 职责 | Store trait 调用 | Storage 事务边界 |
|----------|--------------|------------------|------------------|
| `GET .../views` | 解析 namespace；映射 ViewIdentifier response | `IcebergViewStore::list_views(...)` | 单条只读 join；过滤 `asset_type='view'` |
| `POST .../views` | 校验 Domain/Namespace/同名 Table；ViewMetadataBuilder 构建初始 metadata；写 object store；返回 GetViewResponse | `AssetStore::asset_exists(...)`（同名检查）→ `IcebergViewStore::create_view(...)` | 单事务 insert `assets` + `view_assets` |
| `GET .../views/{view}` | 读取 catalog pointer；从 object store 读取 metadata JSON；返回 GetViewResponse | `IcebergViewStore::get_view(...)` | 单条只读 join |
| `POST .../views/{view}` | 读取当前 metadata；校验 ViewRequirement；应用 ViewUpdate；写新 metadata file；CAS update catalog pointer | `IcebergViewStore::get_view(...)` → `IcebergViewStore::commit_view(...)` | CAS update `view_assets.metadata_location` |
| `DELETE .../views/{view}` | 删除 catalog 记录；不清理 object store | `IcebergViewStore::drop_view(...)` | 单条 delete；FK cascade 删除 `view_assets` |
| `HEAD .../views/{view}` | 检查 active View 存在性 | `IcebergViewStore::view_exists(...)` | 单条只读 `SELECT EXISTS` |
| `POST .../views/rename` | 校验 source/destination；只允许 active View source | `IcebergViewStore::get_view(...)` → `IcebergViewStore::rename_view(...)` | rename 单条 update；唯一约束保证目标冲突 |

---

## 六、Server-side Scan Planning REST API 设计

### 6.1 路径 Alias 设计

Scan Planning 端点存在 OpenAPI 路径与 Java `ResourcePaths` 常量路径的差异。

**V4.2 采用 Option A**：主路径使用 OpenAPI 路径（包含 `namespaces/{namespace}`），Java 常量路径作为 alias（不含 `namespaces/{namespace}`）。

路由实现：

```rust
// quasar/adapter/src/iceberg/mod.rs (扩展)

pub fn routes() -> Router<Arc<dyn CatalogStore>> {
    Router::new()
        // ... V4.0/V4.1 routes ...
        // View routes
        .route("/iceberg/v1/{prefix}/namespaces/{ns}/views",
            get(view::list_views).post(view::create_view))
        .route("/iceberg/v1/{prefix}/namespaces/{ns}/views/{view}",
            get(view::load_view)
                .post(view::replace_view)
                .delete(view::drop_view)
                .head(view::head_view))
        .route("/iceberg/v1/{prefix}/views/rename",
            post(view::rename_view))
        // ...
        // Scan Planning: OpenAPI 主路径
        .route("/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan",
            post(scan_planning::submit_plan))
        .route("/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan/{plan_id}",
            get(scan_planning::fetch_plan).delete(scan_planning::cancel_plan))
        .route("/iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/tasks",
            post(scan_planning::fetch_tasks))
        // Scan Planning: Java ResourcePaths alias（无 namespace segment）
        .route("/iceberg/v1/{prefix}/tables/{table}/plan",
            post(scan_planning::submit_plan_alias))
        .route("/iceberg/v1/{prefix}/tables/{table}/plan/{plan_id}",
            get(scan_planning::fetch_plan_alias).delete(scan_planning::cancel_plan_alias))
        .route("/iceberg/v1/{prefix}/tables/{table}/tasks",
            post(scan_planning::fetch_tasks_alias))
}
```

**Alias handler 设计**：alias handler 从 `{prefix}` 和 `{table}` 推断 namespace。Iceberg REST Catalog 中，`{prefix}` 映射为 Domain name，`{table}` 为 `TableIdent` 的 URL 编码形式（使用 `` 分隔 namespace 层级，如 `dbtable`）。

> ⚠️ **V4.2 最高技术风险项**：Java client 的 `{table}` 路径参数实际格式必须在实现前确认。若 Java client 仅发送单一 table name（不含 namespace），alias handler 将无法从路径推断 namespace，导致所有 alias 请求 400。

**推断策略（按优先级）**：

1. **首选策略**：解析 `{table}` 路径参数。Iceberg Java `TableIdent.toUrlString()` 使用 `` 分隔 namespace 层级，格式为 `{ns1}{ns2}...{name}`。alias handler 按此规则拆分出 namespace 和 table name。
2. **Fallback 策略**（仅 Fetch Tasks）：Fetch Tasks 是 POST 请求，请求体包含 `plan-task` token。若路径解析失败，token 中已编码 `table-identifier`，可直接从中提取 namespace。
3. **降级策略**：若以上均失败，返回 `400 BadRequestException`。

**C1 验证项（Hard Blocker）**：
- 通过 Wireshark/tcpdump 或 Java client 日志，确认 `ResourcePaths.table(prefix, ident)` 实际生成的 `{table}` 参数格式。
- **验证失败 fallback**：若确认 `{table}` 仅包含单一 table name（不含 namespace 信息），则放弃 alias 路由支持，仅在 `/v1/config` endpoints 中声明 OpenAPI 主路径的 Scan Planning 端点。此 fallback 下，Java client 的 Scan Planning 请求若使用 ResourcePaths 常量路径会 404——需在文档中明确标注此兼容性限制，并评估是否需要 Java client 方配合修改。对"所有 alias 请求返回 400"的方案不予采纳，因为它比直接 404（路由不存在）更不友好。

**多层 namespace 拆分算法**（首选策略适用时）：
- 输入：`{table}` 路径参数，格式 `{ns1}␟{ns2}␟...␟{name}`（`␟` = `\x1F` unit separator）
- 拆分规则：按 `␟` 分割，最后一个元素为 table name，前面所有元素为 namespace 层级
- 示例：`analytics␟metrics␟events` → namespace = `analytics.metrics`（编码为 `["analytics", "metrics"]`），table name = `events`
- 单层 namespace：`db␟events` → namespace = `["db"]`，table name = `events`
- 无 namespace 的单层 table name：`events` → 拆分失败，触发 fallback

### 6.2 `POST .../tables/{table}/plan`（Submit Plan）

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`（从 OpenAPI 路径或 alias 推断）、`{table}`。
2. 校验 warehouse 参数。
3. 校验 Namespace 存在。
4. 校验 Table 存在且 format 为 `iceberg`（调用 `TabularStore::get_tabular_asset`）。
5. 解析 `SubmitPlanRequest` JSON（`snapshot-id`、`filter`、`case-sensitive`、`split-size`、`num-splits`、`columns`、`start-snapshot-id`、`end-snapshot-id`）。若 `snapshot-id` 未提供（客户端可选参数），使用 TableMetadata 的 `current-snapshot-id` 作为默认值。
6. 从对象存储读取 Table metadata JSON（通过 `metadata_location`）。
7. 定位指定 `snapshot-id` 的 Manifests（读取 Manifest List 文件）。若 Table 无 snapshot（新创建的空表），返回 `404 NoSuchSnapshotException`。
8. 根据 `filter`、`split-size`、`columns` 等参数计算 FileScanTask splits。
9. 生成 `plan-id`（UUID）和 `plan-task` token（自包含 base64 encoded JSON）。
10. 返回 `SubmitPlanResponse`（`plan-id` + `plan-task`）。

**Core trait 调用链**：

| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `TabularStore` | `get_tabular_asset(domain_name, namespace_name, "iceberg", table_name)` | `prefix`, `namespace`, `table` | `(Asset, TabularAsset)` |

**Storage 事务边界**：无数据库事务（Scan Planning 无持久化）。

**错误映射**：

| 场景 | HTTP | Iceberg error type |
|------|------|--------------------|
| Namespace 不存在 | 404 | `NoSuchNamespaceException` |
| Table 不存在 | 404 | `NoSuchTableException` |
| Snapshot 不存在 | 404 | `NoSuchSnapshotException` |

---

### 6.3 `GET .../tables/{table}/plan/{plan-id}`（Fetch Plan）

**Adapter 层调用逻辑**：
1. 解析 `plan-id`。
2. 根据 token 中编码的 plan 状态信息（或内存缓存中的 plan 结果）返回 Plan 状态。
3. 状态：`pending`（仍在处理）、`completed`（完成，返回 `plan-task` token）、`failed`（失败，返回错误）。

**V4.2 简化实现**：由于 V4.2 的 `plan-task` token 采用策略 B（自包含，编码完整 FileScanTask 列表），`submit_plan` 在**单次请求内完成**——handler 内部 `await` stream collect，客户端收到 `SubmitPlanResponse` 时 Plan 已完成。因此：

- `fetch_plan` 对已完成的 Plan 返回 `completed` 状态 + 同样的 `plan-task` token。
- 对无效 `plan-id` 返回 `404`。
- 实际实现中 `pending` 状态窗口极短（仅在 stream collect 期间），但协议层面仍需支持。

---

### 6.4 `DELETE .../tables/{table}/plan/{plan-id}`（Cancel Plan）

**Adapter 层调用逻辑**：
1. 解析 `plan-id`。
2. 对于 V4.2 的单次请求内完成实现，Cancel 操作实际上无效（Plan 在 submit 响应返回前已完成）。
3. 返回 `204 No Content`（对已完成 Plan），或 `404`（对无效 `plan-id`）。

---

### 6.5 `POST .../tables/{table}/tasks`（Fetch Tasks）

**Adapter 层调用逻辑**：
1. 接收请求体中的 `plan-task` token。
2. Base64 decode token，解析 JSON 结构。
3. 从 token 中提取 FileScanTask 列表（策略 B：token 编码完整 task 列表）。
4. 返回 `FileScanTask[]`。

**Core trait 调用链**：无 store 调用（token 自包含）。

**错误映射**：

| 场景 | HTTP | Iceberg error type |
|------|------|--------------------|
| Invalid plan-task token | 400 | `BadRequestException` |
| Token 过期或已取消 | 404 | 无特定类型 |

---

## 七、View Commit 与 Metadata 设计

### 7.1 View Requirement 覆盖

V4.2 覆盖 Iceberg 1.10.x View 适用的 2 项 requirement：

| Requirement | 适用范围 | 校验目标 |
|-------------|----------|----------|
| `assert-create` | Table + View 共用 | View 不存在时才允许创建 |
| `assert-view-uuid` | View-only | View UUID 必须匹配请求值 |

校验逻辑在 adapter 层 `view_metadata.rs` 中实现：

```rust
pub fn check_view_requirements(
    requirements: &[ViewRequirement],
    current_metadata: Option<&ViewMetadata>,
) -> Result<(), String> {
    for req in requirements {
        match req {
            ViewRequirement::AssertCreate => {
                // Create View 端点不使用 CommitViewRequest；
                // Replace View 端点中 AssertCreate 表示 "view 不存在时才允许"，即创建新 view。
                // 若 view 已存在（Replace View），AssertCreate 失败。
                if current_metadata.is_some() {
                    return Err("View already exists, cannot assert-create".to_string());
                }
            }
            ViewRequirement::AssertViewUuid { uuid } => {
                let metadata = current_metadata.as_ref()
                    .ok_or("View does not exist, cannot assert-view-uuid")?;
                // iceberg crate 0.9.1 ViewMetadata 访问器为 uuid()（不是 view_uuid()）
                if metadata.uuid() != uuid {
                    return Err(format!(
                        "View UUID mismatch: expected {}, found {}",
                        uuid, metadata.uuid()
                    ));
                }
            }
        }
    }
    Ok(())
}
```

校验失败统一返回 `409 CommitFailedException`。

### 7.2 View Update 覆盖

V4.2 支持 Iceberg 1.10.x 全部 8 项 ViewUpdate（与 iceberg crate 0.9.1 `ViewUpdate` enum 一致）：

| Update | Wire Name | 适用范围 | V4.2 行为 |
|--------|-----------|----------|-----------|
| AssignUuid | `assign-uuid` | 共用 | 分配 View UUID |
| UpgradeFormatVersion | `upgrade-format-version` | 共用 | 升级 View format version（仅 V1） |
| AddSchema | `add-schema` | 共用 | 增加新 Schema |
| SetLocation | `set-location` | 共用 | 更新 View 根路径 |
| SetProperties | `set-properties` | 共用 | 设置 View 属性 |
| RemoveProperties | `remove-properties` | 共用 | 移除 View 属性 |
| AddViewVersion | `add-view-version` | View-only | 添加新的 View Version |
| SetCurrentViewVersion | `set-current-view-version` | View-only | 设置当前 View Version ID |

**实现策略**：所有 8 项 ViewUpdate 通过 `ViewMetadataBuilder` 的 builder 方法应用：

```rust
/// 应用 View requirement 校验 + ViewUpdate 到现有 ViewMetadata。
///
/// 与 Table commit 的差异：
/// - Table 使用 `TableUpdate::apply(builder)`（crate 内置 dispatch）；
/// - ViewUpdate 没有 `apply()` 方法，V4.2 逐项 match 调用 builder。
/// - View requirement 由 adapter 层手动校验（`check_view_requirements`），
///   不使用 crate 内置 `check()`（iceberg crate 0.9.1 不提供 ViewRequirement）。
///
/// Version History 清理由 `ViewMetadataBuilder::build()` 内置处理
/// （自动调用 `expire_versions()`），V4.2 无需自行实现。
pub fn apply_view_commit(
    current_metadata: &serde_json::Value,
    requirements: &[ViewRequirement],
    updates: &[iceberg::catalog::ViewUpdate],
) -> Result<serde_json::Value, String> {
    let metadata: iceberg::spec::ViewMetadata =
        serde_json::from_value(current_metadata.clone())
            .map_err(|e| format!("parse view metadata: {e}"))?;

    // 校验 requirements（adapter 层手动实现，非 crate 内置）
    check_view_requirements(requirements, Some(&metadata))?;

    // 构建 new metadata
    // ViewUpdate 没有 apply() 方法（与 TableUpdate 不同），
    // 必须逐项 match 调用 ViewMetadataBuilder 的对应方法。
    let mut builder = iceberg::spec::ViewMetadataBuilder::new_from_metadata(metadata);

    for update in updates {
        match update {
            iceberg::catalog::ViewUpdate::AssignUuid { uuid } => {
                builder = builder.assign_uuid(*uuid);
            }
            iceberg::catalog::ViewUpdate::UpgradeFormatVersion { format_version } => {
                builder = builder.upgrade_format_version(*format_version)?;
            }
            iceberg::catalog::ViewUpdate::AddSchema { schema, last_column_id } => {
                // last_column_id is optional in the REST API; the builder derives
                // it from the schema if not provided.
                builder = builder.add_schema(schema.clone());
                let _ = last_column_id; // builder handles column id assignment internally
            }
            iceberg::catalog::ViewUpdate::SetLocation { location } => {
                builder = builder.set_location(location.clone());
            }
            iceberg::catalog::ViewUpdate::SetProperties { updates } => {
                builder = builder.set_properties(updates.clone())?;
            }
            iceberg::catalog::ViewUpdate::RemoveProperties { removals } => {
                builder = builder.remove_properties(removals);
            }
            iceberg::catalog::ViewUpdate::AddViewVersion { view_version } => {
                // add_version 仅添加新 version 到 versions 列表，不设为 current。
                // Iceberg REST 客户端通常同时发送 AddViewVersion + SetCurrentViewVersion，
                // 但协议允许仅添加 version 不设为 current（例如历史版本回补场景）。
                builder = builder.add_version(view_version.clone())?;
            }
            iceberg::catalog::ViewUpdate::SetCurrentViewVersion { view_version_id } => {
                // set_current_version_id 将已有 version 设为 current。
                // view_version_id = -1 表示"设为最近添加的 version"（crate 内置 LAST_ADDED 语义）。
                builder = builder.set_current_version_id(*view_version_id)?;
            }
        }
    }

    let result = builder.build().map_err(|e| format!("build view metadata: {e}"))?;
    serde_json::to_value(&result.metadata).map_err(|e| format!("serialize: {e}"))
}
```

V4.2 **无 501 拒绝项**：全部 8 项 ViewUpdate 都通过 `ViewMetadataBuilder` 方法覆盖。若后续 Iceberg 版本新增 ViewUpdate，V4.2 对未知 action 返回 `400 BadRequestException`。

**与 Table commit 的关键差异说明**：

| 维度 | Table | View |
|------|-------|------|
| Update dispatch | `TableUpdate::apply(builder)`（crate 内置） | 逐项 match 调用 builder 方法（ViewUpdate 无 `apply()`） |
| Requirement 校验 | `TableRequirement::check(metadata)`（crate 内置） | adapter 层 `check_view_requirements()` 手动实现 |
| Version History 清理 | 无内置 | `ViewMetadataBuilder::build()` 内置 `expire_versions()` |

### 7.3 View Metadata 文件命名

与 Table metadata 命名规则一致：

- 初始 View metadata：`metadata/00001-{uuid}.metadata.json`。
- Replace 后新 metadata location 单调推进 version。
- 若当前 metadata location 无法解析，返回 `500 InternalServerError`。

### 7.4 View Commit 顺序

1. 读取 catalog 当前 `metadata_location`（通过 `IcebergViewStore::get_view`）。
2. 从对象存储读取 View metadata JSON。
3. 使用 `iceberg` crate 解析/校验。
4. adapter 层校验 ViewRequirement。
5. 通过 `ViewMetadataBuilder` 应用 ViewUpdate，生成新 metadata JSON。
6. 写新 metadata file 到对象存储。
7. PostgreSQL CAS 更新 `view_assets.metadata_location`（通过 `IcebergViewStore::commit_view`）。
8. 返回 `GetViewResponse`。

### 7.5 失败窗口

与 Table commit 的失败窗口一致：

- 对象存储写成功但 PostgreSQL CAS 失败：客户端返回 `409 CommitFailedException`；服务端日志包含 domain、namespace、view、old/new metadata_location。V4.2 容忍孤儿 View metadata file。
- PostgreSQL CAS 成功后响应发送失败：catalog 状态以 PostgreSQL 为准。

### 7.6 View/Table Commit 模式对比

View commit 与 Table commit 采用相同的分层架构（adapter 层校验 + Store 层 CAS），差异仅在类型层面：

| 维度 | Table Commit | View Commit |
|------|-------------|-------------|
| Requirement 类型 | `TableRequirement`（iceberg crate 提供） | `ViewRequirement`（adapter 层自定义） |
| Update 类型 | `TableUpdate`（iceberg crate 提供） | `ViewUpdate`（iceberg crate 提供） |
| Builder | `TableMetadataBuilder` | `ViewMetadataBuilder` |
| Requirement 校验 | `req.check(Some(&metadata))`（crate 内置） | adapter 层手动实现 `check_view_requirements()` |
| 初始构建 | `TableMetadataBuilder::new(schema, spec, sort_order, location, ...)` | `ViewMetadataBuilder::from_view_creation(view_creation)` |
| 增量构建 | `metadata.into_builder(None)` | `ViewMetadataBuilder::new_from_metadata(metadata)` |
| CAS Store 方法 | `CasCommitStore::cas_update_metadata_location(...)` | `IcebergViewStore::commit_view(...)` |
| 失败窗口 | 对象存储写成功 + DB CAS 失败 → 409 | 相同 |
| 孤儿文件 | Table metadata.json | View metadata.json |

**设计意图**：View commit 复用 Table commit 的验证流程（读取→校验→build→写对象存储→CAS），仅需替换类型和 builder。这种对称性降低了认知负担，也便于后续统一提取 commit 框架（若需要）。

**注意**：View 端点**不涉及 staged-create 流程**。Iceberg 1.10.x REST Catalog 中 View 没有 staged-create 语义，Create View 直接创建 active View 记录。

---

## 八、Scan Planning 执行设计

### 8.1 Plan Task Token 策略

V4.2 采用 **策略 B**：`plan-task` token 编码完整 FileScanTask 列表。

理由：
1. V4.2 是**单次请求内完成**的实现（`submit_plan` handler 内部 `await` FileScanTaskStream 并 collect 为 Vec，对客户端仍是同步响应），无需跨请求状态管理。
2. 内存缓存方案（策略 C）引入服务端状态，增加实现复杂度，与"无持久化"原则偏离。
3. 重新计算方案（策略 A）在 `/tasks` 端点重复执行 scan planning，性能开销不合理。

**Token 结构**：

```json
{
  "plan-id": "uuid-string",
  "table-id": {
    "namespace": ["namespace"],
    "name": "table-name"
  },
  "snapshot-id": 123456789,
  "filter": null,
  "split-size": 128000000,
  "tasks": [
    {
      "data-file": {
        "file-path": "s3://bucket/data/file.parquet",
        "file-format": "PARQUET",
        "partition": {},
        "record-count": 100000,
        "file-size-in-bytes": 5000000
      },
      "delete-files": [],
      "start": 0,
      "length": 5000000
    }
  ]
}
```

**FileScanTask DTO 解耦**：iceberg crate 内部的 `FileScanTask` 包含 `deletes: Vec<FileScanTaskDeleteFile>` 等 REST 响应不需要的字段。V4.2 在 adapter 层定义 REST 专用的 `FileScanTask` DTO，与 crate 内部结构解耦。DTO 结构与 Iceberg REST OpenAPI 的 `FileScanTask` schema 对齐，使用嵌套 `data-file` 字段（而非扁平化的 file 字段），并包含可选的 `delete-files` 列表：

```rust
/// REST API DataFile（与 Iceberg REST OpenAPI 对齐）。
#[derive(Serialize, Deserialize)]
pub struct DataFile {
    #[serde(rename = "file-path")]
    pub file_path: String,
    #[serde(rename = "file-format")]
    pub file_format: String,
    pub partition: serde_json::Value,
    #[serde(rename = "record-count")]
    pub record_count: i64,
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
    #[serde(rename = "column-masks", skip_serializing_if = "Option::is_none")]
    pub column_masks: Option<serde_json::Value>,
}

/// REST API FileScanTask（与 Iceberg REST OpenAPI 对齐）。
/// OpenAPI 定义：FileScanTask = { data-file: DataFile, delete-files: [DataFile], start: int64, length: int64 }
/// V4.2 初始实现中 delete-files 为空列表（Positional delete file support 推迟到 V4.3）。
#[derive(Serialize, Deserialize)]
pub struct FileScanTask {
    #[serde(rename = "data-file")]
    pub data_file: DataFile,
    #[serde(rename = "delete-files", skip_serializing_if = "Vec::is_empty", default)]
    pub delete_files: Vec<DataFile>,
    pub start: i64,
    pub length: i64,
}
```

转换逻辑：`submit_plan` 中从 crate 的 `FileScanTaskStream` 收集后，将每个 task 的字段映射到 REST DTO，再编码进 token。`fetch_tasks` 端点直接反序列化 token 中的 DTO 列表返回。

**编码方式**：JSON → base64（URL-safe alphabet）。

**大小限制**：若 FileScanTask 列表过大导致 token 超过合理大小（如 > 1MB），V4.2 返回 `500 InternalServerError` 并建议客户端调整 `split-size` 参数减少 task 数量。此场景在 V4.2 初期实现中不优先处理，V4.3 可评估分页式 task 返回。

### 8.2 FileScanTask 计算逻辑

V4.2 的 Scan Planning 执行流程：

1. 读取 TableMetadata JSON（从对象存储）。
2. 定位指定 `snapshot-id` 的 Manifest List。
3. 读取 Manifest List 中的 Manifest 文件。
4. 根据 `filter` 参数筛选 Data File（V4.2 仅支持简单 filter 或无 filter）。
5. 根据 `split-size` 参数将 Data File 拆分为 FileScanTask。
6. 根据 `columns` 参数选择需要读取的列（V4.2 不裁剪列，返回完整 file 信息）。
7. **`await` + collect**：调用 `scan.plan_files().await` 获取 `FileScanTaskStream`，使用 `stream.try_collect::<Vec<_>>().await` 收集为 Vec。
8. 将收集到的 `Vec<FileScanTask>` 转换为 REST DTO 结构（§8.1 DTO 解耦），编码为 `plan-task` token。

**V4.2 简化限制**：

- `filter` 参数：V4.2 仅支持无 filter（`null`）或 Iceberg REST API 的 filter JSON 结构。对复杂 filter 不做 Manifest-level 过滤优化，返回全量 Data File。
- `split-size` 参数：V4.2 按 `split-size` 对 Data File 进行简单切分（单个大文件按 size 切分，小文件合并）。
- `num-splits` 参数：优先级低于 `split-size`，作为参考值。
- `columns` 参数：V4.2 不裁剪列，返回完整 file schema。

### 8.3 Manifest 文件读取

Scan Planning 需要读取 Manifest List 和 Manifest 文件。

V4.2 采用 **直接读取对象存储** 方案：

1. 从 TableMetadata 的 `snapshot.manifest-list` 路径读取 Manifest List（Avro 格式）。
2. 从 Manifest List 中的 `manifest_path` 读取每个 Manifest 文件（Avro 格式）。
3. 解析 Manifest 文件获取 Data File 信息。

**iceberg crate 能力**：`iceberg` crate 0.9.1 提供 `FileScanTask` 类型和 Manifest 读取能力。V4.2 优先使用 crate 的 `Table.scan()` / `plan_files()` 方法；若 crate 不提供 REST-style 参数化 scan，在 adapter 层补充 wrapper。

---

## 九、错误处理设计（增量）

### 9.1 新增 Iceberg 错误类型

在 `quasar/adapter/src/iceberg/error.rs` 中新增：

| 场景 | HTTP | Iceberg error type | message 模板 |
|------|------|--------------------|--------------|
| View 不存在 | 404 | `NoSuchViewException` | `View does not exist: {namespace}.{view}` |
| View 已存在 | 409 | `ViewAlreadyExistsException` | `View already exists: {namespace}.{view}` |
| 同名 Table 已存在 | 409 | `AlreadyExistsException` | `Table with same name already exists: {namespace}.{name}` |
| Snapshot 不存在 | 404 | `NoSuchSnapshotException` | `Snapshot does not exist: {snapshot-id}` |

```rust
// IcebergError enum 扩展（在现有 enum 上增加 variant）
pub enum IcebergError {
    // V4.0/V4.1 已有：
    //   NoSuchNamespaceException, NamespaceAlreadyExistsException,
    //   NoSuchTableException, TableAlreadyExistsException,
    //   BadRequestException, CommitFailedException, ...

    // V4.2 新增
    NoSuchViewException { message: String },
    ViewAlreadyExistsException { message: String },
    /// Generic AlreadyExistsException for cross-resource name conflicts
    /// (e.g., creating a View when a Table with the same name exists).
    AlreadyExistsException { message: String },
    NoSuchSnapshotException { message: String },
}
```

**说明**：
- `AlreadyExistsException` 是 V4.2 **新增**的通用 variant。现有 `error.rs` 中只有 `TableAlreadyExistsException` 和 `NamespaceAlreadyExistsException`，没有跨资源通用的 `AlreadyExistsException`。创建 View 时若同名 Table 已存在，需返回此通用类型（与 Iceberg REST spec 对齐）。
- `NoSuchSnapshotException`：Iceberg REST OpenAPI 中 snapshot 不存在的标准错误类型。若 spec 未定义此类型，降级为 `NoSuchTableException` 并附加 snapshot-id 信息。

### 9.2 Store error 到 View error 映射

```rust
pub fn store_error_to_iceberg_view(err: StoreError) -> IcebergError {
    match err {
        StoreError::NotFound(msg) => IcebergError::NoSuchViewException { message: msg },
        StoreError::AlreadyExists(msg) => IcebergError::ViewAlreadyExistsException { message: msg },
        StoreError::NamespaceNotEmpty { namespace } => IcebergError::CommitFailedException {
            message: format!("namespace '{}' is not empty", namespace),
        },
        StoreError::DomainNotEmpty { domain } => IcebergError::CommitFailedException {
            message: format!("domain '{}' is not empty", domain),
        },
        StoreError::Conflict { msg } => IcebergError::CommitFailedException { message: msg },
        StoreError::InvalidInput(msg) => IcebergError::BadRequestException { message: msg },
        StoreError::DatabaseUnavailable { .. } => IcebergError::ServiceUnavailableException {
            message: "service temporarily unavailable".to_string(),
        },
        StoreError::Timeout { operation } => IcebergError::TimeoutException {
            message: format!("operation '{}' timed out", operation),
        },
        StoreError::Internal { msg, source } => {
            tracing::error!(error = ?source, %msg, "internal store error");
            IcebergError::InternalServerError {
                message: "An internal error occurred".to_string(),
            }
        }
    }
}
```

**说明**：此映射与 `store_error_to_iceberg_table()` 的结构完全对齐，仅将 `NoSuchTableException` 替换为 `NoSuchViewException`、`TableAlreadyExistsException` 替换为 `ViewAlreadyExistsException`。新增的 `NamespaceNotEmpty` / `DomainNotEmpty` / `DatabaseUnavailable` / `Timeout` 分支补充了原文档遗漏的 StoreError variant 处理。

### 9.3 脱敏规则继承

继承 V4.0/V4.1 脱敏规则：
- 客户端错误不包含数据库 SQL、S3 secret、连接串。
- View commit 失败日志包含 domain、namespace、view、metadata_location，不含 credential。
- Scan Planning 错误不暴露 Manifest 文件内部路径的 credential 信息。

---

## 十、测试策略

### 10.1 Adapter 集成测试新增

| 测试组 | 测试名称 | 覆盖范围 |
|--------|----------|----------|
| View create | `test_create_view_success` | 创建 View，返回完整 ViewMetadata |
| View create | `test_create_view_same_name_table_conflict` | 同名 Table 已存在 → 409 |
| View create | `test_create_view_duplicate_conflict` | 同名 View 已存在 → 409 |
| View load | `test_load_view_success` | 加载 View，metadata-location 一致 |
| View load | `test_load_view_not_found` | View 不存在 → 404 |
| View replace | `test_replace_view_version_increment` | Replace View，current-version-id 递增 |
| View replace | `test_replace_view_uuid_assert` | AssertViewUuid 成功/失败路径 |
| View replace | `test_replace_view_cas_conflict` | 并发 Replace CAS 冲突 |
| View drop | `test_drop_view_success` | Drop View，PostgreSQL 记录删除 |
| View drop | `test_drop_view_not_found` | View 不存在 → 404 |
| View head | `test_head_view_exists` | 存在 → 200 |
| View head | `test_head_view_not_found` | 不存在 → 404 |
| View list | `test_list_views_success` | 返回正确 View 名称列表 |
| View list | `test_list_views_empty_namespace` | 空 Namespace → 空 identifiers |
| View rename | `test_rename_view_same_namespace` | 同 Namespace rename 成功 |
| View rename | `test_rename_view_cross_namespace` | 跨 Namespace rename 成功 |
| View rename | `test_rename_view_source_not_found` | Source 不存在 → 404 |
| View rename | `test_rename_view_dest_conflict` | Destination 已存在 → 409 |
| View commit | `test_view_commit_all_updates` | 8 项 ViewUpdate 成功路径 |
| View commit | `test_view_commit_assert_create_on_existing` | 已存在 View + assert-create → 409 |
| View commit | `test_create_view_auto_location` | Create View 时 location 缺失，由服务端按规则自动生成 |
| View commit | `test_replace_view_metadata_location_increment` | Replace View 后 metadata location 版本号递增 |
| View commit | `test_view_commit_empty_requirements` | requirements 为空数组 → 直接通过 |
| View list | `test_list_views_pagination` | offset/limit 分页边界验证 |
| Scan Planning | `test_submit_plan_success` | Submit Plan 成功，返回 plan-id 和 plan-task |
| Scan Planning | `test_fetch_tasks_success` | Fetch Tasks 成功，返回 FileScanTask[] |
| Scan Planning | `test_cancel_plan_success` | Cancel Plan 成功，已取消 Plan 返回 404 |
| Scan Planning | `test_submit_plan_table_not_found` | Table 不存在 → 404 |
| Scan Planning | `test_submit_plan_namespace_not_found` | Namespace 不存在 → 404 |
| Scan Planning | `test_fetch_tasks_invalid_token` | Invalid token → 400 |
| Scan Planning | `test_plan_path_alias` | Java ResourcePaths alias 路径测试 |
| Scan Planning | `test_split_size_parameter` | Split size 参数正确控制 task 粒度 |

### 10.2 View Metadata 兼容测试

必须至少有一个测试由 Spark Iceberg 1.10.x client 直接读取 Quasar 写出的 View metadata file，并验证：

- `view-uuid`
- `current-version-id`
- `representations`（含 `type: "sql"` discriminator）
- `schemas`
- `metadata-location`

### 10.3 并发测试

- View CAS 冲突：两个并行 Replace View 只有一个成功。
- 使用 `testcontainers-postgres` 验证真实并发。

### 10.4 Spark E2E（可选）

Scan Planning E2E 验证为**可选项**（Spark 默认不调用 REST scan planning 端点），但 **View E2E 为必须项**。

- `CREATE VIEW`
- `SELECT FROM VIEW`
- `DROP VIEW`

### 10.5 TEST_MATRIX 更新

新增 V4.2 测试条目，任何测试新增或修改后必须同步更新 `docs/TEST_MATRIX.md`。

---

## 十一、实现顺序

### 11.1 C1: 依赖与 View 类型基线

- 验证 §3.1 全部 5 项 iceberg crate View 能力。
  - **Hard blocker**：若 ViewMetadataBuilder 能力不足，评估 adapter wrapper 补充策略。
- **验证 Scan Planning 的 iceberg crate 能力**：确认 `Table.scan().plan_files().await` 可以收集 `FileScanTaskStream`；确认 `FileScanTask` 的字段结构。
- **验证 Java client 的 `{table}` 路径参数格式**：通过 Wireshark/tcpdump 或 Java client 日志，确认 `ResourcePaths.table(prefix, ident)` 实际生成的 `{table}` 参数格式（是否包含 namespace 分隔符 ``）。
  - **Hard blocker**：若 `{table}` 仅包含单一 table name 而不含 namespace，需重新评估 alias 路径方案。
- 定义 `ViewRequirement` adapter enum（iceberg crate 0.9.1 不提供此类型）。
- 建立 View metadata parse/build/serialize wrapper（`view_metadata.rs`）。
- 定义 View DTO（`CreateViewRequest`、`CommitViewRequest`、`GetViewResponse`、`RenameViewRequest` 等）。

### 11.2 C2: 数据模型与 Store 增量

- 增加 `asset_types 'view'` 注册和 `view_assets` 表 SQL。
- 增加 `ViewAsset`、`View`、`ViewIdentifier` model 类型。
- 增加 `IcebergViewStore` trait 及 `PgCatalogStore` 实现。
- 增加 `IcebergViewStore` 到 `CatalogStore` marker trait。
- 增加 `view_exists`、`create_view`、`get_view`、`drop_view`、`list_views`、`rename_view`、`commit_view` SQL 常量和实现。
- 增加 View error 映射函数 `store_error_to_iceberg_view`。
- 增加 `NoSuchViewException`、`ViewAlreadyExistsException`、`AlreadyExistsException`、`NoSuchSnapshotException` 到 `IcebergError` enum。

### 11.3 C3: View 生命周期端点

- 实现 7 个 View 端点 handler（create / load / replace / drop / head / list / rename）。
- 实现 `CreateViewRequest` → `ViewCreation` → `ViewMetadataBuilder` 转换逻辑。
- 实现 View commit（Replace View）：ViewRequirement 校验 + ViewUpdate 应用 + CAS update。
- `/v1/config` endpoints 字段添加 7 个 View 端点声明。

### 11.4 C4: Scan Planning 端点

- 定义 Scan Planning DTO（`SubmitPlanRequest`、`SubmitPlanResponse`、`FetchPlanResultResponse`、`FileScanTask` 等）。
- 实现 `plan-task` token 编码/解码。
- 实现 4 个 Scan Planning 端点 handler + 4 个 alias handler。
- 实现路径 alias 路由和 namespace 推断逻辑。
- `/v1/config` endpoints 字段添加 4 个 Scan Planning 端点声明。

### 11.5 C5: 测试与文档

- View 端点集成测试（§10.1 View 测试矩阵）。
- Scan Planning 端点集成测试（§10.1 Scan Planning 测试矩阵）。
- View CAS 并发冲突测试。
- Spark View E2E smoke（`CREATE VIEW` / `SELECT FROM VIEW` / `DROP VIEW`）。
- 更新 `docs/TEST_MATRIX.md`。
- 更新 `docs/v4/PROGRESS.md`。
- 更新 `docs/v4/V4_OFFICIAL_REST_API.md` V4.2 范围列。
- 如实现偏离本文档，先修订本文档再进入提交。

---

## 十二、V4.2 不做什么

V4.2 不设计、不实现以下能力：

1. `POST .../register-view` — 不属于 Iceberg 1.10.x 官方端点集合。
2. View Version 历史回滚 — 仅支持当前 version 加载和 replace。
3. Scan Planning 结果持久化 — Plan 结果仅在请求会话内有效。
4. Scan Planning 调度优化 — 不引入任务队列、并行度控制、资源限制。
5. View/Table 跨资源引用验证 — View SQL 中引用 Table 存在性验证由客户端负责。
6. View metadata 文件对象存储清理 — Drop View 不清理 metadata 文件，推迟到 V4.3。
7. Metadata cache — 推迟到 V4.3。
8. Cursor pagination — 推迟到 V4.3。
9. Orphan cleanup — 推迟到 V4.3。
10. Vended credentials / S3 Signer / Encryption key — 推迟到后续安全专项版本。

---

## 十三、修订记录

### V1.1 (2026-06-08)

- §7.1 `check_view_requirements()` 修正：
  - `metadata.view_uuid()` → `metadata.uuid()`（iceberg crate 0.9.1 ViewMetadata 访问器为 `uuid()`，不是 `view_uuid()`）。
- §7.2 `apply_view_commit()` 修正与增强：
  - 补充与 Table commit 的差异说明表格（ViewUpdate 没有 `apply()` 方法、ViewRequirement 由 adapter 手动实现、Version History 清理由 crate 内置处理）。
  - `AddViewVersion` 分支添加注释：`add_version` 仅添加 version 不设为 current，通常伴随 `SetCurrentViewVersion`。
  - `SetCurrentViewVersion` 分支添加注释：`view_version_id = -1` 表示"设为最近添加的 version"。
  - 添加函数级注释说明 ViewUpdate 无 `apply()` 方法和 Version History 由 `build()` 内置清理。
- §5.2 Create View 步骤 5 补充 `ViewCreation.name` 字段说明：
  - 明确标注 `name` 仅用于 REST 层面标识（写入 `assets` 表），不参与 ViewMetadata JSON 构建（`from_view_creation()` 内部忽略此字段）。
- §6.1 Scan Planning alias fallback 策略修正：
  - C1 验证项的 fallback 从模糊表述改为明确决策：若 `{table}` 不含 namespace 信息，放弃 alias 路由支持，仅在 `/v1/config` 声明 OpenAPI 主路径。不采纳"所有 alias 请求返回 400"方案。
  - 补充多层 namespace 拆分算法说明（`␟` 分隔符拆分规则）。
- §4.3 `view_assets` 表设计决策明确化：
  - `current_version_id` 和 `properties` 的快照性质从模糊表述改为明确决策：`commit_view` **不更新**这两个字段。补充与 Table CAS 同步 `schema_snapshot` 的差异对比说明。
- §4.6 `IcebergViewStore: AssetStore` 实现约束说明：
  - 明确 `create_view` 和 `rename_view` 必须使用自定义 SQL 在单个 PostgreSQL 事务内完成双行操作，不能拆分为先调用 `AssetStore` 方法再单独插入 `view_assets`。
- §4.6 `commit_view` 方法注释增强：
  - 明确标注此方法不更新 `view_assets.current_version_id` 和 `view_assets.properties`，与 Table CAS 同步 `schema_snapshot` 的模式不同。
- §9.2 `store_error_to_iceberg_view` 映射补全：
  - 补充遗漏的 `NamespaceNotEmpty` → `CommitFailedException`、`DomainNotEmpty` → `CommitFailedException`、`DatabaseUnavailable` → `ServiceUnavailableException`、`Timeout` → `TimeoutException` 分支。
  - 添加说明：此映射与 `store_error_to_iceberg_table()` 结构对齐。
- §5.7 Rename View 补充 destination Namespace 存在性校验：
  - 步骤 4 新增 `NamespaceStore::namespace_exists` 校验 destination Namespace 存在。
  - Core trait 调用链增加 `NamespaceStore` 步骤。
  - 错误映射增加 "Destination Namespace 不存在 → 404 NoSuchNamespaceException"。
- §6.2 Submit Plan 补充 `snapshot-id` fallback：
  - 明确说明若 `snapshot-id` 未提供，使用 TableMetadata 的 `current-snapshot-id` 作为默认值。
  - 明确说明若 Table 无 snapshot，返回 `404 NoSuchSnapshotException`。
- §8.1 FileScanTask DTO 结构修正（与 Iceberg REST OpenAPI 对齐）：
  - 从扁平化的 `file_path` / `file_format` 等字段改为嵌套 `data-file: DataFile` + `delete-files: [DataFile]` + `start` / `length` 结构。
  - 新增 `DataFile` DTO（含 `file-path`、`file-format`、`partition`、`record-count`、`file-size-in-bytes`、可选 `column-masks`）。
  - Token 结构示例同步更新为嵌套格式。
  - `delete-files` V4.2 初始为空列表，Positional delete file support 推迟到 V4.3。
- §2.3 `/v1/config` endpoints 补充说明：
  - Scan Planning Java ResourcePaths alias 路径不在 `/v1/config` endpoints 中声明（Iceberg `Endpoint` enum 只定义 OpenAPI 路径版本）。
  - Create View 和 Replace View 分别列出的说明。
- 同步更新文档头部版本号为 V1.1。

### V1.0 (2026-06-06)

- 初始 V4.2 设计基线。
- 设计 Iceberg View 生命周期 7 端点：CreateViewRequest → ViewCreation → ViewMetadataBuilder 构建与 CAS commit。
- 设计 View Requirement/Update 覆盖：2 项 requirement（adapter 层定义 ViewRequirement enum）+ 8 项 update（使用 iceberg crate ViewUpdate enum）。
- 设计 `view_assets` 扩展表（Iceberg-only，无 format 字段）和 `View` / `ViewAsset` / `ViewIdentifier` model 类型。
- 设计 `IcebergViewStore` trait（super-trait: `AssetStore`），7 个方法。
- 设计 Server-side Scan Planning 4 端点 + Java ResourcePaths alias 路由。
- 设计 plan-task token 策略 B（自包含编码完整 FileScanTask 列表）。
- 设计 Scan Planning 无持久化、同步计算实现。
- 明确 V4.2 不做什么（register-view、历史回滚、持久化、调度优化等）。