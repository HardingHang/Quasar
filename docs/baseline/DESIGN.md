# Quasar 项目级设计说明书

> **项目**：Quasar —— 面向数据与 AI 系统的统一元数据平面
> **状态**：已确认的设计基线
> **日期**：2026-07-08
> **范围**：项目级、与版本无关；不绑定任何里程碑（如 V4.2）

本文档承接 `docs/baseline/REQUIREMENTS.md` 的实现设计，是设计层面的唯一权威基线。

---

## 1. 架构总览

### 1.1 系统组件

```
┌──────────────────────────────────────────────────────────────┐
│                        quasar-server                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ /iceberg/v1 │  │ /lance/v1   │  │ /unified/v1         │  │
│  │  (adapter)  │  │  (adapter)  │  │  (adapter)          │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         └────────────────┴────────────────────┘              │
│                          │                                   │
│                    Router State                              │
│              Arc<dyn CatalogStore>                           │
└──────────────────────────┬───────────────────────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
┌───────┴───────┐                     ┌───────┴───────┐
│ quasar-adapter│                     │ quasar-storage│
│ (协议适配层)   │                     │ (存储层实现)   │
└───────┬───────┘                     └───────┬───────┘
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                    ┌──────┴──────┐
                    │ quasar-core │
                    │(traits/models)│
                    └──────┬──────┘
                           │
                    PostgreSQL + object store (S3/MinIO/...)
```

### 1.2 设计原则

1. **官方协议优先**：REST 路径、请求/响应字段和错误模型对齐 Iceberg 1.11.x / Lance REST Namespace 规范。
2. **声明即实现**：`/iceberg/v1/config` 的 `endpoints` 只返回实际已实现端点。
3. **元数据兼容优先**：Quasar 写出的 Iceberg metadata.json 能被 Spark Iceberg 1.11.x client 读回。
4. **Catalog 状态由 PostgreSQL 仲裁**：元数据指针与版本顺序由数据库条件更新保证。
5. **对象存储失败可诊断**：容忍对象存储与 PostgreSQL 的非原子窗口，但必须返回明确错误并记录路径上下文。
6. **身份与扩展分离**：`assets` 统一身份层，类型特有字段进入扩展表或 JSONB。
7. **端点级隔离**：格式由 REST 端点路径隐含；Domain 不绑定格式。

---

## 2. Crate 分层与依赖

### 2.1 Workspace 结构

```
quasar/
├── core/          # 核心领域模型与 trait 定义
│                  # - Domain / Namespace / Asset / Version / AssetType / Format
│                  # - StoreError、PatchField、validation
│                  # - 不依赖任何具体框架或存储实现
│
├── storage/       # 存储层实现
│                  # - PostgreSQL 实现 CatalogStore 全部 trait
│                  # - 连接池管理（deadpool-postgres）
│                  # - schema migrations（幂等初始化）
│                  # - queries.rs（SQL 常量集中管理）
│
├── adapter/       # 协议适配层
│   ├── iceberg/   # Iceberg REST Catalog 适配
│   ├── lance/     # Lance REST Namespace 适配
│   └── unified/   # Unified API 适配
│
└── server/        # 服务入口
                   # - axum 路由注册与中间件
                   # - 配置加载（环境变量）
                   # - Health / Readiness 端点
                   # - 优雅关机
```

### 2.2 依赖关系

```
server ──► adapter ──► core
  │                      ▲
  └──► storage ──────────┘
```

- `core` 不依赖任何具体框架或存储实现。
- `storage` 依赖 `core`，实现 `CatalogStore` trait。
- `adapter` 依赖 `core` + `iceberg` crate + `object_store`；通过 trait 操作存储。
- `server` 依赖 `adapter`、`storage`、`core`；创建 `PgCatalogStore` 并注入 handler。

### 2.3 关键依赖版本

| 依赖 | 版本 | 用途 |
|------|------|------|
| axum | 0.8 | Web 框架 |
| tokio | 1 (full) | 异步运行时 |
| tokio-postgres | 0.7 | PostgreSQL 异步驱动 |
| deadpool-postgres | 0.14 | 连接池 |
| object_store | 0.12 (aws feature) | S3/MinIO 对象存储 |
| iceberg | 0.9.1 | Iceberg metadata 类型与工具 |
| serde / serde_json | 1 | 序列化 |
| uuid | 1 (v4, serde) | UUID 生成 |
| chrono | 0.4 (serde) | 时间类型 |
| thiserror | 2 | 错误派生 |
| tracing | 0.1 | 结构化日志 |
| tower-http | 0.6 (trace) | 请求日志中间件 |

> workspace clippy lint：`unwrap_used = "deny"`、`expect_used = "deny"`。

---

## 3. 物理数据模型

### 3.1 Schema 概览

```
注册表:  asset_types | formats
第一层:  domains
第二层:  namespaces ──(domain_id FK)──► domains
第三层:  assets ──(namespace_id FK)──► namespaces
扩展层:  tabular_assets | view_assets | model_assets | ... ──(asset_id)──► assets
版本层:  asset_versions ──(asset_id)──► assets
标签层:  asset_tags ──(asset_id)──► assets
迁移层:  schema_migrations
```

### 3.2 表结构

#### `asset_types`

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | TEXT PK | 唯一标识，如 `table`、`model`。 |
| `description` | TEXT | 描述。 |
| `category` | TEXT | `tabular`、`view`、`model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic`、`generic`。 |
| `validation_schema` | JSONB | 可选 JSON Schema。 |
| `extension_strategy` | TEXT | `jsonb` / `dedicated_table` / `reference_only`。 |
| `supports_native_protocol` | BOOL | 是否预期有原生协议。 |

#### `formats`

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | TEXT PK | 唯一标识，如 `iceberg`、`onnx`。 |
| `description` | TEXT | 描述。 |
| `mime_type` | TEXT | 可选。 |
| `serialization_hint` | TEXT | 可选，如 `json`、`protobuf`。 |

#### `domains`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | `gen_random_uuid()`。 |
| `name` | TEXT UNIQUE | 全局唯一，用于 API 路径；命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。 |
| `comment` | TEXT | 描述。 |
| `properties` | JSONB | 自定义属性。 |
| `storage_type` | TEXT | s3 / minio / hdfs / local。 |
| `storage_config` | JSONB | 存储配置，不得明文存 credential。 |
| `warehouse` | TEXT | 默认 warehouse 根路径。 |
| `created_at` / `updated_at` | TIMESTAMPTZ | 时间戳。 |

#### `namespaces`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `domain_id` | UUID FK→domains | `ON DELETE RESTRICT`。 |
| `path` | TEXT | 层级路径，如 `analytics/teams/finance`；路径段命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。 |
| `depth` | INT | 路径深度。 |
| `comment` | TEXT | 描述。 |
| `properties` | JSONB | 自定义属性。 |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |
| | `UNIQUE(domain_id, path)` | 约束。 |

#### `assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | 治理能力统一引用该 ID。 |
| `namespace_id` | UUID FK→namespaces | `ON DELETE RESTRICT`。 |
| `name` | TEXT | 同一 Namespace 内活动资产名唯一；命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。 |
| `asset_type` | TEXT FK→asset_types(name) | 资产类型。 |
| `format` | TEXT FK→formats(name) | 可选格式。 |
| `comment` | TEXT | 描述。 |
| `properties` | JSONB | 治理/管理面属性。 |
| `current_version_key` | TEXT | 当前版本的原生版本标识，对应 `asset_versions.version_key`；通过 `asset_id = id AND version_key = current_version_key` 可 join 到 `asset_versions`。 |
| `deleted_at` | TIMESTAMPTZ | 软删除时间，NULL 表示活动。 |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |

**索引**：
- `uq_assets_active_name`：partial unique on `(namespace_id, name) WHERE deleted_at IS NULL`。
- `idx_assets_type_active`：partial index on `asset_type WHERE deleted_at IS NULL`。

#### `asset_versions`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `asset_id` | UUID FK→assets | `ON DELETE CASCADE`。 |
| `version_key` | TEXT | 格式原生版本标识。 |
| `version_properties` | JSONB | 版本属性。记录与格式无关的通用信息，如 commit message、作者、操作类型、自定义属性。不是 `content_pointer` 指向的格式原生元数据文件。 |
| `content_inline` | JSONB | 可选少量内联内容（如小 JSON/YAML 配置）。实际工件文件通过 `content_pointer` 指向外部对象存储。 |
| `content_pointer` | TEXT | 可选外部指针，指向对象存储中的实际工件文件。 |
| `previous_version_id` | UUID FK→asset_versions | `ON DELETE RESTRICT`；防止删除被后继版本引用的版本，保护版本链完整性。 |
| `created_at` | TIMESTAMPTZ | — |

**约束**：
- `UNIQUE(asset_id, version_key)`。
- partial unique `uq_asset_versions_root` on `(asset_id) WHERE previous_version_id IS NULL`；保证每个资产至多一个根版本。

#### `asset_tags`

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID FK→assets | `ON DELETE CASCADE`。 |
| `tag` | TEXT | — |
| `created_at` | TIMESTAMPTZ | — |
| | `PRIMARY KEY(asset_id, tag)` | — |

### 3.3 类型特定扩展表示例

#### `tabular_assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；应用层校验 `asset_type='table'`。 |
| `location` | TEXT | 表根路径。 |
| `metadata_location` | TEXT | 当前 metadata.json 指针。 |
| `schema_snapshot` | JSONB | 当前 schema 缓存。 |

#### `view_assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；应用层校验 `asset_type='view'`。 |
| `view_uuid` | UUID | Iceberg view uuid。 |
| `location` | TEXT | — |
| `metadata_location` | TEXT | — |

#### 其他资产类型的扩展表

- 基线阶段仅内置 `table` 和 `view` 两种资产类型，对应 `tabular_assets` 和 `view_assets` 扩展表。
- `model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic` 等类型不预置，后续通过 Unified API 注册后，按 `dedicated_table` 扩展策略创建对应扩展表。
- 具体扩展表 Schema 在实际设计该资产类型接入时确定。

### 3.4 数据库 Schema DDL

以下为与 §3.2 / §3.3 表结构对应的完整建表语句，作为迁移脚本（见 §8.4）的实现依据：

```sql
-- 注册表
CREATE TABLE asset_types (
    name TEXT PRIMARY KEY,
    description TEXT,
    category TEXT NOT NULL CHECK (category IN ('tabular', 'view', 'model', 'agent', 'tool', 'mcp_server', 'fileset', 'topic', 'generic')),
    validation_schema JSONB,
    extension_strategy TEXT NOT NULL CHECK (extension_strategy IN ('jsonb', 'dedicated_table', 'reference_only')),
    supports_native_protocol BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE formats (
    name TEXT PRIMARY KEY,
    description TEXT,
    mime_type TEXT,
    serialization_hint TEXT
);

-- 第一层：Domain
CREATE TABLE domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB,
    storage_type TEXT CHECK (storage_type IN ('s3', 'minio', 'hdfs', 'local')),
    storage_config JSONB,
    warehouse TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 第二层：Namespace
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    path TEXT NOT NULL,
    depth INTEGER NOT NULL,
    comment TEXT,
    properties JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(domain_id, path)
);

-- 第三层：Asset
CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL REFERENCES asset_types(name),
    format TEXT REFERENCES formats(name),
    comment TEXT,
    properties JSONB,
    current_version_key TEXT,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 索引
CREATE UNIQUE INDEX uq_assets_active_name ON assets(namespace_id, name) WHERE deleted_at IS NULL;
CREATE INDEX idx_assets_type_active ON assets(asset_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_assets_namespace ON assets(namespace_id) WHERE deleted_at IS NULL;

-- 版本层
CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_properties JSONB,
    content_inline JSONB,
    content_pointer TEXT,
    previous_version_id UUID REFERENCES asset_versions(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(asset_id, version_key)
);

-- 单根约束
CREATE UNIQUE INDEX uq_asset_versions_root ON asset_versions(asset_id) WHERE previous_version_id IS NULL;

-- 标签层
CREATE TABLE asset_tags (
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, tag)
);

-- 类型扩展表：tabular_assets
CREATE TABLE tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB
);

-- 类型扩展表：view_assets
CREATE TABLE view_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    view_uuid UUID,
    location TEXT,
    metadata_location TEXT
);

-- 迁移记录
CREATE TABLE schema_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

> 所有表的 `updated_at` 由应用层在 UPDATE 语句中显式设置 `updated_at = now()`，不使用数据库触发器。

---

## 4. Store Trait 设计

### 4.1 核心 trait

| Trait | 主要方法 |
|-------|----------|
| `DomainStore` | `create_domain`, `get_domain`, `list_domains`, `update_domain`, `delete_domain`. |
| `NamespaceStore` | `create_namespace`, `get_namespace`, `list_namespaces`, `delete_namespace`, `resolve_path`. |
| `AssetTypeStore` | `register_asset_type`, `register_format`, `list_asset_types`, `get_asset_type`, `get_format`. |
| `AssetStore` | `create_asset`, `get_asset`, `list_assets`, `update_asset`, `rename_asset`, `soft_delete_asset`, `restore_asset`, `hard_delete_asset`. |
| `VersionStore` | `create_version`, `get_version`, `list_versions`, `get_latest_version`, `delete_version`。`create_version` 自动更新 `assets.current_version_key`；`get_latest_version` 基于 `current_version_key` 查询；`delete_version` 仅对非原生协议资产开放（见需求文档 FR-V3、FR-V8）。 |
| `TagStore` | `add_tag`, `remove_tag`, `list_assets_by_tag`. |
| `UnifiedQueryStore` | `query_assets` with filters (domain, namespace, type, format, tags, properties). |
| `CasCommitStore` | `compare_and_swap_pointer` for tabular assets requiring CAS. |
| `CatalogStore` | Marker trait combining all stores. |
| `AdapterRegistry` | Register and route native protocol adapters. |

### 4.2 Adapter 契约

- 原生 adapter 声明其处理的 `(asset_type, format)` 集合。
- 通过 `AdapterRegistry` 注册并接收 `Arc<dyn CatalogStore>`。
- 负责协议请求解析、请求校验、store trait 调用、协议响应构造。
- 负责将内部 `CatalogError` 映射为协议特定错误格式。

> Trait 签名见 §4.3，错误枚举定义见 §7.4。adapter 注册机制（编译时 vs 运行时）与序列化类型的最终取舍仍在后续实现设计文档中确定（见 §10）。

### 4.3 Trait 签名

以下 trait 定义位于 `core` crate，是 `storage` 实现与 `adapter` 调用的唯一契约。所有 trait 通过 `async_trait` 声明，错误统一返回 `CatalogError`（定义见 §7.4）。

```rust
#[async_trait]
pub trait DomainStore: Send + Sync {
    async fn create_domain(&self, input: CreateDomain) -> Result<Domain, CatalogError>;
    async fn get_domain(&self, name: &str) -> Result<Domain, CatalogError>;
    async fn list_domains(&self, page: Page) -> Result<PageResult<Domain>, CatalogError>;
    async fn update_domain(&self, name: &str, patch: DomainPatch) -> Result<Domain, CatalogError>;
    async fn delete_domain(&self, name: &str) -> Result<(), CatalogError>;
}

#[async_trait]
pub trait NamespaceStore: Send + Sync {
    async fn create_namespace(&self, domain: &str, path: &str, input: CreateNamespace) -> Result<Namespace, CatalogError>;
    async fn get_namespace(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError>;
    async fn list_namespaces(&self, domain: &str, prefix: Option<&str>, page: Page) -> Result<PageResult<Namespace>, CatalogError>;
    async fn update_namespace(&self, domain: &str, path: &str, patch: NamespacePatch) -> Result<Namespace, CatalogError>;
    async fn delete_namespace(&self, domain: &str, path: &str) -> Result<(), CatalogError>;
    async fn resolve_path(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError>;
}

#[async_trait]
pub trait AssetStore: Send + Sync {
    async fn create_asset(&self, input: CreateAsset) -> Result<Asset, CatalogError>;
    async fn get_asset(&self, id: Uuid) -> Result<Asset, CatalogError>;
    async fn get_asset_by_name(&self, domain: &str, namespace: &str, name: &str) -> Result<Asset, CatalogError>;
    async fn list_assets(&self, filter: AssetFilter, page: Page) -> Result<PageResult<Asset>, CatalogError>;
    async fn update_asset(&self, id: Uuid, patch: AssetPatch) -> Result<Asset, CatalogError>;
    async fn rename_asset(&self, id: Uuid, new_name: &str) -> Result<Asset, CatalogError>;
    async fn soft_delete_asset(&self, id: Uuid) -> Result<(), CatalogError>;
    async fn restore_asset(&self, id: Uuid) -> Result<Asset, CatalogError>;
    async fn hard_delete_asset(&self, id: Uuid) -> Result<(), CatalogError>;
}

#[async_trait]
pub trait VersionStore: Send + Sync {
    async fn create_version(&self, input: CreateVersion) -> Result<AssetVersion, CatalogError>;
    async fn get_version(&self, asset_id: Uuid, version_key: &str) -> Result<AssetVersion, CatalogError>;
    async fn list_versions(&self, asset_id: Uuid, page: Page) -> Result<PageResult<AssetVersion>, CatalogError>;
    async fn get_latest_version(&self, asset_id: Uuid) -> Result<AssetVersion, CatalogError>;
    async fn delete_version(&self, asset_id: Uuid, version_key: &str) -> Result<(), CatalogError>;
}

#[async_trait]
pub trait TagStore: Send + Sync {
    async fn add_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn remove_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn list_tags(&self, asset_id: Uuid) -> Result<Vec<String>, CatalogError>;
    async fn list_assets_by_tag(&self, domain: &str, tag: &str, page: Page) -> Result<PageResult<Asset>, CatalogError>;
}

#[async_trait]
pub trait UnifiedQueryStore: Send + Sync {
    async fn query_assets(&self, query: AssetQuery) -> Result<PageResult<Asset>, CatalogError>;
}

#[async_trait]
pub trait CasCommitStore: Send + Sync {
    async fn compare_and_swap_pointer(
        &self,
        asset_id: Uuid,
        expected_version_key: &str,
        new_version: CreateVersion,
    ) -> Result<AssetVersion, CatalogError>;
}

#[async_trait]
pub trait AssetTypeStore: Send + Sync {
    async fn register_asset_type(&self, input: RegisterAssetType) -> Result<AssetType, CatalogError>;
    async fn register_format(&self, input: RegisterFormat) -> Result<Format, CatalogError>;
    async fn list_asset_types(&self, category: Option<&str>, page: Page) -> Result<PageResult<AssetType>, CatalogError>;
    async fn get_asset_type(&self, name: &str) -> Result<AssetType, CatalogError>;
    async fn get_format(&self, name: &str) -> Result<Format, CatalogError>;
}

pub trait CatalogStore: DomainStore + NamespaceStore + AssetTypeStore + AssetStore + VersionStore + TagStore + UnifiedQueryStore + CasCommitStore + Send + Sync {}
```

#### 支撑类型

分页、过滤与补丁类型同样定义在 `core` crate：

```rust
pub struct Page {
    pub page: u64,
    pub page_size: u64,
}

pub struct PageResult<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u64,
    pub page_size: u64,
}

pub struct AssetFilter {
    pub domain: Option<String>,
    pub namespace: Option<String>,
    pub asset_type: Option<String>,
    pub format: Option<String>,
    pub tags: Vec<String>,
    pub properties: HashMap<String, String>,
    pub include_deleted: bool,
}

pub struct AssetQuery {
    pub domain: String,
    pub namespace_prefix: Option<String>,
    pub asset_type: Option<String>,
    pub format: Option<String>,
    pub tags: Vec<String>,
    pub properties: HashMap<String, String>,
    pub include_deleted: bool,
    pub page: Page,
}

pub struct AssetPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

pub enum PatchField<T> {
    Set(T),
    Unset,
    NoChange,
}

// ---------- 创建与注册输入类型 ----------

pub struct CreateDomain {
    pub name: String,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
    pub storage_type: Option<String>,
    pub storage_config: Option<serde_json::Value>,
    pub warehouse: Option<String>,
}

pub struct DomainPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
    pub storage_type: PatchField<String>,
    pub storage_config: PatchField<serde_json::Value>,
    pub warehouse: PatchField<String>,
}

pub struct CreateNamespace {
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
}

pub struct NamespacePatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

pub struct CreateAsset {
    pub domain: String,
    pub namespace: String,
    pub name: String,
    pub asset_type: String,
    pub format: Option<String>,
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
}

pub struct CreateVersion {
    pub asset_id: Uuid,
    pub version_key: String,
    pub version_properties: Option<serde_json::Value>,
    pub content_inline: Option<serde_json::Value>,
    pub content_pointer: Option<String>,
    pub previous_version_id: Option<Uuid>,
}

pub struct RegisterAssetType {
    pub name: String,
    pub description: Option<String>,
    pub category: String,
    pub validation_schema: Option<serde_json::Value>,
    pub extension_strategy: String,
    pub supports_native_protocol: bool,
}

pub struct RegisterFormat {
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    pub serialization_hint: Option<String>,
}
```

> 领域模型类型（`Domain`、`Namespace`、`Asset`、`AssetVersion`、`AssetType`、`Format`）与 §3.2 表结构一一对应，此处不再重复定义。

- `Page` / `PageResult`：所有列表方法的统一分页契约，对应 §5.4 的响应格式。
- `AssetFilter`：`AssetStore::list_assets` 的存储层过滤条件，用于已知 Domain/Namespace 上下文的资产列举；`namespace` 为精确匹配。
- `AssetQuery`：`UnifiedQueryStore::query_assets` 的发现层查询条件，用于跨 Namespace 资产发现；`domain` 必填，`namespace_prefix` 为前缀匹配，支持层级查询。
- `PatchField<T>`：三态补丁语义——`Set` 更新为指定值、`Unset` 清除该字段、`NoChange` 保持不变，避免 `Option<Option<T>>` 的歧义。

---

## 5. API 设计

### 5.1 Iceberg REST Catalog

- 路径前缀 `/iceberg/v1/...`。
- `{prefix}` 映射为 Domain 名。
- 覆盖 Config、Namespace、Table、View、Transaction、Metrics 端点（见需求文档 §6.1）。
- 错误格式为 Iceberg JSON error body。

### 5.2 Lance REST Namespace

- 路径前缀 `/lance/v1/...`。
- `{id}` 使用 `$` 分隔符序列化 Domain/Namespace/Table。
- 错误格式为 RFC-7807 Problem Details。

### 5.3 Unified API

- 路径前缀 `/unified/v1/...`。
- Domain/Namespace 管理。
- Asset 管理：对没有原生协议的资产类型提供完整生命周期管理（CRUD、重命名、恢复、删除）；对已有原生协议的资产类型仅提供列表/获取，生命周期操作由原生协议负责。
- Version 管理：对原生协议资产仅提供列表/获取，且版本不可删除；对非原生协议资产提供完整生命周期管理（含创建与删除）。
- AssetType/Format 注册与管理。
- Tag 管理：为资产添加、移除、查询标签。
- 发现过滤：按 Domain、Namespace、类型、格式、标签、属性组合过滤（见需求文档 §4.6）。
- 错误格式为 RFC-7807 Problem Details，扩展 `code` 与 `request_id`。

具体端点定义见需求文档 §6.2。

### 5.4 统一分页与响应格式

Unified API 的所有列表端点使用统一的分页响应格式，与 `PageResult<T>`（见 §4.3）一一对应：

```json
{
  "items": [...],
  "total": 100,
  "page": 1,
  "page_size": 20
}
```

- `page` 从 1 开始；`page_size` 有服务端上限（超出时按上限截断）。
- `total` 为满足过滤条件的记录总数，用于客户端分页计算。
- 分页参数通过 query string 传递：`?page=1&page_size=20`。

### 5.5 统一错误响应格式

Unified API 的错误响应采用 RFC-7807 Problem Details，并扩展 `code` 与 `request_id` 字段：

```json
{
  "type": "https://quasar.io/errors/not-found",
  "title": "Not Found",
  "status": 404,
  "detail": "asset not found",
  "instance": "/unified/v1/assets/...",
  "code": "NOT_FOUND",
  "request_id": "req-01H8XJ..."
}
```

- `type`：错误类型的稳定 URI，供客户端程序化识别。
- `code`：机器可读错误码，与 HTTP 状态码的映射见 §7.5。
- `request_id`：请求唯一标识，与服务端日志关联，便于问题排查。
- `detail` 遵循 §7.3 的脱敏规则，不包含 SQL、credential 等内部信息。

---

## 6. 数据流与生命周期

### 6.1 Native protocol 数据流

1. 客户端调用原生端点。
2. Adapter 解析协议请求，解析 Domain/Namespace/Asset。
3. Adapter 校验资产类型与格式规则。
4. Adapter 在事务内调用 store trait：创建/更新资产身份与类型扩展字段，将原生版本镜像写入 `asset_versions`（含 `version_key`、`content_pointer` 等），并将 `assets.current_version_key` 更新为最新版本的 `version_key`。
5. Adapter 返回协议响应。

### 6.2 Unified API 数据流

1. 客户端调用 Unified 端点。
2. Handler 解析 Domain 与 Namespace 路径。
3. Handler 调用通用 store trait。
4. Handler 返回 Unified 响应。

### 6.3 软删除生命周期

- `DELETE` 标记 `assets.deleted_at`。
- 软删除期间版本历史保留。
- `POST /unified/v1/assets/{asset_id}/restore` 清除 `deleted_at`。若同一 Namespace 内同名资产已存在，则返回 `409 Conflict`，恢复失败；用户需先处理冲突资产。
- 保留期后执行硬删除，级联清理扩展表与版本记录。

### 6.4 并发控制设计

#### current_version_key 乐观锁

更新资产的 `current_version_key` 时使用条件更新：

```sql
UPDATE assets
SET current_version_key = $1, updated_at = now()
WHERE id = $2 AND (current_version_key = $3 OR current_version_key IS NULL)
RETURNING *;
```

如果返回行数为 0，说明版本已被其他事务更新，返回 `Conflict`。

#### CAS commit 实现

`compare_and_swap_pointer` 在事务内执行：

1. `SELECT current_version_key FROM assets WHERE id = $1 FOR UPDATE`
2. 校验 `current_version_key == expected_version_key`
3. `INSERT INTO asset_versions (...)`
4. `UPDATE assets SET current_version_key = $new_version_key WHERE id = $1`
5. 提交事务

若步骤 2 失败，返回 `CommitFailedException`。

#### 事务隔离级别

- 统一使用 `READ COMMITTED`（PostgreSQL / openGauss 默认级别）。
- CAS commit、软删除/恢复、多表事务均通过 `SELECT ... FOR UPDATE` 显式锁定目标行，结合 RC 的语句级最新读，避免快照过期导致的 CAS 失败。
- 多表事务按 `asset_id` 排序后依次锁定，防止死锁。

---

## 7. 错误处理设计

### 7.1 内部错误

- 统一 `CatalogError`：覆盖 NotFound、AlreadyExists、Conflict、Validation、Transient、Internal 等变体。
- 错误上下文用于日志，响应时脱敏。

### 7.2 协议错误映射

- Iceberg：`{"error":{"message":"...","type":"...","code":409}}`。
- Lance / Unified API：RFC-7807 Problem Details，包含 `type`、`title`、`status`、`detail`、`instance`、`code`、`request_id`。

### 7.3 脱敏规则

- 不返回 SQL、S3 secret、连接串、credential。
- 服务端日志可包含 metadata location 与 table location，但不得包含 credential。

### 7.4 错误枚举定义

`CatalogError` 定义在 `core` crate，是所有 store trait 的统一错误类型：

```rust
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("validation failed: {0}")]
    Validation(String),

    #[error("transient error: {0}")]
    Transient(String),

    #[error("internal error: {0}")]
    Internal(String),
}
```

- `NotFound`：目标资源不存在（Domain、Namespace、Asset、Version 等）。
- `AlreadyExists`：创建时违反唯一性约束（如同名资产、重复 tag）。
- `Conflict`：并发冲突或状态冲突（如 CAS 失败、恢复时同名冲突）。
- `Validation`：请求参数不合法（命名规则、必填字段、格式约束）。
- `Transient`：可重试的临时错误（连接超时、连接池耗尽、序列化冲突）。
- `Internal`：不可预期的内部错误；携带的上下文仅用于日志，响应时脱敏。

### 7.5 HTTP 状态码映射

各协议 adapter 按下表将 `CatalogError` 映射为协议特定的状态码与错误类型：

| CatalogError | HTTP Status | Iceberg Error Type | RFC-7807 Code |
|-------------|-------------|-------------------|---------------|
| NotFound | 404 | NoSuchNamespaceException / NoSuchTableException | NOT_FOUND |
| AlreadyExists | 409 | AlreadyExistsException | ALREADY_EXISTS |
| Conflict | 409 | CommitFailedException | CONFLICT |
| Validation | 400 | BadRequestException | VALIDATION_FAILED |
| Transient | 503 | ServiceUnavailableException | TRANSIENT_ERROR |
| Internal | 500 | InternalServerError | INTERNAL_ERROR |

> `NotFound` 在 Iceberg 协议下根据资源类型进一步细分：`NoSuchNamespaceException`（Namespace）、`NoSuchTableException` / `NoSuchViewException`（Table/View）。

### 7.6 错误响应格式

Iceberg error body：

```json
{
  "error": {
    "message": "table already exists",
    "type": "AlreadyExistsException",
    "code": 409
  }
}
```

RFC-7807 Problem Details（Lance / Unified API）：

```json
{
  "type": "https://quasar.io/errors/already-exists",
  "title": "Already Exists",
  "status": 409,
  "detail": "table already exists",
  "instance": "/unified/v1/domains/default/namespaces/analytics",
  "code": "ALREADY_EXISTS",
  "request_id": "req-01H8XJ..."
}
```

---

## 8. 对象存储与迁移

### 8.1 对象存储

- 全局默认存储后端由环境变量提供：`QUASAR_WAREHOUSE_PATH`（根路径）及 `QUASAR_S3_*`（对象存储连接信息）。
- Domain 配置 `storage_type`、`storage_config`、`warehouse`，用于覆盖全局默认后端。
- 存储后端解析优先级：Domain 配置 > 全局环境变量。Domain 未指定 `warehouse` 时，使用 `QUASAR_WAREHOUSE_PATH`。
- Native adapter 负责读写 Iceberg metadata.json、Lance manifest 等对象存储文件。

### 8.2 迁移

- 使用内置迁移系统；`schema_migrations` 记录已应用版本。
- 服务启动时按顺序应用未执行的迁移。
- 迁移脚本幂等、可重试。

### 8.3 对象存储路径设计

#### 目录结构约定

```
{warehouse}/
├── {domain}/
│   ├── {namespace_path}/
│   │   ├── {asset_name}/
│   │   │   ├── metadata/          # Iceberg metadata.json 或 Lance manifest
│   │   │   │   ├── v1.metadata.json
│   │   │   │   ├── v2.metadata.json
│   │   │   │   └── ...
│   │   │   └── data/              # 实际数据文件，由 Iceberg/Lance client 直接写入，Quasar 不管理
│   │   │       └── ...
```

#### 路径模板

- Iceberg metadata.json: `{warehouse}/{domain}/{namespace_path}/{asset_name}/metadata/v{version_key}.metadata.json`
- Lance manifest: `{warehouse}/{domain}/{namespace_path}/{asset_name}/metadata/{version_key}.manifest`

> `namespace_path` 中的 `/` 会自然映射为对象存储的多级 key（如 `analytics/teams/finance` 对应 S3 key 前缀 `analytics/teams/finance/`）。

#### 路径冲突避免

- 使用 `version_key` 作为文件名，天然避免冲突。
- 并发写入同一版本时，通过 `current_version_key` CAS 保证只有一个成功（见 §6.4）。

### 8.4 迁移脚本设计

#### 文件命名

```
migrations/
├── 0001_init.down.sql
├── 0001_init.up.sql
├── 0002_add_asset_tags.down.sql
├── 0002_add_asset_tags.up.sql
```

#### 执行规则

- 服务启动时按版本号升序执行未应用的 `.up.sql`。
- `.down.sql` 仅用于开发环境手动回滚，生产环境不自动执行。
- 每个迁移在独立事务中执行。
- 迁移失败则服务启动失败，不跳过。

---

## 9. Server 组装与配置

### 9.1 配置项

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_DATABASE_URL` | 是 | `postgres://user:password@localhost:5432/quasar` | PostgreSQL 连接串；生产环境必须替换为实际凭据。 |
| `QUASAR_HOST` | 否 | `0.0.0.0` | HTTP 监听地址。 |
| `QUASAR_PORT` | 否 | `8080` | HTTP 监听端口。 |
| `QUASAR_LOG_LEVEL` | 否 | `info` | tracing 日志级别。 |
| `QUASAR_WAREHOUSE_PATH` | 否 | — | 全局默认对象存储根路径。当 Domain 未指定 `warehouse` 时使用；支持 `s3://...`、`/data/quasar/warehouse` 等 URI 或本地路径。 |
| `QUASAR_S3_ENDPOINT` | 否 | — | S3/MinIO endpoint。 |
| `QUASAR_S3_ACCESS_KEY` | 否 | — | S3 access key。 |
| `QUASAR_S3_SECRET_KEY` | 否 | — | S3 secret key。 |
| `QUASAR_S3_REGION` | 否 | `us-east-1` | S3 region。 |
| `QUASAR_S3_ALLOW_HTTP` | 否 | `false` | 是否允许 HTTP 访问对象存储。 |
| `QUASAR_DB_MAX_CONNECTIONS` | 否 | `10` | 连接池最大连接数。 |

### 9.2 Feature flags

| Feature | 默认 | 控制内容 |
|---------|------|---------|
| `lance` | 启用 | Lance adapter。 |
| `iceberg` | 启用 | Iceberg adapter。 |
| `unified` | 启用 | Unified API adapter。 |

### 9.3 优雅关机

- 接收 SIGTERM/SIGINT 后停止接受新请求。
- 等待在途请求处理完毕。
- 关闭连接池后退出。

### 9.4 健康检查设计

#### /healthz

- 返回 `200 OK` 如果进程存活。
- 不检查外部依赖。

#### /readyz

- 检查 PostgreSQL 连通性：`SELECT 1`。
- 数据库不可达时返回 `503 Service Unavailable`。
- 可选：检查对象存储连通性（基线阶段不检查）。

---

## 10. 待明确与延期事项

以下设计细节留待后续实现设计文档确定。本节关注**实现层面未明确的技术方案**；需求层面的功能与约束延期事项见 `docs/baseline/REQUIREMENTS.md` §10。

1. Adapter 的序列化类型与请求/响应结构体的具体定义（Store trait 签名见 §4.3，错误枚举见 §7.4）。
2. Adapter 注册机制：采用编译时 feature flag；运行时动态插件作为后续版本可选方向。
3. 内联内容大小限制与对象存储卸载策略。
4. 软删除保留窗口与硬删除策略。
5. 从旧里程碑 Schema 到新通用 Schema 的迁移脚本。
6. model、agent、tool、mcp_server 等资产类型的扩展表字段与原生协议。

---

## 11. 总结

Quasar 的设计基线采用**通用身份 + 类型化扩展**的架构：

- `Domain → 层级 Namespace → Asset` 提供统一身份模型。
- `AssetType` 与 `Format` 的动态注册支持未来扩展。
- `assets` + `asset_versions` + 类型扩展表构成可扩展的物理模型。
- Iceberg、Lance 等原生协议作为 adapter 实现。
- Unified API 提供跨格式的管理与发现。
- 强一致性、软删除/恢复、无状态水平扩展。

该设计在满足当前 Iceberg/Lance 需求的同时，为模型、Agent、Tool、MCP Server 等非表资产预留了清晰的扩展路径。
