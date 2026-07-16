# Quasar 设计说明书

> **项目名称**：Quasar —— 面向 Lakehouse 架构的通用 Catalog Service
> **文档状态**：当前权威基线
> **适用版本**：V4.2
>
> 本文档承接 `docs/REQUIREMENTS.md` 的实现设计，是设计层面的唯一权威。
> 需求与验收标准见 `docs/REQUIREMENTS.md`；部署见 `docs/DEPLOYMENT.md`；测试矩阵见 `docs/TEST_MATRIX.md`；官方端点清单见 `docs/archive/v4/V4_OFFICIAL_REST_API.md`。
> 历史版本设计文档归档于 `docs/archive/`，仅作参考，不再维护。

---

## 目录

1. [架构总览](#1-架构总览)
2. [Crate 分层与依赖](#2-crate-分层与依赖)
3. [数据模型设计](#3-数据模型设计)
4. [Store Trait 设计](#4-store-trait-设计)
5. [Iceberg REST API 设计](#5-iceberg-rest-api-设计)
6. [Lance REST API 设计](#6-lance-rest-api-设计)
7. [Unified API 设计](#7-unified-api-设计)
8. [Commit 与 Metadata 设计](#8-commit-与-metadata-设计)
9. [对象存储与 Purge 设计](#9-对象存储与-purge-设计)
10. [Scan Planning 设计](#10-scan-planning-设计)
11. [错误处理设计](#11-错误处理设计)
12. [Feature Flag 与依赖设计](#12-feature-flag-与依赖设计)
13. [Server 组装与配置](#13-server-组装与配置)

---

## 1. 架构总览

### 1.1 系统组件图

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
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-adapter                           │
│  ┌───────────────────────┼───────────────────────────────┐   │
│  │    quasar-core        │   (traits + models + errors)  │   │
│  │  ┌────────────────────┴───────────────────────────┐   │   │
│  │  │ DomainStore | NamespaceStore | AssetStore      │   │   │
│  │  │ TabularStore | VersionStore | TabularVersionStore│ │   │
│  │  │ CasCommitStore | UnifiedQueryStore              │   │   │
│  │  │ IcebergStaging/Register/Metrics/Purge/Transaction/ViewStore │ │
│  │  └────────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-storage                           │
│              PgCatalogStore (impl all traits)                │
│            queries.rs (SQL 常量) | schema/init.sql           │
└──────────────────────────┬───────────────────────────────────┘
                           │
                    PostgreSQL + S3/MinIO object store
```

### 1.2 设计原则

1. **官方协议路径优先**：REST 路径、请求/响应字段和错误模型对齐 Iceberg 1.10.x / Lance REST Namespace 规范。
2. **声明即实现**：`/v1/config` 的 `endpoints` 只返回实际已实现端点。
3. **metadata 兼容优先**：Quasar 写出的 metadata.json 能被 Spark Iceberg 1.10.x client 读回。
4. **Catalog 状态由 PostgreSQL 仲裁**：`metadata_location` 是当前元数据指针，CAS 由数据库条件更新保证。
5. **对象存储失败可诊断**：容忍 object store 与 PostgreSQL 的非原子窗口，但必须返回明确错误并记录路径上下文。
6. **身份与扩展分离**：`assets` 统一身份层，类型特有字段进入扩展表。
7. **端点级隔离**：格式由 REST 端点路径隐含，Domain 不绑定格式。

### 1.3 Domain 映射

| Quasar | Iceberg REST | Lance REST |
|--------|--------------|------------|
| `Domain.name` | `{prefix}` | `{id}` 第一段 |
| `Namespace.name` | `{namespace}` | `{id}` 第二段 |
| `Asset.name` | `{table}` / `{view}` | `{id}` 第三段 |
| `tabular_assets.format = 'iceberg'` | Iceberg 端点可见性过滤 | — |
| `tabular_assets.format = 'lance'` | — | Lance 端点可见性过滤 |

示例：`GET /iceberg/v1/prod/namespaces/analytics/tables/events` 中 `prod` 是 Quasar Domain（也是 Iceberg `{prefix}`）。`/iceberg` 是服务部署 base path，不属于 Iceberg 协议操作路径。

---

## 2. Crate 分层与依赖

### 2.1 Workspace 结构

```
quasar/
├── core/          # 核心领域模型与 trait 定义
│                  # - Domain / Namespace / Asset / TabularAsset / ViewAsset / AssetVersion 等
│                  # - 8 个 V3 基础 trait + 6 个 Iceberg 专用 trait + CatalogStore marker
│                  # - StoreError、PatchField、validation
│                  # - 不依赖任何具体框架或存储实现
│
├── storage/       # 存储层实现
│                  # - PostgreSQL 实现 CatalogStore 全部 trait
│                  # - 连接池管理（deadpool-postgres）
│                  # - schema/init.sql（幂等初始化脚本）
│                  # - queries.rs（SQL 常量集中管理）
│
├── adapter/       # 协议适配层
│   ├── iceberg/   # Iceberg REST Catalog 适配（需启用 `iceberg` feature）
│   │              # - /iceberg/v1/... 路由与 handler
│   │              # - 请求/响应 DTO（严格遵循 Iceberg REST Spec）
│   │              # - metadata.rs / view_metadata.rs：metadata parse/build/serialize wrapper
│   │              # - scan_planning.rs：plan-task token 编解码
│   │              # - object_store_util.rs：对象存储 helper
│   │              # - Iceberg 错误映射
│   │
│   ├── lance/     # Lance REST Namespace 适配（需启用 `lance` feature）
│   │              # - /lance/v1/... 路由与 handler
│   │              # - {id} 路径解析（$ 分隔符）
│   │              # - RFC-7807 错误响应
│   │
│   └── unified/   # Unified API 适配（需启用 `unified` feature）
│                  # - /unified/v1/... 路由与 handler
│                  # - Domain 管理 API + 跨格式聚合查询
│                  # - Problem Details 错误响应
│
└── server/        # 服务入口
                   # - axum 路由注册与中间件（tracing、错误处理）
                   # - 配置加载（环境变量）
                   # - Health / Readiness 端点
                   # - main 函数与优雅关机
```

### 2.2 依赖关系

```
server ──► adapter ──► core
  │                      ▲
  └──► storage ──────────┘
```

- `core` 不依赖任何具体框架或存储实现，只包含数据结构和 trait 定义；不依赖 `iceberg` crate。
- `storage` 依赖 `core`，实现 `CatalogStore` trait；只存储 JSONB 和 catalog 指针，不依赖 `iceberg` 类型。
- `adapter` 依赖 `core` + `iceberg` crate + `object_store`；通过 trait 操作存储，不感知具体实现是 PostgreSQL 还是其他后端。
- `server` 依赖 `adapter`、`storage`、`core`；创建 `PgCatalogStore` 并作为 `Arc<dyn CatalogStore>` 注入 adapter handler。

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

## 3. 数据模型设计

### 3.1 物理 Schema 概览

```
注册表:  asset_types | tabular_formats
第一层:  domains
第二层:  namespaces ──(domain_id FK)──► domains
第三层:  assets ──(namespace_id FK)──► namespaces
扩展层:  tabular_assets ──(asset_id)──► assets
         view_assets    ──(asset_id)──► assets
版本层:  asset_versions ──(asset_id)──► assets
         tabular_asset_versions ──(version_id)──► asset_versions
预留层:  asset_permissions ──(asset_id)──► assets
运营层:  iceberg_staged_tables | iceberg_scan_metrics_reports | iceberg_purge_operations
```

### 3.2 表结构（字段级）

#### 3.2.1 注册表

| 表 | 字段 | 说明 |
|----|------|------|
| `asset_types` | `name` PK, `comment`, `created_at` | 已注册：`table`、`view` |
| `tabular_formats` | `name` PK, `comment`, `supports_cas_commit` bool, `created_at` | 已注册：`iceberg`(TRUE)、`lance`(FALSE) |

> 用注册表替代硬编码 `CHECK` 约束，新增资产类型/表格式不需 schema migration。

#### 3.2.2 domains

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | `gen_random_uuid()` |
| `name` | TEXT UNIQUE | 全局唯一，用于 API 路径与权限边界 |
| `comment` | TEXT | 描述说明 |
| `properties` | JSONB | 业务自定义属性 |
| `storage_type` | TEXT | s3 / minio / hdfs / local |
| `storage_config` | JSONB | 存储配置，不得明文存 credential |
| `warehouse` | TEXT | 默认 warehouse 根路径 |
| `owner` | TEXT | 所有者标识 |
| `created_at` / `updated_at` | TIMESTAMPTZ | 时间戳 |

#### 3.2.3 namespaces

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `domain_id` | UUID FK→domains | `ON DELETE RESTRICT` |
| `name` | TEXT | 同一 Domain 内唯一 |
| `comment` / `properties` | TEXT / JSONB | — |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |
| | `UNIQUE(domain_id, name)` | 约束 |

> 单层 Namespace，不预留 `parent_id`。`idx_namespaces_domain` 加速按 Domain 列出。

#### 3.2.4 assets（统一身份层）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | 治理能力统一引用该 ID |
| `namespace_id` | UUID FK→namespaces | `ON DELETE RESTRICT` |
| `name` | TEXT | 同一 Namespace 内活动资产名唯一 |
| `asset_type` | TEXT FK→asset_types(name) | `table` / `view` |
| `comment` | TEXT | 描述说明 |
| `properties` | JSONB | 治理/管理面属性，不存格式内部状态 |
| `deleted_at` | TIMESTAMPTZ | 软删除时间，NULL 表示活动 |
| `created_by` / `updated_by` | TEXT | 审计预留 |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |

**索引**：
- `uq_assets_active_name`：partial unique on `(namespace_id, name) WHERE deleted_at IS NULL` —— 同时承担活动资产定位查询的索引职责。
- `idx_assets_type_active`：partial index on `asset_type WHERE deleted_at IS NULL`。

#### 3.2.5 tabular_assets（表资产扩展）

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE` |
| `format` | TEXT FK→tabular_formats(name) | `iceberg` / `lance` |
| `location` | TEXT | 表数据根路径 |
| `metadata_location` | TEXT | 当前 metadata.json 指针（Iceberg）；Lance 可为空 |
| `schema_snapshot` | JSONB | schema 快照缓存 |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |

**触发器** `trg_tabular_assets_type_check`：保证 `asset_id` 对应的 `assets.asset_type = 'table'`（ERRCODE 23514）。
**索引** `idx_tabular_assets_format_asset` on `(format, asset_id)`。

#### 3.2.6 view_assets（View 扩展，Iceberg-only）

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE` |
| `view_uuid` | UUID | Iceberg ViewMetadata UUID，用于 CAS 校验 |
| `location` | TEXT | View 根路径 |
| `current_version_id` | INT | 创建时初始 version ID（=1）；快照性质，commit 后不更新 |
| `metadata_location` | TEXT | 当前 View metadata 文件指针 |
| `properties` | JSONB | View 属性快照（如 `version.history.num-entries`） |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |

> 无 `format` 字段（View 当前只有 Iceberg 支持）。`current_version_id` 和 `properties` 为快照性质——`commit_view` 不更新这两个字段，Load View 总是从对象存储读取完整 metadata。这与 Table CAS 同步更新 `schema_snapshot` 的模式不同。
**触发器** `trg_view_assets_type_check`：保证 `asset_type = 'view'`。

#### 3.2.7 asset_versions（通用版本身份）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `asset_id` | UUID FK→assets | `ON DELETE CASCADE` |
| `version_key` | TEXT | 格式原生版本标识，同 Asset 内唯一 |
| `version_order` | BIGINT | 可比较顺序，允许为空 |
| `previous_version_id` | UUID FK→asset_versions | `ON DELETE SET NULL` |
| `comment` | TEXT | 版本说明 |
| `properties` | JSONB | 版本级属性 |
| `created_at` | TIMESTAMPTZ | — |
| | `UNIQUE(asset_id, version_key)` | 约束 |

**索引**：
- `uq_asset_versions_order`：partial unique on `(asset_id, version_order) WHERE version_order IS NOT NULL`。
- `idx_asset_versions_latest`：partial index on `(asset_id, version_order DESC) WHERE version_order IS NOT NULL`。
- `idx_asset_versions_previous` on `previous_version_id`。

**触发器** `trg_asset_versions_previous_version_check`：保证 `previous_version_id` 指向同一 Asset 下的版本。

#### 3.2.8 tabular_asset_versions

| 字段 | 类型 | 说明 |
|------|------|------|
| `version_id` | UUID PK FK→asset_versions | `ON DELETE CASCADE` |
| `metadata_location` | TEXT | 该版本 manifest 位置 |
| `created_at` | TIMESTAMPTZ | — |

#### 3.2.9 运营表

| 表 | 关键字段 | 说明 |
|----|---------|------|
| `iceberg_staged_tables` | `(domain_name, namespace_name, table_name)` UNIQUE, `table_uuid`, `metadata_location`, `metadata_json` JSONB, `expires_at` | staged create 弱引用记录，24h 过期 |
| `iceberg_scan_metrics_reports` | `asset_id` FK→assets ON DELETE SET NULL, `(domain_name, namespace_name, table_name, created_at DESC)` index, `report` JSONB, `user_agent` | scan metrics 原始 report |
| `iceberg_purge_operations` | `(domain_name, namespace_name, table_name, requested_at DESC)` index, `status`, `error_message`, `completed_at` | purge 操作记录，status: started/catalog_dropped/completed/failed |

#### 3.2.10 预留表

`asset_permissions`（RBAC，表存在但不暴露 API）：`asset_id` FK CASCADE, `subject`, `action`, `granted_by`, `granted_at`, `expires_at`, `UNIQUE(asset_id, subject, action)`。

### 3.3 Core Rust 模型

| 模型 | 字段概要 | 对应表 |
|------|---------|--------|
| `Domain` | id, name, comment, properties, storage_type, storage_config(JSON Value), warehouse, owner, created_at, updated_at | domains |
| `Namespace` | id, domain_id, name, comment, properties, created_at, updated_at | namespaces |
| `Asset` | id, namespace_id, name, asset_type(String), comment, properties, deleted_at, created_by, updated_by, created_at, updated_at | assets |
| `TabularAsset` | asset_id, format(String), location, metadata_location(Option), schema_snapshot(Option<JSON>), created_at, updated_at | tabular_assets |
| `ViewAsset` | asset_id, view_uuid, location, current_version_id, metadata_location, properties(JSON Value), created_at, updated_at | view_assets |
| `View` | asset: Asset, view: ViewAsset | assets ⋈ view_assets 投影 |
| `AssetVersion` | id, asset_id, version_key, version_order(Option), previous_version_id(Option), comment, properties, created_at | asset_versions |
| `TabularAssetVersion` | version_id, metadata_location, created_at | tabular_asset_versions |
| `ViewIdentifier` | namespace: Vec<String>, name: String | REST 响应 |

> 类型安全策略：`asset_type` / `format` 使用普通 `String`（引用注册表），避免硬编码枚举限制扩展。`PatchField<T>` 三态枚举（`Missing` / `Null` / `Value(T)`）支持 PATCH 语义。

完整 DDL 见 `quasar/storage/src/schema/init.sql`（幂等，可重执行）；完整 Rust 定义见 `quasar/core/src/models.rs`。

---

## 4. Store Trait 设计

### 4.1 Trait 总览

Quasar 将存储能力拆分为多个专注 trait，`PgCatalogStore` 实现全部。

| Trait | 职责 | super-trait |
|------|------|-------------|
| `DomainStore` | Domain CRUD + update | — |
| `NamespaceStore` | Namespace CRUD（带 domain 上下文） | — |
| `AssetStore` | 格式无关 Asset 身份操作（不带 format 参数） | — |
| `TabularStore` | 表资产读写（带 format 过滤） | `AssetStore` |
| `VersionStore` | 通用版本操作（按 asset_id 定位） | — |
| `TabularVersionStore` | 表版本扩展操作 | `VersionStore` |
| `CasCommitStore` | Iceberg CAS commit | `TabularStore` |
| `UnifiedQueryStore` | Unified API 跨格式聚合查询 | — |
| `IcebergStagingStore` | staged create 生命周期 | `TabularStore` |
| `IcebergRegisterStore` | 注册外部 Iceberg 表 | `TabularStore` |
| `IcebergMetricsStore` | scan metrics 持久化 | — |
| `IcebergPurgeStore` | DROP TABLE PURGE 操作 | `TabularStore` |
| `IcebergTransactionStore` | 多表事务 CAS | `TabularStore` |
| `IcebergViewStore` | View 生命周期 | `AssetStore` |

### 4.2 Marker trait 组合

```rust
// 通用目录能力 marker
pub trait CatalogStore:
    DomainStore + NamespaceStore + AssetStore + TabularStore
    + VersionStore + TabularVersionStore + CasCommitStore + UnifiedQueryStore
    + IcebergStagingStore + IcebergRegisterStore + IcebergMetricsStore
    + IcebergPurgeStore + IcebergTransactionStore + IcebergViewStore
    + Send + Sync
{}
impl<T: ...> CatalogStore for T where T: ... + ?Sized {}

// Iceberg 专用能力 marker（文档与未来 state 拆分用）
pub trait IcebergCatalogStore:
    CatalogStore + CasCommitStore
    + IcebergStagingStore + IcebergRegisterStore + IcebergMetricsStore
    + IcebergPurgeStore + IcebergTransactionStore + IcebergViewStore
    + Send + Sync
{}
```

> **State 类型说明**：因 Rust stable 不支持 `dyn IcebergCatalogStore` → `dyn CatalogStore` 的 trait object upcast，Axum `Router::merge` 无法合并不同 state 类型的子路由。当前所有 handler 统一使用 `Arc<dyn CatalogStore>` 作为 state；`IcebergCatalogStore` 仅作 marker。Lance/Unified handler 不依赖它们不需要的 Iceberg 方法（即使 `CatalogStore` 含 `CasCommitStore`，它们也不调用）。

### 4.3 关键方法签名

#### 4.3.1 AssetStore（格式无关，活动名唯一）

```rust
async fn create_asset(&self, domain, namespace, name, asset_type, comment, properties) -> Result<Asset>
async fn get_asset(&self, domain, namespace, name) -> Result<Asset>           // 不带 format
async fn asset_exists(&self, domain, namespace, name) -> Result<bool>
async fn drop_asset(&self, domain, namespace, name) -> Result<()>             // 不带 format
async fn rename_asset(&self, domain, namespace, name, new_name, new_namespace: Option) -> Result<()>
async fn update_asset(&self, domain, namespace, name, comment: PatchField, removals, updates) -> Result<Asset>
```

#### 4.3.2 TabularStore（带 format 过滤）

```rust
async fn create_tabular_asset(&self, domain, namespace, name, format, location, metadata_location: Option, schema_snapshot: Option, properties) -> Result<(Asset, TabularAsset)>
async fn list_tabular_assets(&self, domain, namespace, format: Option, offset, limit) -> Result<Vec<(Asset, TabularAsset)>>
async fn get_tabular_asset(&self, domain, namespace, format, name) -> Result<(Asset, TabularAsset)>
async fn get_tabular_asset_with_current_version(&self, domain, namespace, format, name) -> Result<(Asset, TabularAsset, Option<(AssetVersion, TabularAssetVersion)>)>
```

#### 4.3.3 CasCommitStore（Iceberg CAS）

```rust
async fn cas_update_metadata_location(
    &self, domain, namespace, asset_name, format,
    expected_location, new_location,
    new_schema_snapshot: Option<JSON>,
    property_removals, property_updates,
) -> Result<(), StoreError>   // 行数=0 → Conflict
```

#### 4.3.4 IcebergStagingStore（事务级方法）

```rust
async fn create_staged_table(&self, domain, namespace, table_name, table_uuid, location, metadata_location, metadata_json, properties) -> Result<()>
async fn get_staged_table(&self, domain, namespace, table_name) -> Result<Option<JSON>>
async fn delete_staged_table(&self, domain, namespace, table_name) -> Result<()>
async fn commit_staged_table(&self, domain, namespace, table_name, location, metadata_location, metadata_json, properties) -> Result<(Asset, TabularAsset)>
// commit_staged_table 在单事务内：校验 active 不存在 → 锁定未过期 staged → 插入 assets+tabular_assets → 删除 staged
```

#### 4.3.5 IcebergRegisterStore / IcebergPurgeStore / IcebergTransactionStore

```rust
// Register：单事务插入 assets+tabular_assets，不写对象存储
async fn register_iceberg_table(&self, domain, namespace, table_name, location, metadata_location, metadata_json, properties) -> Result<(Asset, TabularAsset)>

// Purge：单事务读取 location + 创建 operation(status=catalog_dropped) + 删除 catalog
async fn begin_iceberg_purge_and_drop_catalog(&self, domain, namespace, table_name) -> Result<(operation_id: Uuid, table_location: String, metadata_location: Option<String>)>
async fn update_purge_operation(&self, operation_id, status, error_message: Option) -> Result<()>

// Transaction：单事务多表 CAS，元组 (domain, namespace, table, expected, new, schema_snapshot)
async fn commit_transaction_tables(&self, table_updates: Vec<(String, String, String, String, String, Option<JSON>)>) -> Result<(), StoreError>
```

#### 4.3.6 IcebergViewStore（View 生命周期）

```rust
async fn create_view(&self, domain, namespace, view_name, view_uuid, location, metadata_location, current_version_id, properties: JSON) -> Result<View>
async fn get_view(&self, domain, namespace, view_name) -> Result<View>
async fn commit_view(&self, domain, namespace, view_name, expected_location, new_location) -> Result<()>
// commit_view 不更新 current_version_id / properties（快照性质）
async fn drop_view(&self, domain, namespace, view_name) -> Result<()>     // FK cascade 删 view_assets
async fn list_views(&self, domain, namespace, offset, limit) -> Result<Vec<ViewIdentifier>>
async fn rename_view(&self, source_domain, source_namespace, source_name, dest_domain, dest_namespace, dest_name) -> Result<()>
async fn view_exists(&self, domain, namespace, view_name) -> Result<bool>
```

> **实现约束**：`IcebergViewStore: AssetStore` 使 View 身份通过 `assets` 表管理，但 `create_view` / `rename_view` 必须使用自定义 SQL 在单个 PostgreSQL 事务内完成 `assets` + `view_assets` 双行操作，不能拆分为先调用 `AssetStore::create_asset` 再单独插入 `view_assets`。

### 4.4 SQL 集中管理

所有 SQL 集中在 `quasar/storage/src/queries.rs` 常量模块，按功能域组织（`domain` / `namespace` / `asset` / `version` / `iceberg_staged` / `iceberg_metrics` / `iceberg_purge` / `iceberg_transaction` / `view` 等）。

设计约定：
- 常量名 `SCREAMING_SNAKE_CASE`，参数占位符 `$1, $2`。
- 查询通过 `JOIN domains` / `JOIN namespaces` 从自然键（name）解析到 ID，避免 handler 层传递 ID。
- 禁止字符串拼接 SQL。

### 4.5 Schema 初始化

- 初始化脚本 `quasar/storage/src/schema/init.sql` 是工程的一部分，**不是 migration**，不放在 `migrations/` 目录。
- 脚本幂等：表/索引用 `IF NOT EXISTS`，函数用 `CREATE OR REPLACE`，触发器先 `DROP` 再建，注册表 seed 用 `ON CONFLICT DO NOTHING`。
- 服务启动时 `PgCatalogStore::initialize()` 显式加载该脚本。
- 不提供历史版本 schema 迁移路径；全新部署从空数据库开始。

---

## 5. Iceberg REST API 设计

本章按 endpoint 描述 adapter → core → storage 调用链。Handler state 类型为 `Arc<dyn CatalogStore>`。

### 5.1 端点 → Store 调用矩阵

| Endpoint | Adapter 职责 | Store trait 调用 | Storage 事务边界 |
|----------|--------------|------------------|------------------|
| `GET /v1/config` | 返回实际已实现 endpoints、warehouse 默认/覆盖配置 | 无 store 调用 | 无事务 |
| `GET /v1/{prefix}/namespaces` | 解析 `{prefix}` 为 Domain；分页 | `NamespaceStore::list_namespaces` | 单条只读 JOIN |
| `POST /v1/{prefix}/namespaces` | 校验名称/properties | `NamespaceStore::create_namespace` | 单条 insert + domain 存在校验 |
| `GET /v1/{prefix}/namespaces/{ns}` | 加载属性 | `NamespaceStore::get_namespace` | 单条只读 |
| `HEAD /v1/{prefix}/namespaces/{ns}` | 存在性，无 body | `NamespaceStore::namespace_exists` | `SELECT EXISTS` |
| `DELETE /v1/{prefix}/namespaces/{ns}` | 删空 namespace | `NamespaceStore::drop_namespace` | 单条 delete；FK RESTRICT 阻止非空 |
| `POST /v1/{prefix}/namespaces/{ns}/properties` | 解析 removals/updates | `NamespaceStore::update_namespace` | 读旧 + 增量 update 单事务 |
| `GET /v1/{prefix}/namespaces/{ns}/tables` | 只列 active Iceberg 表 | `TabularStore::list_tabular_assets(..., Some("iceberg"), ...)` | 只读 JOIN，过滤 `format='iceberg'` |
| `POST /v1/{prefix}/namespaces/{ns}/tables` (non-staged) | iceberg crate 构建初始 metadata；写对象存储；返回 LoadTableResponse | `TabularStore::create_tabular_asset(..., "iceberg", ...)` | 单事务 insert assets+tabular_assets |
| `POST /v1/{prefix}/namespaces/{ns}/tables` (staged) | 校验 active 不存在；构建 staged metadata；写对象存储；返回 staged response | `IcebergStagingStore::create_staged_table` | 清理过期 + 检查 active + 插入 staged 单事务 |
| `POST /v1/{prefix}/namespaces/{ns}/register` | 读外部 metadata-location；iceberg crate 校验；不重写对象存储 | `IcebergRegisterStore::register_iceberg_table` | 单事务 insert assets+tabular_assets |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` | 读 catalog pointer；读对象存储 metadata；返回 LoadTableResponse | `TabularStore::get_tabular_asset(..., "iceberg", ...)` | 只读 JOIN |
| `HEAD /v1/{prefix}/namespaces/{ns}/tables/{table}` | 检查 active Iceberg 表存在 | `TabularStore::get_tabular_asset` | 只读 JOIN |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` (existing commit) | 读当前 metadata；校验 requirements；应用 updates；写新 metadata；CAS | `TabularStore::get_tabular_asset` → `CasCommitStore::cas_update_metadata_location` | CAS update metadata_location+schema_snapshot+properties 单事务 |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` (staged commit) | active 不存在时读 staged metadata；要求 `assert-create`；应用 updates；写最终 metadata | `IcebergStagingStore::get_staged_table` → `commit_staged_table` | commit_staged_table 单事务 |
| `DELETE .../tables/{table}?purgeRequested=false` | 只删 catalog | `AssetStore::drop_asset` | 单条 delete；FK cascade |
| `DELETE .../tables/{table}?purgeRequested=true` | 安全校验；删对象存储 prefix；回写 operation | `TabularStore::get_tabular_asset` → `IcebergPurgeStore::begin_iceberg_purge_and_drop_catalog` + `update_purge_operation` | begin 单事务 |
| `POST /v1/{prefix}/tables/rename` | 校验 source/destination；只允许 active Iceberg source | `TabularStore::get_tabular_asset` → `AssetStore::rename_asset` | 单条 update；唯一约束 |
| `POST .../tables/{table}/metrics` | 校验表存在；保留原始 report JSON；204 | `TabularStore::get_tabular_asset` → `IcebergMetricsStore::record_scan_metrics_report` | metrics insert |
| `POST /v1/{prefix}/transactions/commit` | Phase 1 预写对象存储 + Phase 2 DB 事务 | `TabularStore::get_tabular_asset` × N → `IcebergTransactionStore::commit_transaction_tables` | 单事务多表 CAS |
| View 7 端点 | 见 §5.3 | `IcebergViewStore::*` | 见 §5.3 |
| Scan Planning 4 端点 + 4 alias | 见 §10 | `TabularStore::get_tabular_asset`（仅校验） | 无事务（无持久化） |

### 5.2 关键 Handler 行为

**`GET /v1/config`**：返回 `ConfigResponse { defaults, overrides, endpoints }`。`endpoints` 由 `supported_endpoints()` 返回实际已实现端点列表。支持 `?warehouse=` 查询参数覆盖默认配置；所有 Iceberg handler 通过 `validate_warehouse()` 校验 warehouse 参数。

**`POST /v1/{prefix}/namespaces/{ns}/tables`（create table）**：
- 请求字段：`name`（必填）、`location`（可选，缺省由 warehouse 生成）、`schema`（必填或按 Spark 请求解析）、`partition-spec` / `write-order`（可选）、`properties`、`stage-create`（缺省 false）。
- non-staged：`TableMetadataBuilder::new()` → `build()` → `serde_json::to_value()` → 写对象存储 `metadata/00001-{uuid}.metadata.json` → `create_tabular_asset`。
- staged：校验 active 不存在 → 构建 staged metadata → 写对象存储 → `create_staged_table`（不写 assets）→ 返回 staged LoadTableResponse（list/load/head 不可见）。

**`POST /v1/{prefix}/namespaces/{ns}/register`（register table）**：
- 请求字段：`name`（必填）、`metadata-location`（必填）。
- 从对象存储读 metadata JSON → `serde_json::from_value::<TableMetadata>()` 校验 → 提取 uuid/location → `register_iceberg_table`（不重写对象存储）。
- 失败：metadata 不存在 404 `NoSuchMetadataException`；JSON 非法 400；表已存在 409。

**`POST .../tables/{table}`（commit table）**：分两条路径（见 §8）。

**`DELETE .../tables/{table}`（drop table）**：
- `purgeRequested=false`/缺省：只删 catalog，返回 204。
- `purgeRequested=true`：加载 table location → 安全校验（见 §9.2）→ `begin_iceberg_purge_and_drop_catalog`（单事务创建 operation + 删 catalog + 标记 catalog_dropped）→ 删对象存储 prefix → `update_purge_operation(completed/failed)`。

### 5.3 View 端点调用矩阵

| Endpoint | Adapter 职责 | Store trait 调用 | Storage 事务边界 |
|----------|--------------|------------------|------------------|
| `GET .../views` | 解析 namespace；映射 ViewIdentifier response | `IcebergViewStore::list_views` | 只读 JOIN，过滤 `asset_type='view'` |
| `POST .../views` (create) | 校验 Domain/Namespace/同名 Table；`ViewMetadataBuilder::from_view_creation()` 构建；写对象存储；返回 GetViewResponse | `AssetStore::asset_exists`（同名检查）→ `IcebergViewStore::create_view` | 单事务 insert assets+view_assets |
| `GET .../views/{view}` | 读 catalog pointer；读对象存储 metadata；返回 GetViewResponse | `IcebergViewStore::get_view` | 只读 JOIN |
| `POST .../views/{view}` (replace) | 读当前 metadata；校验 ViewRequirement；应用 ViewUpdate；写新 metadata；CAS | `IcebergViewStore::get_view` → `commit_view` | CAS update view_assets.metadata_location |
| `DELETE .../views/{view}` | 删 catalog 记录；不清理对象存储 | `IcebergViewStore::drop_view` | 单条 delete；FK cascade 删 view_assets |
| `HEAD .../views/{view}` | 检查 active View 存在 | `IcebergViewStore::view_exists` | `SELECT EXISTS` |
| `POST .../views/rename` | 校验 source/destination；只允许 active View source | `NamespaceStore::namespace_exists` → `IcebergViewStore::get_view` → `rename_view` | 单条 update；唯一约束 |

> **Create View 请求体**：Iceberg 1.10.x 的 `CreateViewRequest`（独立结构，与 Replace View 的 `CommitViewRequest` 不同）。adapter 将其字段转换为 `ViewCreation`（iceberg crate 类型），通过 `ViewMetadataBuilder::from_view_creation()` 构建 ViewMetadata。`ViewCreation.name` 仅用于 REST 层面标识（写入 `assets.name`），不参与 ViewMetadata JSON 构建。
> **Replace View 请求体**：`CommitViewRequest`（含 `requirements` + `updates`）。
> **同名冲突**：Create View 时若同 Namespace 下存在同名 Table，返回 `409 AlreadyExistsException`（"Table with same name already exists"）。现有 `uq_assets_active_name` 索引已保证此约束。

---

## 6. Lance REST API 设计

### 6.1 Domain 上下文与 ID 解析

Lance 端点从官方 `{id}` 解析 Domain，不增加 Quasar 自定义路径段。`{id}` 使用 `$` 分隔符：

```rust
// namespace id:
"$"                  => LanceNamespaceRef::Root          // 列出所有 Domain
"prod"               => LanceNamespaceRef::Domain { domain: "prod" }
"prod$analytics"     => LanceNamespaceRef::Namespace { domain: "prod", namespace: "analytics" }

// table id:
"prod$analytics$embeddings" => LanceTableRef { domain: "prod", namespace: "analytics", table: "embeddings" }
```

解析规则：
- `{id}="$"` 表示 Lance root namespace，列出所有 Domain。
- `{id}` 一段：Domain 的虚拟 Lance namespace。
- `{id}` 两段：Quasar `Domain + Namespace`。
- 表 `{id}` 必须三段：`Domain + Namespace + Table`。
- 更深层级返回官方 Lance `Unsupported` / `InvalidInput`。

### 6.2 端点 → Store 调用矩阵

| Endpoint | Adapter 职责 | Store trait 调用 | 错误格式 |
|----------|--------------|------------------|---------|
| `POST /v1/namespace/{id}/create` | `{id}` 一段创建 Domain，两段创建 Namespace | `DomainStore::create_domain` / `NamespaceStore::create_namespace` | RFC-7807 |
| `GET /v1/namespace/{id}/list` | root 列 Domain，一段列 Namespace | `DomainStore::list_domains` / `NamespaceStore::list_namespaces` | RFC-7807 |
| `POST /v1/namespace/{id}/describe` | 返回 Domain/Namespace 视图 | `DomainStore::get_domain` / `NamespaceStore::get_namespace` | RFC-7807 |
| `POST /v1/namespace/{id}/drop` | 删空 Domain/Namespace | `DomainStore::drop_domain` / `NamespaceStore::drop_namespace` | RFC-7807 |
| `POST /v1/namespace/{id}/exists` | 检查存在 | `domain_exists` / `namespace_exists` | RFC-7807 |
| `GET /v1/namespace/{id}/table/list` | 列 Lance 表 | `TabularStore::list_tabular_assets(..., Some("lance"), ...)` | RFC-7807 |
| `POST /v1/table/{id}/declare` | 声明表，分配 location/storage_options | `TabularStore::create_tabular_asset(..., "lance", ...)` | RFC-7807 |
| `POST /v1/table/{id}/describe` | 返回表信息（可指定版本） | `TabularStore::get_tabular_asset_with_current_version` | RFC-7807 |
| `POST /v1/table/{id}/register` | 注册已有 Lance 数据集 | `TabularStore::create_tabular_asset` | RFC-7807 |
| `POST /v1/table/{id}/deregister` | 注销但保留数据 | `AssetStore::drop_asset` | RFC-7807 |
| `POST /v1/table/{id}/drop` | 删除表 | `AssetStore::drop_asset` | RFC-7807 |
| `POST /v1/table/{id}/exists` | 检查 Lance 表存在 | `TabularStore::get_tabular_asset(..., "lance", ...)` | RFC-7807 |
| `POST /v1/table/{id}/rename` | 重命名（同 Domain 跨 Namespace） | `AssetStore::rename_asset` | RFC-7807 |
| `POST /v1/table/{id}/version/create` | 注册已写 S3 的版本 | `TabularVersionStore::create_tabular_version` | RFC-7807（409 `TableVersionAlreadyExists`） |
| `GET /v1/table/{id}/version/list` | 按 `version_order ASC` 列出 | `TabularVersionStore::list_tabular_versions` | RFC-7807 |
| `POST /v1/table/{id}/version/describe` | 返回版本详情 | `TabularVersionStore::get_tabular_version` | RFC-7807 |

### 6.3 Lance 版本语义

- **版本注册本质**：客户端通过 Lance SDK 写对象存储生成 manifest，再通知 Catalog 在 DB 记录版本。Catalog 不扫描数据文件，不验证 manifest 存在性。最终一致语义。
- **Lance 当前版本**：`version_order = version_id`，`version_key = version_id::text`；latest 通过 `version_order DESC` 查询。禁止从 `version_key` 字符串解析 fallback。
- **declare/register 不创建初始版本**：`current_version` 返回 null；显式 `create_table_version` 后才有版本记录。
- **location 分配**：`declare_table` 时若配置 `warehouse_path`，location = `{warehouse_path}/{namespace}/{table}/`；否则 `lance://{namespace}/{table}`（向后兼容）。`storage_options` 返回给客户端并持久化到 asset properties。

---

## 7. Unified API 设计

### 7.1 端点 → Store 调用矩阵

| Endpoint | Adapter 职责 | Store trait 调用 |
|----------|--------------|------------------|
| `GET /unified/v1/domains` | 列 Domain，脱敏 storage_config | `DomainStore::list_domains` |
| `POST /unified/v1/domains` | 创建 Domain | `DomainStore::create_domain` |
| `GET /unified/v1/domains/{domain}` | 获取 Domain，脱敏 | `DomainStore::get_domain` |
| `PATCH /unified/v1/domains/{domain}` | 更新 Domain（DomainPatch） | `DomainStore::update_domain` |
| `DELETE /unified/v1/domains/{domain}` | 删空 Domain | `DomainStore::drop_domain`（非空 → `DomainNotEmpty`） |
| `GET .../domains/{domain}/namespaces` | 列 Namespace | `NamespaceStore::list_namespaces` |
| `POST .../domains/{domain}/namespaces` | 创建 Namespace | `NamespaceStore::create_namespace` |
| `GET .../domains/{domain}/namespaces/{ns}` | 获取 Namespace | `NamespaceStore::get_namespace` |
| `DELETE .../domains/{domain}/namespaces/{ns}` | 删空 Namespace | `NamespaceStore::drop_namespace` |
| `PATCH .../domains/{domain}/namespaces/{ns}` | 更新 Namespace | `NamespaceStore::update_namespace` |
| `GET .../assets` | 列 Asset（format/name 过滤、分页） | `UnifiedQueryStore::list_assets_unified` |
| `POST .../assets` | **不提供创建**，返回 405 | `asset::create_asset_not_allowed` |
| `GET .../assets/{name}` | 获取 Asset（无需 format） | `UnifiedQueryStore::get_asset_unified` |
| `DELETE .../assets/{name}` | 删除 Asset | `AssetStore::drop_asset` |
| `PATCH .../assets/{name}` | 更新 comment/properties | `AssetStore::update_asset` |
| `POST .../assets/{name}/rename` | 重命名 | `AssetStore::rename_asset` |

### 7.2 Asset 响应字段

```rust
struct AssetResponse {
    id: String,
    name: String,
    asset_type: String,        // "table" / "view"
    format: Option<String>,    // 表资产为 "iceberg"/"lance"，非表为 None
    location: Option<String>,  // 非表为 None
    metadata_location: Option<String>,
    comment: Option<String>,
    properties: HashMap<String, String>,
    current_version: Option<CurrentVersionResponse>,
    created_at: String,
}
```

### 7.3 查询参数与分页

- 请求/响应 JSON 字段统一 `snake_case`。
- 分页：`pageToken` / `pageSize`（默认 100，最大 1000，超限返回 400 `PageSizeTooLarge`）。
- Asset 列表过滤：`?format=iceberg|lance`（可选）、`?name=users`（精确匹配，可选）。
- Asset 单资源操作：**无需 `format` 参数**（活动名唯一）。
- 列表排序稳定：Namespace 按 `name ASC`，Asset 按 `name ASC, format ASC`。
- PATCH 三态语义：`comment` 字段缺省不变 / `null` 清空 / 值覆盖；`properties` 增量更新（`removals` + `updates`）。

---

## 8. Commit 与 Metadata 设计

### 8.1 依赖与类型策略

Quasar 在 `quasar/adapter` 引入 `iceberg` crate 0.9.1 作为 metadata 兼容性基线：

- `quasar/core` 不依赖 `iceberg`，避免核心 store trait 与 Iceberg 实现细节耦合。
- `quasar/storage` 只存储 JSONB 和 catalog 指针，不依赖 `iceberg` 类型。
- `quasar/adapter` 用 `iceberg` crate 解析、构建、序列化 Iceberg table/view metadata。
- REST 请求/响应字段名由 Quasar DTO 控制，metadata 内部结构优先转换为 `iceberg` crate 类型或由其校验。

### 8.2 Table Commit 流程

**existing table commit**：
1. 读取 catalog 当前 `metadata_location`（`TabularStore::get_tabular_asset`）。
2. 从对象存储读取 metadata JSON。
3. `serde_json::from_value::<TableMetadata>()` 解析。
4. `TableRequirement::check()` 逐项校验 8 项 requirement。
5. `metadata.into_builder()` → `TableUpdate::apply()` 应用 updates → `build()`。
6. `serde_json::to_value()` 序列化新 metadata。
7. 生成新 `metadata_location`（`metadata::next_metadata_location`，版本号单调推进，无法解析时返回 500）。
8. 写新 metadata file 到对象存储。
9. `cas_update_metadata_location()` CAS 更新 catalog pointer（metadata_location + schema_snapshot + properties delta）。
10. 返回新 `LoadTableResponse`。

**staged create commit**：
1. 确认 active table 不存在。
2. 读取未过期 staged record（`IcebergStagingStore::get_staged_table`）。
3. 校验请求包含 `assert-create`（缺失 → 409 `CommitFailedException`）。
4. 从 staged metadata 起步，同上解析 → 校验 → 应用 updates → `build()`。
5. 写最终 metadata file 到对象存储。
6. `commit_staged_table()` 单事务插入 `assets` + `tabular_assets` 并删除 staged record。
7. 返回新 `LoadTableResponse`。

> 若 active table 不存在且无 staged record → 404 `NoSuchTableException`。staged commit 的 storage-NotFound 映射为 409 `CommitFailed`（并发输家语义）。

### 8.3 View Commit 流程

1. 读取 catalog 当前 `metadata_location`（`IcebergViewStore::get_view`）。
2. 从对象存储读取 View metadata JSON。
3. `serde_json::from_value::<ViewMetadata>()` 解析。
4. **adapter 层校验 `ViewRequirement`**（`check_view_requirements`）：
   - `AssertCreate`：View 不存在时才允许；Replace View 时若 View 已存在则失败。
   - `AssertViewUuid`：`metadata.uuid()` 必须等于请求 UUID。
5. `ViewMetadataBuilder::new_from_metadata(metadata)` → 逐项 match 调用 `ViewUpdate` 对应的 builder 方法 → `build()`。
6. 序列化新 metadata。
7. 生成新 `metadata_location`（版本号递增）。
8. 写新 metadata file 到对象存储。
9. `IcebergViewStore::commit_view()` CAS 更新 `view_assets.metadata_location`。
10. 返回 `GetViewResponse`。

### 8.4 View/Table Commit 模式对比

| 维度 | Table Commit | View Commit |
|------|-------------|-------------|
| Requirement 类型 | `TableRequirement`（iceberg crate 提供） | `ViewRequirement`（adapter 层自定义） |
| Update 类型 | `TableUpdate`（iceberg crate 提供） | `ViewUpdate`（iceberg crate 提供） |
| Builder | `TableMetadataBuilder` | `ViewMetadataBuilder` |
| Requirement 校验 | `req.check(Some(&metadata))` crate 内置 | adapter 层手动实现 `check_view_requirements()` |
| Update dispatch | `TableUpdate::apply(builder)` crate 内置 | 逐项 match 调用 builder 方法（`ViewUpdate` 无 `apply()`） |
| 初始构建 | `TableMetadataBuilder::new(schema, spec, sort_order, location, ...)` | `ViewMetadataBuilder::from_view_creation(view_creation)` |
| 增量构建 | `metadata.into_builder(None)` | `ViewMetadataBuilder::new_from_metadata(metadata)` |
| CAS Store 方法 | `CasCommitStore::cas_update_metadata_location` | `IcebergViewStore::commit_view` |
| Version History 清理 | 无 | `ViewMetadataBuilder::build()` 内置 `expire_versions()`（默认保留 10） |
| staged-create | 有 | 无 |
| 失败窗口 | 对象存储写成功 + DB CAS 失败 → 409 | 相同 |

### 8.5 Metadata 文件命名

- 新表/View 初始 metadata：`metadata/00001-{uuid}.metadata.json`（遵循 Iceberg 常见做法，version 从 `00001` 开始）。
- commit 后新 metadata location 必须单调推进 version。
- 若 `iceberg` crate 提供 metadata location helper，优先使用；若 crate helper 行为与 `00001` 不一致，以 crate 行为为准。
- 若当前 metadata location 无法解析，返回 `500 InternalServerError`，不得生成不可预测 fallback 路径。

### 8.6 Commit Requirements / Updates 覆盖

见 `docs/REQUIREMENTS.md` §5.2.7。要点：
- Table：8 项 requirement 全覆盖；已实现 21 项 update；明确拒绝 2 项 encryption key（501）；未知 action 400。
- View：2 项 requirement（adapter 自定义）；8 项 update（iceberg crate `ViewUpdate`）全覆盖；未知 action 400。

### 8.7 失败窗口

- 对象存储写成功但 PostgreSQL CAS 失败：客户端返回 5xx 或 409 `CommitFailedException`；服务端日志含 domain/namespace/table/view、old+new metadata_location；容忍孤儿 metadata file。
- PostgreSQL CAS 成功后响应发送失败：catalog 状态以 PostgreSQL 为准；客户端重试时通过 load 看到新 metadata_location。

---

## 9. 对象存储与 Purge 设计

### 9.1 Object Store Helper

`quasar/adapter/src/object_store_util.rs` 提供：

| helper | 用途 |
|--------|------|
| `read_json` | 读取对象存储 JSON 文件 |
| `write_json` | 写入 JSON 文件 |
| `object_exists` | 检查对象存在 |
| `list_prefix` | 列出 prefix 下对象 |
| `delete_objects` | 批量删除对象 |
| `delete_prefix` | list + delete 同步封装 |

所有 helper 接受已解析的 bucket-relative path，禁止在 SQL 或 handler 中拼接未校验路径。

### 9.2 Purge 安全边界

`purgeRequested=true` 只能删除满足以下条件的 prefix：

1. table location 可转换为当前配置 bucket 内 path。
2. path 位于配置的 warehouse prefix 下。
3. path 不是 bucket root、warehouse root 或空字符串。
4. path 与当前 table 的 metadata_location 同源。
5. path 必须等于 table location 或为其子目录，防止误删其他表的共享存储路径。
6. （可选增强）删除前校验对象存储中当前 metadata file 的 `table-uuid` 与 catalog 记录一致。

任一校验失败返回 `400 BadRequestException` 或 `500 InternalServerError`，不得执行删除。

### 9.3 Purge 执行顺序

可诊断的同步 purge：

1. 读取 table 并创建 `started` purge operation。
2. `begin_iceberg_purge_and_drop_catalog`（单事务）：读 table location + 创建 operation(status=`catalog_dropped`) + 删 catalog 记录。
3. 删除对象存储 table location prefix 下对象。
4. 删除成功 → `update_purge_operation(completed)`；失败 → `update_purge_operation(failed)`。
5. 返回 204（成功）或 5xx（失败）。

> 第 1-2 步必须由 `begin_iceberg_purge_and_drop_catalog` 在单个 PostgreSQL 事务中完成。handler 不得通过 `get_tabular_asset` + `drop_asset` + `update_purge_operation` 多次独立调用拼接该事务边界。
> 对象清理失败时返回 5xx，错误响应不包含 secret/access key/完整底层驱动错误；日志和 operation record 保留 table location、metadata_location 与脱敏原因。

---

## 10. Scan Planning 设计

### 10.1 路径 Alias 设计

Scan Planning 端点存在 OpenAPI 路径与 Java `ResourcePaths` 常量路径的差异：

| 端点 | OpenAPI 主路径 | Java ResourcePaths alias |
|------|---------------|--------------------------|
| Submit Plan | `.../namespaces/{ns}/tables/{table}/plan` | `.../tables/{table}/plan` |
| Fetch Plan | `.../namespaces/{ns}/tables/{table}/plan/{plan-id}` | `.../tables/{table}/plan/{plan-id}` |
| Cancel Plan | 同 Fetch Plan | 同 Fetch Plan |
| Fetch Tasks | `.../namespaces/{ns}/tables/{table}/tasks` | `.../tables/{table}/tasks` |

Quasar 同时支持两种路径：OpenAPI 为主路径，Java 常量路径为 alias（路由层兼容）。alias handler 从 `{prefix}` 和 `{table}` 推断 namespace（Iceberg Java `TableIdent.toUrlString()` 使用 `\x1F` unit separator 分隔 namespace 层级）。

> Java `ResourcePaths` alias 路径**不在 `/v1/config` endpoints 中声明**（Iceberg `Endpoint` enum 只定义 OpenAPI 路径版本）。alias 仅用于路由层兼容。
> **技术风险**：若 Java client 的 `{table}` 参数仅含单一 table name（不含 namespace），alias handler 无法推断 namespace，所有 alias 请求 400。此时 fallback 为放弃 alias 路由支持，仅在 `/v1/config` 声明 OpenAPI 主路径（不采纳"所有 alias 请求返回 400"方案，直接 404 更友好）。

### 10.2 Plan Task Token 策略

Quasar 采用**策略 B**：`plan-task` token 编码完整 FileScanTask 列表（JSON → base64 URL-safe）。

理由：
1. Scan Planning 是单次请求内完成实现（`submit_plan` handler 内部 `await` stream collect），无需跨请求状态管理。
2. 内存缓存方案引入服务端状态，与"无持久化"原则偏离。
3. 重新计算方案在 `/tasks` 端点重复执行 scan planning，性能开销不合理。

**Token 结构**：

```json
{
  "plan-id": "uuid",
  "table-id": { "namespace": ["..."], "name": "..." },
  "snapshot-id": 123456789,
  "filter": null,
  "split-size": 128000000,
  "tasks": [
    {
      "data-file": {
        "file-path": "s3://.../file.parquet",
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

> `FileScanTask` DTO 与 Iceberg REST OpenAPI 对齐（嵌套 `data-file` 结构，含可选 `delete-files`）。V4.2 初始实现 `delete-files` 为空列表，Positional delete file support 推迟。
> 若 FileScanTask 列表过大导致 token 超过合理大小（如 > 1MB），返回 `500 InternalServerError` 并建议客户端调整 `split-size`。

### 10.3 执行流程

**Submit Plan**：
1. 校验 Domain/Namespace/Table 存在且 format 为 `iceberg`。
2. 解析 `SubmitPlanRequest`（`snapshot-id`、`filter`、`case-sensitive`、`split-size`、`num-splits`、`columns`、`start-snapshot-id`、`end-snapshot-id`）。`snapshot-id` 未提供时使用 TableMetadata 的 `current-snapshot-id`。
3. 从对象存储读取 Table metadata JSON，定位指定 snapshot 的 Manifests。Table 无 snapshot → `404 NoSuchSnapshotException`。
4. 根据 `filter`、`split-size`、`columns` 计算FileScanTask splits（`scan.plan_files().await` + `stream.try_collect::<Vec<_>>().await`）。
5. 生成 `plan-id`（UUID）和 `plan-task` token（自包含）。
6. 返回 `SubmitPlanResponse`。

**Fetch Plan**：对已完成的 Plan 返回 `completed` + 同样的 `plan-task` token；对无效 `plan-id` 返回 404。

**Cancel Plan**：单次请求内完成实现下 Cancel 实际无效；返回 204（已完成）或 404（无效 plan-id）。

**Fetch Tasks**：base64 decode token → 解析 JSON → 提取 FileScanTask 列表返回。无 store 调用（token 自包含）。

### 10.4 简化限制

- `filter`：仅支持无 filter 或 Iceberg REST API 的 filter JSON 结构；复杂 filter 不做 Manifest-level 过滤优化。
- `split-size`：按 size 简单切分（大文件按 size 切，小文件合并）。
- `columns`：不裁剪列，返回完整 file schema。
- Plan 结果不持久化到 PostgreSQL，仅会话内有效。

---

## 11. 错误处理设计

### 11.1 StoreError（内部统一错误）

```rust
pub enum StoreError {
    NotFound(String),
    AlreadyExists(String),
    Conflict { msg: String },
    InvalidInput(String),
    NamespaceNotEmpty { namespace: String },
    DomainNotEmpty { domain: String },
    DatabaseUnavailable { source: Option<BoxSource> },
    Timeout { operation: String },
    Internal { msg: String, source: Option<BoxSource> },
}
```

| 变体 | HTTP 映射 |
|------|-----------|
| `NotFound` | 404 |
| `AlreadyExists` | 409 |
| `Conflict` | 409 |
| `InvalidInput` | 400 |
| `NamespaceNotEmpty` | 409 |
| `DomainNotEmpty` | 409 |
| `DatabaseUnavailable` | 503 |
| `Timeout` | 504 |
| `Internal` | 500（客户端脱敏） |

### 11.2 客户端安全分离

- **日志侧**：`tracing::error!(error = ?source, %msg, ...)` 保留完整 source。
- **客户端侧**（适配器层统一脱敏）：
  - `Internal { source }` → 客户端收到 `"An internal error occurred"`，不暴露 msg/source。
  - `DatabaseUnavailable` → `"Service temporarily unavailable"`（503）。
  - `Timeout` → `"Request timeout"`（504）。
  - 其他变体 → 原样传递 msg（已确保不含敏感信息）。

### 11.3 Adapter 错误映射

**Iceberg Adapter**（`store_error_to_iceberg_table` / `store_error_to_iceberg_view`）：

| StoreError | Iceberg error type | HTTP |
|------------|-------------------|------|
| `NotFound` | `NoSuchNamespaceException` / `NoSuchTableException` / `NoSuchViewException` | 404 |
| `AlreadyExists` | `*AlreadyExistsException` | 409 |
| `NamespaceNotEmpty` | `NamespaceNotEmptyException` | 409 |
| `DomainNotEmpty` | `NamespaceNotEmptyException` | 409 |
| `Conflict` | `CommitFailedException` | 409 |
| `InvalidInput` | `BadRequestException` | 400 |
| `DatabaseUnavailable` | `ServiceUnavailableException` | 503 |
| `Timeout` | `TimeoutException` / `InternalServerError` | 504 |
| `Internal` | `InternalServerError` | 500 |

Iceberg 错误响应格式：`{"error": {"message": "...", "type": "...", "code": N}}`。

**Lance Adapter**：RFC-7807 `{"error": "...", "code": N, "detail": "...", "instance": "..."}`，错误码如 `NamespaceNotFound`/`TableNotFound`/`TableAlreadyExists`/`TableVersionAlreadyExists`/`NamespaceNotEmpty`/`InvalidInput`。

**Unified Adapter**：RFC-7807 Problem Details `application/problem+json`，扩展 `code`（机器可读错误码）与 `request_id`（请求追踪 ID）。错误码枚举：`NamespaceNotFound`、`NamespaceAlreadyExists`、`NamespaceNotEmpty`、`DomainNotEmpty`、`AssetNotFound`、`AssetAlreadyExists`、`InvalidInput`、`InvalidFormat`、`InvalidPageToken`、`PageSizeTooLarge`、`MethodNotAllowed`、`Conflict`、`ServiceUnavailable`、`InternalError`。

> Unified API 的 Problem Details 错误格式不得泄漏到 `/iceberg/v1/...` 与 `/lance/v1/...` 标准协议端点。

### 11.4 字符串匹配消除

错误分类直接 `match StoreError` 变体，不通过 `msg.contains("not empty")` 或 `msg.starts_with("version ")` 字符串匹配判断错误类型。SQL 状态码（如 `RESTRICT_VIOLATION` / 唯一约束 23505）在 storage 层直接构造对应 `StoreError` 变体。

---

## 12. Feature Flag 与依赖设计

### 12.1 Feature 定义

| Feature | 默认 | 控制内容 | 不控制内容 |
|---------|------|---------|-----------|
| `lance` | ✅ | Lance REST 路由 + handler 编译 | — |
| `iceberg` | ✅ | Iceberg REST 路由 + handler 编译 | `object_store` / `iceberg` crate 可用性 |
| `unified` | ✅ | Unified REST 路由 + handler 编译 | 自动依赖 `lance` + `iceberg` |

### 12.2 实现机制

```toml
# adapter/Cargo.toml
[features]
default = ["lance", "iceberg", "unified"]
lance = []
iceberg = []
unified = ["lance", "iceberg"]

[dependencies]
object_store = { workspace = true }   # 公共依赖，非 optional
iceberg = { workspace = true }        # 公共依赖
```

```rust
// adapter/src/lib.rs
#[cfg(feature = "iceberg")] pub mod iceberg;
#[cfg(feature = "lance")]   pub mod lance;
#[cfg(feature = "unified")] pub mod unified;

// server/src/lib.rs
#[cfg(feature = "lance")]   { router = router.merge(quasar_adapter::lance::routes()); }
#[cfg(feature = "iceberg")] { router = router.merge(quasar_adapter::iceberg::routes()); }
#[cfg(feature = "unified")] { router = router.merge(quasar_adapter::unified::routes()); }
```

- `object_store` 是公共依赖（非 optional），`UnifiedConfig.object_store` 无条件编译。
- 测试文件顶部加 `#![cfg(feature = "...")]`，feature 关闭时测试不参与编译。

### 12.3 编译命令

```bash
cargo build                                              # 全部协议（默认）
cargo build --no-default-features --features lance       # 仅 Lance
cargo build --no-default-features --features iceberg     # 仅 Iceberg
cargo build --no-default-features                        # 仅基础设施端点
```

---

## 13. Server 组装与配置

### 13.1 AppConfig

```rust
#[derive(Clone, Default)]
pub struct AppConfig {
    #[cfg(feature = "lance")]   pub lance: LanceConfig,
    #[cfg(feature = "iceberg")] pub iceberg: IcebergConfig,
    #[cfg(feature = "unified")] pub unified: UnifiedConfig,
}

pub struct IcebergConfig {
    pub warehouse_path: Option<String>,
    pub object_store: Option<Arc<dyn ObjectStore>>,
    pub s3_bucket: Option<String>,
    pub default_warehouse: String,   // multi-warehouse 参数校验
}

pub struct LanceConfig {
    pub warehouse_path: Option<String>,
    pub storage_options: HashMap<String, String>,
}

pub struct UnifiedConfig {
    pub object_store: Option<Arc<dyn ObjectStore>>,
    pub s3_bucket: Option<String>,
}
```

### 13.2 路由组装

`create_app_with_config(pool, config)`：
1. 创建 `PgCatalogStore::new(pool)` 并 `initialize()` 加载 schema。
2. 注册 `/healthz`、`/readyz`。
3. 按 feature 合并 `iceberg::routes()` / `lance::routes()` / `unified::routes()`，通过 `Extension` 注入对应 config。
4. `tower_http::TraceLayer` 请求日志中间件。
5. `with_state(Arc<dyn CatalogStore>)`。

### 13.3 main 流程

1. `Config::from_env()` 加载环境变量配置。
2. 初始化 `tracing_subscriber`（EnvFilter 回退到 `QUASAR_LOG_LEVEL` / `RUST_LOG`）。
3. 构建 deadpool-postgres 连接池。
4. 创建 `PgCatalogStore` 并 `initialize()` 加载 schema。
5. 按 feature 构建 `IcebergConfig`（含 S3 `AmazonS3Builder`，MinIO path-style）、`LanceConfig`、`UnifiedConfig`。
6. `create_app_with_config`。
7. `axum::serve(listener, app).with_graceful_shutdown(shutdown_signal())`。

### 13.4 优雅关机

`shutdown_signal()`：监听 Ctrl+C（`tokio::signal::ctrl_c`）+ SIGTERM（Unix `signal::unix::SignalKind::terminate`）。收到信号后停止 accept，等待存量请求处理完毕，关闭连接池，退出。

### 13.5 健康检查

- `GET /healthz`：存活检查，始终返回 `{"status":"ok"}`。
- `GET /readyz`：就绪检查，执行 `SELECT 1`；DB 不可达返回 503。

### 13.6 部署形态

- 单二进制 + PostgreSQL 实例；无状态，支持水平扩展。
- 容器化：多阶段构建 Dockerfile（rust:1.86-slim → debian:bookworm-slim），暴露 8080 端口。
- 部署与验证流程见 `docs/DEPLOYMENT.md`（最小部署 / Lance 集成环境 / Spark 集成环境）。

---

## 附录：关键文件索引

| 文件 | 职责 |
|------|------|
| `quasar/core/src/models.rs` | Domain / Namespace / Asset / TabularAsset / ViewAsset / AssetVersion / PatchField / ViewIdentifier |
| `quasar/core/src/store.rs` | 14 个 trait 定义 + CatalogStore / IcebergCatalogStore marker |
| `quasar/core/src/error.rs` | StoreError 枚举 |
| `quasar/core/src/validation.rs` | 名称校验 |
| `quasar/storage/src/schema/init.sql` | 幂等数据库初始化脚本 |
| `quasar/storage/src/schema/mod.rs` | schema 加载逻辑 |
| `quasar/storage/src/store.rs` | PgCatalogStore 实现全部 trait |
| `quasar/storage/src/queries.rs` | SQL 常量集中管理 |
| `quasar/adapter/src/iceberg/mod.rs` | Iceberg 路由 + IcebergConfig + validate_warehouse + config handler |
| `quasar/adapter/src/iceberg/metadata.rs` | Table metadata wrapper（parse/build/apply_commit/next_location） |
| `quasar/adapter/src/iceberg/view_metadata.rs` | View metadata wrapper + ViewRequirement |
| `quasar/adapter/src/iceberg/table_metadata.rs` | V3 自研 TableMetadata DTO（历史保留，不再作为权威） |
| `quasar/adapter/src/iceberg/table.rs` | Table handler（create/register/load/commit/drop/rename/metrics/transaction） |
| `quasar/adapter/src/iceberg/view.rs` | View handler（7 端点） |
| `quasar/adapter/src/iceberg/scan_planning.rs` | Scan Planning handler + token 编解码 |
| `quasar/adapter/src/iceberg/dto.rs` | Iceberg REST DTO |
| `quasar/adapter/src/iceberg/error.rs` | IcebergError + store error 映射 |
| `quasar/adapter/src/object_store_util.rs` | 对象存储 helper |
| `quasar/adapter/src/lance/mod.rs` | Lance 路由 + LanceConfig |
| `quasar/adapter/src/lance/id.rs` | Lance {id} 解析 |
| `quasar/adapter/src/unified/mod.rs` | Unified 路由 + UnifiedConfig |
| `quasar/adapter/src/unified/domain.rs` / `asset.rs` / `namespace.rs` | Unified handler |
| `quasar/server/src/main.rs` | main 函数 + 优雅关机 |
| `quasar/server/src/config.rs` | 环境变量配置 |
| `quasar/server/src/health.rs` | /healthz /readyz |
| `quasar/server/src/lib.rs` | create_app_with_config |
