# Quasar MVP 设计文档（V1.0）

> 本文档整合《项目定位与设计目标》、《V1 (MVP) 需求澄清与分析》、《核心模型设计》三份文档，为编码阶段提供可直接参考的完整设计规格。

---

## 目录

1. [项目概述](#1-项目概述)
2. [MVP 范围](#2-mvp-范围)
3. [系统架构](#3-系统架构)
4. [数据模型](#4-数据模型)
5. [协议端点设计](#5-协议端点设计)
6. [Crate 分层与模块职责](#6-crate-分层与模块职责)
7. [核心接口定义](#7-核心接口定义)
8. [并发控制与一致性](#8-并发控制与一致性)
9. [错误处理规范](#9-错误处理规范)
10. [配置与部署](#10-配置与部署)
11. [开发阶段划分](#11-开发阶段划分)
12. [模块视角分工参考](#12-模块视角分工参考)

---

## 1. 项目概述

### 1.1 定位

Quasar 是一个面向 Lakehouse 架构的、独立的通用 Catalog Service 组件。它是可纳管多种数据资产的元数据底座。

### 1.2 可纳管资产类型（MVP 及未来）

| 阶段 | 资产类型 | 协议 |
|------|---------|------|
| MVP Phase 1 | Lance 表 | Lance REST Namespace Spec |
| MVP Phase 2 | Iceberg 表 | Iceberg REST Catalog Spec |
| 未来扩展 | AI 模型、特征（Feature）、Delta Lake 表等 | 待定 |

### 1.3 设计目标

1. **协议开放**：对每种资产类型，忠实实现其上游标准协议。Iceberg 遵循 Iceberg REST Catalog 规范，Lance 遵循 Lance REST Namespace 规范，确保各自生态的引擎和客户端零改动接入。
2. **模型通用**：核心对象模型不绑定任何单一数据格式，任何资产类型都可以注册到 Catalog 中。
3. **一致性保障**：对 Iceberg 等需要原子提交的资产类型，通过乐观并发控制保证元数据更新的正确性。
4. **性能优先**：采用 Rust + axum + tokio-postgres 构建，追求低延迟、高并发、小内存占用。
5. **部署简洁**：服务本身无状态，单二进制部署，依赖单一 PostgreSQL 实例即可运行。
6. **可扩展**：通过清晰的 crate 分层，后续新增资产类型或接入协议只需扩展对应 crate，不侵入核心。

---

## 2. MVP 范围

### 2.1 交付阶段

MVP 分两个阶段交付。核心模型和 crate 分层从一开始就按双协议设计，但端点实现分阶段推进。

| 阶段 | 功能范围 | 里程碑 |
|------|---------|--------|
| **Phase 1** | Lance REST Namespace（F4）+ Namespace 管理（F2）+ 基础设施端点（F6） | Lance SDK（Python）和 Spark（lance-spark）端到端跑通 |
| **Phase 2** | Iceberg REST Catalog（F1 + F3）+ Spark 集成验证（F5） | Spark 通过 Iceberg REST Catalog 端到端读写 Iceberg 表 |

### 2.2 功能需求汇总

| ID | 需求 | 阶段 | 说明 |
|----|------|------|------|
| F1 | Iceberg REST Catalog 协议支持 | Phase 2 | 核心端点，路径前缀 `/iceberg/v1/...` |
| F2 | Namespace 管理 | Phase 1 | 创建、删除、列出 Namespace。**限定单级** |
| F3 | Iceberg Table 元数据管理 | Phase 2 | 存储 metadata-location、snapshot-id、schema 等。**必须实现乐观并发控制** |
| F4 | Lance REST Namespace 协议支持 | Phase 1 | 基础操作及版本管理端点，路径前缀 `/lance/v1/...` |
| F5 | Spark 集成验证（Iceberg） | Phase 2 | 测试脚本验证 Spark 3.4+ 通过 Iceberg REST Catalog 读写 |
| F6 | 基础设施端点 | Phase 1 | `GET /healthz`、`GET /readyz`（含 DB 连通性验证） |

### 2.3 非功能需求

| ID | 需求 | 说明 |
|----|------|------|
| NF1 | 技术栈 | Rust（axum + tokio-postgres + deadpool-postgres + refinery） |
| NF2 | 无状态 | 服务不依赖本地状态，可水平扩展 |
| NF3 | 无鉴权 | MVP 阶段不引入认证授权 |
| NF4 | 单租户 | MVP 阶段不引入租户隔离 |
| NF5 | 可观测性 | 集成 `tracing` 打印结构化日志 |
| NF6 | 数据库迁移 | 使用 refinery 管理 DDL 版本 |
| NF7 | 配置管理 | 环境变量加载配置，支持 `.env` 文件 |
| NF8 | 优雅关机 | SIGTERM 后停止接受新请求，等待存量请求处理完毕后退出 |

---

## 3. 系统架构

### 3.1 双协议架构

```
                      +---------------------------------------+
                      |            Quasar Server               |
                      |                                        |
  Spark / Trino       |   /iceberg/v1/...                      |
  Flink (Iceberg)  --->|   +---------------------+             |
                      |   |  Iceberg REST        |             |
                      |   |  Catalog Adapter     |--+          |
                      |   +---------------------+  |          |
                      |                            |          |
                      |                     +--------------+  |
                      |                     |  Core Model   |  |
                      |                     |  + Storage    |  |
                      |                     |  (PostgreSQL) |  |
                      |                     +--------------+  |
                      |                            ^          |
  Lance SDK           |   +---------------------+  |          |
  (Python/Rust/Java)-->|   |  Lance REST         |--+          |
  Spark (Lance)       |   |  Namespace Adapter   |             |
  Ray / Trino         |   +---------------------+             |
                      |   /lance/v1/...                       |
                      +---------------------------------------+
```

### 3.2 Namespace 隔离策略

- 通过 Iceberg 端点创建的 Namespace/Table 只在 `/iceberg/v1/...` 下可见。
- 通过 Lance 端点创建的 Namespace/Table 只在 `/lance/v1/...` 下可见。
- 两个空间中**允许同名**，互不干扰。
- 核心模型层面，隔离通过两层约束实现：`namespaces` 表的 `UNIQUE(name, format)` 确保同格式下 Namespace 名唯一；`assets` 表的 `UNIQUE(namespace_id, name)` 确保同 Namespace 下表名唯一。由于 Namespace 已绑定 format，两者组合等效于 `(format, namespace_name, table_name)` 全局唯一。

### 3.3 与业界方案对比

| 维度 | Apache Gravitino | Unity Catalog | Hive Metastore | **Quasar** |
|------|-----------------|---------------|----------------|-----------|
| 层级结构 | `Metalake → Catalog → Schema → Table` | `Catalog → Schema → Table` | `Database → Table` | **`Namespace → Asset`** |
| 最小路径 | `metalake.catalog.schema.table` | `catalog.schema.table` | `database.table` | `namespace.asset` |
| 存储方式 | 代理模式（连接 HMS/Iceberg REST 等） | 自包含 | 自包含 | **自包含** |
| 格式划分 | `Catalog.type` | Schema 级 | 无 | **`Namespace.format`** |

Quasar 选择扁平化层级的原因：
- **没有异构后端**：所有元数据直接存在 PostgreSQL 中，不存在 connector 路由。
- **协议即入口**：`/iceberg/` vs `/lance/` 天然区分格式，无需额外的 Catalog 层。
- **路径更短、查询更简单、部署更轻量**。

---

## 4. 数据模型

### 4.1 设计原则

1. **格式无关的核心抽象**：核心实体不绑定任何具体数据格式。
2. **指针而非内容**：Catalog 只存储指向实际数据/元数据的指针，不存储数据文件内容。
3. **扁平化层级**：`Namespace → Asset` 两层结构，MVP 限定单级 Namespace。
4. **自包含存储**：所有元数据直接持久化在 PostgreSQL 中。
5. **并发安全**：Iceberg CAS + Lance 唯一性约束，均利用 PostgreSQL 原子性。
6. **预留扩展位**：JSONB 字段和 `format` 枚举为未来新增资产类型预留空间。

### 4.2 实体关系

```
Namespace (1) ------< (N) Asset (1) ------< (N) AssetVersion

- 一个 Namespace 下有多张 Asset（表）
- 一张 Asset（Lance 格式）有多个版本记录
- Iceberg 格式的 Asset 不创建 AssetVersion 记录（版本信息在 metadata.json 中自描述）
- Namespace 仅允许在为空（无下属 Asset）时删除（ON DELETE RESTRICT）
- Asset 删除时级联删除其下所有 AssetVersion 记录
- Asset 的格式由所属 Namespace 的 format 决定，Asset 自身不重复存储 format
```

### 4.3 PostgreSQL DDL

```sql
-- 扩展：UUID 生成
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- namespaces: 资产命名空间
-- ============================================
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    format TEXT NOT NULL CHECK (format IN ('iceberg', 'lance')),
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN namespaces.format IS
    '资产格式标识，当前支持 iceberg / lance，未来可扩展';

-- ============================================
-- assets: 统一资产实体（表）
-- ============================================
CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    metadata_location TEXT,             -- Iceberg 专用：当前 metadata.json URI
    schema_snapshot JSONB,              -- 可选缓存的 Schema 快照
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (namespace_id, name)
);

CREATE INDEX idx_assets_namespace ON assets(namespace_id);

COMMENT ON COLUMN assets.location IS
    '表的数据基础路径。Iceberg: warehouse 路径；Lance: 数据集根目录';
COMMENT ON COLUMN assets.metadata_location IS
    'Iceberg 专用：当前生效的 metadata.json URI，CAS 校验的比较目标。Lance 始终为 NULL';

-- ============================================
-- asset_versions: Lance 版本记录
-- ============================================
CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_id BIGINT NOT NULL,
    metadata_location TEXT NOT NULL,
    previous_version_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (asset_id, version_id)
);

CREATE INDEX idx_versions_asset ON asset_versions(asset_id);

COMMENT ON TABLE asset_versions IS
    '仅 Lance 格式使用。客户端写入数据后将 manifest 路径注册到此表。Iceberg 版本历史自包含在 metadata.json 链中。';
```

### 4.4 索引设计

| 表 | 索引 | 用途 |
|---|---|---|
| namespaces | `UNIQUE(name)` | 防止同名 Namespace 重复 |
| assets | `UNIQUE(namespace_id, name)` | 防止同 Namespace 下同名表 |
| assets | `idx_assets_namespace` | 加速 `list assets in namespace` 查询 |
| asset_versions | `UNIQUE(asset_id, version_id)` | 防止同一表的版本号重复（并发控制） |
| asset_versions | `idx_versions_asset` | 加速 `list versions` 查询 |

### 4.5 核心实体 Rust 定义

```rust
pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub format: AssetFormat,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AssetFormat {
    Iceberg,
    Lance,
}

impl AssetFormat {
    pub fn as_str(&self) -> &'static str {
        (*self).into()
    }
}

pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub location: String,
    pub metadata_location: Option<String>,
    pub schema_snapshot: Option<serde_json::Value>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub version_id: i64,
    pub metadata_location: String,
    pub previous_version_id: Option<i64>,
    pub timestamp: DateTime<Utc>,
}
```

---

## 5. 协议端点设计

### 5.1 Iceberg REST Catalog 端点（Phase 2）

路径前缀：`/iceberg/v1/...`

Spark 等引擎配置时设置 `uri=http://host:port/iceberg`，引擎自动拼接 `/v1/...`。

**MVP 必须实现：**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/config` | 返回 Catalog 配置 |
| GET | `/v1/namespaces` | 列出 Namespace（支持 `pageToken` / `pageSize`） |
| POST | `/v1/namespaces` | 创建 Namespace |
| GET | `/v1/namespaces/{ns}` | 获取 Namespace 详情及 properties |
| DELETE | `/v1/namespaces/{ns}` | 删除 Namespace（仅允许删除空 Namespace） |
| POST | `/v1/namespaces/{ns}/properties` | 更新 Namespace properties |
| GET | `/v1/namespaces/{ns}/tables` | 列出 Table（支持 `pageToken` / `pageSize`） |
| POST | `/v1/namespaces/{ns}/tables` | 创建 Table（接收 metadata-location 或内联 metadata） |
| GET | `/v1/namespaces/{ns}/tables/{table}` | 加载 Table |
| POST | `/v1/namespaces/{ns}/tables/{table}` | 提交 Table 更新（**校验 requirements 实现 CAS**） |
| DELETE | `/v1/namespaces/{ns}/tables/{table}` | 删除 Table |
| HEAD | `/v1/namespaces/{ns}/tables/{table}` | 检查 Table 是否存在 |
| POST | `/v1/tables/rename` | 重命名 Table（同 Namespace 内） |

**MVP 明确不实现：** 多表事务、View 相关端点、OAuth 认证、指标上报。

### 5.2 Lance REST Namespace 端点（Phase 1）

路径前缀：`/lance/v1/...`

Lance 客户端配置时设置 `uri=http://host:port/lance`，SDK 自动拼接 `/v1/...`。

MVP 阶段限定单级 Namespace，`{id}` 即为 Namespace 名称本身。

**Namespace 操作：**

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/namespace/{id}/create` | 创建 Namespace |
| GET | `/v1/namespace/{id}/list` | 列出子 Namespace（支持 `page_token` / `limit`） |
| POST | `/v1/namespace/{id}/describe` | 获取 Namespace 详情及 properties |
| POST | `/v1/namespace/{id}/drop` | 删除 Namespace（仅允许删除空 Namespace） |
| POST | `/v1/namespace/{id}/exists` | 检查 Namespace 是否存在 |

**Table 基础操作：**

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/table/{id}/declare` | 声明 Lance 表（预留表名，返回 location 和 storage_options） |
| GET | `/v1/namespace/{id}/table/list` | 列出指定 Namespace 下的 Lance 表 |
| POST | `/v1/table/{id}/describe` | 获取 Lance 表详情（location、schema、version） |
| POST | `/v1/table/{id}/register` | 注册已有 Lance 表（提供 location） |
| POST | `/v1/table/{id}/deregister` | 注销 Lance 表（保留存储上的数据文件） |
| POST | `/v1/table/{id}/drop` | 删除 Lance 表（同时移除数据） |
| POST | `/v1/table/{id}/exists` | 检查 Lance 表是否存在 |
| POST | `/v1/table/{id}/rename` | 重命名 Lance 表 |

**Table 版本管理：**

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/table/{id}/version/create` | 注册新版本。若版本号已存在则返回 `409 Conflict` |
| GET | `/v1/table/{id}/version/list` | 列出版本历史（支持 `descending` / `limit`） |
| POST | `/v1/table/{id}/version/describe` | 获取指定版本的详细信息 |

**MVP 明确不实现：** 数据面操作（Insert/Query 等）、批量版本操作、Index/Tag/事务管理、Schema 变更、ListAllTables、CreateTable（含初始数据）。

### 5.3 基础设施端点（Phase 1）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/healthz` | 存活检查（服务进程是否运行） |
| GET | `/readyz` | 就绪检查（含 DB 连通性验证） |

---

## 6. Crate 分层与模块职责

```
quasar/
├── core/                # 核心领域模型与 trait 定义
│                        # - Namespace / Asset / AssetVersion 等领域对象
│                        # - CatalogStore trait（存储抽象）
│                        # - 协议无关的错误类型
│                        # - 不依赖具体框架或存储实现
│
├── storage/             # 存储层实现
│                        # - PostgreSQL 实现 CatalogStore trait
│                        # - 连接池管理（deadpool-postgres）
│                        # - 数据库 schema 迁移（refinery）
│
├── adapter/             # 协议适配层
│   ├── iceberg/         # Iceberg REST Catalog 适配（需启用 `iceberg` feature）
│   │                    # - /iceberg/v1/... 路由与 handler
│   │                    # - 请求/响应 DTO（严格遵循 Iceberg REST Spec）
│   │                    # - TableMetadata 解析、updates 应用、序列化
│   │                    # - CAS commit 协调
│   │                    # - 查询时自动注入 format=iceberg 过滤条件
│   │
│   └── lance/           # Lance REST Namespace 适配（需启用 `lance` feature）
│                        # - /lance/v1/... 路由与 handler
│                        # - 请求/响应 DTO（严格遵循 Lance REST Namespace Spec）
│                        # - DeclareTable / Register / Version 等特有语义
│                        # - 查询时自动注入 format=lance 过滤条件
│                        # - RFC-7807 错误响应格式
│
└── server/              # 服务入口
                         # - axum 路由注册与中间件（tracing、错误处理）
                         # - 配置加载（环境变量 / .env 文件）
                         # - Health / Readiness 端点
                         # - main 函数与优雅关机
```

### 6.1 依赖关系

```
server ──► adapter ──► core
  │                      ▲
  └──► storage ──────────┘
```

- `core` 不依赖任何具体框架或存储实现，只包含数据结构和 trait 定义。
- `storage` 依赖 `core`，实现 `CatalogStore` trait。
- `adapter` **只依赖 `core`**，通过 `CatalogStore` trait 操作存储，不感知具体实现是 PostgreSQL 还是其他后端。
- `server` 依赖 `adapter`、`storage` 和 `core`，负责创建 `PgCatalogStore`（来自 storage）并将其作为 `Arc<dyn CatalogStore>` 注入到 adapter 的 handler 中。

### 6.2 条件编译（Cargo Features）

`adapter` 和 `server` crate 均定义 `lance` 和 `iceberg` 两个 Cargo feature（默认全部启用）：

| Feature | 默认 | 控制内容 | 附加依赖 |
|---------|------|---------|---------|
| `lance` | ✅ | `adapter/src/lance/` 模块及路由 | 无 |
| `iceberg` | ✅ | `adapter/src/iceberg/` 模块及路由 | `object_store` |

**编译命令示例：**

```bash
# 全部协议（默认）
cargo build

# 仅 Lance
cargo build --no-default-features --features lance

# 仅 Iceberg
cargo build --no-default-features --features iceberg
```

**实现机制：**

1. `adapter/src/lib.rs` 使用 `#[cfg(feature = "...")]` 条件编译模块
2. `server/src/lib.rs` 使用 `#[cfg(feature = "...")]` 条件合并路由
3. `adapter/tests/` 和 `server/tests/` 中的协议相关测试使用 `#![cfg(feature = "...")]` 条件编译
4. `object_store` 在 `Cargo.toml` 中标记为 `optional = true`，仅在 `iceberg` feature 启用时引入

---

## 7. 核心接口定义

### 7.1 CatalogStore Trait

```rust
#[async_trait]
pub trait CatalogStore: Send + Sync {
    // ── Namespace ──────────────────────────────
    async fn create_namespace(
        &self, name: &str, format: AssetFormat, properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self, format: AssetFormat, offset: i64, limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, name: &str, format: AssetFormat) -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, name: &str, format: AssetFormat) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, name: &str, format: AssetFormat) -> Result<(), StoreError>;

    /// 增量更新 properties：removals 删除指定 key，updates 新增/覆盖指定 key-value
    /// 对应 Iceberg REST 的 updateProperties 端点语义
    async fn update_namespace_properties(
        &self,
        name: &str,
        format: AssetFormat,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    // ── Asset ──────────────────────────────────
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
    ) -> Result<Asset, StoreError>;

    async fn list_assets(
        &self, namespace_name: &str, format: AssetFormat,
    ) -> Result<Vec<Asset>, StoreError>;

    async fn get_asset(
        &self, namespace_name: &str, format: AssetFormat, name: &str,
    ) -> Result<Asset, StoreError>;

    /// Get asset with its current version in a single query.
    /// Returns (Asset, Option<AssetVersion>) - version may be None if no versions exist.
    async fn get_asset_with_current_version(
        &self, namespace_name: &str, format: AssetFormat, name: &str,
    ) -> Result<(Asset, Option<AssetVersion>), StoreError>;

    async fn asset_exists(
        &self, namespace_name: &str, format: AssetFormat, name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_asset(
        &self, namespace_name: &str, format: AssetFormat, name: &str,
    ) -> Result<(), StoreError>;

    async fn rename_asset(
        &self, namespace_name: &str, format: AssetFormat, name: &str, new_name: &str,
    ) -> Result<(), StoreError>;

    // ── Version ────────────────────────────────
    async fn load_version(
        &self, namespace_name: &str, format: AssetFormat, asset_name: &str, version_id: i64,
    ) -> Result<AssetVersion, StoreError>;

    async fn load_current_version(
        &self, namespace_name: &str, format: AssetFormat, asset_name: &str,
    ) -> Result<AssetVersion, StoreError>;

    async fn list_versions(
        &self, namespace_name: &str, format: AssetFormat, asset_name: &str,
    ) -> Result<Vec<AssetVersion>, StoreError>;

    /// Create a version record.
    /// If `previous_version_id` is Some, performs CAS check against the current
    /// latest version before inserting. Returns Conflict if the check fails.
    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersion, StoreError>;

    /// Atomically update metadata_location if it matches the expected value.
    /// Returns Conflict if the current location does not match.
    async fn cas_update_metadata_location(
        &self,
        namespace_name: &str,
        asset_name: &str,
        format: AssetFormat,
        expected_location: &str,
        new_location: &str,
        new_schema_snapshot: Option<serde_json::Value>,
    ) -> Result<(), StoreError>;
}
```

### 7.2 StoreError

```rust
pub enum StoreError {
    NotFound(String),
    AlreadyExists(String),
    Conflict(String),
    InvalidInput(String),
    Internal(String),
}
```

### 7.4 Lance 端点路径参数解析规则

Lance REST Namespace 规范使用 `$` 作为默认分隔符将多级 Namespace 标识化序列化到路径中。
MVP 阶段限定单级 Namespace，`{id}` 的解析规则：

- 路径参数 `{id}` 不包含 `$` 时，直接作为 Namespace/Table 名称。
- 包含 `$` 时，按 `$` 分割为多级标识符（预留，MVP 不启用多级）。

### 7.5 Lance 表完整标识符格式

Lance 表中 `{id}` 的完整格式为 `{namespace_name}${table_name}`，如 `prod$users`。
MVP 阶段单级 Namespace 下，`namespace_name` 即为 Namespace 名，`table_name` 为表名。

---

## 8. 并发控制与一致性

### 8.1 Iceberg 乐观并发控制（CAS）

完整流程：

1. 客户端发送 Commit 请求，携带 `requirements`（断言条件）和 `updates`（变更操作列表）。
2. **Iceberg 适配器从对象存储加载当前 metadata.json，反序列化为 TableMetadata 对象**。
3. **适配器校验 requirements 是否满足**（如断言 metadata_location 仍为 V1）。
4. **适配器将 updates 逐一应用到 TableMetadata 上，生成新的 TableMetadata 对象**。
5. **适配器将新 TableMetadata 序列化为新的 metadata.json（V2），写入对象存储**（路径由适配器决定，通常为递增编号或 UUID）。
6. 适配器调用 `CatalogStore.cas_update_metadata_location()`，以 `metadata_location = V1` 为 CAS 条件，原子更新数据库中的 `metadata_location` 为 V2 及 `schema_snapshot`。
7. 若 CAS 条件不满足（metadata_location 已不是 V1），返回 `409 Conflict`。

**CAS SQL：**

```sql
UPDATE assets
SET metadata_location = $new_metadata_location,
    schema_snapshot = COALESCE($new_schema_snapshot, schema_snapshot)
WHERE namespace_id = (SELECT id FROM namespaces WHERE name = $namespace_name AND format = $format)
  AND name = $asset_name
  AND metadata_location = $expected_metadata_location;
```

应用层检查 `UPDATE` 影响的行数：
- 行数 = 1 → 成功
- 行数 = 0 → 冲突，返回 `409 Conflict`

**关键决策**：Quasar 选择自己实现 TableMetadata 的解析、updates 应用和 metadata.json 序列化逻辑，不依赖外部库（如 `iceberg-rust`）的 TableMetadata 能力。这确保了协议语义的完全可控。

### 8.2 Lance 版本唯一性约束

```sql
INSERT INTO asset_versions (asset_id, version_id, metadata_location, previous_version_id)
VALUES ($1, $2, $3, $4);
```

若违反 `UNIQUE(asset_id, version_id)` → 返回 `409 Conflict`（`TableVersionAlreadyExists`）

### 8.3 Namespace 属性增量更新

```sql
UPDATE namespaces
SET properties = (properties - $removals::text[]) || $updates::jsonb
WHERE name = $name AND format = $format;
```

单条 SQL 原子完成移除和新增，无需 read-modify-write，避免并发更新互相覆盖。

---

## 9. 错误处理规范

### 9.1 两层错误体系

Quasar 内部使用统一的错误类型，各协议适配器在返回 HTTP 响应时映射为各自规范定义的错误格式。

### 9.2 Iceberg 端点错误格式

遵循 Iceberg REST 规范定义的 `ErrorResponse`：

```json
{
  "error": {
    "message": "Table already exists: prod.users",
    "type": "AlreadyExistsException",
    "code": 409
  }
}
```

**状态码映射：**

| 场景 | HTTP 状态码 | error.type |
|------|------------|------------|
| Namespace/Table 已存在 | 409 | `AlreadyExistsException` |
| Namespace/Table 不存在 | 404 | `NoSuchNamespaceException` / `NoSuchTableException` |
| CAS 冲突 | 409 | `CommitFailedException` |
| 请求参数无效 | 400 | `BadRequestException` |
| 内部错误 | 500 | `InternalServerError` |

### 9.3 Lance 端点错误格式

遵循 RFC-7807 `Problem Details`：

```json
{
  "error": "NamespaceNotFound",
  "code": 404,
  "detail": "Namespace 'prod' not found",
  "instance": "/lance/v1/namespace/prod/describe"
}
```

**状态码映射：**

| 场景 | HTTP 状态码 | error |
|------|------------|-------|
| Namespace/Table 已存在 | 409 | `NamespaceAlreadyExists` / `TableAlreadyExists` |
| Namespace/Table 不存在 | 404 | `NamespaceNotFound` / `TableNotFound` |
| 版本已存在 | 409 | `TableVersionAlreadyExists` |
| 请求参数无效 | 400 | `InvalidInput` |
| Namespace 非空 | 409 | `NamespaceNotEmpty` |
| 内部错误 | 500 | `InternalError` |

### 9.4 统一内部错误到 HTTP 的映射策略

```
StoreError::NotFound      → 404
StoreError::AlreadyExists → 409
StoreError::Conflict      → 409
StoreError::InvalidInput  → 400
StoreError::Internal      → 500
```

适配器层负责将 `StoreError` 转换为各自协议的错误响应格式。

---

## 10. 配置与部署

### 10.1 配置项（环境变量）

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_DATABASE_URL` | 是 | - | PostgreSQL 连接串 |
| `QUASAR_LISTEN_ADDR` | 否 | `0.0.0.0:8080` | HTTP 监听地址 |
| `QUASAR_WAREHOUSE_PATH` | 否 | - | 默认 warehouse 路径（如 `s3://bucket/warehouse/`） |
| `QUASAR_LOG_LEVEL` | 否 | `info` | tracing 日志级别 |
| `RUST_LOG` | 否 | - | 标准 RUST_LOG 环境变量 |

### 10.2 部署形态

- 单二进制文件 + PostgreSQL 实例
- 无状态服务，支持水平扩展（多实例共享同一个 PostgreSQL）
- 容器化：提供 Dockerfile，暴露 8080 端口
- 健康检查：`/healthz`（存活）、`/readyz`（就绪，含 DB 连通性）

### 10.3 条件编译部署

通过 Cargo feature flags 可以编译只包含特定协议的 binary：

```bash
# 仅 Lance（最小 binary，无对象存储依赖）
cargo build --release --no-default-features --features lance

# 仅 Iceberg（包含 object_store + AWS SDK）
cargo build --release --no-default-features --features iceberg

# 全部协议（默认）
cargo build --release
```

Docker 构建时可通过 `--build-arg` 传递 feature 选择：

```dockerfile
ARG FEATURES="lance,iceberg"
RUN cargo build --release --no-default-features --features ${FEATURES}
```

### 10.4 优雅关机

1. 收到 SIGTERM 信号
2. 停止接受新的 HTTP 连接
3. 等待存量请求处理完毕（设置超时上限，如 30 秒）
4. 关闭数据库连接池
5. 进程退出

---

## 11. 开发阶段划分

> 以下阶段按时间线编排，**每个阶段完成后必须能编译通过且有明确的验收标准**。阶段之间呈线性依赖关系，前一阶段验收通过后方可进入下一阶段。

### 总览

| 阶段 | 名称 | 前置依赖 | 核心目标 |
|------|------|---------|---------|
| S0 | 项目骨架 | 无 | Cargo workspace 可编译 |
| S1 | Core 层定义 | S0 | 所有 trait 和数据结构定义完毕 |
| S2 | Storage 基础 | S1 | PostgreSQL 可连接，DDL 可迁移，基础 CRUD 可运行 |
| S3 | Lance Namespace 端点 | S2 | curl 可操作 Lance Namespace |
| S4 | Lance Table 基础操作 | S3 | curl 可操作 Lance Table |
| S5 | Lance 版本管理 | S4 | curl 可注册和查询版本 |
| S6 | 基础设施 | S5 | 容器化部署，/healthz 和 /readyz 正常 |
| S7 | Lance 集成验证 | S6 | Lance Python SDK 端到端跑通 |
| S8 | Iceberg Namespace + Table CRUD | S7 | curl 可操作 Iceberg Namespace 和 Table |
| S9 | Iceberg CAS Commit | S8 | 并发 commit 冲突正确返回 409 |
| S10 | Spark 集成验证 | S9 | Spark 端到端读写 Iceberg 表成功 |

---

### S0: 项目骨架

**工作内容：**
- 初始化 Cargo workspace：`quasar/Cargo.toml` + workspace 成员列表
- 创建 4 个 crate 目录：`core/`、`storage/`、`adapter/`、`server/`
- 每个 crate 配置基本依赖（仅 core 可先为空，其他依赖 core）
- `server` 添加 `axum`、`tokio`、`tracing`
- 编写 `server/src/main.rs`：空的 `#[tokio::main] async fn main()`，监听配置端口
- `.gitignore`、README.md

**验收标准：**
```bash
cargo build  # 成功编译，无错误
./target/debug/quasar-server  # 进程启动，监听 8080
```

---

### S1: Core 层定义

**前置依赖：** S0

**工作内容：**
- `core/src/lib.rs`：定义 `Namespace`、`Asset`、`AssetVersion`、`AssetFormat`、`StoreError` 等全部数据结构
- `core/src/store.rs`：定义 `CatalogStore` trait（完整签名，含所有方法）
- `storage`、`adapter` crate 添加对 `core` 的依赖，编译通过
- 暂不实现任何方法体，仅签名和结构定义

**验收标准：**
```bash
cargo build  # 4 个 crate 全部编译通过
# core 中没有 axum/tokio-postgres 等框架依赖
```

---

### S2: Storage 层 + 基础 CRUD

**前置依赖：** S1 + 本地 PostgreSQL 实例

**工作内容：**
- `storage/src/migrations/`：refinery 迁移脚本 V1（完整 DDL，见 4.3 节）
- `storage/src/lib.rs`：`PgCatalogStore` 结构体 + `impl CatalogStore for PgCatalogStore`
- 实现以下方法（不含 `cas_update_metadata_location`）：
  - `create_namespace`、`get_namespace`、`list_namespaces`、`namespace_exists`、`drop_namespace`、`update_namespace_properties`
  - `create_asset`、`get_asset`、`list_assets`、`get_asset_with_current_version`、`asset_exists`、`drop_asset`、`rename_asset`
  - `load_version`、`load_current_version`、`list_versions`、`create_version`
- deadpool-postgres 连接池配置
- 单元测试：每个方法至少一个正向用例 + 一个异常用例（如重复创建返回 `AlreadyExists`）

**验收标准：**
```bash
# 1. 迁移：server 启动时自动执行（refinery embed_migrations + run()）
QUASAR_DATABASE_URL=postgres://user:pass@localhost/quasar_test ./target/debug/quasar-server
# 日志显示 "Applied migration V1__initial_schema"

# 2. 单元测试全部通过
cargo test -p storage

# 3. 验证数据库约束
# - 重复创建同名同 format 的 namespace → 23505 错误 → StoreError::AlreadyExists
# - 删除非空 namespace → 外键约束阻止
```

---

### S3: Lance Namespace 端点

**前置依赖：** S2

**工作内容：**
- `adapter/src/lance/mod.rs`：路由注册（仅 Namespace 相关）
- `adapter/src/lance/namespace.rs`：Namespace handler
  - `POST /lance/v1/namespace/{id}/create`
  - `GET /lance/v1/namespace/{id}/list`
  - `POST /lance/v1/namespace/{id}/describe`
  - `POST /lance/v1/namespace/{id}/drop`
  - `POST /lance/v1/namespace/{id}/exists`
- `adapter/src/lance/error.rs`：RFC-7807 错误格式，`StoreError` → Lance 错误映射
- `server/src/main.rs`：将 Lance Namespace 路由挂载到 axum router
- `{id}` 路径参数解析：MVP 单级，直接作为 name

**验收标准：**
```bash
# 1. 编译通过
cargo build

# 2. 启动服务
./target/debug/quasar-server &

# 3. curl 验证全部 5 个端点
curl -X POST http://localhost:8080/lance/v1/namespace/prod/create
# 列出顶级 Namespace（请求根 Namespace，$ 需 URL 编码为 %24）
curl "http://localhost:8080/lance/v1/namespace/%24/list"
curl -X POST http://localhost:8080/lance/v1/namespace/prod/describe
curl -X POST http://localhost:8080/lance/v1/namespace/prod/drop
curl -X POST http://localhost:8080/lance/v1/namespace/prod/exists

# 4. 验证错误格式（RFC-7807）
curl -X POST http://localhost:8080/lance/v1/namespace/prod/create
# 第二次应返回 409 + {"error": "NamespaceAlreadyExists", "code": 409, ...}
```

---

### S4: Lance Table 基础操作

**前置依赖：** S3

**工作内容：**
- `adapter/src/lance/table.rs`：Table handler
  - `POST /lance/v1/table/{id}/declare`
  - `POST /lance/v1/table/{id}/describe`
  - `POST /lance/v1/table/{id}/register`
  - `POST /lance/v1/table/{id}/deregister`
  - `POST /lance/v1/table/{id}/drop`
  - `POST /lance/v1/table/{id}/exists`
  - `POST /lance/v1/table/{id}/rename`
- `GET /lance/v1/namespace/{id}/table/list`（在 namespace.rs 中实现）
- `{id}` 解析：`prod$users` → namespace=`prod`, table=`users`
- DeclareTable 时分配 `location`（基于 warehouse 路径 + namespace + table）

**验收标准：**
```bash
# 1. 先创建 namespace
curl -X POST http://localhost:8080/lance/v1/namespace/prod/create

# 2. curl 验证 Table 生命周期
curl -X POST http://localhost:8080/lance/v1/table/prod$users/declare \
  -H "Content-Type: application/json" -d '{"options": {"mode": "create"}}'
# 返回包含 location 和 storage_options

curl http://localhost:8080/lance/v1/namespace/prod/table/list
curl -X POST http://localhost:8080/lance/v1/table/prod$users/describe
curl -X POST http://localhost:8080/lance/v1/table/prod$users/exists
curl -X POST http://localhost:8080/lance/v1/table/prod$users/drop
curl -X POST http://localhost:8080/lance/v1/table/prod$users/exists
# 返回 404
```

---

### S5: Lance 版本管理

**前置依赖：** S4

**工作内容：**
- `adapter/src/lance/version.rs`：3 个 handler
  - `POST /lance/v1/table/{id}/version/create`
  - `GET /lance/v1/table/{id}/version/list`
  - `POST /lance/v1/table/{id}/version/describe`
- 路由注册

**验收标准：**
```bash
# 1. 声明表
curl -X POST http://localhost:8080/lance/v1/table/prod$users/declare

# 2. 注册版本
curl -X POST http://localhost:8080/lance/v1/table/prod$users/version/create \
  -H "Content-Type: application/json" \
  -d '{"version": 1, "manifest_path": "s3://bucket/warehouse/prod_users/_versions/1.manifest", "naming_scheme": "V2"}'
# 返回 200

# 3. 重复注册相同版本
curl -X POST ... -d '{"version": 1, ...}'
# 返回 409 + TableVersionAlreadyExists

# 4. 列出版本
curl http://localhost:8080/lance/v1/table/prod$users/version/list
# 返回 [version: 1]

# 5. 描述版本
curl -X POST http://localhost:8080/lance/v1/table/prod$users/version/describe \
  -d '{"version": 1}'
# 返回 version, manifest_path, naming_scheme
```

**此阶段完成标志：Lance REST Namespace 协议全部 MVP 端点可用。**

---

### S6: 基础设施

**前置依赖：** S5

**工作内容：**
- `GET /healthz`：返回 `{"status": "ok"}`（仅检查进程存活）
- `GET /readyz`：执行一次 `SELECT 1`，返回 `{"status": "ok"}` 或 `503`
- 配置加载：envy + serde 解析环境变量，支持 `.env` 文件
- `tracing` 中间件：记录每个请求的方法、路径、状态码、耗时
- 优雅关机：SIGTERM → 停止 accept → 等待现有请求（30s 超时）→ 关闭连接池
- Dockerfile：多阶段构建，最终镜像基于 `debian:bookworm-slim`
- docker-compose.yml：Quasar + PostgreSQL

**验收标准：**
```bash
# 1. 容器构建
docker build -t quasar:latest .

# 2. docker-compose 启动
docker-compose up -d

# 3. healthz
curl http://localhost:8080/healthz
# {"status": "ok"}

# 4. readyz（含 DB 检查）
curl http://localhost:8080/readyz
# {"status": "ok"}

# 5. 停止 PostgreSQL 后再请求 readyz → 503
docker-compose stop postgres
curl http://localhost:8080/readyz
# HTTP 503

# 6. SIGTERM 后服务优雅退出
docker-compose stop quasar
# 日志显示 "shutting down gracefully, waiting for connections..."
```

**此阶段完成标志：Phase 1 服务端全部功能可用，可容器化部署。**

---

### S7: Lance 集成验证

**前置依赖：** S6 + Lance Python SDK 环境

**工作内容：**
- 编写 `tests/lance_integration.py`：
  - 使用 `lance-namespace` Python SDK（或 requests 直接调用 REST）连接 Quasar
  - 创建 Namespace：`prod`
  - DeclareTable：`prod.users`，获取分配的 `location`
  - 使用 Lance Python SDK 直接写入 Arrow 数据到 `location`
  - CreateTableVersion：注册 `version=1` 及 `manifest_path`
  - DescribeTable：验证 `current_version=1`，`location` 正确
  - 使用 Lance Python SDK 读取数据，验证行数和 schema
  - 写入第二批数据，注册 `version=2`
  - ListVersions：验证返回 `[1, 2]`
  - DropTable + DropNamespace：清理
- 提供 `docker-compose.lance.yml`：Quasar + PostgreSQL + MinIO（本地对象存储）
- 测试报告文档

**验收标准：**
```bash
# 1. 启动完整环境（含本地对象存储）
docker-compose -f docker-compose.lance.yml up -d

# 2. 运行测试脚本
cd tests && python lance_integration.py

# 3. 测试输出应包含：
# [PASS] Create namespace: prod
# [PASS] Declare table: prod.users, location=s3://minio/warehouse/prod_users/
# [PASS] Write data to location (3 rows)
# [PASS] Create table version: v1
# [PASS] Describe table: current_version=1
# [PASS] Read data back (3 rows, schema matches)
# [PASS] Write second batch (5 rows)
# [PASS] Create table version: v2
# [PASS] List versions: [1, 2]
# [PASS] Drop table
# [PASS] Drop namespace
```

**此阶段完成标志：Phase 1 全部功能验证通过，Lance Python SDK 可通过 Quasar Catalog 完成完整的表生命周期管理。**

---

### S8: Iceberg Namespace + Table CRUD

**前置依赖：** S7

**工作内容：**
- `adapter/src/iceberg/mod.rs`：路由注册
- `adapter/src/iceberg/config.rs`：Catalog 配置端点
  - `GET /iceberg/v1/config`：返回 Catalog 配置（warehouse 地址、defaults、overrides）
- `adapter/src/iceberg/namespace.rs`：Namespace handler
  - `GET /iceberg/v1/namespaces`
  - `POST /iceberg/v1/namespaces`
  - `GET /iceberg/v1/namespaces/{ns}`
  - `DELETE /iceberg/v1/namespaces/{ns}`
  - `POST /iceberg/v1/namespaces/{ns}/properties`
- `adapter/src/iceberg/table.rs`：Table handler（不含 commit）
  - `GET /iceberg/v1/namespaces/{ns}/tables`
  - `POST /iceberg/v1/namespaces/{ns}/tables`
  - `GET /iceberg/v1/namespaces/{ns}/tables/{table}`
  - `DELETE /iceberg/v1/namespaces/{ns}/tables/{table}`
  - `HEAD /iceberg/v1/namespaces/{ns}/tables/{table}`
  - `POST /iceberg/v1/tables/rename`
- `adapter/src/iceberg/error.rs`：Iceberg `ErrorResponse` 格式
- `adapter/src/iceberg/dto.rs`：请求/响应 DTO（LoadTableResponse、CreateTableRequest 等）
- CreateTable 时生成 `table-uuid`（即 `assets.id`），构造初始 `TableMetadata`，返回 `metadata_location`

**验收标准：**
```bash
# 1. 创建 Iceberg namespace
curl -X POST http://localhost:8080/iceberg/v1/namespaces \
  -H "Content-Type: application/json" -d '{"namespace": ["prod"]}'

# 2. 创建 Iceberg table
curl -X POST http://localhost:8080/iceberg/v1/namespaces/prod/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users",
    "schema": {...},
    "location": "s3://bucket/warehouse/prod/users"
  }'
# 返回包含 metadata-location、table-uuid

# 3. 加载表
curl http://localhost:8080/iceberg/v1/namespaces/prod/tables/users
# 返回包含 metadata-location、schema、current-snapshot-id

# 4. HEAD 检查存在
curl -I http://localhost:8080/iceberg/v1/namespaces/prod/tables/users
# HTTP 200

# 5. 删除表
curl -X DELETE http://localhost:8080/iceberg/v1/namespaces/prod/tables/users

# 6. 错误格式验证
curl http://localhost:8080/iceberg/v1/namespaces/prod/tables/nonexistent
# 返回 {"error": {"message": "...", "type": "NoSuchTableException", "code": 404}}
```

---

### S9: Iceberg CAS Commit

**前置依赖：** S8

**工作内容：**
- **引入对象存储客户端**：`object_store` crate（支持 S3/GCS/Azure），用于 Iceberg 适配器读写 metadata.json
  - 配置项新增：`QUASAR_S3_ENDPOINT`、`QUASAR_S3_ACCESS_KEY`、`QUASAR_S3_SECRET_KEY`（MinIO 本地测试用）
- `adapter/src/iceberg/table_metadata.rs`：
  - `TableMetadata` 结构定义（JSON 序列化/反序列化）
  - `TableMetadata::apply_updates()` 方法
  - `TableMetadata::check_requirements()` 方法
  - `TableMetadata::to_json()` 方法
- `storage/src/store.rs`：实现 `cas_update_metadata_location` 方法（CAS SQL）
- `adapter/src/iceberg/table.rs`：实现 `POST /iceberg/v1/namespaces/{ns}/tables/{table}`（commit handler）
  - 解析 `requirements` + `updates`
  - 从对象存储加载当前 metadata.json
  - 校验 requirements
  - 应用 updates，生成新 metadata.json
  - 写入对象存储（新路径）
  - 调用 `cas_update_metadata_location` CAS 更新 DB
  - 若冲突返回 `409 CommitFailedException`
- 单元测试：模拟并发 commit 冲突场景

**验收标准：**
```bash
# 1. 创建表并获取初始 metadata-location
curl -X POST .../prod/tables -d '{"name": "users", ...}'
# 假设返回 metadata-location = "s3://.../00001.metadata.json"

# 2. 正常 commit（满足 requirements）
curl -X POST .../prod/tables/users \
  -d '{
    "requirements": [{"type": "assert-current-snapshot-id", "snapshot-id": null}],
    "updates": [{"action": "add-snapshot", ...}]
  }'
# 返回 200，metadata-location 更新为 00002.metadata.json

# 3. 模拟并发冲突
# 同时发送两个 commit，都基于 00002.metadata.json
# 第一个成功，第二个返回 409
# 可通过两个终端同时 curl 验证

# 4. 单元测试验证 CAS
# cargo test -p storage cas_update_metadata_location_concurrent
# 验证：两次并行 commit 基于相同 expected_location，恰好一个成功一个失败
```

**此阶段完成标志：Iceberg REST Catalog 协议全部 MVP 端点可用，CAS 并发控制正确。**

---

### S10: Spark 集成验证

**前置依赖：** S9 + Spark 3.4+ 环境

**工作内容：**
- 编写 `tests/spark_iceberg_integration.py`：
  - 配置 Spark 使用 Iceberg REST Catalog：`spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog`，`spark.sql.catalog.iceberg.uri=http://localhost:8080/iceberg`
  - 创建表：`CREATE TABLE iceberg.prod.test (id INT, name STRING)`
  - 插入数据：`INSERT INTO iceberg.prod.test VALUES (1, 'Alice')`
  - 查询数据：`SELECT * FROM iceberg.prod.test`
  - 验证返回行数 = 1
- 提供 `docker-compose.spark.yml`：Quasar + PostgreSQL + Spark 容器
- 测试报告文档

**验收标准：**
```bash
# 1. 启动完整环境
docker-compose -f docker-compose.spark.yml up -d

# 2. 运行测试脚本
cd tests && python spark_iceberg_integration.py

# 3. 测试输出应包含：
# [PASS] CREATE TABLE
# [PASS] INSERT INTO
# [PASS] SELECT * (返回 1 行)
# [PASS] DROP TABLE
```

**此阶段完成标志：MVP 全部功能验证通过。**

---

## 12. 模块视角分工参考

> 以下按 crate 模块视角列出所有需要实现的内容，供开发时按模块组织代码时参考。与第 11 章的时间线视角互补。

### 12.1 Phase 1 模块分工（S0 ~ S7）

**core：**
- `Namespace`、`Asset`、`AssetVersion` 数据结构
- `AssetFormat` 枚举
- `CatalogStore` trait（完整签名）
- `StoreError`

**storage：**
- refinery 迁移脚本（DDL）
- deadpool-postgres 连接池配置
- `PgCatalogStore` 全部方法实现（S2 完成不含 CAS；S9 补充 `cas_update_metadata_location`）

**adapter/lance：**
- Namespace 全部 handler（S3）
- Table 全部 handler（S4）
- Version 全部 handler（S5）
- RFC-7807 错误响应格式
- `{id}` 路径参数解析

**server：**
- axum 路由注册（`/lance/v1/...` + `/healthz` + `/readyz`）
- tracing 中间件
- 配置加载（环境变量 + `.env`）
- 优雅关机
- Dockerfile + docker-compose

**tests：**
- Lance 集成测试脚本（S7）

### 12.2 Phase 2 模块分工（S8 ~ S10）

**adapter/iceberg：**
- Namespace 全部 handler（S8）
- Table 全部 handler（S8 + S9）
- `TableMetadata` 结构定义与解析
- `requirements` 校验逻辑
- `updates` 应用逻辑
- metadata.json 序列化/反序列化
- CAS commit 协调
- Iceberg 错误响应格式

**storage：**
- `cas_update_metadata_location` 方法实现（S9）

**server：**
- axum 路由注册（`/iceberg/v1/...`，S8）

**tests：**
- Spark 集成测试脚本（S10）

---

## 附录：关键决策清单

| # | 决策 | 选择 | 理由 |
|---|------|------|------|
| 1 | 技术栈 | axum + tokio-postgres + deadpool-postgres + refinery | 零编译时 DB 依赖，运行时灵活 |
| 2 | 层级结构 | `Namespace → Asset`（两层） | 无需 Metalake/Catalog 等联邦代理层 |
| 3 | Namespace 深度 | MVP 单级，DB 预留多级扩展 | 简化 Phase 1 实现 |
| 4 | format 位置 | Namespace 层 only | 消除冗余，避免不一致 |
| 5 | CAS 机制 | 基于 `metadata_location` 条件更新 | metadata.json URI 天然唯一，无需 etag |
| 6 | Iceberg TableMetadata | MVP 自实现核心子集，不依赖 iceberg-rust | 协议语义完全可控；代价是实现复杂度高（updates 类型有十余种），为 Phase 2 最大技术风险。若自实现成本超预期，可回退到引入 iceberg-rust |
| 7 | Lance 当前版本 | `SELECT MAX(version)` 实时查询 | 避免 properties 中冗余存储 |
| 8 | 存储架构 | 自包含（PostgreSQL 直接存储） | 性能优先，少一层网络跳转 |
| 9 | 对象存储交互 | Lance：不交互，只存指针；Iceberg：适配器读写 metadata.json | Lance 数据和 manifest 均由客户端直接操作对象存储；Iceberg 的 metadata.json 由服务端生成和管理（需引入 `object_store` crate） |
| 10 | 分页 | MVP 基于 offset 的简单分页 | 满足基本需求，后续可优化 |
| 11 | properties 类型 | `Map<String, String>` | 匹配上游协议定义，用点号分隔 key |
| 12 | 鉴权 | MVP 不实现 | 内网/测试环境使用 |
| 13 | 多格式架构方案 | Gravitino 式双协议适配（非 Polaris 式 Generic Table） | Lance 作为一等公民，需要完整实现 Lance REST Namespace 规范 |
| 14 | Namespace 隔离 | Iceberg 和 Lance 各自独立的 Namespace 空间，允许同名 | 消除跨格式命名冲突，每种协议在其独立空间内自由操作 |
| 15 | 交付顺序 | Lance 优先（Phase 1），Iceberg 后做（Phase 2） | Lance 对 Catalog 的要求更轻（只存指针），可更快验证核心架构 |
| 16 | Namespace 删除策略 | ON DELETE RESTRICT，仅允许删除空 Namespace | 两套协议规范均要求非空 Namespace 不可删除 |
| 17 | 条件编译 | Cargo feature flags（`lance` / `iceberg`），`object_store` 为可选依赖 | 部署方可按需裁剪 binary，只部署 Lance 时不引入对象存储依赖，减小体积并降低攻击面 |

---

## 修订记录

### V1.0

- 新增：项目概述（定位、可纳管资产类型、6 条设计目标）
- 新增：MVP 范围定义（Phase 1 / Phase 2 交付阶段、6 项功能需求、8 项非功能需求）
- 新增：系统架构（双协议架构图、Namespace 隔离策略、与业界方案三维度对比表）
- 新增：数据模型完整规格（6 条设计原则、ER 图、PostgreSQL DDL、5 个索引、实体 Rust 定义）
- 新增：协议端点设计（Iceberg 13 个端点、Lance 16 个端点、基础设施 2 个端点，含明确不实现清单）
- 新增：Crate 分层与模块职责（4 个 crate 目录结构、依赖关系图）
- 新增：核心接口定义（`CatalogStore` trait 17 个方法、`StoreError`、Lance 路径解析规则）
- 新增：并发控制与一致性（Iceberg CAS 7 步流程 + SQL、Lance 版本唯一性约束 SQL、Namespace 增量更新 SQL）
- 新增：错误处理规范（两层错误体系、Iceberg ErrorResponse 格式、Lance RFC-7807 格式、状态码映射策略）
- 新增：配置与部署（5 个环境变量、部署形态、优雅关机流程）
- 新增：10 阶段开发划分（S0~S10，每个阶段含前置依赖、工作内容、验收标准）
- 新增：模块视角分工参考（Phase 1 / Phase 2 按 crate 组织的工作项）
- 新增：16 条关键决策清单（V1.1 增至 17 条）

### V1.1

- 新增：6.2 节「条件编译（Cargo Features）」—— feature flags 定义、编译命令、实现机制
- 新增：10.3 节「条件编译部署」—— Docker 构建参数传递 feature 选择
- 更新：6.1 节 Crate 分层注释——adapter 子模块标注 feature 依赖
- 更新：附录关键决策清单——新增第 17 条「条件编译」决策
- 更新：修订记录 V1.0 中「16 条关键决策」→「17 条关键决策」

### V1.2

- **更新：7.1 节 CatalogStore trait**——与实际代码同步：
  - Asset 操作定位方式从 `namespace_id: Uuid` 改为 `namespace_name: &str, format: AssetFormat`
  - 新增 `namespace_exists`、`asset_exists`、`rename_asset`、`get_asset_with_current_version`
  - 删除 `namespace_is_empty`、`update_asset`
  - `delete_namespace` 重命名为 `drop_namespace`，返回 `Result<(), StoreError>`
  - `delete_asset` 重命名为 `drop_asset`
  - `commit_iceberg_table` 重命名为 `cas_update_metadata_location`，参数从 `namespace_id + AssetCommitUpdate` 改为 `namespace_name + format + expected_location + new_location + new_schema_snapshot`
  - Lance 版本方法从 `get_version`/`get_latest_version`/`create_version(&AssetVersion)` 改为 `load_version`/`load_current_version`/`create_version(namespace_name, format, asset_name, version_id, metadata_location, previous_version_id)`
  - 方法总数从 15 个增至 17 个
- **删除：7.2 节 AssetCommitUpdate**——该结构体已在实现中被移除，CAS 参数直接展开到 `cas_update_metadata_location` 方法签名中
- **更新：7.3 节 StoreError**——删除 `Database(String)` 变体，合并入 `Internal`
- **更新：8.1 节 CAS SQL**——与实际实现同步，仅更新 `metadata_location` 和 `schema_snapshot`
- **更新：9.4 节 错误映射**——删除 `StoreError::Database → 500` 映射
- **更新：S1/S2/S9 工作内容**——同步方法名变更，删除 `AssetCommitUpdate` 引用

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。
