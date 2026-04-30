# V2 (Unified REST API) 需求澄清与分析

> 本文档定义 Quasar V2 的核心需求——Unified REST API。
> V2 的交付标志是：一套格式无关的 REST API 可供管理后台/数据平台统一纳管 Iceberg 与 Lance 资产，同时 Iceberg REST Catalog 和 Lance REST Namespace 两套标准协议继续独立运行。
>
> **V2 数据模型决策说明**：V2 是全新的版本基线，V1 schema 与 V1 数据处理不在 V2 范围内。若本文档与 MVP 设计文档中“Namespace 绑定 format、Asset 不存 format”的早期表述冲突，以本文档为准；进入实现前需同步更新 `docs/ARCHITECTURE.md` 与 `docs/mvp/DATA_MODEL.md`。

---

## 目录

1. [演进背景与动机](#1-演进背景与动机)
2. [V2 定位与边界](#2-v2-定位与边界)
3. [数据模型基线](#3-数据模型基线)
4. [功能需求](#4-功能需求)
5. [非功能需求](#5-非功能需求)
6. [Unified API 端点设计](#6-unified-api-端点设计)
7. [关键决策与约束](#7-关键决策与约束)
8. [验收标准](#8-验收标准)
9. [修订记录](#9-修订记录)

---

## 1. 演进背景与动机

### 1.1 V1 (MVP) 交付成果

V1 实现了两条独立的标准协议入口：

- **Iceberg REST Catalog** (`/iceberg/v1/...`)：面向 Spark、Trino、Flink 等计算引擎
- **Lance REST Namespace** (`/lance/v1/...`)：面向 LanceDB Python SDK、lance-spark 等客户端

两者共享同一套 `CatalogStore` 存储抽象和 PostgreSQL 后端，但协议入口、错误格式和 Asset/Table 可见性相互隔离。

### 1.2 V1 遗留问题

**Namespace 已解耦 format，Asset 尚未完成。**

V1 后期通过一次 DDL 变更将 `namespaces` 表的 `format` 字段移除，使 Namespace 成为格式无关的组织单元。但 `assets` 表至今未引入可区分资产类型与格式子类的字段：

- `assets` 当前约束为 `UNIQUE(namespace_id, name)`，同一 Namespace 下不允许同名 Asset 跨格式共存
- `CatalogStore::list_assets` 方法签名接收 `format: AssetFormat`，但 `PgCatalogStore` 实现中忽略该参数，导致 Iceberg 的 `list_tables` 可能混入 Lance Asset（反之亦然）
- 管理视角（如"`prod` 这个 Namespace 下有哪些表，分别是什么格式"）无法回答

### 1.3 V2 核心动机

引入一套**格式无关的 Unified REST API**，解决以下场景：

| 场景 | V1 的问题 | V2 的解决 |
|------|----------|----------|
| 数据平台管理后台需要列出某 Namespace 下的全部 Asset | 需要分别调 Iceberg 和 Lance 两个端点 | 一个 `/unified/v1/namespaces/{ns}/assets` 返回全部 |
| 跨格式搜索（"查找叫 `users` 的所有表"） | 无法实现 | Unified API 支持按名称过滤 |
| 统一的数据发现/血缘入口 | 需维护两套客户端 | 单一协议入口 |
| 为 Asset 赋予类型身份 | 数据库层面缺失 | DDL 引入 `asset_type` / `asset_subtype`，并拆出表资产明细表 |

---

## 2. V2 定位与边界

### 2.1 三层架构视图

```
┌─────────────────────────────────────────────────────────┐
│                      客户端层                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Iceberg     │  │  Lance       │  │  管理后台/   │  │
│  │  引擎        │  │  客户端      │  │  数据平台    │  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │
└─────────┼────────────────┼────────────────┼──────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────┐
│                     协议适配层                           │
│  ┌──────────────────┐  ┌──────────────────┐            │
│  │ /iceberg/v1/...  │  │ /lance/v1/...    │            │
│  │ Iceberg REST     │  │ Lance REST       │            │
│  │ Catalog Spec     │  │ Namespace Spec   │            │
│  └────────┬─────────┘  └────────┬─────────┘            │
│           │                     │                       │
│           │                     ┌────────────────┐       │
│           └─────────────────────┤ /unified/v1/...│       │
│                                 │ Unified REST   │       │
│                                 │ API (新增)     │       │
│                                 └───────┬────────┘       │
└────────────────────────┼───────────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   CatalogStore      │
              │   (trait)           │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   PgCatalogStore    │
              │   (PostgreSQL)      │
              └─────────────────────┘
```

### 2.2 边界声明

**必须保留（V2 不触碰）：**

- Iceberg REST Catalog 协议 (`/iceberg/v1/...`) 的端点路径、请求/响应格式、错误格式
- Lance REST Namespace 协议 (`/lance/v1/...`) 的端点路径、请求/响应格式、错误格式
- Iceberg 的 CAS commit 语义（requirements/updates/CAS）
- Lance 的 version 注册语义

**V2 新增：**

- Unified REST API (`/unified/v1/...`) —— 格式无关的管理层端点，面向管理后台/数据平台
- `assets` 通用资产注册表、`tabular_assets` 表资产明细表、通用版本表与表版本表拆分及配套约束

**V2 不实现：**

- 替代 Iceberg/Lance 标准协议（公共子集，不是替代）
- 格式内部语义变更管理（Iceberg 的 schema/partition/snapshot 变更、Lance 的 manifest 注册仍走标准协议）
- 多租户、鉴权、审计
- 数据面操作（读写实际数据文件）
- 多 Catalog 层（维持 `Namespace → Asset` 扁平结构，不引入 Catalog 实体层）
- Namespace 级 warehouse 覆盖（全局 `warehouse_path` 不变，后续演进可选）
- 版本历史列表（Unified API 只返回 `current_version`）
- V1 schema / V1 数据的自动处理

**隔离边界澄清：**

- Namespace 是格式无关的共享组织单元，`prod` 在 Iceberg、Lance、Unified API 中指向同一个 Namespace 记录。
- Asset 是格式相关的资源，`prod.users` 可以同时存在 Iceberg 与 Lance 两个 Asset。
- 标准协议的隔离边界在 Asset/Table 层：Iceberg 端点只返回 Iceberg Asset，Lance 端点只返回 Lance Asset。

**V2 部署模式**：

V2 是全新部署，从空数据库开始。不涉及 V1 数据迁移或 V1 schema 处理。若需要保留历史数据，应在 V2 之外做一次性人工导入。

---

## 3. 数据模型基线

### 3.1 V1 参考基线

```sql
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(namespace_id, name)  -- 同名 Asset 跨格式不可共存
);
```

**说明**：以上只用于说明 V2 为什么需要调整模型，不构成 V2 实现要求。

### 3.2 V2 目标状态

```sql
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('table')),
    asset_subtype TEXT NOT NULL CHECK (asset_subtype IN ('iceberg', 'lance')),
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(namespace_id, name, asset_type, asset_subtype)
);

CREATE TABLE tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB
);

CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_order BIGINT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, version_key)
);

CREATE TABLE tabular_asset_versions (
    asset_version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    metadata_location TEXT NOT NULL,
    previous_asset_version_id UUID REFERENCES asset_versions(id)
);

CREATE INDEX idx_assets_namespace ON assets(namespace_id);
CREATE INDEX idx_assets_type ON assets(asset_type);
CREATE INDEX idx_assets_namespace_type_subtype ON assets(namespace_id, asset_type, asset_subtype);
CREATE INDEX idx_asset_versions_asset ON asset_versions(asset_id);
CREATE INDEX idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;
CREATE UNIQUE INDEX idx_asset_versions_asset_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;
```

**Core 模型目标状态**：

```rust
pub enum AssetType {
    Table,
}

pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub asset_type: AssetType,
    // V2 仅支持 table 资产，asset_subtype 为 "iceberg" 或 "lance"。
    pub asset_subtype: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

pub struct TabularAsset {
    pub asset_id: Uuid,
    pub format: AssetFormat,
    pub location: String,
    pub metadata_location: Option<String>,
    pub schema_snapshot: Option<serde_json::Value>,
}

pub struct AssetWithTabular {
    pub asset: Asset,
    pub tabular: TabularAsset,
}

pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub version_key: String,
    pub version_order: Option<i64>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

pub struct TabularAssetVersion {
    pub asset_version_id: Uuid,
    pub metadata_location: String,
    pub previous_asset_version_id: Option<Uuid>,
}

pub struct AssetVersionWithTabular {
    pub version: AssetVersion,
    pub tabular_version: TabularAssetVersion,
}
```

### 3.3 Schema 初始化策略

1. **V2 以目标 DDL 初始化**：`assets` 表从一开始就是通用资产注册表，表资产明细写入 `tabular_assets`，同名跨格式表资产通过 `(namespace_id, name, asset_type, asset_subtype)` 唯一约束区分。
2. **不提供 V1 数据自动转换**：V2 不负责将已有 V1 数据转换为 Iceberg 或 Lance。需要保留历史数据时，应在 V2 之外做一次性人工导入。
3. **PgCatalogStore 调整**：所有通过 `(namespace_name, asset_name)` 查询表资产的 SQL 都必须增加 `a.asset_type = 'table' AND a.asset_subtype = $format` 条件，并按需联查 `tabular_assets`。
4. **Comment 字段范围**：`namespaces` 与通用注册表 `assets` 引入 nullable `comment TEXT`；`asset_versions` 不引入 `comment`，避免把版本提交说明与资源说明混为一类。
5. **版本表定位**：当前实现已有独立 `asset_versions` 表。V2 将其调整为通用版本注册表，记录版本身份与排序；表资产版本字段写入 `tabular_asset_versions`。Iceberg 默认不创建版本记录，Lance 使用这两张表记录 version。
6. **版本排序语义**：`version_key` 是同一 Asset 下的原生版本标识，必须唯一；`version_order` 是可选排序键，Lance version 必须写入且等于原生 `version_id`。`tabular_asset_versions.previous_asset_version_id` 必须指向同一 Asset 下的版本记录，由存储层校验。

**说明**：V2 是新的版本基线，schema 设计以目标 DDL 为准，不要求保留 V1 schema 形态或处理路径。

### 3.4 模型变化对比

| 层面 | V1 | V2 |
|------|----|----|
| Namespace | 格式无关 | 格式无关（不变） |
| Asset | 承载表资产字段，同 namespace 同名唯一 | 通用资产注册表，承载 `asset_type`/`asset_subtype`、comment、properties |
| TabularAsset | 不存在，表资产字段直接放在 `assets` | 表资产明细表，承载 location、metadata_location、schema_snapshot |
| AssetVersion | 独立表，主要服务 Lance 版本 | 通用版本注册表，承载版本身份、排序、properties |
| TabularAssetVersion | 不存在，表版本字段直接放在 `asset_versions` | 表资产版本明细表，承载 metadata_location、previous_asset_version_id |

**关键影响**：V2 后，`assets` 只负责统一身份、类型和治理元数据；表资产的格式细节落在 `tabular_assets`。`prod.users` 可以同时作为 Iceberg 表和 Lance 表存在（同名但 `asset_subtype` 不同），互不干扰。各标准协议适配器通过注入 `asset_type=table` 与 `format=iceberg|lance` 保证隔离性。

---

## 4. 功能需求

### 4.1 需求汇总

| ID | 需求 | 说明 |
|----|------|------|
| V2-F1 | Asset 类型与表资产格式标识 | `assets` 表作为通用注册表，`asset_type`/`asset_subtype` 支持按资产类型和表格式查询 |
| V2-F2 | Unified Namespace 管理 | 格式无关的 Namespace CRUD（复用现有 CatalogStore 能力） |
| V2-F3 | Unified Asset 发现 | 跨格式列出 Asset，支持按 `format` 筛选 |
| V2-F4 | Unified Asset 管理 | Asset 的删除、重命名、属性更新（格式无关接口）；V2 暂不提供 Unified create |
| V2-F5 | 当前版本查看 | Asset 详情中返回 `current_version`（Iceberg: sequence-number; Lance: latest version_id）。版本历史列表不纳入 V2 |
| V2-F6 | 各协议适配器格式隔离 | Iceberg 和 Lance 适配器的 `list_assets`/`get_asset` 等查询必须按 `asset_type` 与表格式过滤 |

### 4.2 V2-F1：Asset 类型与表资产格式标识（详细）

**目标**：数据库层面为每个 Asset 记录通用资产类型，并为 V2 的表资产记录表格式。

**验收**：
- `assets` 表存在 `asset_type` 与 `asset_subtype` 列；V2 中 `asset_type='table'`，`asset_subtype` 为 `'iceberg'` 或 `'lance'`
- `tabular_assets` 表存在 `location`、`metadata_location`、`schema_snapshot` 等表资产字段
- 新创建表资产记录必须同时写入 `assets` 与 `tabular_assets`；标准协议入口由协议前缀隐式决定，Unified API V2 不提供 Asset 创建端点
- `PgCatalogStore` 的 `list_assets`、`get_asset` 等方法按 `asset_type='table'` 与 `asset_subtype` 过滤查询

**说明**：V2 是新的 schema 基线。实现阶段可以重建数据库或重建初始化 DDL，不需要提供 V1 schema 的处理路径。

### 4.3 V2-F2：Unified Namespace 管理（详细）

**目标**：通过 `/unified/v1/...` 端点管理 Namespace，行为与现有 CatalogStore 一致。

**范围**：
- `GET /unified/v1/namespaces` —— 列出所有 Namespace
- `POST /unified/v1/namespaces` —— 创建 Namespace
- `GET /unified/v1/namespaces/{ns}` —— 获取 Namespace 详情
- `DELETE /unified/v1/namespaces/{ns}` —— 删除 Namespace（仅空 Namespace 可删除）
- `PATCH /unified/v1/namespaces/{ns}` —— 更新 Namespace comment 与 properties（properties 为增量更新：removals + updates）

**说明**：
- Namespace 在 V1 已解耦 format，V2 无需变更存储层，只需暴露新的端点
- `comment` 是 Namespace 的一等说明字段，可在 create 时设置，也可通过 PATCH 更新或清空
- `PATCH` 中 properties 语义为增量更新（与 Iceberg `update_namespace_properties` 对齐）：`removals` 删除指定 key，`updates` 新增/覆盖指定 key，未提及的 key 保持不变
- Namespace 级 warehouse 覆盖（如通过 properties 传 `warehouse` 影响建表 location）**不纳入 V2**，当前架构对此完全开放（`properties` 已存储但未读取），后续演进可选
- 标准协议 Namespace 端点继续操作同一套共享 Namespace；V2 不引入 per-format Namespace properties

### 4.4 V2-F3：Unified Asset 发现（详细）

**目标**：跨格式发现和浏览 Asset。

**范围**：
- `GET /unified/v1/namespaces/{ns}/assets` —— 列出 Namespace 下全部 Asset
- `GET /unified/v1/namespaces/{ns}/assets?format=iceberg` —— 仅列出 Iceberg Asset
- `GET /unified/v1/namespaces/{ns}/assets?format=lance` —— 仅列出 Lance Asset
- `GET /unified/v1/namespaces/{ns}/assets?name=users` —— 按名称过滤

**响应模型**：

```json
{
  "assets": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "users",
      "asset_type": "table",
      "format": "iceberg",
      "location": "s3://bucket/warehouse/prod/users",
      "metadata_location": "s3://bucket/warehouse/prod/users/metadata/00001-xxx.metadata.json",
      "comment": "User profile table",
      "properties": {},
      "created_at": "2024-01-15T08:30:00Z"
    },
    {
      "id": "660e8400-e29b-41d4-a716-446655440001",
      "name": "users",
      "asset_type": "table",
      "format": "lance",
      "location": "s3://bucket/warehouse/prod/users/",
      "metadata_location": null,
      "comment": "User embedding dataset",
      "properties": {},
      "created_at": "2024-01-15T09:00:00Z"
    }
  ],
  "next_page_token": null
}
```

**关键设计点**：
- 同一 Namespace 下允许同名 Asset 跨格式共存（`prod.users` 既是 Iceberg 也是 Lance）
- 返回结果中每个表 Asset 携带 `asset_type="table"` 与 `format` 字段，使调用方无需推断

### 4.5 V2-F4：Unified Asset 管理（详细）

**目标**：通过统一接口删除、重命名、修改 Asset 的 Catalog 级元数据。

**范围**：
- `GET /unified/v1/namespaces/{ns}/assets/{name}?format={format}` —— 获取 Asset（**format 必填**，因同名跨格式可共存）
- `DELETE /unified/v1/namespaces/{ns}/assets/{name}?format={format}` —— 删除 Asset（**format 必填**）
- `PATCH /unified/v1/namespaces/{ns}/assets/{name}?format={format}` —— 更新 Asset comment 与 properties（**format 必填**）
- `POST /unified/v1/namespaces/{ns}/assets/{name}/rename?format={format}` —— 重命名 Asset（**format 必填**）

**创建边界**：
- V2 暂不提供 `POST /unified/v1/namespaces/{ns}/assets`。
- Iceberg 表创建继续由 Iceberg REST Catalog 的原生 create table 请求和 Quasar 现有 Iceberg adapter 内部处理逻辑承载。
- Lance 表/Asset 创建继续由 Lance 原生 API 或 Quasar 当前内部处理逻辑承载。
- 通过原生 API 创建的表 Asset 必须写入 `assets.asset_type='table'`、`assets.asset_subtype` 与 `tabular_assets`，并可被 Unified Asset list/get 发现。
- 决策依据：不同格式的建表参数、元数据文件写入、初始化语义差异较大。V2 的 Unified API 先聚焦发现与 Catalog 级管理，不强行抽象一套不稳定的跨格式 create 请求体。

**边界**：Unified API 的 `PATCH /assets` 只更新 Catalog 级别的 `comment` 与 `properties`（即 `assets.comment` 与 `assets.properties` 字段），不触碰格式内部状态：
- Iceberg：不修改 `metadata.json` 中的 properties
- Lance：不修改 manifest 或数据文件

**删除与重命名边界**：
- `DELETE /assets/{name}` 只删除 Catalog 记录，不删除对象存储中的 Iceberg metadata/data 文件或 Lance 数据文件
- `POST /assets/{name}/rename` 只修改 Catalog 中的 Asset 名称，不移动对象存储路径，不修改 Iceberg `metadata.json` 中的 table location
- 如调用方需要数据面清理或路径迁移，应通过格式自身工具或后续专门的数据治理能力完成

### 4.6 V2-F5：当前版本查看（详细）

**目标**：提供查看 Asset 当前版本的能力。

**范围**：
- `GET /unified/v1/namespaces/{ns}/assets/{name}?format={format}` —— Asset 详情中返回 `current_version`（已包含在 AssetResponse 中）

**Iceberg 当前版本**：
- 从 `tabular_assets` 表的 `metadata_location` 指针定位当前 metadata.json
- 读取 metadata.json 中的 `current-snapshot-id` 和 `last-sequence-number`
- 返回简化信息：`sequence_number`、`snapshot_id`、`timestamp_ms`
- 新建但尚无 snapshot 的 Iceberg 表，`snapshot_id` 和 `timestamp_ms` 返回 `null`，`sequence_number` 返回 `last-sequence-number`

**Lance 当前版本**：
- 通过 `asset_versions.version_order` 获取当前最新版本：`ORDER BY version_order DESC LIMIT 1`
- Lance 写入版本时，`version_key = version_id::text`，`version_order = version_id`
- `metadata_location` 与前驱关系从 `tabular_asset_versions` 获取；`previous_version_id` 通过 `previous_asset_version_id` 关联回 `asset_versions.version_order`
- 返回 `version_id`、`metadata_location`、`previous_version_id`、`timestamp`（来自 `asset_versions.created_at`）
- 尚未注册版本的 Lance 表，`current_version` 返回 `null`

**Lance 表创建后的版本状态**：

根据 [Lance REST Namespace 实现规范](https://lance.org/format/namespace/rest/impl-spec/)：
- `POST /v1/table/{id}/declare`：规范明确"reserving the table name and location **without creating actual data files**"，不创建版本。
- `POST /v1/table/{id}/register`：规范未明确是否自动推断版本。Quasar 决策：**不创建初始版本**，`current_version` 返回 `null`。

**决策依据**：
1. MVP 文档已约束"Catalog 不扫描 Lance 数据文件"，自动推断版本需要访问对象存储，违反此约束。
2. Lance REST 规范设计了独立的 `CreateTableVersion` 端点，暗示版本由客户端显式管理。
3. `declare_table`、`register_table`、`create_table_version` 三者形成一致的语义：Catalog 只维护指针，版本由客户端控制。

**用户使用流程**：
```
# 1. 注册已有 Lance 数据
POST /v1/table/prod$users/register
{"location": "s3://bucket/users.lance"}

# 2. 客户端读取当前版本（通过 Lance SDK 直接访问数据文件）
version = lance_dataset.version()

# 3. 显式注册版本到 Catalog
POST /v1/table/prod$users/version/create
{"version": version, "manifest_path": "...", "naming_scheme": "V2"}
```

**Iceberg 版本查看的 S3 容错行为**：

当 S3 不可用或 metadata.json 不存在时：
- 返回 `current_version=null`，而非请求失败。
- 理由：管理后台的核心需求是"看到 Asset 存在"，版本是次要信息，不应因版本查询失败导致整个 Asset 详情请求不可用。

**为什么不提供版本历史列表**

主流 Catalog 服务（Polaris、Gravitino、Unity Catalog）均不暴露"列出所有历史版本"的端点。版本历史通常通过底层格式自身的机制获取（Iceberg 的 metadata.json、Lance 的 `dataset.versions()`）。V2 的 Unified API 定位为管理层入口，管理后台的刚需是"当前状态"而非"历史变迁"。版本历史列表作为可选能力，后续有明确审计/血缘需求时再单独设计。

### 4.7 V2-F6：各协议适配器格式隔离（详细）

**目标**：V2 中所有标准协议适配器都必须按 `asset_type=table` 与表格式隔离查询和写入。

**变更范围**：
- `PgCatalogStore::list_assets`：SQL 增加 `AND a.asset_type = 'table' AND a.asset_subtype = $format` 条件，并联查 `tabular_assets`
- `PgCatalogStore::get_asset`：SQL 增加 `AND a.asset_type = 'table' AND a.asset_subtype = $format` 条件，并联查 `tabular_assets`
- `PgCatalogStore::get_asset_with_current_version`：asset 联查增加 `asset_type/subtype` 条件
- `PgCatalogStore::asset_exists`：SQL 增加 `asset_type/subtype` 条件
- `PgCatalogStore::drop_asset`：SQL 增加 `asset_type/subtype` 条件，删除 `assets` 记录时由 `tabular_assets` 明细随之清理
- `PgCatalogStore::rename_asset`：SQL 增加 `asset_type/subtype` 条件
- `PgCatalogStore::create_asset`：创建表资产时写入 `assets` 与 `tabular_assets`
- `PgCatalogStore::load_version`：asset 联查增加 `asset_type/subtype` 条件；按 Lance `version_id` 查询时映射到 `asset_versions.version_order`
- `PgCatalogStore::load_current_version`：asset 联查增加 `asset_type/subtype` 条件；按 `asset_versions.version_order DESC` 获取 latest，并联查 `tabular_asset_versions`
- `PgCatalogStore::list_versions`：asset 联查增加 `asset_type/subtype` 条件，并联查 `tabular_asset_versions`
- `PgCatalogStore::create_version`：asset 查询/INSERT SELECT 增加 `asset_type/subtype` 条件；先写 `asset_versions`，再写 `tabular_asset_versions`
- `PgCatalogStore::cas_update_metadata_location`：CAS UPDATE 改为更新 `tabular_assets.metadata_location/schema_snapshot`，并通过 `assets.asset_type/subtype` 限定目标

**注意**：V2 的标准协议适配器以 `asset_type=table` 与 `format` 作为强制隔离条件。Iceberg 端点只操作 Iceberg 表 Asset，Lance 端点只操作 Lance 表 Asset。

---

## 5. 非功能需求

| ID | 需求 | 说明 |
|----|------|------|
| V2-NF1 | 条件编译 | Unified API 作为 Cargo feature `unified`，自动依赖 `iceberg` + `lance` feature，默认全部启用 |
| V2-NF2 | 标准协议并存 | Iceberg/Lance 标准协议端点继续独立存在，端点格式遵循各自上游规范 |
| V2-NF3 | Schema 基线 | V2 以新的目标 DDL 初始化，允许重建数据库；不提供 V1 schema 或 V1 数据处理路径 |
| V2-NF4 | 错误格式 | Unified API 使用 RFC 7807 Problem Details 风格错误体，并扩展 `code` 与 `request_id` |
| V2-NF5 | 无状态 | Unified API 不引入服务端状态，仍可水平扩展 |

### 5.1 Unified API 错误格式

统一使用 RFC 7807 Problem Details 风格错误体，`Content-Type` 为 `application/problem+json`。在标准字段之外扩展：

- `code`：稳定的 Quasar 机器可读错误码
- `request_id`：请求追踪 ID，便于日志检索和排障

**决策依据**：
- RFC 7807 是 HTTP API 常用的问题详情格式，定义了 `type`、`title`、`status`、`detail`、`instance` 等通用字段，避免 Quasar 自行发明一套语义接近但不标准的错误结构。
- Gravitino 等 Catalog/治理系统也提供机器可读错误信息（如 `code` / `type` / `message`），说明管理面 API 需要同时服务程序判断和人工排障。
- Iceberg 与 Lance 标准协议继续使用各自规范错误格式；Problem Details 仅用于 Quasar 自有 Unified API，不混入标准协议端点。

```json
{
  "type": "https://quasar.dev/problems/asset-not-found",
  "title": "Asset not found",
  "status": 404,
  "detail": "Asset 'prod.users' with format 'iceberg' not found",
  "instance": "/unified/v1/namespaces/prod/assets/users?format=iceberg",
  "code": "AssetNotFound",
  "request_id": "018f6f2f-9b7d-7c6b-a31f-4f6b8f5d9f8a"
}
```

**状态码映射**：

| 场景 | HTTP 状态码 | code |
|------|------------|-------|
| Namespace 不存在 | 404 | `NamespaceNotFound` |
| Namespace 已存在 | 409 | `NamespaceAlreadyExists` |
| Namespace 非空（删除时） | 409 | `NamespaceNotEmpty` |
| Asset 不存在 | 404 | `AssetNotFound` |
| Asset 已存在 | 409 | `AssetAlreadyExists` |
| 参数缺失/无效 | 400 | `InvalidInput` |
| format 参数非法 | 400 | `InvalidFormat` |
| 分页 token 无效 | 400 | `InvalidPageToken` |
| pageSize 超过上限 | 400 | `PageSizeTooLarge` |
| 方法不允许 | 405 | `MethodNotAllowed` |
| CAS 冲突 | 409 | `Conflict` |
| 对象存储不可用 | 503 | `ServiceUnavailable` |
| 内部错误 | 500 | `InternalError` |

---

## 6. Unified API 端点设计

### 6.1 路径前缀

统一使用 `/unified/v1/...`，与 `/iceberg/v1/...` 和 `/lance/v1/...` 形成层次对应：

- `/unified` 表达这是 Quasar 自有的统一管理/发现入口，避免纯数字 `/v2` 混淆“产品版本 V2”和“API contract 版本”。
- `/v1` 表示 Unified API 自身的接口版本。后续即使 Quasar 产品进入 V3，只要 Unified API 合同不破坏，路径仍可保持 `/unified/v1`。
- 不使用 `/api` 作为前缀，避免产生一个过宽泛、缺少领域语义的入口。

| 协议 | 前缀 |
|------|------|
| Iceberg REST Catalog | `/iceberg/v1/...` |
| Lance REST Namespace | `/lance/v1/...` |
| Unified REST API | `/unified/v1/...` |

### 6.2 端点清单

#### Namespace

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/unified/v1/namespaces` | 列出 Namespace（支持 `pageToken` / `pageSize`） |
| POST | `/unified/v1/namespaces` | 创建 Namespace |
| GET | `/unified/v1/namespaces/{ns}` | 获取 Namespace 详情及 comment/properties |
| DELETE | `/unified/v1/namespaces/{ns}` | 删除 Namespace（仅空 Namespace） |
| PATCH | `/unified/v1/namespaces/{ns}` | 更新 Namespace comment 与 properties |

#### Asset

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/unified/v1/namespaces/{ns}/assets` | 列出 Asset（支持 `format` 筛选、`name` 过滤、分页） |
| GET | `/unified/v1/namespaces/{ns}/assets/{name}` | 获取 Asset 详情（查询参数 `format` 必填，因同名可跨格式共存） |
| DELETE | `/unified/v1/namespaces/{ns}/assets/{name}` | 删除 Asset（查询参数 `format` 必填） |
| PATCH | `/unified/v1/namespaces/{ns}/assets/{name}` | 更新 Asset comment 与 properties（查询参数 `format` 必填） |
| POST | `/unified/v1/namespaces/{ns}/assets/{name}/rename` | 重命名 Asset（查询参数 `format` 必填） |

#### Version

V2 不提供独立版本历史端点。`GET /unified/v1/namespaces/{ns}/assets/{name}?format={format}` 的 Asset 详情响应中嵌入 `current_version`。

### 6.3 查询参数规范

**命名约定**：
- 请求/响应 JSON 字段统一使用 `snake_case`
- 分页查询参数沿用 Iceberg 风格：`pageToken` / `pageSize`
- `next_page_token` 为响应字段名，不使用 `nextPageToken`

**Asset 列表过滤**：
- `?format=iceberg|lance` —— 按表格式筛选（映射到 `asset_type='table' AND asset_subtype={format}`；不传则返回全部 V2 表资产格式）
- `?name=users` —— 按名称精确匹配（可选）
- `?pageToken=...&pageSize=...` —— 分页

**Asset 单资源操作**：
- `?format=iceberg|lance` —— **必填**（已确认），因同名跨格式可共存
- 非法值（如 `format=parquet`）或空字符串返回 400 `InvalidFormat`
- 参数缺失时单资源操作返回 400 `InvalidInput`

**分页规则**：
- 默认 `pageSize=100`
- 最大 `pageSize=1000`，超过上限返回 400 `PageSizeTooLarge`
- `pageToken` 使用服务端生成的不透明 token；V2 初版可用 offset 编码实现，但客户端不得解析 token 内容
- 列表排序必须稳定：Namespace 按 `name ASC`，Asset 按 `name ASC, format ASC`

### 6.4 状态码规范

| 操作 | 成功状态码 |
|------|------------|
| `POST /unified/v1/namespaces` | 201 Created |
| `GET /unified/v1/namespaces` / `GET /unified/v1/namespaces/{ns}` | 200 OK |
| `PATCH /unified/v1/namespaces/{ns}` | 200 OK |
| `DELETE /unified/v1/namespaces/{ns}` | 204 No Content |
| `GET /unified/v1/namespaces/{ns}/assets` / `GET /unified/v1/namespaces/{ns}/assets/{name}` | 200 OK |
| `PATCH /unified/v1/namespaces/{ns}/assets/{name}` | 200 OK |
| `POST /unified/v1/namespaces/{ns}/assets/{name}/rename` | 200 OK |
| `DELETE /unified/v1/namespaces/{ns}/assets/{name}` | 204 No Content |

### 6.5 请求/响应 DTO 概览

**Namespace 请求/响应**（与 V1 基本一致）：

```rust
// POST /unified/v1/namespaces
struct CreateNamespaceRequest {
    name: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

// PATCH /unified/v1/namespaces/{ns}
// OptionalNullable 是说明性伪类型：实现时需区分 omitted/null/value 三种 JSON 状态。
enum OptionalNullable<T> {
    Unset,
    Null,
    Value(T),
}

struct UpdateMetadataRequest {
    // 三态字段：缺省=不变，null=清空，string=更新。
    #[serde(default)]
    comment: OptionalNullable<String>,
    #[serde(default)]
    removals: Vec<String>,
    #[serde(default)]
    updates: HashMap<String, String>,
}

// NamespaceResponse
struct NamespaceResponse {
    id: String,
    name: String,
    comment: Option<String>,
    properties: HashMap<String, String>,
    created_at: String, // RFC 3339
}
```

**Asset 请求/响应**：

```rust
// PATCH /unified/v1/namespaces/{ns}/assets/{name}?format={format}
// 只更新 Catalog 级 comment/properties，不修改 Iceberg metadata.json 或 Lance manifest
type UpdateAssetMetadataRequest = UpdateMetadataRequest;

// POST /unified/v1/namespaces/{ns}/assets/{name}/rename?format={format}
struct RenameAssetRequest {
    new_name: String,
}

// AssetResponse
struct AssetResponse {
    id: String,
    name: String,
    asset_type: String, // V2 固定为 "table"
    format: String, // "iceberg" | "lance"
    location: String,
    metadata_location: Option<String>,
    comment: Option<String>,
    properties: HashMap<String, String>,
    // 列表接口默认不返回 current_version；详情接口返回。
    current_version: Option<CurrentVersionResponse>,
    created_at: String,
}

#[serde(tag = "format", rename_all = "snake_case")]
enum CurrentVersionResponse {
    Iceberg {
        sequence_number: i64,
        snapshot_id: Option<i64>,
        timestamp_ms: Option<i64>,
    },
    Lance {
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
        timestamp: String, // RFC 3339
    },
}
```

**字段校验**：
- Asset 单资源操作必须提供 `format=iceberg|lance`，缺失或非法时返回 400 `InvalidInput`
- `comment` 更新为三态语义：字段缺省表示不修改，显式传 `null` 表示清空，传字符串表示设置/覆盖
- `PATCH /assets` 与 `PATCH /namespaces` 复用 `UpdateMetadataRequest`，`removals` 删除指定 property key，`updates` 新增/覆盖指定 property key
- `new_name`、Namespace 名称复用 core 层名称校验规则

---

## 7. 关键决策与约束

### 7.1 决策清单

| # | 决策 | 选择 | 理由 |
|---|------|------|------|
| 1 | API 前缀 | `/unified/v1/...` | `/unified` 表达统一入口语义，`/v1` 表达 Unified API 合同版本，避免纯 `/v2` 混淆产品版本和 API 版本 |
| 2 | 能力范围 | 统一发现 + Catalog 级管理 | Iceberg/Lance 原生 API 继续承载格式相关建表逻辑；Unified API V2 不提供跨格式 create 抽象 |
| 3 | Asset 注册表与表资产明细 | `assets` 作为通用注册表；`tabular_assets` 承载表资产字段 | 避免把 `location`、`metadata_location`、`schema_snapshot` 等表语义字段塞进所有资产类型，为后续 fileset/model/function 等非表资产保留扩展空间 |
| 4 | 同名 Asset 跨格式 | **允许共存** | 与 V1 设计文档中"允许同名"的初衷一致；V1 因缺少 `format` 字段未能实现 |
| 5 | 协议适配器隔离 | `PgCatalogStore` 查询增加 `asset_type='table'` 与 `asset_subtype=format` 过滤 | V2 强制按表资产格式隔离标准协议视图 |
| 6 | Unified API 错误格式 | RFC 7807 Problem Details + `code` + `request_id` | 使用通用 HTTP API 错误模型，同时保留机器可读错误码和请求追踪能力；仅用于 Unified API，不影响 Iceberg/Lance 标准错误格式 |
| 7 | Iceberg 版本查看 | 读取 `tabular_assets.metadata_location` 指向的 metadata.json 推断 | Iceberg 版本信息自包含在 metadata.json 中，无需额外存储 |
| 8 | Unified PATCH /assets | 只改 Catalog 级 comment/properties，不碰格式内部状态 | 保持边界清晰；schema/partition 变更仍走 Iceberg commit 协议 |
| 9 | Schema 基线 | V2 以目标 DDL 初始化，允许重建数据库，不提供 V1 schema 或 V1 数据处理路径 | V2 是全新版本，不承担历史版本复杂度 |
| 10 | 条件编译 | `unified` feature 自动依赖 `iceberg` + `lance` | Unified API 需要读取和管理两种格式的 Asset 视图 |
| 11 | 多 Catalog 层 | **不引入** | 维持 `Namespace → Asset` 扁平结构；协议入口（`/iceberg/` vs `/lance/`）区分类型 |
| 12 | Namespace 级 warehouse | **不纳入 V2** | 当前架构完全开放（properties 已存储），后续演进可选 |
| 13 | 标准协议边界 | Iceberg/Lance 标准协议继续独立暴露，Unified API 不替代标准协议 | 保证计算引擎继续走各自协议，管理后台走 Unified API |
| 14 | Unified 删除/重命名 | 仅修改 Catalog 记录，不操作对象存储 | Unified API 是管理面，不做数据面清理或路径迁移 |
| 15 | Unified Asset 创建 | **不纳入 V2** | 不同格式的建表请求、元数据初始化和对象存储写入差异较大，继续由 Iceberg/Lance 原生 API 及 Quasar 当前内部逻辑承载 |
| 16 | 版本历史列表 | 不纳入 Unified API | 历史版本属于格式内部能力；Iceberg 历史在 metadata 链中，Lance 历史由格式/客户端机制消费，Unified API 只服务管理面当前态 |
| 17 | Asset 单资源 format 参数 | `format` 必填 | 同一 Namespace 下允许跨格式同名 Asset，单资源操作必须避免隐式猜测 |
| 18 | Comment 字段 | `namespaces` 与通用注册表 `assets` 引入 nullable `comment` 一等字段；`asset_versions` 不引入 | Gravitino、Unity Catalog 都把 schema/table 的说明性文本作为一等元数据暴露；Polaris Generic Table 的核心字段未列出 comment，说明它不是格式互操作的必需字段。Quasar 服务数据发现/管理后台时，Namespace/Asset comment 有明确展示价值；AssetVersion 的 comment 更接近 commit summary，语义不同，V2 暂不混入 |
| 19 | Asset Version 表 | `asset_versions` 作为通用版本注册表；`tabular_asset_versions` 承载表资产版本字段 | `version_key` 表达原生版本标识，`version_order` 表达可比较的版本顺序；`metadata_location`、前驱版本等表语义字段不污染通用版本表，为后续 model/fileset/function 等版本类型保留扩展空间 |

---

## 8. 验收标准

### 8.1 Schema 与约束验收

- 从空数据库按 V2 目标 DDL 初始化成功，服务可正常启动。
- `namespaces` 表包含 nullable `comment`；`assets` 表作为通用资产注册表，只包含资产身份、类型、comment、properties 与审计时间，不包含 `location`、`metadata_location`、`schema_snapshot` 等表资产字段。
- `tabular_assets` 表包含 `asset_id`、`location`、`metadata_location`、`schema_snapshot`，且 `asset_id` 一对一指向 `assets(id)`。
- `asset_versions` 表作为通用版本注册表，包含 `asset_id`、`version_key`、`version_order`、`properties`、`created_at`，不包含 `comment`、`metadata_location`。
- `tabular_asset_versions` 表包含 `asset_version_id`、`metadata_location`、`previous_asset_version_id`，且一对一指向 `asset_versions(id)`。
- 外键删除行为符合设计：非空 Namespace 不可被级联删除；删除 Asset 时清理对应 `tabular_assets`、`asset_versions`、`tabular_asset_versions` 记录。
- 约束覆盖以下场景：非法 `asset_type` / `asset_subtype` 被拒绝；同一 Namespace 下同名同类型同子类资产重复创建被拒绝；同名 Iceberg / Lance 表资产允许共存；同一 Asset 下重复 `version_key` 被拒绝；同一 Asset 下重复非空 `version_order` 被拒绝；`previous_asset_version_id` 指向其他 Asset 的版本时由存储层拒绝。

### 8.2 Unified API 正向验收

- Namespace CRUD：创建 Namespace 时可写入 `comment` 与 `properties`；详情与列表响应可读回 `comment`；PATCH 支持 comment 字符串覆盖、显式 `null` 清空、字段缺省保持不变；空 Namespace 可删除。
- Asset 发现：分别通过 Iceberg 原生 API 与 Lance 原生 API 创建同名表资产后，`GET /unified/v1/namespaces/{ns}/assets` 返回两条记录；响应包含 `asset_type=table`、`format`、`location`、`metadata_location`、`comment`、`properties` 等管理面字段。
- Asset 查询过滤：`format=iceberg` 只返回 Iceberg 表资产，`format=lance` 只返回 Lance 表资产；`name` 过滤、`pageSize`、`pageToken` 生效；分页结果排序稳定，建议按 `name ASC, format ASC`。
- Asset 详情：`GET /unified/v1/namespaces/{ns}/assets/{name}?format={format}` 的 `format` 必填；详情响应包含 `current_version`，无当前版本时返回 `null`。
- Asset 更新：PATCH 只更新通用注册表 `assets.comment` 与 `assets.properties`，不修改对象存储文件、Iceberg metadata.json、Lance manifest 或版本记录。
- Asset 重命名：重命名某一格式资产不会影响同 Namespace 下同名的另一格式资产；目标名冲突时返回冲突错误。
- Asset 删除：删除某一格式资产只删除 Catalog 记录及其表资产/版本明细记录，不删除对象存储中的数据文件或元数据文件。
- Unified Asset 创建：`POST /unified/v1/namespaces/{ns}/assets` 不作为 V2 端点暴露；若请求该路径，应返回 405 `MethodNotAllowed`，且不得创建任何 Catalog 记录。

### 8.3 Unified API 负向与错误格式验收

- Unified API 错误响应 `Content-Type` 为 `application/problem+json`，响应体包含 `type`、`title`、`status`、`detail`、`instance`、`code`、`request_id`。
- 缺失必填 `format` 返回 400 `InvalidInput`；空字符串 `format` 或非法 `format` 返回 400 `InvalidFormat`。
- 非法 `pageToken` 返回 400 `InvalidPageToken`；`pageSize > 1000` 返回 400 `PageSizeTooLarge`。
- Namespace 不存在、Asset 不存在分别返回 404，并使用可区分的机器可读错误码。
- 重复创建 Namespace、重复创建同名同类型同子类 Asset、重命名到已存在目标时返回 409。
- 删除非空 Namespace 返回 409 `NamespaceNotEmpty`。
- Iceberg CAS commit 冲突返回 409；对象存储不可用返回 503，但标准协议端点继续使用各自协议规定的错误格式。
- 验证 Unified API 的 Problem Details 错误格式不会泄漏到 `/iceberg/v1/...` 与 `/lance/v1/...` 标准协议端点。

### 8.4 标准协议回归验收

- Iceberg REST Catalog 的 namespace、table create、load、list、drop、rename、commit 端点路径、请求格式、响应格式、错误格式均保持 MVP 约定不变。
- Iceberg 原生 create table 成功后同时写入 `assets` 与 `tabular_assets`；commit 成功后更新 `tabular_assets.metadata_location` 与 `schema_snapshot`。
- Lance REST Namespace 的 declare、register、describe、drop、rename、create version、list versions 端点路径、请求格式、响应格式、错误格式均保持 MVP 约定不变。
- Lance `declare_table` 与 `register_table` 只创建资产记录，不创建初始 `asset_versions`；显式 `create_table_version` 成功后同时写入 `asset_versions` 与 `tabular_asset_versions`。
- Iceberg list/load/drop/rename/commit 只作用于 `asset_subtype=iceberg` 的表资产；Lance list/describe/drop/rename/version 只作用于 `asset_subtype=lance` 的表资产；同一 Namespace 下同名 Iceberg / Lance 表资产互不干扰。

### 8.5 当前版本与版本模型验收

- Iceberg 当前版本：metadata.json 存在且无当前 snapshot 时，`sequence_number` 来自 `last-sequence-number`，`snapshot_id` 与 `timestamp_ms` 返回 `null`；存在当前 snapshot 时，字段从 metadata.json 正确解析。
- Iceberg 对象存储异常：metadata.json 缺失或对象存储临时不可用时，Unified Asset 详情仍返回 200，`current_version=null`，不得导致整个 Asset 详情失败。
- Lance 无版本状态：`declare_table` 或 `register_table` 后立即查询 Unified Asset 详情，`current_version=null`。
- Lance 版本排序：创建 v1、v2 后，通过 `asset_versions.version_order DESC` 得到 v2 为 latest；`previous_version_id` 通过 `tabular_asset_versions.previous_asset_version_id` 关联回前驱版本的 `version_order`。
- Lance 版本映射：`version_key = version_id::text`，`version_order = version_id`；重复 `version_key` 或重复非空 `version_order` 均返回冲突。
- 表版本隔离：`previous_asset_version_id` 必须指向同一 Asset 的版本；跨 Asset 引用被拒绝。
- Unified API 不提供版本历史列表；历史版本能力仍由标准协议或格式内部机制承载。

### 8.6 事务一致性验收

- Iceberg 原生 create table 写对象存储失败时，DB 不产生 `assets` 或 `tabular_assets` 记录。
- 表资产创建过程中，如果 `assets` 写入成功但 `tabular_assets` 写入失败，事务回滚，两张表均不留下半成品。
- Lance `create_table_version` 过程中，如果 `asset_versions` 写入成功但 `tabular_asset_versions` 写入失败，事务回滚，两张版本表均不留下半成品。
- Iceberg CAS commit 中对象存储写入成功但 DB CAS 更新失败时，返回 409 冲突，Catalog 指针不更新；遗留 metadata 文件的清理策略沿用 MVP 对象存储约定。
- 并发创建同 Namespace、同名、同类型、同子类资产时，只允许一个成功，其余返回冲突。
- 并发创建同一 Lance `version_id` 时，只允许一个成功，其余返回冲突。
- 并发 Iceberg commit 使用过期 `metadata_location` 时返回冲突，不能覆盖最新指针。

### 8.7 构建、Feature 与无状态验收

- 格式检查通过：`cargo fmt --all --manifest-path quasar/Cargo.toml`。
- 全量测试通过：`cargo test --all-features --manifest-path quasar/Cargo.toml`。
- 启用 `unified` feature 时，`/unified/v1/...` 路由注册成功，且 Iceberg/Lance 相关 feature 依赖满足。
- 如项目支持关闭 `unified` feature，关闭后 Iceberg/Lance 标准协议仍可构建并通过既有回归测试；如不支持关闭，应在构建配置中明确约束。
- 启动两个 server 实例连接同一 PostgreSQL，执行读写 smoke test，确认请求处理不依赖进程内状态。

---

## 9. 修订记录

### V1.0

- 新增：演进背景与动机（V1 交付成果、遗留问题、V2 核心动机）
- 新增：V2 定位与边界（三层架构视图、保留/新增/不实现清单）
- 新增：数据模型基线（V1 参考基线、V2 目标状态、Schema 初始化策略、对比表）
- 新增：功能需求 V2-F1 ~ V2-F6（Asset format 标识、Namespace 管理、Asset 发现、Asset 管理、Version 查看、适配器格式隔离）
- 新增：非功能需求 V2-NF1 ~ V2-NF5（条件编译、标准协议并存、Schema 基线、错误格式、无状态）
- 新增：Unified API 端点设计（路径前缀 `/unified/v1/...`、Namespace/Asset/Version 端点清单、查询参数规范、DTO 概览）
- 新增：关键决策清单（API 前缀、能力范围、format 存储、错误格式等）
- 新增：验收标准（数据库 Schema、Unified API 端点、标准协议）

### V1.1

- **确认**：Iceberg Asset 创建时 schema 可由请求体提供
- **确认**：版本历史列表不纳入 V2；V2-F5 从"版本历史查看"收窄为"当前版本查看"
- **确认**：`GET /assets/{name}` 的 `format` 查询参数必填
- **确认**：`unified` feature 自动依赖 `iceberg` + `lance` feature
- **确认**：V2 是新的 schema 基线，不提供 V1 schema 处理路径
- **确认**：V2-F6 要求标准协议适配器按 format 隔离
- **确认**：Namespace 级 warehouse 覆盖不纳入 V2，当前架构对扩展完全开放
- **确认**：PATCH /namespaces 语义为增量更新（removals + updates）
- **确认**：Iceberg Asset 创建时 S3 不可用遵循 MVP 已确认的"先 S3 后 DB"策略
- **新增**：V2 定位与边界中增加"不做事项"清单
- **新增**：Unified API 定位澄清——面向管理后台/数据平台，计算引擎仍走各自标准协议入口
- **更新**：关键决策清单从 10 条增至 13 条
- **更新**：V2-F1 说明——新创建 Asset 时显式指定 format 仅针对 Unified API；标准协议入口由协议前缀隐式决定
- **更新**：V2-F2 说明——PATCH 语义为增量更新；Namespace 级 warehouse 覆盖不纳入 V2
- **更新**：V2-F4 补充——Iceberg Asset 创建时对象存储不可用处理策略
- **更新**：非功能需求 V2-NF1/NF2/NF3——feature 依赖关系、标准协议边界、Schema 基线

### V1.2

- **澄清**：V2 文档作为 Unified API 需求基准；与 MVP 旧文档中 Namespace/Asset format 表述冲突时，以 V2 文档为准
- **澄清**：Namespace 是共享组织单元，Asset/Table 是格式隔离边界；标准协议路径、响应格式、错误格式保持不变
- **修正**：Schema 策略改为 V2 全新基线；`assets.format` 是 V2 初始 DDL 的必备字段
- **补充**：V2-F6 格式隔离覆盖 version、CAS 等所有通过 `(namespace, asset_name)` 定位 Asset 的存储方法
- **补充**：Unified API 状态码、分页规则、字段命名约定、PATCH/rename DTO、`current_version` 结构化响应
- **澄清**：Iceberg Unified 创建时 `schema` 可选，缺省创建空 schema；Lance 创建不接受 `schema`
- **修正**：对象存储不可用统一返回 503，且 DB 不产生 Asset 记录
- **澄清**：Unified 删除/重命名仅修改 Catalog 记录，不删除或移动对象存储数据
- **更新**：验收标准增加标准协议测试与测试覆盖验收

### V1.3

- **澄清**：V2 是全新的版本基线，V1 schema 与 V1 数据处理不在 V2 范围内
- **更新**：数据模型章节从“演进”改为“基线”，V2 目标状态直接给出完整目标 DDL
- **删除**：历史版本适配、协议复测命名、反向 schema 处理、后台变更服务等相关表述
- **更新**：Schema 策略改为 V2 以目标 DDL 初始化，允许重建数据库
- **更新**：标准协议验收只验证 V2 下 Iceberg/Lance 协议并存和 format 隔离，不再描述为历史版本验收

### V1.4

- **确认**：Unified API 路径前缀改为 `/unified/v1/...`，避免纯 `/v2` 混淆产品版本和 API 合同版本
- **确认**：Unified API 错误格式采用 RFC 7807 Problem Details 风格，并扩展 `code` 与 `request_id`
- **新增**：错误格式决策依据，说明与 RFC 7807、Catalog/治理系统机器可读错误码、标准协议错误格式隔离之间的关系
- **更新**：已确认事项并入关键决策清单，不再单列“已确认的待决策项”

### V1.5

- **确认**：Unified API V2 暂不提供 Asset 创建端点；Iceberg/Lance 表创建继续由各自原生 API 和 Quasar 当前内部处理逻辑承载。
- **删除**：`POST /unified/v1/namespaces/{ns}/assets` 端点、`CreateAssetRequest` DTO、Unified create 相关验收与测试要求。
- **更新**：V2-F4 范围收窄为 Asset get/delete/patch/rename，创建边界改为明确不纳入 V2。
- **新增**：`comment` 字段设计评估，并给出“仅在 Namespace/Asset 引入”的建议。

### V1.6

- **确认**：`comment` 按“仅 `namespaces` + `assets` 一等字段，`asset_versions` 不加”的方案落入正式决策。
- **更新**：V2 目标 DDL、Core 模型、Namespace/Asset DTO 增加 nullable `comment` 字段。
- **更新**：PATCH 语义增加 comment 三态更新：字段缺省不变，显式 `null` 清空，字符串设置/覆盖。
- **更新**：验收标准与测试覆盖增加 comment 字段验证。

### V1.7

- **确认**：`assets` 改为通用资产注册表，不再承载表资产专有字段。
- **新增**：`tabular_assets` 表，承载表资产的 `location`、`metadata_location`、`schema_snapshot` 等字段。
- **更新**：`assets.format` 方案被 `assets.asset_type` + `assets.asset_subtype` 取代；V2 中 `asset_type=table`，`asset_subtype=iceberg|lance`。
- **澄清**：当前实现已有独立 `asset_versions` 表；V2 保留该表名，但语义限定为表资产版本记录，外键指向 `tabular_assets(asset_id)`。
- **更新**：功能需求、DTO、验收标准和关键决策同步资产注册表/表资产明细表拆分。

### V1.8

- **确认**：版本表按”通用版本注册表 + 资产类型版本明细表”拆分。
- **更新**：`asset_versions` 改为通用版本注册表，包含 `version_key`、`version_order`、`properties`、`created_at`，不再直接存储 `metadata_location`。
- **新增**：`tabular_asset_versions` 表，承载表资产版本的 `metadata_location` 与 `previous_asset_version_id`。
- **更新**：Lance version 映射规则：`version_key = version_id::text`，`version_order = version_id`，latest 通过 `version_order DESC` 查询。
- **更新**：验收标准与存储测试要求覆盖 `asset_versions` + `tabular_asset_versions` 双表写入。

### V1.9

- **确认**：Lance `declare_table` 和 `register_table` 均不创建初始版本，`current_version` 返回 `null`。
- **补充**：Lance 版本初始化决策依据——基于 Lance REST Namespace 规范，Catalog 不扫描数据文件，版本由客户端通过 `create_table_version` 显式注册。
- **补充**：用户使用流程示例，展示 Lance 表注册后显式创建版本的步骤。
- **确认**：Iceberg 版本查看时 S3 不可用返回 `current_version=null`，而非请求失败。
- **补充**：`format` 参数校验规则——非法值或空字符串返回 400 `InvalidInput`，错误码 `InvalidFormat`。
- **补充**：完整错误码枚举，新增 `NamespaceNotFound`、`NamespaceAlreadyExists`、`NamespaceNotEmpty`、`AssetNotFound`、`AssetAlreadyExists`、`InvalidFormat`、`InvalidPageToken`、`PageSizeTooLarge` 等场景化错误码。
- **补充**：V2 部署模式声明——全新部署，从空数据库开始，不涉及 V1 数据迁移。
- **修正**：架构图格式错位问题。

### V1.10

- **更新**：验收标准从手工 smoke check 扩展为可测试验收矩阵，覆盖 Schema 约束、Unified API 正向路径、负向错误格式、标准协议回归、当前版本模型、事务一致性、feature 构建与无状态部署。
- **补充**：验收标准明确 Unified Asset 创建端点不暴露，误请求不得创建 Catalog 记录。
- **补充**：验收标准明确 Iceberg/Lance 标准协议错误格式与 Unified Problem Details 错误格式隔离。
- **补充**：验收标准明确 `assets` 通用注册表、`tabular_assets` 表资产明细表、`asset_versions` 通用版本注册表、`tabular_asset_versions` 表版本明细表的字段边界与约束。
- **修正**：V2 核心动机与新增范围中残留的 `assets.format` 表述，统一改为 `asset_type` / `asset_subtype` 与表资产明细表拆分。

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。
