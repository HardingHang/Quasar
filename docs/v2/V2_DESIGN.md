# Quasar V2 设计文档

> 本文档基于 `docs/v2/V2_REQUIREMENTS.md` 需求分析，定义 Quasar V2（Unified REST API）的完整技术设计。
> V2 交付标志：一套格式无关的 Unified REST API（`/unified/v1/...`）可供管理后台/数据平台统一纳管 Iceberg 与 Lance 资产，同时 Iceberg REST Catalog（`/iceberg/v1/...`）和 Lance REST Namespace（`/lance/v1/...`）两套标准协议继续独立运行。
>
> **前置假设**：V2 是全新部署，从空数据库开始。不涉及 V1 数据迁移或 V1 schema 处理。

---

## 目录

1. [概述](#1-概述)
2. [V2 范围与边界](#2-v2-范围与边界)
3. [数据模型设计](#3-数据模型设计)
4. [Crate 分层与模块职责](#4-crate-分层与模块职责)
5. [核心接口定义](#5-核心接口定义)
6. [Unified API 端点设计](#6-unified-api-端点设计)
7. [错误处理](#7-错误处理)
8. [标准协议适配器修改](#8-标准协议适配器修改)
9. [版本查看实现](#9-版本查看实现)
10. [条件编译](#10-条件编译)
11. [开发阶段划分](#11-开发阶段划分)
12. [关键决策清单](#12-关键决策清单)
13. [验收标准](#13-验收标准)
14. [修订记录](#14-修订记录)

---

## 1. 概述

### 1.1 V1 遗留问题

V1 后期通过 DDL 变更将 `namespaces` 表的 `format` 字段移除，使 Namespace 成为格式无关的组织单元。但 `assets` 表至今未引入区分资产类型与格式子类的字段，导致：

- 同一 Namespace 下不允许同名 Asset 跨格式共存
- Iceberg 的 `list_tables` 可能混入 Lance Asset（反之亦然）
- 管理视角无法回答"`prod` 下有哪些表，分别是什么格式"

### 1.2 V2 目标

引入一套**格式无关的 Unified REST API**，解决管理后台/数据平台的统一纳管需求，同时：

- 修复 V1 的数据模型缺陷（`assets` 通用化 + `asset_type`/`asset_subtype` 隔离）
- 标准协议端点（Iceberg/Lance）继续独立运行，格式不变
- 为后续非表资产（AI 模型、特征等）预留扩展空间

### 1.3 架构变化概览

```
V1 架构（当前）                          V2 架构（目标）
┌─────────────────┐                     ┌─────────────────┐
│ /iceberg/v1/... │                     │ /iceberg/v1/... │
│ /lance/v1/...   │         ───►        │ /lance/v1/...   │
└────────┬────────┘                     │ /unified/v1/... │
         │                              └────────┬────────┘
         │                                       │
    ┌────┴────┐                             ┌────┴────┐
    │ assets  │                             │ assets  │◄── 通用资产注册表
    │ (表字段)│                             │(通用字段)│
    └────┬────┘                             └───┬─────┘
         │                                      │
    ┌────┴────┐                             ┌──┴──────────────┐
    │asset_   │                             │ tabular_assets  │◄── 表资产明细
    │versions │                             │ asset_versions  │◄── 通用版本注册表
    │(含表字段)│                             │tabular_asset_   │◄── 表版本明细
    └─────────┘                             │  versions       │
                                            └─────────────────┘
```

---

## 2. V2 范围与边界

### 2.1 必须保留（V2 不触碰）

| 项目 | 说明 |
|------|------|
| Iceberg REST Catalog 端点路径 | `/iceberg/v1/...` 保持不变 |
| Lance REST Namespace 端点路径 | `/lance/v1/...` 保持不变 |
| 请求/响应格式 | 两套标准协议的 JSON 结构不变 |
| 错误格式 | Iceberg `ErrorResponse`、Lance RFC-7807 不变 |
| CAS commit 语义 | Iceberg requirements/updates/CAS 不变 |
| Lance version 注册语义 | declare/register/version-create 不变 |

### 2.2 V2 新增

| 项目 | 说明 |
|------|------|
| Unified REST API | `/unified/v1/...` 端点集 |
| 通用资产注册表 | `assets` 表增加 `asset_type`/`asset_subtype`/`comment` |
| 表资产明细表 | `tabular_assets` 承载 `location`/`metadata_location`/`schema_snapshot` |
| 通用版本注册表 | `asset_versions` 重构为 `version_key`/`version_order`/`properties` |
| 表版本明细表 | `tabular_asset_versions` 承载 `metadata_location`/`previous_asset_version_id` |
| Namespace comment | `namespaces` 表增加 nullable `comment` |
| RFC 7807 错误体 | Unified API 专用，扩展 `code`/`request_id` |

### 2.3 V2 不实现

| 项目 | 说明 |
|------|------|
| 替代标准协议 | Unified API 是公共子集，不是替代 |
| 格式内部语义变更 | Iceberg schema/partition/snapshot、Lance manifest 仍走标准协议 |
| 多租户/鉴权/审计 | 不在 V2 范围 |
| 数据面操作 | 不读写实际数据文件 |
| 多 Catalog 层 | 维持 `Namespace → Asset` 扁平结构 |
| Namespace 级 warehouse 覆盖 | 全局 `warehouse_path` 不变 |
| 版本历史列表 | Unified API 只返回 `current_version` |
| Unified Asset 创建 | `POST /unified/v1/.../assets` 不暴露 |
| V1 数据处理 | 不提供 V1 schema 自动转换或数据迁移 |

---

## 3. 数据模型设计

### 3.1 设计原则

1. **通用注册表 + 类型明细表**：`assets` 只存通用身份和治理元数据，类型特有字段落在明细表（`tabular_assets`），为后续 `model`、`fileset` 等资产类型预留空间。
2. **通用版本注册表 + 类型版本明细表**：`asset_versions` 只存版本身份和排序，类型版本字段落在 `tabular_asset_versions`。
3. **格式隔离在 Asset 层**：Namespace 是共享组织单元，Asset 按 `(asset_type, asset_subtype)` 隔离。
4. **V2 全新基线**：Schema 以目标 DDL 初始化，不要求保留 V1 处理路径。

### 3.2 PostgreSQL DDL（V2 目标状态）

```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- namespaces: 格式无关的组织单元
-- ============================================
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================
-- assets: 通用资产注册表
-- ============================================
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

CREATE INDEX idx_assets_namespace ON assets(namespace_id);
CREATE INDEX idx_assets_type ON assets(asset_type);
CREATE INDEX idx_assets_namespace_type_subtype ON assets(namespace_id, asset_type, asset_subtype);

-- ============================================
-- tabular_assets: 表资产明细表
-- ============================================
CREATE TABLE tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB
);

-- ============================================
-- asset_versions: 通用版本注册表
-- ============================================
CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_order BIGINT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, version_key)
);

CREATE INDEX idx_asset_versions_asset ON asset_versions(asset_id);
CREATE INDEX idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;
CREATE UNIQUE INDEX idx_asset_versions_asset_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;

-- ============================================
-- tabular_asset_versions: 表资产版本明细表
-- ============================================
CREATE TABLE tabular_asset_versions (
    asset_version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    metadata_location TEXT NOT NULL,
    previous_asset_version_id UUID REFERENCES asset_versions(id)
);
```

### 3.3 Core 领域模型

```rust
// ============================================================
// quasar-core/src/models.rs
// ============================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use strum::{Display, EnumString, IntoStaticStr};
use uuid::Uuid;

/// 资产格式枚举，用于标准协议适配器的格式隔离
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display, EnumString, IntoStaticStr,
)]
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

/// 通用资产类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum AssetType {
    Table,
}

impl AssetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetType::Table => "table",
        }
    }
}

/// PATCH 字段三态：
/// - Missing：请求体中没有该字段，不修改
/// - Null：请求体中显式传 null，清空
/// - Value(T)：请求体中传具体值，设置/覆盖
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchField<T> {
    Missing,
    Null,
    Value(T),
}

impl<T> Default for PatchField<T> {
    fn default() -> Self {
        Self::Missing
    }
}

impl<'de, T> Deserialize<'de> for PatchField<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(|value| match value {
            Some(v) => Self::Value(v),
            None => Self::Null,
        })
    }
}

/// Namespace: 格式无关的组织单元
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: Uuid,
    pub name: String,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// Asset: 通用资产注册表实体
/// V2 中只包含身份、类型、comment、properties 等通用字段
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,
    pub name: String,
    pub asset_type: AssetType,
    pub asset_subtype: String,  // V2: "iceberg" | "lance"
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// TabularAsset: 表资产明细
/// 承载表资产特有的 location、metadata_location、schema_snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAsset {
    pub asset_id: Uuid,
    pub location: String,
    pub metadata_location: Option<String>,
    pub schema_snapshot: Option<serde_json::Value>,
}

/// Asset + TabularAsset 组合，用于需要同时访问通用字段和表字段的场景
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetWithTabular {
    pub asset: Asset,
    pub tabular: TabularAsset,
}

/// AssetVersion: 通用版本注册表实体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,
    pub version_key: String,
    pub version_order: Option<i64>,
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// TabularAssetVersion: 表资产版本明细
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TabularAssetVersion {
    pub asset_version_id: Uuid,
    pub metadata_location: String,
    pub previous_asset_version_id: Option<Uuid>,
}

/// AssetVersion + TabularAssetVersion 组合
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetVersionWithTabular {
    pub version: AssetVersion,
    pub tabular_version: TabularAssetVersion,
}
```

### 3.4 模型变化对比（V1 → V2）

| 层面 | V1 状态 | V2 状态 | 影响 |
|------|---------|---------|------|
| `namespaces` | 无 `comment` | 增加 `comment TEXT` | PATCH /namespaces 可更新 comment |
| `assets` | 承载表字段（location、metadata_location、schema_snapshot） | 通用注册表，新增 `asset_type`/`asset_subtype`/`comment` | 标准协议创建表资产需同时写 `assets` + `tabular_assets` |
| `tabular_assets` | 不存在 | 新增，承载表资产字段 | 所有通过 `location`/`metadata_location` 查询的地方需联查 |
| `asset_versions` | 含 `version_id`/`metadata_location`/`previous_version_id` | 通用版本注册表，含 `version_key`/`version_order`/`properties` | Lance 版本写入需拆分为两表 |
| `tabular_asset_versions` | 不存在 | 新增，承载表版本字段 | Lance `create_version` 需先写 `asset_versions` 再写 `tabular_asset_versions` |

### 3.5 外键与级联行为

| 父表 | 子表 | 删除行为 | 说明 |
|------|------|----------|------|
| `namespaces` | `assets` | `ON DELETE RESTRICT` | 非空 Namespace 不可删除 |
| `assets` | `tabular_assets` | `ON DELETE CASCADE` | 删除 Asset 自动清理表明细 |
| `assets` | `asset_versions` | `ON DELETE CASCADE` | 删除 Asset 自动清理版本记录 |
| `asset_versions` | `tabular_asset_versions` | `ON DELETE CASCADE` | 删除版本自动清理表版本明细 |
| `asset_versions` | `tabular_asset_versions.previous_asset_version_id` | `REFERENCES asset_versions(id)` | 存储层需校验指向同一 Asset |

---

## 4. Crate 分层与模块职责

### 4.1 目标分层

```
quasar/
├── quasar-core          # 核心领域模型与 trait 定义
│                        # - Namespace / Asset / AssetVersion 等领域对象
│                        # - AssetType / AssetFormat 枚举
│                        # - CatalogStore trait（存储抽象）
│                        # - 协议无关的错误类型 StoreError
│                        # - 不依赖具体框架或存储实现
│
├── quasar-storage       # 存储层实现
│                        # - PostgreSQL 实现 CatalogStore trait
│                        # - V2 目标 DDL 初始化脚本
│                        # - 连接池管理（deadpool-postgres）
│
├── quasar-adapter       # 协议适配层
│   ├── iceberg          # Iceberg REST Catalog 适配
│   │                    # - /iceberg/v1/... 路由与 handler
│   │                    # - 查询时注入 asset_type='table' AND asset_subtype='iceberg'
│   │                    # - CAS commit 协调
│   │                    # - 创建表资产时同时写 assets + tabular_assets
│   │
│   ├── lance            # Lance REST Namespace 适配
│   │                    # - /lance/v1/... 路由与 handler
│   │                    # - 查询时注入 asset_type='table' AND asset_subtype='lance'
│   │                    # - 创建表资产时同时写 assets + tabular_assets
│   │                    # - 创建版本时同时写 asset_versions + tabular_asset_versions
│   │
│   └── unified          # Unified REST API 适配（新增，需 unified feature）
│                        # - /unified/v1/... 路由与 handler
│                        # - RFC 7807 Problem Details 错误格式
│                        # - Iceberg current_version 读取（对象存储）
│                        # - Lance current_version 查询（数据库）
│
└── quasar-server        # 服务入口
                         # - axum 路由注册与中间件
                         # - Feature flag 条件合并路由
                         # - Health / Readiness 端点
```

### 4.2 依赖关系

```
server ──► adapter ──► core
  │                      ▲
  └──► storage ──────────┘
```

### 4.3 Crate 级 Cargo.toml 变更

#### `quasar/Cargo.toml`（workspace 根）

无变更。`unified` feature 在 adapter/server 层面定义。

#### `quasar/adapter/Cargo.toml`

```toml
[features]
default = ["lance", "iceberg", "unified"]
lance = []
iceberg = ["dep:object_store"]
unified = ["lance", "iceberg"]  # Unified API 依赖两套协议
```

#### `quasar/server/Cargo.toml`

```toml
[features]
default = ["lance", "iceberg", "unified"]
lance = ["quasar-adapter/lance"]
iceberg = ["quasar-adapter/iceberg", "dep:object_store"]
unified = ["lance", "iceberg", "quasar-adapter/unified"]
```

### 4.4 模块条件编译

```rust
// adapter/src/lib.rs
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
#[cfg(feature = "unified")]
pub mod unified;
#[cfg(feature = "iceberg")]
pub mod object_store_util;
pub use quasar_core::*;
```

```rust
// server/src/lib.rs
#[cfg(feature = "unified")]
use quasar_adapter::unified::UnifiedConfig;

#[derive(Clone, Default)]
pub struct AppConfig {
    #[cfg(feature = "lance")]
    pub lance: LanceConfig,
    #[cfg(feature = "iceberg")]
    pub iceberg: IcebergConfig,
    #[cfg(feature = "unified")]
    pub unified: UnifiedConfig,
}

// 在 create_app_with_config 中：
#[cfg(feature = "unified")]
{
    router = router
        .merge(quasar_adapter::unified::routes())
        .layer(Extension(config.unified));
}
```

---

## 5. 核心接口定义

### 5.1 CatalogStore Trait（V2 版本）

V2 对 `CatalogStore` trait 做以下调整：

1. **Namespace 方法**：增加 `comment` 创建与三态 PATCH 支持
2. **Asset 方法**：`create_asset` 改为同时写 `assets` + `tabular_assets`；所有查询增加 `asset_type`/`asset_subtype` 过滤
3. **Version 方法**：`create_version` 拆分为先写 `asset_versions` 再写 `tabular_asset_versions`
4. **CAS 方法**：在同一事务内更新 `tabular_assets` 指针与 `assets.properties`

```rust
// quasar-core/src/store.rs

use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::StoreError;
use crate::models::{
    Asset, AssetFormat, AssetVersionWithTabular,
    Namespace, PatchField, TabularAsset,
};

#[async_trait]
pub trait CatalogStore: Send + Sync {
    // ── Namespace（格式无关，V2 增加 comment）──────────────

    async fn create_namespace(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self, offset: i64, limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(&self, name: &str) -> Result<Namespace, StoreError>;

    async fn namespace_exists(&self, name: &str) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, name: &str) -> Result<(), StoreError>;

    async fn update_namespace(
        &self,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    // ── Asset（表资产，强制 format 隔离）─────────────────────

    /// 创建表资产。事务内同时写入 `assets` 和 `tabular_assets`。
    ///
    /// 注意：标准协议（Iceberg/Lance）的建表请求体中通常不包含 comment
    /// 字段，因此创建时 comment 固定为 NULL。如需设置 comment，应通过
    /// `update_asset_properties` 在创建后更新。
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

    /// 列出某 Namespace 下的表资产。返回 Asset 及其表资产明细。
    /// SQL 必须注入 `a.asset_type = 'table' AND a.asset_subtype = $format`。
    async fn list_assets(
        &self,
        namespace_name: &str,
        format: AssetFormat,
    ) -> Result<Vec<AssetWithTabular>, StoreError>;

    async fn get_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<Asset, StoreError>;

    /// 获取 Asset 及其表资产明细（联查 tabular_assets）
    async fn get_asset_with_tabular(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError>;

    /// Get asset with its tabular fields and current version in a single query.
    async fn get_asset_with_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(Asset, TabularAsset, Option<AssetVersionWithTabular>), StoreError>;

    async fn asset_exists(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
    ) -> Result<(), StoreError>;

    async fn rename_asset(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError>;

    /// 更新 Asset 的 Catalog 级 comment 和 properties
    /// （Unified API PATCH /assets 使用，不触碰格式内部状态）
    async fn update_asset_properties(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError>;

    // ── Version（Lance 专用）────────────────────────────────

    async fn load_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
    ) -> Result<AssetVersionWithTabular, StoreError>;

    async fn load_current_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Option<AssetVersionWithTabular>, StoreError>;

    async fn list_versions(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
    ) -> Result<Vec<AssetVersionWithTabular>, StoreError>;

    /// Create a version record.
    /// 事务内先写 `asset_versions`，再写 `tabular_asset_versions`。
    async fn create_version(
        &self,
        namespace_name: &str,
        format: AssetFormat,
        asset_name: &str,
        version_id: i64,
        metadata_location: String,
        previous_version_id: Option<i64>,
    ) -> Result<AssetVersionWithTabular, StoreError>;

    // ── Iceberg CAS ────────────────────────────────────────

    /// Atomically update tabular_assets.metadata_location if it matches.
    /// SQL 必须通过 `assets.asset_type = 'table' AND assets.asset_subtype = $format` 限定目标。
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
    ) -> Result<(), StoreError>;

    // ── Unified API 专用 ───────────────────────────────────

    /// 跨格式列出 Asset（Unified API 使用）。
    /// `format` 为 None 时返回全部表资产；为 Some 时按 subtype 过滤。
    /// `name` 为 Some 时按名称精确匹配。
    async fn list_assets_unified(
        &self,
        namespace_name: &str,
        format: Option<AssetFormat>,
        name: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError>;

    /// 获取 Asset 及其表资产明细（Unified API 使用，不绑定 format）
    async fn get_asset_unified(
        &self,
        namespace_name: &str,
        name: &str,
        format: AssetFormat,
    ) -> Result<(Asset, TabularAsset), StoreError>;
}
```

### 5.2 StoreError（不变）

```rust
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("internal error: {0}")]
    Internal(String),
}
```

### 5.3 PgCatalogStore 关键 SQL 片段

#### 行数据转换辅助函数

```rust
fn row_to_namespace(row: &Row) -> Result<Namespace, StoreError> {
    let props: serde_json::Value = try_get!(row, "properties");
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

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
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    let asset_type_str: String = try_get!(row, "asset_type");
    let asset_type = match asset_type_str.as_str() {
        "table" => AssetType::Table,
        _ => return Err(StoreError::Internal(format!("unknown asset_type: {}", asset_type_str))),
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
    let properties: HashMap<String, String> = serde_json::from_value(props)
        .map_err(|e| StoreError::Internal(format!("properties JSON: {}", e)))?;

    Ok(AssetVersion {
        id: try_get!(row, "id"),
        asset_id: try_get!(row, "asset_id"),
        version_key: try_get!(row, "version_key"),
        version_order: row.try_get("version_order").ok(),
        properties,
        created_at: try_get!(row, "created_at"),
    })
}
```

#### 创建表资产（事务内双表写入）

```rust
async fn create_asset(...) -> Result<Asset, StoreError> {
    let mut client = self.get_client().await?;
    let tx = client.transaction().await?;

    // 1. 写入 assets（通用注册表）
    let asset_row = tx.query_opt(
        "INSERT INTO assets (namespace_id, name, asset_type, asset_subtype, comment, properties)
         SELECT id, $2, 'table', $3, NULL, $4 FROM namespaces WHERE name = $1
         RETURNING *",
        &[&namespace_name, &name, &format.as_str(), &props_json],
    ).await.map_err(|e| match e.code() {
        Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) =>
            StoreError::AlreadyExists(...),
        _ => StoreError::Internal(e.to_string()),
    })?.ok_or_else(|| StoreError::NotFound(format!(
        "namespace '{}'", namespace_name
    )))?;

    let asset = row_to_asset(&asset_row)?;

    // 2. 写入 tabular_assets（表资产明细）
    tx.execute(
        "INSERT INTO tabular_assets (asset_id, location, metadata_location, schema_snapshot)
         VALUES ($1, $2, $3, $4)",
        &[&asset.id, &location, &metadata_location, &schema_snapshot],
    ).await.map_err(|e| {
        // tabular_assets 写入失败时回滚，assets 不留记录
        StoreError::Internal(e.to_string())
    })?;

    tx.commit().await?;
    Ok(asset)
}
```

#### 按 format 列出表资产（标准协议适配器）

```sql
SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype,
       a.comment, a.properties, a.created_at,
       t.location, t.metadata_location, t.schema_snapshot
FROM assets a
JOIN tabular_assets t ON a.id = t.asset_id
JOIN namespaces n ON a.namespace_id = n.id
WHERE n.name = $1
  AND a.asset_type = 'table'
  AND a.asset_subtype = $2
ORDER BY a.name ASC
```

#### 跨格式列出表资产（Unified API）

```sql
SELECT a.id, a.namespace_id, a.name, a.asset_type, a.asset_subtype,
       a.comment, a.properties, a.created_at,
       t.location, t.metadata_location, t.schema_snapshot
FROM assets a
JOIN tabular_assets t ON a.id = t.asset_id
JOIN namespaces n ON a.namespace_id = n.id
WHERE n.name = $1
  AND a.asset_type = 'table'
  AND ($2::text IS NULL OR a.asset_subtype = $2)
  AND ($3::text IS NULL OR a.name = $3)
ORDER BY a.name ASC, a.asset_subtype ASC
LIMIT $4 OFFSET $5
```

#### CAS UPDATE（同事务更新 tabular_assets 与 assets）

```rust
async fn cas_update_metadata_location(...) -> Result<(), StoreError> {
    let mut client = self.get_client().await?;
    let tx = client.transaction().await?;

    let updated = tx.execute(
        "UPDATE tabular_assets ta
         SET metadata_location = $1,
             schema_snapshot = COALESCE($2, schema_snapshot)
         FROM assets a
         JOIN namespaces n ON a.namespace_id = n.id
         WHERE ta.asset_id = a.id
           AND n.name = $3
           AND a.name = $4
           AND a.asset_type = 'table'
           AND a.asset_subtype = $5
           AND ta.metadata_location = $6",
        &[&new_location, &new_schema_snapshot, &namespace_name, &asset_name, &format.as_str(), &expected_location],
    ).await?;

    if updated == 0 {
        let exists = tx.query_opt(
            "SELECT 1
             FROM assets a
             JOIN tabular_assets ta ON a.id = ta.asset_id
             JOIN namespaces n ON a.namespace_id = n.id
             WHERE n.name = $1 AND a.name = $2
               AND a.asset_type = 'table' AND a.asset_subtype = $3",
            &[&namespace_name, &asset_name, &format.as_str()],
        ).await?.is_some();

        return if exists {
            Err(StoreError::Conflict("metadata_location changed".to_string()))
        } else {
            Err(StoreError::NotFound(format!("asset '{}'", asset_name)))
        };
    }

    apply_property_patch_in_tx(
        &tx,
        namespace_name,
        asset_name,
        format,
        property_removals,
        property_updates,
    ).await?;

    tx.commit().await?;
    Ok(())
}
```

#### Lance 创建版本（事务内双表写入）

```rust
// 1. 写入 asset_versions（通用版本注册表）
let version_row = tx.query_opt(
    "INSERT INTO asset_versions (asset_id, version_key, version_order, properties)
     SELECT a.id, $3::text, $3, '{}'
     FROM assets a
     JOIN namespaces n ON a.namespace_id = n.id
     WHERE n.name = $1 AND a.name = $2
       AND a.asset_type = 'table' AND a.asset_subtype = 'lance'
     RETURNING *",
    &[&namespace_name, &asset_name, &version_id],
).await?.ok_or_else(|| StoreError::NotFound(format!(
    "asset '{}'", asset_name
)))?;

let asset_version = row_to_asset_version(&version_row)?;

// previous_version_id 先解析为同一 Asset 下的 previous_asset_version_id。
// 若传入的 previous_version_id 不属于当前 Asset，返回 StoreError::Conflict。
let previous_asset_version_id = resolve_previous_asset_version_id_in_tx(
    &tx,
    asset_version.asset_id,
    previous_version_id,
).await?;

// 2. 写入 tabular_asset_versions（表版本明细）
tx.execute(
    "INSERT INTO tabular_asset_versions (asset_version_id, metadata_location, previous_asset_version_id)
     VALUES ($1, $2, $3)",
    &[&asset_version.id, &metadata_location, &previous_asset_version_id],
).await?;
```

#### 事务内辅助函数

```rust
/// 在事务内对 assets.properties 执行增量更新
async fn apply_property_patch_in_tx(
    tx: &Transaction<'_>,
    namespace_name: &str,
    asset_name: &str,
    format: AssetFormat,
    removals: &[String],
    updates: &HashMap<String, String>,
) -> Result<(), StoreError> {
    let props_json = serde_json::to_value(updates)
        .map_err(|e| StoreError::Internal(format!("properties serialization: {}", e)))?;

    let updated = tx.execute(
        "UPDATE assets
         SET properties = (properties - $1::text[]) || $2::jsonb
         FROM namespaces n
         WHERE assets.namespace_id = n.id
           AND n.name = $3
           AND assets.name = $4
           AND assets.asset_type = 'table'
           AND assets.asset_subtype = $5",
        &[&removals, &props_json, &namespace_name, &asset_name, &format.as_str()],
    ).await.map_err(|e| StoreError::Internal(e.to_string()))?;

    if updated == 0 {
        return Err(StoreError::NotFound(format!("asset '{}'", asset_name)));
    }

    Ok(())
}

/// 将 Lance version_id (i64) 解析为同一 Asset 下的 asset_versions.id (UUID)
async fn resolve_previous_asset_version_id_in_tx(
    tx: &Transaction<'_>,
    asset_id: Uuid,
    previous_version_id: Option<i64>,
) -> Result<Option<Uuid>, StoreError> {
    let Some(prev_id) = previous_version_id else {
        return Ok(None);
    };

    let row = tx.query_opt(
        "SELECT id FROM asset_versions
         WHERE asset_id = $1 AND version_order = $2",
        &[&asset_id, &prev_id],
    ).await.map_err(|e| StoreError::Internal(e.to_string()))?;

    match row {
        Some(r) => Ok(Some(r.get("id"))),
        None => Err(StoreError::Conflict(format!(
            "previous version {} not found for asset", prev_id
        ))),
    }
}
```

---

## 6. Unified API 端点设计

### 6.1 路径前缀

统一使用 `/unified/v1/...`：

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
| GET | `/unified/v1/namespaces/{ns}` | 获取 Namespace 详情 |
| DELETE | `/unified/v1/namespaces/{ns}` | 删除 Namespace（仅空 Namespace） |
| PATCH | `/unified/v1/namespaces/{ns}` | 更新 comment 与 properties（增量更新） |

#### Asset

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/unified/v1/namespaces/{ns}/assets` | 列出 Asset（支持 `format` 筛选、`name` 过滤、分页） |
| GET | `/unified/v1/namespaces/{ns}/assets/{name}` | 获取 Asset 详情（`format` 查询参数**必填**） |
| DELETE | `/unified/v1/namespaces/{ns}/assets/{name}` | 删除 Asset（`format`**必填**） |
| PATCH | `/unified/v1/namespaces/{ns}/assets/{name}` | 更新 comment 与 properties（`format`**必填**） |
| POST | `/unified/v1/namespaces/{ns}/assets/{name}/rename` | 重命名 Asset（`format`**必填**） |

**不暴露的端点**：`POST /unified/v1/namespaces/{ns}/assets` —— V2 不提供 Unified Asset 创建。

### 6.3 查询参数规范

**分页**：
- `pageToken`：服务端生成的不透明 token（V2 初版可用 offset 编码）
- `pageSize`：默认 100，最大 1000
- 客户端不得解析 `pageToken` 内容

**Asset 列表过滤**：
- `?format=iceberg|lance`：按表格式筛选
- `?name=users`：按名称精确匹配

**Asset 单资源操作**：
- `?format=iceberg|lance`：**必填**（因同名跨格式可共存）
- 非法值或空字符串 → 400 `InvalidFormat`
- 参数缺失 → 400 `InvalidInput`

**排序**：
- Namespace 列表：`name ASC`
- Asset 列表：`name ASC, asset_subtype ASC`

### 6.4 请求/响应 DTO

#### Namespace DTO

```rust
use quasar_core::PatchField;

// POST /unified/v1/namespaces
#[derive(Debug, Deserialize)]
struct CreateNamespaceRequest {
    name: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    properties: HashMap<String, String>,
}

// PATCH /unified/v1/namespaces/{ns}
// 三态语义：缺省=不变，null=清空，string=更新
#[derive(Debug, Deserialize)]
struct UpdateNamespaceRequest {
    #[serde(default)]
    comment: PatchField<String>,
    #[serde(default)]
    removals: Vec<String>,
    #[serde(default)]
    updates: HashMap<String, String>,
}

// 响应
#[derive(Debug, Serialize)]
struct NamespaceResponse {
    id: String,
    name: String,
    comment: Option<String>,
    properties: HashMap<String, String>,
    created_at: String,  // RFC 3339
}
```

#### Asset DTO

```rust
// PATCH /unified/v1/namespaces/{ns}/assets/{name}?format={format}
#[derive(Debug, Deserialize)]
struct UpdateAssetRequest {
    #[serde(default)]
    comment: PatchField<String>,
    #[serde(default)]
    removals: Vec<String>,
    #[serde(default)]
    updates: HashMap<String, String>,
}

// POST /unified/v1/namespaces/{ns}/assets/{name}/rename?format={format}
#[derive(Debug, Deserialize)]
struct RenameAssetRequest {
    new_name: String,
}

// current_version 响应（ tagged union ）
#[derive(Debug, Serialize)]
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
        timestamp: String,  // RFC 3339
    },
}

// Asset 列表项（不含 current_version）
#[derive(Debug, Serialize)]
struct AssetListItem {
    id: String,
    name: String,
    asset_type: String,  // V2 固定为 "table"
    format: String,      // "iceberg" | "lance"
    location: String,
    metadata_location: Option<String>,
    comment: Option<String>,
    properties: HashMap<String, String>,
    created_at: String,  // RFC 3339
}

// Asset 详情响应（含 current_version）
#[derive(Debug, Serialize)]
struct AssetResponse {
    id: String,
    name: String,
    asset_type: String,  // V2 固定为 "table"
    format: String,      // "iceberg" | "lance"
    location: String,
    metadata_location: Option<String>,
    comment: Option<String>,
    properties: HashMap<String, String>,
    current_version: Option<CurrentVersionResponse>,
    created_at: String,  // RFC 3339
}

// Asset 列表响应
#[derive(Debug, Serialize)]
struct ListAssetsResponse {
    assets: Vec<AssetListItem>,
    next_page_token: Option<String>,
}
```

`PatchField<String>` 的 JSON 映射规则：

| 请求片段 | Rust 值 | 语义 |
|---------|---------|------|
| `{}` | `PatchField::Missing` | 不修改 comment |
| `{"comment": null}` | `PatchField::Null` | 清空 comment |
| `{"comment": "updated"}` | `PatchField::Value("updated")` | 设置/覆盖 comment |

### 6.5 状态码

| 操作 | 成功状态码 |
|------|------------|
| `POST /unified/v1/namespaces` | 201 Created |
| `GET /unified/v1/namespaces` / `GET /unified/v1/namespaces/{ns}` | 200 OK |
| `PATCH /unified/v1/namespaces/{ns}` | 200 OK |
| `DELETE /unified/v1/namespaces/{ns}` | 204 No Content |
| `GET /unified/v1/namespaces/{ns}/assets` / `GET .../assets/{name}` | 200 OK |
| `PATCH /unified/v1/namespaces/{ns}/assets/{name}` | 200 OK |
| `POST /unified/v1/namespaces/{ns}/assets/{name}/rename` | 200 OK |
| `DELETE /unified/v1/namespaces/{ns}/assets/{name}` | 204 No Content |
| `POST /unified/v1/namespaces/{ns}/assets`（未实现） | 405 Method Not Allowed |

---

## 7. 错误处理

### 7.1 两层错误体系（不变）

Quasar 内部继续使用统一的 `StoreError`，各协议适配器映射为各自规范的错误格式。Unified API 新增独立的 Problem Details 映射层。

### 7.2 Unified API 错误格式（RFC 7807 Problem Details）

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

**字段说明**：
- `type`：问题类型的 URI 标识符
- `title`：问题的人类可读标题
- `status`：HTTP 状态码
- `detail`：问题的详细描述
- `instance`：发生问题的请求 URI
- `code`：Quasar 机器可读错误码（大写 CamelCase）
- `request_id`：请求追踪 ID

**状态码映射**：

| 场景 | HTTP 状态码 | code |
|------|------------|-------|
| Namespace 不存在 | 404 | `NamespaceNotFound` |
| Namespace 已存在 | 409 | `NamespaceAlreadyExists` |
| Namespace 非空 | 409 | `NamespaceNotEmpty` |
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

### 7.3 Unified API 错误类型定义

```rust
// adapter/src/unified/error.rs

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use quasar_core::StoreError;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
    pub code: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnifiedErrorCode {
    NamespaceNotFound,
    NamespaceAlreadyExists,
    NamespaceNotEmpty,
    AssetNotFound,
    AssetAlreadyExists,
    InvalidInput,
    InvalidFormat,
    InvalidPageToken,
    PageSizeTooLarge,
    MethodNotAllowed,
    Conflict,
    ServiceUnavailable,
    InternalError,
}

impl UnifiedErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound => "NamespaceNotFound",
            Self::NamespaceAlreadyExists => "NamespaceAlreadyExists",
            Self::NamespaceNotEmpty => "NamespaceNotEmpty",
            Self::AssetNotFound => "AssetNotFound",
            Self::AssetAlreadyExists => "AssetAlreadyExists",
            Self::InvalidInput => "InvalidInput",
            Self::InvalidFormat => "InvalidFormat",
            Self::InvalidPageToken => "InvalidPageToken",
            Self::PageSizeTooLarge => "PageSizeTooLarge",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::Conflict => "Conflict",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::InternalError => "InternalError",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NamespaceNotFound | Self::AssetNotFound => StatusCode::NOT_FOUND,
            Self::NamespaceAlreadyExists | Self::AssetAlreadyExists | Self::NamespaceNotEmpty | Self::Conflict => StatusCode::CONFLICT,
            Self::InvalidInput | Self::InvalidFormat | Self::InvalidPageToken | Self::PageSizeTooLarge => StatusCode::BAD_REQUEST,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn problem_slug(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound => "namespace-not-found",
            Self::NamespaceAlreadyExists => "namespace-already-exists",
            Self::NamespaceNotEmpty => "namespace-not-empty",
            Self::AssetNotFound => "asset-not-found",
            Self::AssetAlreadyExists => "asset-already-exists",
            Self::InvalidInput => "invalid-input",
            Self::InvalidFormat => "invalid-format",
            Self::InvalidPageToken => "invalid-page-token",
            Self::PageSizeTooLarge => "page-size-too-large",
            Self::MethodNotAllowed => "method-not-allowed",
            Self::Conflict => "conflict",
            Self::ServiceUnavailable => "service-unavailable",
            Self::InternalError => "internal-error",
        }
    }
}

/// Unified API 专用的错误类型
pub struct UnifiedError {
    pub code: UnifiedErrorCode,
    pub detail: String,
    pub instance: String,
    pub request_id: String,
}

impl UnifiedError {
    pub fn new(
        code: UnifiedErrorCode,
        detail: impl Into<String>,
        instance: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            code,
            detail: detail.into(),
            instance: instance.into(),
            request_id: request_id.into(),
        }
    }
}

impl IntoResponse for UnifiedError {
    fn into_response(self) -> Response {
        let status = self.code.status_code();
        let problem = ProblemDetails {
            problem_type: format!("https://quasar.dev/problems/{}", self.code.problem_slug()),
            title: self.code.as_str().to_string(),
            status: status.as_u16(),
            detail: self.detail,
            instance: self.instance,
            code: self.code.as_str().to_string(),
            request_id: self.request_id,
        };

        let body = serde_json::to_string(&problem).unwrap_or_else(|_| {
            r#"{"type":"https://quasar.dev/problems/internal-error","title":"InternalError","status":500,"detail":"failed to serialize error response","instance":"","code":"InternalError","request_id":""}"#.to_string()
        });

        (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            body,
        ).into_response()
    }
}

// StoreError → UnifiedError 必须在 handler 上下文中映射，不使用 From<StoreError>。
// Namespace handler 使用 map_namespace_error，Asset/Version handler 使用 map_asset_error。
pub fn map_namespace_error(
    err: StoreError,
    instance: &str,
    request_id: &str,
) -> UnifiedError {
    match err {
        StoreError::NotFound(msg) => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotFound,
            msg,
            instance,
            request_id,
        ),
        StoreError::AlreadyExists(msg) => UnifiedError::new(
            UnifiedErrorCode::NamespaceAlreadyExists,
            msg,
            instance,
            request_id,
        ),
        StoreError::Conflict(msg) if msg.contains("not empty") => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotEmpty,
            msg,
            instance,
            request_id,
        ),
        StoreError::Conflict(msg) => UnifiedError::new(
            UnifiedErrorCode::Conflict,
            msg,
            instance,
            request_id,
        ),
        StoreError::InvalidInput(msg) => UnifiedError::new(
            UnifiedErrorCode::InvalidInput,
            msg,
            instance,
            request_id,
        ),
        StoreError::Internal(msg) => UnifiedError::new(
            UnifiedErrorCode::InternalError,
            msg,
            instance,
            request_id,
        ),
    }
}

pub fn map_asset_error(
    err: StoreError,
    instance: &str,
    request_id: &str,
) -> UnifiedError {
    match err {
        StoreError::NotFound(msg) => UnifiedError::new(
            UnifiedErrorCode::AssetNotFound,
            msg,
            instance,
            request_id,
        ),
        StoreError::AlreadyExists(msg) => UnifiedError::new(
            UnifiedErrorCode::AssetAlreadyExists,
            msg,
            instance,
            request_id,
        ),
        StoreError::Conflict(msg) => UnifiedError::new(
            UnifiedErrorCode::Conflict,
            msg,
            instance,
            request_id,
        ),
        StoreError::InvalidInput(msg) => UnifiedError::new(
            UnifiedErrorCode::InvalidInput,
            msg,
            instance,
            request_id,
        ),
        StoreError::Internal(msg) => UnifiedError::new(
            UnifiedErrorCode::InternalError,
            msg,
            instance,
            request_id,
        ),
    }
}
```

`request_id` 获取规则：

- 请求进入 server 时，middleware 优先读取客户端传入的 `X-Request-Id`。
- 若客户端未传入，则生成新的 UUID 字符串。
- middleware 将 request id 写入 request extensions，并在响应 header 中返回同一个 `X-Request-Id`。
- Unified handler 映射错误时从 request extensions 读取 request id，写入 Problem Details 的 `request_id` 字段。
- 日志 span 同步记录 request id，用于把客户端错误响应与服务端日志关联。

### 7.4 隔离保证

Unified API 的 Problem Details 错误格式**不得**泄漏到 `/iceberg/v1/...` 和 `/lance/v1/...` 端点。标准协议继续使用各自的错误格式：

- Iceberg：`{"error": {"message": "...", "type": "NoSuchTableException", "code": 404}}`
- Lance：`{"error": "TableNotFound", "code": 404, "detail": "...", "instance": "..."}`

---

## 8. 标准协议适配器修改

### 8.1 修改范围

V2-F6 要求所有标准协议适配器按 `asset_type='table'` 与 `asset_subtype=format` 隔离查询和写入。

#### Iceberg 适配器修改清单

| 方法 | 修改内容 |
|------|----------|
| `create_table` | 调用 `CatalogStore::create_asset`（已自动写入 assets + tabular_assets） |
| `list_tables` | `list_assets` SQL 增加 `AND a.asset_type = 'table' AND a.asset_subtype = 'iceberg'` |
| `load_table` | `get_asset_with_tabular` SQL 增加 format 条件 |
| `drop_table` | `drop_asset` SQL 增加 format 条件 |
| `rename_table` | `rename_asset` SQL 增加 format 条件 |
| `commit_table` | `cas_update_metadata_location` SQL 通过 assets 限定 format |
| `table_exists` | `asset_exists` SQL 增加 format 条件 |

#### Lance 适配器修改清单

| 方法 | 修改内容 |
|------|----------|
| `declare_table` / `register_table` | 调用 `CatalogStore::create_asset`（自动写入 assets + tabular_assets） |
| `list_tables` | `list_assets` SQL 增加 `AND a.asset_type = 'table' AND a.asset_subtype = 'lance'` |
| `describe_table` | `get_asset_with_current_version` SQL 增加 format 条件 |
| `drop_table` / `deregister_table` | `drop_asset` SQL 增加 format 条件 |
| `rename_table` | `rename_asset` SQL 增加 format 条件 |
| `create_version` | 先写 `asset_versions`，再写 `tabular_asset_versions` |
| `list_versions` | `list_versions` 联查 `tabular_asset_versions` |
| `describe_version` | `load_version` 联查 `tabular_asset_versions` |

### 8.2 Iceberg create_table 修改

当前实现调用 `CatalogStore::create_asset`，该方法已在 V2 中改为事务内同时写入 `assets` + `tabular_assets`。Iceberg 适配器无需额外修改，只需确保传入的 `format = AssetFormat::Iceberg`。

### 8.3 Lance create_version 修改

```rust
// adapter/src/lance/version.rs

pub async fn create_version_handler(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(table_id): Path<String>,
    Json(req): Json<CreateVersionRequest>,
) -> Result<impl IntoResponse, LanceError> {
    let (namespace, table) = parse_table_id(&table_id)?;

    // V2: create_version 内部先写 asset_versions，再写 tabular_asset_versions
    let version = store
        .create_version(
            &namespace,
            AssetFormat::Lance,
            &table,
            req.version,
            req.manifest_path,
            req.previous_version.map(|v| v as i64),
        )
        .await
        .map_err(LanceError::from)?;

    Ok(Json(version_response(&version)))
}
```

### 8.4 同名跨格式共存验证

V2 的 `assets` 唯一约束为 `UNIQUE(namespace_id, name, asset_type, asset_subtype)`。同一 Namespace 下：

- `prod.users`（iceberg）和 `prod.users`（lance）→ **允许共存**
- `prod.users`（iceberg）和 `prod.users`（iceberg）→ **拒绝，返回 AlreadyExists**

---

## 9. 版本查看实现

### 9.1 Iceberg 当前版本

从 `tabular_assets.metadata_location` 指针定位当前 metadata.json，读取并解析：

```rust
// adapter/src/unified/version.rs

use object_store::{ObjectStore, path::Path};
use serde_json::Value;

pub async fn get_iceberg_current_version(
    object_store: &dyn ObjectStore,
    metadata_location: &str,
) -> Result<Option<IcebergCurrentVersion>, object_store::Error> {
    // 解析 metadata_location 为 object_store 路径
    let path = parse_metadata_path(metadata_location)?;

    let bytes = match object_store.get(&path).await {
        Ok(result) => result.bytes().await?,
        Err(e) => {
            // S3 不可用或文件不存在 → 返回 null，不报错
            tracing::warn!("failed to read metadata.json at {}: {}", metadata_location, e);
            return Ok(None);
        }
    };

    let metadata: Value = serde_json::from_slice(&bytes)
        .map_err(|e| object_store::Error::Generic {
            store: "json_parse",
            source: Box::new(e),
        })?;

    let sequence_number = metadata
        .get("last-sequence-number")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let current_snapshot_id = metadata
        .get("current-snapshot-id")
        .and_then(|v| v.as_i64());

    let timestamp_ms = if let Some(snapshot_id) = current_snapshot_id {
        // 从 snapshots 数组中找到对应 snapshot 的 timestamp-ms
        metadata
            .get("snapshots")
            .and_then(|s| s.as_array())
            .and_then(|snapshots| {
                snapshots.iter().find(|s| {
                    s.get("snapshot-id")
                        .and_then(|id| id.as_i64())
                        == Some(snapshot_id)
                })
            })
            .and_then(|s| s.get("timestamp-ms").and_then(|t| t.as_i64()))
    } else {
        None
    };

    Ok(Some(IcebergCurrentVersion {
        sequence_number,
        snapshot_id: current_snapshot_id,
        timestamp_ms,
    }))
}
```

**关键决策**：S3 不可用或 metadata.json 不存在时返回 `current_version = null`，不导致整个 Asset 详情请求失败。

### 9.2 Lance 当前版本

```rust
// 在 PgCatalogStore 中

async fn load_current_version(
    &self,
    namespace_name: &str,
    format: AssetFormat,
    asset_name: &str,
) -> Result<Option<AssetVersionWithTabular>, StoreError> {
    let client = self.get_client().await?;

    let row = client.query_opt(
        "SELECT av.id, av.asset_id, av.version_key, av.version_order,
                av.properties, av.created_at,
                tav.metadata_location, tav.previous_asset_version_id
         FROM asset_versions av
         JOIN tabular_asset_versions tav ON av.id = tav.asset_version_id
         JOIN assets a ON av.asset_id = a.id
         JOIN namespaces n ON a.namespace_id = n.id
         WHERE n.name = $1 AND a.name = $2
           AND a.asset_type = 'table' AND a.asset_subtype = $3
         ORDER BY av.version_order DESC NULLS LAST
         LIMIT 1",
        &[&namespace_name, &asset_name, &format.as_str()],
    ).await.map_err(|e| StoreError::Internal(e.to_string()))?;

    match row {
        Some(r) => {
            let version = row_to_asset_version(&r)?;
            let tabular_version = TabularAssetVersion {
                asset_version_id: version.id,
                metadata_location: try_get!(r, "metadata_location"),
                previous_asset_version_id: r.try_get("previous_asset_version_id").ok(),
            };
            Ok(Some(AssetVersionWithTabular { version, tabular_version }))
        }
        // Asset 已存在但无版本，返回 None，由 Unified API 序列化为 current_version = null。
        None => Ok(None),
    }
}
```

**Lance `previous_version_id` 映射**：

`tabular_asset_versions.previous_asset_version_id` 是 UUID，指向 `asset_versions.id`。Unified API 响应需要 `previous_version_id`（即 Lance 的 version_id / `version_order`）：

```rust
// 从 previous_asset_version_id 关联回 version_order
async fn get_previous_version_order(
    &self,
    previous_asset_version_id: Uuid,
) -> Result<Option<i64>, StoreError> {
    let client = self.get_client().await?;
    let row = client
        .query_opt(
            "SELECT version_order FROM asset_versions WHERE id = $1",
            &[&previous_asset_version_id],
        )
        .await
        .map_err(|e| StoreError::Internal(e.to_string()))?;

    Ok(row.map(|r| r.get::<_, i64>("version_order")))
}
```

### 9.3 无版本状态

- **Iceberg 新建表**：metadata.json 存在但无 snapshot → `sequence_number` = `last-sequence-number`，`snapshot_id` = null，`timestamp_ms` = null
- **Lance declare/register 后**：无 `asset_versions` 记录 → `current_version` = null

---

## 10. 条件编译

### 10.1 Feature 定义

| Feature | 默认 | 说明 | 依赖关系 |
|---------|------|------|----------|
| `lance` | ✅ | Lance REST Namespace 适配器 | 无 |
| `iceberg` | ✅ | Iceberg REST Catalog 适配器 | `object_store` |
| `unified` | ✅ | Unified REST API | 自动依赖 `lance` + `iceberg` |

### 10.2 编译命令

```bash
# 全部协议（默认）
cargo build --manifest-path quasar/Cargo.toml

# 仅编译标准协议（不含 Unified API）
cargo build --no-default-features --features "lance,iceberg" --manifest-path quasar/Cargo.toml

# 仅 Lance
cargo build --no-default-features --features lance --manifest-path quasar/Cargo.toml

# 仅 Iceberg
cargo build --no-default-features --features iceberg --manifest-path quasar/Cargo.toml

# 仅基础设施端点
cargo build --no-default-features --manifest-path quasar/Cargo.toml
```

### 10.3 实现机制

1. **Crate 依赖层**：`adapter/Cargo.toml` 中 `unified = ["lance", "iceberg"]`
2. **模块层**：`adapter/src/lib.rs` 中 `#[cfg(feature = "unified")] pub mod unified;`
3. **路由层**：`server/src/lib.rs` 中条件合并 `unified` 路由
4. **测试层**：Unified API 测试文件顶部添加 `#![cfg(feature = "unified")]`

---

## 11. 开发阶段划分

V2 建立在 V1（MVP）已完成的基础上，按以下阶段推进。每个阶段完成后必须能编译通过且有明确的验收标准。

### 总览

| 阶段 | 名称 | 前置依赖 | 核心目标 |
|------|------|---------|---------|
| V2-S1 | DDL + Core 模型 | V1 完成 | V2 目标 DDL、Core 模型、PatchField |
| V2-S2a | Storage Namespace | V2-S1 | Namespace CRUD + comment 支持 |
| V2-S2b | Storage Asset | V2-S2a | Asset 双表写入 + format 隔离查询 |
| V2-S2c | Storage Version + CAS | V2-S2b | Version 双表写入 + CAS 事务 |
| V2-S3 | 标准协议适配器隔离 | V2-S2c | Iceberg/Lance 适配器 format 过滤 |
| V2-S4 | Unified API 骨架 + Namespace | V2-S2a | Unified 路由、Problem Details、Namespace |
| V2-S5 | Unified Asset | V2-S4, V2-S3 | Asset 发现 + 管理 |
| V2-S6 | 版本查看 | V2-S5 | Iceberg/Lance `current_version` |
| V2-S7 | Feature 与集成 | V2-S6 | 条件编译、无状态、全量回归 |

### V2-S1: DDL + Core 模型更新

**工作内容**：
- 创建 V2 目标 DDL 初始化脚本 `storage/src/migrations/V1__v2_initial_schema.sql`；V2 从空数据库启动，不保留 V1 schema 处理路径
- 更新 `core/src/models.rs`：
  - 新增 `AssetType` 枚举
  - 新增 `PatchField<T>`，用于 PATCH 字段的 Missing / Null / Value 三态表达
  - 重构 `Asset`（移除 location/metadata_location/schema_snapshot，增加 asset_type/asset_subtype/comment）
  - 新增 `TabularAsset`、`AssetWithTabular`
  - 重构 `AssetVersion`（version_id → version_key/version_order，移除 metadata_location/previous_version_id）
  - 新增 `TabularAssetVersion`、`AssetVersionWithTabular`
  - 更新 `Namespace`（增加 comment）
- 更新 `core/src/lib.rs` 导出
- V2 初始 DDL 最前面必须包含 `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`

**验收标准**：
```bash
# 1. 编译通过
cargo build --all-features --manifest-path quasar/Cargo.toml

# 2. 从空数据库启动服务，迁移成功执行
# 启动服务连接到全新 PostgreSQL 实例
cargo run --all-features --manifest-path quasar/Cargo.toml
# 日志应显示 "applied migration: V1__v2_initial_schema"

# 3. 手动验证表结构
psql $QUASAR_DATABASE_URL -c "\dt"  # 应显示 5 张表
psql $QUASAR_DATABASE_URL -c "\d assets"  # 应显示 asset_type、asset_subtype、comment
psql $QUASAR_DATABASE_URL -c "\d tabular_assets"  # 应显示 location、metadata_location
psql $QUASAR_DATABASE_URL -c "\d asset_versions"  # 应显示 version_key、version_order
psql $QUASAR_DATABASE_URL -c "\d tabular_asset_versions"  # 应显示 previous_asset_version_id

# 4. 验证 CHECK 约束
psql $QUASAR_DATABASE_URL -c "
  INSERT INTO assets(id,namespace_id,name,asset_type,asset_subtype,properties)
  VALUES(gen_random_uuid(),gen_random_uuid(),'test','invalid','iceberg','{}')
"
# 应报错：CHECK 约束 violation
```

### V2-S2a: Storage Namespace

**前置依赖**：V2-S1

**工作内容**：
- `create_namespace`：支持写入 nullable comment
- `list_namespaces`：分页支持
- `get_namespace`：返回 comment
- `namespace_exists`
- `drop_namespace`
- `update_namespace`：comment 三态 PATCH（Missing/Null/Value）+ properties 增量更新

**验收标准**：
```bash
cargo test -p quasar-storage --all-features --manifest-path quasar/Cargo.toml namespace

# 正向
create_namespace("prod", Some("production"), {("team", "data")}) → Ok
get_namespace("prod") → comment = Some("production"), properties["team"] = "data"

# 负向：重复创建
create_namespace("prod", None, {}) → AlreadyExists

# 负向：删除非空 Namespace
drop_namespace("prod") 前通过 storage 直接创建 asset → Conflict

# 负向：PATCH 三态
update_namespace("prod", Value("updated"), [], {}) → comment = "updated"
update_namespace("prod", Null, [], {}) → comment = None
update_namespace("prod", Missing, ["team"], {}) → properties 无 "team"，comment 不变
```

### V2-S2b: Storage Asset

**前置依赖**：V2-S2a

**工作内容**：
- `create_asset`：事务内同时写 `assets` + `tabular_assets`
- `list_assets`：返回 `Vec<AssetWithTabular>`，注入 `asset_type`/`asset_subtype` 过滤
- `get_asset`：注入 format 过滤
- `get_asset_with_tabular`：新增，联查 `tabular_assets`
- `asset_exists`
- `drop_asset`
- `rename_asset`
- `update_asset_properties`：新增
- `get_asset_unified` / `list_assets_unified`：新增，跨 format 查询

**验收标准**：
```bash
cargo test -p quasar-storage --all-features --manifest-path quasar/Cargo.toml asset

# 正向：双表写入
create_asset("prod", Iceberg, "users", "s3://bucket/users", ...) → Ok
get_asset_with_tabular("prod", Iceberg, "users")
  → asset.name = "users", tabular.location = "s3://bucket/users"

# 正向：同名跨格式共存
create_asset("prod", Iceberg, "users", ...) → Ok
create_asset("prod", Lance, "users", ...) → Ok
list_assets("prod", Iceberg) → 1 条
list_assets("prod", Lance) → 1 条
list_assets_unified("prod", None, None, 0, 100) → 2 条

# 负向：重复创建
create_asset("prod", Iceberg, "users", ...) → AlreadyExists

# 负向：事务回滚
# 模拟 tabular_assets 写入失败场景，确认 assets 记录未残留
```

### V2-S2c: Storage Version + CAS

**前置依赖**：V2-S2b

**工作内容**：
- `create_version`：事务内先写 `asset_versions`，取返回 UUID 后再写 `tabular_asset_versions`
- `create_version`：校验 `previous_version_id` 必须属于同一 Asset，否则返回 Conflict
- `load_version` / `load_current_version` / `list_versions`：返回 `AssetVersionWithTabular`
- `load_current_version`：返回 `Option<AssetVersionWithTabular>`，无版本时返回 `None`
- `cas_update_metadata_location`：同事务更新 `tabular_assets.metadata_location/schema_snapshot` 与 `assets.properties`，按 affected rows 区分 NotFound 与 Conflict

**验收标准**：
```bash
cargo test -p quasar-storage --all-features --manifest-path quasar/Cargo.toml version
cargo test -p quasar-storage --all-features --manifest-path quasar/Cargo.toml cas

# 正向：version 双表写入
create_version("prod", Lance, "users", 1, "s3://manifest/1", None) → Ok
load_current_version("prod", Lance, "users") → Some(...), version_key = "1"

# 正向：CAS 成功
cas_update_metadata_location("prod", "users", Iceberg, "old", "new", ...) → Ok

# 负向：version 跨 Asset 引用
create_version("prod", Lance, "users", 2, "s3://manifest/2", Some(999))
  → Conflict（999 不属于 users Asset）

# 负向：CAS 冲突
# 并发场景：两个请求基于同一个 expected_location 调用 CAS
# 应只有一个成功，另一个返回 Conflict

# 负向：version 事务回滚
# asset_versions 写入成功但 tabular_asset_versions 失败时，两表均不残留
```

### V2-S3: 标准协议适配器格式隔离

**前置依赖**：V2-S2c

**工作内容**：
- Iceberg 适配器：
  - `create_table`：确认通过 `CatalogStore::create_asset` 正确写入（已自动双表）
  - `list_tables`：消费 `Vec<AssetWithTabular>`，验证只返回 iceberg 资产
  - `load_table`：消费 `get_asset_with_tabular`
  - `commit_table`：验证 `cas_update_metadata_location` 正确限定 format
  - `drop_table` / `rename_table` / `table_exists`：注入 format 过滤
- Lance 适配器：
  - `declare_table`/`register_table`：确认通过 `CatalogStore::create_asset` 正确写入
  - `create_version`：适配新的双表写入流程
  - `list_versions`/`describe_version`：消费 `AssetVersionWithTabular`
  - `load_current_version`：消费 `Option<AssetVersionWithTabular>`
  - `drop_table` / `rename_table`：注入 format 过滤

**验收标准**：
```bash
cargo test -p quasar-adapter --all-features --manifest-path quasar/Cargo.toml

# 正向：Iceberg 端到端
# create_namespace → create_table → commit_table → list_tables → load_table → drop_table

# 正向：Lance 端到端
# create_namespace → declare_table → create_version → list_versions → describe_table → drop_table

# 负向：format 隔离
# 通过 Lance 创建 prod.users → Iceberg list_tables 不应返回该表
# 通过 Iceberg 创建 prod.users → Lance list_tables 不应返回该表
```

### V2-S4: Unified API 骨架 + Namespace

**前置依赖**：V2-S2a（只需要 Namespace 方法，不需要 Asset/Version）

**工作内容**：
- 创建 `adapter/src/unified/` 目录：
  - `mod.rs`：路由注册
  - `namespace.rs`：Namespace handler（list/create/get/delete/patch）
  - `error.rs`：Problem Details 错误类型、上下文错误映射函数
  - `dto.rs`：请求/响应 DTO
  - `request_id.rs`：`X-Request-Id` 生成/透传 middleware
- `UnifiedConfig`：空结构体（预留配置扩展）
- 集成测试

**验收标准**：
```bash
# 1. 编译通过
cargo build --all-features --manifest-path quasar/Cargo.toml

# 2. 正向验证
curl http://localhost:8080/unified/v1/namespaces
curl -X POST http://localhost:8080/unified/v1/namespaces -d '{"name":"prod","comment":"production"}'
curl http://localhost:8080/unified/v1/namespaces/prod
curl -X PATCH http://localhost:8080/unified/v1/namespaces/prod -d '{"comment":"updated"}'
curl -X DELETE http://localhost:8080/unified/v1/namespaces/prod

# 3. 负向验证：Problem Details 格式
curl -X POST http://localhost:8080/unified/v1/namespaces -d '{"name":"prod"}'
# 第二次 → 409
# 验证响应 Content-Type = application/problem+json
# 验证响应体包含 type, title, status, detail, instance, code, request_id

# 4. 负向验证：删除非空 Namespace
curl -X POST http://localhost:8080/unified/v1/namespaces -d '{"name":"prod2"}'
# 通过 storage 直接插入一个 asset 到 prod2
curl -X DELETE http://localhost:8080/unified/v1/namespaces/prod2  # 409 NamespaceNotEmpty
```

### V2-S5: Unified Asset

**前置依赖**：V2-S4, V2-S3（**需要 S3 才能用原生 API 创建测试资产**）

**工作内容**：
- `adapter/src/unified/asset.rs`：
  - Asset list handler：支持 `format`、`name`、`pageToken`、`pageSize` 查询参数，分页（offset 编码 token）
  - Asset get/delete/patch/rename handler
  - `format` 查询参数校验（必填、合法值）
  - PATCH 支持 comment 三态语义 + properties 增量更新
- 联查 `assets` + `tabular_assets`
- 集成测试

**验收标准**：
```bash
# 前置：通过 Iceberg/Lance 原生 API 创建测试资产
curl -X POST http://localhost:8080/iceberg/v1/namespaces/prod/tables \
  -d '{"name":"users","schema":{...}}'
curl -X POST http://localhost:8080/lance/v1/table/prod$users/declare
curl -X POST http://localhost:8080/lance/v1/table/prod$users/register \
  -d '{"location":"s3://bucket/users.lance"}'

# 正向：列表
curl http://localhost:8080/unified/v1/namespaces/prod/assets
# → 2 条（iceberg + lance）

curl "http://localhost:8080/unified/v1/namespaces/prod/assets?format=iceberg"
# → 1 条（仅 iceberg）

# 正向：单资源操作
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/users?format=iceberg"
# → 200，含 location、metadata_location

curl -X PATCH "http://localhost:8080/unified/v1/namespaces/prod/assets/users?format=iceberg" \
  -d '{"comment":"updated","updates":{"owner":"team-a"}}'
# → 200，只更新 Catalog 级 comment/properties

curl -X POST "http://localhost:8080/unified/v1/namespaces/prod/assets/users/rename?format=iceberg" \
  -d '{"new_name":"users_v2"}'
# → 200，重命名

curl -X DELETE "http://localhost:8080/unified/v1/namespaces/prod/assets/users_v2?format=iceberg"
# → 204，删除

# 负向：format 参数
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/users"
# → 400 InvalidInput（format 缺失）

curl "http://localhost:8080/unified/v1/namespaces/prod/assets/users?format=parquet"
# → 400 InvalidFormat

# 负向：分页
curl "http://localhost:8080/unified/v1/namespaces/prod/assets?pageSize=5000"
# → 400 PageSizeTooLarge
```

### V2-S6: 版本查看

**前置依赖**：V2-S5

**工作内容**：
- Iceberg `current_version`：
  - 从 `metadata_location` 读取 metadata.json
  - 解析 `last-sequence-number`、`current-snapshot-id`
  - S3 不可用时返回 null
- Lance `current_version`：
  - 查询 `asset_versions.version_order DESC`
  - 联查 `tabular_asset_versions`
  - 无版本时返回 null
- `CurrentVersionResponse` tagged union 序列化

**验收标准**：
```bash
# Iceberg：commit 后查看
curl -X POST .../prod/tables/users/commit  # 产生 metadata.json
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/users?format=iceberg"
# → current_version.sequence_number >= 1

# Iceberg：S3 不可用容错
# 临时断开 S3/MinIO 连接
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/users?format=iceberg"
# → 200 OK，current_version = null（不是 503）

# Lance：declare 后无版本
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/items?format=lance"
# → current_version = null

# Lance：create_version 后
curl -X POST http://localhost:8080/lance/v1/table/prod$items/version/create \
  -d '{"version":1,"manifest_path":"s3://bucket/1.manifest"}'
curl "http://localhost:8080/unified/v1/namespaces/prod/assets/items?format=lance"
# → current_version.version_id = 1
```

### V2-S7: Feature 与集成

**前置依赖**：V2-S6

**工作内容**：
- 验证 `unified` feature 可关闭（`--no-default-features --features "lance,iceberg"`）
- 关闭 unified 后标准协议回归测试通过
- 启动两个 server 实例连接同一 PostgreSQL，执行读写 smoke test

**验收标准**：
```bash
# 1. 关闭 unified feature 编译通过
cargo build --no-default-features --features "lance,iceberg" --manifest-path quasar/Cargo.toml

# 2. 标准协议回归（不含 unified）
cargo test -p quasar-adapter --features "lance,iceberg" --manifest-path quasar/Cargo.toml

# 3. 全量测试（含 unified）
cargo test --all-features --manifest-path quasar/Cargo.toml

# 4. 格式检查
cargo fmt --all --manifest-path quasar/Cargo.toml -- --check

# 5. 双实例无状态验证
# terminal 1: cargo run --all-features --manifest-path quasar/Cargo.toml
# terminal 2: cargo run --all-features --manifest-path quasar/Cargo.toml -- --port 8081
# T1 写入 Namespace，T2 读取 → 数据一致
```

---

## 12. 关键决策清单

| # | 决策 | 选择 | 理由 |
|---|------|------|------|
| 1 | 数据模型 | `assets` 通用注册表 + `tabular_assets` 明细表 | 为后续非表资产预留扩展空间，避免表字段污染通用注册表 |
| 2 | 版本模型 | `asset_versions` 通用注册表 + `tabular_asset_versions` 明细表 | 版本身份/排序与类型版本字段分离，支持未来 model/fileset 版本 |
| 3 | API 前缀 | `/unified/v1/...` | `/unified` 表达统一入口语义，`/v1` 表达 API 合同版本 |
| 4 | format 标识 | `asset_type` + `asset_subtype` 而非 `format` | 区分资产类型（table/model/fileset）和格式子类（iceberg/lance） |
| 5 | 同名跨格式 | 允许共存 | `(namespace_id, name, asset_type, asset_subtype)` 唯一约束 |
| 6 | Unified 创建 | 不提供 `POST /assets` | 不同格式建表参数差异大，继续由原生 API 承载 |
| 7 | Unified 删除/重命名 | 只改 Catalog 记录 | Unified API 是管理面，不做数据面清理 |
| 8 | PATCH 语义 | 增量更新（removals + updates）+ comment 三态 | 与 Iceberg namespace properties patch 思路对齐 |
| 9 | Iceberg 版本 | 读取 metadata.json 推断 | 版本信息自包含在 metadata.json 中 |
| 10 | Iceberg S3 容错 | 返回 `current_version = null` | 管理后台核心是"看到 Asset 存在"，版本是次要信息 |
| 11 | Lance 版本初始化 | declare/register 不创建初始版本 | Catalog 不扫描数据文件，版本由客户端显式管理 |
| 12 | 错误格式 | RFC 7807 Problem Details + `code` + `request_id` | 通用 HTTP API 错误模型 + 机器可读错误码 |
| 13 | Schema 基线 | V2 以目标 DDL 初始化，允许重建数据库 | V2 是全新版本基线，不承担历史版本复杂度 |
| 14 | Feature flag | `unified` 自动依赖 `iceberg` + `lance` | Unified API 需要读取和管理两种格式 |
| 15 | Comment 字段 | 仅 `namespaces` + `assets` 引入，`asset_versions` 不加 | 资源说明 vs 版本提交说明语义不同 |
| 16 | 版本排序 | `version_key`（原生标识）+ `version_order`（可比较排序） | Lance version_id 同时作为 key 和 order；Iceberg 可能只存 key |
| 17 | 事务一致性 | 双表写入在同事务内完成 | assets+tabular_assets、asset_versions+tabular_asset_versions 原子写入 |
| 18 | PATCH 字段表达 | 自定义 `PatchField<T>` | 明确区分字段缺省、显式 null、具体值，避免 `Option<Option<T>>` 语义不清 |
| 19 | request_id | 复用/生成 `X-Request-Id`，写入响应 header、错误体和日志 | 便于把客户端错误响应与服务端日志关联 |
| 20 | UUID 生成 | PostgreSQL `pgcrypto` + `gen_random_uuid()` | 与 MVP 迁移和现有 storage insert 方式保持一致，避免 Rust 侧重复传 id |

---

## 13. 验收标准

### 13.1 Schema 与约束

- [ ] 从空数据库按 V2 目标 DDL 初始化成功，服务可正常启动
- [ ] 初始 DDL 包含 `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`，`DEFAULT gen_random_uuid()` 可用
- [ ] `namespaces` 表包含 `comment` 字段
- [ ] `assets` 表作为通用注册表，只包含身份、类型、comment、properties 字段
- [ ] `tabular_assets` 表一对一指向 `assets(id)`，包含 `location`、`metadata_location`、`schema_snapshot`
- [ ] `asset_versions` 表作为通用版本注册表，包含 `version_key`、`version_order`、`properties`
- [ ] `tabular_asset_versions` 表一对一指向 `asset_versions(id)`，包含 `metadata_location`、`previous_asset_version_id`
- [ ] 非法 `asset_type`/`asset_subtype` 被拒绝
- [ ] 同一 Namespace 下同名同类型同子类资产重复创建被拒绝
- [ ] 同名 Iceberg / Lance 表资产允许共存
- [ ] 同一 Asset 下重复 `version_key` 被拒绝，重复非空 `version_order` 被拒绝
- [ ] `previous_asset_version_id` 指向其他 Asset 的版本时由存储层拒绝
- [ ] 删除 Asset 时 `tabular_assets`、`asset_versions`、`tabular_asset_versions` 级联清理

### 13.2 Unified API 正向路径

- [ ] `POST /unified/v1/namespaces` 可创建 Namespace 并写入 comment/properties
- [ ] `GET /unified/v1/namespaces` 返回列表，含 comment
- [ ] `GET /unified/v1/namespaces/{ns}` 返回详情
- [ ] `PATCH /unified/v1/namespaces/{ns}` 支持 comment 字符串覆盖、显式 null 清空、缺省不变
- [ ] `DELETE /unified/v1/namespaces/{ns}` 仅删除空 Namespace
- [ ] `GET /unified/v1/namespaces/{ns}/assets` 跨格式返回全部 Asset
- [ ] `?format=iceberg` 只返回 Iceberg 资产，`?format=lance` 只返回 Lance 资产
- [ ] `?name=users` 按名称过滤
- [ ] 分页 `pageToken`/`pageSize` 生效，排序稳定
- [ ] `GET /unified/v1/namespaces/{ns}/assets/{name}?format={format}` 返回详情，含 `current_version`
- [ ] `PATCH /unified/v1/namespaces/{ns}/assets/{name}?format={format}` 只更新 comment/properties，且 comment 支持字符串覆盖、显式 null 清空、缺省不变
- [ ] `POST /unified/v1/namespaces/{ns}/assets/{name}/rename?format={format}` 重命名
- [ ] `DELETE /unified/v1/namespaces/{ns}/assets/{name}?format={format}` 删除
- [ ] `POST /unified/v1/namespaces/{ns}/assets` 返回 405

### 13.3 Unified API 错误格式

- [ ] 错误响应 `Content-Type` 为 `application/problem+json`
- [ ] 响应体包含 `type`、`title`、`status`、`detail`、`instance`、`code`、`request_id`
- [ ] 客户端传入 `X-Request-Id` 时响应 header 与错误体复用该值；未传入时服务端生成 UUID
- [ ] 缺失必填 `format` → 400 `InvalidInput`
- [ ] 非法/空 `format` → 400 `InvalidFormat`
- [ ] 非法 `pageToken` → 400 `InvalidPageToken`
- [ ] `pageSize > 1000` → 400 `PageSizeTooLarge`
- [ ] Namespace/Asset 不存在 → 404，使用可区分错误码
- [ ] 重复创建 → 409
- [ ] 删除非空 Namespace → 409 `NamespaceNotEmpty`
- [ ] Unified API 的 Problem Details 错误格式不会出现在 `/iceberg/v1/...` 或 `/lance/v1/...`

### 13.4 标准协议回归

- [ ] Iceberg REST Catalog 全部端点路径、请求/响应/错误格式保持不变
- [ ] Iceberg `list_tables` 只返回 `asset_subtype='iceberg'` 的 Asset
- [ ] Iceberg `commit_table` CAS 正确更新 `tabular_assets.metadata_location`
- [ ] Lance REST Namespace 全部端点路径、请求/响应/错误格式保持不变
- [ ] Lance `list_tables` 只返回 `asset_subtype='lance'` 的 Asset
- [ ] Lance `create_version` 正确写入 `asset_versions` + `tabular_asset_versions`
- [ ] 同名 Iceberg / Lance 表资产互不干扰

### 13.5 版本查看

- [ ] Iceberg `current_version.sequence_number` 来自 `last-sequence-number`
- [ ] Iceberg 有 snapshot 时 `snapshot_id`/`timestamp_ms` 正确
- [ ] Iceberg 无 snapshot 时 `snapshot_id`/`timestamp_ms` 为 null
- [ ] Iceberg S3 不可用时 `current_version` 为 null，请求不失败
- [ ] Lance declare/register 后 `current_version` 为 null
- [ ] Lance 创建版本后 `current_version.version_id` 正确
- [ ] Lance `previous_version_id` 正确映射
- [ ] Lance 创建重复 `version_id` 返回冲突
- [ ] Lance `previous_version_id` 不属于同一 Asset 时返回冲突

### 13.6 事务一致性

- [ ] Iceberg create table：S3 写入失败时 DB 不产生记录
- [ ] 表资产创建：`assets` 成功但 `tabular_assets` 失败时事务回滚
- [ ] Lance create version：`asset_versions` 成功但 `tabular_asset_versions` 失败时事务回滚
- [ ] Iceberg CAS commit：同事务更新 `tabular_assets.metadata_location/schema_snapshot` 与 `assets.properties`
- [ ] Iceberg CAS commit：对象存储成功但 DB CAS 失败时返回 409，Catalog 指针不更新
- [ ] 并发创建同名同类型同子类资产：只允许一个成功
- [ ] 并发创建同一 Lance version_id：只允许一个成功
- [ ] 并发 Iceberg commit 使用过期 `metadata_location` 时返回冲突，不能覆盖最新指针

### 13.7 构建与部署

- [ ] `cargo fmt --all --manifest-path quasar/Cargo.toml -- --check` 通过
- [ ] `cargo test --all-features --manifest-path quasar/Cargo.toml` 全部通过
- [ ] `unified` feature 启用时 `/unified/v1/...` 路由注册
- [ ] `unified` feature 关闭时 Iceberg/Lance 仍可构建并通过测试
- [ ] 双实例共享 PostgreSQL smoke test 通过

---

## 14. 修订记录

### V1.0

- 新增：概述（V1 遗留问题、V2 目标、架构变化概览）
- 新增：V2 范围与边界（保留/新增/不实现清单）
- 新增：数据模型设计（DDL、Core 模型、V1→V2 对比、外键级联行为）
- 新增：Crate 分层与模块职责（含 unified 模块、Cargo.toml 变更、条件编译）
- 新增：核心接口定义（CatalogStore trait V2 版本、StoreError、PgCatalogStore 关键 SQL 片段）
- 新增：Unified API 端点设计（路径前缀、端点清单、查询参数、DTO、状态码）
- 新增：错误处理（两层体系、RFC 7807 Problem Details、状态码映射、错误类型定义、隔离保证）
- 新增：标准协议适配器修改（Iceberg/Lance 修改清单、create_version 修改、同名共存验证）
- 新增：版本查看实现（Iceberg metadata.json 解析、Lance 数据库查询、无版本状态、S3 容错）
- 新增：条件编译（feature 定义、编译命令、实现机制）
- 新增：开发阶段划分（V2-S1 ~ V2-S8，每个阶段含前置依赖、工作内容、验收标准）
- 新增：17 条关键决策清单
- 新增：验收标准（Schema、Unified API 正向、错误格式、标准协议回归、版本查看、事务一致性、构建部署）

### V1.1

- **确认**：V2 初始 DDL 使用 PostgreSQL `pgcrypto` 与 `gen_random_uuid()` 生成 UUID，DDL 最前面必须包含 `CREATE EXTENSION IF NOT EXISTS "pgcrypto";`。
- **更新**：Core 模型新增 `PatchField<T>`，用于表达 PATCH 字段的 Missing / Null / Value 三态；Namespace 与 Asset comment PATCH 均使用该类型。
- **更新**：`CatalogStore` Namespace 接口支持 comment 创建与三态更新；`load_current_version` 返回 `Option<AssetVersionWithTabular>`，无版本不再映射为 NotFound。
- **修正**：Lance version 双表写入流程，`tabular_asset_versions.asset_version_id` 使用 `asset_versions.id` UUID，不使用 Lance 原生 `version_id`。
- **更新**：CAS commit 设计改为同事务更新 `tabular_assets.metadata_location/schema_snapshot` 与 `assets.properties`，并按 affected rows 区分 NotFound 与 Conflict。
- **更新**：Unified Problem Details 响应显式设置 `Content-Type: application/problem+json`；`request_id` 由 `X-Request-Id` middleware 生成或透传，并写入响应 header、错误体和日志。
- **更新**：`StoreError` 到 Unified 错误码不再使用字符串猜测，改为 handler 上下文映射函数。
- **更新**：server `unified` feature 明确依赖自身 `lance`、`iceberg` 以及 `quasar-adapter/unified`；所有构建命令统一使用 `--manifest-path quasar/Cargo.toml`。
- **更新**：验收标准补充 PatchField 三态、request_id、版本约束、CAS properties 原子更新、并发 commit 冲突等测试项。

### V1.2

- **重构**：开发阶段划分（§11）重构。V2-S2（Storage 层改造）拆分为 S2a（Namespace）、S2b（Asset）、S2c（Version + CAS），降低单阶段问题定位半径。
- **增强**：V2-S1 验收标准增加 DDL 迁移验证、表结构校验、CHECK 约束验证，避免 DDL 错误泄漏到 Storage 阶段。
- **增强**：所有阶段验收标准增加负向测试用例（重复创建、删除非空、format 缺失/非法、分页超限、CAS 冲突、S3 不可用容错）。
- **优化**：V2-S4（Unified Namespace）前置依赖从 V2-S3 放宽为 V2-S2a，允许 Unified API 骨架与标准协议适配器并行开发。
- **优化**：V2-S5（Unified Asset 发现）与 V2-S6（Unified Asset 管理）合并为单一阶段 V2-S5，减少阶段碎片化。
- **显式化**：V2-S5 明确声明前置依赖包含 V2-S3（需要原生 API 创建测试资产），消除隐含依赖。
- **前置**：标准协议回归验证从最终阶段（原 S8）前置到 V2-S3 验收，缩短问题反馈循环。

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。
