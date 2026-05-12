# Quasar V3 设计说明书

> **版本**: V1.4
> **日期**: 2026-05-12
> **状态**: 已确认，待实现
>
> 本文档整合并取代以下分散文档：
> - `V3_REQUIREMENTS.md`（需求分析）
> - `V3_DATA_MODEL_DESIGN.md`（数据模型设计草案）
> - `V3_PROBLEMS.md`（V2 问题清单）
> - `FORMAT_ISOLATION_ANALYSIS.md`（格式隔离策略分析）
>
> 本文档面向实现者，目标是让开发者仅凭此文档即可开始 V3 编码，无需回溯其他文档。
> 官方 REST API 来源、完整端点能力和 V3 范围由 `V3_OFFICIAL_REST_API.md` 维护。

---

## 目录

- [一、执行摘要](#一执行摘要)
- [二、架构总览](#二架构总览)
- [三、Data Core Model](#三data-core-model)
- [四、REST API 设计](#四rest-api-设计)
- [五、存储层设计](#五存储层设计)
- [六、错误处理设计](#六错误处理设计)
- [七、Feature Flag 与依赖设计](#七feature-flag-与依赖设计)
- [八、协议适配器层变更](#八协议适配器层变更)
- [九、V2 问题闭环矩阵](#九v2-问题闭环矩阵)
- [十、实现顺序](#十实现顺序)
- [十一、测试策略](#十一测试策略)
- [十二、附录](#十二附录)
- [十三、修订记录](#十三修订记录)

---

## 一、执行摘要

### 1.1 V3 核心目标

V3 的核心目标是**彻底重构 V2 的数据模型**，建立一套长期稳定、可扩展的 Data Core Model，为 V4 及以后的权限、安全、Git4Data 等功能奠定坚实基础。

### 1.2 V2 → V3 关键变更一览

| 设计域 | V2 状态 | V3 变更 |
|--------|---------|---------|
| **组织结构** | 两层（Namespace → Asset） | 三层（Domain → Namespace → Asset） |
| **格式隔离** | `asset_subtype` 同时承载资产子类型和格式 | 端点级隔离（路径隐含格式） |
| **资产身份** | `asset_subtype` 语义模糊 | 统一身份层 + 扩展表 |
| **资产命名** | 同名跨格式共存 | 同一 Namespace 内活动资产名唯一 |
| **版本模型** | `version_key`/`version_order` 混用 | 明确分工，禁止字符串解析 fallback |
| **存储 trait** | 单一 `CatalogStore` 职责膨胀 | 拆分为 8 个专注 trait |
| **错误处理** | `Internal(String)` 信息泄露 | `Internal { msg, source }` 客户端脱敏 |
| **SQL 管理** | 内联分散 | `queries.rs` 常量模块集中管理 |
| **Feature Flag** | `object_store` 条件编译复杂 | 公共依赖，简化语义 |

### 1.3 V3 不改什么

以下事项明确**不在** V3 范围内：

- 不引入 metadata 缓存（Iceberg 实时读取对象存储维持现状）
- 不引入 cursor-based 分页（OFFSET 性能隐患维持现状）
- 不实现 Unified API 的 Asset 创建（维持 405）
- 不解决对象存储与数据库的原子性（维持后台清理方向）
- 不实现 Catalog-Aware Lance Commits（维持客户端写 S3 + 事后注册）
- 不实现完整 RBAC（`asset_permissions` 预留但不暴露 API）
- 不实现嵌套 Namespace（单层 Namespace，后续单独设计）

---

## 二、架构总览

### 2.1 三层层级架构

```
┌─────────────────────────────────────────────┐
│  第一层：Domain（顶层容器）                   │
│  • 存储配置边界（S3 bucket、warehouse 路径）  │
│  • 权限隔离边界                               │
│  • 多租户隔离边界（如果有）                   │
├─────────────────────────────────────────────┤
│  第二层：Namespace（业务组织）                │
│  • 逻辑分组（如 analytics、marketing、ml）   │
│  • V3 单层，嵌套 Namespace 后续单独设计       │
│  • 同一 Domain 内 Namespace 名唯一            │
├─────────────────────────────────────────────┤
│  第三层：Asset（实际资产）                    │
│  • table、model、fileset、topic 等           │
│  • 同一 Namespace 内活动资产名唯一            │
│  • 格式信息在扩展表中                         │
└─────────────────────────────────────────────┘
```

### 2.2 格式隔离策略：端点级隔离

**核心原则：Domain 不绑定格式，格式由 REST 端点路径隐含。**

```
Domain 'prod'
  └── Namespace 'analytics'
        ├── Asset 'events' (format=iceberg)
        │     → /iceberg/v1/prod/namespaces/analytics/tables/events
        └── Asset 'embeddings' (format=lance)
              → /lance/v1/table/prod$analytics$embeddings/describe
```

**选择端点级隔离的考量：**

1. **管理后台需要统一视图**：同一页面需要同时看到 Iceberg 表、Lance 数据集、Model 等所有资产。如果 Domain 绑定格式，管理后台需要跨 Domain 聚合。
2. **Iceberg 和 Lance 是平等的一等公民**：两者都有完整的协议能力，不存在主次关系。
3. **基础设施共享**：Iceberg 和 Lance 的元数据都存储在 Quasar 的 PostgreSQL 中，数据文件都在同一个 S3 bucket 下。
4. **用户心智负担**：用户配置一个 Domain 连接即可管理所有资产。

**代价**：每个协议端点内部需要按 `format` 字段过滤，用错端点会返回 `TableNotFoundException`。

**官方兼容原则**：
- Iceberg：`Domain.name` 映射为官方 REST Catalog 的 `{prefix}`。
- Lance：官方 REST Namespace 不允许新增 `/domains/{domain}` 这类协议路径；`Domain.name` 编码为 Lance `{id}` 的第一段。
- Unified / Domain 管理 API 是 Quasar-specific API，可以显式使用 `/domains/{domain}` 路径。

### 2.3 系统组件图

```
┌──────────────────────────────────────────────────────────────┐
│                        quasar-server                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ /iceberg/v1 │  │ /lance/v1   │  │ /unified/v1         │  │
│  │  (adapter)  │  │  (adapter)  │  │  (adapter)          │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                    │              │
│         └────────────────┴────────────────────┘              │
│                          │                                   │
│                    Router State                              │
│              Arc<dyn StoreCollection>                        │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-adapter                           │
│  ┌───────────────────────┼───────────────────────────────┐   │
│  │    quasar-core        │   (traits + models + errors)  │   │
│  │  ┌────────────────────┴───────────────────────────┐   │   │
│  │  │ DomainStore | NamespaceStore | AssetStore      │   │   │
│  │  │ TabularStore | VersionStore | TabularVersionStore│  │   │
│  │  │ CasCommitStore | UnifiedQueryStore              │   │   │
│  │  └────────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-storage                           │
│              PgCatalogStore (impl all traits)                │
│                     queries.rs (SQL 常量)                     │
└──────────────────────────┬───────────────────────────────────┘
                           │
                    PostgreSQL + S3
```

**Feature Flag 边界**：
- `iceberg`：控制 Iceberg REST 路由和 handler 是否编译
- `lance`：控制 Lance REST 路由和 handler 是否编译
- `unified`：控制 Unified REST 路由和 handler 是否编译
- `object_store`：公共依赖，不受 feature flag 控制

---

## 三、Data Core Model

### 3.1 设计原则（核心不变量）

V3 Data Core Model 必须满足以下不变量：

1. **资产身份与类型属性分离**：所有资产共享统一身份层；表、模型、文件集等类型的专有字段进入各自扩展层。
2. **格式信息不在资产身份层**：`iceberg`、`lance`、`delta` 等表格式属于表资产属性，不属于所有资产的公共身份。
3. **类型与格式可扩展**：新增资产类型或表格式不应要求修改核心表结构；通过注册表机制校验合法值。
4. **版本模型同时保留原生标识与排序语义**：`version_key` 记录格式原生版本标识，`version_order` 提供可比较顺序；禁止从字符串解析 fallback 伪造排序。
5. **核心完整性由数据库约束兜底**：Domain、Namespace、Asset、Version 之间的身份关系、唯一性和引用完整性不能只依赖应用层。
6. **删除必须显式且可控**：Domain / Namespace 这类上层容器不得因普通删除语句隐式级联删除大量下游资产；非空容器删除应返回冲突。
7. **性能约束与完整性约束并重**：必须为外键引用方、主要列表查询和 latest-version 查询提供必要索引。
8. **敏感配置不得明文暴露**：Domain 级存储配置如涉及 credential，应存储 secret reference 或加密后的配置，并在 API 响应中脱敏。

### 3.2 逻辑模型

```
domains
  └── namespaces
        └── assets
              ├── tabular_assets
              ├── model_assets        # 后续版本引入
              ├── fileset_assets      # 后续版本引入
              └── asset_versions
                    ├── tabular_asset_versions
                    └── model_asset_versions  # 后续版本引入
```

权限、标签、审计、血缘等治理能力优先绑定 `assets.id`。

### 3.3 物理 Schema（DDL）

#### 3.3.1 注册表

V3 使用注册表替代硬编码 `CHECK` 约束：

```sql
CREATE TABLE asset_types (
    -- 资产大类名称，如 table / model / fileset / topic。
    name TEXT PRIMARY KEY,
    -- 资产类型说明，用于管理后台展示和运维识别。
    comment TEXT,
    -- 注册时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tabular_formats (
    -- 表资产格式名称，如 iceberg / lance / delta。
    name TEXT PRIMARY KEY,
    -- 表格式说明，用于管理后台展示和运维识别。
    comment TEXT,
    -- 是否支持由 Catalog Service 协调的 catalog-managed CAS commit。
    -- Iceberg 支持；当前 Lance 只支持外部写入后的版本注册，不支持 catalog-managed commit。
    supports_cas_commit BOOLEAN NOT NULL DEFAULT FALSE,
    -- 注册时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO asset_types (name, comment)
VALUES ('table', 'Tabular dataset')
ON CONFLICT DO NOTHING;

INSERT INTO tabular_formats (name, comment, supports_cas_commit)
VALUES
    ('iceberg', 'Apache Iceberg table', TRUE),
    ('lance', 'Lance dataset', FALSE)
ON CONFLICT DO NOTHING;
```

#### 3.3.2 domains

```sql
CREATE TABLE domains (
    -- Domain 唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Domain 名称，全局唯一；用于 API 路径、管理后台展示和权限边界识别。
    name TEXT NOT NULL UNIQUE,
    -- Domain 描述说明。
    comment TEXT,
    -- 业务自定义属性，如标签、成本中心、环境、负责人等。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 存储类型，如 s3 / minio / hdfs / local；用于选择 object_store 初始化逻辑。
    storage_type TEXT,
    -- 存储配置。不得明文存储 credential；应存 secret reference 或加密后的敏感值。
    storage_config JSONB NOT NULL DEFAULT '{}',
    -- 默认 warehouse 根路径；创建托管表时可基于该路径分配物理位置。
    warehouse TEXT,
    -- Domain 所有者标识，如团队、用户或服务账号。
    owner TEXT,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

设计说明：
- Domain 不绑定 `format` / `provider`。
- `storage_config` 不应存明文 credential；优先存 secret reference。
- `warehouse` 是默认物理路径分配根。

#### 3.3.3 namespaces

```sql
CREATE TABLE namespaces (
    -- Namespace 唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Domain。非空 Domain 不允许被删除。
    domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    -- Namespace 名称；同一 Domain 内唯一。
    name TEXT NOT NULL,
    -- Namespace 描述说明。
    comment TEXT,
    -- 业务自定义属性，如标签、owner、默认配置等。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 保证同一 Domain 内 Namespace 名称唯一。
    UNIQUE(domain_id, name)
);

CREATE INDEX idx_namespaces_domain ON namespaces(domain_id);
```

设计说明：
- V3 只实现单层 Namespace。
- 不在 V3 DDL 中加入 `parent_id` 作为半成品预留。
- 非空 Domain 删除由 FK `RESTRICT` 阻止。

#### 3.3.4 assets

```sql
CREATE TABLE assets (
    -- Asset 唯一标识，后续权限、审计、血缘等治理能力统一引用该 ID。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Namespace。非空 Namespace 不允许被删除。
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    -- Asset 名称；同一 Namespace 内活动资产名唯一。
    name TEXT NOT NULL,
    -- 资产大类，如 table / model / fileset；引用资产类型注册表。
    asset_type TEXT NOT NULL REFERENCES asset_types(name),
    -- Asset 描述说明。
    comment TEXT,
    -- 业务自定义属性；只承载治理/管理面属性，不存格式内部状态。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 软删除时间。NULL 表示活动资产；非 NULL 表示已软删除。
    deleted_at TIMESTAMPTZ,
    -- 创建者标识，预留给审计或权限系统。
    created_by TEXT,
    -- 最后更新者标识，预留给审计或权限系统。
    updated_by TEXT,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_assets_active_name
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_assets_type_active
    ON assets(asset_type)
    WHERE deleted_at IS NULL;
```

设计说明：
- `assets` 是统一身份层，不存表格式。
- 活动资产名在 Namespace 内唯一（部分唯一索引）。
- 协议端点创建表时，若同名活动资产已存在，即使格式不同也返回冲突。
- `deleted_at` / `created_by` / `updated_by` 为 V4 审计/软删除预留。
- `uq_assets_active_name` 同时承担活动资产定位查询的索引职责，因此不再额外创建同列普通索引。

#### 3.3.5 tabular_assets

```sql
CREATE TABLE tabular_assets (
    -- 关联的通用 Asset ID；一个表资产对应一个通用资产身份。
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    -- 表格式，如 iceberg / lance；引用表格式注册表。
    format TEXT NOT NULL REFERENCES tabular_formats(name),
    -- 表数据根路径或数据集根路径。
    location TEXT NOT NULL,
    -- 当前元数据位置。Iceberg 为当前 metadata.json；Lance 可为空。
    metadata_location TEXT,
    -- schema 快照缓存，用于管理面快速展示；格式内权威状态仍以协议语义为准。
    schema_snapshot JSONB,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tabular_assets_format_asset
    ON tabular_assets(format, asset_id);

CREATE OR REPLACE FUNCTION ensure_tabular_asset_type()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM assets
        WHERE id = NEW.asset_id
          AND asset_type = 'table'
    ) THEN
        RAISE EXCEPTION 'tabular asset % must reference an asset with asset_type=table', NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tabular_assets_asset_type
BEFORE INSERT OR UPDATE OF asset_id ON tabular_assets
FOR EACH ROW
EXECUTE FUNCTION ensure_tabular_asset_type();
```

设计说明：
- `format` 属于表资产扩展层。
- `metadata_location` 对 Iceberg 是当前 metadata.json 指针；对 Lance 可以为空。
- `trg_tabular_assets_asset_type` 保证 `tabular_assets.asset_id` 对应的 `assets.asset_type = 'table'`，该核心不变量不只依赖应用层。

#### 3.3.6 asset_versions

```sql
CREATE TABLE asset_versions (
    -- 版本记录唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Asset；删除 Asset 时清理其版本记录。
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- 格式原生版本标识。同一 Asset 内唯一，如 Lance 的 "1" 或模型版本 "v1.2.0"。
    version_key TEXT NOT NULL,
    -- 可比较的版本顺序。支持自然递增版本的格式应写入；无稳定数字顺序时可为空。
    version_order BIGINT,
    -- 前驱版本记录 ID，用于表达版本链。
    previous_version_id UUID REFERENCES asset_versions(id) ON DELETE SET NULL,
    -- 版本说明，如提交摘要或人工备注。
    comment TEXT,
    -- 版本级自定义属性，不存表版本 metadata_location 等类型专有字段。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 保证同一 Asset 下原生版本标识唯一。
    UNIQUE(asset_id, version_key)
);

CREATE UNIQUE INDEX uq_asset_versions_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;

CREATE INDEX idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;

CREATE INDEX idx_asset_versions_previous
    ON asset_versions(previous_version_id);

CREATE OR REPLACE FUNCTION ensure_previous_version_same_asset()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.previous_version_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM asset_versions
           WHERE id = NEW.previous_version_id
             AND asset_id = NEW.asset_id
       ) THEN
        RAISE EXCEPTION 'previous_version_id % must reference a version of the same asset %',
            NEW.previous_version_id, NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_asset_versions_previous_same_asset
BEFORE INSERT OR UPDATE OF asset_id, previous_version_id ON asset_versions
FOR EACH ROW
EXECUTE FUNCTION ensure_previous_version_same_asset();
```

设计说明：
- `version_key` 是原生版本标识（如 Lance 的 `"1"`、模型版本的 `"v1.2.0"`）。
- `version_order` 是可比较顺序；Lance 写入 `version_order = version_id`；不具备稳定数字顺序的格式可以为空。
- latest 查询不得从 `version_key` 解析 fallback。
- `trg_asset_versions_previous_same_asset` 保证 `previous_version_id` 必须指向同一 Asset 下版本。

#### 3.3.7 tabular_asset_versions

```sql
CREATE TABLE tabular_asset_versions (
    -- 关联的通用版本记录 ID；一个表版本对应一个通用版本身份。
    version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    -- 该表版本的元数据文件或 manifest 位置。
    metadata_location TEXT NOT NULL,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

设计说明：
- 表版本的 metadata / manifest 位置放在表版本扩展层。
- Iceberg V3 初始仍可只用 `tabular_assets.metadata_location` 表达当前指针。
- Lance `create_version` 写入 `asset_versions` 与 `tabular_asset_versions`，且必须在同一数据库事务中完成。

#### 3.3.8 asset_permissions（预留）

```sql
CREATE TABLE asset_permissions (
    -- 权限记录唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 授权目标 Asset；权限系统不感知资产类型和格式。
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- 权限主体，如 user / role / group 的标识符。
    subject TEXT NOT NULL,
    -- 权限动作，如 read / write / delete / admin。
    action TEXT NOT NULL,
    -- 授权人标识。
    granted_by TEXT NOT NULL,
    -- 授权时间。
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 过期时间；NULL 表示长期有效。
    expires_at TIMESTAMPTZ,
    -- 防止同一主体对同一资产重复授予同一动作。
    UNIQUE(asset_id, subject, action)
);

CREATE INDEX idx_asset_permissions_asset ON asset_permissions(asset_id);
CREATE INDEX idx_asset_permissions_subject ON asset_permissions(subject);
```

V3 不实现完整 RBAC，该表可创建但不暴露 API。

### 3.4 数据库约束与级联规则

| 父子关系 | ON DELETE | 理由 |
|---------|-----------|------|
| `domains` → `namespaces` | `RESTRICT` | 非空 Domain 不可删除，防止误删 |
| `namespaces` → `assets` | `RESTRICT` | 非空 Namespace 不可删除，防止误删 |
| `assets` → `tabular_assets` | `CASCADE` | 删除 Asset 连带清理扩展记录（仅由 Asset 删除 API 触发） |
| `assets` → `asset_versions` | `CASCADE` | 同上 |
| `asset_versions` → `tabular_asset_versions` | `CASCADE` | 版本删除连带清理扩展记录 |
| `asset_versions` → `asset_versions` (previous) | `SET NULL` | 前驱版本被删除时断开链接 |

**补充约束机制**：
- `tabular_assets.asset_id` 必须引用 `asset_type='table'` 的 Asset，由 `trg_tabular_assets_asset_type` 触发器兜底。
- `asset_versions.previous_version_id` 必须指向同一 Asset 下的版本，由 `trg_asset_versions_previous_same_asset` 触发器兜底。
- 应用层仍需在同一事务内做显式校验，用于返回更清晰的协议错误；数据库触发器是最后防线。

### 3.5 Core Rust 模型

V3 Core 模型使用可扩展的 `String` 替代硬编码枚举，标准协议适配器可继续使用内部枚举做路由。

```rust
pub struct Domain {
    /// Domain 唯一标识。
    pub id: Uuid,
    /// Domain 名称，全局唯一。
    pub name: String,
    /// Domain 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性，如标签、成本中心、环境、负责人等。
    pub properties: HashMap<String, String>,
    /// 存储类型，如 s3 / minio / hdfs / local。
    pub storage_type: Option<String>,
    /// 存储配置。使用 JSON 值承载嵌套配置和 secret reference，API 响应必须脱敏。
    pub storage_config: serde_json::Value,
    /// 默认 warehouse 根路径。
    pub warehouse: Option<String>,
    /// Domain 所有者标识。
    pub owner: Option<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct Namespace {
    /// Namespace 唯一标识。
    pub id: Uuid,
    /// 所属 Domain ID。
    pub domain_id: Uuid,
    /// Namespace 名称，同一 Domain 内唯一。
    pub name: String,
    /// Namespace 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性。
    pub properties: HashMap<String, String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct Asset {
    /// Asset 唯一标识，治理能力统一引用该 ID。
    pub id: Uuid,
    /// 所属 Namespace ID。
    pub namespace_id: Uuid,
    /// Asset 名称，同一 Namespace 内活动资产名唯一。
    pub name: String,
    /// 资产大类，如 table / model / fileset。
    pub asset_type: String,
    /// Asset 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性，不存格式内部状态。
    pub properties: HashMap<String, String>,
    /// 软删除时间；None 表示活动资产。
    pub deleted_at: Option<DateTime<Utc>>,
    /// 创建者标识。
    pub created_by: Option<String>,
    /// 最后更新者标识。
    pub updated_by: Option<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct TabularAsset {
    /// 关联的通用 Asset ID。
    pub asset_id: Uuid,
    /// 表格式，如 iceberg / lance。
    pub format: String,
    /// 表数据根路径或数据集根路径。
    pub location: String,
    /// 当前元数据位置；Iceberg 为 metadata.json，Lance 可为空。
    pub metadata_location: Option<String>,
    /// schema 快照缓存。
    pub schema_snapshot: Option<serde_json::Value>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct AssetVersion {
    /// 版本记录唯一标识。
    pub id: Uuid,
    /// 所属 Asset ID。
    pub asset_id: Uuid,
    /// 格式原生版本标识，同一 Asset 内唯一。
    pub version_key: String,
    /// 可比较版本顺序；无稳定数字顺序时为空。
    pub version_order: Option<i64>,
    /// 前驱版本记录 ID。
    pub previous_version_id: Option<Uuid>,
    /// 版本说明。
    pub comment: Option<String>,
    /// 版本级自定义属性。
    pub properties: HashMap<String, String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

pub struct TabularAssetVersion {
    /// 关联的通用版本记录 ID。
    pub version_id: Uuid,
    /// 该表版本的元数据文件或 manifest 位置。
    pub metadata_location: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}
```

**类型安全策略**：V3 先用普通 `String`，若后续出现因类型混淆导致的 bug，再引入 newtype（如 `AssetTypeName(String)`、`TabularFormatName(String)`）。

### 3.6 版本模型规范

| 字段 | 类型 | 语义 | 示例 |
|------|------|------|------|
| `version_key` | `TEXT` | 格式原生版本标识，同一 Asset 内唯一 | Lance: `"42"`，模型: `"v1.2.0"` |
| `version_order` | `BIGINT` | 可比较顺序，允许为空 | Lance: `42`，Iceberg 初始: `NULL` |
| `previous_version_id` | `UUID` | 前驱版本 ID | 指向同一 Asset 下的版本 |

**格式特定映射**：

| 格式 | version_key | version_order | latest 查询方式 |
|------|-------------|---------------|-----------------|
| Lance | `"1"`, `"2"`, ... | `1`, `2`, ... | `version_order DESC` |
| Iceberg (V3) | 可选（若使用版本表） | `NULL` | `tabular_assets.metadata_location` |
| 未来 Model | `"v1.2.0"` | `NULL` | 格式内权威指针或返回无 latest |

**禁止事项**：
- 禁止 `version_key.parse().unwrap_or(0)` 作为排序 fallback。
- latest 查询只能基于明确的 `version_order` 或格式内权威指针。
- `previous_version_id` 跨 Asset 引用必须被应用层拒绝，并由数据库触发器兜底。

---

## 四、REST API 设计

### 4.1 URL 结构与路由

官方 REST API 的完整来源、端点能力和 V3 实现范围见 `V3_OFFICIAL_REST_API.md`。本章只描述 Quasar V3 的路由挂载、Domain 映射和当前实现子集。

#### 4.1.1 Iceberg REST Catalog

服务部署前缀：`/iceberg`。官方协议路径仍为 `/v1/...`，Quasar 不改变 Iceberg REST Catalog 请求/响应格式。

Domain 映射规则：`{prefix}` 为 `Domain.name`。

| 方法 | 路径 | Handler | 说明 |
|------|------|---------|------|
| GET | `/config` | `config::get_config` | 返回 Iceberg catalog 配置、默认参数和支持端点列表；不带 `{prefix}`，可根据 `warehouse` 查询参数或服务端默认 Domain 生成响应。 |
| GET | `/{prefix}/namespaces` | `namespace::list_namespaces` | 列出 `{prefix}` 对应 Domain 下的单层 Namespace；只返回该 Domain 内可见的 Namespace。 |
| POST | `/{prefix}/namespaces` | `namespace::create_namespace` | 在 `{prefix}` 对应 Domain 下创建 Namespace；若 Domain 不存在返回 `NoSuchNamespaceException` 或配置错误语义，重名返回 409。 |
| GET | `/{prefix}/namespaces/{ns}` | `namespace::get_namespace` | 加载指定 Namespace 的属性；`{ns}` 必须属于 `{prefix}` 对应 Domain。 |
| HEAD | `/{prefix}/namespaces/{ns}` | `namespace::namespace_exists` | 检查 Namespace 是否存在；存在返回 200，不存在返回 404，不返回响应体。 |
| DELETE | `/{prefix}/namespaces/{ns}` | `namespace::drop_namespace` | 删除空 Namespace；若 Namespace 下仍有活动 Asset，返回 409，不触发级联删除。 |
| POST | `/{prefix}/namespaces/{ns}/properties` | `namespace::update_namespace_properties` | 按 Iceberg `removals` / `updates` 语义更新 Namespace properties；不允许修改 Domain 归属。 |
| GET | `/{prefix}/namespaces/{ns}/tables` | `table::list_tables` | 列出指定 Namespace 下 Iceberg 表；查询必须过滤 `asset_type='table'` 且 `format='iceberg'`。 |
| POST | `/{prefix}/namespaces/{ns}/tables` | `table::create_table` | 创建 Iceberg 表资产并写入初始 metadata 指针；同名活动 Asset 即使是其他格式也返回 `TableAlreadyExistsException`。 |
| GET | `/{prefix}/namespaces/{ns}/tables/{table}` | `table::load_table` | 加载 Iceberg 表 metadata；若同名资产存在但不是 Iceberg 格式，按官方语义返回 `NoSuchTableException`。 |
| POST | `/{prefix}/namespaces/{ns}/tables/{table}` | `table::commit_table` | 执行 Iceberg CAS commit；校验 requirements，写入新 metadata 后原子更新 `metadata_location`。 |
| DELETE | `/{prefix}/namespaces/{ns}/tables/{table}` | `table::drop_table` | 删除 Iceberg 表 catalog 记录；`purgeRequested` 的数据清理语义由适配器配置决定，V3 可先只做 catalog drop。 |
| HEAD | `/{prefix}/namespaces/{ns}/tables/{table}` | `table::table_exists` | 检查 Iceberg 表是否存在；只认 `format='iceberg'` 的表，不返回响应体。 |
| POST | `/{prefix}/tables/rename` | `table::rename_table` | 按 Iceberg rename 请求将表移动到目标 Namespace/名称；需校验源表存在、目标 Namespace 存在且目标名未被占用。 |

#### 4.1.2 Lance REST API

服务部署前缀：`/lance`。官方协议路径仍为 `/v1/...`，Quasar 不改变 Lance REST Namespace 请求/响应格式。

Domain 映射规则：Lance `{id}` 使用配置的 delimiter（默认 `$`）序列化对象路径，`Domain.name` 是第一段：

| Quasar 对象 | Lance `{id}` |
|-------------|--------------|
| Domain `prod` | `prod` |
| Domain `prod` + Namespace `analytics` | `prod$analytics` |
| Domain `prod` + Namespace `analytics` + Table `embeddings` | `prod$analytics$embeddings` |

V3 对 Lance Namespace ID 的解析规则：
- `{id}="$"` 表示 Lance root namespace；用于列出所有 Domain。
- `{id}` 只有一段时表示 Domain 的虚拟 Lance namespace。
- `{id}` 有两段时表示 Quasar `Domain + Namespace`。
- 表 `{id}` 必须有三段：`Domain + Namespace + Table`。
- V3 不支持更深层级的 Lance namespace；超过上述段数返回官方 Lance `Unsupported` 或 `InvalidInput` 语义。

| 方法 | 路径 | Handler | 说明 |
|------|------|---------|------|
| POST | `/namespace/{id}/create` | `namespace::create_namespace` | 创建 Lance Namespace；`id=prod` 映射为创建 Domain，`id=prod$analytics` 映射为在 Domain 下创建 Namespace。 |
| GET | `/namespace/{id}/list` | `namespace::list_namespaces` | 列出子 Namespace；`id=$` 列出 Domain，`id=prod` 列出该 Domain 下 Namespace，V3 不返回更深层级。 |
| POST | `/namespace/{id}/describe` | `namespace::describe_namespace` | 返回 Namespace 属性；一段 `id` 返回 Domain 视图，两段 `id` 返回 Namespace 视图。 |
| POST | `/namespace/{id}/drop` | `namespace::drop_namespace` | 删除空 Domain 或空 Namespace；非空容器返回 409，不级联删除下游对象。 |
| POST | `/namespace/{id}/exists` | `namespace::namespace_exists` | 检查 Domain 或 Namespace 是否存在；按 Lance 错误模型返回存在性结果。 |
| GET | `/namespace/{id}/table/list` | `table::list_tables` | 列出 `id=domain$namespace` 下 Lance 表；查询必须过滤 `asset_type='table'` 且 `format='lance'`。 |
| POST | `/table/{id}/declare` | `table::declare_table` | 声明 Lance 表并占用名称/位置；`id` 必须为 `domain$namespace$table`，同名活动 Asset 返回冲突。 |
| POST | `/table/{id}/describe` | `table::describe_table` | 返回 Lance 表信息；可按请求参数描述指定版本，未指定时返回 latest 视图。 |
| POST | `/table/{id}/register` | `table::register_table` | 注册已存在的 Lance 数据集位置为表资产；不负责创建底层数据文件。 |
| POST | `/table/{id}/deregister` | `table::deregister_table` | 注销 catalog 记录但保留对象存储数据；用于断开 Catalog 与已有 Lance 数据集的绑定。 |
| POST | `/table/{id}/drop` | `table::drop_table` | 删除 Lance 表 catalog 记录；是否清理对象存储数据由 Lance 请求语义和服务端配置决定。 |
| POST | `/table/{id}/exists` | `table::table_exists` | 检查 Lance 表是否存在；只认 `format='lance'` 的表。 |
| POST | `/table/{id}/rename` | `table::rename_table` | 重命名 Lance 表；需保证目标 `domain$namespace$table` 的 Namespace 存在且目标名未被占用。 |
| POST | `/table/{id}/version/create` | `version::create_version` | 注册已由 Lance 客户端写入对象存储的版本；写入 `asset_versions` 与 `tabular_asset_versions`。 |
| GET | `/table/{id}/version/list` | `version::list_versions` | 按 `version_order` 升序列出 Lance 表版本；禁止从 `version_key` 字符串解析排序。 |
| POST | `/table/{id}/version/describe` | `version::describe_version` | 返回指定 Lance 表版本的元数据位置和版本信息；版本不存在返回 Lance 版本 not found 语义。 |

#### 4.1.3 Unified API

前缀：`/unified/v1`

| 方法 | 路径 | Handler | 说明 |
|------|------|---------|------|
| GET | `/domains` | `domain::list_domains` | 列出 Quasar Domain；用于管理面查看存储/治理边界，响应中的敏感存储配置必须脱敏。 |
| POST | `/domains` | `domain::create_domain` | 创建 Domain，并写入存储类型、warehouse、owner、properties 和脱敏/引用化的 storage config。 |
| GET | `/domains/{domain}` | `domain::get_domain` | 获取单个 Domain 的管理视图；响应必须脱敏 `storage_config` 中的 credential 或 secret 值。 |
| PATCH | `/domains/{domain}` | `domain::update_domain` | 更新 Domain 描述、属性、owner、warehouse 或存储配置；不得隐式迁移已有资产数据。 |
| DELETE | `/domains/{domain}` | `domain::drop_domain` | 删除空 Domain；若仍包含 Namespace，返回 `DomainNotEmpty` / 409。 |
| GET | `/domains/{domain}/namespaces` | `namespace::list_namespaces` | 列出 Domain 下单层 Namespace；不跨 Domain 聚合。 |
| POST | `/domains/{domain}/namespaces` | `namespace::create_namespace` | 在指定 Domain 下创建 Namespace；同一 Domain 内名称唯一。 |
| GET | `/domains/{domain}/namespaces/{ns}` | `namespace::get_namespace` | 获取 Namespace 管理视图，包括 comment、properties 和创建更新时间。 |
| DELETE | `/domains/{domain}/namespaces/{ns}` | `namespace::drop_namespace` | 删除空 Namespace；若仍包含活动 Asset，返回 `NamespaceNotEmpty` / 409。 |
| PATCH | `/domains/{domain}/namespaces/{ns}` | `namespace::update_namespace` | 更新 Namespace 描述和 properties；不允许通过 PATCH 改变所属 Domain。 |
| GET | `/domains/{domain}/namespaces/{ns}/assets` | `asset::list_assets` | 列出 Namespace 下活动 Asset；支持 `?format=` 过滤表格式，并可扩展 `asset_type` / `name` 过滤。 |
| POST | `/domains/{domain}/namespaces/{ns}/assets` | `asset::create_asset_not_allowed` | V3 暂不提供格式无关 Asset 创建；返回 405，引导调用方使用 Iceberg/Lance 官方创建端点。 |
| GET | `/domains/{domain}/namespaces/{ns}/assets/{name}` | `asset::get_asset` | 获取单个 Asset 的统一视图；活动资产名唯一，因此无需 `?format=`。 |
| DELETE | `/domains/{domain}/namespaces/{ns}/assets/{name}` | `asset::drop_asset` | 删除 Asset 的 catalog 记录并清理扩展表/版本记录；不默认删除对象存储数据。 |
| PATCH | `/domains/{domain}/namespaces/{ns}/assets/{name}` | `asset::update_asset` | 更新 Asset 描述和 properties；不允许修改 `asset_type` 或表格式。 |
| POST | `/domains/{domain}/namespaces/{ns}/assets/{name}/rename` | `asset::rename_asset` | 重命名 Asset；目标名称在同一 Namespace 内必须未被活动资产占用。 |

### 4.2 Iceberg REST Catalog API

**Domain 上下文注入**：
- 从路径 `{prefix}` 提取 Domain 名称。
- `GET /v1/config` 不带 `{prefix}`；可通过 `warehouse` 查询参数或服务端默认配置返回可用 catalog 配置。
- 所有 Namespace 操作需校验 Domain 存在。
- 所有 Table 操作需同时校验 Domain 和 Namespace。
- Quasar 的 `/iceberg` 只是服务部署 base path；adapter 注册路由时不得把 `/iceberg` 写入 Iceberg DTO 或 endpoint capability。

**表过滤逻辑**：
```sql
-- Iceberg 端点内部查询必须附加过滤条件：
WHERE asset_type = 'table'
  AND tabular_assets.format = 'iceberg'
```
- 若 Iceberg 端点查询到同名但 format='lance' 的资产，返回 `NoSuchTableException`（404）。
- 若 Iceberg 端点创建表时同名资产已被其他 format 占用，返回 `TableAlreadyExistsException`（409）。

**CAS Commit 流程**（逻辑不变，trait 方法迁移至 `CasCommitStore`）：
1. Handler 读取当前 metadata.json。
2. 应用 updates 和 requirements。
3. 生成新的 metadata.json 并写入对象存储。
4. 调用 `cas_update_metadata_location(..., expected, new, ...)`。
5. 若 expected 不匹配，返回 `CommitFailedException`。

### 4.3 Lance REST API

**Domain 上下文**：Lance 端点从官方 `{id}` 解析 Domain，不增加 Quasar 自定义路径段。

**ID 解析规则**：
```rust
// delimiter 默认 "$"，可由 Lance namespace 配置覆盖。
// namespace id:
"$"                  => LanceNamespaceRef::Root
"prod"               => LanceNamespaceRef::Domain { domain: "prod" }
"prod$analytics"     => LanceNamespaceRef::Namespace { domain: "prod", namespace: "analytics" }

// table id:
"prod$analytics$embeddings"
    => LanceTableRef { domain: "prod", namespace: "analytics", table: "embeddings" }
```

**创建语义**：
- `POST /v1/namespace/{id}/create` 中 `{id}` 为一段时，创建 Quasar Domain。
- `{id}` 为两段时，在第一段 Domain 下创建 Quasar Namespace。
- V3 不支持更深层 Namespace；更深层 `{id}` 返回官方 Lance `Unsupported` 或 `InvalidInput`。

**create_version 语义澄清**：
- `POST /v1/table/{id}/version/create` 的本质是**注册已存在的版本**，不是协调写入。
- Lance SDK 已经直接写 S3 生成 manifest，Catalog Service 仅在 DB 中记录该版本。
- 这是**最终一致**语义：S3 和 DB 可能短暂不一致。

**版本响应格式**：
```rust
// V2（错误）：
version: version.version_order.unwrap_or_else(||
    v.version.version_key.parse().unwrap_or(0)
)

// V3（正确）：
version: version.version_order.ok_or_else(||
    StoreError::Internal { msg: "version_order is missing".into(), source: None }
)?
```

### 4.4 Unified API

**V2 → V3 关键变更**：

| 变更项 | V2 | V3 |
|--------|-----|-----|
| Domain 管理 | 无 | `GET/POST/PATCH/DELETE /domains...` |
| Asset GET | `GET /assets/{name}?format=iceberg` | `GET /domains/{domain}/namespaces/{ns}/assets/{name}`（无 format） |
| Asset DELETE | `DELETE /assets/{name}?format=iceberg` | `DELETE /domains/{domain}/namespaces/{ns}/assets/{name}`（无 format） |
| Asset PATCH | `PATCH /assets/{name}?format=iceberg` | `PATCH /domains/{domain}/namespaces/{ns}/assets/{name}`（无 format） |
| Asset rename | `POST /assets/{name}/rename?format=iceberg` | `POST /domains/{domain}/namespaces/{ns}/assets/{name}/rename`（无 format） |
| Asset 列表 | 支持 `?format=` 可选过滤 | `GET /domains/{domain}/namespaces/{ns}/assets?format=...` |
| Asset 创建 | 405 | 405（维持） |

**响应字段变更**：
```rust
pub struct AssetResponse {
    pub id: String,
    pub name: String,
    pub asset_type: String,    // V3 新增：如 "table"
    pub format: Option<String>, // V3：表资产为 "iceberg"/"lance"，非表资产为 None
    pub location: String,
    pub metadata_location: Option<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
    pub current_version: Option<CurrentVersionResponse>,
    pub created_at: String,
}
```

### 4.5 请求/响应 DTO 变更汇总

**Iceberg DTOs**：基本不变，维持 Iceberg REST Catalog 协议兼容性。

**Lance DTOs**：
- 请求/响应 DTO 维持 Lance REST Namespace 协议兼容性。
- 请求路径不增加 `domain` 参数；adapter 从官方 `{id}` 第一段解析 Domain。
- `VersionResponse.version` 字段改为从 `version_order` 直接读取（不再解析 `version_key`）。

**Unified DTOs**：
- 新增 `DomainResponse` / `DomainListResponse` / `CreateDomainRequest` / `UpdateDomainRequest`。
- `AssetResponse` / `AssetListItem`：新增 `asset_type: String` 字段，`format` 改为 `Option<String>`。
- `AssetDetailQuery`：删除 `format` 必填字段（列表过滤仍支持）。

---

## 五、存储层设计

### 5.1 Store Trait 拆分

V3 将单一 `CatalogStore` 拆分为 8 个专注 trait：

```rust
#[async_trait]
pub trait DomainStore: Send + Sync {
    async fn create_domain(
        &self,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
        storage_type: Option<String>,
        storage_config: serde_json::Value,
        warehouse: Option<String>,
        owner: Option<String>,
    ) -> Result<Domain, StoreError>;

    async fn list_domains(&self, offset: i64, limit: i32) -> Result<Vec<Domain>, StoreError>;
    async fn get_domain(&self, name: &str) -> Result<Domain, StoreError>;
    async fn domain_exists(&self, name: &str) -> Result<bool, StoreError>;
    async fn drop_domain(&self, name: &str) -> Result<(), StoreError>;
    async fn update_domain(
        &self,
        name: &str,
        patch: DomainPatch,
    ) -> Result<Domain, StoreError>;
}

pub struct DomainPatch {
    pub comment: PatchField<String>,
    pub property_removals: Vec<String>,
    pub property_updates: HashMap<String, String>,
    pub storage_type: PatchField<String>,
    pub storage_config: PatchField<serde_json::Value>,
    pub warehouse: PatchField<String>,
    pub owner: PatchField<String>,
}

#[async_trait]
pub trait NamespaceStore: Send + Sync {
    async fn create_namespace(
        &self,
        domain_name: &str,
        name: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    async fn list_namespaces(
        &self,
        domain_name: &str,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<Namespace>, StoreError>;

    async fn get_namespace(
        &self,
        domain_name: &str,
        name: &str,
    ) -> Result<Namespace, StoreError>;

    async fn namespace_exists(
        &self,
        domain_name: &str,
        name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_namespace(&self, domain_name: &str, name: &str) -> Result<(), StoreError>;

    async fn update_namespace(
        &self,
        domain_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;
}

#[async_trait]
pub trait AssetStore: Send + Sync {
    // 格式无关的 Asset 身份操作
    async fn create_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        asset_type: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<Asset, StoreError>;

    async fn get_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<Asset, StoreError>;

    async fn asset_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<bool, StoreError>;

    async fn drop_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(), StoreError>;

    async fn rename_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        new_name: &str,
    ) -> Result<(), StoreError>;

    async fn update_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
        comment: PatchField<String>,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Asset, StoreError>;
}

#[async_trait]
pub trait TabularStore: AssetStore {
    // 表资产特有操作，带 format 过滤
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
    ) -> Result<(Asset, TabularAsset), StoreError>;

    async fn list_tabular_assets(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
    ) -> Result<Vec<(Asset, TabularAsset)>, StoreError>;

    async fn get_tabular_asset(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<(Asset, TabularAsset), StoreError>;

    async fn get_tabular_asset_with_current_version(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: &str,
        name: &str,
    ) -> Result<(Asset, TabularAsset, Option<(AssetVersion, TabularAssetVersion)>), StoreError>;
}

#[async_trait]
pub trait VersionStore: Send + Sync {
    // 通用版本操作
    async fn create_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<AssetVersion, StoreError>;

    async fn get_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<AssetVersion, StoreError>;

    async fn list_versions(
        &self,
        asset_id: Uuid,
    ) -> Result<Vec<AssetVersion>, StoreError>;

    async fn get_latest_version(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<AssetVersion>, StoreError>;
}

#[async_trait]
pub trait TabularVersionStore: VersionStore {
    // 表版本扩展操作
    async fn create_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
        version_order: Option<i64>,
        previous_version_id: Option<Uuid>,
        metadata_location: &str,
        comment: Option<String>,
        properties: HashMap<String, String>,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError>;

    async fn get_tabular_version(
        &self,
        asset_id: Uuid,
        version_key: &str,
    ) -> Result<(AssetVersion, TabularAssetVersion), StoreError>;

    async fn list_tabular_versions(
        &self,
        asset_id: Uuid,
    ) -> Result<Vec<(AssetVersion, TabularAssetVersion)>, StoreError>;

    async fn get_latest_tabular_version(
        &self,
        asset_id: Uuid,
    ) -> Result<Option<(AssetVersion, TabularAssetVersion)>, StoreError>;
}

#[async_trait]
pub trait CasCommitStore: TabularStore {
    // Iceberg CAS commit
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
    ) -> Result<(), StoreError>;
}

#[async_trait]
pub trait UnifiedQueryStore: Send + Sync {
    // Unified API 跨格式聚合查询
    async fn list_assets_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        format: Option<&str>,
        name_filter: Option<&str>,
        offset: i64,
        limit: i32,
    ) -> Result<Vec<(Asset, Option<TabularAsset>)>, StoreError>;

    async fn get_asset_unified(
        &self,
        domain_name: &str,
        namespace_name: &str,
        name: &str,
    ) -> Result<(Asset, Option<TabularAsset>), StoreError>;
}
```

**trait 依赖关系**：
```
DomainStore (独立)
NamespaceStore (独立)
AssetStore (独立)
  └── TabularStore: AssetStore
        └── CasCommitStore: TabularStore
VersionStore (独立)
  └── TabularVersionStore: VersionStore
UnifiedQueryStore (独立，内部组合查询)
```

### 5.2 V2 → V3 方法签名迁移表

| V2 `CatalogStore` 方法 | V3 Trait | 签名变化 |
|------------------------|----------|----------|
| `create_namespace(name, comment, props)` | `NamespaceStore::create_namespace` | 增加 `domain_name` 参数 |
| `list_namespaces(offset, limit)` | `NamespaceStore::list_namespaces` | 增加 `domain_name` 参数 |
| `get_namespace(name)` | `NamespaceStore::get_namespace` | 增加 `domain_name` 参数 |
| `drop_namespace(name)` | `NamespaceStore::drop_namespace` | 增加 `domain_name` 参数 |
| `update_namespace(name, ...)` | `NamespaceStore::update_namespace` | 增加 `domain_name` 参数 |
| `create_asset(ns, format, name, ...)` | `TabularStore::create_tabular_asset` | 增加 `domain_name`，`format` 从枚举变字符串 |
| `list_assets(ns, format)` | `TabularStore::list_tabular_assets` | 增加 `domain_name`，`format` 可选 |
| `get_asset(ns, format, name)` | `AssetStore::get_asset` | 移除 `format`（格式无关） |
| `get_asset_with_tabular(ns, format, name)` | `TabularStore::get_tabular_asset` | 增加 `domain_name` |
| `drop_asset(ns, format, name)` | `AssetStore::drop_asset` | 移除 `format` |
| `rename_asset(ns, format, name, new)` | `AssetStore::rename_asset` | 移除 `format` |
| `create_version(ns, format, asset, ver, loc, prev)` | `TabularVersionStore::create_tabular_version` | 改为 `asset_id` 直接定位 |
| `load_version(ns, format, asset, ver_key)` | `TabularVersionStore::get_tabular_version` | 改为 `asset_id` 直接定位 |
| `list_versions(ns, format, asset)` | `TabularVersionStore::list_tabular_versions` | 改为 `asset_id` 直接定位 |
| `cas_update_metadata_location(...)` | `CasCommitStore::cas_update_metadata_location` | 增加 `domain_name`，`format` 变字符串 |
| `list_assets_unified(ns, format, name, ...)` | `UnifiedQueryStore::list_assets_unified` | 增加 `domain_name`，返回 `Option<TabularAsset>` |
| `get_asset_unified(ns, name, format)` | `UnifiedQueryStore::get_asset_unified` | 增加 `domain_name`，移除 `format` 参数 |

**关键设计点**：
- `AssetStore` 操作（`get_asset`、`drop_asset`、`rename_asset`）不再传 `format`，因为活动资产名在 Namespace 内唯一。
- `TabularStore` 操作保留 `format` 用于列表和查询过滤。
- 版本操作改为直接通过 `asset_id` 定位，不再通过 `(namespace, name)` 间接解析，减少重复查询。

### 5.3 SQL 集中管理

新建 `quasar/storage/src/queries.rs` 模块，按功能域组织 SQL 常量：

```rust
// quasar/storage/src/queries.rs

pub mod domain {
    pub const CREATE: &str = r#"
        INSERT INTO domains (name, comment, properties, storage_type, storage_config, warehouse, owner)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
    "#;

    pub const LIST: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
        FROM domains
        ORDER BY name
        LIMIT $1 OFFSET $2
    "#;

    pub const GET_BY_NAME: &str = r#"
        SELECT id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
        FROM domains WHERE name = $1
    "#;

    pub const EXISTS: &str = r#"
        SELECT EXISTS(SELECT 1 FROM domains WHERE name = $1)
    "#;

    pub const DELETE: &str = r#"
        DELETE FROM domains WHERE name = $1
    "#;

    pub const UPDATE: &str = r#"
        UPDATE domains
        SET comment = $1,
            properties = $2,
            storage_type = $3,
            storage_config = $4,
            warehouse = $5,
            owner = $6,
            updated_at = NOW()
        WHERE name = $7
        RETURNING id, name, comment, properties, storage_type, storage_config, warehouse, owner, created_at, updated_at
    "#;
}

pub mod namespace {
    pub const CREATE: &str = r#"
        INSERT INTO namespaces (domain_id, name, comment, properties)
        VALUES ($1, $2, $3, $4)
        RETURNING id, domain_id, name, comment, properties, created_at, updated_at
    "#;

    pub const LIST_BY_DOMAIN: &str = r#"
        SELECT n.id, n.domain_id, n.name, n.comment, n.properties, n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON n.domain_id = d.id
        WHERE d.name = $1
        ORDER BY n.name
        LIMIT $2 OFFSET $3
    "#;

    pub const GET_BY_NAME: &str = r#"
        SELECT n.id, n.domain_id, n.name, n.comment, n.properties, n.created_at, n.updated_at
        FROM namespaces n
        JOIN domains d ON n.domain_id = d.id
        WHERE d.name = $1 AND n.name = $2
    "#;

    // ... 其他查询
}

pub mod asset {
    pub const CREATE: &str = r#"
        INSERT INTO assets (namespace_id, name, asset_type, comment, properties)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, namespace_id, name, asset_type, comment, properties, deleted_at,
                  created_by, updated_by, created_at, updated_at
    "#;

    pub const GET_BY_NAME: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties, a.deleted_at,
               a.created_by, a.updated_by, a.created_at, a.updated_at
        FROM assets a
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3 AND a.deleted_at IS NULL
    "#;

    pub const GET_TABULAR_BY_NAME: &str = r#"
        SELECT a.id, a.namespace_id, a.name, a.asset_type, a.comment, a.properties, a.deleted_at,
               a.created_by, a.updated_by, a.created_at, a.updated_at,
               ta.asset_id, ta.format, ta.location, ta.metadata_location, ta.schema_snapshot,
               ta.created_at, ta.updated_at
        FROM assets a
        JOIN tabular_assets ta ON a.id = ta.asset_id
        JOIN namespaces ns ON a.namespace_id = ns.id
        JOIN domains d ON ns.domain_id = d.id
        WHERE d.name = $1 AND ns.name = $2 AND a.name = $3 AND a.deleted_at IS NULL
          AND a.asset_type = 'table' AND ta.format = $4
    "#;

    // ... 其他查询
}

pub mod version {
    pub const CREATE: &str = r#"
        INSERT INTO asset_versions (asset_id, version_key, version_order, previous_version_id, comment, properties)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
    "#;

    pub const GET_LATEST: &str = r#"
        SELECT id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
        FROM asset_versions
        WHERE asset_id = $1 AND version_order IS NOT NULL
        ORDER BY version_order DESC
        LIMIT 1
    "#;

    pub const LIST_BY_ASSET: &str = r#"
        SELECT id, asset_id, version_key, version_order, previous_version_id, comment, properties, created_at
        FROM asset_versions
        WHERE asset_id = $1
        ORDER BY version_order ASC NULLS LAST
    "#;

    // ... 其他查询
}
```

**设计约定**：
- 常量名使用 `SCREAMING_SNAKE_CASE`。
- 参数占位符用 `$1`, `$2`，在注释中说明参数顺序。
- 查询通过 `JOIN domains` / `JOIN namespaces` 从自然键（name）解析到 ID，避免 handler 层传递 ID。
- 特别复杂的 SQL 可考虑用 `include_str!("sql/xxx.sql")` 外置。

### 5.4 V3 初始 Schema

V3 不要求从旧数据库 schema 演进到新 schema，也不需要处理 V1/V2 旧数据迁移。工程应提供一份初次部署使用的数据库初始化建表脚本，用于新库初始化或测试库重建。

**脚本定位**：
- 初始化建表脚本是工程的一部分，不是文档附件。
- 初始化建表脚本不属于 migration，不应放在 `migrations/` 目录，也不应使用 `V1__`、`V2__`、`v3` 等开发版本或迁移序号命名。
- 推荐路径：`quasar/storage/src/schema/init.sql`。
- 如果服务启动时需要自动初始化数据库，应在 storage/server 层显式加载该 init 脚本；不要通过历史 migration runner 表达初始化语义。

**初始化内容**：
1. 创建 `asset_types` 和 `tabular_formats` 注册表。
2. 创建 `domains` 表。
3. 创建 `namespaces` 表，包含 `domain_id`。
4. 创建 `assets` 表，包含 `asset_type`、审计预留字段和活动资产唯一约束。
5. 创建 `tabular_assets` 表，包含 `format`、`location`、`metadata_location` 和 schema 快照。
6. 创建 `asset_versions` 表，包含 `version_key`、`version_order` 和 `previous_version_id`。
7. 创建 `tabular_asset_versions` 表。
8. 创建必要索引和触发器。

**旧脚本处理原则**：
- V3 初始 schema 确认后，旧版本 schema/migration 脚本如果不再被当前工程启动、测试或部署使用，应从工程活动目录中删除。
- 不建议为了“历史留存”在源码树中维护旧版本归档目录；历史内容由 Git 记录承担。
- 删除旧脚本前需同步调整启动初始化逻辑和集成测试清理逻辑，避免仍引用旧 `migrations/` 目录。

---

## 六、错误处理设计

### 6.1 StoreError 重构

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("conflict: {msg}")]
    Conflict { msg: String },

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("namespace not empty: {namespace}")]
    NamespaceNotEmpty { namespace: String },

    #[error("domain not empty: {domain}")]
    DomainNotEmpty { domain: String },

    #[error("database unavailable")]
    DatabaseUnavailable {
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("timeout: {operation}")]
    Timeout { operation: String },

    #[error("internal error: {msg}")]
    Internal {
        msg: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}
```

**新增/变更变体说明**：

| 变体 | 用途 | HTTP 映射 |
|------|------|-----------|
| `NotFound` | 资源不存在 | 404 |
| `AlreadyExists` | 资源已存在 | 409 |
| `Conflict` | 通用冲突（CAS 失败等） | 409 |
| `InvalidInput` | 输入参数非法 | 400 |
| `NamespaceNotEmpty` | Namespace 有下游资产 | 409 |
| `DomainNotEmpty` | Domain 有下游 Namespace | 409 |
| `DatabaseUnavailable` | 数据库连接失败 | 503 |
| `Timeout` | 操作超时 | 504 |
| `Internal` | 内部错误（客户端脱敏） | 500 |

### 6.2 客户端安全分离规则

**日志侧**（通过 `tracing::error!`）：
```rust
tracing::error!(
    error = ?source,
    "{}",
    msg
);
```

**客户端侧**（适配器层统一脱敏）：
- `StoreError::Internal { msg, source }` → 客户端收到 `"An internal error occurred"`，不暴露 `msg` 和 `source`。
- `StoreError::DatabaseUnavailable { .. }` → 客户端收到 `"Service temporarily unavailable"`（503）。
- `StoreError::Timeout { operation }` → 客户端收到 `"Request timeout"`（504）。
- 其他变体 → 原样传递 `msg`（已确保不含敏感信息）。

### 6.3 Adapter 错误映射

#### Unified Adapter

| StoreError | UnifiedErrorCode | HTTP |
|------------|-----------------|------|
| `NotFound` | `NamespaceNotFound` / `AssetNotFound` | 404 |
| `AlreadyExists` | `NamespaceAlreadyExists` / `AssetAlreadyExists` | 409 |
| `NamespaceNotEmpty` | `NamespaceNotEmpty` | 409 |
| `DomainNotEmpty` | `DomainNotEmpty` | 409 |
| `Conflict` | `Conflict` | 409 |
| `InvalidInput` | `InvalidInput` | 400 |
| `DatabaseUnavailable` | `ServiceUnavailable` | 503 |
| `Timeout` | `ServiceUnavailable` | 504 |
| `Internal` | `InternalError` | 500 |

#### Iceberg Adapter

| StoreError | IcebergError | HTTP |
|------------|-------------|------|
| `NotFound` | `NoSuchNamespaceException` / `NoSuchTableException` | 404 |
| `AlreadyExists` | `NamespaceAlreadyExistsException` / `TableAlreadyExistsException` | 409 |
| `NamespaceNotEmpty` | `NamespaceNotEmptyException` | 409 |
| `DomainNotEmpty` | `NamespaceNotEmptyException` | 409 |
| `Conflict` | `CommitFailedException` | 409 |
| `InvalidInput` | `BadRequestException` | 400 |
| `DatabaseUnavailable` | `ServiceUnavailable` | 503 |
| `Timeout` | `InternalServerError` | 504 |
| `Internal` | `InternalServerError` | 500 |

#### Lance Adapter

| StoreError | LanceError | HTTP |
|------------|-----------|------|
| `NotFound` | `NamespaceNotFound` / `TableNotFound` | 404 |
| `AlreadyExists` | `NamespaceAlreadyExists` / `TableAlreadyExists` | 409 |
| `NamespaceNotEmpty` | `NamespaceNotEmpty` | 409 |
| `DomainNotEmpty` | `NamespaceNotEmpty` | 409 |
| `Conflict` | 按上下文映射为具体 Lance 冲突错误，如 `TableVersionAlreadyExists` / `TableAlreadyExists` | 409 |
| `InvalidInput` | `InvalidInput` | 400 |
| `DatabaseUnavailable` | `ServiceUnavailable` | 503 |
| `Timeout` | `InternalError` | 504 |
| `Internal` | `InternalError` | 500 |

### 6.4 字符串匹配消除

| V2 代码位置 | V2 做法 | V3 替换 |
|-------------|---------|---------|
| `adapter/src/unified/error.rs:159` | `msg.contains("not empty")` | 直接 `match StoreError::DomainNotEmpty` / `StoreError::NamespaceNotEmpty` |
| `adapter/src/lance/error.rs:167` | `msg.starts_with("version ")` | `StoreError::AlreadyExists` 增加上下文标签或变体 |
| `storage/src/store.rs:213` | `RESTRICT_VIOLATION` 通用处理 | 按 SQL 状态码和当前删除对象直接构造 `DomainNotEmpty` / `NamespaceNotEmpty` |

---

## 七、Feature Flag 与依赖设计

### 7.1 V2 问题

V2 中 `object_store` 仅在 `iceberg` feature 启用时引入，导致：
- `UnifiedConfig.object_store` 被 `#[cfg(feature = "iceberg")]` 包裹。
- `unified` feature 隐式依赖 `iceberg` feature。
- `server` 层 `object_store` 也是 `optional = true`。

### 7.2 V3 简化设计

**Cargo.toml 变更**：

```toml
# workspace/Cargo.toml
[workspace.dependencies]
object_store = { version = "0.12", features = ["aws"] }  # 移除 optional

# adapter/Cargo.toml
[features]
default = ["lance", "iceberg", "unified"]
iceberg = []
lance = []
unified = []

[dependencies]
object_store = { workspace = true }  # 不再是 optional

# server/Cargo.toml
[features]
default = ["lance", "iceberg", "unified"]
lance = ["quasar-adapter/lance"]
iceberg = ["quasar-adapter/iceberg"]
unified = ["quasar-adapter/unified"]

[dependencies]
object_store = { workspace = true }  # 不再是 optional
```

**UnifiedConfig 变更**：
```rust
// V2（条件编译）：
#[derive(Clone, Default)]
pub struct UnifiedConfig {
    #[cfg(feature = "iceberg")]
    pub object_store: Option<Arc<dyn ObjectStore>>,
    #[cfg(feature = "iceberg")]
    pub s3_bucket: Option<String>,
}

// V3（无条件编译）：
#[derive(Clone, Default)]
pub struct UnifiedConfig {
    pub object_store: Option<Arc<dyn ObjectStore>>,
    pub s3_bucket: Option<String>,
}
```

### 7.3 Feature Flag 语义

| Feature | 控制内容 | 不控制内容 |
|---------|----------|-----------|
| `iceberg` | Iceberg REST 路由 + handler 编译 | `object_store` 可用性 |
| `lance` | Lance REST 路由 + handler 编译 | — |
| `unified` | Unified REST 路由 + handler 编译 | `iceberg`/`lance` 编译 |

---

## 八、协议适配器层变更

### 8.1 Iceberg 适配器变更

**文件变更清单**：

| 文件 | 变更内容 |
|------|----------|
| `iceberg/mod.rs` | 保持官方 `/v1/config` 与 `/v1/{prefix}/...` 路径；服务层可挂载到 `/iceberg` base path |
| `iceberg/namespace.rs` | Handler 提取 `prefix` 作为 Domain，传给 store |
| `iceberg/table.rs` | Handler 提取 Domain；表查询过滤 `format='iceberg'`；trait bound 改为 `TabularStore + CasCommitStore` |
| `iceberg/error.rs` | 增加 `DomainNotEmpty` / `NamespaceNotEmpty` 映射 |

**路由注册示例**（V3）：
```rust
pub fn routes<S>() -> Router<S>
where
    S: TabularStore + CasCommitStore + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/iceberg/v1/config", get(config::get_config))
        .route("/iceberg/v1/{prefix}/namespaces", get(namespace::list_namespaces).post(namespace::create_namespace))
        // ... 其他路由
}
```

### 8.2 Lance 适配器变更

**文件变更清单**：

| 文件 | 变更内容 |
|------|----------|
| `lance/mod.rs` | 保持官方 `/v1/namespace/{id}/...` 与 `/v1/table/{id}/...` 路径；服务层可挂载到 `/lance` base path |
| `lance/namespace.rs` | 从 `{id}` 解析 root / Domain / Namespace，传给 store |
| `lance/table.rs` | 从 `{id}` 解析 Domain / Namespace / Table；表查询过滤 `format='lance'`；trait bound 改为 `TabularStore + TabularVersionStore` |
| `lance/version.rs` | `version_order` 直接读取，禁止字符串解析；trait bound 改为 `TabularVersionStore` |
| `lance/error.rs` | 移除 `msg.starts_with("version ")` 字符串匹配 |

### 8.3 Unified 适配器变更

**文件变更清单**：

| 文件 | 变更内容 |
|------|----------|
| `unified/mod.rs` | 路由增加 `/domains/{domain}` 层级，trait bound 改为 `DomainStore + AssetStore + TabularStore + UnifiedQueryStore` |
| `unified/domain.rs` | 新增 Domain 管理 API；响应脱敏 `storage_config` |
| `unified/asset.rs` | 删除 `parse_format` 函数；Asset GET/DELETE/PATCH/rename 不再要求 `?format=`；响应增加 `asset_type` 字段 |
| `unified/version.rs` | 删除 `#[cfg(feature = "iceberg")]` 条件编译 |
| `unified/error.rs` | 使用 `match` 变体替代字符串匹配 |

### 8.4 Server 组装变更

**AppConfig 变更**：
```rust
#[derive(Clone, Default)]
pub struct AppConfig {
    #[cfg(feature = "lance")]
    pub lance: LanceConfig,
    #[cfg(feature = "iceberg")]
    pub iceberg: IcebergConfig,
    #[cfg(feature = "unified")]
    pub unified: UnifiedConfig,
}
```

**路由组装**：
```rust
pub fn create_app_with_config(pool: Pool, config: AppConfig) -> Router {
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    // ...

    #[cfg(feature = "iceberg")]
    {
        router = router
            .merge(quasar_adapter::iceberg::routes())
            .layer(Extension(config.iceberg));
    }

    // ... 其他 feature 条件编译

    router
        .with_state(store)
}
```

**State 类型问题**：V2 所有路由共享 `Arc<dyn CatalogStore>`。V3 拆分后，不同路由需要不同的 trait bound：
- Iceberg: `TabularStore + CasCommitStore`
- Lance: `TabularStore + TabularVersionStore`
- Unified: `AssetStore + TabularStore + UnifiedQueryStore`

**解决方案**：`PgCatalogStore` 实现所有 trait，路由各自声明需要的 trait bound。Axum 的 `State` 使用 `Arc<dyn TraitCombination>` 或泛型参数。

推荐方案：使用一个标记 trait 组合：
```rust
pub trait CatalogStore:
    DomainStore + NamespaceStore + AssetStore + TabularStore
    + VersionStore + TabularVersionStore + CasCommitStore + UnifiedQueryStore
{}

impl<T> CatalogStore for T where
    T: DomainStore + NamespaceStore + AssetStore + TabularStore
        + VersionStore + TabularVersionStore + CasCommitStore + UnifiedQueryStore
{}
```

各 adapter 根据实际需要声明更小的 trait bound。

---

## 九、V2 问题闭环矩阵

### 9.1 核心解决（11 个问题）

| # | 问题 | V3 解决方案 | 归属章节 | 需修改文件 |
|---|------|------------|---------|-----------|
| 1 | AssetFormat / AssetType 枚举硬编码 | 数据库取消 CHECK；Core 用 `String`；适配器保留内部枚举 | 3.3, 3.5 | `models.rs`, DDL |
| 2 | `asset_subtype` 语义模糊 | 删除 `asset_subtype`，`format` 下放 `tabular_assets.format` | 3.3, 3.5 | `models.rs`, DDL, `store.rs` |
| 3 | TabularAsset 硬绑定 | trait 拆分：`CatalogStore` → `AssetStore` + `TabularStore` | 5.1 | `store.rs` |
| 4 | CatalogStore 职责膨胀 | 拆分为 8 个专注 trait | 5.1 | `store.rs`, `lib.rs` |
| 6 | StoreError 粒度太粗 + 信息泄露 | `Internal { msg, source }`；新增 `DatabaseUnavailable`、`Timeout`；客户端脱敏 | 6.1, 6.2 | `error.rs`, `*/error.rs` |
| 7 | SQL 查询分散 | `storage/src/queries.rs` 常量模块 | 5.3 | 新建 `queries.rs`, `store.rs` |
| 8 | Asset 操作强制 format 参数 | 端点级隔离：端点路径隐含格式 | 4.1, 4.4 | `unified/asset.rs` |
| 10 | UnifiedConfig 与 iceberg feature 耦合 | `object_store` 改为公共依赖 | 7.2 | `Cargo.toml` x4, `unified/mod.rs` |
| 12 | `version_key` vs `version_order` 混用 | 明确语义边界；禁止字符串解析 fallback | 3.6, 5.1 | `models.rs`, `lance/version.rs` |
| 13 | 字符串匹配判断错误类型 | 新增 `DomainNotEmpty` / `NamespaceNotEmpty` 变体；SQL 状态码直接构造 | 6.1, 6.4 | `error.rs`, `*/error.rs`, `store.rs` |
| 14 | `lance_version_to_response` unwrap | `version_order` 直接读取，缺失时返回错误 | 3.6, 8.2 | `lance/version.rs` |

### 9.2 暂不解决（4 个问题）

| # | 问题 | 不解决理由 | 演进方向 |
|---|------|-----------|---------|
| 5 | 分页 OFFSET 性能隐患 | 不影响数据模型重构 | 后续版本引入 cursor-based 分页 |
| 9 | Unified API 不暴露 Asset 创建 | 资产创建涉及格式特有语义 | 后续版本设计格式无关请求格式 |
| 11 | Iceberg 实时读取对象存储 | 性能优化，不影响正确性 | 后续版本引入 metadata 缓存 |
| 15 | 对象存储与数据库非原子性 | 不影响正确性 | 后台清理任务（`docs/EVOLUTION.md`） |

---

## 十、实现顺序

### Phase 1：基础层（不碰 adapter）

1. **数据库初始化 Schema**：编写 `quasar/storage/src/schema/init.sql` 和对应加载逻辑，不处理旧 schema 迁移。
2. **Core 模型更新**：
   - 新增 `Domain` 结构体
   - 修改 `Namespace`（增加 `domain_id`）
   - 修改 `Asset`（删除 `asset_subtype`，`asset_type` 改为 `String`）
   - 修改 `AssetVersion`（增加 `previous_version_id`）
   - 删除 `TabularAssetVersion` 的 `previous_asset_version_id` 和 `previous_version_order`
3. **StoreError 重构**：新变体 + `Internal { msg, source }`
4. **queries.rs 模块**：创建文件，按 domain/namespace/asset/version 组织

**验收**：`cargo check` 通过，`core` crate 编译成功。

### Phase 2：存储层

5. **Store trait 拆分**：在 `core/src/store.rs` 定义 8 个 trait
6. **PgCatalogStore 重构**：
   - 实现所有 trait
   - 所有 SQL 改为引用 `queries.rs` 常量
   - 增加 Domain 相关方法
   - 修改 Namespace 方法（增加 Domain 上下文）
   - 修改 Asset 方法（移除 `format` 参数或改为 `&str`）
   - 修改版本方法（改为 `asset_id` 直接定位）
7. **集成测试更新**：适配新 schema 和 trait

**验收**：`cargo test -p quasar-storage` 全部通过。

### Phase 3：适配器层

8. **Feature flag 简化**：修改 4 个 `Cargo.toml`
9. **UnifiedConfig 解耦**：移除条件编译
10. **Iceberg 适配器**：
    - 路由增加 `{prefix}`
    - `GET /v1/config` 不带 `{prefix}`
    - Handler 提取 Domain
    - 表查询过滤 `format='iceberg'`
    - trait bound 改为 `TabularStore + CasCommitStore`
11. **Lance 适配器**：
    - 保持官方 `/v1/namespace/{id}` 和 `/v1/table/{id}` 路径
    - Handler 从 `{id}` 第一段解析 Domain
    - 版本响应使用 `version_order`
    - trait bound 改为 `TabularStore + TabularVersionStore`
12. **Unified 适配器**：
    - 新增 Domain 管理 API
    - Namespace/Asset 路由增加 `/domains/{domain}` 层级
    - Asset 操作移除 `format` 参数
    - 响应增加 `asset_type`
    - 错误映射使用 match 变体

**验收**：`cargo test -p quasar-adapter` 全部通过。

### Phase 4：集成与验证

13. **Server 组装更新**：`AppConfig`、`create_app_with_config`
14. **端到端测试**：覆盖跨格式隔离、名称冲突、容器删除
15. **文档最终检查**：确保所有 11 个核心问题有对应实现

**验收**：`cargo test` 全部通过，所有集成测试覆盖新增场景。

---

## 十一、测试策略

### 11.1 单元测试

| 模块 | 测试内容 | 验收标准 |
|------|----------|----------|
| `core::models` | Domain/Asset serde 序列化/反序列化；PatchField 边界行为（空值、Missing/Present/Set） | roundtrip 100% 成功；所有 PatchField 变体覆盖 |
| `core::error` | 所有 `StoreError` 变体的 `Display` 输出、错误构造 | 所有变体覆盖 |
| `core::store` | trait bound 编译验证 | `cargo build` 无编译错误 |

### 11.2 集成测试（storage 层）

| 场景 | 验证点 | 优先级 |
|------|--------|--------|
| Domain CRUD 完整流程 | 创建 → 列表 → 获取 → 更新 properties → 删除；重复创建返回 `AlreadyExists` | P0 |
| 初始化建表脚本 | 空数据库执行 `schema/init.sql` 成功；核心表、索引、触发器和注册表初始数据存在 | P0 |
| 初始化脚本边界 | `schema/init.sql` 不依赖旧 `migrations/` 目录；重复执行策略明确（幂等成功或明确失败） | P1 |
| Namespace CRUD | 带 Domain 上下文的生命周期；Namespace 名称在不同 Domain 可重复 | P0 |
| 非空 Domain 删除 | `RESTRICT` 阻止，返回 `DomainNotEmpty` | P0 |
| 非空 Namespace 删除 | `RESTRICT` 阻止，返回 `NamespaceNotEmpty` | P0 |
| 容器删除不级联 | 非空 Domain/Namespace 删除返回冲突；下游 Namespace/Asset/版本记录保持不变 | P0 |
| Asset 创建冲突 | 同名同 Namespace 不同 format 返回 `AlreadyExists`；同 Domain 不同 Namespace 同名允许 | P0 |
| Asset 删除级联 | 删除 Asset 连带清理 tabular_assets 和版本 | P0 |
| Lance 版本链 | version_key/version_order 映射、previous_version_id | P0 |
| 跨 Asset previous_version | 拒绝并返回 `Conflict` | P0 |
| 前驱版本删除行为 | 如后续暴露版本删除，删除前驱版本后下游版本的 `previous_version_id` 按 `ON DELETE SET NULL` 断开 | P2 |
| 并发版本创建 | 同一 Asset 同时创建两个相同 version_order，期望一个返回 `Conflict` | P1 |
| latest 查询 | 基于 `version_order DESC` | P0 |
| 触发器约束 | `tabular_assets` 只能引用 table Asset；previous version 不得跨 Asset | P1 |

### 11.3 集成测试（adapter 层）

| 场景 | 验证点 | 优先级 |
|------|--------|--------|
| Iceberg 端点隔离 | Iceberg 端点看不到 Lance 表（404，返回 `NoSuchTableException`） | P0 |
| Lance 端点隔离 | Lance 端点看不到 Iceberg 表（404，返回 `TableNotFound`） | P0 |
| Iceberg config 路径 | `GET /iceberg/v1/config` 不要求 `{prefix}`；返回正确 endpoints | P0 |
| Iceberg prefix 解析 | `GET /iceberg/v1/{prefix}/namespaces` 正确提取 Domain | P0 |
| Lance 官方路径 | 使用 `/lance/v1/table/prod$analytics$table/describe` 等官方路径；`{id}` 解析正确 | P0 |
| Lance 版本顺序验证 | `list_versions` 按 `version_order ASC NULLS LAST` 返回；latest 查询单独验证 DESC | P1 |
| Unified Asset 无 format | GET/DELETE/PATCH 不传入 format 参数；响应包含 `asset_type` | P0 |
| Unified Domain 路由 | Namespace/Asset 操作必须带 `/domains/{domain}` | P0 |
| Unified Domain 不存在 | `/domains/missing/namespaces` 返回 404 | P0 |
| Unified 列表过滤 | `?format=iceberg` 正确过滤；`?format=unknown` 返回 400 | P1 |
| Iceberg CAS commit 并发冲突 | 两个客户端同时 commit 同一表，第二个返回 409（`CommitFailedException`） | P1 |
| Lance 版本注册 | create_version 正确记录版本；metadata_location 可访问 | P0 |
| Lance `{id}` 解析边界 | 覆盖 root id、Domain id、Namespace id、Table id、段数过多、空段、URL 编码和 delimiter 冲突 | P0 |
| 错误映射完整性 | StoreError 所有变体正确映射到 Iceberg/Lance/Unified 协议错误格式 | P0 |

### 11.4 端到端测试

| 场景 | 验证点 | 优先级 |
|------|--------|--------|
| 完整 Iceberg 表生命周期 | 创建 namespace → 创建表 → 加载 → 提交 → 删除 | P0 |
| 完整 Lance 表生命周期 | 官方路径声明 → 注册版本 → 列表版本 → 删除 | P0 |
| 同名跨格式冲突 | Iceberg 创建 `events` 后 Lance 创建 `events` 返回 409 | P0 |
| Domain 隔离 | `prod/events` 和 `staging/events` 互不影响 | P0 |
| 多 Domain 并发操作 | 同时在 prod/staging 创建同名表，各自独立成功 | P1 |
| 非空 Domain 删除保护 | 删除非空 Domain 返回 409，且 Namespace/Asset/版本记录保持不变 | P0 |
| Lance-Iceberg 混合场景 | 同一 Domain 同时存在两种格式表，各自隔离工作 | P1 |
| 错误传播路径 | storage 层错误 → adapter 层 → HTTP 响应格式正确（content-type、error type、code） | P1 |

### 11.5 测试优先级定义

| 优先级 | 定义 | 执行时机 |
|--------|------|----------|
| P0 | 核心功能阻断性测试 | 每个 Phase 完成后 `cargo test` 必须通过 |
| P1 | 重要边界场景 | PR 合入主分支前必须通过 |
| P2 | 补充覆盖场景 | 发布前补充验证 |

### 11.6 测试环境与数据管理

- **数据库初始化**：使用 `schema/init.sql` 初始化测试数据库；初始化脚本本身必须有 P0 测试，验证空库执行成功、核心表/索引/触发器存在、旧 `migrations/` 目录不参与初始化。
- **测试隔离策略**：所有会写数据库的 storage/adapter/e2e 测试必须使用独立 database/schema，或在全局互斥锁下串行执行 `TRUNCATE ... CASCADE`；禁止并行测试共享同一数据库并在测试前 TRUNCATE。
- **并发测试模拟**：使用 `tokio::join!` 或 `tokio::spawn!` 模拟并发场景，验证 CAS 冲突检测
- **测试数据 fixtures**：建议定义标准测试数据（如预置 Domain `prod`/`staging`）用于端到端场景

---

## 十二、附录

### 附录 A：术语表

| 术语 | 定义 |
|------|------|
| **Domain** | 顶层存储与治理容器，不绑定表格式。对应 Iceberg 的 prefix 或独立租户边界。 |
| **Namespace** | Domain 内的业务组织单元。V3 为单层，后续可扩展嵌套。 |
| **Asset** | Namespace 内的实际资产身份。V3 支持 table，后续扩展 model/fileset/topic。 |
| **Tabular Asset** | 表类型资产，具有 format（iceberg/lance）、location、metadata_location 等字段。 |
| **端点级隔离** | 格式由 REST 端点路径隐含（`/iceberg/v1/...` vs `/lance/v1/...`），Domain 不绑定格式。 |
| **CAS Commit** | Compare-And-Swap 提交：Iceberg 中服务端校验当前 metadata_location 后原子更新。 |
| **版本注册** | Lance 中客户端先写 S3，再通知 Catalog Service 在 DB 中记录版本信息。 |
| **统一身份层** | `assets` 表仅保存所有资产共有的身份字段，类型特有字段进入扩展表。 |

### 附录 B：V2 → V3 文件变更映射

| 文件 | 操作 | 变更内容 |
|------|------|----------|
| `core/src/models.rs` | 修改 | 新增 `Domain`；修改 `Namespace`、`Asset`、`AssetVersion`；删除 `asset_subtype` 相关 |
| `core/src/store.rs` | 替换 | 单一 `CatalogStore` → 8 个拆分 trait |
| `core/src/error.rs` | 修改 | 新增变体，`Internal` 改为结构体变体 |
| `storage/src/store.rs` | 修改 | 实现新 traits，使用 `queries.rs`，增加 Domain 操作 |
| `storage/src/queries.rs` | **新增** | 所有 SQL 常量按功能域组织 |
| `storage/src/schema/init.sql` | **新增** | 初次部署数据库建表脚本，不属于 migration，不带产品版本或迁移序号 |
| `storage` schema 初始化加载逻辑 | **新增/修改** | 启动或测试初始化时加载 `schema/init.sql`；移除对旧 `migrations/` 目录的依赖 |
| `adapter/src/iceberg/mod.rs` | 修改 | 保持官方 `/v1/config` 与 `/v1/{prefix}/...` 路径 |
| `adapter/src/iceberg/namespace.rs` | 修改 | 提取 Domain 上下文 |
| `adapter/src/iceberg/table.rs` | 修改 | Domain + format 过滤；trait bound 变更 |
| `adapter/src/iceberg/error.rs` | 修改 | `DomainNotEmpty` / `NamespaceNotEmpty` 映射 |
| `adapter/src/lance/mod.rs` | 修改 | 保持官方 Lance REST Namespace 路径 |
| `adapter/src/lance/namespace.rs` | 修改 | 从 `{id}` 解析 Domain/Namespace |
| `adapter/src/lance/table.rs` | 修改 | 从 `{id}` 解析 Domain/Namespace/Table；format 过滤；trait bound 变更 |
| `adapter/src/lance/version.rs` | 修改 | `version_order` 直接读取 |
| `adapter/src/lance/error.rs` | 修改 | 移除字符串匹配 |
| `adapter/src/unified/mod.rs` | 修改 | 增加 `/domains/{domain}` 层级；trait bound 变更 |
| `adapter/src/unified/domain.rs` | **新增** | Domain 管理 API |
| `adapter/src/unified/asset.rs` | 修改 | 移除 format 参数要求；响应增加 `asset_type` |
| `adapter/src/unified/version.rs` | 修改 | 移除条件编译 |
| `adapter/src/unified/error.rs` | 修改 | match 变体替代字符串匹配 |
| `server/src/lib.rs` | 修改 | `AppConfig`、`create_app_with_config` |
| `server/src/main.rs` | 修改 | Domain 配置初始化 |
| `Cargo.toml` (workspace) | 修改 | `object_store` 移除 optional |
| `Cargo.toml` (adapter) | 修改 | feature flag 简化，`object_store` 非 optional |
| `Cargo.toml` (server) | 修改 | feature flag 简化，`object_store` 非 optional |

### 附录 C：遗留事项与未来工作

| 事项 | 当前状态 | 未来方向 | 参考 |
|------|---------|---------|------|
| Lance 版本一致性 | 客户端写 S3 + 事后注册，最终一致 | 关注 Lance RFC #5229，适配 Catalog-Aware Commits | V3_REQUIREMENTS.md 第七章 |
| 对象存储孤儿文件 | 后台清理任务 | 后台扫描 + 清理 | docs/EVOLUTION.md |
| metadata 缓存 | 实时读取 S3 | 引入 TTL 缓存（如 `moka`） | V3_PROBLEMS.md #11 |
| cursor-based 分页 | OFFSET 分页 | 替换为 cursor 分页 | V3_PROBLEMS.md #5 |
| Unified Asset 创建 | 返回 405 | 设计格式无关创建请求格式 | V3_PROBLEMS.md #9 |
| 嵌套 Namespace | 单层 | 后续版本设计路径解析和层级约束 | — |
| RBAC | 表预留 | 完整权限系统 | — |
| 软删除恢复 | 字段预留 | 实现资产恢复机制 | — |

---

## 十三、修订记录

### V1.4

- 修正测试策略，使容器删除测试与 `RESTRICT` 设计保持一致。
- 统一 Lance 版本列表顺序测试与 SQL 设计，明确 latest 查询单独验证。
- 修正数据库测试隔离策略，禁止并行测试共享数据库并在测试前 TRUNCATE。
- 补充 `schema/init.sql` 初始化脚本测试和 Lance `{id}` 解析边界测试。

### V1.3

- 明确数据库初始化 SQL 是工程的一部分，但不是 migration；推荐放在 `quasar/storage/src/schema/init.sql`。
- 明确初始化脚本命名不体现产品开发版本或迁移序号。
- 明确旧版本 schema/migration 脚本在 V3 不再使用后应从工程活动目录删除，由 Git 历史承担留存。

### V1.2

- 补充第四章所有 REST 端点的说明，明确每个端点的语义、上下文解析、主要约束和错误边界。

### V1.1

- 修正 REST API 设计，明确 Iceberg 使用官方 `{prefix}` 承载 Domain，Lance 使用官方 `{id}` 第一段承载 Domain。
- 新增 `V3_OFFICIAL_REST_API.md`，独立维护官方 API 来源、端点能力和 V3 实现范围。
- Unified API 增加显式 `/domains/{domain}` 层级，并新增 Domain 管理 API。
- 补充 DDL 与 Core Rust 模型字段注释，修复 `storage_config`、审计字段和时间字段不一致。
- 将表资产类型约束和版本前驱同 Asset 约束落实为数据库触发器。
- 修正错误映射、初始 schema 表述、适配器变更清单和测试策略中的 Domain/Lance 官方兼容描述。

### V1.0

- 初始整合版设计说明书。
