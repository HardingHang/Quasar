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

> 用途：资产类型的**全局注册表**。资产的 `asset_type` 必须在此注册；`category` 用于分类浏览；`validation_schema` 可选约束资产 properties；`extension_strategy` 决定类型特有字段的存储方式（`dedicated_table` 建扩展表，见 §3.3）；`supports_native_protocol` 供 Unified API 判定只读语义（见 §5.3）。内置 `table`、`view`，其余类型通过 Unified API 动态注册（FR-T3）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | TEXT PK | 唯一标识，如 `table`、`model`。 |
| `description` | TEXT | 描述。 |
| `category` | TEXT | `tabular`、`view`、`model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic`、`generic`。 |
| `validation_schema` | JSONB | 可选 JSON Schema。 |
| `extension_strategy` | TEXT | `jsonb` / `dedicated_table` / `reference_only`。 |
| `supports_native_protocol` | BOOL | 是否预期有原生协议。 |

#### `formats`

> 用途：格式注册表。格式与资产类型**正交**——同一类型可有多种格式（如 table 可以是 iceberg 或 lance）；资产的 `format` 必须在此注册，协议隔离按 `assets.format` 过滤。内置 `iceberg`、`lance`（FR-T3）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | TEXT PK | 唯一标识，如 `iceberg`、`onnx`。 |
| `description` | TEXT | 描述。 |
| `mime_type` | TEXT | 可选。 |
| `serialization_hint` | TEXT | 可选，如 `json`、`protobuf`。 |

#### `domains`

> 用途：顶层组织边界（多团队/多环境隔离）。名称全局唯一并用于 API 路径（Iceberg `{prefix}`、Lance id 首段、Unified 路径段）。`storage_type`/`storage_config`/`warehouse` 为 Domain 级对象存储配置，未指定时回落全局环境变量（解析优先级见 §8.1）；`storage_config` 不得明文存 credential。

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

> 用途：Domain 内的**层级**命名空间，资产的容器。`path` 物化完整层级路径（段间 `/` 分隔，每段为 URL-safe slug），`depth` 冗余存储深度以支持前缀查询（FR-N3）；创建时隐式创建中间节点（FR-N2）；`ON DELETE RESTRICT` + 应用层非空校验保证只能删除空 Namespace。

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

> 用途：所有资产的**统一身份**。治理操作（软删除/恢复/标签）统一引用 `id`（重命名不影响引用）；`format` 上移到本表后，协议隔离（Iceberg/Lance 各自只认自己的格式）按此列过滤，无需 JOIN 扩展表；`current_version_key` 指向当前版本，是 CAS 成功后同步更新的结果而非校验锚点（见 §6.4）；`deleted_at` 为软删除标记，软删除期间名字被释放（部分唯一索引仅约束活动资产）。

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

> 用途：版本历史，**不可变**（无 `updated_at`）。原生协议 commit 时镜像写入（FR-V2）；`version_key` 为格式原生标识（Iceberg = metadata 序号如 `00001`，Lance = 原生版本号）；`content_pointer` 指向对象存储中的元数据文件（Iceberg 为完整 metadata_location，Lance 为 manifest 路径）；`previous_version_id` 构成版本链——`ON DELETE RESTRICT` 防止删除被后继引用的版本，单根约束保证每资产至多一个根版本；CAS 路径下前驱由存储层自动链接（见 §6.4 S4）。

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

> 用途：资产标签（治理标注），供 Discovery 按标签过滤（FR-Q5）。标签写入不触碰原生协议状态，因此对所有资产（含原生协议资产）开放。

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID FK→assets | `ON DELETE CASCADE`。 |
| `tag` | TEXT | — |
| `created_at` | TIMESTAMPTZ | — |
| | `PRIMARY KEY(asset_id, tag)` | — |

### 3.3 类型特定扩展表示例

#### `tabular_assets`

> 用途：`table` 类型的扩展表（`extension_strategy = dedicated_table`）。`metadata_location` 是当前内容指针的**热路径缓存**——load 时无需 join 版本表，也是 CAS 的校验锚点（见 §6.4）；`schema_snapshot` 为当前 schema 缓存。`asset_type='table'` 由应用层校验（不使用数据库触发器）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；应用层校验 `asset_type='table'`。 |
| `location` | TEXT | 表根路径。 |
| `metadata_location` | TEXT | 当前 metadata.json 指针。 |
| `schema_snapshot` | JSONB | 当前 schema 缓存。 |

#### `view_assets`

> 用途：`view` 类型的扩展表。`view_uuid` 对应 Iceberg view metadata 中的 uuid（commit 时校验 `assert-view-uuid`）；`metadata_location` 同为当前内容指针缓存。`asset_type='view'` 由应用层校验。

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

### 3.5 协议实现层辅助表（Iceberg adapter 私有）

以下三张表是 **Iceberg adapter 的实现层私表**，用于支撑 Iceberg REST spec 的 stage-create、metrics、purge 能力。它们**不属于通用模型**：不挂 `assets` 外键（`iceberg_scan_metrics_reports.asset_id` 除外）、不被其他 adapter 感知、Schema 由 Iceberg adapter 私有演进。它们与通用表包含在同一个迁移版本中（见 §8.4）。

#### `iceberg_staged_tables`

用途：Iceberg `stage-create` 的暂存记录。客户端以 `stage-create=true` 创建表时，元数据先暂存于此（默认 24 小时过期），首个 commit 到达时转正为正式资产；超时未 commit 的记录可被清理。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `domain_name` / `namespace_path` / `table_name` | TEXT | 弱引用（无 FK），按名定位暂存表；`namespace_path` 为层级路径。 |
| `table_uuid` | UUID | 暂存表的 table-uuid，commit 转正时校验。 |
| `location` | TEXT | 表根路径。 |
| `metadata_location` | TEXT | 暂存 metadata.json 指针。 |
| `metadata_json` | JSONB | 暂存的完整元数据。 |
| `properties` | JSONB | 表属性。 |
| `expires_at` | TIMESTAMPTZ | 过期时间；过期记录视为不存在。 |
| `created_at` | TIMESTAMPTZ | — |
| | `UNIQUE(domain_name, namespace_path, table_name)` | 同名暂存表唯一。 |

#### `iceberg_scan_metrics_reports`

用途：Iceberg `POST .../tables/{table}/metrics` 端点上报的 scan metrics 记录，仅追加、供运维分析。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `asset_id` | UUID FK→assets | `ON DELETE SET NULL`；资产删除后报告保留。 |
| `domain_name` / `namespace_path` / `table_name` | TEXT | 上报时的名字快照（资产可能随后重命名）。 |
| `report` | JSONB | 客户端上报的原始报告。 |
| `user_agent` | TEXT | 客户端标识。 |
| `created_at` | TIMESTAMPTZ | — |

#### `iceberg_purge_operations`

用途：`DROP TABLE ...?purgeRequested=true` 时记录 purge 操作状态，用于跟踪/补偿对象存储文件的清理过程（catalog 行删除与对象文件删除无法同事务）。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | 操作 ID。 |
| `domain_name` / `namespace_path` / `table_name` / `table_location` | TEXT | 被 purge 表的定位信息快照。 |
| `metadata_location` | TEXT | 删除时的 metadata.json 指针。 |
| `status` | TEXT | `started` / `catalog_dropped` / `completed` / `failed`。 |
| `error_message` | TEXT | 失败原因。 |
| `requested_at` / `completed_at` | TIMESTAMPTZ | — |

---

## 4. Store Trait 设计

### 4.1 核心 trait

| Trait | 主要方法 |
|-------|----------|
| `DomainStore` | `create_domain`, `get_domain`, `list_domains`, `update_domain`, `delete_domain`. |
| `NamespaceStore` | `create_namespace`, `get_namespace`, `list_namespaces`, `delete_namespace`, `resolve_path`. |
| `AssetTypeStore` | `register_asset_type`, `register_format`, `list_asset_types`, `get_asset_type`, `get_format`. |
| `AssetStore` | `create_asset`, `get_asset`, `list_assets`, `update_asset`, `rename_asset`, `soft_delete_asset`, `restore_asset`, `hard_delete_asset`. |
| `VersionStore` | `create_version`, `get_version`, `list_versions`, `get_latest_version`。`create_version` 自动更新 `assets.current_version_key`；`get_latest_version` 基于 `current_version_key` 查询。`delete_version` 基线延后、无调用方（见需求文档 §10）。 |
| `TagStore` | `add_tag`, `remove_tag`, `list_tags`, `list_assets_by_tag`. |
| `UnifiedQueryStore` | `query_assets` with filters (domain, namespace, type, format, tags, properties). |
| `CasCommitStore` | `compare_and_swap_pointer`：以当前内容指针为锚点的 CAS 版本提交（见 §6.4）。 |
| `CatalogStore` | Marker trait combining all stores. |
| `AdapterRegistry` | Register and route native protocol adapters. |

> **协议扩展 trait**：通用 `CatalogStore` 之外，原生 adapter 可拥有协议专属的扩展 trait。当前唯一实例是 `IcebergCatalogStore` = `CatalogStore` + 6 个 Iceberg 扩展 trait：`IcebergStagingStore`（stage-create 暂存/转正）、`IcebergRegisterStore`（register 外部表）、`IcebergMetricsStore`（scan metrics 记录）、`IcebergPurgeStore`（purge 操作跟踪）、`IcebergTransactionStore`（多表事务原子 commit）、`IcebergViewStore`（view 生命周期）。这些能力无法用通用 trait 组合实现（如多表事务要求单事务内多表 CAS 整体回滚），其操作落在 §3.5 的协议私表上。扩展 trait 与通用 trait 遵循同样的错误、分页与支撑类型约定。

### 4.2 Adapter 契约

- 原生 adapter 声明其处理的 `(asset_type, format)` 集合。
- 通过 `AdapterRegistry` 注册并接收 `Arc<dyn CatalogStore>`。
- 负责协议请求解析、请求校验、store trait 调用、协议响应构造。
- 负责将内部 `CatalogError` 映射为协议特定错误格式。

> Trait 签名见 §4.3，错误枚举定义见 §7.4。adapter 注册机制（编译时 vs 运行时）与序列化类型的最终取舍仍在后续实现设计文档中确定（见 §10）。

### 4.3 Trait 签名

以下 trait 定义位于 `core` crate，是 `storage` 实现与 `adapter` 调用的唯一契约。所有 trait 通过 `async_trait` 声明，错误统一返回 `CatalogError`（定义见 §7.4）。

```rust
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
    async fn create_namespace(&self, domain: &str, path: &str, input: CreateNamespace) -> Result<Namespace, CatalogError>;
    async fn get_namespace(&self, domain: &str, path: &str) -> Result<Namespace, CatalogError>;
    /// prefix 为路径前缀过滤（层级查询）；None 列出 Domain 下全部。
    async fn list_namespaces(&self, domain: &str, prefix: Option<&str>, offset: u64, limit: u64) -> Result<Vec<Namespace>, CatalogError>;
    async fn update_namespace(&self, domain: &str, path: &str, patch: NamespacePatch) -> Result<Namespace, CatalogError>;
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
    async fn get_asset_by_name(&self, domain: &str, namespace: &str, name: &str) -> Result<Asset, CatalogError>;
    async fn list_assets(&self, filter: AssetFilter, offset: u64, limit: u64) -> Result<Vec<Asset>, CatalogError>;
    async fn update_asset(&self, id: Uuid, patch: AssetPatch) -> Result<Asset, CatalogError>;
    async fn rename_asset(&self, id: Uuid, new_name: &str) -> Result<Asset, CatalogError>;
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
    async fn get_version(&self, asset_id: Uuid, version_key: &str) -> Result<AssetVersion, CatalogError>;
    async fn list_versions(&self, asset_id: Uuid, offset: u64, limit: u64) -> Result<Vec<AssetVersion>, CatalogError>;
    /// 基于 assets.current_version_key 查询当前版本。
    async fn get_latest_version(&self, asset_id: Uuid) -> Result<AssetVersion, CatalogError>;
}

/// 资产标签（治理标注）。标签写入不触碰原生协议状态，对所有资产开放。
#[async_trait]
pub trait TagStore: Send + Sync {
    async fn add_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn remove_tag(&self, asset_id: Uuid, tag: &str) -> Result<(), CatalogError>;
    async fn list_tags(&self, asset_id: Uuid) -> Result<Vec<String>, CatalogError>;
    async fn list_assets_by_tag(&self, domain: &str, tag: &str, offset: u64, limit: u64) -> Result<Vec<Asset>, CatalogError>;
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
    async fn register_asset_type(&self, input: RegisterAssetType) -> Result<AssetType, CatalogError>;
    async fn register_format(&self, input: RegisterFormat) -> Result<Format, CatalogError>;
    async fn list_asset_types(&self, category: Option<&str>, offset: u64, limit: u64) -> Result<Vec<AssetType>, CatalogError>;
    async fn get_asset_type(&self, name: &str) -> Result<AssetType, CatalogError>;
    async fn get_format(&self, name: &str) -> Result<Format, CatalogError>;
}

pub trait CatalogStore: DomainStore + NamespaceStore + AssetTypeStore + AssetStore + VersionStore + TagStore + UnifiedQueryStore + CasCommitStore + Send + Sync {}
```

**分页约定**：所有列表方法统一 `(offset: u64, limit: u64)` 入参并返回当前页条目。当返回条数等于 `limit` 时，调用方（adapter）生成 `next_token = offset + 返回条数`；否则无下一页。token 的编解码在 adapter 协议层完成（见 §5.4），存储层不做 COUNT 查询。

#### 支撑类型

过滤、补丁与创建输入类型同样定义在 `core` crate。分页不使用独立类型：列表方法统一 `(offset, limit)` 入参（见上方分页约定）。

```rust
/// AssetStore::list_assets 的存储层过滤条件，用于已知 Domain/Namespace 上下文的资产列举。
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

/// 三态补丁字段：区分"不修改"与"清除为 NULL"，避免 `Option<Option<T>>` 的歧义。
pub enum PatchField<T> {
    /// 更新为指定值。
    Set(T),
    /// 清除该字段（置 NULL）。
    Unset,
    /// 保持不变（请求中未携带该字段）。
    NoChange,
}

/// Asset 的可补丁字段（comment、properties）。
pub struct AssetPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

// ---------- 创建与注册输入类型 ----------

/// 创建 Domain 的输入。未指定 warehouse 时使用全局 QUASAR_WAREHOUSE_PATH。
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
pub struct DomainPatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
    pub storage_type: PatchField<String>,
    pub storage_config: PatchField<serde_json::Value>,
    pub warehouse: PatchField<String>,
}

/// 创建 Namespace 的输入（Domain 与路径在 trait 方法参数中给出）。
pub struct CreateNamespace {
    pub comment: Option<String>,
    pub properties: Option<serde_json::Value>,
}

/// Namespace 的可补丁字段。
pub struct NamespacePatch {
    pub comment: PatchField<String>,
    pub properties: PatchField<serde_json::Value>,
}

/// 创建 Asset 的输入（由原生 adapter 调用）。
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
pub struct RegisterFormat {
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    /// 序列化提示，如 json、protobuf。
    pub serialization_hint: Option<String>,
}
```

> 领域模型类型（`Domain`、`Namespace`、`Asset`、`AssetVersion`、`AssetType`、`Format`）与 §3.2 表结构一一对应，此处不再重复定义。

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
- Domain/Namespace 管理：完整生命周期（Domain 无原生协议可管，Namespace 非资产，均由 Unified 负责）。
- Asset：基线不假设非原生协议资产，所有资产生命周期归原生协议，Unified 仅提供**只读**（列表/按名/按 ID 获取）；唯一写例外是软删除恢复 `POST /assets/{asset_id}/restore` 与标签管理（治理操作，不触碰原生状态）。
- Version：只读（列表/获取）；版本由原生 adapter 镜像，基线不提供版本创建/删除端点。
- AssetType/Format 注册与管理。
- 发现过滤：按 Domain、Namespace 前缀、类型、格式、标签、属性组合过滤（见需求文档 §4.6）。
- 错误格式为 RFC-7807 Problem Details，扩展 `code` 与 `request_id`。

寻址风格（集合/成员分离）：单资产操作统一走 `/unified/v1/assets/{asset_id}`；层级名路径仅用于创建（Namespace/Domain）与按名查找（`GET .../namespaces/{ns}/assets/{asset}`）；`GET /unified/v1/assets` 为跨 Namespace 的 Discovery 集合查询。同 Namespace 内活动资产名唯一（`uq_assets_active_name`），保证按名查找结果唯一。

具体端点定义见需求文档 §6.2。

### 5.4 统一分页

所有协议的列表端点统一使用**令牌分页**（与 Iceberg/Lance 上游 spec 一致）：

- 请求：`?pageToken=<token>&pageSize=<n>`；`pageSize` 有服务端上限（超出按上限截断）。
- 响应：`{ "items": [...], "next_page_token": "<token>" }`；无下一页时省略 `next_page_token`。
- token 对客户端不透明；当前实现为 offset 编码（`next_token = offset + 本页条数`，仅当本页条数等于 pageSize 时产生）。
- 存储层对应 `(offset, limit)` 入参（见 §4.3 分页约定），不做 COUNT 查询；token ↔ offset 的编解码在 adapter 协议层完成。

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

#### CAS 锚点：当前内容指针

版本提交的乐观锁锚点是**当前内容指针**（tabular 资产即 `tabular_assets.metadata_location`；未来的通用资产为当前版本的 `content_pointer`），而不是 `current_version_key`：

- 指针是协议语义的自然锚点——Iceberg 客户端 load 响应中即携带 `metadata-location`，且每次 commit 产生全局唯一的新指针（`NNNNN-uuid.metadata.json` 不会复用），指针相等 ⇔ 状态相等，天然防 ABA。
- `current_version_key` 不是校验锚点，而是 CAS 成功后**同步更新的结果**。

#### 锁外校验 + 锁内 CAS 的分工

一次 Iceberg commit 的完整时序：

```
load(L1)                      -- adapter 按名解析资产，读到当前指针 L1
  → 读 L1 的 metadata.json    -- 不可变文件，对象存储
  → 校验 assert requirements  -- 锁外进行（见下）
  → 构建新 metadata，写入 L2  -- 对象存储
  → BEGIN
  → 锁内重读指针并比对        -- 线性化点
  → 插镜像版本 / 更新指针
  → COMMIT
```

- **assert 校验在锁外**：requirements（如 `assert-ref-snapshot-id`）针对不可变的 L1 元数据校验，在写入 L2 之前完成，失败即快速返回 `CommitFailedException`。不进锁的原因：持 DB 行锁做对象存储网络 I/O 会把锁持有时间从毫秒拉到百毫秒级，高并发下导致锁竞争与连接池耗尽。
- **指针 CAS 在锁内**：锁内重读指针与 L1 比对，通过则证明被校验的 L1 在此刻仍是当前状态——锁外校验结果在线性化点上有效。比对失败则回滚，客户端重试（重新 load 走一遍）。
- 不能只用 assert snapshot 代替指针 CAS：requirements 可选（可为空），且 snapshot id 不等于完整状态（仅改 properties 的 commit 产生新指针但 snapshot 不变）。

#### compare_and_swap_pointer 事务（S1–S6）

`compare_and_swap_pointer(asset_id, expected_pointer, new_version)` 在事务内执行（`$1`=asset_id，`$2`=expected_pointer，`$3`=新 version_key，`$4`=新 content_pointer，`$5`=version_properties，`$6`=新 schema_snapshot）：

```sql
BEGIN;

-- S1 锁资产行（同时取出当前 current_version_key）
SELECT id, current_version_key FROM assets
WHERE id = $1 AND deleted_at IS NULL
FOR UPDATE;
-- 0 行 → NotFound

-- S2 读当前指针（锁已持有，无需再锁 tabular_assets）
SELECT metadata_location FROM tabular_assets WHERE asset_id = $1;

-- S3 应用层比对：S2 结果 ≠ $2 → ROLLBACK → Conflict（Iceberg 映射 CommitFailedException）

-- S4 插入镜像版本，previous_version_id 自动链接当前版本
--    （current_version_key 为 NULL 时子查询得 NULL → 根版本，受单根约束保护）
INSERT INTO asset_versions
    (id, asset_id, version_key, version_properties, content_pointer, previous_version_id)
VALUES (
    gen_random_uuid(), $1, $3, $5, $4,
    (SELECT id FROM asset_versions
     WHERE asset_id = $1 AND version_key = <S1 返回的 current_version_key>)
);

-- S5 更新当前版本指针
UPDATE assets SET current_version_key = $3, updated_at = now() WHERE id = $1;

-- S6 更新 tabular 热路径缓存（load 无需 join）
UPDATE tabular_assets SET metadata_location = $4, schema_snapshot = $6 WHERE asset_id = $1;

COMMIT;
```

要点：

- **锁 assets 主键行即可**：同一资产的所有 commit 路径（单表、staged、多表事务中的该表分支）第一步都锁同一行，天然串行化；`tabular_assets` 与 `current_version_key` 的所有写入方都必须先持有这把锁（存储层纪律），故无需第二把锁。
- `previous_version_id` 在 S4 自动链接，adapter 无需显式传递，版本链自然形成。
- Iceberg 镜像版本：`version_key` = metadata 序号（`00001`…），`content_pointer` = 完整 metadata_location；建表（含 staged 转正）走同一套逻辑插入首个版本。
- 事务失败时已写入对象存储的 L2 成为孤儿文件——对象存储无事务，这是固有代价；L2 文件名含 uuid 天然避免冲突，重试即可。
- **Lance 不走 CAS**：版本创建由客户端显式给版本号，直接 `INSERT asset_versions` + S5/S6，重复版本靠 `UNIQUE(asset_id, version_key)` 兜底返回 `409`。

#### 事务隔离级别

- 统一使用 `READ COMMITTED`（PostgreSQL / openGauss 默认级别）。
- CAS commit、软删除/恢复、多表事务均通过 `SELECT ... FOR UPDATE` 显式锁定目标行，结合 RC 的语句级最新读，避免快照过期导致的 CAS 失败。
- 多表事务按 `asset_id` 升序依次锁定，防止死锁。
- `update_*` 的 properties 合并使用单语句 JSONB 运算 `(properties - removals) || updates`，行锁天然串行化，无 lost-update。

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
