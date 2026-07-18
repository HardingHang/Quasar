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
| `owner` | TEXT | 所有者标识。 |
| `created_at` / `updated_at` | TIMESTAMPTZ | 时间戳。 |

#### `namespaces`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | — |
| `domain_id` | UUID FK→domains | `ON DELETE RESTRICT`。 |
| `path` | TEXT | 层级路径，如 `analytics/teams/finance`；路径段命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。 |
| `depth` | INT | 路径深度。 |
| `comment` / `properties` | TEXT / JSONB | — |
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
| `current_version_key` | TEXT | 当前版本的原生版本标识，对应 `asset_versions.version_key`；通过 `(id, current_version_key)` 可 join 到 `asset_versions`。 |
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
| `version_order` | BIGINT NOT NULL | 可比较顺序，用于版本历史排序。 |
| `version_properties` | JSONB | 版本属性。记录与格式无关的通用信息，如 commit message、作者、操作类型、自定义属性。不是 `content_pointer` 指向的格式原生元数据文件。 |
| `content_inline` | JSONB | 可选少量内联内容（如小 JSON/YAML 配置）。实际工件文件通过 `content_pointer` 指向外部对象存储。 |
| `content_pointer` | TEXT | 可选外部指针，指向对象存储中的实际工件文件。 |
| `previous_version_id` | UUID FK→asset_versions | `ON DELETE RESTRICT`；防止删除被后继版本引用的版本，保护版本链完整性。 |
| `created_at` | TIMESTAMPTZ | — |

**约束**：
- `UNIQUE(asset_id, version_key)`。
- `UNIQUE(asset_id, version_order)`。
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
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；触发器保证 `asset_type='table'`。 |
| `location` | TEXT | 表根路径。 |
| `metadata_location` | TEXT | 当前 metadata.json 指针。 |
| `schema_snapshot` | JSONB | 当前 schema 缓存。 |

#### `view_assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；触发器保证 `asset_type='view'`。 |
| `view_uuid` | UUID | Iceberg view uuid。 |
| `location` | TEXT | — |
| `metadata_location` | TEXT | — |

#### 其他内置类型的扩展表

- `model_assets`、`agent_assets`、`tool_assets`、`mcp_server_assets`、`fileset_assets`、`topic_assets` 等所有内置资产类型均使用 `dedicated_table` 扩展策略，各自拥有独立的类型扩展表。
- 具体扩展表 Schema 在实际设计该资产类型接入时确定；基线阶段以 `tabular_assets` 和 `view_assets` 为示例。

---

## 4. Store Trait 设计

### 4.1 核心 trait

| Trait | 主要方法 |
|-------|----------|
| `DomainStore` | `create_domain`, `get_domain`, `list_domains`, `update_domain`, `delete_domain`. |
| `NamespaceStore` | `create_namespace`, `get_namespace`, `list_namespaces`, `delete_namespace`, `resolve_path`. |
| `AssetTypeStore` | `register_asset_type`, `register_format`, `list_asset_types`, `get_asset_type`, `get_format`. |
| `AssetStore` | `create_asset`, `get_asset`, `list_assets`, `update_asset`, `rename_asset`, `soft_delete_asset`, `restore_asset`, `hard_delete_asset`. |
| `VersionStore` | `create_version`, `get_version`, `list_versions`, `get_latest_version`, `delete_version`（`delete_version` 仅对非原生协议资产开放；原生协议资产版本不可删除）。`create_version` 时自动将 `assets.current_version_key` 更新为最新版本的 `version_key`；`get_latest_version` 基于 `assets.current_version_key` 查询，若其为 `NULL` 则返回 `NotFound`。 |
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

> **延期**：具体 trait 签名、错误枚举、序列化类型、adapter 注册机制（编译时 vs 运行时）将在后续实现设计文档中确定。

---

## 5. API 设计

### 5.1 Iceberg REST Catalog

- 路径前缀 `/iceberg/v1/...`。
- `{prefix}` 映射为 Domain 名。
- 覆盖 Namespace、Table、View、Transaction、Metrics 端点（见需求文档 §7.1）。
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
- 发现过滤。
- 错误格式为 RFC-7807 Problem Details，扩展 `code` 与 `request_id`。

---

## 6. 数据流与生命周期

### 6.1 Native protocol 数据流

1. 客户端调用原生端点。
2. Adapter 解析协议请求，解析 Domain/Namespace/Asset。
3. Adapter 校验资产类型与格式规则。
4. Adapter 在事务内调用 store trait：创建/更新资产身份与类型扩展字段，将原生版本镜像写入 `asset_versions`（含 `version_key`、`version_order`、`content_pointer` 等），并将 `assets.current_version_key` 更新为最新版本的 `version_key`。
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

---

## 9. Server 组装与配置

### 9.1 配置项

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_DATABASE_URL` | 是 | `postgres://postgres:postgres@localhost:5432/quasar` | PostgreSQL 连接串。 |
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

---

## 10. 待明确与延期事项

以下设计细节留待后续实现设计文档确定。本节关注**实现层面未明确的技术方案**；需求层面的功能与约束延期事项见 `docs/baseline/REQUIREMENTS.md` §10。

1. Store trait 与 Adapter 的具体签名、错误枚举、序列化类型。
2. Adapter 注册机制：采用编译时 feature flag；运行时动态插件作为后续版本可选方向。
3. 层级 Namespace 的查询实现：物化路径 vs `ltree`。
4. 内联内容大小限制与对象存储卸载策略。
5. 软删除保留窗口与硬删除策略。
6. 从旧里程碑 Schema 到新通用 Schema 的迁移脚本。
7. model、agent、tool、mcp_server 等资产类型的扩展表字段与原生协议。

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
