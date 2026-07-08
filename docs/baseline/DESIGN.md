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
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-adapter                           │
│  ┌───────────────────────┼───────────────────────────────┐   │
│  │    quasar-core        │   (traits + models + errors)  │   │
│  │  ┌────────────────────┴───────────────────────────┐   │   │
│  │  │ DomainStore | NamespaceStore | AssetTypeStore  │   │   │
│  │  │ AssetStore | VersionStore | TagStore          │   │   │
│  │  │ UnifiedQueryStore | CasCommitStore            │   │   │
│  │  │ CatalogStore | AdapterRegistry                │   │   │
│  │  └────────────────────────────────────────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
└──────────────────────────┬───────────────────────────────────┘
                           │
┌──────────────────────────┼───────────────────────────────────┐
│                     quasar-storage                           │
│              PgCatalogStore (impl all traits)                │
│            queries.rs (SQL 常量) | schema migrations         │
└──────────────────────────┬───────────────────────────────────┘
                           │
                    PostgreSQL + object store (S3/MinIO/...)
```

### 1.2 设计原则

1. **官方协议优先**：REST 路径、请求/响应字段和错误模型对齐 Iceberg 1.10.x / Lance REST Namespace 规范。
2. **声明即实现**：`/iceberg/v1/config` 的 `endpoints` 只返回实际已实现端点。
3. **元数据兼容优先**：Quasar 写出的 Iceberg metadata.json 能被 Spark Iceberg 1.10.x client 读回。
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
| `category` | TEXT | `tabular`、`view`、`model`、`agent`、`tool`、`generic`。 |
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
| `supports_cas_commit` | BOOL | 仅表格式。 |

#### `domains`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | `gen_random_uuid()`。 |
| `name` | TEXT UNIQUE | 全局唯一，用于 API 路径与权限边界。 |
| `tenant` | TEXT | 可选，供外部认证系统使用。 |
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
| `path` | TEXT | 层级路径，如 `analytics/teams/finance`。 |
| `depth` | INT | 路径深度。 |
| `comment` / `properties` | TEXT / JSONB | — |
| `created_at` / `updated_at` | TIMESTAMPTZ | — |
| | `UNIQUE(domain_id, path)` | 约束。 |

#### `assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | UUID PK | 治理能力统一引用该 ID。 |
| `namespace_id` | UUID FK→namespaces | `ON DELETE RESTRICT`。 |
| `name` | TEXT | 同一 Namespace 内活动资产名唯一。 |
| `asset_type` | TEXT FK→asset_types(name) | 资产类型。 |
| `format` | TEXT FK→formats(name) | 可选格式。 |
| `comment` | TEXT | 描述。 |
| `properties` | JSONB | 治理/管理面属性。 |
| `tags` | TEXT[] | 标签数组，或关联 `asset_tags`。 |
| `deleted_at` | TIMESTAMPTZ | 软删除时间，NULL 表示活动。 |
| `created_by` / `updated_by` | TEXT | 审计预留。 |
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
| `version_order` | BIGINT | 可比较顺序。 |
| `metadata` | JSONB | 版本元数据。 |
| `content_inline` | JSONB / BYTEA | 可选少量内联内容。 |
| `content_pointer` | TEXT | 可选外部指针。 |
| `previous_version_id` | UUID FK→asset_versions | `ON DELETE SET NULL`。 |
| `created_at` | TIMESTAMPTZ | — |

**约束**：
- `UNIQUE(asset_id, version_key)`。
- partial unique `uq_asset_versions_order` on `(asset_id, version_order) WHERE version_order IS NOT NULL`。

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
| `current_version_id` | UUID FK→asset_versions | — |

#### `view_assets`

| 字段 | 类型 | 说明 |
|------|------|------|
| `asset_id` | UUID PK FK→assets | `ON DELETE CASCADE`；触发器保证 `asset_type='view'`。 |
| `view_uuid` | UUID | Iceberg view uuid。 |
| `location` | TEXT | — |
| `current_version_id` | UUID FK→asset_versions | — |
| `metadata_location` | TEXT | — |

#### `model_assets` / `agent_assets` / `tool_assets`

- 根据具体资产类型定义，可存 `framework`、`runtime`、`signature`、`artifact_uri`、`capabilities`、`interface_schema`、`endpoint`、`auth_reference` 等字段。
- 或使用 `jsonb` 通用扩展策略，避免频繁 DDL。

---

## 4. Store Trait 设计

### 4.1 核心 trait

| Trait | 主要方法 |
|-------|----------|
| `DomainStore` | `create_domain`, `get_domain`, `list_domains`, `update_domain`, `delete_domain`. |
| `NamespaceStore` | `create_namespace`, `get_namespace`, `list_namespaces`, `delete_namespace`, `resolve_path`. |
| `AssetTypeStore` | `register_asset_type`, `register_format`, `list_asset_types`, `get_asset_type`, `get_format`. |
| `AssetStore` | `create_asset`, `get_asset`, `list_assets`, `update_asset`, `rename_asset`, `soft_delete_asset`, `restore_asset`, `hard_delete_asset`. |
| `VersionStore` | `create_version`, `get_version`, `list_versions`, `get_latest_version`, `delete_version`. |
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
- 覆盖 Namespace、Table、View、Transaction、Scan Planning、Metrics 端点（见需求文档 §6.1）。
- 错误格式为 Iceberg JSON error body。

### 5.2 Lance REST Namespace

- 路径前缀 `/lance/v1/...`。
- `{id}` 使用 `$` 分隔符序列化 Domain/Namespace/Table。
- 错误格式为 RFC-7807 Problem Details。

### 5.3 Unified API

- 路径前缀 `/unified/v1/...`。
- Domain/Namespace 管理。
- Asset 管理（CRUD、重命名、恢复、删除）。
- Version 管理。
- AssetType/Format 注册。
- 发现过滤。
- 错误格式为 RFC-7807 Problem Details，扩展 `code` 与 `request_id`。

---

## 6. 数据流与生命周期

### 6.1 Native protocol 数据流

1. 客户端调用原生端点。
2. Adapter 解析协议请求，解析 Domain/Namespace/Asset。
3. Adapter 校验资产类型与格式规则。
4. Adapter 在事务内调用 store trait 创建/更新资产及版本。
5. Adapter 返回协议响应。

### 6.2 Unified API 数据流

1. 客户端调用 Unified 端点。
2. Handler 解析 Domain 与 Namespace 路径。
3. Handler 调用通用 store trait。
4. Handler 返回 Unified 响应。

### 6.3 软删除生命周期

- `DELETE` 标记 `assets.deleted_at`。
- 软删除期间版本历史保留。
- `POST /restore` 清除 `deleted_at`（需名称唯一性仍满足）。
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

- Domain 配置 `storage_type`、`storage_config`、`warehouse`。
- 默认使用全局共享后端；Domain 可覆盖。
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
| `QUASAR_WAREHOUSE_PATH` | 否 | — | 默认 warehouse 路径。 |
| `QUASAR_WAREHOUSE` | 否 | `default` | 默认 warehouse 名称。 |
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

以下设计细节留待后续实现设计文档确定：

1. Store trait 与 Adapter 的具体签名、错误枚举、序列化类型。
2. Adapter 注册机制：编译时 feature flag vs 运行时注册。
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
