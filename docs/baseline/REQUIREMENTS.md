# Quasar 项目级需求规格说明书

> **项目**：Quasar —— 面向数据与 AI 系统的统一元数据平面
> **状态**：已确认的需求基线
> **日期**：2026-07-08
> **范围**：项目级、与版本无关；不绑定任何里程碑（如 V4.2）

本文档定义 Quasar 的功能需求、非功能需求及验收标准，是需求层面的唯一权威基线。设计实现细节见 `docs/baseline/DESIGN.md`。

---

## 1. 项目定位与设计目标

### 1.1 定位

Quasar 是面向数据与 AI 系统的**统一元数据平面**。它是一个无状态的 Catalog Service，使异构引擎与平台能够在存在标准协议时通过原生协议注册、版本化、发现与管理资产；在其他情况下通过 Unified API 完成同样的操作。

Quasar 不替代数据面系统。它记录身份、元数据、版本指针，并在需要时保存少量内联内容，使客户端能够定位和使用工件，而无需每个系统各自重建目录。

### 1.2 用户

| 角色 | 核心关注点 |
|------|------------|
| 数据工程师 | 在 Iceberg、Lance 表上运行 Spark/Trino/Flink 分析与 ETL。 |
| ML 工程师 | 模型、数据集、特征的注册、版本化与发现。 |
| AI 平台工程师 | Agent、Tool、MCP Server 等 AI 资产的注册与治理。 |
| 平台管理员 | Domain/Namespace 治理、存储配置、生命周期管理。 |

### 1.3 设计目标

| 编号 | 目标 | 含义 |
|------|------|------|
| G1 | 协议开放 | 只要存在标准协议，就忠实实现，使现有客户端无需改动即可接入。 |
| G2 | 通用身份 | 核心对象模型不绑定单一资产类型或格式；任何资产类型都可注册。 |
| G3 | 格式独立 | 资产类型与格式是独立维度，例如 `model` 可以是 `onnx`、`gguf` 或 `safetensors`。 |
| G4 | 动态可扩展 | 可在运行时注册新的资产类型和格式，无需修改代码。使用 `jsonb` 或 `reference_only` 扩展策略时无需 DDL；使用 `dedicated_table` 时需配套内置迁移。 |
| G5 | 强一致性 | 每个资产的更新都是原子的，且立即可见。 |
| G6 | 无状态部署 | 服务为单一二进制，所有状态存放于 PostgreSQL。 |
| G7 | 务实边界 | 将数据面读写、血缘、认证授权、审计、复杂生命周期自动化等保持在目录之外。 |

---

## 2. 术语表

| 术语 | 定义 |
|------|------|
| **Domain** | 顶层容器与组织边界。全局唯一的 URL-safe slug 名称。系统提供全局默认存储后端，Domain 可覆盖 `storage_type`、`storage_config` 与 `warehouse`。 |
| **Namespace** | Domain 下的层级路径，用于组织资产。示例：`analytics/teams/finance`。 |
| **Asset** | 被注册的实体本身：表、视图、模型、Agent、Tool、MCP Server 等。 |
| **AssetType** | 已注册的资产种类，例如 `table`、`view`、`model`、`agent`、`tool`。 |
| **Format** | 资产的序列化或表示形式，例如 `iceberg`、`lance`、`onnx`、`gguf`、`json`。 |
| **Version** | 资产的特定版本，通过类型原生的 `version_key` 标识。 |
| **Native protocol adapter** | 针对特定资产类型/格式实现上游标准协议的适配器。 |
| **Unified API** | Quasar 自有的、与格式无关的管理与发现接口。 |
| **Metadata-only** | Quasar 默认仅存储元数据、指针与引用；实际工件内容存放于外部存储。 |

---

## 3. 系统边界与职责

### 3.1 范围内

- 在 `Domain → Namespace → Asset` 下注册与解析资产身份。
- 动态注册资产类型与格式，并支持可选的 JSON Schema 校验。
- 维护每个资产的元数据、属性、标签与版本历史。
- 为存在上游标准的格式提供 Native protocol adapter。基线阶段覆盖 Iceberg REST Catalog 与 Lance REST Namespace。
- 提供 Unified API 用于管理、发现与版本化访问。
- 内置数据库迁移与幂等初始化。
- 保证强单资产一致性与软删除/恢复语义。
- 服务二进制无状态，可水平扩展。

### 3.2 范围外

- 实际数据面文件的读写（Parquet、Lance 数据文件、模型权重等）。
- 跨资产血缘或依赖追踪。
- 内置认证、授权、多租户隔离或审计日志。Domain 作为组织边界，认证授权由外部系统承担。
- 自动生命周期过期、归档或状态转换策略。
- 事件通知、Webhook 或发布/订阅通道。
- 全文检索；仅提供结构化过滤。
- 外部工件存在性与正确性校验。
- 数据面操作，如训练模型、调用 Tool、执行查询。

---

## 4. 功能需求

本节按功能域划分功能需求（Functional Requirement，FR）。每条 FR 均给出验收标准，作为实现与测试的依据。

### 4.1 Domain 管理

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-D1 | 创建 Domain，需指定全局唯一的 URL-safe slug 名称 | 创建成功后可立即通过名称查询到；重名创建返回 `409 Conflict` |
| FR-D2 | 支持可选的 comment、properties、storage_type、storage_config、warehouse 字段 | 创建后可更新；未指定 warehouse 时使用 `QUASAR_WAREHOUSE_PATH` |
| FR-D3 | 查询 Domain 详情（按名称或 ID） | 返回 Domain 完整字段；不存在返回 `404 Not Found` |
| FR-D4 | 列出所有 Domain | 支持分页；按名称排序 |
| FR-D5 | 更新 Domain 的 comment、properties、storage_type、storage_config、warehouse | 更新后立即生效；非法字段返回 `400 Bad Request` |
| FR-D6 | 删除 Domain | 仅当 Domain 内不存在任何 Namespace 时允许删除；否则返回 `409 Conflict` |
| FR-D7 | Domain 存储配置覆盖全局默认 | 当 Domain 指定 storage_type/storage_config/warehouse 时，该 Domain 下的资产使用 Domain 级配置；否则使用全局环境变量 |

### 4.2 Namespace 管理

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-N1 | 创建 Namespace，需指定 Domain 和层级路径 | 路径如 `analytics/teams/finance`；路径段为 URL-safe slug；创建后可立即查询 |
| FR-N2 | 创建时隐式创建中间路径节点 | 创建 `a/b/c` 时，若 `a` 和 `a/b` 不存在，自动创建 |
| FR-N3 | 路径以文本形式物化，并存储 depth 整数 | depth 等于路径段数；支持按前缀查询子路径 |
| FR-N4 | 查询 Namespace 详情（按 Domain + 路径） | 返回 Namespace 完整字段；不存在返回 `404 Not Found` |
| FR-N5 | 列出指定 Domain 下的 Namespace | 支持按路径前缀过滤；支持分页；按路径排序 |
| FR-N6 | 更新 Namespace 的 comment、properties | 更新后立即生效；非法字段返回 `400 Bad Request` |
| FR-N7 | 删除 Namespace | 仅当 Namespace 内不存在子 Namespace 或 Asset 时允许删除；否则返回 `409 Conflict` |

### 4.3 Asset 管理

> 基线不假设存在非原生协议资产：所有资产均为原生协议资产，其创建、更新、重命名、删除等生命周期操作一律通过原生协议端点完成（见 §4.7）；Unified API 对资产只读，唯一例外是软删除恢复（FR-A9）与标签管理（FR-A3），二者属治理操作、不触碰原生协议状态。

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-A1 | 注册 Asset（通过原生协议端点），需指定 Domain、Namespace 路径、名称、asset_type、可选 format | 名称为 URL-safe slug；活动资产名在同一 Namespace 内唯一；创建后可立即查询；Unified API 不提供资产创建端点 |
| FR-A2 | 资产携带 comment、properties 元数据 | 创建和更新时可设置（经原生协议端点）；properties 为 JSONB |
| FR-A3 | 资产支持标签（tag）管理 | 可通过 Unified API 添加/移除/查询标签；标签存储于 `asset_tags` 表 |
| FR-A4 | 查询 Asset 详情（按 ID 或 Domain + Namespace + 名称） | 返回 Asset 完整字段；不存在返回 `404 Not Found` |
| FR-A5 | 列出 Asset，支持按 Domain、Namespace、类型、格式、标签、属性过滤 | 过滤条件可组合；支持分页；按名称排序 |
| FR-A6 | 更新 Asset 的 comment、properties（通过原生协议端点） | 更新后立即生效；非法字段返回 `400 Bad Request` |
| FR-A7 | 重命名 Asset（通过原生协议端点） | 新名称在同一 Namespace 内唯一；重命名后所有版本历史保留 |
| FR-A8 | 软删除 Asset（通过原生协议端点） | 标记 `deleted_at`；软删除期间版本历史保留；名称释放可供新资产使用 |
| FR-A9 | 恢复软删除的 Asset（通过 Unified API，治理例外） | 清除 `deleted_at`；若同名活动资产已存在，返回 `409 Conflict` |
| FR-A10 | 硬删除 Asset | 级联删除扩展表记录与版本记录；仅当软删除保留期后或管理员触发时执行 |
| FR-A11 | 原生协议资产由 adapter 直接创建与变更 | Iceberg/Lance adapter 通过原生端点创建/更新资产；Unified API 对资产只读（恢复与标签除外） |
| FR-A12 | 资产类型与格式校验 | 创建时校验 asset_type 和 format 已注册；format 与 asset_type 的兼容性由 adapter 校验 |

### 4.4 Version 管理

> 版本由原生协议 adapter 在 commit/变更时镜像创建（见 FR-V2）；Unified API 对版本只读。基线不假设存在非原生协议资产，因此**不提供版本创建与删除端点**（存储层 `delete_version` 延后，见 §10）。

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-V1 | 每个资产拥有版本历史，版本包含 version_key、version_properties、内容、前一版本指针 | 原生协议 commit 后可通过 list_versions 查询到对应版本 |
| FR-V2 | 原生协议资产的版本必须镜像到 `asset_versions` | Iceberg/Lance commit 后，`asset_versions` 中存在对应记录；`content_pointer` 指向原生元数据文件 |
| FR-V3 | 最新版本通过 `assets.current_version_key` 确定 | create_version 时自动更新 `current_version_key`；get_latest_version 基于该字段查询 |
| FR-V4 | 每个资产至多一个根版本（`previous_version_id IS NULL`） | 通过 partial unique 索引保证；尝试创建第二个根版本返回 `409 Conflict` |
| FR-V5 | 版本链完整性保护 | `previous_version_id` 使用 `ON DELETE RESTRICT`；删除被后继引用的版本返回 `409 Conflict` |
| FR-V6 | 查询版本详情（按 version_key） | 返回版本完整字段；不存在返回 `404 Not Found` |
| FR-V7 | 列出资产的版本历史 | 按 `version_key` 或 `created_at` 排序；支持分页；可包含软删除资产的版本 |
| FR-V8 | 版本不可删除 | 原生协议资产版本只增不删；基线不提供任何版本删除端点 |
| FR-V9 | 版本内容支持内联与外部指针两种方式 | `content_inline` 存小配置；`content_pointer` 存外部文件路径；两者至少其一 |

### 4.5 AssetType / Format 管理

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-T1 | 注册 AssetType，需指定唯一名称、category、extension_strategy、可选 validation_schema | 创建后可立即用于资产注册；重名返回 `409 Conflict` |
| FR-T2 | 注册 Format，需指定唯一名称、可选 mime_type、serialization_hint | 创建后可立即用于资产注册；重名返回 `409 Conflict` |
| FR-T3 | 内置初始化资产类型与格式 | 启动时自动注册 `table`、`view`；自动注册 `iceberg`、`lance`。其他类型与格式不预置，可通过 Unified API 动态注册 |
| FR-T4 | 查询 AssetType / Format 详情 | 返回完整字段；不存在返回 `404 Not Found` |
| FR-T5 | 列出所有 AssetType / Format | 支持按 category 过滤 AssetType；支持分页 |
| FR-T6 | 资产元数据 JSON Schema 校验 | 若 AssetType 定义了 validation_schema，创建/更新资产时校验通过；失败返回 `400 Bad Request` |
| FR-T7 | AssetType 的 extension_strategy 决定类型字段存储方式 | `dedicated_table` 类型有独立扩展表；`jsonb` 类型字段存 JSONB；`reference_only` 类型不存类型字段 |
| FR-T8 | AssetType 的 supports_native_protocol 标记 | 用于 Unified API 判断该类型是否只读；不影响 adapter 实际行为 |

### 4.6 Discovery 查询

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-Q1 | 按 Domain 过滤资产 | 返回指定 Domain 下的所有资产；空 Domain 返回空列表 |
| FR-Q2 | 按 Namespace 路径过滤资产 | 返回指定 Namespace 及其子 Namespace 下的资产；路径不存在返回 `404 Not Found` |
| FR-Q3 | 按 asset_type 过滤资产 | 返回指定类型的资产；类型未注册返回 `400 Bad Request` |
| FR-Q4 | 按 format 过滤资产 | 返回指定格式的资产；格式未注册返回 `400 Bad Request` |
| FR-Q5 | 按标签（tag）过滤资产 | 返回包含指定标签的资产；多标签为 AND 关系 |
| FR-Q6 | 按 properties 属性过滤资产 | 支持 `properties->>'key' = 'value'` 精确匹配；多条件为 AND 关系 |
| FR-Q7 | 组合过滤条件 | Domain、Namespace、类型、格式、标签、属性可任意组合；各条件间为 AND 关系 |
| FR-Q8 | 分页与排序 | 支持 pageToken/pageSize 令牌分页（与原生协议一致）；默认按名称排序；支持按 created_at/updated_at 排序 |
| FR-Q9 | 过滤时排除软删除资产 | 默认只返回活动资产；显式指定 `include_deleted=true` 时可返回软删除资产 |

### 4.7 Native Protocol

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-P1 | 提供 Iceberg REST Catalog 端点，路径前缀 `/iceberg/v1/...`，遵循 Apache Iceberg REST Catalog spec 1.11.x | 路径、请求/响应格式、错误格式遵循上游规范 |
| FR-P2 | Iceberg `{prefix}` 映射为 Domain 名 | `/iceberg/v1/{prefix}/namespaces/...` 中的 `{prefix}` 对应 Domain 的 name |
| FR-P3 | 提供 Lance REST Namespace 端点，路径前缀 `/lance/v1/...`，遵循 Lance REST Namespace spec（官方 OpenAPI 定义） | 路径、请求/响应格式、错误格式遵循上游规范 |
| FR-P4 | Lance `{id}` 使用 `$` 分隔符序列化 Domain/Namespace/Table | 如 `{domain}${namespace}${table}`；解析正确 |
| FR-P5 | `/iceberg/v1/config` 的 `endpoints` 字段与实际实现一致 | 只返回已实现的端点；客户端可据此探测能力 |
| FR-P6 | 原生协议 adapter 是其资产生命周期操作的权威 | 创建、更新、删除、重命名等操作通过原生端点完成，直接调用 store trait（软删除恢复除外，由 Unified API 提供，见 FR-A9） |
| FR-P7 | 原生协议版本镜像到 `asset_versions` | 每次 commit/变更都在 `asset_versions` 写入记录，含 version_key、content_pointer |
| FR-P8 | CAS commit 与多表事务并发冲突返回协议错误码 | Iceberg CAS 冲突返回 `409` 及 Iceberg 规范错误体；多表事务冲突按协议处理 |
| FR-P9 | 协议错误格式映射正确 | Iceberg 返回 Iceberg JSON error body；Lance 返回 RFC-7807 Problem Details |
| FR-P10 | 原生协议 adapter 通过编译时 feature flag 控制 | `--features iceberg` / `--features lance` 可独立启用/禁用对应 adapter |

### 4.8 Consistency 一致性

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-C1 | 强单资产一致性 | 每个资产的更新都是原子的，且立即可见；并发更新同一资产时通过数据库条件更新保证一致性 |
| FR-C2 | Domain 名称全局唯一 | 创建重名 Domain 返回 `409 Conflict` |
| FR-C3 | Namespace 路径在同一 Domain 内唯一 | 创建重复路径返回 `409 Conflict` |
| FR-C4 | 活动资产名称在同一 Namespace 内唯一 | 通过 partial unique 索引保证；创建重名活动资产返回 `409 Conflict` |
| FR-C5 | 版本 version_key 在同一 Asset 内唯一 | 重复 version_key 返回 `409 Conflict` |
| FR-C6 | 软删除/恢复语义 | 软删除标记 `deleted_at`；恢复时若同名活动资产存在返回 `409 Conflict` |
| FR-C7 | 原生协议资产版本不可变 | 原生协议资产版本只增不删；基线不提供版本删除端点 |
| FR-C8 | 版本链完整性 | `previous_version_id` 使用 `ON DELETE RESTRICT`；删除被引用版本返回 `409 Conflict` |
| FR-C9 | 非法资产类型与格式被拒绝 | 创建资产时若 asset_type 或 format 未注册，返回 `400 Bad Request` |

### 4.9 Infrastructure 基础设施

| 编号 | 功能需求 | 验收标准 |
|------|---------|---------|
| FR-I1 | 提供存活检查端点 `GET /healthz` | 返回 `200 OK`；服务不可用时返回 `503 Service Unavailable` |
| FR-I2 | 提供就绪检查端点 `GET /readyz` | 包含 PostgreSQL 连通性校验；数据库不可达时返回 `503 Service Unavailable` |
| FR-I3 | 支持优雅关机 | 接收 SIGTERM/SIGINT 后停止接受新请求；等待在途请求完成；关闭连接池后退出 |
| FR-I4 | 内置数据库迁移与幂等初始化 | 从空数据库启动时自动创建 Schema；迁移可重复执行；`schema_migrations` 记录已应用版本 |
| FR-I5 | 多实例水平扩展 | 多个服务实例连接同一 PostgreSQL；无本地状态；读写不依赖进程内状态 |
| FR-I6 | 配置通过环境变量加载 | `QUASAR_DATABASE_URL`、`QUASAR_HOST`、`QUASAR_PORT`、`QUASAR_S3_*` 等环境变量可配置 |
| FR-I7 | 结构化日志 | 使用 tracing 输出结构化日志；日志级别可通过 `QUASAR_LOG_LEVEL` 配置 |
| FR-I8 | 请求日志中间件 | 使用 tower-http trace 记录 HTTP 请求日志 |
| FR-I9 | 单一二进制部署 | 所有 adapter 编译为单一可执行文件；通过 feature flag 裁剪 |

---

## 5. 数据模型需求

### 5.1 资产类型与格式注册

资产类型与格式不是硬编码的，而是在目录中注册，可通过受控的 Unified API 端点或初始化脚本创建。

**AssetType 字段**

| 字段 | 说明 |
|------|------|
| `name` | 唯一标识，例如 `table`、`model`。 |
| `description` | 人类可读描述。 |
| `category` | 逻辑分组：`tabular`、`view`、`model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic`、`generic`。 |
| `validation_schema` | 可选的 JSON Schema，用于校验资产元数据。 |
| `extension_strategy` | 类型专属元数据字段的扩展存储策略：`jsonb`（通用 JSONB 扩展行）、`dedicated_table`（独立扩展表）、`reference_only`（不存储类型字段，仅依赖外部引用）。该策略只决定类型特定字段如何落库，与版本内容（`asset_versions.content_inline` / `content_pointer`）的存储方式无关。 |
| `supports_native_protocol` | 该类型是否预期有原生协议适配器。 |

**Format 字段**

| 字段 | 说明 |
|------|------|
| `name` | 唯一标识，例如 `iceberg`、`onnx`。 |
| `description` | 人类可读描述。 |
| `mime_type` | 可选 MIME 类型提示。 |
| `serialization_hint` | 可选提示，如 `json`、`protobuf`、`parquet`。 |

初始化时内置的资产类型：`table`、`view`。内置格式包括：`iceberg`、`lance`。其他资产类型（如 `model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic`）与格式（如 `onnx`、`gguf`、`safetensors`、`json`、`yaml`）不预置，可通过 Unified API 动态注册。

内置资产类型均采用 `dedicated_table` 扩展策略，各自拥有独立的类型扩展表。具体扩展表 Schema 在实际设计该资产类型接入时确定；基线阶段以 `table` 和 `view` 为示例。

### 5.2 资产内容存储默认规则

Quasar 默认采用 metadata-only 原则：目录只存储元数据、指针与引用，实际工件内容存放于外部对象存储。每个版本的内容通过 `asset_versions` 表的以下字段承载：

- `content_pointer`：指向外部对象存储中的工件文件（如 Iceberg `metadata.json`、Lance manifest）。
- `content_inline`：仅在格式不自包含或外部存储不现实时，用于存放少量内联内容。

本条与 §5.1 的 `extension_strategy` 是独立维度：后者决定类型专属元数据字段的存储方式，本条决定版本实际内容的存储方式。

---

### 5.3 Domain

- 顶层容器与组织边界。
- 拥有全局唯一的人类可读名称，命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`（小写字母、数字、连字符、下划线，以字母或数字开头）。
- 系统提供全局默认存储后端（由环境变量配置）；Domain 可覆盖 `storage_type`、`storage_config` 与 `warehouse`。优先级：Domain 配置 > 全局环境变量。
- 当 Domain 内仍存在 Namespace 时，禁止删除。

### 5.4 Namespace

- Domain 下的层级路径，例如 `analytics/teams/finance`。
- 路径以文本形式物化，并存储 `depth` 整数以支持前缀查询。
- 路径段命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。
- 资产名称在叶子 Namespace 内唯一，命名规则为 URL-safe slug：`^[a-z0-9][a-z0-9_-]{0,62}$`。
- 创建 Namespace 时可隐式创建中间路径节点。
- 当 Namespace 内仍存在子 Namespace 或 Asset 时，禁止删除。

### 5.5 Asset

- 属于且仅属于一个 Namespace。
- 具有 `asset_type` 与可选的 `format`。
- 携带元数据、属性、标签与软删除时间戳。
- 所有资产均版本化。
- Native protocol adapter 直接创建与变更资产。

### 5.6 Version

- 每个资产都拥有版本历史。
- 一个版本包含 `version_key`、版本属性（`version_properties`，与格式无关的通用信息）、内容（`content_inline` 内联或 `content_pointer` 外部指针）、前一版本指针。
- 最新版本通过 `assets.current_version_key` 确定；版本历史通过 `version_key`（原生协议可排序时）或 `created_at`（兜底）排序与遍历。
- 对于原生协议资产类型（如 Iceberg、Lance），其原生版本必须在 `asset_versions` 中镜像保存。`version_key` 采用原生版本标识，`content_pointer` 指向原生元数据文件；版本历史通过 `version_key` 或 `created_at` 排序，保证 Unified API 与原生协议端点可观测到一致的版本历史。

---

## 6. API 接口需求

### 6.1 Native protocol adapter

| 协议 | 路径前缀 | 资产 | 标准 |
|------|----------|------|------|
| Iceberg REST Catalog | `/iceberg/v1/...` | Iceberg 表与视图 | Apache Iceberg REST Catalog spec，基线 1.11.x |
| Lance REST Namespace | `/lance/v1/...` | Lance 表 | Lance REST Namespace spec（官方 OpenAPI 定义） |

Native adapter 是其资产生命周期操作的权威。它们直接通过核心 store trait 创建、更新、删除资产。

未来资产类型可通过新增 adapter 实现并开启对应 Cargo feature 添加原生协议；基线阶段所有 adapter 均为编译时静态包含，不支持运行时动态加载。

**Iceberg REST Catalog 端点**

- Config：`GET /iceberg/v1/config`
- Namespace：`GET` / `POST /iceberg/v1/{prefix}/namespaces`；`GET` / `DELETE /iceberg/v1/{prefix}/namespaces/{namespace}`；`POST /iceberg/v1/{prefix}/namespaces/{namespace}/properties`
- Table：`GET` / `POST /iceberg/v1/{prefix}/namespaces/{namespace}/tables`；`GET` / `POST` / `DELETE /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}`
- View：`GET` / `POST /iceberg/v1/{prefix}/namespaces/{namespace}/views`；`GET` / `POST` / `DELETE /iceberg/v1/{prefix}/namespaces/{namespace}/views/{view}`
- Transaction：`POST /iceberg/v1/{prefix}/transactions/commit`
- Metrics：`POST /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`

**Lance REST Namespace 端点**

- Namespace：`GET /lance/v1/namespace/{id}/list`；`POST /lance/v1/namespace/{id}/create`；`POST /lance/v1/namespace/{id}/describe`；`POST /lance/v1/namespace/{id}/drop`
- Table：`GET /lance/v1/namespace/{id}/table/list`；`POST /lance/v1/table/{id}/declare`；`POST /lance/v1/table/{id}/describe`；`POST /lance/v1/table/{id}/deregister`

> Table 端点的 `{id}` 为完整表标识（`{domain}${namespace}${table}`，见 FR-P4），与官方 OpenAPI 一致；describe 为 POST（官方 OpenAPI 定义）。

### 6.2 Unified API

Unified API 是位于 `/unified/v1/...` 的、与格式和协议无关的管理与发现接口。基线不假设存在非原生协议资产（见 §4.3），因此资产与版本为**只读**，唯一写例外是软删除恢复与标签管理（治理操作）：

- Domain 生命周期管理。
- Namespace 生命周期管理，包括层级路径。
- 资产的只读访问：列表与获取（按 ID 或按 Domain + Namespace + 名称）。资产的创建、更新、重命名、删除等生命周期操作一律通过原生协议端点完成，Unified API 不提供资产写端点。
- 软删除恢复：`POST /unified/v1/assets/{asset_id}/restore`，治理操作的唯一写例外。
- 标签管理：为资产添加、移除、查询标签（治理标注，非生命周期操作）。
- 版本的只读访问：列表与获取。版本历史由原生协议 adapter 镜像到 `asset_versions`；基线不提供版本创建与删除端点。
- AssetType 与 Format 的注册与管理。
- 发现：按 Domain、Namespace 路径、资产类型、格式、标签、属性过滤。

**Unified API 端点**

- Domain：`GET` / `POST /unified/v1/domains`；`GET` / `PATCH` / `DELETE /unified/v1/domains/{domain}`
- Namespace：`GET` / `POST /unified/v1/domains/{domain}/namespaces`；`GET` / `PATCH` / `DELETE /unified/v1/domains/{domain}/namespaces/{namespace}`
- Asset（只读）：`GET /unified/v1/domains/{domain}/namespaces/{namespace}/assets`（列表）；`GET /unified/v1/domains/{domain}/namespaces/{namespace}/assets/{asset}`（按名）；`GET /unified/v1/assets/{asset_id}`（按 ID）
- Restore：`POST /unified/v1/assets/{asset_id}/restore`
- Tag：`GET` / `POST /unified/v1/assets/{asset_id}/tags`；`DELETE /unified/v1/assets/{asset_id}/tags/{tag}`
- Version（只读）：`GET /unified/v1/assets/{asset_id}/versions`；`GET /unified/v1/assets/{asset_id}/versions/{version}`
- AssetType：`GET` / `POST /unified/v1/asset-types`；`GET /unified/v1/asset-types/{name}`
- Format：`GET` / `POST /unified/v1/formats`；`GET /unified/v1/formats/{name}`
- Discovery：`GET /unified/v1/assets`（支持查询参数过滤，见 §4.6）

> 所有列表端点使用 pageToken/pageSize 令牌分页（见 §4.6 FR-Q8），与原生协议分页机制一致。

### 6.3 基础设施端点

- `GET /healthz` —— 存活检查。
- `GET /readyz` —— 就绪检查，包含 PostgreSQL 连通性校验。

### 6.4 API 版本

- Native protocol 遵循各自上游版本。
- Unified API 通过路径版本化（`/unified/v1/...`）。

---

## 7. 非功能需求

| 编号 | 需求 | 说明 |
|------|------|------|
| NF1 | 技术栈 | Rust、axum、tokio、tokio-postgres/deadpool-postgres、PostgreSQL。 |
| NF2 | 无状态 | 无本地状态；通过增加实例水平扩展。 |
| NF3 | 无内置认证 | 认证授权由外部承担；Domain 为组织边界。 |
| NF4 | 单一二进制 | 一个可部署二进制承载所有适配器。 |
| NF5 | 内置迁移 | Schema 变更自动应用并记录。 |
| NF6 | SQL 规范 | 所有 SQL 集中管理；禁止运行时拼接 SQL。 |
| NF7 | 代码质量 | 生产代码避免 `unwrap()` 与 `expect()`；workspace clippy 强制执行。 |
| NF8 | Feature flag | 适配器可在编译时按需包含或排除。 |

---

## 8. 测试策略

| 层次 | 方法 |
|------|------|
| 核心模型 | 使用 store trait 内存假实现进行单元测试。 |
| 存储层 | 针对 PostgreSQL（embedded 或 testcontainer）的集成测试。 |
| 原生适配器 | Iceberg、Lance 端点的协议一致性测试。 |
| Unified API | CRUD、过滤、软删除/恢复、动态资产类型注册的测试。 |
| 端到端 | 使用完整 server 二进制与真实 PostgreSQL 的冒烟测试。 |

---

## 9. 验收标准

### 9.1 数据模型与约束

- 从空数据库按内置迁移初始化成功，服务可正常启动；迁移可重复执行。
- Domain 名称全局唯一；Namespace 路径在同一 Domain 内唯一；活动资产名称在同一 Namespace 内唯一。
- 非法资产类型与格式被注册表拒绝。
- 版本 `version_key` 在同一 Asset 内唯一。
- 软删除资产恢复时，若同一 Namespace 内同名活动资产已存在，返回 `409 Conflict`。

### 9.2 Native protocol 验收

- Iceberg 与 Lance 端点的路径、请求/响应格式、错误格式遵循各自上游规范。
- `/iceberg/v1/config` 的 `endpoints` 字段与实际实现一致。
- CAS commit 与多表事务的并发冲突返回正确的协议错误码。

### 9.3 Unified API 验收

- Domain、Namespace 的 CRUD，Asset/Version 的只读访问与过滤，按本需求实现。
- Unified API 不提供资产创建、更新、重命名、删除及版本写端点（生命周期归原生协议）；资产列表/获取、软删除恢复、标签管理端点保持可用。
- 列表端点使用 pageToken/pageSize 令牌分页。
- 错误格式为 RFC-7807 Problem Details，且不泄漏到标准协议端点。

### 9.4 构建与部署验收

- `cargo fmt --all --manifest-path quasar/Cargo.toml` 通过。
- `cargo test --all-features --all-targets` 全部通过。
- `cargo clippy --workspace --tests` 无错误。
- Feature flag 可裁剪：`--no-default-features --features lance` / `--features iceberg` / `--features unified` 均可构建。
- 多实例连接同一 PostgreSQL 的读写冒烟测试不依赖进程内状态。

---

## 10. 待明确与延期事项

以下需求细节留待后续专门讨论。本节关注**需求层面未明确的功能与约束**；实现层面的技术方案延期事项见 `docs/baseline/DESIGN.md` §10。

1. 各资产类型的具体 JSON Schema 与校验规则。
2. 内联内容大小限制与对象存储卸载阈值。
3. 软删除资产的保留窗口与硬删除策略。
4. 从旧里程碑 Schema 到新通用 Schema 的迁移策略。
5. model、agent、tool、mcp_server 等资产类型的具体原生协议（如有）。
6. 版本删除能力（`delete_version`）：基线不假设非原生协议资产，无调用方；随非原生协议资产类型引入时开放。

---

## 11. 总结

Quasar 的需求基线定义了一个统一、可扩展的元数据平面：

- 通用的 `Domain → 层级 Namespace → Asset` 身份模型。
- 资产类型与格式的动态注册。
- 每个资产的版本历史。
- Iceberg、Lance 等原生协议适配器。
- 面向所有资产的管理与发现 Unified API。
- 强单资产一致性、软删除/恢复、无状态水平扩展。

数据面操作、血缘、认证授权、审计、生命周期自动化与事件通知均明确在范围之外。
