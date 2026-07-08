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
| **Domain** | 顶层容器与租户边界。全局唯一的人类可读名称。承载存储配置与 warehouse 根路径。 |
| **Namespace** | Domain 下的层级路径，用于组织资产。示例：`analytics/teams/finance`。 |
| **Asset** | 被注册的实体本身：表、视图、模型、Agent、Tool、MCP Server 等。 |
| **AssetType** | 已注册的资产种类，例如 `table`、`view`、`model`、`agent`、`tool`。 |
| **Format** | 资产的序列化或表示形式，例如 `iceberg`、`lance`、`onnx`、`gguf`、`json`。 |
| **Version** | 资产的特定版本，通过类型原生的 `version_key` 与单调的 `version_order` 标识。 |
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
- 内置认证、授权、多租户隔离或审计日志。Domain 作为租户边界，认证授权由外部系统承担。
- 自动生命周期过期、归档或状态转换策略。
- 事件通知、Webhook 或发布/订阅通道。
- 全文检索；仅提供结构化过滤。
- 外部工件存在性与正确性校验。
- 数据面操作，如训练模型、调用 Tool、执行查询。

---

## 4. 资产模型需求

### 4.1 资产类型与格式注册

资产类型与格式不是硬编码的，而是在目录中注册，可通过受控的 Unified API 端点或初始化脚本创建。

**AssetType 字段**

| 字段 | 说明 |
|------|------|
| `name` | 唯一标识，例如 `table`、`model`。 |
| `description` | 人类可读描述。 |
| `category` | 逻辑分组：`tabular`、`view`、`model`、`agent`、`tool`、`generic`。 |
| `validation_schema` | 可选的 JSON Schema，用于校验资产元数据。 |
| `extension_strategy` | `jsonb`、`dedicated_table` 或 `reference_only`。 |
| `supports_native_protocol` | 该类型是否预期有原生协议适配器。 |

**Format 字段**

| 字段 | 说明 |
|------|------|
| `name` | 唯一标识，例如 `iceberg`、`onnx`。 |
| `description` | 人类可读描述。 |
| `mime_type` | 可选 MIME 类型提示。 |
| `serialization_hint` | 可选提示，如 `json`、`protobuf`、`parquet`。 |
| `supports_cas_commit` | 仅用于表格式（Iceberg 为 true，Lance 为 false）。 |

初始化时内置的资产类型：`table`、`view`、`model`、`agent`、`tool`、`mcp_server`、`fileset`、`topic`。内置格式包括：`iceberg`、`lance`、`onnx`、`gguf`、`safetensors`、`json`、`yaml`。

### 4.2 内容存储策略

每个资产类型选择以下三种策略之一：

- `jsonb`：类型特定字段以 JSONB 形式存储在通用扩展行中。
- `dedicated_table`：该类型拥有独立的扩展表，用于严格 Schema 与索引。
- `reference_only`：目录仅存储外部指针；内容存放于别处。

默认规则是 metadata-only：目录存储指针与引用，实际工件内容存放于外部存储。当某种格式不自包含或外部存储不现实时，资产类型可选择存储少量内联内容。

---

## 5. 核心组织模型需求

### 5.1 Domain

- 顶层容器与租户边界。
- 拥有全局唯一的人类可读名称。
- 可携带可选的 tenant 标签，供外部认证系统使用。
- 定义默认存储后端，并支持按 Domain 覆盖。
- 当 Domain 内仍存在 Namespace 时，禁止删除。

### 5.2 Namespace

- Domain 下的层级路径，例如 `analytics/teams/finance`。
- 路径以文本形式物化，并存储 `depth` 整数以支持前缀查询。
- 资产名称在叶子 Namespace 内唯一。
- 创建 Namespace 时可隐式创建中间路径节点。

### 5.3 Asset

- 属于且仅属于一个 Namespace。
- 具有 `asset_type` 与可选的 `format`。
- 携带元数据、属性、标签、审计字段与软删除时间戳。
- 所有资产均版本化。
- Native protocol adapter 直接创建与变更资产。

### 5.4 Version

- 每个资产都拥有版本历史。
- 一个版本包含 `version_key`、`version_order`、版本元数据、内容（内联或指针）、前一版本指针。
- 最新版本通过 `version_order DESC` 解析。
- 对于已自行维护版本历史的原生协议资产类型，目录既可以将原生版本镜像到 `asset_versions`，也可以直接通过原生协议暴露其历史。无论如何，版本历史必须可观测且有序。

---

## 6. API 接口需求

### 6.1 Native protocol adapter

| 协议 | 路径前缀 | 资产 | 标准 |
|------|----------|------|------|
| Iceberg REST Catalog | `/iceberg/v1/...` | Iceberg 表与视图 | Apache Iceberg REST Catalog spec，基线 1.10.x |
| Lance REST Namespace | `/lance/v1/...` | Lance 表 | Lance REST Namespace spec |

Native adapter 是其资产生命周期操作的权威。它们直接通过核心 store trait 创建、更新、删除资产。

未来资产类型可通过实现 adapter 插件契约添加原生协议。

### 6.2 Unified API

Unified API 是位于 `/unified/v1/...` 的、与格式和协议无关的管理与发现接口：

- Domain 生命周期管理。
- Namespace 生命周期管理，包括层级路径。
- 资产的列表/获取/更新/重命名/恢复/删除。对于已有原生协议的资产类型，Unified API 不提供创建能力。
- 版本的列表/获取/创建/删除。
- AssetType 与 Format 的注册与管理。
- 发现：按 Domain、Namespace 路径、资产类型、格式、标签、属性过滤。

对于没有原生协议的非表资产，Unified API 同时是创建接口。

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
| NF3 | 无内置认证 | 认证授权由外部承担；Domain 为租户边界。 |
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
- 版本 `version_key` 在同一 Asset 内唯一；非空 `version_order` 在同一 Asset 内唯一。

### 9.2 Native protocol 验收

- Iceberg 与 Lance 端点的路径、请求/响应格式、错误格式遵循各自上游规范。
- `/iceberg/v1/config` 的 `endpoints` 字段与实际实现一致。
- CAS commit 与多表事务的并发冲突返回正确的协议错误码。

### 9.3 Unified API 验收

- Domain、Namespace、Asset、Version 的 CRUD 与过滤按本需求实现。
- 对已有原生协议的资产类型，Unified API 创建端点返回 405 或等效拒绝。
- 错误格式为 RFC-7807 Problem Details，且不泄漏到标准协议端点。

### 9.4 构建与部署验收

- `cargo fmt --all --manifest-path quasar/Cargo.toml` 通过。
- `cargo test --all-features --all-targets` 全部通过。
- `cargo clippy --workspace --tests` 无错误。
- Feature flag 可裁剪：`--no-default-features --features lance` / `--features iceberg` / `--features unified` 均可构建。
- 多实例连接同一 PostgreSQL 的读写冒烟测试不依赖进程内状态。

---

## 10. 待明确与延期事项

以下需求细节留待后续专门讨论：

1. 各资产类型的具体 JSON Schema 与校验规则。
2. 内联内容大小限制与对象存储卸载阈值。
3. 软删除资产的保留窗口与硬删除策略。
4. 从旧里程碑 Schema 到新通用 Schema 的迁移策略。
5. model、agent、tool、mcp_server 等资产类型的具体原生协议（如有）。

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
