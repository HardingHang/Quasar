# Quasar 需求规格说明书

> **项目名称**：Quasar —— 面向 Lakehouse 架构的通用 Catalog Service
> **文档状态**：当前权威基线
> **适用版本**：V4.2（含 Iceberg REST Catalog baseline、Table Capability Completion、Views & Scan Planning）
>
> 本文档定义 Quasar 的功能与非功能需求及验收标准，是需求层面的唯一权威。
> 设计与实现细节见 `docs/DESIGN.md`；部署与验证见 `docs/DEPLOYMENT.md`；测试矩阵见 `docs/TEST_MATRIX.md`。
> 历史版本需求文档归档于 `docs/archive/`，仅作参考，不再维护。

---

## 目录

1. [项目定位与设计目标](#1-项目定位与设计目标)
2. [术语与核心概念](#2-术语与核心概念)
3. [系统边界与职责划分](#3-系统边界与职责划分)
4. [数据资产与协议矩阵](#4-数据资产与协议矩阵)
5. [功能需求](#5-功能需求)
6. [非功能需求](#6-非功能需求)
7. [数据模型需求](#7-数据模型需求)
8. [并发控制与一致性需求](#8-并发控制与一致性需求)
9. [错误处理需求](#9-错误处理需求)
10. [明确不做的事项](#10-明确不做的事项)
11. [验收标准](#11-验收标准)

---

## 1. 项目定位与设计目标

### 1.1 定位

Quasar 是一个面向 Lakehouse 架构的、独立的通用 Catalog Service 组件。它是可纳管多种数据资产的元数据底座，为计算引擎、客户端 SDK 和管理后台提供统一的元数据访问入口。

### 1.2 可纳管资产类型

| 资产类型 | 表格式 | 接入协议 | 状态 |
|---------|-------|---------|------|
| 表（Table） | Apache Iceberg | Iceberg REST Catalog | 已实现 |
| 表（Table） | Lance | Lance REST Namespace | 已实现 |
| 视图（View） | Iceberg View | Iceberg REST Catalog | 已实现 |
| AI 模型 / 特征 / 文件集 / Topic 等 | — | — | 预留扩展，未实现 |

### 1.3 设计目标

| # | 目标 | 含义 |
|---|------|------|
| G1 | 协议开放 | 对每种资产类型忠实实现其上游标准协议，而非发明私有 API。Iceberg 遵循 Iceberg REST Catalog 规范，Lance 遵循 Lance REST Namespace 规范，确保各自生态的引擎和客户端零改动接入。 |
| G2 | 模型通用 | 核心对象模型不绑定任何单一数据格式，任何资产类型都可注册到 Catalog。各协议适配器将上游规范映射到统一的核心模型。 |
| G3 | 一致性保障 | 对 Iceberg 等需要原子提交的资产类型，通过乐观并发控制（CAS）保证元数据更新的正确性；多表事务保证原子性。 |
| G4 | 性能优先 | 采用 Rust + axum + tokio-postgres 构建，追求低延迟、高并发、小内存占用。 |
| G5 | 部署简洁 | 服务本身无状态，单二进制部署，依赖单一 PostgreSQL 实例即可运行。 |
| G6 | 可扩展 | 通过清晰的 crate 分层，新增资产类型或接入协议只需扩展对应 crate，不侵入核心。 |

---

## 2. 术语与核心概念

| 术语 | 定义 |
|------|------|
| **Domain** | 顶层存储与治理容器，不绑定表格式。对应 Iceberg 的 `{prefix}` 或 Lance `{id}` 的第一段。承载存储配置、warehouse 路径、权限隔离边界。 |
| **Namespace** | Domain 内的业务组织单元（类似数据库的 schema）。单层，不嵌套。同一 Domain 内名称唯一。 |
| **Asset** | Namespace 内的实际资产身份。同一 Namespace 内活动资产名唯一（不区分类型/格式）。 |
| **Tabular Asset** | 表类型资产扩展层，具有 format（iceberg/lance）、location、metadata_location 等字段。 |
| **View Asset** | Iceberg 视图扩展层。View 当前只有 Iceberg 支持，无 format 字段。 |
| **端点级隔离** | 格式由 REST 端点路径隐含（`/iceberg/v1/...` vs `/lance/v1/...`），Domain 不绑定格式。 |
| **CAS Commit** | Compare-And-Swap 提交：服务端校验当前 `metadata_location` 后原子更新。用于 Iceberg Table/View 提交。 |
| **版本注册** | Lance 中客户端先写对象存储生成 manifest，再通知 Catalog 在 DB 中记录版本信息。最终一致语义。 |
| **统一身份层** | `assets` 表仅保存所有资产共有的身份字段，类型特有字段进入扩展表。 |
| **staged create** | Iceberg 表的两阶段创建：先写 staged metadata 不创建 catalog 记录，commit 阶段在事务内验证 `assert-create` 并插入记录。 |
| **plan-task token** | Scan Planning 中自包含的 base64 编码 token，编码 FileScanTask 列表，无服务端状态。 |

---

## 3. 系统边界与职责划分

### 3.1 Catalog Service 职责

Quasar **负责**：

- 资产身份与命名空间管理（Domain / Namespace / Asset 的 CRUD）。
- 元数据指针管理（Iceberg `metadata_location`、Lance `location` / `manifest_path`）。
- 元数据内容读写（Iceberg metadata.json 由服务端生成、校验、序列化；Lance manifest 由客户端生成）。
- 乐观并发提交（Iceberg CAS commit、多表事务、View CAS commit）。
- 对象存储清理（Iceberg `DROP TABLE PURGE` 的表路径清理）。
- Scan Planning 计算（服务端生成 FileScanTask 列表）。
- Scan metrics report 接收与持久化。

Quasar **不负责**：

- 数据面读写（Parquet/Lance 数据文件的读写由客户端/引擎通过 location 直接访问对象存储完成）。
- Lance 版本协调写入（Lance SDK 直接写对象存储，Catalog 仅事后注册版本；最终一致）。
- 生产级鉴权/授权（无认证、无租户隔离；内网/测试环境使用）。
- Vended credentials 签发、S3 远程签名（推迟到后续安全专项）。

### 3.2 三层架构

```
┌─────────────────────────────────────────────┐
│  第一层：Domain（顶层容器）                   │
│  • 存储配置边界（S3 bucket、warehouse 路径）  │
│  • 权限隔离边界 / 多租户隔离边界（未来）      │
├─────────────────────────────────────────────┤
│  第二层：Namespace（业务组织）                │
│  • 逻辑分组（如 analytics、marketing、ml）   │
│  • 单层，不嵌套                               │
│  • 同一 Domain 内名称唯一                     │
├─────────────────────────────────────────────┤
│  第三层：Asset（实际资产）                    │
│  • table、view、（未来）model/fileset/topic  │
│  • 同一 Namespace 内活动资产名唯一            │
│  • 格式信息在扩展表中                         │
└─────────────────────────────────────────────┘
```

### 3.3 双协议架构

Quasar 在单一进程内为每套协议暴露独立的路径前缀，内部通过统一的存储层和核心模型承载。

```
                      ┌───────────────────────────────────────┐
                      │            Quasar Server               │
                      │                                       │
  Spark / Trino       │   /iceberg/v1/...                     │
  Flink (Iceberg)  ───►│   ┌─────────────────────┐            │
                      │   │  Iceberg REST        │            │
                      │   │  Catalog Adapter     │──┐         │
                      │   └─────────────────────┘  │         │
                      │                            ▼         │
                      │                    ┌──────────────┐  │
                      │                    │  Core Model   │  │
                      │                    │  + Storage    │  │
                      │                    │  (PostgreSQL) │  │
                      │                    └──────────────┘  │
                      │                            ▲         │
  Lance SDK           │   ┌─────────────────────┐  │         │
  (Python/Rust/Java)──►   │  Lance REST         │──┘         │
  Spark (Lance)       │   │  Namespace Adapter   │            │
  Ray / Trino         │   └─────────────────────┘            │
                      │   /lance/v1/...                       │
                      │                                       │
  管理后台 / 数据平台  │   /unified/v1/...  (Quasar 自有)      │
                      └───────────────────────────────────────┘
```

### 3.4 格式隔离策略：端点级隔离

**核心原则**：Domain 不绑定格式，格式由 REST 端点路径隐含。

- Iceberg 端点下的 Namespace / Table / View 只对 Iceberg 客户端可见。
- Lance 端点下的 Namespace / Table 只对 Lance 客户端可见。
- 同一 Domain 内允许 Iceberg 表和 Lance 表并存，但**同一 Namespace 内活动资产名唯一**——即 Iceberg 表 `events` 和 Lance 表 `events` 不能在同一 Namespace 下共存；用错端点查询返回 not found 语义（如 Iceberg 端点查 Lance 表返回 `NoSuchTableException`）。
- 管理后台通过 Unified API 获得跨格式统一视图。

**官方兼容映射**：

| Quasar 对象 | Iceberg REST | Lance REST |
|-------------|--------------|------------|
| Domain `prod` | `{prefix}=prod` | `{id}` 第一段 `prod` |
| Namespace `analytics` | `{namespace}=analytics` | `{id}` 第二段，`prod$analytics` |
| Table `events` | `{table}=events` | `{id}` 第三段，`prod$analytics$events` |

---

## 4. 数据资产与协议矩阵

### 4.1 协议入口

| 协议 | 路径前缀 | 面向客户端 | `/v1` 含义 |
|------|---------|-----------|-----------|
| Iceberg REST Catalog | `/iceberg/v1/...` | Spark、Trino、Flink、PyIceberg | Iceberg REST OpenAPI 自带版本前缀 |
| Lance REST Namespace | `/lance/v1/...` | LanceDB SDK、lance-spark、Ray | Lance REST Namespace 规范自带版本前缀 |
| Unified REST API | `/unified/v1/...` | 管理后台、数据平台 | Quasar 自有 API 合同版本 |

### 4.2 官方协议基线

| 协议 | 固定基线 | 用途 |
|------|---------|------|
| Iceberg REST Catalog | Apache Iceberg **1.10.x** 发布版 | REST Catalog 端点、请求/响应模型、错误模型权威来源 |
| Lance REST Namespace | Lance REST Namespace Catalog Spec + Implementation Spec | Lance 路由、错误码、实现约定 |
| iceberg crate | 0.9.1（crates.io） | Rust 端 Iceberg metadata 类型与工具库 |

> 不得以 Iceberg `main` 分支漂移内容或 1.11+ 作为实现范围；新版本端点须先评估再纳入。

---

## 5. 功能需求

### 5.1 需求总览

| ID | 需求 | 说明 |
|----|------|------|
| F1 | Iceberg REST Catalog 协议支持 | 实现 Iceberg 1.10.x OpenAPI 的 Spark E2E 必需端点子集（见 §5.2） |
| F2 | Lance REST Namespace 协议支持 | 实现 Lance REST Namespace 规范的基础操作及版本管理端点（见 §5.3） |
| F3 | Unified REST API | 格式无关的管理层端点，面向管理后台/数据平台（见 §5.4） |
| F4 | 基础设施端点 | `GET /healthz`、`GET /readyz`（含 DB 连通性） |
| F5 | Domain 生命周期管理 | Unified API 下 Domain 的 CRUD 与存储配置管理 |
| F6 | Namespace 管理 | 跨三套协议的 Namespace CRUD（共享同一存储） |
| F7 | Iceberg Table 元数据管理 | 创建/加载/提交/删除/重命名/注册表，含 staged create 与 CAS commit |
| F8 | Iceberg View 生命周期 | 7 个 View 端点（list/create/load/replace/drop/head/rename） |
| F9 | Server-side Scan Planning | 4 个端点（submit/fetch/cancel plan、fetch tasks） |
| F10 | Multi-table Transactions | `POST /v1/{prefix}/transactions/commit` 原子性批量提交 |
| F11 | Spark 集成验证 | Spark 3.5 + Iceberg 1.10.x 端到端读写 Iceberg 表与 View |

### 5.2 F1：Iceberg REST Catalog 端点

路径前缀 `/iceberg/v1/...`，`{prefix}` 映射为 Domain 名。Spark 引擎配置 `uri=http://host:port/iceberg`，自动拼接 `/v1/...`。

#### 5.2.1 Configuration

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/v1/config` | 返回 catalog 配置（defaults、overrides、`endpoints` 已实现端点列表）；支持 `warehouse` 查询参数 |

#### 5.2.2 Namespace

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/v1/{prefix}/namespaces` | 列出 Domain 下 Namespace（`pageToken` / `pageSize` 分页） |
| POST | `/v1/{prefix}/namespaces` | 创建 Namespace |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 加载 Namespace 属性 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}` | 检查存在（无 body） |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 删除空 Namespace（非空返回 409） |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 按 `removals` / `updates` 增量更新 properties |

#### 5.2.3 Table

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/v1/{prefix}/namespaces/{namespace}/tables` | 列出 Iceberg 表（过滤 `format='iceberg'`） |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables` | 创建表或 staged create（`stage-create` 字段） |
| POST | `/v1/{prefix}/namespaces/{namespace}/register` | 注册已有 metadata file 为表（不重写对象存储） |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 加载表 metadata（返回 LoadTableResponse） |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 检查表存在 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 提交表更新（CAS commit，校验 requirements） |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 删除表，支持 `purgeRequested` |
| POST | `/v1/{prefix}/tables/rename` | 重命名表（同 Domain 跨 Namespace） |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | 上报 scan metrics report |

#### 5.2.4 View（Iceberg 1.10.x）

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/v1/{prefix}/namespaces/{namespace}/views` | 列出 View |
| POST | `/v1/{prefix}/namespaces/{namespace}/views` | 创建 View |
| GET | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 加载 View metadata |
| POST | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 替换 View（CAS commit） |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 删除 View |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 检查 View 存在 |
| POST | `/v1/{prefix}/views/rename` | 重命名 View |

#### 5.2.5 Scan Planning

| 方法 | 路径 | 能力 |
|------|------|------|
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | 提交 scan planning 请求 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 获取 plan 结果 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 取消 plan |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | 拉取 FileScanTask 列表 |

> Scan Planning 同时提供 Java `ResourcePaths` 常量路径 alias（不含 `namespaces/{namespace}` 段），用于兼容 Java 客户端。

#### 5.2.6 Transaction

| 方法 | 路径 | 能力 |
|------|------|------|
| POST | `/v1/{prefix}/transactions/commit` | 原子提交多个表更新 |

#### 5.2.7 Iceberg Commit Requirements / Updates 覆盖

**Table Requirements（Iceberg 1.10.x `BaseRequirement` 8 项 table 适用）**：

| Requirement | 校验目标 |
|-------------|----------|
| `assert-create` | active table 不存在；staged commit 必须满足 |
| `assert-table-uuid` | metadata `table-uuid` 等于请求值 |
| `assert-ref-snapshot-id` | branch/tag/current ref 指向请求 snapshot id |
| `assert-current-schema-id` | metadata `current-schema-id` 等于请求值 |
| `assert-default-spec-id` | metadata `default-spec-id` 等于请求值 |
| `assert-default-sort-order-id` | metadata `default-sort-order-id` 等于请求值 |
| `assert-last-assigned-field-id` | metadata `last-column-id` 等于请求值 |
| `assert-last-assigned-partition-id` | metadata `last-partition-id` 等于请求值 |

**View Requirements（2 项）**：`assert-create`（Table/View 共用）、`assert-view-uuid`（View-only）。

**Table Updates（已实现）**：`assign-uuid`、`upgrade-format-version`、`add-schema`、`set-current-schema`、`add-spec`、`set-default-spec`、`add-sort-order`、`set-default-sort-order`、`add-snapshot`、`set-snapshot-ref`、`remove-snapshots`、`remove-snapshot-ref`、`set-location`、`set-properties`、`remove-properties`、`remove-partition-specs`、`set-statistics`、`remove-statistics`、`set-partition-statistics`、`remove-partition-statistics`、`remove-schemas`。

**View Updates（8 项，与 iceberg crate 0.9.1 `ViewUpdate` 一致）**：`assign-uuid`、`upgrade-format-version`、`add-schema`、`set-location`、`set-properties`、`remove-properties`、`add-view-version`、`set-current-view-version`。

**明确拒绝（返回 501 `NotImplementedException`）**：`add-encryption-key`、`remove-encryption-key`（推迟到后续安全专项）。未知 action 或字段结构错误返回 400 `BadRequestException`，不得静默忽略。

### 5.3 F2：Lance REST Namespace 端点

路径前缀 `/lance/v1/...`。`{id}` 使用 `$` 分隔符序列化多级标识符，`Domain.name` 为第一段。Lance 客户端配置 `uri=http://host:port/lance`，SDK 自动拼接 `/v1/...`。

#### 5.3.1 Namespace

| 方法 | 路径 | 能力 |
|------|------|------|
| POST | `/v1/namespace/{id}/create` | 创建 Namespace（`id=prod` 创建 Domain，`id=prod$analytics` 在 Domain 下创建 Namespace） |
| GET | `/v1/namespace/{id}/list` | 列出子 Namespace（`id=$` 列出 Domain，`id=prod` 列出该 Domain 下 Namespace） |
| POST | `/v1/namespace/{id}/describe` | 查询 Namespace 属性 |
| POST | `/v1/namespace/{id}/drop` | 删除空 Domain 或空 Namespace |
| POST | `/v1/namespace/{id}/exists` | 检查存在 |

#### 5.3.2 Table

| 方法 | 路径 | 能力 |
|------|------|------|
| POST | `/v1/table/{id}/declare` | 声明 Lance 表，分配 location 和 storage_options（不创建数据文件） |
| GET | `/v1/namespace/{id}/table/list` | 列出 Namespace 下 Lance 表 |
| POST | `/v1/table/{id}/describe` | 查询表 metadata（可指定版本） |
| POST | `/v1/table/{id}/register` | 注册已有 Lance 数据集 |
| POST | `/v1/table/{id}/deregister` | 注销表但保留存储数据 |
| POST | `/v1/table/{id}/drop` | 删除表及其数据 |
| POST | `/v1/table/{id}/exists` | 检查表存在 |
| POST | `/v1/table/{id}/rename` | 重命名表 |

#### 5.3.3 Version

| 方法 | 路径 | 能力 |
|------|------|------|
| POST | `/v1/table/{id}/version/create` | 注册新版本（客户端写 S3 后通知 Catalog 记录） |
| GET | `/v1/table/{id}/version/list` | 列出版本历史 |
| POST | `/v1/table/{id}/version/describe` | 查询指定版本 |

> `{id}` 解析规则：`$` 表示 root namespace（列出所有 Domain）；一段为 Domain 虚拟 namespace；两段为 `Domain + Namespace`；表 `{id}` 必须三段 `Domain + Namespace + Table`。更深层级返回 `Unsupported` / `InvalidInput`。

### 5.4 F3：Unified REST API 端点

路径前缀 `/unified/v1/...`，Quasar 自有 API，面向管理后台/数据平台。计算引擎仍走各自标准协议入口。

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/unified/v1/domains` | 列出 Domain（响应脱敏 `storage_config`） |
| POST | `/unified/v1/domains` | 创建 Domain |
| GET | `/unified/v1/domains/{domain}` | 获取 Domain |
| PATCH | `/unified/v1/domains/{domain}` | 更新 Domain（comment/properties/storage/warehouse/owner） |
| DELETE | `/unified/v1/domains/{domain}` | 删除空 Domain（非空返回 409） |
| GET | `/unified/v1/domains/{domain}/namespaces` | 列出 Namespace |
| POST | `/unified/v1/domains/{domain}/namespaces` | 创建 Namespace |
| GET | `/unified/v1/domains/{domain}/namespaces/{ns}` | 获取 Namespace |
| DELETE | `/unified/v1/domains/{domain}/namespaces/{ns}` | 删除空 Namespace |
| PATCH | `/unified/v1/domains/{domain}/namespaces/{ns}` | 更新 Namespace |
| GET | `/unified/v1/domains/{domain}/namespaces/{ns}/assets` | 列出 Asset（支持 `format` / `name` 过滤、分页） |
| GET | `/unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}` | 获取 Asset（无需 `format`，活动名唯一） |
| DELETE | `/unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}` | 删除 Asset |
| PATCH | `/unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}` | 更新 Asset comment/properties |
| POST | `/unified/v1/domains/{domain}/namespaces/{ns}/assets/{name}/rename` | 重命名 Asset |

> Unified API 不提供 Asset 创建端点（`POST /assets` 返回 405）。表/View 创建由各自原生协议承载。

### 5.5 F4：基础设施端点

| 方法 | 路径 | 能力 |
|------|------|------|
| GET | `/healthz` | 存活检查，返回 `{"status":"ok"}` |
| GET | `/readyz` | 就绪检查，执行 `SELECT 1` 验证 DB 连通性；不可达返回 503 |

---

## 6. 非功能需求

| ID | 需求 | 说明 |
|----|------|------|
| NF1 | 技术栈 | Rust（axum 0.8 + tokio-postgres + deadpool-postgres 连接池 + PostgreSQL） |
| NF2 | 无状态 | 服务不依赖本地状态，可水平扩展（多实例共享同一 PostgreSQL） |
| NF3 | 无鉴权 | 不引入认证授权；内网/测试环境使用 |
| NF4 | 单租户 | 不引入租户隔离概念 |
| NF5 | 可观测性 | 集成 `tracing` 打印结构化日志；`/readyz` 含 DB 连通性；scan metrics 接收 |
| NF6 | 数据库初始化 | 使用单一 `init.sql` 脚本初始化（非 migration），脚本幂等可重执行 |
| NF7 | 配置管理 | 环境变量加载配置（支持 `.env`），见 §6.2 |
| NF8 | 优雅关机 | SIGTERM/SIGINT 后停止接受新请求，等待存量请求处理完毕后退出 |
| NF9 | 条件编译 | Cargo feature flags（`lance` / `iceberg` / `unified`）编译时选择协议适配器；默认全部启用 |
| NF10 | 代码质量 | 生产代码禁止 `unwrap()` / `expect()`（workspace clippy lint deny） |
| NF11 | SQL 集中 | 所有 SQL 集中在 `storage/src/queries.rs` 常量模块，禁止字符串拼接 SQL |
| NF12 | 声明即实现 | `/v1/config` 的 `endpoints` 字段只返回实际已实现端点，不得提前声明未实现能力 |

### 6.1 Feature Flags

| Feature | 默认 | 控制内容 | 附加依赖 |
|---------|------|---------|---------|
| `lance` | ✅ 启用 | `adapter/src/lance/` 模块及路由 | — |
| `iceberg` | ✅ 启用 | `adapter/src/iceberg/` 模块及路由 | `iceberg` crate、`object_store` |
| `unified` | ✅ 启用 | `adapter/src/unified/` 模块及路由 | 自动依赖 `lance` + `iceberg` |

> `object_store` 为公共依赖（非 optional），不受 feature flag 控制。允许部署方按需裁剪 binary：`cargo build --no-default-features --features lance`。

### 6.2 配置项（环境变量）

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_DATABASE_URL` | 是 | `postgres://postgres:postgres@localhost:5432/quasar` | PostgreSQL 连接串 |
| `QUASAR_HOST` | 否 | `0.0.0.0` | HTTP 监听地址 |
| `QUASAR_PORT` | 否 | `8080` | HTTP 监听端口 |
| `QUASAR_LOG_LEVEL` | 否 | `info` | tracing 日志级别（回退到 `RUST_LOG`） |
| `QUASAR_WAREHOUSE_PATH` | 否 | — | 默认 warehouse 路径（如 `s3://bucket/warehouse/`） |
| `QUASAR_WAREHOUSE` | 否 | `default` | 默认 warehouse 名称（multi-warehouse 校验用） |
| `QUASAR_S3_ENDPOINT` | 否 | — | S3/MinIO endpoint |
| `QUASAR_S3_ACCESS_KEY` | 否 | — | S3 access key |
| `QUASAR_S3_SECRET_KEY` | 否 | — | S3 secret key |
| `QUASAR_S3_REGION` | 否 | `us-east-1` | S3 region |
| `QUASAR_S3_ALLOW_HTTP` | 否 | `false` | 是否允许 HTTP（非 HTTPS）访问对象存储 |
| `QUASAR_DB_MAX_CONNECTIONS` | 否 | `10` | 连接池最大连接数 |

---

## 7. 数据模型需求

### 7.1 设计原则（核心不变量）

1. **资产身份与类型属性分离**：所有资产共享统一身份层（`assets`）；表、View、模型等类型的专有字段进入各自扩展层。
2. **格式信息不在资产身份层**：`iceberg`、`lance` 等表格式属于表资产属性，存于 `tabular_assets.format`；不属于所有资产的公共身份。
3. **类型与格式可扩展**：新增资产类型或表格式不应要求修改核心表结构；通过注册表（`asset_types` / `tabular_formats`）校验合法值。
4. **版本模型同时保留原生标识与排序语义**：`version_key` 记录格式原生版本标识，`version_order` 提供可比较顺序；禁止从字符串解析 fallback 伪造排序。
5. **核心完整性由数据库约束兜底**：身份关系、唯一性和引用完整性不能只依赖应用层。
6. **删除必须显式且可控**：Domain / Namespace 这类上层容器不得因普通删除语句隐式级联删除大量下游资产；非空容器删除返回冲突。
7. **性能约束与完整性约束并重**：为外键引用方、主要列表查询和 latest-version 查询提供必要索引。
8. **敏感配置不得明文暴露**：Domain 级存储配置如涉及 credential，应存 secret reference 或加密值，并在 API 响应中脱敏。

### 7.2 实体关系

```
domains (1) ──< (N) namespaces (1) ──< (N) assets (1) ──┬──< (N) asset_versions
                                                         │
                                                         ├── (1) tabular_assets (1) ──< (N) tabular_asset_versions
                                                         └── (1) view_assets
```

- `domains` → `namespaces`：`ON DELETE RESTRICT`（非空 Domain 不可删除）。
- `namespaces` → `assets`：`ON DELETE RESTRICT`（非空 Namespace 不可删除）。
- `assets` → `tabular_assets` / `view_assets` / `asset_versions`：`ON DELETE CASCADE`（删除 Asset 连带清理扩展记录，仅由 Asset 删除 API 触发）。
- `asset_versions` → `tabular_asset_versions`：`ON DELETE CASCADE`。
- `asset_versions.previous_version_id` → `asset_versions`：`ON DELETE SET NULL`（前驱被删则断开链接）。

### 7.3 核心表清单

| 表 | 层级 | 职责 | 关键约束 |
|----|------|------|---------|
| `asset_types` | 注册表 | 资产大类名称（`table` / `view`） | `name` PK |
| `tabular_formats` | 注册表 | 表格式（`iceberg` / `lance`）+ `supports_cas_commit` 标志 | `name` PK |
| `domains` | 第一层 | Domain 身份 + 存储配置 + warehouse | `name` UNIQUE |
| `namespaces` | 第二层 | Namespace 身份，归属 Domain | `UNIQUE(domain_id, name)` |
| `assets` | 第三层 | 统一资产身份（`asset_type` + comment + properties + 软删除 + 审计预留） | `uq_assets_active_name` partial unique on `(namespace_id, name) WHERE deleted_at IS NULL` |
| `tabular_assets` | 扩展层 | 表资产 format/location/metadata_location/schema_snapshot | `asset_id` PK→`assets(id)` CASCADE；触发器保证 `asset_type='table'` |
| `view_assets` | 扩展层 | View 资产 view_uuid/location/current_version_id/metadata_location | `asset_id` PK→`assets(id)` CASCADE；触发器保证 `asset_type='view'` |
| `asset_versions` | 版本身份 | `version_key` + `version_order` + `previous_version_id` + comment + properties | `UNIQUE(asset_id, version_key)`；`uq_asset_versions_order` partial unique on `(asset_id, version_order) WHERE version_order IS NOT NULL`；触发器保证 previous 指向同 Asset |
| `tabular_asset_versions` | 版本扩展 | 表版本 metadata_location | `version_id` PK→`asset_versions(id)` CASCADE |
| `asset_permissions` | 预留 | RBAC 权限记录 | 表存在但未暴露 API |
| `iceberg_staged_tables` | 运营 | staged create 记录（弱引用文本键，24h 过期） | `UNIQUE(domain_name, namespace_name, table_name)` |
| `iceberg_scan_metrics_reports` | 运营 | scan metrics 原始 report JSON | `asset_id` ON DELETE SET NULL |
| `iceberg_purge_operations` | 运营 | DROP TABLE PURGE 操作记录 | status: started/catalog_dropped/completed/failed |

> 完整 DDL 与 Rust 模型见 `docs/DESIGN.md` 第四章。

### 7.4 版本模型规范

| 字段 | 类型 | 语义 | 示例 |
|------|------|------|------|
| `version_key` | `TEXT` | 格式原生版本标识，同一 Asset 内唯一 | Lance: `"42"`，模型: `"v1.2.0"` |
| `version_order` | `BIGINT` | 可比较顺序，允许为空 | Lance: `42`，Iceberg: `NULL` |
| `previous_version_id` | `UUID` | 前驱版本 ID（同一 Asset 内） | 指向同 Asset 下的版本 |

**格式特定映射**：

| 格式 | version_key | version_order | latest 查询方式 |
|------|-------------|---------------|-----------------|
| Lance | `"1"`, `"2"`, ... | `1`, `2`, ... | `version_order DESC` |
| Iceberg | 不使用版本表 | — | `tabular_assets.metadata_location` 指针 |

**禁止事项**：
- 禁止 `version_key.parse().unwrap_or(0)` 作为排序 fallback。
- latest 查询只能基于明确的 `version_order` 或格式内权威指针。
- `previous_version_id` 跨 Asset 引用必须被应用层拒绝，并由数据库触发器兜底。

### 7.5 删除与生命周期

- 非空 Domain 不可删除（返回 `DomainNotEmpty` / 409）。
- 非空 Namespace 不可删除（返回 `NamespaceNotEmpty` / 409）。
- 删除 Asset 清理该 Asset 下的扩展记录和版本记录，由明确的 Asset 删除 API 触发。
- Unified API 删除 Catalog 记录时不删除对象存储中的数据文件；标准协议定义的数据删除语义（如 Iceberg `purgeRequested=true`）由对应协议适配器处理。
- 软删除字段 `deleted_at` 已预留；唯一性约束以"活动资产"为边界，已软删除资产不阻塞名称复用。

---

## 8. 并发控制与一致性需求

### 8.1 Iceberg CAS Commit（Table）

完整流程：

1. 客户端发送 commit 请求，携带 `requirements` 和 `updates`。
2. 适配器从对象存储加载当前 metadata.json，反序列化为 `TableMetadata`。
3. 校验 requirements（如断言 `metadata_location` 仍为 V1）。
4. 应用 updates 生成新 `TableMetadata`，序列化为新 metadata.json。
5. 写入对象存储（路径 `metadata/0000N-{uuid}.metadata.json`，版本号单调推进）。
6. 以 `metadata_location = V1` 为 CAS 条件，原子更新数据库指针为 V2。
7. 若 CAS 不满足（metadata_location 已变），返回 `409 CommitFailedException`。

> TableMetadata 的解析、updates 应用、序列化使用 `iceberg` crate 0.9.1（`TableMetadataBuilder` / `TableRequirement::check()` / `TableUpdate::apply()`），保证与 Spark Iceberg 1.10.x client 的元数据兼容性。

### 8.2 Iceberg CAS Commit（View）

View commit 与 Table commit 采用相同分层架构（adapter 层校验 + Store 层 CAS），差异：

- `ViewRequirement`（`assert-create` / `assert-view-uuid`）由 adapter 层自定义实现（iceberg crate 0.9.1 不提供此类型）。
- `ViewUpdate` 使用 iceberg crate 0.9.1 的 `ViewUpdate` enum，逐项 match 调用 `ViewMetadataBuilder` 方法（ViewUpdate 无 `apply()` 方法）。
- Version History 清理由 `ViewMetadataBuilder::build()` 内置 `expire_versions()` 处理，默认保留 10 个版本（`version.history.num-entries`）。
- View 无 staged-create 流程，Create View 直接创建 active 记录。

### 8.3 Staged Create Commit

- staged create 只写对象存储 metadata file 和 `iceberg_staged_tables`，不写 `assets`。
- commit 阶段在单个 PostgreSQL 事务内：校验 active table 不存在 → 锁定未过期 staged record → 应用 updates → 写最终 metadata → 插入 `assets` + `tabular_assets` → 删除 staged record。
- staged record 默认 24h 过期；过期 record 由后续同名 staged create / commit 同步清理。

### 8.4 Multi-table Transactions

- `POST /v1/{prefix}/transactions/commit` 采用"对象存储预写 + PostgreSQL 事务原子提交"方案。
- Phase 1（无 DB）：解析请求 → 按表名分组 → 校验所有表存在 → 逐表读取 metadata、校验 requirements、应用 updates、写新 metadata 到对象存储。
- Phase 2（PostgreSQL 事务）：按 `(domain, namespace, table)` 字典序逐表执行 CAS update；任一 CAS 失败则事务回滚，返回 409。
- 对象存储写入失败返回 500，不执行任何 DB 操作。
- 死锁预防：事务与单表 commit 使用一致的字典序锁顺序。
- 容忍对象存储孤儿 metadata 文件（已写入但 CAS 失败），客户端收到 5xx/409，日志含可定位路径与 commit 上下文。

### 8.5 Lance 版本唯一性

- `INSERT INTO asset_versions` 违反 `UNIQUE(asset_id, version_key)` → 返回 `409 TableVersionAlreadyExists`。
- Lance 版本注册本质是"注册已存在的版本"，不是协调写入；S3 与 DB 可能短暂不一致（最终一致）。

### 8.6 Namespace / Asset 属性增量更新

- properties 增量更新使用单条原子 SQL：`properties = (properties - removals) || updates`，避免 read-modify-write 覆盖。

### 8.7 失败窗口与孤儿文件

- 对象存储写成功但 PostgreSQL CAS 失败：客户端收到 5xx 或 409；服务端日志含 domain/namespace/table/old+new metadata_location；容忍孤儿 metadata file，后台清理推迟。
- PostgreSQL CAS 成功后响应发送失败：catalog 状态以 PostgreSQL 为准；客户端重试时通过 load 看到新 metadata_location。

---

## 9. 错误处理需求

### 9.1 两层错误体系

Quasar 内部使用统一的 `StoreError`，各协议适配器在返回 HTTP 响应时映射为各自规范定义的错误格式。

### 9.2 Iceberg 端点错误格式

```json
{
  "error": {
    "message": "...",
    "type": "...",
    "code": 409
  }
}
```

| 场景 | HTTP | error.type |
|------|------|------------|
| Namespace 不存在 | 404 | `NoSuchNamespaceException` |
| Namespace 已存在 | 409 | `NamespaceAlreadyExistsException` |
| Namespace 非空 | 409 | `NamespaceNotEmptyException` |
| Domain 非空 | 409 | `NamespaceNotEmptyException` |
| Table 不存在 | 404 | `NoSuchTableException` |
| Table 已存在 | 409 | `AlreadyExistsException` |
| View 不存在 | 404 | `NoSuchViewException` |
| View 已存在 | 409 | `ViewAlreadyExistsException` |
| 同名 Table/View 冲突 | 409 | `AlreadyExistsException` |
| Snapshot 不存在 | 404 | `NoSuchSnapshotException` |
| Warehouse 不存在 | 404 | `NoSuchWarehouseException` |
| Requirement 不满足 / CAS 冲突 | 409 | `CommitFailedException` |
| 已识别但未支持的 update | 501 | `NotImplementedException` |
| 请求参数无效 / 未知 discriminator | 400 | `BadRequestException` |
| metadata file 不存在 | 404 | `NoSuchMetadataException` |
| 对象存储 transient failure | 503/504 | 依据 `StoreError` transient 分类 |
| 内部错误 | 500 | `InternalServerError` |

### 9.3 Lance 端点错误格式

遵循 RFC-7807 Problem Details：

```json
{
  "error": "NamespaceNotFound",
  "code": 404,
  "detail": "Namespace 'prod' not found",
  "instance": "/lance/v1/namespace/prod/describe"
}
```

| 场景 | HTTP | error |
|------|------|-------|
| Namespace 已存在 | 409 | `NamespaceAlreadyExists` |
| Namespace 不存在 | 404 | `NamespaceNotFound` |
| Namespace 非空 | 409 | `NamespaceNotEmpty` |
| Table 已存在 | 409 | `TableAlreadyExists` |
| Table 不存在 | 404 | `TableNotFound` |
| 版本已存在 | 409 | `TableVersionAlreadyExists` |
| 请求参数无效 | 400 | `InvalidInput` |
| 内部错误 | 500 | `InternalError` |

### 9.4 Unified API 错误格式

RFC-7807 Problem Details 风格，`Content-Type: application/problem+json`，扩展 `code` 与 `request_id`：

```json
{
  "type": "https://quasar.dev/problems/asset-not-found",
  "title": "Asset not found",
  "status": 404,
  "detail": "Asset 'prod.users' not found",
  "instance": "/unified/v1/domains/prod/namespaces/analytics/assets/users",
  "code": "AssetNotFound",
  "request_id": "018f6f2f-..."
}
```

> Unified API 的 Problem Details 错误格式不得泄漏到 `/iceberg/v1/...` 与 `/lance/v1/...` 标准协议端点。

### 9.5 脱敏规则

- 客户端错误响应不得包含数据库 SQL、S3 secret、完整连接串、对象存储 credential。
- 服务端日志可包含 metadata location 和 table location，但不得包含 credential。
- `StoreError::Internal { source }` 只在日志中保留 source，响应使用脱敏 message。

---

## 10. 明确不做的事项

| # | 不做项 | 理由 / 演进方向 |
|---|--------|----------------|
| 1 | Iceberg REST Catalog 全量官方端点覆盖声明 | 只承诺 Spark 3.5 + Iceberg 1.10.x E2E 必需路径 |
| 2 | `POST /v1/oauth/tokens` OAuth token endpoint | 生产鉴权另行设计 |
| 3 | Vended credentials（`GET .../tables/{table}/credentials`） | 推迟到后续安全专项版本 |
| 4 | S3 Signer API（`POST /v1/aws/s3/sign`） | 推迟到后续安全专项版本 |
| 5 | Encryption key actions（`add-encryption-key` / `remove-encryption-key`） | 推迟到后续安全专项版本 |
| 6 | Iceberg table-format spec v3 支持 | 仅支持 v2 |
| 7 | `POST .../register-view` 端点 | 不属于 Iceberg 1.10.x 官方端点集合 |
| 8 | View Version 历史回滚 / 审计 API | 仅支持当前 version 加载和 replace |
| 9 | Scan Planning 结果持久化 / 调度优化 | Plan 结果仅会话内有效；不引入任务队列、并行度控制 |
| 10 | View/Table 跨资源引用验证 | View SQL 中引用 Table 存在性由客户端负责 |
| 11 | Metadata cache（Iceberg 实时读取对象存储） | 性能优化，推迟到后续版本 |
| 12 | Cursor-based 分页 | 当前基于 OFFSET 的简单分页 |
| 13 | 对象存储孤儿文件后台清理 | 推迟到后续版本（容忍孤儿，日志可诊断） |
| 14 | Catalog-Aware Lance Commits | 维持客户端写 S3 + 事后注册；关注 Lance 社区 RFC 进展 |
| 15 | 完整 RBAC | `asset_permissions` 表预留但不暴露 API |
| 16 | 嵌套 Namespace | 单层，后续单独设计 |
| 17 | 多 warehouse 存储后端隔离 | 仅参数校验层（`NoSuchWarehouseException`），不扩展配置模型 |
| 18 | 多租户 / 鉴权 / 审计 | 内网/测试环境使用 |
| 19 | Unified API 暴露 Asset 创建 | 创建由 Iceberg/Lance 原生协议承载（`POST /assets` 返回 405） |
| 20 | Lance 新端点扩展（数据面 / Index / Tag / Schema 变更等） | 维持 V3 现状 |
| 21 | Statistics 文件存在性校验 | 不校验，容忍引用不存在文件（与 Iceberg Java 实现一致） |
| 22 | V3 存量数据自动转换工具 | 全新部署，从空数据库开始 |

---

## 11. 验收标准

### 11.1 Schema 与约束验收

- 从空数据库按 `init.sql` 初始化成功，服务可正常启动；脚本幂等可重执行。
- 核心表与扩展表字段边界符合 §7.3；外键删除行为符合 §7.2。
- 约束覆盖：非法 `asset_type` / `format` 被注册表拒绝；同名活动 Asset 冲突被 `uq_assets_active_name` 拒绝；重复 `version_key` / 重复非空 `version_order` 被拒绝；`previous_version_id` 跨 Asset 引用被触发器拒绝；`tabular_assets` / `view_assets` 引用非 table/view Asset 被触发器拒绝。

### 11.2 Iceberg REST Catalog 验收

- Namespace / Table / View / Transaction / Scan Planning 端点路径、请求/响应格式、错误格式遵循 Iceberg 1.10.x 规范。
- staged create：返回 staged metadata，list/load/head 不可见，commit 阶段 `assert-create` 创建 catalog 记录。
- register table：读取外部 metadata file 校验后注册，不重写对象存储。
- commit：8 项 requirement 成功/失败路径覆盖；已实现 update 成功路径；未支持 update 返回 501；未知 action 返回 400。
- purge：`purgeRequested=false` 只删 catalog；`true` 删 catalog + 对象存储表路径，失败记录 operation。
- metrics：接收 scan report 并持久化，返回 204。
- 并发 CAS：真实并行 commit（testcontainers-postgres）仅一个成功，其余 409。
- View：7 端点全生命周期；View CAS 冲突测试；同名 Table/View 冲突返回 409。
- Scan Planning：4 端点 + Java ResourcePaths alias；plan-task token 自包含；无效 token 返回 400。
- `/v1/config` 的 `endpoints` 字段与实际实现一致。

### 11.3 Spark E2E 验收

- Spark 3.5 + Iceberg 1.10.x + MinIO + Postgres + Quasar 端到端跑通：
  - `CREATE TABLE` / `CREATE TABLE IF NOT EXISTS` / DataFrame `.writeTo().create()` / `INSERT INTO` / `SELECT` / `DROP TABLE` / `DROP TABLE PURGE`
  - schema evolution（ADD / DROP / RENAME / ALTER COLUMN）
  - partition transform（`identity` + 时间型 `days`/`hours` + `bucket(N)`）
  - snapshot read（time travel）、create/drop branch、create/drop tag、`expire_snapshots`
  - `CREATE VIEW` / `SELECT FROM VIEW` / `DROP VIEW`
- Quasar 写出的 metadata.json 能被 Spark Iceberg 1.10.x client 直接读取并打印 schema/snapshot/partition spec/table uuid。

### 11.4 Lance REST Namespace 验收

- Namespace / Table / Version 端点路径、请求/响应格式、错误格式遵循 Lance REST Namespace 规范。
- `{id}` 解析覆盖 root / Domain / Namespace / Table / 段数过多 / 空段 / URL 编码。
- `declare` / `register` 不创建初始版本，`current_version` 返回 null；`create_table_version` 后写入 `asset_versions` + `tabular_asset_versions`。
- Lance Python SDK 端到端跑通（创建 Namespace → DeclareTable → 写数据 → 注册版本 → 读回验证 → 追加 → 列版本 → 清理）。

### 11.5 Unified API 验收

- Domain CRUD + 存储配置脱敏；Namespace CRUD；Asset 列表/获取/删除/重命名/更新。
- Asset 单资源操作无需 `format` 参数；列表支持 `format` / `name` 过滤与分页。
- `POST /assets` 返回 405 且不创建任何 Catalog 记录。
- PATCH 三态语义（comment 缺省不变 / null 清空 / 值覆盖）；properties 增量更新。
- 错误格式为 Problem Details，不泄漏到标准协议端点。

### 11.6 事务一致性验收

- Iceberg create table 写对象存储失败时，DB 不产生记录。
- `assets` + `tabular_assets` 双行写入事务原子；Lance `asset_versions` + `tabular_asset_versions` 双行写入事务原子。
- Iceberg CAS commit 对象存储写成功但 DB CAS 失败时返回 409，指针不更新。
- 多表事务：部分表 CAS 失败时全部回滚；对象存储写入失败不进入 DB。
- 并发创建同 Namespace/同名 Asset/同 version 仅一个成功。

### 11.7 构建、Feature 与无状态验收

- `cargo fmt --all --manifest-path quasar/Cargo.toml` 通过。
- `cargo test --all-features --all-features --all-targets` 全部通过（测试矩阵见 `docs/TEST_MATRIX.md`）。
- `cargo clippy --workspace --tests` 无错误（workspace 禁止 `unwrap`/`expect`）。
- Feature flag 可裁剪：`--no-default-features --features lance` / `--features iceberg` / `--features unified` 均可构建。
- 启动两个 server 实例连接同一 PostgreSQL，读写 smoke test 不依赖进程内状态。

### 11.8 部署验收

- 容器化部署（Dockerfile + docker-compose）正常；`/healthz` / `/readyz` 正常；DB 不可达时 `/readyz` 返回 503。
- 优雅关机：SIGTERM 后停止 accept，等待存量请求，关闭连接池，退出。
- 部署与验证流程见 `docs/DEPLOYMENT.md`。

---

## 附录：参考资料

- Apache Iceberg REST Catalog Spec: <https://iceberg.apache.org/rest-catalog-spec/>
- Apache Iceberg 1.10.0 OpenAPI: <https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml>
- Apache Iceberg Views Documentation: <https://iceberg.apache.org/docs/nightly/views/>
- iceberg crate (Rust): <https://docs.rs/iceberg/>
- Lance REST Namespace Catalog Spec: <https://lance.org/format/namespace/rest/catalog-spec/>
- Lance REST Namespace Implementation Spec: <https://lance.org/format/namespace/rest/impl-spec/>
- RFC 7807 Problem Details: <https://datatracker.ietf.org/doc/html/rfc7807>
