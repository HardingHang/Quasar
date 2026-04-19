# V1 (MVP) 需求澄清与分析

## 交付阶段

MVP 分两个阶段交付。核心模型和 crate 分层从一开始就按双协议设计，但端点实现分阶段推进。

- **Phase 1**：Lance REST Namespace（F4）+ Namespace 管理（F2）+ 基础设施端点（F6）。里程碑：Lance SDK（Python）和 Spark（lance-spark）端到端跑通。
- **Phase 2**：Iceberg REST Catalog（F1 + F3）+ Spark 集成验证（F5）。里程碑：Spark 通过 Iceberg REST Catalog 端到端读写 Iceberg 表。

## 功能需求

| ID | 需求 | 阶段 | 说明 |
|----|------|------|------|
| F1 | Iceberg REST Catalog 协议支持 | Phase 2 | 实现 Iceberg REST OpenAPI 中的核心端点（详见下方端点清单），路径前缀 `/iceberg/v1/...` |
| F2 | Namespace 管理 | Phase 1 | 支持创建、删除、列出 Namespace。**MVP 阶段限定为单级 Namespace**。Iceberg 和 Lance 维护各自独立的 Namespace 空间（详见 Namespace 隔离策略）。 |
| F3 | Iceberg Table 元数据管理 | Phase 2 | 存储并返回 `metadata-location`、`snapshot-id`、`schema` 等关键字段。实际的 Iceberg metadata.json 仍存放在对象存储中，Catalog 只存指针。**Table 提交必须实现乐观并发控制**（详见下方说明）。 |
| F4 | Lance REST Namespace 协议支持 | Phase 1 | 实现 Lance REST Namespace 规范中的基础操作及版本管理端点（详见下方端点清单），路径前缀 `/lance/v1/...`，确保 Lance SDK（Python/Rust/Java）、Spark（lance-spark）、Ray 等引擎零改动接入。 |
| F5 | Spark 集成验证（Iceberg） | Phase 2 | 提供测试脚本，验证 Spark 3.4+ 通过 Iceberg REST Catalog 成功读写 Iceberg 表（`uri=http://host:port/iceberg`）。 |
| F6 | 基础设施端点 | Phase 1 | 提供 `GET /healthz`（存活检查）和 `GET /readyz`（就绪检查，含 DB 连通性验证），支持容器化部署。 |

---

### Namespace 隔离策略

Quasar 需要同时支持 Iceberg 和 Lance 两种数据格式，每种格式有其独立的客户端生态和标准协议。Iceberg 客户端（Spark、Trino、Flink）和 Lance 客户端（LanceDB Python SDK、lance-namespace Rust crate）对 Namespace/Table 的命名规则和访问路径有不同假设。为避免协议间的命名冲突和语义干扰，同时允许用户为同一份业务数据同时维护两种格式的表（例如 Iceberg 的 `prod.users` 和 Lance 的 `prod.users` 互不冲突），Quasar 将两套协议的 Namespace 空间设计为相互隔离：

- 通过 Iceberg 端点创建的 Namespace/Table 只在 `/iceberg/v1/...` 下可见。
- 通过 Lance 端点创建的 Namespace/Table 只在 `/lance/v1/...` 下可见。
- 两个空间中**允许同名**，互不干扰。

核心模型层面，唯一约束为 `(namespace_name, table_name, format)`，其中 `format` 取值为 `iceberg` 或 `lance`。各协议适配器在查询和写入时自动注入对应的 `format` 值，确保不同格式的资产在逻辑上隔离、在物理上共存。

---

### F1 端点清单（Iceberg REST Catalog）— Phase 2

路径前缀：`/iceberg/v1/...`

其中 `/v1` 是 Iceberg REST OpenAPI 规范自带的版本前缀。Spark 等引擎配置时只需设置 `uri=http://host:port/iceberg`，引擎会自动拼接 `/v1/...`。

**MVP 必须实现：**

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/config` | 返回 Catalog 配置（warehouse 地址、默认值等） |
| GET | `/v1/namespaces` | 列出 Namespace（支持 `pageToken` / `pageSize`） |
| POST | `/v1/namespaces` | 创建 Namespace |
| GET | `/v1/namespaces/{ns}` | 获取 Namespace 详情及 properties |
| DELETE | `/v1/namespaces/{ns}` | 删除 Namespace（仅允许删除空 Namespace） |
| POST | `/v1/namespaces/{ns}/properties` | 更新 Namespace properties |
| GET | `/v1/namespaces/{ns}/tables` | 列出 Table（支持 `pageToken` / `pageSize`） |
| POST | `/v1/namespaces/{ns}/tables` | 创建 Table（接收 metadata-location 或内联 metadata） |
| GET | `/v1/namespaces/{ns}/tables/{table}` | 加载 Table（返回 metadata-location、schema、current-snapshot-id 等） |
| POST | `/v1/namespaces/{ns}/tables/{table}` | 提交 Table 更新（**必须校验 requirements 实现 CAS**） |
| DELETE | `/v1/namespaces/{ns}/tables/{table}` | 删除 Table |
| HEAD | `/v1/namespaces/{ns}/tables/{table}` | 检查 Table 是否存在 |
| POST | `/v1/tables/rename` | 重命名 Table（同 Namespace 内） |

**MVP 明确不实现：**

- `POST /v1/transactions/commit`（多表事务）
- View 相关端点（`/v1/namespaces/{ns}/views/...`）
- `POST /v1/oauth/tokens`（OAuth 认证）
- `POST /v1/namespaces/{ns}/tables/{table}/metrics`（指标上报）

---

### F3 乐观并发控制说明

Iceberg REST 规范要求服务端深度参与 commit 流程。完整流程如下：

1. 客户端发送 Commit 请求，携带 `requirements`（断言条件，如 `assert-current-table-metadata`）和 `updates`（变更操作列表，如 `add-snapshot`、`set-current-schema` 等）。
2. **Iceberg 适配器从对象存储加载当前 metadata.json，反序列化为 TableMetadata 对象**。
3. **适配器校验 requirements 是否满足**（如断言 metadata_location 仍为 V1）。
4. **适配器将 updates 逐一应用到 TableMetadata 上，生成新的 TableMetadata 对象**。
5. **适配器将新 TableMetadata 序列化为新的 metadata.json（V2），写入对象存储**（路径由适配器决定，通常为递增编号或 UUID）。
6. 适配器调用 CatalogStore，以 `metadata_location = V1` 为 CAS 条件，原子更新数据库中的 `metadata_location` 为 V2 及其他相关字段。
7. 若 CAS 条件不满足（metadata_location 已不是 V1），返回 `409 Conflict`。

**关键决策**：Quasar 选择自己实现 TableMetadata 的解析、updates 应用和 metadata.json 序列化逻辑，不依赖外部库（如 `iceberg-rust`）的 TableMetadata 能力。这确保了协议语义的完全可控，但增加了实现工作量。

**CAS SQL**（CatalogStore 层）：
```sql
UPDATE assets
SET metadata_location = $new_metadata_location,
    location = COALESCE($new_location, location),
    schema_snapshot = COALESCE($new_schema_snapshot, schema_snapshot),
    properties = (properties - $removals::text[]) || $updates::jsonb,
    updated_at = NOW()
WHERE namespace_id = $namespace_id
  AND name = $name
  AND metadata_location = $expected_metadata_location;
```

---

### F4 端点清单（Lance REST Namespace）— Phase 1

路径前缀：`/lance/v1/...`

其中 `/v1` 是 Lance REST Namespace 规范自带的版本前缀。Lance 客户端配置时设置 `uri=http://host:port/lance`，SDK 会自动拼接 `/v1/...`。

Lance REST Namespace 使用 `$` 作为默认分隔符将多级 Namespace 标识符序列化到路径中（如 Namespace `["prod", "analytics"]` 序列化为 `prod$analytics`）。MVP 阶段限定单级 Namespace，`{id}` 即为 Namespace 名称本身。

**MVP 必须实现：**

Namespace 操作：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/namespace/{id}/create` | 创建 Namespace |
| GET | `/v1/namespace/{id}/list` | 列出子 Namespace（支持 `page_token` / `limit`） |
| POST | `/v1/namespace/{id}/describe` | 获取 Namespace 详情及 properties |
| POST | `/v1/namespace/{id}/drop` | 删除 Namespace（仅允许删除空 Namespace） |
| POST | `/v1/namespace/{id}/exists` | 检查 Namespace 是否存在 |

Table 基础操作：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/table/{id}/declare` | 声明 Lance 表（预留表名，返回分配的 location 和 storage_options，不创建数据文件） |
| GET | `/v1/namespace/{id}/table/list` | 列出指定 Namespace 下的 Lance 表（支持 `page_token` / `limit`） |
| POST | `/v1/table/{id}/describe` | 获取 Lance 表详情（location、schema、version） |
| POST | `/v1/table/{id}/register` | 注册已有 Lance 表（提供 location） |
| POST | `/v1/table/{id}/deregister` | 注销 Lance 表（保留存储上的数据文件） |
| POST | `/v1/table/{id}/drop` | 删除 Lance 表（同时移除数据） |
| POST | `/v1/table/{id}/exists` | 检查 Lance 表是否存在 |
| POST | `/v1/table/{id}/rename` | 重命名 Lance 表 |

Table 版本管理（客户端写入数据后注册新版本的必要路径）：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/table/{id}/version/create` | 注册新版本。客户端写入数据文件并生成 manifest 后，提交 `{"version": N, "manifest_path": "...", "naming_scheme": "V2"}`。Catalog 存储 `version → manifest_path` 映射；若版本号已存在则返回 `409 Conflict`（`TableVersionAlreadyExists`） |
| GET | `/v1/table/{id}/version/list` | 列出版本历史（支持 `descending` / `limit`） |
| POST | `/v1/table/{id}/version/describe` | 获取指定版本的详细信息（含 manifest_path、naming_scheme） |

**MVP 明确不实现：**

- 数据面操作：Insert / MergeInsert / Update / Delete / Query / Count（Lance 数据读写由客户端通过 location 直接访问对象存储完成，不经过 Catalog）
- 批量版本操作：BatchCreateTableVersions / BatchDeleteTableVersions / RestoreTable
- Index 管理：CreateTableIndex / ListTableIndices / DropTableIndex 等
- Tag 管理：CreateTableTag / DeleteTableTag / UpdateTableTag 等
- 事务管理：DescribeTransaction / AlterTransaction
- Schema 变更：AlterTableAddColumns / AlterTableAlterColumns / AlterTableDropColumns
- GetTableStats / UpdateTableSchemaMetadata
- ListAllTables（跨 Namespace 列出所有表）
- CreateTable（含初始数据的建表，MVP 阶段通过 DeclareTable + 客户端写入 + CreateTableVersion 流程替代）

**错误响应格式：** Lance REST Namespace 规范要求遵循 RFC-7807，返回包含 `error`、`code`、`detail`、`instance` 字段的 JSON 错误体，错误码到 HTTP 状态码的映射见规范定义（如 NamespaceNotFound → 404，TableAlreadyExists → 409，InvalidInput → 400 等）。

---

## 非功能需求

| ID | 需求 | 说明 |
|----|------|------|
| NF1 | 技术栈 | Rust（axum + tokio-postgres + deadpool-postgres 连接池 + PostgreSQL） |
| NF2 | 无状态 | 服务不依赖本地状态，可水平扩展 |
| NF3 | 无鉴权 | MVP 阶段不引入认证授权，内网或测试环境使用 |
| NF4 | 单租户 | MVP 阶段不引入租户隔离概念 |
| NF5 | 可观测性 | 集成 `tracing` 打印结构化日志，便于调试 |
| NF6 | 数据库迁移 | 使用 refinery 管理 DDL 版本，迁移脚本纳入版本控制 |
| NF7 | 配置管理 | 通过环境变量加载配置（数据库连接串、监听端口、warehouse 默认路径等），支持 `.env` 文件 |
| NF8 | 优雅关机 | 收到 SIGTERM 后停止接受新请求，等待存量请求处理完毕后退出 |

## 关键约束与决策

1. **存储**：主存储为 PostgreSQL，使用 `tokio-postgres` + `deadpool-postgres`（连接池）进行异步访问。
2. **Namespace 层级**：MVP 限定为**单级 Namespace**。
3. **Namespace 隔离**：Iceberg 和 Lance 维护各自独立的 Namespace 空间，允许同名 Namespace 和 Table 在不同格式下共存。`namespaces` 表唯一约束为 `(name, format)`，`assets` 表唯一约束为 `(namespace_id, name)`（format 由所属 Namespace 决定，Asset 自身不冗余存储）。
4. **双协议适配**：Quasar 同时暴露 Iceberg REST Catalog（`/iceberg/v1/...`）和 Lance REST Namespace（`/lance/v1/...`）两套标准协议端点，后端共享统一的核心模型和存储层。两套协议中的 `/v1` 均为上游规范自带的路径前缀。
5. **对象存储**：Catalog 服务本身不管理对象存储（S3/OSS）。Iceberg 的 `metadata-location` 和 `location` 由客户端/引擎在创建表时提供；Lance 表的 `location` 由 DeclareTable 操作时分配或由注册方提供。
6. **Schema 变更（Evolution）**：MVP 阶段 Iceberg 表的 Schema 变更由引擎端发起，Catalog 只负责更新元数据指针。
7. **并发控制**：Iceberg Table 的 Commit 操作必须实现基于 `requirements` 的乐观并发校验，不满足条件时返回 `409 Conflict`。
8. **分页**：Iceberg 端点使用 `pageToken` + `pageSize`，Lance 端点使用 `page_token` + `limit`，分别遵循各自上游规范。MVP 阶段可先实现基于 offset 的简单分页。
9. **错误响应**：Iceberg 端点使用 Iceberg REST 规范定义的 ErrorResponse 格式（含 `error.message`、`error.type`、`error.code`）；Lance 端点使用 Lance 规范定义的 RFC-7807 格式（含 `error`、`code`、`detail`、`instance`）。各协议保持各自的错误格式，不混用。
10. **Lance 版本管理**：Catalog 不扫描 Lance 数据文件，也不推断版本数量。它仅维护精确的 `(table_id, version, manifest_path)` 映射。客户端在写入数据后显式调用 `CreateTableVersion` 注册版本，并发冲突通过版本号唯一性约束（`409 Conflict`）协调。这与 Iceberg 的 `metadata-location` 指针设计本质一致，只是 Lance 使用显式数字版本号。

---

## 修订记录

### V1.0

- 新增：MVP Phase 1 功能需求（Lance REST Namespace + Namespace 管理 + 基础设施端点）
- 新增：MVP Phase 2 功能需求（Iceberg REST Catalog + Iceberg Table 元数据管理 + Spark 集成验证）
- 新增：非功能需求（NF1~NF8：技术栈、无状态、无鉴权、单租户、可观测性、数据库迁移、配置管理、优雅关机）
- 新增：F1 端点清单（Iceberg REST Catalog）—— 13 个必须实现端点 + 4 个明确不实现端点
- 新增：F3 Iceberg 乐观并发控制说明 —— 7 步服务端 commit 流程 + CAS SQL
- 新增：F4 端点清单（Lance REST Namespace）—— 16 个必须实现端点 + 12 个明确不实现端点
- 新增：Namespace 隔离策略 —— Iceberg 和 Lance 各自独立空间，允许同名，核心约束 `(namespace_name, table_name, format)`
- 新增：9 条关键约束与决策（存储、Namespace 层级、隔离策略、双协议适配、对象存储、Schema 变更、并发控制、分页、错误响应）

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。