# Quasar V4.0 设计说明书

> **版本**: V1.6
> **日期**: 2026-05-19
> **状态**: V4.0 设计基线（评审修订后）
>
> 本文档只承接 V4.0 "Spark-ready Iceberg baseline" 的实现设计。
> V4.1 及以后的候选小版本能力不在本文档中展开设计。
>
> 官方 REST API 来源、端点全集和小版本范围矩阵以
> `docs/v4/V4_OFFICIAL_REST_API.md` 为准；本文档只描述 Quasar 的 V4.0
> 实现选择。

---

## 目录

- [一、执行摘要](#一执行摘要)
- [二、架构总览](#二架构总览)
- [三、依赖与类型策略](#三依赖与类型策略)
- [四、数据模型设计](#四数据模型设计)
- [五、Iceberg REST API 设计](#五iceberg-rest-api-设计)
- [六、Commit 与 Metadata 设计](#六commit-与-metadata-设计)
- [七、对象存储与 Purge 设计](#七对象存储与-purge-设计)
- [八、错误处理设计](#八错误处理设计)
- [九、测试策略](#九测试策略)
- [十、实现顺序](#十实现顺序)
- [十一、V4.0 不做什么](#十一v40-不做什么)
- [十二、修订记录](#十二修订记录)

---

## 一、执行摘要

### 1.1 V4.0 核心目标

V4.0 的核心目标是把 Quasar 从 V3 的 "Iceberg REST 协议骨架可用"
推进到 **Spark 3.5 + Iceberg 1.10.x 可稳定接入的 REST Catalog baseline**。

V4.0 不追求 Iceberg REST Catalog 全量能力覆盖，只交付 Spark 基础 DDL/DML
与元数据兼容所需路径：

- Spark 能通过 Quasar 创建、加载、提交、删除 Iceberg 表。
- Quasar 写出的 `metadata.json` 能被 Spark Iceberg 1.10.x client 读回。
- staged create、register table、commit requirements/actions、scan metrics、
  `DROP TABLE PURGE` 进入可测试闭环。
- V3 的 Domain / Namespace / Asset / Tabular 数据核心模型继续作为目录身份层。
- Lance REST Namespace 与 Unified Domain API 维持 V3 行为，只接受共享模型引发的兼容修复。

### 1.2 V3 -> V4.0 关键设计变更

| 设计域 | V3 状态 | V4.0 设计 |
|--------|---------|-----------|
| Iceberg metadata 类型 | 自研 `TableMetadata` DTO | 引入 `iceberg` crate 作为兼容性基线，Quasar 保留 REST wrapper |
| staged create | commit handler 先查表，`assert-create` 表不存在路径不可达 | 增加 staged table 记录，commit 阶段验证 `assert-create` 并创建 catalog 记录 |
| register table | Iceberg endpoint 未实现 | 读取外部 metadata file，验证后注册 catalog 记录，不重写 metadata |
| commit requirements | 只覆盖 V3 子集 | 覆盖 Iceberg 1.10.x table 适用 8 项 requirement |
| commit updates | 只覆盖 V3 子集，`add-spec` wire name 错误 | 覆盖 V4.0 必需 update，修正 wire name，候选项明确拒绝 |
| scan metrics | endpoint 未实现 | 接收并持久化原始 report JSON，便于测试和排障 |
| purge | `purgeRequested=true` 返回 501 | 执行 catalog drop + 表路径对象清理，并记录 purge 结果 |
| 并发测试 | 缺少真实并行 CAS 覆盖 | 引入容器化 Postgres 或等价隔离方案验证真实冲突 |

### 1.3 V4.0 设计原则

1. **官方协议路径优先**：REST 路径、请求/响应字段和错误模型对齐 Iceberg 1.10.x。
2. **声明即实现**：`/v1/config` 的 `endpoints` 只返回 V4.0 实际支持 endpoint。
3. **metadata 兼容优先**：能被 Spark 读回比自研 DTO 简洁性更重要。
4. **Catalog 状态由 PostgreSQL 仲裁**：`metadata_location` 仍是当前元数据指针，CAS 由数据库条件更新保证。
5. **对象存储失败可诊断**：V4.0 容忍 object store 与 PostgreSQL 的非原子窗口，但必须返回明确错误并记录路径上下文。
6. **候选能力不提前设计成承诺**：credentials、transactions、views、scan planning、metadata cache、后台 orphan cleanup 不进入 V4.0 设计。

---

## 二、架构总览

### 2.1 V4.0 组件边界

```
Spark Iceberg 1.10.x client
        |
        v
  /iceberg/v1/{prefix}/...
        |
        v
quasar-adapter::iceberg
  - REST DTO wrapper
  - Iceberg error mapping
  - metadata read/write orchestration
  - staged create / register / commit / purge / metrics handlers
  - state type: Arc<dyn IcebergCatalogStore>
        |
        v
quasar-core
  - V3 Store trait family (CatalogStore)
  - V4.0 IcebergCatalogStore = CatalogStore + CasCommitStore
    + IcebergStagingStore + IcebergRegisterStore
    + IcebergMetricsStore + IcebergPurgeStore
  - StoreError semantic errors
        |
        v
quasar-storage
  - PostgreSQL catalog identity rows
  - metadata_location CAS
  - staged table records
  - scan metrics reports
  - purge operation records
        |
        v
PostgreSQL + S3/MinIO object store
```

### 2.2 Domain 映射保持不变

V4.0 沿用 V3 的官方兼容映射：

| Quasar | Iceberg REST |
|--------|--------------|
| `Domain.name` | `{prefix}` |
| `Namespace.name` | `{namespace}` |
| `Asset.name` | `{table}` |
| `tabular_assets.format = 'iceberg'` | Iceberg endpoint 可见性过滤 |

示例：

```text
GET /iceberg/v1/prod/namespaces/analytics/tables/events
```

其中 `prod` 是 Quasar Domain，也是 Iceberg `{prefix}`。

### 2.3 Lance / Unified 边界

V4.0 不扩展 Lance 和 Unified API：

- Lance endpoint 继续通过 `{id}` 第一段承载 Domain。
- Unified API 继续只承担 Domain 和跨格式只读聚合职责。
- 任何 V4.0 Iceberg 数据模型调整不得破坏 Lance 已有注册、注销、版本查询行为。

---

## 三、依赖与类型策略

### 3.1 引入 `iceberg` crate

V4.0 采用 Apache Iceberg 官方 Native Rust 实现 `iceberg` crate 作为
metadata 兼容性基线。

依赖规则：

- 在 `quasar/adapter` 引入 `iceberg`，用于解析、构建和序列化 Iceberg table metadata。
- `quasar/core` 不直接依赖 `iceberg`，避免核心 store trait 与 Iceberg 实现细节耦合。
- `quasar/storage` 只存储 JSONB 和 catalog 指针，不依赖 `iceberg` 类型。
- patch 版本必须在 `Cargo.lock` 中固定；V4.0 实现启动时以 crates.io 上的稳定版本为准，当前设计基线采用 `iceberg = "0.9.1"`。

### 3.1.1 Crate 能力验证

V4.0 实现启动前必须在 `quasar/adapter` 中验证 `iceberg` crate 0.9.1 的以下能力：

| 验证项 | 验证方法 | Fallback |
|--------|----------|----------|
| V2 format 支持 | 构建并序列化 `format_version: 2` 的 TableMetadata | 若不支持，评估是否可降级到 V1 或需要自研补充 |
| metadata 文件命名 helper | 调用 crate 的 metadata location 构建方法 | 若缺失，实现 V4.0 自研 helper，并以 §6.3 的 V4.0 命名规则为准 |
| REST commit update/requirement wrapper | 检查 crate 是否提供 `TableUpdate` / `TableRequirement` 枚举 | 若缺失或 wire name 不一致，在 adapter 定义兼容枚举 |

验证结果记录在实现进度文档中。若 crate 能力不足，优先采用 adapter wrapper + crate 内部类型混合策略，不得因 crate 缺失阻塞 Spark E2E。

### 3.2 Wrapper 策略

Quasar 不直接把 `iceberg` crate 的所有类型暴露为 REST handler 的公共边界。

Adapter 层保留薄 wrapper：

- REST 请求/响应字段名由 Quasar DTO 控制，保证与 Iceberg REST OpenAPI 对齐。
- metadata 内部结构优先转换为 `iceberg` crate 类型或由其校验。
- 暂无 crate 覆盖的 REST commit update/requirement discriminator，可在 adapter 定义枚举，但必须使用官方 1.10.x wire name。
- 所有未知 discriminator 和不支持 action 必须在反序列化或校验阶段显式失败，不得静默忽略。

### 3.3 V4.0 metadata 标准与旧 DTO 处理

V4.0 以新实现写出的 Iceberg 1.10.x metadata 为唯一标准，不要求兼容 V3
自研 metadata DTO 写出的历史文件，也不设计 V3 -> V4.0 metadata 迁移或互操作路径。
V3 的 `adapter/src/iceberg/table_metadata.rs` 不再作为 metadata 权威。

V4.0 实现时应按以下方式收敛：

1. 将官方 wire name、requirement、update 枚举迁移到 V4.0 commit 模块。
2. metadata read/write 优先走 `iceberg` crate 类型或校验器。
3. 仅在 crate 当前缺失 REST update wrapper 时保留 Quasar 自定义 enum。
4. 删除或降级旧 DTO 中与官方 1.10.x 不一致的命名，例如 `AddPartitionSpec` 自动派生出的 `add-partition-spec`。

测试要求：
- 不编写 V3 -> V4.0 metadata 互操作测试。
- 不验证 V3 写入的 metadata 文件能否被 V4.0 新类型解析。
- 必须验证 V4.0 新代码写入的 metadata 文件能被 Spark Iceberg 1.10.x client 读回（§九 §9.3 覆盖）。

---

## 四、数据模型设计

### 4.1 现有核心表保持不变

V4.0 继续使用 V3 的核心目录模型：

```text
domains
  -> namespaces
      -> assets
          -> tabular_assets
```

`tabular_assets.metadata_location` 继续保存当前 Iceberg table metadata 指针。
`tabular_assets.schema_snapshot` 继续保存当前 metadata JSON 快照，用于无对象存储测试环境和快速诊断。

### 4.2 新增 staged table 记录

staged create 不能在 `assets` 中提前插入活动表，否则 `assert-create`
在 commit 阶段无法表达 "表不存在时创建" 的官方语义。

V4.0 增加 staged table 记录，用于连接 create staged response 与后续 commit：

```sql
CREATE TABLE iceberg_staged_tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_name TEXT NOT NULL,
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_uuid UUID NOT NULL,
    location TEXT NOT NULL,
    metadata_location TEXT NOT NULL,
    metadata_json JSONB NOT NULL,
    properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (domain_name, namespace_name, table_name)
);
```

设计规则：

- staged create 只写对象存储 metadata file 和 `iceberg_staged_tables`，不写 `assets`。
- 如果同名 active table 已存在，staged create 返回 `409 AlreadyExistsException`。
- 如果同名 staged record 未过期，新的 staged create 返回 `409 AlreadyExistsException`。
- `expires_at` 默认值设置为创建时间后 24 小时（`NOW() + INTERVAL '24 hours'`），客户端不可覆盖。
- commit 成功后，在同一 PostgreSQL 事务中插入 catalog 表并删除 staged record。
- expired staged record 在下一次同名 staged create 请求中同步清理后允许创建；后台清理不进入 V4.0。
- 若过期 staged record 未清理且发生同名 active table 创建冲突，按 active table 优先原则返回 `409 AlreadyExistsException`。
- staged record 使用 `(domain_name, namespace_name, table_name)` 文本键保存短生命周期弱引用，不通过 FK 绑定 `domains` / `namespaces`。
- staged create 和 staged commit 都必须重新校验 Domain / Namespace 仍然存在。
- Domain / Namespace 删除不依赖 FK 自动清理 staged record；V4.0 只在同名 staged create 或 staged commit 路径同步清理过期 record。

### 4.3 新增 scan metrics report 记录

V4.0 的 metrics endpoint 不接入 Prometheus 后端，只保证 Spark scan report 可接收、可验证、可排障。

```sql
CREATE TABLE iceberg_scan_metrics_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    domain_name TEXT NOT NULL,
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    report JSONB NOT NULL,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_iceberg_scan_metrics_reports_table_time
    ON iceberg_scan_metrics_reports(domain_name, namespace_name, table_name, created_at DESC);
```

设计规则：

- handler 只验证 table 存在且 format 为 `iceberg`。
- report JSON 保留原始官方请求体，不在 V4.0 做统计聚合。
- `asset_id` 允许在表删除后置空；`domain_name` / `namespace_name` / `table_name` 文本快照用于删除后的测试追踪和排障。
- 写入失败返回 Iceberg error model 的 5xx 响应，并脱敏底层数据库错误。

### 4.4 新增 purge operation 记录

`purgeRequested=true` 涉及 PostgreSQL 与对象存储，二者无法原子提交。
V4.0 通过同步清理和持久化 operation record 保证失败可诊断。

```sql
CREATE TABLE iceberg_purge_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_name TEXT NOT NULL,
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_location TEXT NOT NULL,
    metadata_location TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_iceberg_purge_operations_table_time
    ON iceberg_purge_operations(domain_name, namespace_name, table_name, requested_at DESC);
```

`status` 只允许以下值：

| status | 含义 |
|--------|------|
| `started` | 已接收 purge 请求 |
| `catalog_dropped` | catalog 记录已删除，对象清理仍在进行或失败 |
| `completed` | catalog 记录和对象清理均完成 |
| `failed` | 清理失败，`error_message` 记录脱敏原因 |

V4.0 不实现后台 orphan cleanup；失败记录只用于同步请求排障与后续人工处理。

### 4.5 Store trait 增量与层级

V4.0 将通用目录能力和 Iceberg 专用能力分层。`CatalogStore` 保持 Domain / Namespace / Asset / Tabular / Version / Unified 查询等通用语义；Iceberg commit、staged create、register、metrics、purge 等能力只进入 `IcebergCatalogStore`。

V4.0 在 `quasar-core` 增加 4 个 Iceberg 专用窄接口。它们与 `TabularStore` 是平行关系（peer），不是继承关系；但跨表一致性操作必须由 storage 层提供事务级方法，不能由 handler 通过多个独立 store 调用拼接。

| Trait | 方法 | 操作目标 | 事务边界 |
|-------|------|----------|----------|
| `IcebergStagingStore` | `create_staged_table` / `get_staged_table` / `delete_staged_table` / `commit_staged_table` | `iceberg_staged_tables` + `assets` + `tabular_assets` | `create_staged_table` 内部自动清理同名 expired staged record；`commit_staged_table` 必须在单个 PostgreSQL 事务中校验 active table 不存在、锁定未过期 staged record、插入 catalog 记录并删除 staged record |
| `IcebergRegisterStore` | `register_iceberg_table` | `assets` + `tabular_assets` | 在单个 PostgreSQL 事务中注册外部 metadata 指针；不写对象存储 metadata file |
| `IcebergMetricsStore` | `record_scan_metrics_report` | `iceberg_scan_metrics_reports` | handler 先校验 Iceberg table 存在，store 持久化原始 report 并保留表名快照 |
| `IcebergPurgeStore` | `begin_iceberg_purge_and_drop_catalog` / `update_purge_operation` | `iceberg_purge_operations` + `assets` + `tabular_assets` | `begin_iceberg_purge_and_drop_catalog` 必须在单个 PostgreSQL 事务中读取 table location、创建 operation、删除 catalog 记录并标记 `catalog_dropped` |

**trait 层级设计目标**：

```
// 通用层（V3 已有）
pub trait CatalogStore:
    DomainStore + NamespaceStore + AssetStore + TabularStore
    + VersionStore + TabularVersionStore + UnifiedQueryStore + Send + Sync {}

// Iceberg 专用层（V4.0 新增）
pub trait IcebergCatalogStore:
    CatalogStore + CasCommitStore
    + IcebergStagingStore + IcebergRegisterStore
    + IcebergMetricsStore + IcebergPurgeStore {}
```

当前 V3 代码中 `CatalogStore` 仍组合了 `CasCommitStore`。V4.0 实现时需要按上述目标拆分：Lance / Unified 继续使用不含 Iceberg CAS 的 `Arc<dyn CatalogStore>`，Iceberg handler 使用 `Arc<dyn IcebergCatalogStore>`。

**Axum state 类型分离**：

| Adapter | State 类型 |
|---------|-----------|
| Iceberg | `Arc<dyn IcebergCatalogStore>` |
| Lance | `Arc<dyn CatalogStore>` |
| Unified | `Arc<dyn CatalogStore>` |

server 层组装示例：

```rust
let store = Arc::new(PgCatalogStore::new(pool));
let ice_store: Arc<dyn IcebergCatalogStore> = store.clone();
let common_store: Arc<dyn CatalogStore> = store.clone();

let app = Router::new()
    .merge(iceberg::routes().with_state(ice_store))
    .merge(lance::routes().with_state(common_store))
    .merge(unified::routes().with_state(common_store));
```

**设计理由**：
1. `CatalogStore` 保持通用语义，不被 Iceberg 特有 trait 污染。
2. Lance / Unified handler 不依赖它们不需要的能力。
3. `PgCatalogStore` 同时实现 `CatalogStore` 和 `IcebergCatalogStore`，内部自由组合实现逻辑。
4. `CasCommitStore` 进入 Iceberg 专用层，因为它直接服务 Iceberg metadata pointer 的 CAS commit，不应继续作为 Lance / Unified state 的必需能力。
5. staged commit 和 purge catalog-drop 是跨表事务，必须由 Iceberg 专用 store 方法承载，避免 handler 级多调用造成中间状态不可解释。

---

## 五、Iceberg REST API 设计与调用链

本章按 endpoint 逐条描述 adapter → core → storage 的完整调用逻辑。每个子章节包含：adapter 层伪代码流程、core trait 调用链、storage 事务边界和错误映射。

---

### 5.1 `GET /v1/config`

**Adapter 层调用逻辑**：
1. 读取 `IcebergConfig.warehouse_path` 作为默认 warehouse。
2. 若请求带 `?warehouse=` 查询参数，覆盖默认配置。
3. 返回 `ConfigResponse`，`endpoints` 字段只包含 V4.0 实际实现的 endpoint 路径列表。

**Core trait 调用链**：无 store 调用。

**Storage 事务边界**：无数据库事务。

**错误映射**：无 store 错误场景。

---

### 5.2 `GET /v1/{prefix}/namespaces`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 作为 `domain_name`。
2. 解析分页参数 `pageSize` / `pageToken`。
3. 调用 `NamespaceStore::list_namespaces(domain_name, offset, limit)`。
4. 将 `Vec<Namespace>` 映射为 Iceberg `ListNamespacesResponse`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `list_namespaces(domain_name, offset, limit)` | `prefix`, `offset`, `limit` | `Vec<Namespace>` |

**Storage 事务边界**：单条只读查询；`JOIN domains` 后按 `domain_id` 过滤。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Domain 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |

---

### 5.3 `POST /v1/{prefix}/namespaces`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 作为 `domain_name`。
2. 校验 `name` 字段（非空、合法标识符）。
3. 调用 `NamespaceStore::create_namespace(domain_name, name, comment, properties)`。
4. 将 `Namespace` 映射为 Iceberg `CreateNamespaceResponse`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `create_namespace(domain_name, name, comment, properties)` | `prefix`, `name`, `comment`, `properties` | `Namespace` |

**Storage 事务边界**：单条 `INSERT`；依赖 `domains` 存在校验和 `UNIQUE(domain_id, name)` 约束。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Domain 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |
| Namespace 已存在 | `AlreadyExists` | 409 | `NamespaceAlreadyExistsException` |

---

### 5.4 `GET /v1/{prefix}/namespaces/{namespace}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 作为 `domain_name`，`{namespace}` 作为 `namespace_name`。
2. 调用 `NamespaceStore::get_namespace(domain_name, namespace_name)`。
3. 将 `Namespace` 映射为 Iceberg namespace properties response。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `get_namespace(domain_name, namespace_name)` | `prefix`, `namespace` | `Namespace` |

**Storage 事务边界**：单条只读查询；`JOIN domains` 后按 `domain_name` + `namespace_name` 定位。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |

---

### 5.5 `HEAD /v1/{prefix}/namespaces/{namespace}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 和 `{namespace}`。
2. 调用 `NamespaceStore::namespace_exists(domain_name, namespace_name)` 或 `get_namespace`。
3. 存在返回 `200 OK`，不存在返回 `404 NoSuchNamespaceException`（无 body）。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `namespace_exists(domain_name, namespace_name)` | `prefix`, `namespace` | `bool` |

**Storage 事务边界**：单条只读查询（`SELECT EXISTS`）。

**错误映射**：无；`false` 直接映射为 404。

---

### 5.6 `DELETE /v1/{prefix}/namespaces/{namespace}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 和 `{namespace}`。
2. 调用 `NamespaceStore::drop_namespace(domain_name, namespace_name)`。
3. 成功返回 `204 No Content`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `drop_namespace(domain_name, namespace_name)` | `prefix`, `namespace` | `()` |

**Storage 事务边界**：单条 `DELETE`；FK `ON DELETE RESTRICT` 阻止非空 Namespace 删除；storage 层将 `RESTRICT_VIOLATION` 转换为 `StoreError::NamespaceNotEmpty`。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |
| Namespace 非空 | `NamespaceNotEmpty` | 409 | `NamespaceNotEmptyException` |

---

### 5.7 `POST /v1/{prefix}/namespaces/{namespace}/properties`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 和 `{namespace}`。
2. 解析请求体 `removals`（要删除的 property key 列表）和 `updates`（要新增/覆盖的 property map）。
3. 调用 `NamespaceStore::update_namespace(domain_name, namespace_name, comment, removals, updates)`。
4. 返回更新后的 properties（Iceberg `UpdateNamespacePropertiesResponse` 格式）。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `NamespaceStore` | `update_namespace(domain_name, namespace_name, comment, removals, updates)` | `prefix`, `namespace`, `comment`, `removals`, `updates` | `Namespace` |

**Storage 事务边界**：读旧 properties → 应用 delta → 写回；必须在单个事务内完成以保证并发安全。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |

---

### 5.8 `GET /v1/{prefix}/namespaces/{namespace}/tables`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}` 和 `{namespace}`。
2. 解析分页参数。
3. 调用 `TabularStore::list_tabular_assets(domain_name, namespace_name, Some("iceberg"), offset, limit)`。
4. 过滤 `format = "iceberg"` 且 `deleted_at IS NULL` 的 active asset。
5. 将结果映射为 Iceberg `ListTablesResponse`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `TabularStore` | `list_tabular_assets(domain_name, namespace_name, Some("iceberg"), offset, limit)` | `prefix`, `namespace`, `"iceberg"`, `offset`, `limit` | `Vec<(Asset, TabularAsset)>` |

**Storage 事务边界**：单条只读 `JOIN` 查询；必须同时过滤 `asset_type = 'table'` 和 `tabular_assets.format = 'iceberg'`。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Namespace 不存在 | `NotFound` | 404 | `NoSuchNamespaceException` |

---

### 5.9 `POST /v1/{prefix}/namespaces/{namespace}/tables`

Create table 请求新增官方字段：

| 字段 | 规则 |
|------|------|
| `name` | 必填，映射为 Quasar `Asset.name` |
| `location` | 可选；缺省时由 Domain warehouse / server warehouse path / namespace-table 规则生成 |
| `schema` | 必填或按 Spark 请求解析；缺省时仅测试路径可生成空 schema |
| `partition-spec` / `write-order` | 如 Spark 发送则进入 metadata 构建 |
| `properties` | 写入 Iceberg metadata properties，并同步必要 catalog properties |
| `stage-create` | 缺省 `false`；`true` 时只创建 staged metadata，不创建 catalog active table |

非 staged create：

**Adapter 层调用逻辑**：
1. 校验 Domain 和 Namespace 存在。
2. **iceberg crate**: `TableMetadataBuilder::new(schema, spec, sort_order, location, FormatVersion::V2, properties)` 构建 initial metadata → `build()` → `serde_json::to_value()` 序列化为 JSON。
3. 写入 `metadata/00001-<uuid>.metadata.json` 到对象存储。
4. 插入 `assets` + `tabular_assets`（传递 `serde_json::Value`）。
5. 返回 `LoadTableResponse`。

staged create：

**Adapter 层调用逻辑**：
1. 校验 active table 不存在。
2. **iceberg crate**: 同上 `TableMetadataBuilder::new()` → `build()` → `serde_json::to_value()` 构建 staged metadata。
3. 写入 staged metadata file 到对象存储。
4. 插入 `iceberg_staged_tables`（传递 `serde_json::Value`）。
5. 返回 `LoadTableResponse`，但不暴露到 list/load/head。

### 5.10 `POST /v1/{prefix}/namespaces/{namespace}/register`

Register table 请求体：

| 字段 | 规则 |
|------|------|
| `name` | 必填，注册后的 table name |
| `metadata-location` | 必填，已有 Iceberg metadata file 位置 |

**Adapter 层调用逻辑**：
1. 校验 Domain、Namespace 和 table name；校验同名 active table 不存在。
2. 从对象存储读取 `metadata-location` 得到 metadata JSON。
3. **iceberg crate**: `serde_json::from_value::<TableMetadata>()` 解析并校验 metadata JSON → 提取 `uuid()`、`location()` 等字段。
4. 插入 `assets` + `tabular_assets`（`metadata_location` 指向外部传入位置，不重写对象存储）。
5. 返回 `LoadTableResponse`。

失败规则：

- metadata file 不存在返回 `404 NoSuchMetadataException` 或等价 Iceberg metadata not found error。
- metadata JSON 不合法返回 `400 BadRequestException`。
- table 已存在返回 `409 AlreadyExistsException`。

### 5.11 `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{table}`。
2. 调用 `TabularStore::get_tabular_asset(domain_name, namespace_name, "iceberg", table_name)`。
3. 从返回的 `TabularAsset.metadata_location` 读取对象存储中的 metadata JSON。
4. **iceberg crate**（可选校验）: `serde_json::from_value::<TableMetadata>()` 验证 metadata 格式合法性。
5. 构建并返回 `LoadTableResponse`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `TabularStore` | `get_tabular_asset(domain_name, namespace_name, "iceberg", table_name)` | `prefix`, `namespace`, `"iceberg"`, `table` | `(Asset, TabularAsset)` |

**Storage 事务边界**：单条只读 `JOIN` 查询；必须过滤 `format = 'iceberg'` 和 `deleted_at IS NULL`。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Table 不存在（或 format 非 iceberg） | `NotFound` | 404 | `NoSuchTableException` |

---

### 5.12 `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`、`{namespace}`、`{table}`。
2. 调用 `TabularStore::get_tabular_asset(domain_name, namespace_name, "iceberg", table_name)`。
3. 成功返回 `200 OK`，不存在返回 `404 NoSuchTableException`（无 body）。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `TabularStore` | `get_tabular_asset(domain_name, namespace_name, "iceberg", table_name)` | `prefix`, `namespace`, `"iceberg"`, `table` | `(Asset, TabularAsset)` |

**Storage 事务边界**：单条只读 `JOIN` 查询。

**错误映射**：`NotFound` 映射为 404 `NoSuchTableException`。

---

### 5.13 `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`

Commit handler 分为两条路径：

| 路径 | 条件 | 行为 |
|------|------|------|
| existing table commit | active table 存在 | 1. 从对象存储读取当前 metadata JSON。<br>2. **iceberg crate**: `serde_json::from_value::<TableMetadata>()` 解析 → `TableRequirement::check()` 逐项校验 → `into_builder()` 转 builder → `TableUpdate::apply()` 应用 updates → `build()` → `serde_json::to_value()` 序列化。<br>3. 写入新 metadata file 到对象存储。<br>4. `cas_update_metadata_location()` CAS 更新 catalog pointer。 |
| staged create commit | active table 不存在，存在未过期 staged record，且含 `assert-create` | 1. 从 staged record 读取 metadata JSON。<br>2. **iceberg crate**: 同上解析 → `TableRequirement::check()` 校验（含 `assert-create`）→ `into_builder()` → `TableUpdate::apply()` → `build()` → `serde_json::to_value()`。<br>3. 写入最终 metadata file 到对象存储。<br>4. `commit_staged_table()` 在事务中插入 catalog 并删除 staged record。 |

如果 active table 不存在且没有 staged record，返回 `404 NoSuchTableException`。
如果 staged commit 缺少 `assert-create`，返回 `409 CommitFailedException`。

### 5.14 `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`

`purgeRequested=false` 或缺省：

- 只删除 catalog 记录。
- 不删除对象存储文件。
- 返回 `204 No Content`。

`purgeRequested=true`：

- 加载 table location 和 metadata location。
- 创建 purge operation record。
- 删除 catalog 记录。
- 删除 table location prefix 下对象。
- 更新 purge operation 状态。
- 全部完成后返回 `204 No Content`。

对象清理失败时返回 5xx，并在 purge operation 中记录失败状态。

### 5.15 `POST /v1/{prefix}/tables/rename`

**Adapter 层调用逻辑**：
1. 提取 `{prefix}`。
2. 解析请求体 `source` 和 `destination`（均包含 namespace + table name）。
3. 调用 `TabularStore::get_tabular_asset(domain_name, src_namespace, "iceberg", src_table)` 校验 source 是 active Iceberg table。
4. 调用 `AssetStore::rename_asset(domain_name, src_namespace, src_table, dst_table, Some(dst_namespace))`。
5. 返回 `200 OK`。

**Core trait 调用链**：
| 步骤 | Trait | 方法 | 输入 | 输出 |
|------|-------|------|------|------|
| 1 | `TabularStore` | `get_tabular_asset(domain_name, src_namespace, "iceberg", src_table)` | `prefix`, `src_ns`, `"iceberg"`, `src_table` | `(Asset, TabularAsset)` |
| 2 | `AssetStore` | `rename_asset(domain_name, src_namespace, src_table, dst_table, new_namespace)` | `prefix`, `src_ns`, `src_table`, `dst_table`, `dst_ns` | `()` |

**Storage 事务边界**：`rename_asset` 单条 `UPDATE`；`uq_assets_active_name` 唯一约束保证目标冲突。

**错误映射**：
| 场景 | StoreError | HTTP | Iceberg error type |
|------|------------|------|--------------------|
| Source table 不存在 | `NotFound` | 404 | `NoSuchTableException` |
| Destination 已存在 | `AlreadyExists` | 409 | `TableAlreadyExistsException` |

---

### 5.16 `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`

Metrics handler：

1. 校验 table 存在且 format 为 `iceberg`。
2. 接收官方 scan report 请求体为 `serde_json::Value`。
3. 持久化到 `iceberg_scan_metrics_reports.report`。
4. 返回 `204 No Content`。

V4.0 不聚合 report，不转成 Prometheus 指标。

### 5.17 Endpoint -> Store 调用矩阵（汇总）

本矩阵是 V4.0 Store trait 分层的实现依据。设计顺序必须是：
先确认 endpoint 的协议语义，再确认 adapter 负责的 metadata / object store 编排，
最后确定 core store trait 与 storage 事务边界。

| Endpoint | Adapter 职责 | Store trait 调用 | Storage 事务边界 |
|----------|--------------|------------------|------------------|
| `GET /v1/config` | 返回实际已实现 endpoint、warehouse 默认配置和覆盖配置；不得声明 V4.1+ 候选能力 | 无 store 调用 | 无数据库事务 |
| `GET /v1/{prefix}/namespaces` | 解析 `{prefix}` 为 Domain；处理分页参数；映射 Iceberg namespace response | `NamespaceStore::list_namespaces(prefix, offset, limit)` | 单条只读查询 |
| `POST /v1/{prefix}/namespaces` | 校验 namespace 名称和 properties；映射 Iceberg 错误模型 | `NamespaceStore::create_namespace(prefix, name, comment, properties)` | 单条 insert；依赖 domain 存在校验和唯一约束 |
| `GET /v1/{prefix}/namespaces/{namespace}` | 加载 namespace properties | `NamespaceStore::get_namespace(prefix, namespace)` | 单条只读查询 |
| `HEAD /v1/{prefix}/namespaces/{namespace}` | 返回存在性状态；不返回 body | `NamespaceStore::namespace_exists(prefix, namespace)` 或 `get_namespace` | 单条只读查询 |
| `DELETE /v1/{prefix}/namespaces/{namespace}` | 删除空 namespace；非空映射为 Iceberg commit/namespace 错误 | `NamespaceStore::drop_namespace(prefix, namespace)` | 单条 delete；依赖 FK / storage 错误分类保证非空不可删 |
| `POST /v1/{prefix}/namespaces/{namespace}/properties` | 解析 removals / updates；保持 Iceberg property update response | `NamespaceStore::update_namespace(prefix, namespace, comment, removals, updates)` | 读旧 properties + update 必须在 storage 内保持一致 |
| `GET /v1/{prefix}/namespaces/{namespace}/tables` | 只列 active Iceberg table；staged table 不可见 | `TabularStore::list_tabular_assets(prefix, namespace, Some("iceberg"), offset, limit)` | 单条只读 join；必须过滤 `format = 'iceberg'` 和 active asset |
| `POST /v1/{prefix}/namespaces/{namespace}/tables` non-staged | 校验请求；用 `iceberg` crate 构建 initial metadata；写 object store metadata file；构建 response | `TabularStore::create_tabular_asset(prefix, namespace, name, "iceberg", location, metadata_location, metadata_json, properties)` | 插入 `assets` + `tabular_assets` 必须在一个事务内完成；同名冲突返回 AlreadyExists |
| `POST /v1/{prefix}/namespaces/{namespace}/tables` staged | 校验 active table 不存在；构建 staged metadata；写 object store metadata file；返回 staged load response | `IcebergStagingStore::create_staged_table(...)` | 清理同名 expired staged record、检查 active table 不存在、插入 staged record 必须由 storage 保证一致 |
| `POST /v1/{prefix}/namespaces/{namespace}/register` | 读取外部 `metadata-location`；用 `iceberg` crate 校验 metadata；不重写 object store 文件 | `IcebergRegisterStore::register_iceberg_table(...)` | 插入 `assets` + `tabular_assets` 必须在一个事务内完成；metadata_location 指向外部传入位置 |
| `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` | 读取 catalog pointer；从 object store 读取 metadata JSON；返回 LoadTableResponse | `TabularStore::get_tabular_asset(prefix, namespace, "iceberg", table)` | 单条只读 join；必须过滤 active Iceberg table |
| `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}` | 检查 active Iceberg table 是否存在 | `TabularStore::get_tabular_asset(prefix, namespace, "iceberg", table)` | 单条只读 join；不得把同名 Lance table 视为存在 |
| `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` existing commit | 读取当前 metadata；校验 requirements；应用 updates；写新 metadata file；返回新 LoadTableResponse | `TabularStore::get_tabular_asset(...)` 后调用 `CasCommitStore::cas_update_metadata_location(...)` | CAS 更新 `metadata_location`、`schema_snapshot`、catalog properties delta 必须在一个事务内完成 |
| `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` staged commit | 在 active table 不存在时读取 staged metadata；要求 `assert-create`；应用 updates；写最终 metadata file | `IcebergStagingStore::get_staged_table(...)` 后调用 `IcebergStagingStore::commit_staged_table(...)` | `commit_staged_table` 必须在一个事务内校验 active table 不存在、锁定未过期 staged record、插入 catalog 记录并删除 staged record |
| `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=false` | 校验目标是 active Iceberg table；只删 catalog，不删对象 | `TabularStore::get_tabular_asset(...)` 后调用 `AssetStore::drop_asset(prefix, namespace, table)` | `drop_asset` 单条 delete；FK cascade 删除 tabular extension；不得删除同名非 Iceberg table |
| `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=true` | 先只读加载 table location 并校验对象路径安全边界；校验通过后执行 object store prefix 删除；按结果回写 purge operation | `TabularStore::get_tabular_asset(...)` 用于安全校验；随后 `IcebergPurgeStore::begin_iceberg_purge_and_drop_catalog(...)` 和 `update_purge_operation(...)` | begin 方法必须重新读取并锁定 table，在一个事务内创建 operation、删除 catalog、标记 `catalog_dropped` 并返回清理所需位置 |
| `POST /v1/{prefix}/tables/rename` | 校验 source/destination；只允许 active Iceberg source；映射 namespace 移动 | `TabularStore::get_tabular_asset(...)` 后调用 `AssetStore::rename_asset(...)` | rename 单条 update；唯一约束保证目标冲突；handler 先做 format 隔离 |
| `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | 校验 table 存在；保留官方 scan report 原始 JSON；返回 `204` | `TabularStore::get_tabular_asset(...)` 后调用 `IcebergMetricsStore::record_scan_metrics_report(...)` | metrics insert 单事务；`asset_id` 可为空或删除后置空，文本快照必须保留 |

约束：

- Adapter 负责 HTTP DTO、Iceberg requirement/update 语义、metadata parse/build/serialize、object store 读写和错误映射。
- Core trait 签名只使用 Quasar model、`String`、`HashMap`、`serde_json::Value` 等中立类型，不暴露 `iceberg` crate 类型。
- Storage 负责 SQL、事务、唯一约束、FK 行为、CAS 条件更新和 `StoreError` 语义分类。
- staged commit、existing table CAS commit、register table、purge catalog-drop 是事务边界明确的 store 原语，不得由 handler 多次通用 CRUD 调用拼接。

---

## 六、Commit 与 Metadata 设计

### 6.1 Requirement 覆盖

V4.0 覆盖 Iceberg 1.10.x table 适用 8 项 requirement：

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

校验失败统一返回 `409 CommitFailedException`。

### 6.2 Update 覆盖

V4.0 支持以下 update：

| Update | V4.0 行为 |
|--------|-----------|
| `assign-uuid` | 设置 table uuid；仅 staged create 或未设置 uuid 的 metadata 可接受 |
| `upgrade-format-version` | 支持 Iceberg format v2；不支持升级到 v3 |
| `add-schema` | 增加 schema，并更新 last assigned field id |
| `set-current-schema` | 切换 current schema id |
| `add-spec` | 增加 partition spec；wire name 必须是 `add-spec` |
| `set-default-spec` | 切换 default spec id |
| `add-sort-order` | 增加 sort order |
| `set-default-sort-order` | 切换 default sort order id |
| `add-snapshot` | 增加 snapshot 和 snapshot log |
| `set-snapshot-ref` | 设置 branch/tag ref；`main` 同步 current snapshot |
| `remove-snapshots` | 删除指定 snapshot 并维护 refs/log 的一致性 |
| `remove-snapshot-ref` | 删除指定 branch/tag ref；不得删除必需的 `main` ref |
| `set-location` | 更新 metadata `location` |
| `set-properties` | 更新 metadata properties，并同步 catalog properties delta |
| `remove-properties` | 删除 metadata properties，并同步 catalog properties delta |
| `remove-partition-specs` | 删除非默认 partition specs；默认 spec 不允许删除 |

V4.0 明确认识但不实现以下 update，统一返回 `501 NotImplementedException`：

- `set-statistics`
- `remove-statistics`
- `set-partition-statistics`
- `remove-partition-statistics`
- `remove-schemas`
- `add-encryption-key`
- `remove-encryption-key`

未知 action 或字段结构错误返回 `400 BadRequestException`。

### 6.3 Metadata 文件命名

V4.0 使用统一的 Iceberg metadata 文件命名规则作为唯一实现标准。

命名规则：

- 新表初始 metadata 使用 Iceberg 兼容命名：`metadata/00001-<uuid>.metadata.json`（遵循 Iceberg 常见做法，version 从 `00001` 开始）。
- commit 后新 metadata location 必须单调推进 version。
- 如果 `iceberg` crate 提供 metadata location helper，优先使用 crate helper；若 crate helper 行为与 `00001` 不一致，以 crate 行为为准并在 §3.1.1 验证项中记录。
- 如果当前 metadata location 无法解析，返回 `500 InternalServerError`，不得生成不可预测 fallback 路径。

V4.0 不提供历史 metadata location 兼容 fallback。若当前 metadata location
不符合 V4.0 命名和解析规则，即使对象文件存在，也返回明确错误并要求重新注册
或重建为 V4.0 标准 metadata。

### 6.4 Commit 顺序

existing table commit：

1. 读取 catalog 当前 `metadata_location`。
2. 从 object store 读取 metadata JSON。
3. 使用 `iceberg` crate 解析/校验。
4. 校验 requirements。
5. 应用 updates，生成新 metadata JSON。
6. 写新 metadata file 到 object store，并 HEAD 验证。
7. PostgreSQL CAS 更新 `metadata_location`、`schema_snapshot` 和 catalog properties。
8. 返回新的 `LoadTableResponse`。

staged create commit：

1. 确认 active table 不存在。
2. 读取未过期 staged record。
3. 校验请求包含 `assert-create`。
4. 从 staged metadata 起步应用 updates。
5. 写最终 metadata file 到 object store。
6. PostgreSQL 事务中插入 `assets` + `tabular_assets` 并删除 staged record。
7. 返回新的 `LoadTableResponse`。

### 6.5 失败窗口

对象存储写成功但 PostgreSQL CAS 失败：

- 客户端返回 5xx 或 `409 CommitFailedException`，按失败类型区分。
- 服务端日志必须包含 domain、namespace、table、old metadata location、new metadata location。
- V4.0 容忍孤儿 metadata file，不做后台清理。

PostgreSQL CAS 成功后响应发送失败：

- catalog 状态以 PostgreSQL 为准。
- 客户端重试时通过 load table 看到新 metadata location。

---

## 七、对象存储与 Purge 设计

### 7.1 Object store helper 增量

V4.0 在现有 `read_json`、`write_json`、`object_exists` 基础上增加：

| helper | 用途 |
|--------|------|
| `list_prefix` | 列出 table location prefix 下对象 |
| `delete_objects` | 批量删除对象 |
| `delete_prefix` | list + delete 的同步封装 |

所有 helper 必须接受已经解析过的 bucket-relative path，禁止在 SQL 或 handler 中拼接未校验对象路径。

### 7.2 Purge 安全边界

`purgeRequested=true` 只能删除满足以下条件的 prefix：

- table location 可转换为当前配置 bucket 内 path。
- path 位于配置的 warehouse prefix 下。
- path 不是 bucket root、warehouse root 或空字符串。
- path 与当前 table 的 metadata location 同源。
- path 必须等于 table location 或为其子目录，防止误删其他表的共享存储路径。
- （可选增强）删除前校验对象存储中当前 metadata file 的 `table-uuid` 与 catalog 记录一致，防止 catalog 与对象存储状态不一致时误删。

任一校验失败返回 `400 BadRequestException` 或 `500 InternalServerError`，不得执行删除。

### 7.3 Purge 顺序

V4.0 采用可诊断的同步 purge：

1. 读取 table 并创建 `started` purge operation。
2. 删除 catalog 记录。
3. 将 operation 更新为 `catalog_dropped`。
4. 删除对象存储 prefix。
5. 删除成功后更新为 `completed`。

其中第 1-3 步必须由 `IcebergPurgeStore::begin_iceberg_purge_and_drop_catalog`
在单个 PostgreSQL 事务中完成，并返回对象清理所需的 table location、
metadata location 和 purge operation id。handler 不得通过 `get_tabular_asset`、
`drop_asset`、`update_purge_operation` 多次独立调用拼接该事务边界。

若第 4 步失败：

- operation 更新为 `failed`。
- 返回 5xx。
- 错误响应不包含 secret、access key、完整底层驱动错误。
- 日志和 operation record 保留 table location、metadata location 与脱敏原因。

---

## 八、错误处理设计

### 8.1 Iceberg 错误模型

V4.0 继续返回 Iceberg REST error response：

```json
{
  "error": {
    "message": "...",
    "type": "...",
    "code": 409
  }
}
```

### 8.2 主要映射

| 场景 | HTTP | Iceberg error type |
|------|------|--------------------|
| table 已存在 | 409 | `AlreadyExistsException` |
| table 不存在 | 404 | `NoSuchTableException` |
| namespace 不存在 | 404 | `NoSuchNamespaceException` |
| requirement 不满足 | 409 | `CommitFailedException` |
| CAS 冲突 | 409 | `CommitFailedException` |
| recognized but unsupported update | 501 | `NotImplementedException` |
| malformed request / unknown discriminator | 400 | `BadRequestException` |
| metadata file 不存在 | 404 | `NoSuchMetadataException` |
| object store transient failure | 503 或 504 | 依据 `StoreError` transient 分类 |
| internal failure | 500 | `InternalServerError` |

### 8.3 脱敏规则

- 客户端错误不得包含数据库 SQL、S3 secret、完整连接串、对象存储 credential。
- 服务端日志可包含 metadata location 和 table location，但不得包含 credential。
- `StoreError::Internal { source }` 继续只在日志中保留 source，响应使用脱敏 message。

---

## 九、测试策略

### 9.1 Adapter 集成测试

必须新增或修改以下测试：

| 测试组 | 必测行为 |
|--------|----------|
| create table | non-staged create、staged create、重复 staged create、active table 冲突 |
| register table | 成功注册、metadata 不存在、metadata JSON 非法、同名冲突 |
| commit requirements | 8 项 requirement 的成功与失败路径 |
| commit updates | V4.0 支持 update 成功；V4.1 候选 update 返回 501 |
| staged commit | 表不存在 + staged record + `assert-create` 成功创建 catalog |
| CAS conflict | 两个真实并行 commit 只有一个成功 |
| purge | `purgeRequested=false` 只删 catalog；`true` 删除对象；对象失败记录 operation |
| metrics | report 接收、table 不存在、非 Iceberg table 不可见 |
| config | `endpoints` 与 V4.0 实际实现一致 |

### 9.2 Spark E2E

V4.0 验收必须包含 Spark 3.5 + Iceberg 1.10.x + MinIO + Postgres + Quasar：

- `CREATE TABLE`
- `CREATE TABLE IF NOT EXISTS`
- DataFrame `.writeTo(...).create()`
- `INSERT INTO`
- `SELECT`
- `DROP TABLE`
- `DROP TABLE PURGE`
- schema add/drop/rename/alter column smoke
- partition transform：`identity`、`days(...)` 或 `hours(...)`、`bucket(N, ...)`
- snapshot read
- create/drop branch
- create/drop tag
- `expire_snapshots` smoke

### 9.3 Metadata 兼容测试

必须至少有一个测试由 Spark Iceberg 1.10.x client 直接读取 Quasar 写出的
metadata file，并打印或断言：

- schema fields
- current snapshot id
- partition spec
- table uuid
- metadata location

### 9.4 并发测试基础设施

现有 `postgres_embedded` + `serial_test` 已确认无法进行真实并行写入（全部 196 个集成测试串行执行）。V4.0 采用**增量策略**：

- 现有 `postgresql_embedded` + `serial_test` 测试保持不动。
- 新增独立的并发测试文件（如 `iceberg_commit_concurrent.rs`），使用 `testcontainers-postgres` 启动独立容器。
- `testcontainers` 作为 `dev-dependencies` 仅用于新增并发测试文件。

并发 CAS 测试不能只模拟 `StoreError::Conflict`；必须让两个请求通过 `tokio::join!` 同时命中同一个 `metadata_location` expected value，验证仅有一个成功返回 `409 CommitFailedException`。

### 9.5 Test matrix

任何测试新增或修改后，必须同步更新 `docs/TEST_MATRIX.md`：

- 新增 V4.0 Iceberg staged create 测试条目。
- 新增 register table 测试条目。
- 新增 commit requirement/update 覆盖矩阵。
- 新增 purge 和 metrics endpoint 测试条目。
- 新增 Spark E2E 验收条目。

新增条目示例：

| 测试组 | 测试名称 | 覆盖范围 | 状态 |
|--------|----------|----------|------|
| Iceberg staged create | `test_staged_create_success` | 创建 staged table，验证 staged record | pending |
| Iceberg staged create | `test_staged_create_commit` | staged record + assert-create -> active table | pending |
| Iceberg register table | `test_register_existing_metadata` | 外部 metadata 注册为 catalog 表 | pending |
| Iceberg commit | `test_all_requirements` | 8 项 requirement 成功/失败路径 | pending |
| Iceberg commit | `test_v4_updates` | V4.0 支持 update 成功路径 | pending |
| Iceberg commit | `test_unsupported_update_501` | 候选 update 返回 NotImplementedException | pending |
| Iceberg purge | `test_purge_catalog_only` | purgeRequested=false 只删 catalog | pending |
| Iceberg purge | `test_purge_with_objects` | purgeRequested=true 删除对象 | pending |
| Iceberg metrics | `test_metrics_report_receive` | 接收 scan report 并持久化 | pending |
| Spark E2E | `spark_create_table` | Spark CREATE TABLE | pending |
| Spark E2E | `spark_insert_select` | Spark INSERT INTO + SELECT | pending |
| Spark E2E | `spark_schema_evolution` | ADD/DROP/RENAME COLUMN | pending |
| Spark E2E | `spark_partition_transform` | identity + days/hours + bucket | pending |

---

## 十、实现顺序

### 10.1 C1: 依赖与 metadata 类型基线

- 引入 `iceberg` crate。
- 验证 §3.1.1 全部 3 项 crate 能力（V2 format、metadata helper、commit wrapper）。
  - **Hard blocker**：若验证失败，按 §3.1.1 Fallback 策略实现并更新本文档 §3.1，然后才能进入 C2。
- 建立 metadata parse/build/serialize wrapper。
- 修正 commit update/requirement enum wire name。
- 建立 V4.0 metadata 写出测试；不增加 V3 -> V4.0 metadata 迁移或互操作测试。

### 10.2 C2: 数据模型与 store 增量

- 增加 staged table、scan metrics、purge operation 初始化 SQL。
- 先完成 `CatalogStore` / `IcebergCatalogStore` 分层调整：`CatalogStore` 不再要求 Iceberg CAS 能力，Iceberg handler 使用 `Arc<dyn IcebergCatalogStore>`。
- 增加对应 core trait 与 storage 实现，包含 staged commit 和 purge catalog-drop 的事务级方法。
- staged commit 与 purge catalog-drop 不得先用 handler 多次 store 调用临时拼接。
- 保持 SQL 集中在 `queries.rs`。

### 10.3 C3: create/register/load/config

- create table 支持 `stage-create`。
- register table endpoint 落地。
- `/v1/config` endpoints 与实现同步。
- load/head/list 继续只暴露 active table。

### 10.4 C4: commit requirement/update 完整化

- existing table commit 覆盖 V4.0 requirements/actions。
- staged create commit 走 `assert-create` 创建 catalog。
- unsupported official update 返回明确 501。
- 增加真实 CAS conflict 测试。

### 10.5 C5: purge 与 metrics

- metrics endpoint 接收并持久化 report。
- purge true 同步删除对象并记录 operation。
- 补齐错误映射和脱敏日志。

### 10.6 C6: Spark E2E 与文档同步

- 跑通 Spark E2E 必测清单。
- 更新 `docs/TEST_MATRIX.md`。
- 更新 `docs/v4/PROGRESS.md`。
- 如实现偏离本文档，先修订本文档再进入提交。

---

## 十一、V4.0 不做什么

V4.0 不设计、不实现以下能力：

1. Iceberg REST Catalog 全量 endpoint 覆盖声明。
2. OAuth token endpoint。
3. vended credentials。
4. S3 signer。
5. multi-table transaction commit。
6. Iceberg views。
7. server-side scan planning。
8. metadata cache。
9. 后台 orphan metadata/data cleanup。
10. 多 warehouse 完整语义。
11. Iceberg table-format v3。
12. Lance 新 endpoint。
13. Unified Asset 创建。

---

## 十二、修订记录

### V1.6 (2026-05-19)

补充 adapter 层 Iceberg Rust SDK 调用细节：

- §5.9 create table：补充 `TableMetadataBuilder::new()` → `build()` → `serde_json::to_value()` 调用链。
- §5.10 register table：补充 `serde_json::from_value::<TableMetadata>()` 解析外部 metadata 调用点。
- §5.11 load table：补充可选 `serde_json::from_value::<TableMetadata>()` 格式校验。
- §5.13 commit table：补充完整 iceberg crate 调用链：`parse` → `TableRequirement::check()` → `into_builder()` → `TableUpdate::apply()` → `build()` → `to_value()`。

### V1.5 (2026-05-19)

V4.0 metadata 标准收敛：

- §3.3 明确 V4.0 以新实现写出的 Iceberg 1.10.x metadata 为唯一标准，不要求兼容 V3 自研 metadata DTO 写出的历史文件。
- §3.3 删除 V3 -> V4.0 metadata 互操作测试和 V3 metadata 读回验证要求，只保留 Spark Iceberg 1.10.x 读取 V4.0 metadata 的验证。
- §6.3 删除 V3 metadata location 兼容 fallback；不符合 V4.0 命名和解析规则时返回明确错误。
- §10.1 删除旧 V3 DTO 迁移测试要求，改为建立 V4.0 metadata 写出测试。

### V1.4 (2026-05-19)

Endpoint 到 Store 调用矩阵修订：

- §5.7 新增 V4.0 "Endpoint -> Store 调用矩阵"，逐项明确 Iceberg endpoint、adapter 职责、store trait 调用和 storage 事务边界。
- §5.7 明确 adapter 负责 HTTP DTO、Iceberg metadata 语义、object store 编排和错误映射；storage 负责 SQL、事务、约束、CAS 和 `StoreError` 分类。
- §5.7 明确 staged commit、existing table CAS commit、register table、purge catalog-drop 不得由 handler 多次通用 CRUD 调用拼接。

### V1.3 (2026-05-19)

Store trait 与事务边界修订：

- §4.2 staged table 明确文本键是短生命周期弱引用；staged create / commit 必须重新校验 Domain / Namespace 存在，过期 record 由后续请求同步清理。
- §4.3 metrics report 将 `asset_id` 外键改为 `ON DELETE SET NULL`，保留表名文本快照用于删除后的排障与测试追踪。
- §4.5 明确 `CatalogStore` 不再包含 Iceberg CAS 能力，`CasCommitStore` 进入 `IcebergCatalogStore` 专用层。
- §4.5 为 staged commit 增加 `commit_staged_table` 事务级方法，为 purge 增加 `begin_iceberg_purge_and_drop_catalog` 事务级方法。
- §7.3 和 §10.2 明确 staged commit 与 purge catalog-drop 不得由 handler 多次独立 store 调用拼接。

### V1.6 (2026-05-19)

调用矩阵审视与细化：

- §4.5 从 `IcebergStagingStore` 方法列表中移除 `cleanup_expired_staged_tables`（降级为 storage 内部实现细节）。
- §5 重构为按每个 endpoint 一个子章节（5.1–5.16），新增 namespace（5.2–5.7）、table list（5.8）、table get/head（5.11–5.12）、rename（5.15）的独立调用链子章节。
- §5.17 保留 Endpoint -> Store 调用矩阵作为汇总。

### V1.2 (2026-05-19)

评审后修订（用户确认）：

- §2.1 组件边界更新：Iceberg handler state 类型改为 `Arc<dyn IcebergCatalogStore>`，Lance/Unified 保持 `Arc<dyn CatalogStore>`。
- §4.2 staged table 补充过期后同名 staged record 处理规则：同步清理过期 record 后允许新的 staged create。
- §4.5 重写 Store trait 增量与层级：明确 4 个新 trait 与 `TabularStore` 是平行关系（peer），不是继承关系；新增 `IcebergCatalogStore` 分层组合设计（`CatalogStore` 通用层 + `IcebergCatalogStore` Iceberg 专用层）。
- §6.3 明确初始 metadata 版本号为 `00001`（遵循 Iceberg 常见做法）。
- §7.2 purge 安全边界增加可选增强项：删除前校验对象存储中 metadata file 的 `table-uuid` 与 catalog 记录一致。
- §9.4 明确并发测试采用增量策略：现有 `postgresql_embedded` + `serial_test` 测试保持不动，新增独立并发测试文件使用 `testcontainers-postgres`。
- §10.1 C1 增加 hard blocker 说明：crate 能力验证失败不得进入 C2。

### V1.1 (2026-05-19)

评审修订：

- §3.1 增加 §3.1.1 Crate 能力验证小节，明确 V2 format、metadata helper、commit wrapper 验证要求。
- §3.3 增加 迁移测试要求，明确 V3 -> V4.0 互操作测试和 Spark 兼容性验证。
- §4.2 staged table 增加 `expires_at` 默认值说明（24 小时）。
- §6.3 增加 V3 兼容性处理，容忍 V3 手写路径格式不规范。
- §7.2 purge 安全边界增加第 5 条校验（path 必须等于 table location 或子目录）。
- §9.5 TEST_MATRIX 增加 14 条新增测试示例。

### V1.0 (2026-05-19)

- 初始 V4.0 设计基线。
- 明确 V4.0 采用 `iceberg` crate 作为 metadata 兼容性基线。
- 设计 staged create、register table、commit requirements/actions、metrics、purge 和 Spark E2E 验收路径。
- 明确 V4.1+ 候选能力不进入本文档实现设计。
