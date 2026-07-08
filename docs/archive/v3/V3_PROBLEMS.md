# Quasar V3 问题清单

> 本文档记录 V2 版本中识别出的问题，作为 V3 需求分析的基线。
> 每个问题经过讨论确认后标记状态：**待确认** / **确认** / **非问题**。
> 确认的问题将保留并补充细节；非问题将删除或说明原因。

---

## 一、数据模型与扩展性

### 问题 1: AssetFormat / AssetType 枚举硬编码

**状态**: 确认保留，V3 解决数据库与 Core 存储模型硬约束；热插拔机制后续演进

**描述**:
`AssetFormat` 枚举只有两个变体 `Iceberg` / `Lance`，数据库 DDL 中有 `CHECK (asset_subtype IN ('iceberg', 'lance'))` 硬约束。

**影响**:
- 新增格式（如 Delta Lake）需要同时修改 Rust 枚举、数据库 DDL、所有协议适配器。
- 数据库 CHECK 约束会把格式扩展变成 schema migration。
- 闭合 Rust 枚举会把存储层与当前协议适配器绑定在一起。
- 完整热插拔仍需要后续注册表 / 插件机制，不作为 V3 完整交付目标。

**V3 方向（已确认）**:
- 数据库层取消硬编码 `CHECK` 枚举，改用资产类型与表格式注册表或等价校验机制。
- Core 存储模型使用可扩展名称或 newtype，不要求所有未来格式进入闭合枚举。
- Iceberg / Lance 标准协议适配器仍可保留固定常量或枚举，用于协议路由和错误映射。

**相关代码**:
- `quasar/core/src/models.rs:8-22` (`AssetFormat` 枚举)
- `quasar/storage/src/migrations/V1__v2_initial_schema.sql:21` (DDL CHECK 约束)

---

### 问题 2: asset_subtype 语义模糊

**状态**: 确认保留，V3 方向已明确

**描述**:
`asset_subtype` 字段同时承担了"格式标识"和"资产子类型"两个职责。当前所有资产都是表，`asset_subtype` 存 `"iceberg"` 或 `"lance"`，表示数据格式。但未来出现非表资产（如 AI 模型）时，`asset_subtype` 的语义将变得混乱。

**举例**:
- `AssetType::Model` 的 `asset_subtype` 应该填 `"pytorch"` 还是 `"huggingface"`？
- `"pytorch"` 与 `"iceberg"` 不在同一语义维度，却共用同一字段。

**V3 方向（已确认）**:
- 采用端点级隔离：Domain 不绑定格式，不同格式通过不同 REST 端点区分（`/iceberg/v1/...` vs `/lance/v1/...`）。
- 同一 Domain 的同一 Namespace 内资产名唯一（不区分格式），放弃 V2 的"同名跨格式共存"设计。
- `format` 信息将下放至扩展表（如 `tabular_assets.format`），`assets` 主表不再保留 `asset_subtype`。
- 主表保留 `asset_type`（`table`、`model` 等），用于区分资产大类。
- 服务端在各协议端点内部按 `format` 字段过滤，非匹配格式返回 `TableNotFoundException`。
- 具体方案（Domain 模型设计、路由前缀变化等）待 V3 设计阶段细化。

**相关代码**:
- `quasar/core/src/models.rs:78-88` (`Asset` 结构体)
- `quasar/storage/src/migrations/V1__v2_initial_schema.sql:14-31` (assets 表定义)

---

### 问题 3: TabularAsset / TabularAssetVersion 的硬绑定

**状态**: 确认保留，V3 解决

**描述**:
`CatalogStore` trait 中所有 Asset 相关操作的返回类型都隐式假设资产是表（返回 `AssetWithTabular`）。非表资产无法复用这套接口。

**影响**:
- V2 设计文档中提到"为后续非表资产预留扩展空间"，但接口层面并未实现。
- 新增非表资产类型需要定义新的 trait 方法或绕过现有接口。

**V3 方向（已确认）**:
采用 trait 拆分（方案 1）：
- 保留通用 `CatalogStore` trait，仅包含 Namespace 操作和格式无关的 Asset 基础操作。
- 新增 `TabularStore: CatalogStore` 子 trait，承载所有表资产特有操作（`create_table`、`commit_table`、`list_tables` 等）。
- 未来新增非表资产时（如 Model），新增对应的 `ModelStore: CatalogStore` 子 trait。
- `PgCatalogStore` 同时实现多个 trait，路由层按需组合。
- 简记：通用 trait 管身份，子 trait 管类型行为。

**相关代码**:
- `quasar/core/src/store.rs:38-119` (CatalogStore Asset 方法签名)
- `quasar/core/src/models.rs:90-105` (TabularAsset / AssetWithTabular)

---

## 二、存储层实现

### 问题 4: CatalogStore trait 职责膨胀

**状态**: 确认保留，与问题 3 合并解决

**描述**:
`CatalogStore` trait 同时承载了：
- 标准协议操作（`create_asset`、`cas_update_metadata_location`）
- Lance 专属操作（`create_version`、`load_version`、`list_versions`）
- Unified API 专属操作（`list_assets_unified`、`get_asset_unified`）

**影响**:
- 随着协议和功能增加，trait 方法数会持续膨胀。
- 任何存储实现都必须实现全部方法，即使某些方法在特定部署中从不使用。
- 违反接口隔离原则（ISP）。

**V3 方向（已确认）**:
与问题 3 共用 trait 拆分方案：
- 通用 `CatalogStore`：仅 Namespace 和格式无关基础操作。
- `TabularStore: CatalogStore`：表资产通用 CRUD。
- `VersionedStore: TabularStore`：Lance 版本管理专属。
- `CasCommitStore: TabularStore`：Iceberg CAS 专属。
- `UnifiedQueryable: CatalogStore`：Unified API 跨格式查询专属。
- `PgCatalogStore` 按需实现 trait 组合。

**相关代码**:
- `quasar/core/src/store.rs:10-195` (CatalogStore trait 定义)

---

### 问题 5: 分页使用 OFFSET，存在深度分页性能隐患

**状态**: 确认保留，V3 暂不解决

**描述**:
`list_namespaces` 和 `list_assets_unified` 使用 `OFFSET $n LIMIT $m` 实现分页。

**影响**:
- PostgreSQL 的 OFFSET 在数据量大时性能线性下降（需要跳过前面所有行）。
- 没有提供排序参数，调用方无法控制返回顺序。

**相关代码**:
- `quasar/storage/src/store.rs:167-178` (`list_namespaces`)
- `quasar/storage/src/store.rs` (list_assets_unified，后续偏移处)

---

### 问题 6: StoreError 错误粒度太粗

**状态**: 确认保留，V3 解决

**描述**:
`StoreError` 只有 5 个变体：`NotFound`、`AlreadyExists`、`Conflict`、`InvalidInput`、`Internal`。

**影响**:
- 数据库连接失败、SQL 语法错误、约束违反、锁超时等不同层级的错误都被统一包装为 `Internal`。
- 上层无法区分"数据库挂了"和"SQL 写错了"，不利于监控告警和降级决策。

**关联问题：Internal 错误信息泄露给客户端（安全问题）**

当前 V2 中，`StoreError::Internal(msg)` 的字符串会被原样传递给客户端：

```json
{
    "detail": "create_asset failed: db error: ERROR: connection refused"
}
```

这暴露了数据库连接状态、表名、列名等内部信息，属于信息泄露（Information Disclosure）。

**V3 方向（已确认）**:

采用 **方案 1（`thiserror` 的 `#[source]`）**，实现"日志详情"与"客户端消息"的优雅分离：

1. `StoreError::Internal` 改为结构体变体，保留 `msg`（客户端安全文案）和 `source`（原始错误对象，仅用于日志）：

```rust
#[derive(Debug, Error)]
pub enum StoreError {
    // ... 其他变体不变
    #[error("internal error: {msg}")]
    Internal {
        msg: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
}
```

2. 适配器层统一对 `Internal` 做安全脱敏：
   - **客户端**：固定返回通用文案 `"An internal error occurred"`，不暴露任何内部细节。
   - **日志**：通过 `tracing::error!(error = ?source, "{}", msg)` 打印完整错误链，供运维排查。

3. 新增 `DatabaseUnavailable` 和 `Timeout` 变体（见主问题描述），使可重试错误能够被客户端识别（返回 503/504）。

**相关代码**:
- `quasar/core/src/error.rs:3-19` (StoreError 定义)
- `quasar/adapter/src/unified/error.rs:171-197` (Internal 映射到客户端)

---

### 问题 7: SQL 查询分散且无集中管理

**状态**: 确认保留，V3 解决

**描述**:
数十条 SQL 查询直接硬编码在 `PgCatalogStore` 的各个方法体中，没有集中管理或命名。

**影响**:
- Schema 变更时很难找全需要修改的 SQL 语句。
- 无法对 SQL 进行独立审查、优化或版本控制。

**V3 方向（已确认）**:

采用**常量模块**方案，不改技术栈，纯代码组织优化：

1. 新建 `storage/src/queries.rs` 模块，按功能域（`namespace`、`asset`、`version` 等）组织 SQL 常量。
2. 所有 SQL 从 `store.rs` 的方法体中抽出，改为引用命名常量（如 `queries::namespace::LIST`）。
3. 保持 `tokio-postgres` 不变，不引入 ORM 或查询构建器。
4. 未来若有特别复杂的长 SQL，再考虑外置为 `.sql` 文件并用 `include_str!` 嵌入。

**相关代码**:
- `quasar/storage/src/store.rs` (全文，几乎所有方法都包含内联 SQL)

---

## 三、Unified API 设计

### 问题 8: Asset 操作强制要求 format 参数

**状态**: 确认保留，V3 设计方向改变后自然解决

**描述**:
Unified API 的 Asset GET/DELETE/PATCH/rename 操作都要求 `?format=iceberg` 查询参数。

**根因**:
V2 的 `assets` 表使用 `(namespace_id, name, asset_type, asset_subtype)` 联合唯一约束，支持同一 Namespace 下同名资产跨格式共存。因此操作 Asset 时必须显式指定 `format` 以消除歧义。

**影响**:
- 用户必须先知道资产格式才能操作，与"统一纳管"的愿景有张力。
- 同名资产跨格式共存时，调用方需要自己处理歧义。

**V3 方向（已确认）**:

V3 采用端点级隔离（Domain 不绑定格式），不同格式通过不同 REST 端点区分（`/iceberg/v1/...` vs `/lance/v1/...`）。端点路径本身隐含了格式信息，Asset 操作不再需要 `format` 参数。同一 Namespace 内资产名唯一（不区分格式）。此问题随设计方向改变自然消失。

**相关代码**:
- `quasar/adapter/src/unified/asset.rs:18-45` (`parse_format` 函数)
- `quasar/adapter/src/unified/asset.rs:163-196` (`get_asset` handler)

---

### 问题 9: Unified API 不暴露 Asset 创建

**状态**: 确认保留，V3 暂不解决

**描述**:
`POST /unified/v1/namespaces/{ns}/assets` 返回 405 Method Not Allowed。

**影响**:
- 管理后台无法通过 Unified API 创建资产，必须走 Iceberg 或 Lance 标准协议。
- 限制了 Unified API 作为"管理入口"的实用性。

**V3 方向（已确认）**:

维持 V2 现状，V3 暂不实现 Unified Asset 创建。理由：
- 资产创建涉及格式特有的语义（Iceberg schema/partition、Lance declare/register），抽象层设计复杂。
- Gravitino、Polaris 虽支持统一创建，但实现较重（需定义格式无关的请求格式并映射到各格式语义）。
- V3 优先解决数据模型和 trait 拆分等基础问题，Unified Asset 创建 deferred 到后续版本。

**相关代码**:
- `quasar/adapter/src/unified/asset.rs:284-295` (`create_asset_not_allowed`)
- `quasar/adapter/src/unified/mod.rs:35-37` (路由注册)

---

### 问题 10: UnifiedConfig 与 iceberg feature 隐式耦合

**状态**: 确认保留，V3 解决

**描述**:
V2 中 `object_store` crate 仅在 `iceberg` feature 启用时引入，导致 `UnifiedConfig` 的 `object_store` 字段被 `#[cfg(feature = "iceberg")]` 条件编译包裹。同时 Lance 协议虽然数据也存对象存储，但 Catalog 服务端不直接操作，因此未引入 `object_store`。

**影响**:
- feature flag 逻辑复杂，`unified` 与 `iceberg` 存在隐式依赖关系。
- `UnifiedConfig` 字段在不同 feature 组合下形状不同，增加心智负担。

**V3 方向（已确认）**:

V3 不再区分表格式是否引入 `object_store`，统一作为公共依赖：

1. `object_store` 从 `optional = true` 改为所有 crate 的公共依赖（或至少 adapter/storage/server 的公共依赖）。
2. 移除 `#[cfg(feature = "iceberg")]` 对 `UnifiedConfig.object_store` 的条件编译。
3. 简化 feature flag 设计：`iceberg` / `lance` / `unified` 只控制协议适配器是否编译，不控制底层存储能力。
4. 未来 Lance 如需服务端直接操作对象存储（如读取 manifest），无需再改依赖结构。

**相关代码**:
- `quasar/adapter/Cargo.toml:9,22` (iceberg feature 与 object_store optional 声明)
- `quasar/server/Cargo.toml:13,31` (server 层 iceberg feature 与 object_store optional 声明)
- `quasar/adapter/src/unified/mod.rs:13-19` (UnifiedConfig 条件编译字段)
- `quasar/adapter/src/unified/version.rs:24-28` (Iceberg 分支的条件编译)

---

## 四、版本与性能

### 问题 11: Iceberg current_version 每次实时读取对象存储

**状态**: 确认保留，V3 暂不解决

**描述**:
`get_iceberg_current_version` 每次请求都通过 `object_store.get()` 读取 `metadata.json` 并完整解析 JSON。

**影响**:
- 高并发场景下会成为性能瓶颈。
- 没有缓存机制或预热策略。

**V3 方向（已确认）**:

维持 V2 现状，V3 暂不引入 metadata 缓存。理由：
- 主流项目（Iceberg CachingCatalog、Polaris ETag、Gravitino Caffeine）均采用服务端缓存策略，但 V3 优先解决数据模型和接口设计问题。
- 性能优化 deferred 到后续版本，届时参考 `CachingCatalog` TTL 缓存（30 秒）或 `moka` 本地缓存方案。

**相关代码**:
- `quasar/adapter/src/unified/version.rs:71-131` (`get_iceberg_current_version`)

---

### 问题 12: Lance version_key vs version_order 语义不清

**状态**: 确认保留，V3 解决

**描述**:
`version_key` 是字符串类型，`version_order` 是 `Option<i64>`。两者关系不明确。

**影响**:
- 代码中出现 `version_key.parse().unwrap_or(0)` 作为 fallback，暗示 `version_key` 实际为数字。
- 存在类型设计隐患：如果 `version_key` 不是有效数字，会静默 fallback 到 0。

**V3 方向（已确认）**:

保留两个字段，但严格区分语义：
- `version_key` 表达格式原生版本标识，在同一 Asset 下唯一。
- `version_order` 表达可比较顺序，允许为空；Lance 版本写入时必须等于原生 `version_id`。
- latest 查询只能基于明确的 `version_order` 或格式内权威指针，不允许从 `version_key` 字符串解析 fallback。
- 这样既修复 V2 的混用问题，又不会把未来模型版本、hash/tag 版本等强行压缩成整数。

**相关代码**:
- `quasar/adapter/src/unified/version.rs:56-66` (`lance_version_to_response`)
- `quasar/core/src/models.rs:108-116` (`AssetVersion` 定义)

---

## 五、错误处理与代码质量

### 问题 13: 字符串匹配判断错误类型

**状态**: 确认保留，V3 解决（与问题 6 合并处理）

**描述**:
`map_namespace_error` 通过 `msg.contains("not empty")` 判断是否为 `NamespaceNotEmpty`。

**影响**:
- 脆弱：依赖英文错误文案，文案改动会导致错误映射失效。
- 不精确：其他包含 "not empty" 的 Conflict 错误可能被误判。
- 无法国际化。

**V3 方向（已确认）**:

与问题 6（StoreError 重构）合并处理：
- 给 `StoreError` 新增 `NamespaceNotEmpty` 变体（或结构化标签 `ConflictKind`）。
- `PgCatalogStore` 中根据 SQL 状态码直接构造对应变体，不再通过文案字符串判断。
- 适配器层直接 `match` 变体，消除字符串匹配。

**相关代码**:
- `quasar/adapter/src/unified/error.rs:145-175` (`map_namespace_error`)
- `quasar/storage/src/store.rs:207-231` (`drop_namespace` 的 RESTRICT_VIOLATION 处理)

---

### 问题 14: lance_version_to_response 中的 unwrap

**状态**: 确认保留，与问题 12 合并解决

**描述**:
```rust
version_order.unwrap_or_else(|| v.version.version_key.parse().unwrap_or(0))
```

**影响**:
- 内层 `parse().unwrap_or(0)` 不会 panic，但说明 `version_key` 的类型设计不合理。
- 如果 `version_key` 确实是数字，为何不用数字类型？如果可以是任意字符串，fallback 到 0 是否合理？

**V3 方向（已确认）**:

与问题 12 合并解决。V3 不再通过 `version_key.parse().unwrap_or(0)` 获取排序值；Lance 响应直接读取必填的 `version_order`，缺失时视为存储不变量破坏并返回内部错误或数据校验错误。

**相关代码**:
- `quasar/adapter/src/unified/version.rs:59-62`

---

## 六、已记录但未解决

### 问题 15: 对象存储与数据库的非原子性

**状态**: 确认保留，V3 暂不解决

**描述**:
`create_table` / `commit_table` 先写对象存储（metadata.json），再更新数据库（metadata_location 指针）。如果 S3 写入成功但数据库操作失败，S3 上会留下未被任何 DB 记录引用的孤儿 metadata 文件。

**影响**:
- 长期累积会浪费对象存储空间。
- 不影响正确性，但增加运维成本。

**V3 方向（已确认）**:

维持现状，V3 暂不解决。理由：
- 此问题不影响正确性（Iceberg 客户端始终通过 DB 指针定位文件，不会误读孤儿文件）。
- 引入分布式事务（2PC/Saga）复杂度与收益不成正比。
- 后台清理任务方案已记录在 `docs/EVOLUTION.md` 中，可作为未来演进方向。

**参考**:
- `docs/EVOLUTION.md` 已记录此问题并建议后台清理任务方案。

---

## 统计

| 维度 | 问题数 |
|------|--------|
| 数据模型与扩展性 | 3 |
| 存储层实现 | 4 |
| Unified API 设计 | 3 |
| 版本与性能 | 2 |
| 错误处理与代码质量 | 2 |
| 已记录但未解决 | 1 |
| **合计** | **15** |
