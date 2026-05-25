# Quasar V4.0 实现进度跟踪

> **本文档跟踪 V4.0 从设计到交付的各阶段实现进度。**
> 更新日期：2026-05-20

## 阶段总览

| 阶段 | 主题 | 状态 | 对应提交 |
|------|------|------|----------|
| C1 | 依赖与 metadata 类型基线 | 已完成 | `280e374` |
| C2 | 数据模型与 Store 增量 | 已完成 | `46c4389`, `a188961`, `81cd545`, `0577ac4` |
| C3 | create/register/load/config | 已完成 | `b6ffba3`, `805db10` |
| C4 | commit requirement/update 完整化 | 已完成 | `9f341fb`, `e108849`, `d2b4ac4`, `1e950ba`, `87b1eb3`, `b24534a` |
| C5 | purge 与 metrics | 待启动 | — |
| C6 | Spark E2E 与文档同步 | 待启动 | — |

---

## C1: 依赖与 metadata 类型基线

**状态**: 已完成

### 交付内容

- [x] 引入 `iceberg = "0.9.1"` crate
- [x] 验证 crate 三项能力：V2 format、metadata helper、commit wrapper
- [x] 建立 `metadata.rs` wrapper：`parse_metadata`、`build_initial_metadata`、`apply_commit`、`next_metadata_location`
- [x] 修正 commit update/requirement wire name（`add-spec` 等）
- [x] 建立 V4.0 metadata 写出测试

### 关键文件

- `quasar/adapter/src/iceberg/metadata.rs` — metadata wrapper
- `quasar/adapter/src/iceberg/crate_validation.rs` — crate 能力验证

---

## C2: 数据模型与 Store 增量

**状态**: 已完成

### 交付内容

- [x] 新增数据库表：`iceberg_staged_tables`、`iceberg_scan_metrics_reports`、`iceberg_purge_operations`
- [x] 新增 SQL 查询常量：`iceberg_staged::*`、`iceberg_metrics::*`、`iceberg_purge::*`
- [x] `quasar-core` 新增 4 个 Iceberg 专用 trait：`IcebergStagingStore`、`IcebergRegisterStore`、`IcebergMetricsStore`、`IcebergPurgeStore`
- [x] `quasar-core` 新增 `IcebergCatalogStore` marker trait
- [x] `quasar-storage` `PgCatalogStore` 实现全部 4 个 trait
- [x] `CatalogStore` 保留 `CasCommitStore`（保守 trait 拆分策略）

### 关键文件

- `quasar/core/src/store.rs` — trait 定义
- `quasar/storage/src/store.rs` — PgCatalogStore 实现
- `quasar/storage/src/queries.rs` — SQL 常量
- `quasar/storage/src/schema/init.sql` — 数据库 schema

---

## C3: create/register/load/config

**状态**: 已完成

### 交付内容

- [x] `CreateTableRequest` 支持 `stage-create` 字段
- [x] `create_table` handler 分叉：staged create / non-staged create
- [x] `create_table` 使用 `metadata::build_initial_metadata` wrapper（替换自研 JSON 拼接）
- [x] 新增 `RegisterTableRequest` DTO
- [x] 新增 `register_table` handler：读取外部 metadata → iceberg crate 校验 → catalog 注册
- [x] 新增路由 `POST /v1/{prefix}/namespaces/{namespace}/register`
- [x] `/v1/config` `supported_endpoints()` 同步（包含 register）
- [x] `commit_table` 切换到 `metadata::next_metadata_location` wrapper（无法解析时返回 500）
- [x] `CatalogStore` trait 扩展包含 4 个 Iceberg 专用 trait

### 关键文件

- `quasar/adapter/src/iceberg/dto.rs`
- `quasar/adapter/src/iceberg/table.rs`
- `quasar/adapter/src/iceberg/mod.rs`
- `quasar/core/src/store.rs`

### 测试覆盖

| 测试组 | 测试文件 | 覆盖场景 |
|--------|----------|----------|
| staged create | `iceberg_table.rs` | 成功创建、list/load 不可见、active 冲突 409、重复 staged 409 |
| register table | `iceberg_object_store.rs` | 成功注册、metadata 不存在 404、非法 metadata 400、同名冲突 409 |
| config | `iceberg_config.rs` | endpoints 包含 register |

### 提交记录

- `b6ffba3` feat(adapter): implement V4.0 C3 staged create, register table, and config sync
- `805db10` style: apply cargo fmt to V4.0 C3 changes

---

## C4: commit requirement/update 完整化

**状态**: 已完成

### 交付内容

- [x] existing table commit 覆盖 V4.0 全部 8 项 requirements
  - `assert-create`、`assert-table-uuid`、`assert-ref-snapshot-id`
  - `assert-current-schema-id`、`assert-default-spec-id`、`assert-default-sort-order-id`
  - `assert-last-assigned-field-id`、`assert-last-assigned-partition-id`
- [x] staged create commit 走 `assert-create` 创建 catalog（`commit_staged_table`）
- [x] unsupported official update 返回明确 501
- [x] 真实 CAS conflict 并发测试（`testcontainers-postgres`）

### 关键设计参考

- V4_DESIGN.md §6.1 Requirement 覆盖
- V4_DESIGN.md §6.2 Update 覆盖
- V4_DESIGN.md §9.4 并发测试基础设施

### 关键改动

- `quasar/adapter/src/iceberg/metadata.rs`
  - 删除 `build_initial_metadata` 中 `last-updated-ms = 1` 写死
  - 新增 `check_supported_updates` + `UnsupportedUpdate`（7 个 V4.0 未实现 variant 返回 501）
  - 新增 `apply_commit_for_staged`（staged 路径 requirement check 走 `None`）
- `quasar/adapter/src/iceberg/table.rs`
  - `commit_table` 入口先调用 `check_supported_updates`
  - 改造为 match-分叉：existing-table CAS / staged-create commit
  - 抽取 `commit_existing_table` 和 `commit_staged_table` 两个内部函数
  - staged 路径 storage-NotFound 映射为 409 CommitFailed（并发输家语义）
- `quasar/adapter/Cargo.toml`
  - dev-dependencies 加 `testcontainers`、`testcontainers-modules`
- `quasar/adapter/tests/iceberg_commit.rs`
  - 测试中 add-snapshot timestamp 用占位符 + `now_ms()` 替换，避免被 iceberg crate chronology 校验拒绝
  - 新增 7 个 501 测试、10 个 requirement 测试、8 个 update 测试、5 个 staged commit 测试
  - 删除 ignored 的 `test_concurrent_cas_conflict_end_to_end`
- `quasar/adapter/tests/iceberg_commit_concurrent.rs`（新文件）
  - 每个测试启动独立 `postgres:16` testcontainer
  - 3 个 `tokio::join!` 真实并发测试

### 测试覆盖

| 测试组 | 测试文件 | 覆盖场景 |
|--------|----------|----------|
| commit handler | `iceberg_commit.rs` | 8 项 requirement × 成功/失败、V4.0 update 全覆盖、staged commit 成功/missing assert-create/404/竞态 active/501 |
| unsupported update | `iceberg_commit.rs` | 7 个 variant → 501 NotImplementedException |
| concurrent CAS | `iceberg_commit_concurrent.rs` | 并发 existing commit、并发 staged commit、并发后状态一致性（每个测试独立 PG container） |

### 提交记录

- `9f341fb` fix(adapter): remove last-updated-ms hack and use real timestamps
- `e108849` feat(adapter): reject V4.0-unsupported TableUpdate variants with 501
- `d2b4ac4` test(adapter): cover V4.0 remaining 5 commit requirements
- `1e950ba` test(adapter): cover V4.0 commit update variants
- `87b1eb3` feat(adapter): finalize staged-create commit path
- `b24534a` feat(adapter): real concurrent CAS conflict tests via testcontainers

---

## C5: purge 与 metrics

**状态**: 已完成

### 交付内容

- [x] metrics endpoint `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`
  - 校验 table 存在且 format 为 iceberg
  - 接收 scan report JSON（`serde_json::Value`）并持久化到 `iceberg_scan_metrics_reports`
  - 返回 `204 No Content`
- [x] purge `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}?purgeRequested=true`
  - 安全边界校验：bucket 匹配、非 bucket root、warehouse prefix 下、metadata_location 同源
  - 同步删除对象存储 table location prefix 下所有对象
  - `begin_iceberg_purge_and_drop_catalog`（事务：创建 operation + 删除 catalog）+ `update_purge_operation` 状态流转（completed/failed）
  - 对象清理失败返回 500，错误响应脱敏
- [x] 补齐错误映射和脱敏日志
  - object store 失败映射为 `InternalServerError`，不暴露底层驱动错误
  - 安全边界校验失败映射为 `BadRequestException`
- [x] 新增对象存储 helper：`list_prefix`、`delete_objects`、`delete_prefix`（`object_store_util.rs`）

### 测试覆盖

| 测试组 | 测试文件 | 覆盖场景 |
|--------|----------|----------|
| purge true | `iceberg_object_store.rs` | 对象删除、catalog 删除、operation 记录 completed |
| purge false | `iceberg_object_store.rs` | 只删 catalog、对象保留 |
| purge 安全边界 | `iceberg_object_store.rs` | location 不在 bucket 内 → 400 |
| metrics 成功 | `iceberg_table.rs` | report 接收、204、数据库记录 |
| metrics 404 | `iceberg_table.rs` | table 不存在 → 404 |

### 关键改动文件

- `quasar/adapter/src/object_store_util.rs` — `list_prefix`、`delete_objects`、`delete_prefix`
- `quasar/adapter/src/iceberg/table.rs` — `drop_table` purge 分支、`report_metrics` handler
- `quasar/adapter/src/iceberg/mod.rs` — metrics 路由、`supported_endpoints` 同步
- `quasar/adapter/tests/iceberg_object_store.rs` — purge 集成测试
- `quasar/adapter/tests/iceberg_table.rs` — metrics 集成测试

### 关键设计参考

- V4_DESIGN.md §5.14 DROP TABLE（purge）
- V4_DESIGN.md §5.16 metrics
- V4_DESIGN.md §7.1 Object store helper 增量
- V4_DESIGN.md §7.2 Purge 安全边界
- V4_DESIGN.md §7.3 Purge 执行顺序
- V4_DESIGN.md §8.2 错误映射
- V4_DESIGN.md §9.1 测试策略

---

## C6: Spark E2E 与文档同步

**状态**: 待启动

### 计划内容

- [ ] Spark E2E 必测清单
  - `CREATE TABLE`、`CREATE TABLE IF NOT EXISTS`、DataFrame `.writeTo(...).create()`
  - `INSERT INTO`、`SELECT`
  - `DROP TABLE`、`DROP TABLE PURGE`
  - schema evolution、partition transform、snapshot read、branch/tag、expire_snapshots
- [ ] 验证 V4.0 写出的 metadata 文件能被 Spark Iceberg 1.10.x 读回
- [ ] 更新 `docs/TEST_MATRIX.md`
- [ ] 更新本文档 `PROGRESS.md`

---

## 已知问题 / 技术债

| 问题 | 影响 | 计划修复阶段 |
|------|------|-------------|
| `CatalogStore` 包含 Iceberg 专用 trait（trait object upcast 限制妥协） | Lance/Unified 理论上可调用 Iceberg 方法 | V4.1 评估泛型 state 重构 |
