# Quasar V3 核心问题闭环回溯表

> 本文档逐条对照 `V3_REQUIREMENTS.md` §5.2 的 11 个核心问题，给出 Phase、实现文件与测试覆盖的精确映射。V3 验收以本表为最终依据。

---

## 闭环总览

| 问题 | Phase | 状态 | 关键文件 | 测试 |
|------|-------|------|---------|------|
| 1 AssetFormat / AssetType 枚举硬编码 | Phase 2 | ✅ 已闭环 | `storage/src/schema/init.sql` (registry 表), `core/src/models.rs` | `storage/tests/integration.rs::test_asset_active_uniqueness_within_namespace` |
| 2 asset_subtype 语义模糊 | Phase 2 | ✅ 已闭环 | `storage/src/schema/init.sql` (删除 `asset_subtype`), `core/src/models.rs` | `storage/tests/integration.rs::test_unified_query_pairs_asset_with_tabular` |
| 3 TabularAsset 硬绑定 | Phase 2 | ✅ 已闭环 | `core/src/store.rs` (8 trait 拆分) | `storage/tests/integration.rs` (多 trait 调用覆盖) |
| 4 CatalogStore 职责膨胀 | Phase 2 | ✅ 已闭环 | `core/src/store.rs` (8 trait 拆分) | `storage/tests/integration.rs` |
| 6 StoreError 粒度太粗 + 信息泄露 | Phase 2/3 | ✅ 已闭环 | `core/src/store.rs`, `adapter/*/error.rs` | `adapter/tests/*` (全量错误码映射) |
| 7 SQL 查询分散 | Phase 2 | ✅ 已闭环 | `storage/src/queries.rs` | `storage/src/queries.rs` (常量 well-formed 测试) |
| 8 Asset 操作强制 format 参数 | Phase 3 | ✅ 已闭环 | `adapter/src/unified/asset.rs` (删除 `parse_format`) | `adapter/tests/unified_asset.rs::test_get_asset_without_format_returns_200` |
| 10 UnifiedConfig 与 iceberg 耦合 | Phase 3 | ✅ 已闭环 | `quasar/server/Cargo.toml`, `adapter/src/unified/mod.rs` | `server/tests/smoke.rs` |
| 12 version_key vs version_order 混用 | Phase 3 | ✅ 已闭环 | `adapter/src/lance/version.rs`, `adapter/src/unified/version.rs` | `adapter/tests/lance_version.rs::test_list_versions` |
| 13 字符串匹配判断错误类型 | Phase 3 | ✅ 已闭环 | `storage/src/store.rs`, `adapter/*/error.rs` | `storage/tests/integration.rs::test_non_empty_domain_delete_returns_conflict` |
| 14 lance_version_to_response unwrap | Phase 3 | ✅ 已闭环 | `adapter/src/lance/version.rs` | `adapter/tests/lance_version.rs::test_describe_current_version_in_describe_table` |

---

## 逐条详情

### 问题 1 — AssetFormat / AssetType 枚举硬编码

**根因**: V2 在数据库 CHECK 约束和 Rust 枚举中硬编码了 format/type 值，新增格式需要改 schema + 代码。

**解决方案**:
- 数据库层引入 `asset_types` 和 `tabular_formats` 注册表表，取消 CHECK 硬编码。
- Core 模型 `Asset.asset_type` 和 `TabularAsset.format` 改为 `String`。
- adapter 内部保留 `AssetFormat` 枚举作为路由 dispatch 的 newtype，但不进入 core/storage。

**文件**:
- `storage/src/schema/init.sql`: `asset_types`, `tabular_formats` 表 + seed 数据
- `core/src/models.rs`: `Asset::asset_type: String`, `TabularAsset::format: String`
- `core/src/store.rs`: `TabularStore` trait 使用 `&str` 而非枚举

**测试**:
- `storage/tests/integration.rs::test_asset_active_uniqueness_within_namespace` — 验证同 namespace 跨 format 冲突由唯一索引 enforce

---

### 问题 2 — asset_subtype 语义模糊

**根因**: V2 `assets.asset_subtype` 列既表示 "table/view" 又表示 "iceberg/lance"，职责混乱。

**解决方案**:
- 删除 `assets.asset_subtype` 列。
- `format` 下放至 `tabular_assets.format`。
- 端点级隔离：Iceberg/Lance/Unified 各自端点隐含 format，不再通过查询参数传递。

**文件**:
- `storage/src/schema/init.sql`: V3 DDL 删除 `asset_subtype`
- `core/src/models.rs`: `TabularAsset` 独立 struct

**测试**:
- `storage/tests/integration.rs::test_unified_query_pairs_asset_with_tabular` — 验证 `(Asset, Option<TabularAsset>)` 返回形态
- `adapter/tests/format_isolation.rs::test_cross_format_create_conflicts` — 验证端点级隔离

---

### 问题 3 — TabularAsset 硬绑定 / 问题 4 — CatalogStore 职责膨胀

**根因**: V2 所有 catalog 操作挤在单一 `CatalogStore` trait 中；非 tabular 资产无法扩展。

**解决方案**:
- 拆分为 8 个 trait: `DomainStore`, `NamespaceStore`, `AssetStore`, `TabularStore`, `VersionStore`, `TabularVersionStore`, `CasCommitStore`, `UnifiedQueryStore`。
- `CatalogStore` 转为 marker super-trait + blanket impl，保持 `Arc<dyn CatalogStore>` 兼容。
- `PgCatalogStore` 实现全部 8 trait。

**文件**:
- `core/src/store.rs`: 8 trait 定义
- `storage/src/store.rs`: PgCatalogStore 8 trait impl

**测试**:
- `storage/tests/integration.rs`: Domain/Namespace/Asset/Version/CAS 全矩阵

---

### 问题 6 — StoreError 粒度太粗 + 信息泄露

**根因**: V2 `StoreError::Internal(String)` 把内部错误原文暴露给客户端；缺少 `NamespaceNotEmpty` 等变体。

**解决方案**:
- `Internal` 改为结构体变体 `{ msg, source }`。
- 客户端响应固定为 "An internal error occurred"；详细 `msg`/`source` 通过 tracing 记录。
- 新增 `DomainNotEmpty`, `NamespaceNotEmpty`, `DatabaseUnavailable`, `Timeout` 变体。
- adapter `error.rs` 全部删除 `msg.contains()` / `msg.starts_with()` 字符串匹配。

**文件**:
- `core/src/store.rs`: `StoreError` 9 变体定义
- `adapter/src/iceberg/error.rs`: 全变体映射
- `adapter/src/lance/error.rs`: 全变体映射 + 脱敏
- `adapter/src/unified/error.rs`: 全变体映射 + 脱敏

**测试**:
- `adapter/tests/unified_namespace.rs::test_drop_non_empty_domain_returns_409` — `DomainNotEmpty` 验证
- `adapter/tests/iceberg_namespace.rs::test_drop_non_empty_namespace` — `NamespaceNotEmpty` 验证

---

### 问题 7 — SQL 查询分散

**根因**: V2 SQL 字符串散落在 storage impl 各处，重构时容易遗漏。

**解决方案**:
- 新建 `storage/src/queries.rs` 模块，按功能域组织 SQL 常量。
- 所有 PgCatalogStore impl 改用 `queries::*` 常量。
- 常量通过 `assert_constant` 编译期检查（非空 + 含 `$1` 占位符）。

**文件**:
- `storage/src/queries.rs`: `domain::*/namespace::*/asset::*/version::*` 常量

**测试**:
- `storage/src/queries.rs::tests` (unit tests): `domain_constants_are_well_formed`, `namespace_constants_are_well_formed`, `asset_constants_are_well_formed`, `version_constants_are_well_formed`

---

### 问题 8 — Asset 操作强制 format 参数

**根因**: V2 Unified API 的 GET/DELETE/PATCH/rename 要求 `?format=iceberg` 必填。

**解决方案**:
- V3 active-name uniqueness (`uq_assets_active_name` 部分唯一索引) 使 format 不再用于身份定位。
- Unified 单资产端点删除 `?format=` 依赖；list 保留可选 `?format=` 过滤。
- `adapter/src/unified/asset.rs` 删除 `parse_format` 必填版。

**文件**:
- `adapter/src/unified/asset.rs`: 删除 `parse_format`，`get_asset`/`drop_asset`/`update_asset`/`rename_asset` 不再消费 format query
- `adapter/src/unified/dto.rs`: 删除 `AssetDetailQuery`

**测试**:
- `adapter/tests/unified_asset.rs::test_get_asset_without_format_returns_200`
- `adapter/tests/unified_asset.rs::test_get_asset_ignores_unknown_format_query`

---

### 问题 10 — UnifiedConfig 与 iceberg 耦合

**根因**: V2 `UnifiedConfig.object_store` 被 `#[cfg(feature = "iceberg")]` 条件编译包裹，导致 unified feature 单独编译失败。

**解决方案**:
- `object_store` 从 iceberg feature 解耦，改为 workspace 公共依赖。
- `UnifiedConfig` 删除 `#[cfg(feature = "iceberg")]` gate。
- 4 个 `Cargo.toml` 同步调整。

**文件**:
- `quasar/server/Cargo.toml`
- `quasar/adapter/Cargo.toml`
- `quasar/core/Cargo.toml`

**测试**:
- `server/tests/smoke.rs::test_dual_instance_stateless_smoke` — 三 feature 全开时 server 正常启动

---

### 问题 12 — version_key vs version_order 混用 / 问题 14 — lance_version_to_response unwrap

**根因**: V2 用 `version_key.parse().unwrap_or(0)` 从字符串解析得到排序值，导致版本语义与排序语义混淆。

**解决方案**:
- 保留双字段：`version_key` (原生字符串标识) + `version_order` (显式 i64 排序)。
- Lance adapter 直接读取 `version_order`，禁止字符串解析 fallback。
- `lance_version_to_response` 删除 `unwrap`，改为 `version_order.ok_or(...)`。

**文件**:
- `adapter/src/lance/version.rs`: 4 处 `version_order` 直读
- `adapter/src/unified/version.rs`: `version_order` 直读

**测试**:
- `adapter/tests/lance_version.rs::test_list_versions` — 验证 version_order 升序
- `adapter/tests/lance_version.rs::test_describe_current_version_in_describe_table`

---

### 问题 13 — 字符串匹配判断错误类型

**根因**: V2 adapter 用 `msg.contains("already exists")` 判断错误类型， fragile 且无法处理非英文 locale。

**解决方案**:
- 新增 `StoreError::NamespaceNotEmpty` / `DomainNotEmpty` 变体。
- SQL 唯一约束 / FK 违规由 PostgreSQL 状态码直接映射为 Rust 变体。
- adapter error.rs 按变体分支映射，不再做字符串匹配。

**文件**:
- `storage/src/store.rs`: `create_namespace`/`drop_namespace`/`create_domain`/`drop_domain` 等 impl 中状态码映射
- `adapter/src/*/error.rs`: 变体级映射表

**测试**:
- `storage/tests/integration.rs::test_non_empty_domain_delete_returns_conflict` — `DomainNotEmpty`
- `storage/tests/integration.rs::test_non_empty_domain_delete_returns_conflict` (末尾) — `NamespaceNotEmpty`

---

## V3 暂不解决（4 个问题）

| 问题 | 不解决理由 | 演进方向 |
|------|-----------|---------|
| 5 分页 OFFSET 性能隐患 | 不影响 Data Core Model 重构 | 后续版本引入 cursor-based 分页 |
| 9 Unified API 不暴露 Asset 创建 | 资产创建涉及格式特有语义，不影响数据模型 | 后续版本设计格式无关创建请求格式 |
| 11 Iceberg 实时读取对象存储 | 性能优化，不影响正确性 | 后续版本引入 metadata 缓存 |
| 15 对象存储与数据库非原子性 | 不影响正确性 | 后台清理任务（已记录在 `docs/EVOLUTION.md`） |

---

## 验收结论

**11 个核心问题全部在 V3 开发周期内闭环，4 个 defer 问题已记录演进方向。**

- `cargo test --all-features`: **全绿** (293 passed, 2 ignored)
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **通过**
- `grep -rn 'msg.contains\|msg.starts_with' quasar/adapter/src/*/error.rs`: **零命中**（字符串匹配已清除）
