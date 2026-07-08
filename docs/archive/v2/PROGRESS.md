# Quasar V2 开发进度

> 本文档跟踪 Quasar V2（Unified REST API）各开发阶段的完成状态。
> 每个阶段完成后更新，标注关键提交和验收状态。

---

## 阶段总览

| 阶段 | 名称 | 状态 | 关键提交 | 完成日期 |
|------|------|------|---------|---------|
| V2-S1 | DDL + Core 模型 | 已完成 | - | - |
| V2-S2a | Storage Namespace | 已完成 | - | - |
| V2-S2b | Storage Asset | 已完成 | - | - |
| V2-S2c | Storage Version + CAS | 已完成 | - | - |
| V2-S3 | 标准协议适配器格式隔离 | 已完成 | `1943a31` | - |
| **V2-S4** | **Unified API 骨架 + Namespace** | **已完成** | - | **2026/04/30** |
| V2-S5 | Unified Asset | 已完成 | - | 2026/05/06 |
| V2-S6 | 版本查看 | 已完成 | - | 2026/05/06 |
| V2-S7 | Feature 与集成 | 已完成 | - | 2026/05/06 |

---

## 已完成阶段详情

### V2-S1: DDL + Core 模型

- V2 目标 DDL（namespaces/assets/tabular_assets/asset_versions/tabular_asset_versions）
- Core 模型更新（AssetType, PatchField, Namespace.comment, AssetWithTabular 等）
- `pgcrypto` 扩展 + `gen_random_uuid()`

### V2-S2a: Storage Namespace

- `create_namespace` 支持 comment
- `list_namespaces` 分页
- `get_namespace` / `namespace_exists` / `drop_namespace`
- `update_namespace` comment 三态 PATCH + properties 增量更新

### V2-S2b: Storage Asset

- `create_asset` 事务内双表写入（assets + tabular_assets）
- `list_assets` 注入 `asset_type='table' AND asset_subtype=$format`
- `get_asset` / `get_asset_with_tabular` / `asset_exists` / `drop_asset` / `rename_asset`
- `update_asset_properties`
- `list_assets_unified` / `get_asset_unified`

### V2-S2c: Storage Version + CAS

- `create_version` 事务内双表写入（asset_versions + tabular_asset_versions）
- `load_version` / `load_current_version` / `list_versions`
- `cas_update_metadata_location` 同事务更新 tabular_assets + assets.properties

### V2-S3: 标准协议适配器格式隔离

- Iceberg 适配器所有查询注入 `AssetFormat::Iceberg`
- Lance 适配器所有查询注入 `AssetFormat::Lance`
- 7 个跨格式隔离集成测试（list/load/describe/drop/rename/exists/commit）
- 全部 adapter 回归测试通过（140 tests）

### V2-S4: Unified API 骨架 + Namespace

**前置依赖**: V2-S2a

**新增文件**:

| 文件 | 说明 |
|------|------|
| `adapter/src/unified/mod.rs` | 路由注册 + `UnifiedConfig` |
| `adapter/src/unified/error.rs` | RFC 7807 Problem Details + `UnifiedErrorCode` |
| `adapter/src/unified/dto.rs` | 请求/响应 DTO + 分页查询参数 |
| `adapter/src/unified/namespace.rs` | 5 个 Namespace handler |
| `adapter/tests/unified_namespace.rs` | 13 个集成测试 |

**修改文件**:

| 文件 | 修改内容 |
|------|---------|
| `adapter/Cargo.toml` | 添加 `unified` feature |
| `server/Cargo.toml` | 添加 `unified` feature |
| `adapter/src/lib.rs` | 条件编译导出 `unified` 模块 |
| `server/src/lib.rs` | `AppConfig` + 路由合并 unified |
| `server/src/metrics.rs` | request_id middleware 写入 request extensions |
| `storage/src/store.rs` | `drop_namespace` 检测 `RESTRICT_VIOLATION` (23001) |

**实现的端点**:

| 方法 | 路径 | 状态码 |
|------|------|--------|
| GET | `/unified/v1/namespaces` | 200 |
| POST | `/unified/v1/namespaces` | 201 |
| GET | `/unified/v1/namespaces/{ns}` | 200 |
| DELETE | `/unified/v1/namespaces/{ns}` | 204 |
| PATCH | `/unified/v1/namespaces/{ns}` | 200 |

**集成测试覆盖** (13 tests):

- 正向: create, list, list pagination, get, patch comment (Value/Null), patch properties, delete empty
- 负向: delete non-empty (409), duplicate create (409), not found (404)
- 格式验证: Problem Details Content-Type, request_id 透传

**验收状态**:

- [x] `cargo build --all-features` 编译通过
- [x] `cargo build --no-default-features --features "lance,iceberg"` 编译通过
- [x] 13 个 Unified Namespace 集成测试全部通过
- [x] 全部 adapter 回归测试通过
- [x] Storage 回归测试通过
- [x] Server 回归测试通过
- [x] `cargo fmt --check` 通过
- [x] `cargo clippy --all-features` 零警告
- [x] Problem Details 格式: Content-Type = `application/problem+json`
- [x] X-Request-Id header 透传正确

### V2-S5: Unified Asset

**前置依赖**: V2-S4, V2-S3

**新增文件**:

| 文件 | 说明 |
|------|------|
| `adapter/src/unified/asset.rs` | 5 个 Asset handler（list/get/delete/patch/rename） |
| `adapter/tests/unified_asset.rs` | 16 个集成测试 |

**修改文件**:

| 文件 | 修改内容 |
|------|---------|
| `adapter/src/unified/dto.rs` | 新增 AssetListItem、AssetResponse、CurrentVersionResponse、UpdateAssetRequest、RenameAssetRequest、ListAssetsResponse、AssetListQuery、AssetDetailQuery |
| `adapter/src/unified/mod.rs` | 注册 Asset 端点路由（保留 Namespace 路由） |

**实现的端点**:

| 方法 | 路径 | 状态码 |
|------|------|--------|
| GET | `/unified/v1/namespaces/{ns}/assets` | 200 |
| GET | `/unified/v1/namespaces/{ns}/assets/{name}` | 200 |
| DELETE | `/unified/v1/namespaces/{ns}/assets/{name}` | 204 |
| PATCH | `/unified/v1/namespaces/{ns}/assets/{name}` | 200 |
| POST | `/unified/v1/namespaces/{ns}/assets/{name}/rename` | 200 |

**集成测试覆盖** (16 tests):

- 正向: list 跨格式、list format 过滤、list name 过滤、list 分页、get detail、delete、patch comment、patch properties、rename
- 负向: get 缺 format (400 InvalidInput)、get 非法 format (400 InvalidFormat)、get not found (404)、delete not found (404)、rename 目标名已存在 (409)
- 格式验证: list 非法 format (400 InvalidFormat)

**关键设计决策**:

- `format` 查询参数对单资产操作 **必需**（同名 asset 可跨格式共存），对 list 操作可选
- 不暴露 `POST /assets`（Unified 不处理 Asset 创建）
- `current_version` 字段在 S5 中始终返回 `null`（V2-S6 实现版本读取）
- `#[serde(flatten)]` 与 URL 编码查询参数不兼容，改为在 `AssetListQuery` 中内联分页字段

**验收状态**:

- [x] `cargo build --all-features` 编译通过
- [x] `cargo build --no-default-features --features "lance,iceberg"` 编译通过
- [x] 16 个 Unified Asset 集成测试全部通过
- [x] 13 个 Unified Namespace 集成测试全部通过（无回归）
- [x] 全部 adapter 回归测试通过
- [x] Storage 回归测试通过
- [x] Server 回归测试通过
- [x] `cargo fmt --check` 通过
- [x] `cargo clippy --all-features` 零警告
- [x] Problem Details 格式: Content-Type = `application/problem+json`
- [x] format 参数验证正确: 缺失 → 400 InvalidInput, 非法值 → 400 InvalidFormat

### V2-S6: 版本查看

**前置依赖**: V2-S5

**新增文件**:

| 文件 | 说明 |
|------|------|
| `adapter/src/unified/version.rs` | `get_current_version` 函数：Lance 查询 + Iceberg metadata.json 解析 |

**修改文件**:

| 文件 | 修改内容 |
|------|---------|
| `core/src/models.rs` | `TabularAssetVersion` 新增 `previous_version_order: Option<i64>` |
| `storage/src/store.rs` | `get_asset_with_current_version` / `load_current_version` SQL JOIN `asset_versions prev`；`row_to_tabular_version` 读取 `previous_version_order`；`create_version` 设置 `previous_version_order` |
| `adapter/src/unified/mod.rs` | 注册 `version` 模块；`UnifiedConfig` 扩展 `object_store` + `s3_bucket`（iceberg feature 下） |
| `adapter/src/unified/asset.rs` | `asset_pair_to_response` 接受 `Option<CurrentVersionResponse>`；`get_asset` / `update_asset` handler 调用 `version::get_current_version` |
| `adapter/tests/unified_asset.rs` | 新增 5 个版本测试 |
| `server/src/main.rs` | unified feature block 中将 `IcebergConfig.object_store/s3_bucket` 克隆到 `UnifiedConfig` |

**实现的功能**:

| 格式 | 数据来源 | 字段 |
|------|---------|------|
| Iceberg | `metadata.json`（对象存储） | `sequence_number`, `snapshot_id`, `timestamp_ms` |
| Lance | `asset_versions` + `tabular_asset_versions`（DB） | `version_id`, `metadata_location`, `previous_version_id`, `timestamp` |

**关键设计决策**:

- S3 不可用或 `metadata.json` 不存在时，`current_version` 返回 `null`，请求不失败（log warning）
- `UnifiedConfig` 通过 `#[cfg(feature = "iceberg")]` 条件编译 `object_store` 字段，适配器层面不强制 iceberg 依赖
- Lance `previous_version_id` 通过 SQL LEFT JOIN `asset_versions prev` 一次查询获取，避免额外 round-trip

**集成测试覆盖** (新增 5 tests，总计 21 tests):

- Lance 无版本: `current_version` = null
- Lance 有版本: `version_id` / `metadata_location` / `previous_version_id` 正确
- Iceberg 无 metadata: `current_version` = null
- Iceberg 有 metadata: `sequence_number` / `snapshot_id` / `timestamp_ms` 正确
- Iceberg 无 snapshot: `snapshot_id` = null, `timestamp_ms` = null

**验收状态**:

- [x] `cargo build --all-features` 编译通过
- [x] `cargo build --no-default-features --features "lance,iceberg"` 编译通过
- [x] 21 个 Unified Asset 集成测试全部通过（16 原 + 5 新增）
- [x] 13 个 Unified Namespace 集成测试全部通过（无回归）
- [x] 全部 adapter 回归测试通过
- [x] Storage 回归测试通过
- [x] Server 回归测试通过
- [x] `cargo fmt --check` 通过
- [x] `cargo clippy --all-features` 零警告
- [x] Lance asset 无版本时 `current_version` = null
- [x] Lance asset 有版本时 `current_version` 字段正确
- [x] Iceberg asset metadata.json 不可用时 `current_version` = null（不报错）
- [x] Iceberg asset metadata.json 可解析时 `current_version` 字段正确

### V2-S7: Feature 与集成

**前置依赖**: V2-S6

**新增文件**:

| 文件 | 说明 |
|------|------|
| `server/tests/smoke.rs` | 双实例无状态 smoke test |

**S7 验证内容**:

| # | 验证项 | 结果 |
|---|--------|------|
| 1 | 关闭 `unified` feature 编译通过 | 通过 |
| 2 | 关闭 unified 后标准协议回归测试通过 | 通过（Iceberg 40+ / Lance 29+） |
| 3 | unified 测试被 `#[cfg(feature = "unified")]` 正确排除 | 通过（0 tests） |
| 4 | 全量测试（含 unified）通过 | 通过 |
| 5 | 双实例无状态 smoke test | 通过 |

**Smoke test 场景**:

- App1 (Unified API) 创建 namespace "prod" → App2 (Unified API) 读取确认
- App1 (Lance API) declare table "users" → App2 (Lance API) list 确认
- App1 (Iceberg API) 创建 namespace "staging" → App2 (Iceberg API) list 确认
- App2 (Unified API) 跨格式发现 "users" → App1 (Unified API) 删除 → App2 (Lance API) 确认已删除

**验收状态**:

- [x] `cargo build --all-features` 编译通过
- [x] `cargo build --no-default-features --features "lance,iceberg"` 编译通过
- [x] 全量 adapter 回归测试通过（含 unified 测试）
- [x] Storage 回归测试通过
- [x] Server 回归测试通过（含 smoke test）
- [x] `cargo fmt --check` 通过
- [x] `cargo clippy --all-features` 零警告
- [x] 关闭 unified feature 后 unified 测试被正确排除
- [x] 关闭 unified feature 后标准协议测试无回归
- [x] 双实例共享 PostgreSQL 数据一致

---

## 待开始阶段