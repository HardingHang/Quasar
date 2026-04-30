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
| V2-S5 | Unified Asset | 待开始 | - | - |
| V2-S6 | 版本查看 | 待开始 | - | - |
| V2-S7 | Feature 与集成 | 待开始 | - | - |

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

---

## 待开始阶段

### V2-S5: Unified Asset

**前置依赖**: V2-S4, V2-S3

**目标**: Asset 发现（跨格式 list）+ 管理（get/delete/patch/rename）

**端点**:
- GET `/unified/v1/namespaces/{ns}/assets`
- GET `/unified/v1/namespaces/{ns}/assets/{name}?format={format}`
- DELETE `/unified/v1/namespaces/{ns}/assets/{name}?format={format}`
- PATCH `/unified/v1/namespaces/{ns}/assets/{name}?format={format}`
- POST `/unified/v1/namespaces/{ns}/assets/{name}/rename?format={format}`

### V2-S6: 版本查看

**前置依赖**: V2-S5

**目标**: Iceberg/Lance `current_version` 嵌入 Asset 详情

### V2-S7: Feature 与集成

**前置依赖**: V2-S6

**目标**: 条件编译验证、无状态 smoke test
