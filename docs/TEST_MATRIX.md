# Quasar 测试矩阵

> 本文档集中维护所有测试用例的描述，作为测试覆盖的权威目录。
> 每个测试函数对应一行：名称、类型、场景、验证点。
>
> **运行全部测试：** `cd quasar && cargo test`

---

## 测试统计

| Crate | 单元测试 | 集成测试 | 合计 |
|-------|---------|---------|------|
| quasar-core | 13 | 0 | 13 |
| quasar-server | 7 | 3 | 10 |
| quasar-adapter | 15 | 33 | 48 |
| quasar-storage | 0 | 5 | 5 |
| **合计** | **35** | **41** | **76** |

---

## quasar-core

> 位置：`quasar/core/src/`
> 测试类型：单元测试（`#[cfg(test)] mod tests`）

### StoreError (`src/error.rs`)

| 测试函数 | 场景 | 验证点 |
|---------|------|--------|
| `test_not_found_display` | NotFound 变体 | Display 输出格式：`"not found: {msg}"` |
| `test_already_exists_display` | AlreadyExists 变体 | Display 输出格式：`"already exists: {msg}"` |
| `test_conflict_display` | Conflict 变体 | Display 输出格式：`"conflict: {msg}"` |
| `test_invalid_input_display` | InvalidInput 变体 | Display 输出格式：`"invalid input: {msg}"` |
| `test_internal_display` | Internal 变体 | Display 输出格式：`"internal error: {msg}"` |

### 模型结构体 (`src/models.rs`)

| 测试函数 | 场景 | 验证点 |
|---------|------|--------|
| `test_asset_format_as_str` | AssetFormat 字符串映射 | `Iceberg` → `"iceberg"`, `Lance` → `"lance"` |
| `test_asset_format_serde_roundtrip` | AssetFormat 序列化/反序列化 | `snake_case` 序列化，完整往返正确 |
| `test_namespace_serde_roundtrip` | Namespace 序列化/反序列化 | 所有字段（含 HashMap properties）往返正确 |
| `test_asset_serde_roundtrip` | Asset 序列化/反序列化 | 所有字段（含 namespace_id 关联）往返正确 |
| `test_asset_version_serde_roundtrip` | AssetVersion 序列化/反序列化 | 所有字段（含 previous_version_id: Some）往返正确 |
| `test_asset_version_without_previous` | AssetVersion 无前驱版本 | `previous_version_id: None` 序列化/反序列化正确 |
| `test_asset_commit_update_serde` | AssetCommitUpdate 序列化 | 含 previous_version_id 的完整字段往返 |
| `test_asset_commit_update_without_previous` | AssetCommitUpdate 无 previous | `previous_version_id: None` 序列化/反序列化正确 |

---

## quasar-server

> 位置：`quasar/server/src/`
> 测试类型：单元测试 + 集成测试（`tests/`）

### Config (`src/config.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_config_defaults` | 单元 | 无环境变量 | host/port/db_url/log_level/s3 全部使用默认值 |
| `test_config_custom_values` | 单元 | 全部环境变量自定义 | 每个字段正确读取自定义值 |
| `test_config_log_level_falls_back_to_rust_log` | 单元 | QUASAR_LOG_LEVEL 缺失 | 自动回退到 RUST_LOG 环境变量 |
| `test_config_quasar_log_level_takes_precedence_over_rust_log` | 单元 | 两者同时设置 | QUASAR_LOG_LEVEL 优先级高于 RUST_LOG |
| `test_config_invalid_port_uses_default` | 单元 | QUASAR_PORT 为非法字符串 | parse 失败后回退默认值 8080 |
| `test_config_s3_allow_http_numeric_true` | 单元 | QUASAR_S3_ALLOW_HTTP = "1" | 解析为 true |
| `test_config_s3_allow_http_false` | 单元 | QUASAR_S3_ALLOW_HTTP = "false" | 解析为 false |

### 健康检查 (`tests/health.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_healthz` | 集成 | GET /healthz | 返回 200，`{"status": "ok"}` |
| `test_readyz` | 集成 | GET /readyz（DB 正常） | 返回 200，`{"status": "ready", "checks": {"database": "ok"}}` |
| `test_lance_routes_still_work` | 集成 | Lance 路由与 health 共存 | GET /lance/v1/namespace/$/list 返回 200 空列表 |

---

## quasar-adapter

> 位置：`quasar/adapter/src/lance/` + `quasar/adapter/tests/`
> 测试类型：单元测试 + 集成测试

### Lance 错误映射 (`src/lance/error.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_lance_error_namespace_not_found_to_problem_details` | 单元 | NamespaceNotFound → ProblemDetails | code=404, error="NamespaceNotFound" |
| `test_lance_error_namespace_already_exists_to_problem_details` | 单元 | NamespaceAlreadyExists → ProblemDetails | code=409, error="NamespaceAlreadyExists" |
| `test_lance_error_table_not_found_to_problem_details` | 单元 | TableNotFound → ProblemDetails | code=404, error="TableNotFound" |
| `test_lance_error_table_version_already_exists_to_problem_details` | 单元 | TableVersionAlreadyExists → ProblemDetails | code=409, error="TableVersionAlreadyExists" |
| `test_lance_error_invalid_input_to_problem_details` | 单元 | InvalidInput → ProblemDetails | code=400, detail 原样保留 |
| `test_lance_error_internal_error_to_problem_details` | 单元 | InternalError → ProblemDetails | code=500, error="InternalError" |
| `test_problem_details_serde` | 单元 | ProblemDetails 序列化 | JSON 包含 error/code/detail/instance 字段 |
| `test_store_error_to_lance_mapping` | 单元 | StoreError → LanceError（namespace） | 5 种 StoreError 全变体正确映射 |
| `test_store_error_to_lance_table_mapping` | 单元 | StoreError → LanceError（table） | NotFound/AlreadyExists 映射为 TableNotFound/TableAlreadyExists |
| `test_store_error_to_lance_version_parses_version_number` | 单元 | "version 42 already exists" 解析 | 提取 version=42，映射为 TableVersionAlreadyExists |
| `test_store_error_to_lance_version_non_version_message` | 单元 | 非 version 开头的 AlreadyExists | 回退到 TableAlreadyExists |
| `test_store_error_to_lance_version_not_found` | 单元 | StoreError::NotFound → LanceError（version） | 映射为 TableNotFound |
| `test_store_error_to_lance_version_conflict` | 单元 | StoreError::Conflict → LanceError（version） | 映射为 TableNotEmpty |
| `test_store_error_to_lance_version_invalid_input` | 单元 | StoreError::InvalidInput → LanceError（version） | 映射为 InvalidInput |
| `test_store_error_to_lance_version_internal` | 单元 | StoreError::Internal → LanceError（version） | 映射为 InternalError |

### Lance Namespace 端点 (`tests/lance_namespace.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_describe_namespace` | 集成 | POST create → POST describe | 返回 name/properties/id，数据持久化 |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同名 Namespace | 返回 409，error="NamespaceAlreadyExists" |
| `test_list_namespaces` | 集成 | 创建多个 Namespace 后列表 | 返回全部 Lance format 的 Namespace |
| `test_namespace_exists` | 集成 | 对已存在/不存在的 Namespace exists | true → 200, false → 200 |
| `test_drop_namespace` | 集成 | POST drop 后再次 drop | 首次 200，重复 404 |
| `test_describe_not_found` | 集成 | describe 不存在的 Namespace | 返回 404，error="NamespaceNotFound" |
| `test_list_namespaces_empty` | 集成 | 无 Namespace 时列表 | 返回空数组，next_page_token=null |
| `test_list_namespaces_pagination_offset_beyond_total` | 集成 | offset 大于总数 | 返回空数组，不报错 |
| `test_list_namespaces_pagination_with_limit` | 集成 | limit 限制返回数量 | 返回数量 <= limit |
| `test_list_namespaces_format_isolation` | 集成 | Iceberg/Lance Namespace 共存 | 只返回 Lance format 的 Namespace |

### Lance Table 端点 (`tests/lance_table.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_declare_and_describe_table` | 集成 | POST declare → POST describe | 返回 name/location，current_version=null |
| `test_declare_duplicate_returns_409` | 集成 | 重复 declare 同名 Table | 返回 409，error="TableAlreadyExists" |
| `test_list_tables` | 集成 | 创建多个 Table 后列表 | 返回全部 Table |
| `test_table_exists` | 集成 | 对已存在/不存在的 Table exists | true → 200, false → 200 |
| `test_register_table` | 集成 | POST register 提供 location | 返回 name/location，数据持久化 |
| `test_deregister_table` | 集成 | POST deregister 后再次 | 首次 200，重复 404 |
| `test_drop_table` | 集成 | POST drop 后 exists | drop 200，exists 返回 false |
| `test_rename_table` | 集成 | POST rename 后新旧验证 | 旧名不存在，新名存在 |
| `test_describe_not_found` | 集成 | describe 不存在的 Table | 返回 404，error="TableNotFound" |
| `test_declare_namespace_not_found` | 集成 | 在不存在的 Namespace 中 declare | 返回 404，error="TableNotFound" |
| `test_register_duplicate_returns_409` | 集成 | 对已存在的 Table register | 返回 409，error="TableAlreadyExists" |
| `test_rename_to_existing_name_returns_409` | 集成 | rename 到已存在的名称 | 返回 409，error="TableAlreadyExists" |
| `test_drop_table_not_found` | 集成 | drop 不存在的 Table | 返回 404，error="TableNotFound" |
| `test_rename_table_not_found` | 集成 | rename 不存在的 Table | 返回 404，error="TableNotFound" |
| `test_list_tables_empty_namespace` | 集成 | 无 Table 的 Namespace 列表 | 返回空数组，next_page_token=null |
| `test_exists_table_not_found_in_existing_namespace` | 集成 | exists 不存在的 Table（ns 存在） | 返回 200，`{"exists": false}` |
| `test_exists_namespace_not_found_returns_404` | 集成 | exists 时 Namespace 不存在 | 返回 404，error="TableNotFound" |

### Lance Version 端点 (`tests/lance_version.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_describe_version` | 集成 | POST version/create → POST version/describe | 返回 version/manifest_path |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同版本号 | 返回 409，error="TableVersionAlreadyExists" |
| `test_list_versions` | 集成 | 创建 v1/v2 后列表 | 返回 [1, 2] |
| `test_describe_not_found` | 集成 | describe 不存在的版本 | 返回 404，error="TableNotFound" |
| `test_create_table_not_found` | 集成 | 对不存在的 Table 创建版本 | 返回 404，error="TableNotFound" |
| `test_describe_current_version_in_describe_table` | 集成 | 创建版本后 describe_table | current_version 联动更新为最新版本 |

---

## quasar-storage

> 位置：`quasar/storage/tests/integration.rs`
> 测试类型：集成测试（嵌入式 PostgreSQL）

### CatalogStore 集成 (`tests/integration.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_namespace_crud` | 集成 | Namespace 完整 CRUD | 创建/列出/获取/存在检查/删除全链路 |
| `test_asset_crud` | 集成 | Asset 完整 CRUD | 创建/列出/获取/存在检查/重命名/删除全链路 |
| `test_version_commit_and_load` | 集成 | 版本提交与查询 | 顺序提交 v1→v2，load_version/load_current/list_versions 正确 |
| `test_version_conflict` | 集成 | CAS 乐观锁冲突 | previous_version_id 不匹配返回 Conflict |
| `test_not_found_errors` | 集成 | NotFound 场景 | namespace 不存在、asset 不存在、无版本记录 |

---

## 修订记录

### V1.0（2026-04-20）

- 新增：quasar-core 测试矩阵（13 个单元测试）
- 新增：quasar-server 测试矩阵（10 个测试）
- 新增：quasar-adapter 测试矩阵（48 个测试）
- 新增：quasar-storage 测试矩阵（5 个测试）
- 新增：测试统计总览表

---

**后续修订规则：** 新增/修改/删除测试函数时，同步更新本文档对应条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整）。
