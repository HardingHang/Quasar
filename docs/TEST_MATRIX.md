# Quasar 测试矩阵

> 本文档集中维护所有测试用例的描述，作为测试覆盖的权威目录。
> 每个测试函数对应一行：名称、类型、场景、验证点。
>
> **运行全部测试：** `cargo test --workspace --all-features --all-targets`

---

## 测试统计

| Crate | 单元测试 | 集成测试 | 合计 |
|-------|---------|---------|------|
| quasar-core | 35 | 0 | 35 |
| quasar-server | 9 | 11 | 20 |
| quasar-adapter | 67 | 141 | 208 |
| quasar-storage | 15 | 16 | 31 |
| **合计** | **126** | **168** | **294** |

> 注：统计基于 `cargo test --workspace --all-features --all-targets` 的测试函数数量，不含 doc-tests（当前为 0）。

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
| `test_namespace_not_empty_display` | NamespaceNotEmpty 变体 | Display 输出含 namespace 名 |
| `test_domain_not_empty_display` | DomainNotEmpty 变体 | Display 输出含 domain 名 |
| `test_database_unavailable_display_without_source` | DatabaseUnavailable 无 source | Display 输出格式正确 |
| `test_database_unavailable_chains_source` | DatabaseUnavailable 有 source | source 被正确链式记录 |
| `test_timeout_display` | Timeout 变体 | Display 输出格式正确 |
| `test_internal_display_without_source` | Internal 无 source | Display 输出格式正确 |
| `test_internal_chains_source` | Internal 有 source | source 被正确链式记录 |

### 模型结构体 (`src/models.rs`)

| 测试函数 | 场景 | 验证点 |
|---------|------|--------|
| `test_asset_format_as_str` | AssetFormat 字符串映射 | `Iceberg` → `"iceberg"`, `Lance` → `"lance"` |
| `test_asset_format_serde_roundtrip` | AssetFormat 序列化/反序列化 | `snake_case` 序列化，完整往返正确 |
| `test_asset_type_as_str` | AssetType 字符串映射 | `Table` → `"table"` |
| `test_asset_type_serde_roundtrip` | AssetType 序列化/反序列化 | `snake_case` 序列化，完整往返正确 |
| `test_patch_field_deserialize` | PatchField 三态反序列化 | missing/null/value 分别映射为 Missing/Null/Value |
| `test_patch_field_default` | PatchField 默认值 | 默认值为 Missing |
| `test_domain_default` | Domain 默认值 | 默认 domain name 为 `"default"` |
| `test_domain_serde_roundtrip` | Domain 序列化/反序列化 | 所有字段往返正确 |
| `test_namespace_serde_roundtrip` | Namespace 序列化/反序列化 | 所有字段（含 HashMap properties）往返正确 |
| `test_namespace_without_comment` | Namespace 无 comment | `comment: None` 往返正确 |
| `test_asset_serde_roundtrip` | Asset 序列化/反序列化 | 所有字段（含 namespace_id 关联）往返正确 |
| `test_tabular_asset_serde_roundtrip` | TabularAsset 序列化/反序列化 | location/metadata_location 往返正确 |
| `test_asset_with_tabular_serde_roundtrip` | AssetWithTabular 组合结构 | asset 与 tabular 子结构往返正确 |
| `test_asset_version_serde_roundtrip` | AssetVersion 序列化/反序列化 | version_key/version_order 往返正确 |
| `test_asset_version_without_order` | AssetVersion 无排序号 | `version_order: None` 往返正确 |
| `test_tabular_asset_version_serde_roundtrip` | TabularAssetVersion 序列化/反序列化 | metadata_location/previous 字段往返正确 |
| `test_tabular_asset_version_without_previous` | TabularAssetVersion 无前驱 | previous 字段为 None 时往返正确 |
| `test_asset_version_with_tabular_serde_roundtrip` | AssetVersionWithTabular 组合结构 | version 与 tabular_version 子结构往返正确 |

### 名称校验 (`src/validation.rs`)

| 测试函数 | 场景 | 验证点 |
|---------|------|--------|
| `test_valid_names` | 合法名称集合 | 字母、数字、下划线、短横线、点号、斜杠均可通过 |
| `test_empty_name_fails` | 空名称 | 返回 InvalidInput，提示 empty |
| `test_too_long_name_fails` | 超过 256 字符 | 返回 InvalidInput，提示 exceeds |
| `test_max_length_ok` | 正好 256 字符 | 校验通过 |
| `test_dot_prefix_fails` | 点号开头 | 返回 InvalidInput，提示 dot |
| `test_invalid_characters_fails` | 空格、换行、美元符等非法字符 | 返回 InvalidInput，提示 invalid character |

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
| `test_config_db_max_connections_default` | 单元 | 未设置 QUASAR_DB_MAX_CONNECTIONS | 使用默认连接数 10 |
| `test_config_db_max_connections_custom` | 单元 | 自定义 QUASAR_DB_MAX_CONNECTIONS | 正确解析连接池最大连接数 |

### 健康检查 (`tests/health.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_healthz` | 集成 | GET /healthz | 返回 200，`{"status": "ok"}` |
| `test_readyz` | 集成 | GET /readyz（DB 正常） | 返回 200，`{"status": "ready", "checks": {"database": "ok"}}` |
| `test_lance_routes_still_work` | 集成 | Lance 路由与 health 共存 | GET /lance/v1/namespace/$/list 返回 200 空列表 |
| `test_iceberg_routes_are_mounted` | 集成 | Iceberg 路由挂载 | GET /iceberg/v1/config 返回 200 |

### 无状态烟雾测试 (`tests/smoke.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_dual_instance_stateless_smoke` | 集成 | 两个 server 实例共享同一 PostgreSQL | Unified/Lance/Iceberg 跨实例读写一致，删除后可见性一致 |
| `test_iceberg_full_lifecycle` | 集成 | Iceberg 完整生命周期 | create domain → ns → table → load → commit → drop，各阶段状态正确 |
| `test_lance_full_lifecycle` | 集成 | Lance 完整生命周期 | declare → version → list → drop，各阶段状态正确 |
| `test_domain_isolation_same_namespace_name` | 集成 | 不同 Domain 同名 Namespace 隔离 | prod/analytics 与 staging/analytics 互不干扰 |
| `test_cross_format_name_conflict_server_level` | 集成 | 跨格式同名冲突 | 同一 domain/ns 下 iceberg create 后 lance declare 同名 → 409 |
| `test_non_empty_domain_delete_protection` | 集成 | 非空 Domain 删除保护 | domain 含 namespace 时 DELETE 返回 409 |
| `test_multi_domain_namespace_same_name` | 集成 | 多 Domain 同名 Namespace | prod/analytics 和 staging/analytics 均可创建成功 |

---

## quasar-adapter

> 位置：`quasar/adapter/src/` + `quasar/adapter/tests/`
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
| `test_store_error_to_lance_version_already_exists_maps_to_table_exists` | 单元 | version AlreadyExists 解析 | 映射为 TableAlreadyExists |
| `test_store_error_to_lance_version_not_found` | 单元 | StoreError::NotFound → LanceError（version） | 映射为 TableNotFound |
| `test_store_error_to_lance_version_conflict` | 单元 | StoreError::Conflict → LanceError（version） | 映射为 TableNotEmpty |
| `test_store_error_to_lance_version_invalid_input` | 单元 | StoreError::InvalidInput → LanceError（version） | 映射为 InvalidInput |
| `test_store_error_to_lance_version_internal` | 单元 | StoreError::Internal → LanceError（version） | 映射为 InternalError |

### Lance ID 解析器 (`src/lance/id.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `parse_namespace_id_root` | 单元 | `"$"` 根 Namespace | 解析为 Root |
| `parse_namespace_id_single_segment_is_domain` | 单元 | `"prod"` 单段 | 解析为 Domain |
| `parse_namespace_id_two_segments_is_namespace` | 单元 | `"prod$analytics"` 两段 | 解析为 Namespace {domain, namespace} |
| `parse_namespace_id_three_segments_rejected` | 单元 | `"a$b$c"` 三段 Namespace | 拒绝，返回 Err |
| `parse_namespace_id_four_segments_rejected` | 单元 | `"a$b$c$d"` 四段 | 拒绝，返回 Err |
| `parse_namespace_id_empty_inner_segment_rejected` | 单元 | `"a$$b"` 含空段 | 拒绝，返回 Err |
| `parse_namespace_id_trailing_empty_segment_rejected` | 单元 | `"a$"` 尾部空段 | 拒绝，返回 Err |
| `parse_namespace_id_leading_empty_segment_rejected` | 单元 | `"$$"` 头部空段 | 拒绝，返回 Err |
| `parse_table_id_three_segments_accepted` | 单元 | `"prod$analytics$events"` 三段 Table | 解析为 LanceTableId |
| `parse_table_id_two_segments_rejected` | 单元 | `"analytics$events"` 两段 Table | 拒绝，返回 Err |
| `parse_table_id_four_segments_rejected` | 单元 | `"a$b$c$d"` 四段 Table | 拒绝，返回 Err |
| `parse_table_id_empty_segment_rejected` | 单元 | `"a$$b"` / `"a$b$"` / `"$b$c"` | 拒绝，返回 Err |

### Lance Namespace 端点 (`tests/lance_namespace.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_describe_namespace` | 集成 | POST create → POST describe | 返回 name/properties/id，数据持久化 |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同名 Namespace | 返回 409，error="NamespaceAlreadyExists" |
| `test_list_namespaces` | 集成 | 创建多个 Namespace 后列表 | 返回全部共享 Namespace |
| `test_namespace_exists` | 集成 | 对已存在/不存在的 Namespace exists | true → 200, false → 200 |
| `test_drop_namespace` | 集成 | POST drop 后再次 drop | 首次 200，重复 404 |
| `test_describe_not_found` | 集成 | describe 不存在的 Namespace | 返回 404，error="NamespaceNotFound" |
| `test_list_namespaces_pagination_with_limit` | 集成 | limit 限制返回数量 | 返回数量 <= limit |
| `test_root_list_returns_domains` | 集成 | GET /lance/v1/namespace/$/list | 返回全部 Domain 列表 |
| `test_single_segment_id_rejected_on_create` | 集成 | POST create 单段 id | 拒绝，返回 InvalidInput |
| `test_namespace_in_non_default_domain` | 集成 | 在非 default Domain 下创建 Namespace | 创建成功，数据按 Domain 隔离 |

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
| `test_rename_cross_namespace` | 集成 | 同 Domain 跨 Namespace rename | 返回 200，数据正确迁移 |
| `test_rename_cross_domain_rejected` | 集成 | 跨 Domain rename | 返回 400，error="InvalidInput" |
| `test_list_tables_empty_namespace` | 集成 | 无 Table 的 Namespace 列表 | 返回空数组，next_page_token=null |
| `test_exists_table_not_found_in_existing_namespace` | 集成 | exists 不存在的 Table（ns 存在） | 返回 200，`{"exists": false}` |
| `test_exists_namespace_not_found_returns_false` | 集成 | exists 时 Namespace 不存在 | 返回 200，`{"exists": false}` |
| `test_declare_in_non_default_domain` | 集成 | 在非 default Domain 下 declare Table | 创建成功，数据按 Domain 隔离 |

### Lance Version 端点 (`tests/lance_version.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_describe_version` | 集成 | POST version/create → POST version/describe | 返回 version/manifest_path |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同版本号 | 返回 409，error="TableVersionAlreadyExists" |
| `test_list_versions` | 集成 | 创建 v1/v2 后列表 | 返回 [1, 2]，按 version_order 升序 |
| `test_describe_not_found` | 集成 | describe 不存在的版本 | 返回 404，error="TableNotFound" |
| `test_create_table_not_found` | 集成 | 对不存在的 Table 创建版本 | 返回 404，error="TableNotFound" |
| `test_describe_current_version_in_describe_table` | 集成 | 创建版本后 describe_table | current_version 联动更新为最新版本 |

### Unified 错误映射 (`src/unified/error.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `internal_error_redacts_msg` | 单元 | Internal → UnifiedError | detail 固定为 "An internal error occurred"，不泄露内部信息 |
| `domain_not_empty_maps_to_domain_not_empty` | 单元 | DomainNotEmpty → UnifiedError | code=DomainNotEmpty，detail 含 domain 名 |

### Iceberg 错误映射 (`src/iceberg/error.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_iceberg_error_no_such_namespace_to_response` | 单元 | NoSuchNamespaceException → ErrorResponse | code=404, type="NoSuchNamespaceException" |
| `test_iceberg_error_namespace_already_exists_to_response` | 单元 | NamespaceAlreadyExistsException → ErrorResponse | code=409, type="NamespaceAlreadyExistsException" |
| `test_iceberg_error_no_such_table_to_response` | 单元 | NoSuchTableException → ErrorResponse | code=404, type="NoSuchTableException" |
| `test_iceberg_error_table_already_exists_to_response` | 单元 | TableAlreadyExistsException → ErrorResponse | code=409, type="TableAlreadyExistsException" |
| `test_iceberg_error_bad_request_to_response` | 单元 | BadRequestException → ErrorResponse | code=400, type="BadRequestException" |
| `test_iceberg_error_commit_failed_to_response` | 单元 | CommitFailedException → ErrorResponse | code=409, type="CommitFailedException" |
| `test_iceberg_error_internal_to_response` | 单元 | InternalServerError → ErrorResponse | code=500, type="InternalServerError" |
| `test_iceberg_error_metadata_not_found_to_response` | 单元 | MetadataNotFoundException → ErrorResponse | code=404, type="MetadataNotFoundException" |
| `test_error_response_serde` | 单元 | ErrorResponse 序列化 | JSON 包含 error/message/type/code 字段 |
| `test_store_error_to_iceberg_namespace_mapping` | 单元 | StoreError → IcebergError（namespace） | 5 种 StoreError 全变体正确映射 |
| `test_store_error_to_iceberg_table_mapping` | 单元 | StoreError → IcebergError（table） | 5 种 StoreError 全变体正确映射 |

### Iceberg TableMetadata (`src/iceberg/table_metadata.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_check_requirements_uuid_ok` | 单元 | AssertTableUuid验证通过 | table_uuid匹配 → Ok |
| `test_check_requirements_uuid_fail` | 单元 | AssertTableUuid验证失败 | table_uuid不匹配 → Err |
| `test_check_requirements_snapshot_id_ok` | 单元 | AssertRefSnapshotId(main)通过 | current_snapshot_id匹配 |
| `test_check_requirements_snapshot_id_fail` | 单元 | AssertRefSnapshotId(main)失败 | snapshot-id mismatch错误 |
| `test_check_requirements_assert_create_fails` | 单元 | AssertCreate失败 | 表已存在 → Err |
| `test_check_requirements_snapshot_id_null_ok` | 单元 | snapshot_id=null验证通过 | current_snapshot_id=None → Ok |
| `test_check_requirements_snapshot_id_null_fail` | 单元 | snapshot_id=null验证失败 | current_snapshot_id=Some → Err |
| `test_check_requirements_ref_custom_branch` | 单元 | 自定义ref验证 | staging ref正确/错误snapshot-id |
| `test_check_requirements_multiple` | 单元 | 多requirements组合 | 全通过/任一失败 |
| `test_apply_updates_add_snapshot` | 单元 | AddSnapshot更新 | snapshots/current_snapshot_id/last_sequence_number |
| `test_apply_updates_set_snapshot_ref` | 单元 | SetSnapshotRef更新 | refs/main/current_snapshot_id |
| `test_apply_updates_set_properties` | 单元 | SetProperties更新 | properties HashMap |
| `test_apply_updates_remove_properties` | 单元 | RemoveProperties更新 | properties移除 |
| `test_apply_updates_multiple` | 单元 | 多updates组合 | snapshots+refs+properties联动 |
| `test_next_metadata_location_increment` | 单元 | metadata_location递增 | 00001→00002格式 |
| `test_serde_roundtrip` | 单元 | TableMetadata序列化 | JSON往返正确 |
| `test_snapshot_ref_default_type` | 单元 | SnapshotRef默认type | type="branch" |

### Iceberg DTO 序列化 (`src/iceberg/dto.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_namespace_request_serde` | 单元 | CreateNamespaceRequest 反序列化 | namespace 数组 + properties（可选）正确解析 |
| `test_namespace_response_serde` | 单元 | NamespaceResponse 序列化 | 空 properties 不输出，非空输出 |
| `test_list_namespaces_query_serde` | 单元 | ListNamespacesQuery 反序列化 | pageToken/pageSize 可选字段正确 |
| `test_list_namespaces_response_serde` | 单元 | ListNamespacesResponse 序列化 | nextPageToken 有/无两种情况 |
| `test_update_namespace_properties_request_serde` | 单元 | UpdateNamespacePropertiesRequest | removals/updates 可选字段正确 |
| `test_update_namespace_properties_response_serde` | 单元 | UpdateNamespacePropertiesResponse | missing 空数组时不输出 |
| `test_create_table_request_serde` | 单元 | CreateTableRequest 反序列化 | name 必填，location/schema/properties 可选 |
| `test_load_table_response_serde` | 单元 | LoadTableResponse 序列化 | metadata-location + metadata 正确 |
| `test_list_tables_response_serde` | 单元 | ListTablesResponse 序列化 | identifiers 数组正确 |
| `test_rename_table_request_serde` | 单元 | RenameTableRequest 反序列化 | source/destination 结构正确 |
| `test_table_identifier_serde` | 单元 | TableIdentifier 序列化 | namespace 数组 + name 正确 |

### Iceberg Namespace 端点 (`tests/iceberg_namespace.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_get_namespace` | 集成 | POST create → GET | 返回 namespace 数组，数据持久化 |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同名 Namespace | 返回 409，type="NamespaceAlreadyExistsException" |
| `test_list_namespaces` | 集成 | 创建多个 Namespace 后列表 | 返回全部 Iceberg format 的 Namespace |
| `test_get_namespace_not_found` | 集成 | GET 不存在的 Namespace | 返回 404，type="NoSuchNamespaceException" |
| `test_drop_namespace` | 集成 | DELETE 后再次 GET | 首次 204，再次 404 |
| `test_drop_non_empty_namespace` | 集成 | DELETE 含 Table 的 Namespace | 返回 400，type="BadRequestException" |
| `test_update_namespace_properties` | 集成 | POST properties 更新 | removed/updated/missing 正确，properties 持久化 |
| `test_update_namespace_properties_namespace_not_found` | 集成 | 对不存在的 Namespace 更新 properties | 返回 404，type="NoSuchNamespaceException" |
| `test_list_namespaces_empty` | 集成 | 无 Namespace 时列表 | 返回空数组，nextPageToken=null |
| `test_list_namespaces_pagination_offset_beyond_total` | 集成 | pageToken 超出总数 | 返回空数组，不报错 |
| `test_list_namespaces_pagination_with_limit` | 集成 | pageSize 限制返回数量 | 返回数量 <= limit，nextPageToken 正确 |
| `test_create_namespace_empty_array_returns_400` | 集成 | namespace 数组为空 | 返回 400，type="BadRequestException" |
| `test_drop_namespace_not_found` | 集成 | DELETE 不存在的 Namespace | 返回 404，type="NoSuchNamespaceException" |

### Iceberg Table 端点 (`tests/iceberg_table.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_and_load_table` | 集成 | POST create → GET load | 返回 metadata-location + TableMetadata |
| `test_create_duplicate_returns_409` | 集成 | 重复创建同名 Table | 返回 409，type="TableAlreadyExistsException" |
| `test_list_tables` | 集成 | 创建多个 Table 后列表 | 返回全部 Table identifiers |
| `test_load_table_not_found` | 集成 | GET 不存在的 Table | 返回 404，type="NoSuchTableException" |
| `test_drop_table` | 集成 | DELETE 后再次 GET | 首次 204，再次 404 |
| `test_table_exists` | 集成 | HEAD 存在/不存在检查 | 存在 → 200，不存在 → 404 |
| `test_rename_table` | 集成 | POST rename 后验证 | 旧名 HEAD 404，新名 HEAD 200 |
| `test_rename_cross_namespace` | 集成 | 同 Domain 跨 Namespace rename | 返回 200，数据正确迁移 |
| `test_create_table_namespace_not_found` | 集成 | 在不存在的 Namespace 创建 Table | 返回 404，type="NoSuchNamespaceException" |
| `test_rename_table_source_not_found` | 集成 | rename 不存在的 source Table | 返回 404，type="NoSuchTableException" |
| `test_rename_table_destination_already_exists` | 集成 | rename 到已存在的名称 | 返回 409，type="TableAlreadyExistsException" |
| `test_drop_table_not_found` | 集成 | DELETE 不存在的 Table | 返回 404，type="NoSuchTableException" |
| `test_list_tables_empty_namespace` | 集成 | 无 Table 的 Namespace 列表 | 返回空数组，nextPageToken=null |

### Iceberg Commit 端点 (`tests/iceberg_commit.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_commit_success` | 集成 | POST create → POST commit | 返回 metadata-location + metadata，版本号递增 |
| `test_commit_conflict` | 集成 | metadata_location匹配的commit | 验证CAS预期行为（非真正冲突） |
| `test_commit_requirement_failure` | 集成 | AssertRefSnapshotId验证失败 | 返回 409，type="CommitFailedException" |
| `test_commit_table_not_found` | 集成 | commit不存在的表 | 返回 404，type="NoSuchTableException" |
| `test_commit_updates_persisted` | 集成 | commit后load验证 | metadata/snapshots/properties/refs持久化 |
| `test_commit_cas_conflict_simulated` | 集成 | 存储层CAS冲突 | expected不匹配 → StoreError::Conflict |
| `test_concurrent_cas_conflict_end_to_end` | 集成 | 连续commit requirement冲突 | 第二次返回 409 CommitFailedException |
| `test_assert_ref_snapshot_id_custom_branch` | 集成 | 自定义ref(staging)验证通过 | 正确snapshot-id → 200 OK |
| `test_assert_ref_snapshot_id_custom_branch_fail` | 集成 | 自定义ref(staging)验证失败 | 错误snapshot-id → 409 |
| `test_assert_table_uuid_failure` | 集成 | AssertTableUuid验证失败 | UUID不匹配 → 409 CommitFailedException |
| `test_multiple_commit_version_increment` | 集成 | 多次commit版本号递增 | 00001→00002→00003 |
| `test_set_snapshot_ref_with_tag_type` | 集成 | SetSnapshotRef type=TAG | refs.v1.0.type="tag" |
| `test_commit_namespace_not_found` | 集成 | namespace不存在 | 返回 404，type="NoSuchTableException" |
| `test_commit_empty_updates` | 集成 | requirements有但updates空 | 空updates → 200 OK（no-op） |
| `test_remove_properties_commit` | 集成 | RemoveProperties端到端 | 属性删除持久化验证 |

### Iceberg Object Store 端点 (`tests/iceberg_object_store.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_table_writes_metadata_to_object_store` | 集成 | 创建 Table 时启用 ObjectStore | metadata.json 写入对象存储，内容与响应一致 |
| `test_commit_table_writes_new_metadata_to_object_store` | 集成 | Commit 后生成新 metadata | 新旧 metadata 均存在，新 metadata 包含 snapshot/ref 更新 |
| `test_load_table_reads_from_object_store` | 集成 | Load Table 从 ObjectStore 读取 | 返回对象存储中的最新 metadata 内容 |
| `test_load_table_metadata_not_found` | 集成 | load 时 metadata 缺失 | 返回 404 MetadataNotFoundException |
| `test_commit_table_metadata_not_found` | 集成 | commit 时 metadata 缺失 | 返回 409 CommitFailedException |

### Iceberg Config 端点 (`tests/iceberg_config.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_get_config` | 集成 | GET /iceberg/v1/config | 返回 defaults + overrides 对象 |

### 跨格式隔离 (`tests/format_isolation.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_cross_format_create_conflicts` | 集成 | 同名 Iceberg/Lance 表创建冲突 | 同一 namespace 下第二个创建返回 409 |
| `test_cross_format_load_isolation` | 集成 | Iceberg load Lance-only 表 | 返回 NoSuchTableException |
| `test_cross_format_describe_isolation` | 集成 | Lance describe Iceberg-only 表 | 返回 TableNotFound |
| `test_lance_drop_cannot_target_iceberg_asset` | 集成 | Lance drop Iceberg-only 表 | 返回 TableNotFound |
| `test_lance_rename_cannot_target_iceberg_asset` | 集成 | Lance rename Iceberg-only 表 | 返回 TableNotFound |
| `test_cross_format_exists_isolation` | 集成 | Lance exists Iceberg-only 表 | 返回 exists=false |
| `test_cross_format_commit_isolation` | 集成 | Iceberg commit Lance-only 表 | 返回 NoSuchTableException |
| `test_standard_protocol_errors_do_not_use_unified_problem_details` | 集成 | 标准协议错误响应 | 不使用 Unified Problem Details 格式 |

### Unified Namespace 端点 (`tests/unified_namespace.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_create_namespace` | 集成 | POST 创建 Namespace | comment/properties/id 正确返回 |
| `test_list_namespaces` | 集成 | 列出 Namespace | 返回列表与 next_page_token |
| `test_list_namespaces_pagination` | 集成 | pageSize/pageToken 分页 | 分页 token 生效 |
| `test_list_namespaces_page_size_too_large` | 集成 | pageSize 超上限 | 400 PageSizeTooLarge |
| `test_list_namespaces_page_size_zero` | 集成 | pageSize=0 | 400 InvalidInput |
| `test_get_namespace` | 集成 | GET Namespace 详情 | comment 可读回 |
| `test_get_namespace_not_found` | 集成 | GET 不存在 Namespace | 404 NamespaceNotFound |
| `test_delete_namespace` | 集成 | 删除空 Namespace | 204 No Content |
| `test_delete_non_empty_namespace` | 集成 | 删除非空 Namespace | 409 NamespaceNotEmpty |
| `test_duplicate_create_returns_409` | 集成 | 重复创建 Namespace | 409 NamespaceAlreadyExists |
| `test_create_namespace_invalid_name` | 集成 | 创建非法名称 Namespace | 400 InvalidInput |
| `test_patch_comment_value` | 集成 | PATCH comment 字符串 | comment 被覆盖 |
| `test_patch_comment_null` | 集成 | PATCH comment=null | comment 被清空 |
| `test_patch_properties` | 集成 | PATCH properties 增量更新 | removals/updates 生效 |
| `test_problem_details_content_type` | 集成 | Unified 错误响应 | Content-Type 为 application/problem+json |
| `test_request_id_propagation` | 集成 | 传入 X-Request-Id | header 与错误体复用请求 ID |
| `test_generated_request_id_is_uuid` | 集成 | 未传 X-Request-Id | 服务端生成 UUID 请求 ID |
| `test_domain_crud_smoke` | 集成 | Domain 完整 CRUD | 创建/获取/更新/删除全链路 |
| `test_drop_non_empty_domain_returns_409` | 集成 | 删除含 Namespace 的 Domain | 409 DomainNotEmpty |

### Unified Asset 端点 (`tests/unified_asset.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_list_assets_cross_format` | 集成 | 跨格式列出 Asset | Iceberg/Lance 均可发现 |
| `test_list_assets_filter_by_format_iceberg` | 集成 | format=iceberg | 只返回 Iceberg Asset |
| `test_list_assets_filter_by_format_lance` | 集成 | format=lance | 只返回 Lance Asset |
| `test_list_assets_filter_by_name` | 集成 | name 精确过滤 | 只返回匹配名称 |
| `test_list_assets_pagination` | 集成 | Asset 分页 | pageSize/pageToken 生效 |
| `test_list_assets_page_size_too_large` | 集成 | pageSize 超上限 | 400 PageSizeTooLarge |
| `test_list_assets_page_size_zero` | 集成 | pageSize=0 | 400 InvalidInput |
| `test_list_assets_empty_format_returns_invalid_format` | 集成 | format= 空字符串 | 400 InvalidFormat |
| `test_list_assets_order_by_name` | 集成 | 按名称排序 | 按 name 稳定排序，非 tabular 资产也包含在内 |
| `test_create_asset_not_allowed` | 集成 | POST /assets | 405 MethodNotAllowed 且不创建记录 |
| `test_get_asset_detail` | 集成 | GET Asset 详情 | 返回 tabular 字段与 current_version |
| `test_get_asset_without_format_returns_200` | 集成 | 单资源无 format 参数 | 返回 200，不再强制要求 format |
| `test_get_asset_ignores_unknown_format_query` | 集成 | 单资源带未知 format | 忽略 format 参数，正常返回 200 |
| `test_get_asset_not_found` | 集成 | GET 不存在 Asset | 404 AssetNotFound |
| `test_delete_asset` | 集成 | DELETE Asset | 204 No Content |
| `test_delete_asset_not_found` | 集成 | DELETE 不存在 Asset | 404 AssetNotFound |
| `test_patch_asset_comment` | 集成 | PATCH comment 字符串 | comment 被覆盖 |
| `test_patch_asset_comment_null` | 集成 | PATCH comment=null | comment 被清空 |
| `test_patch_asset_comment_missing_keeps_existing` | 集成 | PATCH 缺省 comment | 原 comment 保持不变 |
| `test_patch_asset_properties` | 集成 | PATCH properties | removals/updates 生效 |
| `test_rename_asset` | 集成 | rename Asset | 旧名不存在，新名存在 |
| `test_rename_asset_to_existing_name` | 集成 | rename 到冲突名 | 409 AssetAlreadyExists |
| `test_rename_asset_invalid_new_name` | 集成 | rename 到非法名称 | 400 InvalidInput |
| `test_rename_asset_cross_namespace` | 集成 | 同 Domain 跨 Namespace rename | 返回 200，数据正确迁移 |
| `test_non_tabular_asset_list_and_get` | 集成 | 非 tabular 资产 list/get | format/location/current_version 均为 null，rename 正常 |
| `test_list_assets_invalid_format` | 集成 | 列表非法 format | 400 InvalidFormat |
| `test_get_lance_asset_no_version` | 集成 | Lance 无版本 | current_version=null |
| `test_get_lance_asset_with_version` | 集成 | Lance 有版本 | version_id/metadata_location 正确 |
| `test_get_iceberg_asset_no_metadata` | 集成 | Iceberg 无对象存储 metadata | current_version=null |
| `test_get_iceberg_asset_with_metadata` | 集成 | Iceberg metadata 可读 | sequence/snapshot/timestamp 正确 |
| `test_get_iceberg_asset_no_snapshot` | 集成 | Iceberg 无 snapshot | snapshot_id/timestamp_ms 为 null |

---

## quasar-storage

> 位置：`quasar/storage/src/` + `quasar/storage/tests/`
> 测试类型：单元测试 + 集成测试（嵌入式 PostgreSQL）

### SQL 常量校验 (`src/queries.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `domain_constants_are_well_formed` | 单元 | Domain SQL 常量 | 非空且含 `$1` 占位符 |
| `namespace_constants_are_well_formed` | 单元 | Namespace SQL 常量 | 非空且含 `$1` 占位符 |
| `asset_constants_are_well_formed` | 单元 | Asset SQL 常量 | 非空且含 `$1` 占位符 |
| `version_constants_are_well_formed` | 单元 | Version SQL 常量 | 非空且含 `$1` 占位符 |
| `asset_get_uses_active_filter` | 单元 | Asset GET 查询 | 包含 `deleted_at IS NULL` 过滤 |
| `version_latest_excludes_nulls` | 单元 | Version 最新查询 | 包含 `version_order IS NOT NULL` 过滤 |
| `cas_update_uses_optimistic_predicate` | 单元 | CAS UPDATE | 使用 `IS NOT DISTINCT FROM` 和 `RETURNING` |
| `unified_queries_use_left_join` | 单元 | Unified 查询 | `LIST_UNIFIED` 和 `GET_UNIFIED` 使用 `LEFT JOIN` |

### Schema 初始化校验 (`src/schema/mod.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `init_sql_is_non_empty` | 单元 | INIT_SQL 非空 | 加载后不为空 |
| `init_sql_contains_expected_tables` | 单元 | V3 数据核心表 | 包含全部 9 张表声明 |
| `init_sql_contains_expected_triggers` | 单元 | V3 触发器 | 包含全部 2 个触发器 |
| `init_sql_contains_expected_indexes` | 单元 | V3 索引 | 包含全部 4 个索引 |
| `init_sql_seeds_registries` | 单元 | 注册表 seed | asset_types 和 tabular_formats 已 seed |
| `init_sql_seeds_default_domain` | 单元 | 默认 Domain seed | 名称为 `"default"` |
| `init_sql_uses_partial_unique_for_active_assets` | 单元 | active 资产唯一索引 | 使用 `WHERE deleted_at IS NULL` 部分唯一索引 |

### CatalogStore 集成 (`tests/integration.rs`)

| 测试函数 | 类型 | 场景 | 验证点 |
|---------|------|------|--------|
| `test_namespace_crud` | 集成 | Namespace 完整 CRUD | 创建/列出/获取/存在检查/删除全链路 |
| `test_asset_crud` | 集成 | Asset 完整 CRUD | 创建/列出/获取/存在检查/重命名/删除全链路 |
| `test_rename_asset_cross_namespace` | 集成 | 同 Domain 跨 Namespace rename | 成功迁移，目标 namespace 正确 |
| `test_version_commit_and_load` | 集成 | 版本提交与查询 | 顺序提交 v1→v2，load_version/load_current/list_versions 正确 |
| `test_version_conflict` | 集成 | CAS 乐观锁冲突 | previous_version_id 不匹配返回 Conflict |
| `test_previous_version_must_belong_to_same_asset` | 集成 | previous_version_id 跨 Asset 使用 | 返回 Conflict，防止版本链串表 |
| `test_drop_asset_cascades_tabular_and_versions` | 集成 | 删除含版本 Asset | assets/tabular_assets/asset_versions/tabular_asset_versions 全部级联清理 |
| `test_not_found_errors` | 集成 | NotFound 场景 | namespace 不存在、asset 不存在、无版本记录 |
| `test_update_namespace_properties` | 集成 | Namespace properties 更新 | removals/updates 生效，缺失 Namespace 返回 NotFound |
| `test_domain_crud_complete_flow` | 集成 | Domain 完整 CRUD | 创建/获取/更新/删除，名称全局唯一 |
| `test_namespace_same_name_across_domains` | 集成 | 不同 Domain 同名 Namespace | 互不干扰，各自独立 |
| `test_non_empty_domain_delete_returns_conflict` | 集成 | 删除含 Namespace 的 Domain | 返回 Conflict，DomainNotEmpty |
| `test_asset_active_uniqueness_within_namespace` | 集成 | active-name 唯一性 | 同一 namespace 下活动资产名唯一，删除后可复用 |
| `test_get_latest_version_uses_version_order` | 集成 | 最新版本查询 | 按 version_order 而非 version_key 排序 |
| `test_unified_query_pairs_asset_with_tabular` | 集成 | Unified 查询返回形态 | `(Asset, Option<TabularAsset>)` 非 tabular 时第二个为 None |
| `test_cas_commit_optimistic_concurrency` | 集成 | CAS Commit 乐观并发 | expected metadata_location 不匹配时拒绝更新 |

---

## 修订记录

### V5.0（2026-05-13）

- 更新：测试统计总览表（126 单元 + 168 集成 = 294 合计），同步 Phase 4 实际数量
- 新增：quasar-core StoreError 扩展变体测试矩阵（11 个单元测试，新增 namespace_not_empty / domain_not_empty / database_unavailable / timeout / internal 结构体变体）
- 新增：quasar-core Domain 模型测试矩阵（2 个单元测试）
- 新增：Lance ID 解析器单元测试矩阵（12 个单元测试，V3 新增）
- 新增：Unified 错误映射单元测试矩阵（2 个单元测试，internal 脱敏 + domain_not_empty）
- 新增：quasar-storage 单元测试矩阵（15 个单元测试，SQL 常量 well-formed + schema init 校验）
- 新增：quasar-server e2e 测试矩阵（6 个集成测试，Iceberg/Lance 全生命周期 + Domain 隔离 + 跨格式冲突 + 非空 Domain 保护）
- 新增：Lance Namespace 端点 id parser 集成测试（3 个测试，root list / single segment / non-default domain）
- 新增：Lance Table 端点跨 Namespace rename 测试（2 个：cross_namespace happy path + cross_domain rejected）
- 新增：Iceberg Table 端点跨 Namespace rename 测试（1 个，行为从 400 改为支持跨 ns）
- 新增：Unified Asset 端点非 tabular 资产测试（1 个）
- 新增：Unified Asset 端点跨 Namespace rename 测试（1 个）
- 新增：Unified Namespace 端点 Domain CRUD 测试（2 个）
- 新增：Storage 集成测试矩阵扩展（8 个新增：cross-ns rename / domain CRUD / namespace isolation / active uniqueness / version_order / unified query / CAS commit）
- 修正：Unified Asset `test_list_assets_order_by_name_then_format` → `test_list_assets_order_by_name`
- 修正：Unified Asset 删除 `test_get_asset_missing_format`、`test_get_asset_invalid_format`（V3 不再强制 format）
- 修正：Unified Asset 新增 `test_get_asset_without_format_returns_200`、`test_get_asset_ignores_unknown_format_query`
- 修正：Lance error `test_store_error_to_lance_version_parses_version_number` / `test_store_error_to_lance_version_non_version_message` → `test_store_error_to_lance_version_already_exists_maps_to_table_exists`
- 修正：跨格式隔离测试矩阵更新（V3 端点级隔离后行为变更：list/drop/rename isolation 改为 create conflicts / lance drop/rename cannot target iceberg）
- 修正：运行命令更新为 `cargo test --workspace --all-features --all-targets`

### V4.0（2026-05-07）

- 更新：测试统计总览表（90 单元 + 144 集成 = 234 合计）
- 补齐：quasar-core 模型结构体与名称校验测试矩阵（27 个单元测试）
- 新增：Lance Namespace 端点测试矩阵恢复为 7 个集成测试
- 补齐：Iceberg 错误映射测试矩阵（11 个单元测试）
- 补齐：Iceberg Object Store 测试矩阵（5 个集成测试）
- 新增：跨格式隔离测试矩阵（8 个集成测试）
- 新增：Unified Namespace 端点测试矩阵（17 个集成测试）
- 新增：Unified Asset 端点测试矩阵（29 个集成测试）
- 新增：server 无状态烟雾测试矩阵（1 个集成测试）
- 更新：storage 集成测试矩阵补齐到 8 个测试

### V3.0（2026-04-22）

- 更新：测试统计总览表（61 单元 + 69 集成 = 130 合计）
- 新增：Iceberg Commit 端点测试矩阵（15 个集成测试）
- 新增：Iceberg TableMetadata 测试矩阵（17 个单元测试）
  - 新增：snapshot_id=null 验证测试（ok/fail）
  - 新增：自定义 ref（staging）验证测试
  - 新增：多 requirements 组合测试
  - 新增：多 updates 组合测试

### V2.0（2026-04-21）

- 更新：测试统计总览表（56 单元 + 61 集成 = 117 合计）
- 新增：Iceberg 错误映射测试矩阵（10 个单元测试）
- 新增：Iceberg DTO 序列化测试矩阵（11 个单元测试）
- 新增：Iceberg Namespace 端点测试矩阵（14 个集成测试）
- 新增：Iceberg Table 端点测试矩阵（13 个集成测试）
- 新增：Iceberg Config 端点测试矩阵（1 个集成测试）
- 新增：server/tests/health.rs 的 Iceberg 路由挂载测试
- 新增：storage/tests/integration.rs 的 update_namespace_properties 测试

### V1.0（2026-04-20）

- 新增：quasar-core 测试矩阵（13 个单元测试）
- 新增：quasar-server 测试矩阵（10 个测试）
- 新增：quasar-adapter 测试矩阵（48 个测试）
- 新增：quasar-storage 测试矩阵（5 个测试）
- 新增：测试统计总览表

---

**后续修订规则：** 新增/修改/删除测试函数时，同步更新本文档对应条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整）。
