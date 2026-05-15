# Quasar V3 Iceberg REST API 测试覆盖改进点

> **版本**: V1.1
> **日期**: 2026-05-14
> **状态**: 部分完成（见修订记录）
>
> 本文档记录V3 Iceberg REST API端点测试覆盖的具体改进点，
> 包括问题背景、官方规范要求、当前实现缺陷和测试建议。
>
> **注意**: 本文档 V1.0 中部分描述基于旧代码，V1.1 已修正。

---

## 一、Config端点改进

### 1.1 `?warehouse=` query参数缺失

**官方规范：**
```yaml
# rest-catalog-open-api.yaml
parameters:
  - name: warehouse
    in: query
    required: false
    schema:
      type: string
    description: Warehouse location or identifier to request from the service
```

**官方语义：**
- 当传入`warehouse`参数时，服务端应返回该warehouse特定的配置
- 如果warehouse不存在，应返回404 `NoSuchWarehouseError`

**当前实现：**
位置：`quasar/adapter/src/iceberg/mod.rs`

`get_config` 已添加 `Query<ConfigQuery>` 提取器处理 `warehouse` 参数。传入的 `warehouse` 值被放入 `overrides` 返回。V3 暂无多 warehouse 验证机制（单 warehouse_path 配置），故不返回 404。

**测试状态：已完成**
- `test_get_config_with_warehouse` — 验证 overrides 包含传入的 warehouse 值
- `test_get_config_endpoints_field` — 验证 endpoints 列表存在且非空

**遗留：**
- 多 warehouse 场景下的 `NoSuchWarehouseException`（需 store 层支持 warehouse 查询）

### 1.2 `endpoints`字段缺失

**官方规范：**
config响应应包含`endpoints`字段，声明服务支持的端点列表：

```json
{
  "defaults": {...},
  "overrides": {...},
  "endpoints": [
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    ...
  ]
}
```

**官方语义：**
- 客户端通过此字段判断服务端能力
- 若服务端不发送此字段，客户端使用默认端点列表
- 这是Iceberg Java客户端兼容性检查的关键字段

**当前实现：**
`ConfigResponse` 已添加 `endpoints: Vec<String>` 字段，包含所有支持的 Iceberg REST 端点列表。

**测试状态：已完成**
- `test_get_config_endpoints_field` — 验证 endpoints 包含关键端点

**遗留：**
- 客户端兼容性验证（需等实际客户端集成测试）

---

## 二、Namespace端点改进

### 2.1 `parent`参数未实现

**官方规范：**
```yaml
parameters:
  - name: parent
    in: query
    description: An optional namespace, underneath which to list namespaces.
```

**官方语义：**
```
GET /v1/{prefix}/namespaces?parent=accounting
→ 返回 ["accounting.tax"]

GET /v1/{prefix}/namespaces?parent=accounting%1Ftax
→ 返回 ["accounting.tax.paid"]

若parent不存在，返回404 NoSuchNamespaceError
```

**当前实现：**
位置：`quasar/adapter/src/iceberg/namespace.rs`

V3 设计文档明确不实现嵌套 Namespace（单层 Namespace，后续单独设计）。`ListNamespacesQuery` 已添加 `parent` 字段，`list_namespaces` 检查 `parent` 若提供则返回 `400 BadRequestException`，明确告知客户端"Nested namespace is not supported in V3"。

**设计立场（已确认）：**
- V3 不实现嵌套 Namespace（V3_DESIGN.md §1.3）
- 传入 `parent` 参数返回 400，而非静默忽略或 501

**测试状态：已完成**
- `test_list_namespaces_with_parent_rejected` — 验证传入 parent 返回 400

### 2.2 pageSize边界值缺失

**代码逻辑：**
```rust
let limit = query.page_size.unwrap_or(100).clamp(1, 1000);
```

**设计立场（已确认）：**
保持 `clamp(1, 1000)` 行为。0 和负数被 clamp 到 1，>1000 被 clamp 到 1000。分页常见做法，无需返回 400。

**测试状态：已完成**
- `test_list_namespaces_page_size_one` — 验证返回 1 个且含 nextPageToken
- `test_list_namespaces_page_size_negative` — 验证 -1 被 clamp 到 1
- `test_list_namespaces_page_size_exceeds_max` — 验证 5000 被 clamp 到 1000

### 2.3 HEAD namespace错误体验证

**问题背景：**
Iceberg规范要求HEAD 404响应也应返回IcebergErrorResponse JSON body。

**官方规范：**
```yaml
head:
  responses:
    404:
      description: Not Found - Namespace not found
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/IcebergErrorResponse'
```

**当前测试：**
```rust
#[tokio::test]
async fn test_namespace_exists_not_found() {
    let response = app.oneshot(
        Request::builder()
            .method("HEAD")
            .uri("/iceberg/v1/default/namespaces/nonexistent")
    );
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    // 未验证response body内容！
}
```

**问题：**
HTTP HEAD请求通常不返回body，但Iceberg规范要求404时返回错误JSON。当前测试未验证这一点。

---

## 三、Table端点改进

### 3.1 schema字段验证

**官方规范：**
创建表时可传入自定义schema。

**当前实现：**
位置：`quasar/adapter/src/iceberg/table.rs`

`build_initial_metadata` 已正确计算 `last_column_id`（从 schema fields 的 `id` 字段取最大值）。

**遗留问题：**
- field id 自动分配（当未提供时）未实现
- nested type（struct 嵌套）的 last-column-id 计算不完整

**测试状态：已完成**
- `test_create_table_with_schema` — 验证自定义 schema 和 last-column-id 计算

### 3.2 list tables分页

**官方规范：**
```yaml
parameters:
  - $ref: '#/components/parameters/page-token'
  - $ref: '#/components/parameters/page-size'
```

**当前实现：**
位置：`quasar/adapter/src/iceberg/table.rs`

`list_tables` 已实现分页逻辑（参照 `list_namespaces` 模式）。store trait `list_tabular_assets` 已添加 `offset`/`limit` 参数，storage 层 SQL 已添加 `LIMIT $4 OFFSET $5`。

**测试状态：已完成**
- `test_list_tables_pagination` — 验证 pageSize + pageToken 分页

### 3.3 格式隔离验证

**测试状态：已完成（文档V1.0描述不准确）**

`format_isolation.rs` 已有 8 个测试，覆盖：
- `test_cross_format_create_conflicts` — 同名跨格式创建冲突（409）
- `test_cross_format_load_isolation` — Iceberg 端点访问 Lance 表返回 404
- `test_cross_format_describe_isolation` — describe 隔离
- `test_cross_format_drop_isolation` — drop 隔离
- `test_cross_format_rename_isolation` — rename 隔离
- `test_cross_format_exists_isolation` — exists 隔离
- `test_cross_format_commit_isolation` — commit 隔离
- `test_standard_protocol_errors_do_not_use_unified_problem_details` — 错误格式隔离

### 3.4 `purgeRequested`参数

**官方规范：**
```yaml
DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}
parameters:
  - name: purgeRequested
    in: query
    description: Whether to purge data files when dropping the table
```

**当前实现：**
`drop_table` 已添加 `Query<DropTableQuery>` 提取器。`purgeRequested=true` 返回 `501 NotImplementedException`（V3 设计：先只做 catalog drop）。

**测试状态：已完成**
- `test_drop_table_with_purge_requested` — 验证 501
- `test_drop_table_without_purge_succeeds` — 验证 purgeRequested=false 正常删除

---

## 四、Commit端点改进

### 4.1 Update Action类型

**官方规范支持的Action：**

| Action | 实现状态 | 测试状态 |
|--------|---------|---------|
| `add-snapshot` | ✓ 已实现 | ✓ 已测试 |
| `set-snapshot-ref` | ✓ 已实现 | ✓ 已测试 |
| `set-properties` | ✓ 已实现 | ✓ 已测试 |
| `remove-properties` | ✓ 已实现 | ✓ 已测试 |
| `add-schema` | ✓ 已实现 | ✓ 已测试（V1.1 补充） |
| `set-current-schema-id` | ✓ 已实现 | ✓ 已测试（V1.1 补充） |
| `add-partition-spec` | ✓ 已实现 | ✓ 已测试（V1.1 补充） |
| `set-default-spec-id` | ✓ 已实现 | ✗ 无独立测试 |
| `add-sort-order` | ✓ 已实现 | ✗ 无独立测试 |
| `set-default-sort-order-id` | ✓ 已实现 | ✗ 无独立测试 |
| `upgrade-format-version` | ✗ 未实现 | — |
| `set-location` | ✗ 未实现 | — |
| `remove-snapshot-ref` | ✗ 未实现 | — |
| `remove-snapshots` | ✗ 未实现 | — |
| `remove-partition-specs` | ✗ 未实现 | — |
| `remove-sort-orders` | ✗ 未实现 | — |

**代码实现：**
位置：`quasar/adapter/src/iceberg/table_metadata.rs`

使用强类型 `TableUpdate` 枚举实现 10/16 个 action：
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    AddSnapshot { snapshot: Snapshot },
    SetSnapshotRef { ref_name: String, snapshot_id: i64, r#type: Option<String> },
    SetProperties { updates: HashMap<String, String> },
    RemoveProperties { removals: Vec<String> },
    AddSchema { schema: Schema },
    SetCurrentSchema { schema_id: i32 },
    AddPartitionSpec { spec: PartitionSpec },
    SetDefaultSpec { spec_id: i32 },
    AddSortOrder { order: SortOrder },
    SetDefaultSortOrder { order_id: i32 },
}
```

> **文档修正（V1.0 → V1.1）**：V1.0 描述基于旧代码（使用 `serde_json::Value` + 字符串匹配 + `_ => {}` 静默忽略）。当前代码使用强类型枚举，实现了 10 个 action，未知 action 会通过反序列化失败报错而非静默忽略。

### 4.2 Requirement类型

**官方规范支持的Requirement：**

| Requirement | 实现状态 | 测试状态 |
|-------------|---------|---------|
| `assert-ref-snapshot-id` | ✓ 已实现 | ✓ 已测试 |
| `assert-table-uuid` | ✓ 已实现 | ✓ 已测试 |
| `assert-create` | ✓ 已实现 | ✓ 已测试（V1.1 补充） |
| `assert-current-schema-id` | ✗ 未实现 | — |
| `assert-default-spec-id` | ✗ 未实现 | — |
| `assert-default-sort-order-id` | ✗ 未实现 | — |
| `assert-last-sequence-number` | ✗ 未实现 | — |

**代码实现：**
位置：`quasar/adapter/src/iceberg/table_metadata.rs`

使用强类型 `TableRequirement` 枚举实现 3/7 个 requirement：
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate,
    AssertTableUuid { uuid: String },
    AssertRefSnapshotId { r#ref: String, snapshot_id: Option<i64> },
}
```

> **文档修正（V1.0 → V1.1）**：V1.0 错误标记 `assert-create` 为未实现。实际已实现。

**遗留问题：**
- `assert-create` 在 staged create 场景中的使用受限：当前 `commit_table` handler 先调用 `get_tabular_asset`，表不存在即返回 404，无法到达 requirement 检查。 staged create 流程需要单独设计。

---

## 五、Object Store集成改进

### 5.1 并发写入冲突

**分析：**
DB 层 CAS（`cas_update_metadata_location`）已保证数据一致性：只有一个 commit 能成功更新 `metadata_location`。失败的 commit 返回 `CommitFailedException`（409）。

object store 层确实无并发控制，但文件名包含 table UUID（跨 commit 不变）+ 序列号。即使两个并发 commit 产生相同序列号，文件名中的 UUID 确保不覆盖（DB CAS 保证只有一个成功）。

**风险：** 失败的 commit 可能在 object store 留下 orphan metadata 文件。

**测试状态：已有覆盖**
- `test_concurrent_cas_conflict_end_to_end`（`iceberg_commit.rs`）— 验证第二个 commit 因 requirement 不匹配返回 409

**遗留：** 真正的并行并发测试（`tokio::join!` 同时发起两个 commit）在当前测试架构下难以稳定实现（PostgreSQL embedded + serial_test）。

### 5.2 metadata版本溢出处理

**代码实现：**
位置：`quasar/adapter/src/iceberg/table.rs`

`next_metadata_location` 已实现 width 动态扩展（如 99999 → 100000，width 从 5 扩展到 6）。

**测试状态：已完成**
- `test_next_metadata_location_basic_increment` — 00001 → 00002
- `test_next_metadata_location_width_overflow_9_to_10` — 00009 → 00010
- `test_next_metadata_location_width_overflow_99_to_100` — 00099 → 00100
- `test_next_metadata_location_width_overflow_99999_to_100000` — 99999 → 100000
- `test_next_metadata_location_unrecognized_format` — 异常格式 fallback

---

## 六、跨端点场景改进

### 6.1 请求头验证

**实现状态：**
Axum 的 `Json` 提取器默认已处理：
- 缺少 Content-Type → 415 Unsupported Media Type
- 错误 Content-Type → 415
- 无效 JSON body → 400 Bad Request（Axum JsonRejection）
- 空 body → 400

**测试状态：已完成**
- `test_create_namespace_missing_content_type` — 415
- `test_create_namespace_wrong_content_type` — 415
- `test_create_namespace_invalid_json` — 400（Axum 返回纯文本 rejection，非 IcebergErrorResponse）
- `test_create_namespace_empty_body` — 400

**备注：**
无效 JSON 的 400 响应 body 是 Axum 的 `JsonRejection` 纯文本消息，不是 `IcebergErrorResponse` JSON。这是 Axum 的默认行为，若要统一为 Iceberg 错误格式，需自定义 rejection handler。

### 6.2 Domain隔离验证

**测试状态：已完成**
- `test_namespace_domain_isolation` — 验证 Domain A 的 namespace 对 Domain B 不可见

**遗留：**
- 跨 Domain table 访问隔离（与 namespace 隔离原理相同，store 层按 domain_name 过滤）
- 跨 Domain rename（当前实现中 rename 只在同一 Domain 内进行）

---

## 七、改进优先级（V1.1 更新）

### 已完成 ✅

| 改进项 | 完成说明 |
|-------|---------|
| `parent`参数处理 | 返回 400，明确拒绝嵌套 namespace |
| schema字段验证测试 | `test_create_table_with_schema` |
| list tables分页 | store trait + storage SQL + handler 实现 |
| 格式隔离验证 | 已有 8 个测试，文档 V1.0 描述不准确 |
| `purgeRequested` | 返回 501 NotImplemented |
| `warehouse`参数 | 放入 overrides 返回 |
| `endpoints`字段 | ConfigResponse 已添加 |
| pageSize边界值 | 4 个测试覆盖 |
| 请求头验证 | 4 个测试覆盖 |
| Domain隔离 | `test_namespace_domain_isolation` |
| metadata版本溢出 | 5 个单元测试覆盖 |
| `assert-create` requirement | 已有实现 + 测试补充 |
| `add-schema` action | 已有实现 + 测试补充 |
| `set-current-schema` action | 已有实现 + 测试补充 |
| `add-partition-spec` action | 已有实现 + 测试补充 |
| Commit CAS冲突 | `test_concurrent_cas_conflict_end_to_end` |

### 仍需实现（功能缺失）

| 改进项 | 原因 | 优先级 |
|-------|------|--------|
| `upgrade-format-version` action | 表演进关键操作 | P1 |
| `set-location` action | 更改表 location | P2 |
| `remove-snapshot-ref` action | 删除分支/标签 | P2 |
| `remove-snapshots` action | 清理旧快照 | P2 |
| `remove-partition-specs` action | 清理分区规范 | P2 |
| `remove-sort-orders` action | 清理排序规则 | P2 |
| `assert-current-schema-id` requirement | schema 变更前置验证 | P2 |
| `assert-default-spec-id` requirement | 分区变更前置验证 | P2 |
| `assert-default-sort-order-id` requirement | 排序变更前置验证 | P2 |
| `assert-last-sequence-number` requirement | 序列号验证 | P2 |

### 仍需测试（功能已有，测试缺失）

| 改进项 | 原因 | 优先级 |
|-------|------|--------|
| `set-default-spec-id` action | 功能已有，无独立测试 | P2 |
| `add-sort-order` action | 功能已有，无独立测试 | P2 |
| `set-default-sort-order-id` action | 功能已有，无独立测试 | P2 |
| HEAD 404 body 验证 | HTTP 语义冲突（HEAD 不应有 body），保持现状 | P3 |
| 真正并行并发 commit | 测试架构限制（PostgreSQL embedded + serial_test） | P3 |

---

## 八、修订记录

### V1.1 (2026-05-14)

- 修正 4.1/4.2 基于旧代码的描述：update actions（10/16 已实现）和 requirements（3/7 已实现）
- 修正 3.3 格式隔离描述：已有 8 个测试覆盖
- 标记所有已修复的问题为"已完成"
- 按"已完成 / 仍需实现 / 仍需测试"重新分类优先级
- 更新 1.1/1.2/2.1/2.2/3.1/3.2/3.4/5.1/5.2/6.1/6.2 的实现和测试状态

### V1.0 (2026-05-14)

- 初始版本，记录Iceberg REST API测试覆盖改进点
- 涵盖Config/Namespace/Table/Commit/Object Store六大模块
- 按P0-P3四级优先级分类