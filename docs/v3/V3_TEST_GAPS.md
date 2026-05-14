# Quasar V3 Iceberg REST API 测试覆盖改进点

> **版本**: V1.0
> **日期**: 2026-05-14
> **状态**: 待改进
>
> 本文档记录V3 Iceberg REST API端点测试覆盖的具体改进点，
> 包括问题背景、官方规范要求、当前实现缺陷和测试建议。

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

**当前实现问题：**
位置：`quasar/adapter/src/iceberg/mod.rs:70`

```rust
pub async fn get_config(
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    // 未处理warehouse参数
    let mut defaults = HashMap::new();
    let overrides = HashMap::new();
    // warehouse参数完全被忽略
}
```

**影响：**
- 多warehouse场景下客户端无法获取正确配置
- 无法验证warehouse是否存在
- 与Iceberg官方规范不符

**测试建议：**
```rust
// 测试1: 传入warehouse参数应返回对应配置
#[tokio::test]
async fn test_get_config_with_warehouse() {
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/config?warehouse=s3://bucket/prod")
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json["overrides"]["warehouse"], "s3://bucket/prod");
}

// 测试2: 传入不存在的warehouse应返回404
#[tokio::test]
async fn test_get_config_warehouse_not_found() {
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/config?warehouse=s3://nonexistent")
    );
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json["error"]["type"], "NoSuchWarehouseException");
}
```

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

**当前实现问题：**
`ConfigResponse`结构体缺少`endpoints`字段：

```rust
#[derive(Serialize)]
pub struct ConfigResponse {
    pub defaults: HashMap<String, String>,
    pub overrides: HashMap<String, String>,
    // 缺少: pub endpoints: Vec<String>,
}
```

**影响：**
- 客户端无法动态发现端点支持情况
- 某些客户端可能拒绝连接不支持特定端点的服务

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

**当前实现问题：**
位置：`quasar/adapter/src/iceberg/namespace.rs:18`

```rust
pub async fn list_namespaces(
    Query(query): Query<ListNamespacesQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    // 只处理了page_size和page_token
    let limit = query.page_size.unwrap_or(100);
    let offset = query.page_token...
    // parent参数未被处理！
}
```

DTO定义也缺少该字段：

```rust
// quasar/adapter/src/iceberg/dto.rs
#[derive(Debug, Deserialize)]
pub struct ListNamespacesQuery {
    pub page_size: Option<i32>,
    pub page_token: Option<String>,
    // 缺少: pub parent: Option<String>,
}
```

**影响：**
- 不支持嵌套Namespace场景
- 与Iceberg官方规范严重不符
- V3设计文档明确要求单层Namespace，但parent参数是官方规范必需

**设计决策待定：**
- V3是否需要支持嵌套Namespace？
- 若不支持，应明确返回409/501告知客户端

**测试建议：**
```rust
#[tokio::test]
async fn test_list_namespaces_with_parent() {
    // 创建嵌套namespace: analytics, analytics.events
    create_nested_namespace(&store, "analytics.events");

    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces?parent=analytics")
    );
    // 应返回子namespace
    assert!(json["namespaces"].contains(&["analytics", "events"]));
}

#[tokio::test]
async fn test_list_namespaces_parent_not_found() {
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces?parent=nonexistent")
    );
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json["error"]["type"], "NoSuchNamespaceException");
}
```

### 2.2 pageSize边界值缺失

**问题背景：**
当前只测试了`pageSize=2`和`pageSize=100`默认值，缺少边界场景。

**代码逻辑：**
```rust
let limit = query.page_size.unwrap_or(100).clamp(1, 1000);
```

**缺失测试：**
- `pageSize=1`：最小值，应触发next_page_token
- `pageSize=1000`：上限值
- `pageSize=0`或负数：应返回400（当前被clamp到1）
- `pageSize>1000`：应被clamp到1000

**测试建议：**
```rust
#[tokio::test]
async fn test_list_namespaces_page_size_one() {
    create_namespace(&store, "ns1");
    create_namespace(&store, "ns2");

    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces?pageSize=1")
    );
    assert_eq!(json["namespaces"].len(), 1);
    assert!(json["nextPageToken"].is_string()); // 应有下一页
}

#[tokio::test]
async fn test_list_namespaces_page_size_negative() {
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces?pageSize=-1")
    );
    // 当前被clamp到1，是否应返回400？
    // 需明确：负值是clamp还是报错
}

#[tokio::test]
async fn test_list_namespaces_page_size_exceeds_max() {
    for i in 0..100 { create_namespace(&store, format!("ns{}", i)); }

    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces?pageSize=5000")
    );
    // 应被clamp到1000
    assert!(json["namespaces"].len() <= 1000);
}
```

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

### 3.1 schema字段验证缺失

**官方规范：**
创建表时可传入自定义schema：

```json
{
  "name": "users",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "type": "int", "required": true},
      {"id": 2, "name": "name", "type": "string"}
    ]
  }
}
```

**当前实现：**
位置：`quasar/adapter/src/iceberg/table.rs:122`

```rust
fn build_initial_metadata(...) -> serde_json::Value {
    let schema = schema.cloned().unwrap_or_else(|| {
        json!({"type": "struct", "schema-id": 0, "fields": []}) // 默认空schema
    });

    // 计算last_column_id
    let last_column_id = schema.get("fields")
        .and_then(|f| f.as_array())
        .map(|fields| fields.iter()
            .filter_map(|f| f.get("id").and_then(|id| id.as_i64()))
            .max().unwrap_or(0) as i32)
        .unwrap_or(0);
}
```

**缺失测试：**
- 自定义schema是否正确写入metadata
- `last-column-id`是否正确计算
- field id自动分配（当未提供时）
- nested type（struct嵌套）的处理

**测试建议：**
```rust
#[tokio::test]
async fn test_create_table_with_schema() {
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/iceberg/v1/default/namespaces/prod/tables")
            .body(Body::from(r#"{
                "name": "users",
                "schema": {
                    "type": "struct",
                    "fields": [
                        {"id": 1, "name": "id", "type": "int"},
                        {"id": 2, "name": "name", "type": "string"}
                    ]
                }
            }"#))
    );

    assert_eq!(json["metadata"]["schemas"][0]["fields"].len(), 2);
    assert_eq!(json["metadata"]["last-column-id"], 2); // 最大field id
}

#[tokio::test]
async fn test_create_table_schema_nested_type() {
    // 嵌套类型：struct包含struct
    let response = app.oneshot(
        .body(Body::from(r#"{
            "name": "events",
            "schema": {
                "fields": [{
                    "name": "user",
                    "type": {
                        "type": "struct",
                        "fields": [{"name": "id"}, {"name": "name"}]
                    }
                }]
            }
        }"#))
    );
    // 验证nested schema正确处理
}

#[tokio::test]
async fn test_create_table_schema_without_field_ids() {
    // 不提供field id时，应自动分配
    let response = app.oneshot(
        .body(Body::from(r#"{
            "name": "users",
            "schema": {"fields": [{"name": "id"}, {"name": "name"}]}
        }"#))
    );
    // Iceberg规范：未提供id时服务端应自动分配递增id
    // 当前实现last_column_id=0，可能不符合规范
}
```

### 3.2 list tables分页缺失

**官方规范：**
```yaml
parameters:
  - $ref: '#/components/parameters/page-token'
  - $ref: '#/components/parameters/page-size'
```

**当前实现：**
位置：`quasar/adapter/src/iceberg/table.rs:174`

```rust
pub async fn list_tables(
    Query(query): Query<ListTablesQuery>,
) -> Result<impl IntoResponse, IcebergError> {
    let _ = query; // pagination placeholder for MVP
    // 实际未处理分页参数！
}
```

**影响：**
- 大量表场景下性能问题
- 与官方规范不符
- Namespace已支持分页，Table未实现

**测试建议：**
```rust
#[tokio::test]
async fn test_list_tables_pagination() {
    // 创建50个表
    for i in 0..50 {
        create_table(&store, &format!("table{}", i));
    }

    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces/prod/tables?pageSize=10")
    );
    assert_eq!(json["identifiers"].len(), 10);
    assert!(json["nextPageToken"].is_string());

    // 获取第二页
    let token = json["nextPageToken"].as_str().unwrap();
    let page2 = app.oneshot(
        .uri(format!("/iceberg/v1/.../tables?pageToken={}", token))
    );
    assert_eq!(page2_json["identifiers"].len(), 10);
}

#[tokio::test]
async fn test_list_tables_page_size_boundary() {
    // pageSize=1000上限测试
}
```

### 3.3 格式隔离验证不足

**设计要求：**
位置：`docs/v3/V3_REQUIREMENTS.md:39`

```
端点级隔离：Domain不绑定格式，不同格式通过不同REST端点区分
代价：每个协议端点内部需要按format字段过滤，
      用错端点会返回TableNotFoundException
```

**当前实现：**
位置：`quasar/adapter/src/iceberg/table.rs:293`

```rust
pub async fn drop_table(...) {
    // V3 endpoint-level isolation: only Iceberg-format assets are visible
    store.get_tabular_asset(&prefix, &ns, "iceberg", &table).await...
}
```

**测试缺失：**
`format_isolation.rs`存在但缺少：
- Iceberg端点尝试访问Lance表应返回404
- Iceberg端点创建表后，Lance端点不应看到
- 同名资产在两个格式端点的隔离验证

**测试建议：**
```rust
#[tokio::test]
async fn test_iceberg_endpoint_cannot_see_lance_table() {
    // 通过Lance端点创建表
    lance_create_table(&store, "embeddings");

    // Iceberg端点查询应返回404
    let response = iceberg_app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/default/namespaces/prod/tables/embeddings")
    );
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json["error"]["type"], "NoSuchTableException");
}

#[tokio::test]
async fn test_same_name_asset_format_isolation() {
    // Iceberg端点创建表 "events"
    iceberg_create_table(&store, "events");

    // Lance端点也应能创建同名表（格式隔离）
    lance_create_table(&store, "events");

    // 验证两边都能正确访问各自的表
    let iceberg_resp = iceberg_load_table("events");
    let lance_resp = lance_load_table("events");

    // 两个应该是不同的资产
    assert_ne!(iceberg_resp["metadata-location"], lance_resp["version"]);
}
```

### 3.4 `purgeRequested`参数缺失

**官方规范：**
```yaml
DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}
parameters:
  - name: purgeRequested
    in: query
    description: Whether to purge data files when dropping the table
```

**当前实现：**
```rust
pub async fn drop_table(...) {
    // 未处理purgeRequested参数
    store.drop_asset(&prefix, &ns, &table).await
}
```

**影响：**
- 无法控制是否删除数据文件
- 仅删除catalog记录，数据文件保留

---

## 四、Commit端点改进

### 4.1 缺失的Update Action类型

**官方规范支持的Action：**

| Action | 当前实现 | 说明 |
|--------|---------|------|
| `add-snapshot` | ✓ 已实现 | 添加快照 |
| `set-snapshot-ref` | ✓ 已实现 | 设置分支/标签引用 |
| `set-properties` | ✓ 已实现 | 设置表属性 |
| `remove-properties` | ✓ 已实现 | 删除表属性 |
| `upgrade-format-version` | ✗ 未实现 | 格式版本升级 |
| `add-schema` | ✗ 未实现 | 添加新schema |
| `set-current-schema-id` | ✗ 未实现 | 切换当前schema |
| `add-partition-spec` | ✗ 未实现 | 添加分区规范 |
| `set-default-spec-id` | ✗ 未实现 | 设置默认分区 |
| `add-sort-order` | ✗ 未实现 | 添加排序规则 |
| `set-default-sort-order-id` | ✗ 未实现 | 设置默认排序 |
| `set-location` | ✗ 未实现 | 更改表location |
| `remove-snapshot-ref` | ✗ 未实现 | 删除分支/标签 |
| `remove-snapshots` | ✗ 未实现 | 清理旧快照 |
| `remove-partition-specs` | ✗ 未实现 | 清理分区规范 |
| `remove-sort-orders` | ✗ 未实现 | 清理排序规则 |

**当前实现：**
位置：`quasar/adapter/src/iceberg/table_metadata.rs`

```rust
pub fn apply_updates(&mut self, updates: &[serde_json::Value]) {
    for update in updates {
        let action = update.get("action").and_then(|a| a.as_str());
        match action {
            Some("add-snapshot") => self.apply_add_snapshot(update),
            Some("set-snapshot-ref") => self.apply_set_snapshot_ref(update),
            Some("set-properties") => self.apply_set_properties(update),
            Some("remove-properties") => self.apply_remove_properties(update),
            _ => {} // 其他action被静默忽略！
        }
    }
}
```

**影响：**
- schema变更、分区变更等关键操作无法执行
- 客户端尝试这些操作被静默忽略，导致数据不一致
- 表演进能力受限

**测试建议：**
```rust
#[tokio::test]
async fn test_commit_upgrade_format_version() {
    // 创建v1格式的表
    create_table_with_format_version(&store, 1);

    let response = app.oneshot(
        .body(Body::from(r#"{
            "requirements": [],
            "updates": [{"action": "upgrade-format-version", "format-version": 2}]
        }"#))
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(json["metadata"]["format-version"], 2);
}

#[tokio::test]
async fn test_commit_add_partition_spec() {
    create_table(&store, "events");

    let response = app.oneshot(
        .body(Body::from(r#"{
            "updates": [{
                "action": "add-partition-spec",
                "spec": {
                    "spec-id": 1,
                    "fields": [{"name": "date", "transform": "day", "source-id": 1}]
                }
            }]
        }"#))
    );
    assert_eq!(json["metadata"]["partition-specs"].len(), 2);
    assert_eq!(json["metadata"]["default-spec-id"], 1);
}

#[tokio::test]
async fn test_commit_add_schema() {
    let response = app.oneshot(
        .body(Body::from(r#"{
            "updates": [{
                "action": "add-schema",
                "schema": {
                    "schema-id": 1,
                    "fields": [{"id": 1, "name": "id", "type": "long"}]
                }
            }]
        }"#))
    );
    assert_eq!(json["metadata"]["schemas"].len(), 2);
}

#[tokio::test]
async fn test_commit_set_current_schema_id() {
    // 添加schema后切换当前schema
    let response = app.oneshot(
        .body(Body::from(r#"{
            "updates": [
                {"action": "add-schema", "schema": {...}},
                {"action": "set-current-schema-id", "schema-id": 1}
            ]
        }"#))
    );
    assert_eq!(json["metadata"]["current-schema-id"], 1);
}

#[tokio::test]
async fn test_commit_remove_snapshot_ref() {
    // 创建带tag的表
    create_table_with_tag(&store, "v1.0");

    let response = app.oneshot(
        .body(Body::from(r#"{
            "updates": [{"action": "remove-snapshot-ref", "ref-name": "v1.0"}]
        }"#))
    );
    assert!(json["metadata"]["refs"]["v1.0"].is_null());
}
```

### 4.2 缺失的Requirement类型

**官方规范支持的Requirement：**

| Requirement | 当前实现 | 说明 |
|-------------|---------|------|
| `assert-ref-snapshot-id` | ✓ 已实现 | 验证ref指向的snapshot-id |
| `assert-table-uuid` | ✓ 已实现 | 验证表UUID |
| `assert-create` | ✗ 未实现 | 表必须不存在 |
| `assert-current-schema-id` | ✗ 未实现 | 验证当前schema-id |
| `assert-default-spec-id` | ✗ 未实现 | 验证默认分区spec-id |
| `assert-default-sort-order-id` | ✗ 未实现 | 验证默认排序order-id |
| `assert-last-sequence-number` | ✗ 未实现 | 验证序列号 |

**`assert-create`的重要性：**
这是创建staged表时必须的requirement：

```json
{
  "requirements": [{"type": "assert-create"}],
  "updates": [{"action": "add-snapshot", ...}]
}
```

语义：表必须不存在才能执行操作。若表已存在，返回409。

**当前实现：**
```rust
pub fn check_requirements(&self, requirements: &[serde_json::Value]) -> Result<(), String> {
    for req in requirements {
        match req.get("type").and_then(|t| t.as_str()) {
            Some("assert-ref-snapshot-id") => self.check_assert_ref_snapshot_id(req),
            Some("assert-table-uuid") => self.check_assert_table_uuid(req),
            // assert-create及其他未实现！
            _ => Ok(()),
        }.ok();
    }
    Ok(())
}
```

**影响：**
- 无法实现staged create场景
- schema/分区变更无法做前置验证
- CAS语义不完整

**测试建议：**
```rust
#[tokio::test]
async fn test_commit_assert_create_success() {
    // 表不存在时，assert-create应成功
    let response = commit_with_updates(
        "nonexistent_table",
        r#"{"requirements": [{"type": "assert-create"}]}"#
    );
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_commit_assert_create_failure() {
    // 表已存在时，assert-create应失败
    create_table(&store, "existing_table");
    let response = commit_with_updates(
        "existing_table",
        r#"{"requirements": [{"type": "assert-create"}]}"#
    );
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(json["error"]["type"], "CommitFailedException");
}

#[tokio::test]
async fn test_commit_assert_current_schema_id() {
    create_table_with_schema(&store, schema_id=0);

    let response = commit_with_updates(
        r#"{"requirements": [{"type": "assert-current-schema-id", "current-schema-id": 0}]}"#
    );
    // schema-id匹配时应成功
    assert_eq!(response.status(), StatusCode::OK);

    // schema-id不匹配时应失败
    let fail_response = commit_with_updates(
        r#"{"requirements": [{"type": "assert-current-schema-id", "current-schema-id": 999}]}"#
    );
    assert_eq!(fail_response.status(), StatusCode::CONFLICT);
}
```

---

## 五、Object Store集成改进

### 5.1 并发写入冲突测试缺失

**问题背景：**
Metadata写入采用乐观锁（CAS），但object store写入无并发控制。

**代码实现：**
位置：`quasar/adapter/src/iceberg/table.rs:26`

```rust
async fn write_metadata_to_store(location, content, config) {
    // 直接写入，无并发控制
    write_json(&**store, &path, content).await
}
```

**风险场景：**
```
时间T1: Commit A计算metadata_location = .../00002-uuid-A.json
时间T2: Commit B计算metadata_location = .../00002-uuid-B.json (相同版本号)
时间T3: Commit A写入00002-A.json到S3
时间T4: Commit B写入00002-B.json到S3 → 覆盖A的内容！
时间T5: DB CAS验证，只有一个commit成功写入DB
结果: S3上有错误的metadata文件，或两个文件同时存在导致混乱
```

**当前问题：**
- `next_metadata_location`基于版本号递增，不同commit可能产生相同版本号
- uuid部分不同，但版本号可能相同（00002-uuid-A vs 00002-uuid-B）

**测试建议：**
```rust
#[tokio::test]
async fn test_concurrent_metadata_write_no_collision() {
    let table = create_table(&store, "users");

    // 同时发起两个commit（无依赖）
    let (r1, r2) = tokio::join!(
        commit_add_snapshot(snapshot_id=1),
        commit_add_snapshot(snapshot_id=2)
    );

    // 至少一个应失败（CAS或requirement check）
    let success_count = [r1, r2].iter().filter(|r| r.status() == OK).count();
    assert!(success_count <= 1);

    // 验证最终metadata一致性
    let final_state = load_table_metadata();
    // 只有一个snapshot应存在于refs
}

#[tokio::test]
async fn test_metadata_location_uuid_uniqueness() {
    // 多次commit验证metadata_location中的uuid唯一
    for i in 0..10 {
        let commit = commit_add_snapshot(snapshot_id=i);
        let location = commit["metadata-location"].as_str();
        // 每个location的uuid部分应不同
        assert!(location.contains(&format!("0000{}-", i+1)));
        assert!(uuid_parts_are_unique(locations));
    }
}
```

### 5.2 metadata版本溢出处理

**代码实现：**
位置：`quasar/adapter/src/iceberg/table.rs:97`

```rust
fn next_metadata_location(current: &str) -> String {
    // 找到序列号并+1
    let new_seq = seq + 1;
    // Handle overflow: extend width when needed
    let new_width = seq_str.len().max(new_seq.to_string().len());
    format!("{}{}{}", prefix, new_seq_str, suffix)
}
```

**边界场景：**
- 版本号从99999→100000时，width从5扩展到6
- 异常格式metadata_location的fallback行为

**测试建议：**
```rust
#[tokio::test]
async fn test_metadata_version_overflow_width() {
    // 构造metadata location接近溢出
    let current = "s3://bucket/metadata/99999-uuid.metadata.json";
    let next = next_metadata_location(current);

    assert!(next.contains("100000-")); // 正确扩展到6位
    assert!(next.contains(".metadata.json"));
}

#[tokio::test]
async fn test_metadata_location_unrecognized_format() {
    // 异常格式应fallback
    let current = "s3://bucket/unusual/location.json";
    let next = next_metadata_location(current);

    // fallback: append timestamp
    assert!(next.contains("-"));
    assert!(next.ends_with(".metadata.json"));
}

#[tokio::test]
async fn test_metadata_version_multiple_widths() {
    // 测试各种width的版本号
    let cases = [
        (".../00001-", ".../00002-"),
        (".../00009-", ".../00010-"), // width扩展
        (".../00099-", ".../00100-"),
        (".../00999-", ".../01000-"),
    ];
    for (before, after) in cases {
        assert!(next_metadata_location(before).starts_with(after));
    }
}
```

---

## 六、跨端点场景改进

### 6.1 请求头验证缺失

**问题背景：**
当前测试都使用正确的Content-Type，缺少异常场景验证。

**缺失场景：**
- 缺少Content-Type header → 应返回415 Unsupported Media Type
- Content-Type非application/json → 应返回415
- body非有效JSON → 应返回400 Bad Request
- body为空 → 应返回400

**测试建议：**
```rust
#[tokio::test]
async fn test_create_namespace_missing_content_type() {
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/iceberg/v1/default/namespaces")
            // 无Content-Type header
            .body(Body::from(r#"{"namespace": ["prod"]}"#))
    );
    // axum默认行为：返回415或尝试解析
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn test_create_namespace_wrong_content_type() {
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .header("Content-Type", "text/plain")
            .body(Body::from(r#"{"namespace": ["prod"]}"#))
    );
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn test_create_namespace_invalid_json() {
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"namespace": [}"#)) // 无效JSON
    );
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(json["error"]["type"], "BadRequestException");
}

#[tokio::test]
async fn test_create_namespace_empty_body() {
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .header("Content-Type", "application/json")
            .body(Body::empty())
    );
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

### 6.2 Domain隔离验证缺失

**问题背景：**
V3采用prefix=Domain.name的映射，但未测试不同Domain间的隔离。

**缺失测试：**
- Domain A的namespace不应被Domain B看到
- Domain A的table不应被Domain B访问
- 跨Domain rename应失败

**测试建议：**
```rust
#[tokio::test]
async fn test_namespace_domain_isolation() {
    // Domain A创建namespace
    create_namespace_in_domain("domain_a", "ns1");

    // Domain B不应看到
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/domain_b/namespaces")
    );
    assert!(!response["namespaces"].contains(&["ns1"]));
}

#[tokio::test]
async fn test_table_cross_domain_access_denied() {
    create_table_in_domain("domain_a", "prod", "users");

    // Domain B访问Domain A的表应返回404
    let response = app.oneshot(
        Request::builder()
            .uri("/iceberg/v1/domain_b/namespaces/prod/tables/users")
    );
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_rename_cross_domain_denied() {
    create_table_in_domain("domain_a", "prod", "users");

    // 尝试rename到Domain B应失败
    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/iceberg/v1/domain_a/tables/rename")
            .body(Body::from(r#"{
                "source": {"namespace": ["prod"], "name": "users"},
                "destination": {"namespace": ["domain_b$prod"], "name": "users"}
            }"#))
    );
    // 当前实现：destination.namespace第一段应为prod而非domain_b
    // 需明确：跨Domain rename是否允许？
}
```

---

## 七、改进优先级

### P0 (立即补充)

| 改进项 | 原因 | 预估工作量 |
|-------|------|-----------|
| `parent`参数实现 | 官方规范核心功能，当前完全缺失 | 需先实现功能再测试 |
| schema字段验证 | 表创建核心功能，影响last-column-id | 2-3个测试 |
| 格式隔离验证 | V3架构核心特性，已有文档但测试不足 | 2-3个测试 |

### P1 (近期补充)

| 改进项 | 原因 | 预估工作量 |
|-------|------|-----------|
| `upgrade-format-version` action | 表演进关键操作 | 先实现功能 |
| `add-partition-spec` action | 分区是Iceberg核心特性 | 先实现功能 |
| `assert-create` requirement | staged create场景必需 | 先实现功能 |
| list tables分页 | 大表场景性能保障 | 先实现功能 |
| 并发写入冲突验证 | 数据一致性保障 | 1-2个测试 |

### P2 (中期补充)

| 改进项 | 原因 | 预估工作量 |
|-------|------|-----------|
| pageSize边界值 | 边界安全验证 | 3-4个测试 |
| 请求头验证 | 健壮性保障 | 4-5个测试 |
| `warehouse`参数 | 多warehouse场景支持 | 先实现功能 |
| `purgeRequested`参数 | 数据清理语义 | 先实现功能 |
| Domain隔离验证 | 多租户场景 | 3个测试 |

### P3 (远期补充)

| 改进项 | 原因 | 预估工作量 |
|-------|------|-----------|
| metadata版本溢出 | 极端边界场景 | 2个测试 |
| `endpoints`字段 | 客户端兼容性 | 先实现功能 |
| HEAD错误体验证 | 规范完整性 | 1个测试 |
| 其他Update Action | 功能完整性 | 随功能实现 |
| 其他Requirement | CAS完整性 | 随功能实现 |

---

## 八、修订记录

### V1.0 (2026-05-14)

- 初始版本，记录Iceberg REST API测试覆盖改进点
- 涵盖Config/Namespace/Table/Commit/Object Store六大模块
- 按P0-P3四级优先级分类