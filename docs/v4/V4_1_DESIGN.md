# Quasar V4.1 设计说明书

> **版本**: V1.0
> **日期**: 2026-06-05
> **状态**: V4.1 设计基线
>
> 本文档承接 V4.1 需求文档 (`V4_1_REQUIREMENTS.md`) 的实现设计。
> 官方 REST API 来源、端点全集和小版本范围矩阵以
> `docs/v4/V4_OFFICIAL_REST_API.md` 为准；本文档只描述 Quasar 的 V4.1
> 实现选择。
>
> **前置依赖**: V4.0 已完成并通过验收；本文档经评审批准后方可进入实现阶段。

---

## 目录

- [一、执行摘要](#一执行摘要)
- [二、架构总览](#二架构总览)
- [三、Multi-table Transactions 设计](#三multi-table-transactions-设计)
- [四、TableUpdate 补全设计](#四tableupdate-补全设计)
- [五、Multi-warehouse 最小支持设计](#五multi-warehouse-最小支持设计)
- [六、数据模型增量](#六数据模型增量)
- [七、Store trait 增量](#七store-trait-增量)
- [八、错误处理增量](#八错误处理增量)
- [九、测试策略](#九测试策略)
- [十、实现顺序](#十实现顺序)
- [十一、V4.1 不做什么](#十一v41-不做什么)
- [十二、修订记录](#十二修订记录)

---

## 一、执行摘要

### 1.1 V4.1 核心目标

V4.1 在 V4.0 "Spark-ready Iceberg baseline" 基础上，补齐 REST Table Capability 的非安全类功能：

- **Multi-table Transactions**: `POST /v1/{prefix}/transactions/commit` 原子性批量提交。
- **TableUpdate 补全**: Statistics 4 项 + `remove-schemas`，补齐 V4.0 返回 501 的非安全类 actions。
- **Multi-warehouse 最小支持**: 所有 Iceberg REST 端点的 `warehouse` 参数校验与 `NoSuchWarehouseException` 错误响应。

V4.1 明确排除所有鉴权/安全/凭证功能（vended credentials、S3 Signer、encryption key），这些功能推迟到后续安全专项版本。

### 1.2 V4.0 -> V4.1 关键设计变更

| 设计域 | V4.0 状态 | V4.1 设计 |
|--------|-----------|-----------|
| Transactions | 不实现 | `POST /v1/{prefix}/transactions/commit` 原子性批量提交，采用"对象存储预写 + PostgreSQL 事务"方案 |
| Statistics actions | 返回 501 | 实现 `set-statistics`、`remove-statistics`、`set-partition-statistics`、`remove-partition-statistics` |
| RemoveSchemas | 返回 501 | 实现 `remove-schemas` action，边界条件校验 |
| Warehouse 参数 | 无校验 | 所有 Iceberg REST 端点校验 `warehouse` 参数，非默认值返回 `NoSuchWarehouseException` |

### 1.3 V4.1 设计原则

1. **继承 V4.0 原则**: 官方协议路径优先、声明即实现、metadata 兼容优先、Catalog 状态由 PostgreSQL 仲裁。
2. **事务原子性优先**: Multi-table transactions 必须保证原子性，任一表失败则全部回滚。
3. **最小变更原则**: Multi-warehouse 仅添加参数校验层，不扩展配置模型或数据模型。
4. **安全功能排除**: 所有凭证/鉴权/加密功能不在 V4.1 范围内。

---

## 二、架构总览

### 2.1 V4.1 组件边界

```
Spark Iceberg 1.10.x client
        |
        v
  /iceberg/v1/{prefix}/...
        |
        v
quasar-adapter::iceberg
  - Transactions handler (新增)
  - Statistics / RemoveSchemas action handlers (扩展)
  - Warehouse validation middleware (新增)
  - state: Arc<dyn CatalogStore>
        |
        v
quasar-core
  - V4.0 Store trait family (继承)
  - IcebergTransactionStore (新增)
  - StoreError semantic errors (继承)
        |
        v
quasar-storage
  - PostgreSQL catalog identity rows (继承)
  - Transaction commit (新增: 单事务内多表 CAS)
  - Warehouse validation helper (新增)
        |
        v
PostgreSQL + S3/MinIO object store
```

### 2.2 V4.1 新增端点

| 端点 | V4.1 状态 |
|------|----------|
| `POST /v1/{prefix}/transactions/commit` | 新增实现 |
| 所有端点的 `warehouse` 参数校验 | 新增校验层 |

### 2.3 `/v1/config` endpoints 更新

V4.1 实现后，`/v1/config` 的 `endpoints` 字段新增：

```json
{
  "endpoints": [
    "...V4.0 endpoints...",
    "/v1/{prefix}/transactions/commit"
  ]
}
```

---

## 三、Multi-table Transactions 设计

### 3.1 协议端点

```
POST /v1/{prefix}/transactions/commit
```

请求体：`CommitTransactionRequest`（Iceberg 1.10.x OpenAPI），包含：
- `table-changes`: 多个表的 `TableCommit` 列表，每个包含 `identifier`、`requirements`、`updates`

### 3.2 原子性方案

采用**对象存储预写 + PostgreSQL 事务原子提交**方案：

#### 3.2.1 执行流程

```
Phase 1: 准备阶段（不涉及 DB）
┌─────────────────────────────────────────┐
│ 1. 解析请求，按表名分组为独立的 TableCommit  │
│ 2. 校验所有表存在（通过 store.get_tabular_asset）│
│ 3. 对每个表：                              │
│    - 读取当前 metadata JSON               │
│    - 校验 requirements                    │
│    - 应用 updates，生成新 metadata JSON   │
│ 4. 统一写入所有新 metadata.json 到对象存储 │
│    - 若任何写入失败 → 返回 500，终止       │
│    - 成功后记录所有新 metadata_location   │
└─────────────────────────────────────────┘

Phase 2: DB 提交阶段（PostgreSQL 事务）
┌─────────────────────────────────────────┐
│ 5. 开启 PostgreSQL 事务                   │
│ 6. 在事务中逐表执行 CAS update：           │
│    - UPDATE tabular_assets               │
│    SET metadata_location = new_location  │
│    WHERE metadata_location = expected    │
│    - 若任何 CAS 失败 → 事务回滚，返回 409  │
│ 7. 提交事务                               │
│ 8. 返回 204 No Content                    │
└─────────────────────────────────────────┘
```

#### 3.2.2 原子性保证

- **DB 层原子性**: PostgreSQL 事务保证所有表的 `metadata_location` 更新原子性。任一 CAS 失败则全部回滚，客户端通过 catalog 读取时不会观察到部分更新状态。
- **对象存储孤儿文件容忍**: 对象存储写入成功但 PostgreSQL CAS 失败时，已写入的 metadata.json 成为孤儿文件。V4.1 容忍此现象，孤儿清理推迟到 V4.3。

#### 3.2.3 死锁预防

为避免事务与单表 commit 之间发生死锁：

- **锁顺序一致**: 所有 CAS update 按 `(domain_name, namespace_name, table_name)` 字典序锁定，事务内逐表 update 严格按此顺序执行。
- **单表 commit 同样顺序**: 单表 `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` 的 CAS update 也使用相同的锁定顺序。

### 3.3 错误处理

| 场景 | 响应 | Iceberg error type |
|------|------|--------------------|
| 单表 CAS 冲突 | 409 | `CommitFailedException`，包含冲突表名 |
| 对象存储写入失败 | 500 | `InternalServerError` |
| 请求中某表不存在 | 404 | `NoSuchTableException`，不执行任何 commit |
| 请求格式错误 | 400 | `BadRequestException` |
| requirement 不满足 | 409 | `CommitFailedException` |

### 3.4 Adapter 层实现

```rust
// quasar/adapter/src/iceberg/transaction.rs (新增)

pub async fn commit_transaction(
    State(store): State<Arc<dyn CatalogStore>>,
    Path(prefix): Path<String>,
    Json(request): Json<CommitTableRequest>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    // Phase 1: 准备阶段
    let table_changes = request.table_changes;
    let mut new_metadata_locations: Vec<(String, String, String, String, String)> = Vec::new();
    
    // 按字典序排序，保证锁顺序一致
    let sorted_changes = sort_table_changes_by_name(&table_changes);
    
    for change in &sorted_changes {
        let (namespace, table) = parse_identifier(&change.identifier)?;
        
        // 校验表存在并读取当前 metadata
        let (_, tabular) = store.get_tabular_asset(&prefix, &namespace, "iceberg", &table).await
            .map_err(store_error_to_iceberg_table)?;
        
        let current_location = tabular.metadata_location.as_ref()
            .ok_or_else(|| IcebergError::InternalServerError {
                message: "table has no metadata_location".to_string(),
            })?;
        
        let metadata_json = read_metadata_from_store(current_location, None, &config).await?;
        let mut metadata: TableMetadata = serde_json::from_value(metadata_json)
            .map_err(|e| IcebergError::BadRequestException {
                message: format!("invalid metadata JSON: {}", e),
            })?;
        
        // 校验 requirements
        metadata.check_requirements(&change.requirements)
            .map_err(|msg| IcebergError::CommitFailedException { message: msg })?;
        
        // 应用 updates
        metadata.apply_updates(&change.updates);
        
        // 生成新 metadata location
        let new_location = generate_next_metadata_location(&metadata, current_location);
        let new_json = serde_json::to_value(&metadata)
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to serialize metadata: {}", e),
            })?;
        
        // 写入对象存储
        write_metadata_to_store(&new_location, &new_json, &config).await?;
        
        new_metadata_locations.push((
            prefix.clone(),
            namespace.clone(),
            table.clone(),
            current_location.clone(),
            new_location.clone(),
        ));
    }
    
    // Phase 2: DB 提交阶段
    store.commit_transaction_tables(new_metadata_locations).await
        .map_err(|e| match e {
            StoreError::Conflict { msg } => IcebergError::CommitFailedException { message: msg },
            other => store_error_to_iceberg_table(other),
        })?;
    
    Ok(StatusCode::NO_CONTENT)
}
```

### 3.5 Store trait 新增

```rust
// quasar/core/src/store.rs (新增)

/// Multi-table transaction commit (V4.1).
/// 在单个 PostgreSQL 事务中执行多表 CAS update，保证原子性。
#[async_trait]
pub trait IcebergTransactionStore: CatalogStore {
    /// 执行多表事务提交。
    /// 参数: Vec<(domain, namespace, table, expected_location, new_location)>
    /// 所有表的 CAS update 在同一事务中执行，任一失败则全部回滚。
    async fn commit_transaction_tables(
        &self,
        table_updates: Vec<(String, String, String, String, String)>,
    ) -> Result<(), StoreError>;
}
```

### 3.6 Storage 实现

```rust
// quasar/storage/src/store.rs (新增)

#[async_trait]
impl IcebergTransactionStore for PgCatalogStore {
    async fn commit_transaction_tables(
        &self,
        table_updates: Vec<(String, String, String, String, String)>,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        
        for (domain, namespace, table, expected, new) in &table_updates {
            // CAS update: metadata_location 条件更新
            let result = sqlx::query!(
                r#"
                UPDATE tabular_assets ta
                SET metadata_location = $1
                FROM assets a
                WHERE a.id = ta.asset_id
                  AND a.domain_name = $2
                  AND a.namespace_name = $3
                  AND a.name = $4
                  AND ta.format = 'iceberg'
                  AND ta.metadata_location = $5
                "#,
                new,
                domain,
                namespace,
                table,
                expected,
            )
            .execute(&mut *tx)
            .await?;
            
            if result.rows_affected() == 0 {
                // CAS 失败，事务自动回滚
                return Err(StoreError::Conflict {
                    msg: format!(
                        "CAS conflict for table {}.{}: expected {}",
                        namespace, table, expected
                    ),
                });
            }
        }
        
        tx.commit().await?;
        Ok(())
    }
}
```

---

## 四、TableUpdate 补全设计

### 4.1 Statistics Actions（4 项）

#### 4.1.1 数据结构

参考 Iceberg 1.10.x OpenAPI：

```rust
// quasar/adapter/src/iceberg/table_metadata.rs (扩展)

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct StatisticsFile {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    pub file_size_in_bytes: i64,
    pub file_footer_size_in_bytes: i64,
    #[serde(default)]
    pub key_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct PartitionStatisticsFile {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "statistics-path")]
    pub statistics_path: String,
    pub file_size_in_bytes: i64,
}

// TableMetadata 扩展字段
#[serde(default)]
pub statistics: Vec<StatisticsFile>,
#[serde(default)]
pub partition_statistics: Vec<PartitionStatisticsFile>,
```

#### 4.1.2 Actions 实现

**SetStatistics**:

```rust
TableUpdate::SetStatistics {
    statistics: StatisticsFile,
} => {
    // 移除同一 snapshot-id 的旧统计信息（如有）
    self.statistics.retain(|s| s.snapshot_id != statistics.snapshot_id);
    self.statistics.push(statistics);
    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
}
```

**RemoveStatistics**:

```rust
TableUpdate::RemoveStatistics {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
} => {
    self.statistics.retain(|s| s.snapshot_id != snapshot_id);
    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
}
```

**SetPartitionStatistics** / **RemovePartitionStatistics**: 类似逻辑，操作 `partition_statistics` 列表。

#### 4.1.3 统计信息文件校验行为

V4.1 设计决策：**不校验统计信息文件存在性**。

- 统计信息文件（JSON）由客户端生成并上传至对象存储，服务端只更新 metadata 中的引用。
- 若引用的统计信息文件在对象存储中不存在，服务端不阻塞 commit，仅记录警告日志。
- 此行为与 Iceberg Java 实现一致（参考：Iceberg 1.10.x `BaseTableOperations.applyStatisticsUpdate`）。

### 4.2 RemoveSchemas Action

#### 4.2.1 实现逻辑

```rust
TableUpdate::RemoveSchemas {
    #[serde(rename = "schema-ids")]
    schema_ids: Vec<i32>,
} => {
    // 校验：不能移除当前 schema
    for id in &schema_ids {
        if *id == self.current_schema_id {
            return Err("Cannot remove current schema".to_string());
        }
    }
    
    // 移除指定的 schemas
    self.schemas.retain(|s| !schema_ids.contains(&s.schema_id));
    
    // 校验：必须保留至少一个 schema
    if self.schemas.is_empty() {
        return Err("Cannot remove all schemas".to_string());
    }
    
    // 若只剩一个 schema，确保其 ID 为 current_schema_id
    if self.schemas.len() == 1 {
        self.current_schema_id = self.schemas[0].schema_id;
    }
    
    self.last_updated_ms = chrono::Utc::now().timestamp_millis();
}
```

#### 4.2.2 边界条件处理

| 条件 | 行为 |
|------|------|
| 移除当前 schema | 返回错误，commit 失败 |
| 移除后无 schema 剩余 | 返回错误，commit 失败 |
| 移除不存在的 schema ID | 静默忽略，commit 成功 |
| 移除后只剩一个 schema | 自动设置 `current_schema_id` 为该 schema ID |

### 4.3 Update Enum 扩展

```rust
// quasar/adapter/src/iceberg/table_metadata.rs (扩展 TableUpdate enum)

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    // V4.0 已有 actions...
    
    // V4.1 新增
    SetStatistics {
        statistics: StatisticsFile,
    },
    RemoveStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    SetPartitionStatistics {
        partition_statistics: PartitionStatisticsFile,
    },
    RemovePartitionStatistics {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    RemoveSchemas {
        #[serde(rename = "schema-ids")]
        schema_ids: Vec<i32>,
    },
    
    // V4.1 明确拒绝（推迟到安全专项）
    #[serde(rename = "add-encryption-key")]
    AddEncryptionKey { .. } => {
        return Err("add-encryption-key not supported in V4.1".to_string());
    },
    #[serde(rename = "remove-encryption-key")]
    RemoveEncryptionKey { .. } => {
        return Err("remove-encryption-key not supported in V4.1".to_string());
    },
}
```

### 4.4 错误映射

| 场景 | HTTP | Iceberg error type |
|------|------|--------------------|
| RemoveSchemas 移除当前 schema | 400 | `BadRequestException` |
| RemoveSchemas 移除后无 schema | 400 | `BadRequestException` |
| encryption key actions | 501 | `NotImplementedException` |

---

## 五、Multi-warehouse 最小支持设计

### 5.1 需求范围

V4.1 仅实现**参数校验层**，不扩展配置模型：

- 所有 Iceberg REST 端点在携带 `warehouse` 查询参数时，校验该 warehouse 是否存在。
- 当前配置语义：**单一 warehouse**（默认值），任何非默认值返回 `NoSuchWarehouseException`。
- 不涉及多 warehouse 存储后端隔离。

### 5.2 校验机制

#### 5.2.1 Warehouse 配置来源

```rust
// quasar/adapter/src/iceberg/mod.rs

pub struct IcebergConfig {
    // V4.0 已有
    pub object_store: Option<Arc<ObjectStore>>,
    pub s3_bucket: Option<String>,
    pub warehouse_path: Option<String>,
    
    // V4.1 新增
    pub default_warehouse: String,  // 默认 warehouse 名称
}
```

默认 warehouse 名称来源：
- 若 `QUASAR_WAREHOUSE` 环境变量设置，使用该值。
- 否则使用 `"default"` 作为默认名称。

#### 5.2.2 校验逻辑

```rust
// quasar/adapter/src/iceberg/warehouse.rs (新增)

pub fn validate_warehouse(
    query_warehouse: Option<&str>,
    config: &IcebergConfig,
) -> Result<(), IcebergError> {
    match query_warehouse {
        None => Ok(()),  // 无参数，使用默认 warehouse
        Some(name) if name == config.default_warehouse => Ok(()),  // 匹配默认值
        Some(name) => Err(IcebergError::NoSuchWarehouseException {
            message: format!("Warehouse does not exist: {}", name),
        }),
    }
}
```

#### 5.2.3 Middleware/Extract 实现

方案：在 Axum handler 中通过 `Query` extractor 提取 `warehouse` 参数并校验。

```rust
// 所有 Iceberg endpoint handler 增加校验

#[derive(Deserialize)]
pub struct WarehouseQuery {
    warehouse: Option<String>,
}

pub async fn load_table(
    State(store): State<Arc<dyn CatalogStore>>,
    Path((prefix, namespace, table)): Path<(String, String, String)>,
    Query(warehouse_query): Query<WarehouseQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<impl IntoResponse, IcebergError> {
    // V4.1: warehouse 校验
    validate_warehouse(warehouse_query.warehouse.as_deref(), &config)?;
    
    // V4.0 逻辑继续...
}
```

### 5.3 错误类型新增

```rust
// quasar/adapter/src/iceberg/error.rs (扩展)

#[derive(Debug)]
pub enum IcebergError {
    // V4.0 已有...
    
    // V4.1 新增
    NoSuchWarehouseException { message: String },
}

impl IcebergError {
    pub fn to_error_response(&self) -> ErrorResponse {
        match self {
            IcebergError::NoSuchWarehouseException { message } => {
                (message.clone(), "NoSuchWarehouseException".to_string(), 404)
            }
            // ...
        }
    }
}
```

### 5.4 覆盖端点范围

所有 Iceberg REST 端点需添加 warehouse 校验：

| 端点类别 | 端点 |
|----------|------|
| Config | `GET /v1/config` |
| Namespace | list, create, get, head, delete, update properties |
| Table | list, create, register, load, head, commit, drop, rename, metrics |
| Transaction | `POST /v1/{prefix}/transactions/commit` |

---

## 六、数据模型增量

### 6.1 无新增数据库表

V4.1 不扩展 PostgreSQL 数据模型：
- Statistics 数据存储于 metadata.json，无需 PostgreSQL 持久化。
- Multi-warehouse 仅参数校验，不扩展 Domain 或配置表。
- Transactions 不引入新表，使用现有 `tabular_assets.metadata_location` CAS。

### 6.2 Metadata JSON 扩展

TableMetadata JSON 新增字段（符合 Iceberg 1.10.x 规范）：

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "statistics": [
    {
      "snapshot-id": 123456789,
      "statistics-path": "s3://bucket/stats/123456789.stats.json",
      "file-size-in-bytes": 1024,
      "file-footer-size-in-bytes": 128
    }
  ],
  "partition-statistics": [
    {
      "snapshot-id": 123456789,
      "statistics-path": "s3://bucket/partition-stats/123456789.stats.json",
      "file-size-in-bytes": 512
    }
  ]
}
```

---

## 七、Store trait 增量

### 7.1 新增 trait

```rust
// quasar/core/src/store.rs

/// Multi-table transaction commit (V4.1).
#[async_trait]
pub trait IcebergTransactionStore: CatalogStore {
    async fn commit_transaction_tables(
        &self,
        table_updates: Vec<(String, String, String, String, String)>,
    ) -> Result<(), StoreError>;
}

// CatalogStore marker trait 扩展
pub trait CatalogStore:
    DomainStore
    + NamespaceStore
    + AssetStore
    + TabularStore
    + VersionStore
    + TabularVersionStore
    + CasCommitStore
    + UnifiedQueryStore
    + IcebergStagingStore
    + IcebergRegisterStore
    + IcebergMetricsStore
    + IcebergPurgeStore
    + IcebergTransactionStore  // V4.1 新增
    + Send
    + Sync
{
}
```

---

## 八、错误处理增量

### 8.1 新增错误类型

| 场景 | HTTP | Iceberg error type | message 模板 |
|------|------|--------------------|--------------|
| Warehouse 不存在 | 404 | `NoSuchWarehouseException` | `Warehouse does not exist: {warehouse}` |

### 8.2 错误响应示例

```json
{
  "error": {
    "message": "Warehouse does not exist: analytics",
    "type": "NoSuchWarehouseException",
    "code": 404
  }
}
```

### 8.3 脱敏规则继承

继承 V4.0 脱敏规则：
- 客户端错误不包含数据库 SQL、S3 secret、连接串。
- 事务失败日志包含 domain、namespace、table、metadata_location，不含 credential。

---

## 九、测试策略

### 9.1 Adapter 集成测试新增

| 测试组 | 测试名称 | 覆盖范围 |
|--------|----------|----------|
| Transactions | `test_transaction_commit_success` | 2~3 表事务成功提交 |
| Transactions | `test_transaction_cas_conflict_rollback` | 单表 CAS 冲突导致全部回滚 |
| Transactions | `test_transaction_object_store_failure` | 对象存储写入失败不进入 DB |
| Transactions | `test_transaction_table_not_found` | 请求中表不存在返回 404 |
| Statistics | `test_set_statistics` | `set-statistics` action 成功 |
| Statistics | `test_remove_statistics` | `remove-statistics` action 成功 |
| Statistics | `test_set_partition_statistics` | `set-partition-statistics` action 成功 |
| Statistics | `test_remove_partition_statistics` | `remove-partition-statistics` action 成功 |
| Statistics | `test_statistics_in_transaction` | Statistics actions 在事务中混合使用 |
| RemoveSchemas | `test_remove_schemas_success` | 成功移除非当前 schema |
| RemoveSchemas | `test_remove_schemas_current_fails` | 移除当前 schema 返回 400 |
| RemoveSchemas | `test_remove_schemas_all_fails` | 移除全部 schema 返回 400 |
| RemoveSchemas | `test_remove_schemas_nonexistent_ignored` | 移除不存在 ID 静默忽略 |
| Warehouse | `test_warehouse_default_ok` | 默认 warehouse 参数通过 |
| Warehouse | `test_warehouse_none_ok` | 无 warehouse 参数通过 |
| Warehouse | `test_warehouse_invalid_404` | 无效 warehouse 返回 NoSuchWarehouseException |
| Warehouse | `test_warehouse_all_endpoints` | 所有端点校验 warehouse |

### 9.2 并发测试

Transactions 并发测试：
- 使用 `testcontainers-postgres` 验证真实并发 CAS。
- `tokio::join!` 同时提交两个事务，验证仅一个成功。

### 9.3 TEST_MATRIX 更新

新增 V4.1 测试条目：

| 测试组 | 测试名称 | 覆盖范围 | 状态 |
|--------|----------|----------|------|
| Iceberg transactions | `test_transaction_commit_success` | 多表事务成功路径 | pending |
| Iceberg transactions | `test_transaction_cas_conflict` | CAS 冲突回滚验证 | pending |
| Iceberg commit | `test_statistics_actions` | Statistics 4 项 actions | pending |
| Iceberg commit | `test_remove_schemas` | RemoveSchemas action | pending |
| Iceberg warehouse | `test_warehouse_validation` | Warehouse 参数校验 | pending |

---

## 十、实现顺序

### 10.1 C1: Warehouse 校验层

- 新增 `IcebergConfig.default_warehouse` 配置。
- 新增 `NoSuchWarehouseException` 错误类型。
- 所有 Iceberg REST handler 添加 `validate_warehouse` 校验。
- 更新 `/v1/config` endpoints 字段（无新增，只是校验层）。

### 10.2 C2: TableUpdate 补全

- 扩展 `TableMetadata` 结构：`statistics`、`partition_statistics` 字段。
- 扩展 `TableUpdate` enum：5 项新 actions + 2 项拒绝的 encryption key。
- 实现 `apply_updates` 中新 actions 处理逻辑。
- RemoveSchemas 边界条件校验。
- Statistics 文件存在性不校验，仅记录警告。

### 10.3 C3: Multi-table Transactions

- 新增 `IcebergTransactionStore` trait。
- Storage 实现 `commit_transaction_tables`：单事务多表 CAS。
- Adapter 实现 `commit_transaction` handler：Phase 1 准备 + Phase 2 DB 提交。
- 死锁预防：按字典序锁定。
- 错误映射：CAS 冲突、对象存储失败、表不存在。

### 10.4 C4: 测试与文档

- Adapter 集成测试：transactions、statistics、remove-schemas、warehouse。
- 并发事务 CAS 测试。
- 更新 `docs/TEST_MATRIX.md`。
- 更新 `docs/v4/PROGRESS.md`。
- `/v1/config` endpoints 字段添加 transactions 端点声明。

---

## 十一、V4.1 不做什么

V4.1 不设计、不实现以下能力：

1. Vended credentials（`GET .../tables/{table}/credentials`）— 推迟到后续安全专项版本。
2. S3 Signer API（`POST /v1/aws/s3/sign`）— 推迟到后续安全专项版本。
3. Encryption key actions（`add-encryption-key`、`remove-encryption-key`）— 推迟到后续安全专项版本。
4. 多 warehouse 存储后端隔离 — 仅参数校验，不扩展配置模型。
5. Statistics 数据 PostgreSQL 持久化 — 仅存储于 metadata.json。
6. Statistics 文件存在性校验 — 不校验，容忍引用不存在文件。
7. 对象存储孤儿文件清理 — 推迟到 V4.3。
8. Iceberg views — 推迟到 V4.2。
9. Server-side scan planning — 推迟到 V4.2。

---

## 十二、修订记录

### V1.0 (2026-06-05)

- 初始 V4.1 设计基线。
- 设计 Multi-table Transactions：对象存储预写 + PostgreSQL 事务方案。
- 设计 Statistics 4 项 + RemoveSchemas actions 实现。
- 设计 Multi-warehouse 参数校验层（单一 warehouse，NoSuchWarehouseException）。
- 明确 V4.1 不做什么（credentials、S3 Signer、encryption key 等安全功能推迟）。