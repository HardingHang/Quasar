# 主流开源项目格式隔离策略分析

> 本文档系统分析 Apache Gravitino、Apache Polaris、Unity Catalog、Project Nessie、Hive Metastore 五个主流项目的格式隔离策略，为 Quasar V3 的 Catalog 设计提供决策依据。
>
> 分析日期：2026-05-09

---

## 目录

1. [分析框架](#一分析框架)
2. [Gravitino —— Catalog 级隔离](#二gravitino--catalog-级隔离)
3. [Polaris —— 端点级隔离](#三polaris--端点级隔离)
4. [Unity Catalog —— 表属性级隔离](#四unity-catalog--表属性级隔离)
5. [Nessie —— Content Type 级隔离](#五nessie--content-type-级隔离)
6. [Hive Metastore —— 不隔离](#六hive-metastore--不隔离)
7. [汇总对比](#七汇总对比)
8. [对 Quasar V3 的启示](#八对-quasar-v3-的启示)

---

## 一、分析框架

格式隔离策略可以从五个维度分析：

| 维度 | 说明 |
|------|------|
| **隔离层级** | 格式差异在哪个层级被隔离（Catalog / Namespace / Table / Content） |
| **格式信息位置** | 格式标识存储在数据模型的哪一层 |
| **客户端指定方式** | 计算引擎如何告知服务端目标格式 |
| **服务端区分机制** | 服务端如何路由到对应格式的处理逻辑 |
| **存储隔离** | 不同格式的物理数据是否隔离存放 |
| **权限治理** | 权限模型是否按格式区分 |

---

## 二、Gravitino —— Catalog 级隔离

### 2.1 核心架构

```
Metalake（租户边界）
  └── Catalog 'iceberg_prod' (provider = lakehouse-iceberg, type = RELATIONAL)
  │     ├── properties: warehouse=s3://bucket/iceberg/, catalog-backend=jdbc
  │     └── Schema 'analytics'
  │           └── Table 'events'          ← 必须是 Iceberg 表
  │           └── Table 'users'           ← 必须是 Iceberg 表
  │
  └── Catalog 'lance_catalog' (provider = lance, type = RELATIONAL)
  │     ├── properties: warehouse=s3://bucket/lance/
  │     └── Schema 'vectors'
  │           └── Table 'embeddings'      ← 必须是 Lance 数据集
  │
  └── Catalog 'kafka_events' (provider = kafka, type = MESSAGING)
        ├── properties: bootstrap.servers=...
        └── Schema 'topic_ns'
              └── Topic 'clicks'          ← 必须是 Kafka Topic
```

### 2.2 格式信息位置

**Catalog 级别**。Catalog 创建时通过 `provider` 字段永久确定格式，之后不可更改：

```json
POST /api/metalakes/prod/catalogs
{
  "name": "iceberg_prod",
  "type": "RELATIONAL",
  "provider": "lakehouse-iceberg",
  "properties": {
    "warehouse": "s3://bucket/iceberg/",
    "catalog-backend": "jdbc",
    "uri": "jdbc:postgresql://..."
  }
}
```

`provider` 的可选值包括：`lakehouse-iceberg`、`lakehouse-paimon`、`lakehouse-delta`、`jdbc-mysql`、`jdbc-postgresql`、`kafka`、`hive` 等。

### 2.3 客户端指定方式

客户端**不需要**在每次操作时指定格式。连接 Catalog 时即隐含了格式：

```properties
# Spark 连接 Gravitino 的 Iceberg Catalog
spark.sql.catalog.iceberg.type = rest
spark.sql.catalog.iceberg.uri = http://gravitino:9001/iceberg/
spark.sql.catalog.iceberg.prefix = iceberg_prod
```

Spark 的 Iceberg connector 发送标准 Iceberg REST 请求，Gravitino 根据 `prefix=iceberg_prod` 路由到 Iceberg Catalog 实现。

### 2.4 服务端区分机制

Gravitino 服务端维护 Catalog 注册表，根据 `provider` 字段加载对应的 Catalog 实现类：

```java
// 简化逻辑
Catalog getCatalog(String name) {
    CatalogConfig config = catalogRegistry.get(name);
    switch (config.provider) {
        case "lakehouse-iceberg": return new IcebergCatalog(config);
        case "lance": return new LanceCatalog(config);
        case "kafka": return new KafkaCatalog(config);
        case "jdbc-mysql": return new JdbcCatalog(config);
        // ...
    }
}
```

Gravitino 使用 `IsolatedClassLoader` 为不同 provider 加载独立的类路径，防止 JAR 冲突。

### 2.5 存储隔离

**完全隔离**。不同 Catalog 可以指向完全不同的存储后端：
- `iceberg_prod` → S3 bucket A + PostgreSQL 元数据库
- `lance_catalog` → S3 bucket B
- `kafka_events` → Kafka 集群（无对象存储）

### 2.6 权限治理

**Catalog 级隔离**。RBAC 权限可以精确到 Catalog：
- 给用户 A 授予 `iceberg_prod` 的 READ 权限
- 给用户 B 授予 `lance_catalog` 的 ADMIN 权限

### 2.7 设计动机

Gravitino 的自我定位是 **"catalog of catalogs"**（元数据联邦），需要对接完全不同的外部系统：
- Hive Metastore（Thrift 协议）
- Iceberg REST Catalog（HTTP/REST 协议）
- MySQL/PostgreSQL（JDBC/SQL 协议）
- Kafka（Kafka Admin 协议）

这些系统的**连接方式、协议语义、配置参数完全不同**，无法在 Namespace 内混合。因此 Catalog 级隔离是必然选择。

---

## 三、Polaris —— 端点级隔离

### 3.1 核心架构

```
Polaris Instance
  └── Catalog 'prod' (storage: s3://bucket/prod/)
        └── Namespace 'analytics'
        │     ├── Iceberg table: 'events'
        │     │     → GET /api/catalog/v1/prod/namespaces/analytics/tables/events
        │     │
        │     └── Generic table: 'vectors' (format=lance)
        │           → GET /api/catalog/v1/prod/namespaces/analytics/generic-tables/vectors
        │
        └── Namespace 'marketing'
              ├── Iceberg table: 'campaigns'
              └── Generic table: 'embeddings' (format=delta)
```

### 3.2 格式信息位置

**表级别**。Catalog 和 Namespace 不绑定格式。表创建时通过 API 端点和 `format` 字段确定：

```json
// 创建 Generic Table（Lance）
POST /api/catalog/v1/prod/namespaces/analytics/generic-tables
{
  "name": "vectors",
  "format": "lance",
  "baseLocation": "s3://bucket/prod/analytics/vectors/"
}
```

Polaris 内部元数据表存储表类型标记：

```sql
-- Polaris 内部表（概念模型）
CREATE TABLE polaris_tables (
    id UUID PRIMARY KEY,
    catalog_id UUID REFERENCES catalogs(id),
    namespace_id UUID REFERENCES namespaces(id),
    name TEXT NOT NULL,
    table_type TEXT NOT NULL,  -- 'ICEBERG' | 'GENERIC'
    format TEXT,               -- 对 Generic: 'lance' | 'delta' | 'csv' | ...
    base_location TEXT,
    properties JSONB,
    -- 同一 Namespace 内名称唯一，不区分格式
    UNIQUE(namespace_id, name)
);
```

### 3.3 客户端指定方式

客户端通过**选择 API 端点**来指定格式：

| 操作 | Iceberg API | Generic Table API |
|------|-------------|-------------------|
| List | `GET /namespaces/{ns}/tables` | `GET /namespaces/{ns}/generic-tables` |
| Load | `GET /namespaces/{ns}/tables/{t}` | `GET /namespaces/{ns}/generic-tables/{t}` |
| Create | `POST /namespaces/{ns}/tables` | `POST /namespaces/{ns}/generic-tables` |
| Drop | `DELETE /namespaces/{ns}/tables/{t}` | `DELETE /namespaces/{ns}/generic-tables/{t}` |

**用错端点返回 `TableNotFoundException`**。

### 3.4 服务端区分机制

Polaris 服务端根据请求路径和内部 `table_type` 字段路由：

```java
// 伪代码
@Path("/api/catalog/v1/{catalog}/namespaces/{ns}/tables/{table}")
Table loadIcebergTable(...) {
    TableRecord t = tableService.load(catalog, ns, table);
    if (!t.tableType.equals("ICEBERG")) {
        throw new TableNotFoundException("Not an Iceberg table");
    }
    return icebergCatalog.loadTable(ns, table);
}

@Path("/api/catalog/v1/{catalog}/namespaces/{ns}/generic-tables/{table}")
GenericTable loadGenericTable(...) {
    TableRecord t = tableService.load(catalog, ns, table);
    if (!t.tableType.equals("GENERIC")) {
        throw new TableNotFoundException("Not a generic table");
    }
    return new GenericTable(t.format, t.baseLocation, t.properties);
}
```

### 3.5 存储隔离

**否**。Iceberg 表和 Generic 表可以共用同一个 S3 bucket 和目录结构。Polaris 只存元数据指针，不管理物理存储隔离。

Generic Table 的 `baseLocation` 由客户端管理，Polaris "is unaware of anything about the underlying table except some loosely defined metadata"。

### 3.6 权限治理

**Namespace 级统一**。权限授予在 Namespace 级别，对两种表都生效：
- `TABLE_CREATE` 权限 → 可以在 Namespace 内创建 Iceberg 表或 Generic 表
- `TABLE_READ` 权限 → 可以读取两种表

但 Generic Table **不支持 Update**，治理能力不完整：
- "no support for Update Generic Table"
- "no commit coordination or update capability provided at the catalog service level"

### 3.7 设计动机

Polaris 的核心定位是 **Iceberg 原生 Catalog**，Generic Table 是扩展能力：
- Iceberg 是 first-class，享受完整协议支持（snapshot、schema evolution、partitioning）
- Generic Table 是降级支持，只提供基本的 CRUD + listing
- 共享 Namespace 是为了统一治理和发现，但协议层面保持隔离

---

## 四、Unity Catalog —— 表属性级隔离

### 4.1 核心架构

```
Metastore（区域级基础设施）
  └── Catalog 'prod'（环境隔离：如生产环境）
        └── Schema 'analytics'
              ├── Table 'events'            ← Delta 格式（managed）
              ├── Table 'external_csv'      ← CSV 格式（external）
              ├── View 'monthly_summary'    ← 虚拟表
              ├── Volume 'raw_files'        ← 非结构化数据
              ├── Function 'parse_json'     ← UDF
              └── Model 'predictor'         ← ML 模型
```

### 4.2 格式信息位置

**表级别（securable object 级别）**。Schema 内完全混合格式，靠 `securable_type` 和表属性区分：

```sql
-- Unity Catalog 内部（概念模型）
-- 统一身份层
CREATE TABLE securable_objects (
    id BIGINT PRIMARY KEY,
    catalog_id BIGINT,
    schema_id BIGINT,
    name TEXT NOT NULL,
    securable_type TEXT NOT NULL,  -- 'TABLE' | 'VIEW' | 'VOLUME' | 'FUNCTION' | 'MODEL'
    owner TEXT,
    created_at TIMESTAMP,
    -- Schema 内名称唯一
    UNIQUE(schema_id, name)
);

-- 表属性层
CREATE TABLE table_properties (
    securable_id BIGINT PRIMARY KEY,
    table_type TEXT,              -- 'MANAGED' | 'EXTERNAL'
    data_source_format TEXT,      -- 'DELTA' | 'PARQUET' | 'CSV' | 'JSON' | 'ORC' | 'AVRO'
    storage_location TEXT,
    -- Delta 特有
    delta_version BIGINT,
    -- 通用
    properties JSONB
);
```

### 4.3 客户端指定方式

客户端**不主动指定格式**。查询时引擎自动处理：

```sql
-- 用户写 SQL，不关心底层格式
SELECT * FROM prod.analytics.events;          -- Delta，Spark 自动用 Delta connector
SELECT * FROM prod.analytics.external_csv;    -- CSV，Spark 自动处理
SELECT * FROM prod.analytics.monthly_summary; -- View，Spark 展开查询计划
```

引擎（Spark/Trino）通过 Unity Catalog 的 Iceberg REST 接口获取元数据，根据 `data_source_format` 选择对应的 reader。

### 4.4 服务端区分机制

Unity Catalog 服务端根据 `securable_type` 路由到对应的处理逻辑：

```scala
// 伪代码
def loadObject(catalog: String, schema: String, name: String): SecurableObject = {
    val so = securableObjectRepo.get(catalog, schema, name)
    so.securableType match {
        case "TABLE"    => loadTable(so.id)
        case "VIEW"     => loadView(so.id)
        case "VOLUME"   => loadVolume(so.id)
        case "FUNCTION" => loadFunction(so.id)
        case "MODEL"    => loadModel(so.id)
    }
}

def loadTable(id: Long): Table = {
    val props = tablePropertiesRepo.get(id)
    props.dataSourceFormat match {
        case "DELTA"   => loadDeltaTable(props)
        case "PARQUET" => loadParquetTable(props)
        case "CSV"     => loadCsvTable(props)
        // ...
    }
}
```

### 4.5 存储隔离

**部分隔离**：
- **Managed 表**：统一由 Metastore 管理存储位置，按 Catalog/Schema 层级组织
- **External 表**：可以指向任意位置，格式信息不影响存储位置

存储位置继承规则：
```
Schema-level location  ← 最高优先级（显式指定）
    ↓
Catalog-level location
    ↓
Metastore default location  ← 最低优先级
```

### 4.6 权限治理

**完全统一**。所有 securable object 共享同一套 RBAC：
- `SELECT` 权限 → 对 Table、View、Volume 都适用
- `EXECUTE` 权限 → 对 Function 适用
- `READ VOLUME` 权限 → 对 Volume 适用
- 列级权限 → 对所有 Table 统一适用
- 血缘追踪 → 跨 Table、View、Model 统一

### 4.7 设计动机

Unity Catalog 的核心定位是 **Databricks 的统一治理层**，格式多样性是"常态"而非"例外"：
- Databricks 生态以 Delta Lake 为核心，但支持导入多种格式
- 资产分类维度是**数据形态**（表/视图/文件集/模型）而非**存储引擎**
- 治理（权限、血缘、审计）必须跨类型统一

---

## 五、Nessie —— Content Type 级隔离

### 5.1 核心架构

```
Repository
  ├── Reference 'main'（Branch）
  │     └── Commit 'abc123'
  │           ├── Content: 'db.events'    (type=ICEBERG_TABLE, payload=...)
  │           ├── Content: 'db.vectors'   (type=DELTA_LAKE_TABLE, payload=...)
  │           ├── Content: 'db.views'     (type=ICEBERG_VIEW, payload=...)
  │           └── Content: 'db.ns'        (type=NAMESPACE, payload=...)
  │
  └── Reference 'dev'（Branch）
        └── Commit 'def456'
              ├── Content: 'db.events'    (type=ICEBERG_TABLE, 不同版本)
              └── Content: 'db.vectors'   (type=DELTA_LAKE_TABLE, 不同版本)
```

### 5.2 格式信息位置

**Content 对象级别**。所有资产在同一个 Namespace 下，靠 `ContentType` 区分：

```java
// Nessie 的 Content 接口
public interface Content {
    ContentType getType();
}

// 内置 Content Type
public enum ContentType {
    ICEBERG_TABLE(1),      // payload_id = 1
    DELTA_LAKE_TABLE(2),   // payload_id = 2
    ICEBERG_VIEW(3),       // payload_id = 3
    NAMESPACE(4),          // payload_id = 4
    UDF(5);                // payload_id = 5
}

public class IcebergTable implements Content {
    private TableMetadata metadata;  // Iceberg 特有的元数据
    private SnapshotId snapshotId;
    // ...
}

public class DeltaLakeTable implements Content {
    private String lastCheckpoint;   // Delta 特有的元数据
    private Map<String, String> metadata;
    // ...
}
```

### 5.3 客户端指定方式

客户端**不指定格式**。获取 Content 时 Nessie 返回序列化后的 payload，客户端根据 `ContentType` 自行反序列化：

```java
// 伪代码
Content content = nessie.getContent("main", "db.events");
switch (content.getType()) {
    case ICEBERG_TABLE:
        IcebergTable iceberg = (IcebergTable) content;
        return iceberg.getMetadata();
    case DELTA_LAKE_TABLE:
        DeltaLakeTable delta = (DeltaLakeTable) content;
        return delta.getLastCheckpoint();
    // ...
}
```

### 5.4 服务端区分机制

Nessie 服务端**不解析格式语义**。它只存二进制 payload 和 Content Type ID：

```java
// 存储层（Version Store）
class ContentStorage {
    // 存储：key → (payload_id, payload_bytes)
    void store(String key, int payloadId, byte[] payload) {
        // payload 是序列化后的二进制 blob
        // Nessie 不关心 blob 的内容
    }

    Content load(String key) {
        int payloadId = readPayloadId(key);     // 1, 2, 3, 4, 5...
        byte[] payload = readPayloadBytes(key);  // 二进制 blob
        // 通过注册表查找对应的反序列化器
        ContentSerializer serializer = registry.get(payloadId);
        return serializer.deserialize(payload);
    }
}
```

序列化/反序列化由 `ContentSerializerBundle` 提供，按 `payloadId` 分发。

### 5.5 存储隔离

**服务端存储完全隔离于格式之上**。Nessie 的存储层（RocksDB/PostgreSQL/DynamoDB/BigTable）只存：
- Commit Log（不可变的历史记录）
- Content Key → (payload_id, payload_blob) 的映射

物理数据文件的位置由各个 Content 对象的 payload 自行管理（Iceberg 的 metadata.json 指向 data files，Delta 的 delta log 指向 parquet 文件）。

### 5.6 权限治理

**Commit/ContentKey 级统一**。权限绑定到 Branch/Tag 和 ContentKey 路径：
- `READ` on `db.*` → 可以读 `db.events`（Iceberg）和 `db.vectors`（Delta）
- `WRITE` on `db.events` → 可以修改 Iceberg 表
- 权限声明中**不区分 Content Type**

### 5.7 设计动机

Nessie 的核心定位是 **Git for Data**，核心能力是跨表原子事务和分支管理：
- 一次 Commit 可以同时修改 Iceberg table 和 Delta table
- 它们必须在同一个 Namespace 下才能被纳入同一个 Commit
- 服务端只负责"版本控制"，不负责"格式解析"
- 格式解析完全委托给客户端引擎

---

## 六、Hive Metastore —— 不隔离

### 6.1 核心架构

```
Hive Metastore
  └── Database 'analytics'
        ├── Table 'events'
        │     → TBLS.TBL_TYPE='EXTERNAL_TABLE'
        │     → SDS.INPUT_FORMAT='org.apache.hadoop.mapred.TextInputFormat'
        │     → SDS.OUTPUT_FORMAT='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        │     → SERDES.SLIB='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        │
        └── Table 'users'
              → TBLS.TBL_TYPE='MANAGED_TABLE'
              → SDS.INPUT_FORMAT='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
              → SDS.OUTPUT_FORMAT='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
              → SERDES.SLIB='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
```

### 6.2 格式信息位置

**Storage Descriptor 级别**。格式信息散落在多张表中：

```sql
-- TBLS: 所有表混在一起，只有 TBL_TYPE
SELECT TBL_NAME, TBL_TYPE FROM TBLS WHERE DB_ID = 1;
-- events | EXTERNAL_TABLE
-- users  | MANAGED_TABLE

-- SDS: 存储格式信息
SELECT t.TBL_NAME, s.INPUT_FORMAT, s.OUTPUT_FORMAT, s.LOCATION
FROM TBLS t JOIN SDS s ON t.SD_ID = s.SD_ID;
-- events | TextInputFormat    | HiveIgnoreKeyTextOutputFormat | s3://bucket/events/
-- users  | MapredParquetInputFormat | MapredParquetOutputFormat | s3://bucket/users/

-- SERDES: 序列化器
SELECT t.TBL_NAME, se.SLIB
FROM TBLS t
JOIN SDS s ON t.SD_ID = s.SD_ID
JOIN SERDES se ON s.SERDE_ID = se.SERDE_ID;
-- events | LazySimpleSerDe
-- users  | ParquetHiveSerDe
```

### 6.3 客户端指定方式

客户端**不指定格式**。Hive/Spark 通过 `INPUT_FORMAT` / `OUTPUT_FORMAT` / `SERDE` 自动推断：

```sql
-- 用户写 Hive SQL
SELECT * FROM analytics.events;  -- Hive 自动用 TextInputFormat + LazySimpleSerDe
SELECT * FROM analytics.users;   -- Hive 自动用 ParquetInputFormat + ParquetHiveSerDe
```

### 6.4 服务端区分机制

HMS 服务端**不区分格式**。它把所有表当作同质对象管理：
- `TBLS` 表存储所有表的通用元数据
- `SDS` 表存储所有 Storage Descriptor
- `SERDES` 表存储所有序列化器

格式信息只是表属性的一部分，HMS 本身不理解格式的语义差异。

### 6.5 存储隔离

**不隔离**。所有表的数据文件可以放在同一个 HDFS/S3 目录下，由 `SDS.LOCATION` 字段分别指定。

### 6.6 权限治理

**不区分格式**。Hive 的权限模型（SQL GRANT/REVOKE）作用于 Database 和 Table 级别，与格式无关。

---

## 七、汇总对比

### 7.1 核心机制对比

| 维度 | Gravitino | Polaris | Unity Catalog | Nessie | Hive Metastore |
|------|-----------|---------|---------------|--------|----------------|
| **隔离层级** | Catalog 级 | 端点级 | 表属性级 | Content Type 级 | 不隔离 |
| **格式信息位置** | `Catalog.provider` | `Table.format` + 端点 | `Table.data_source_format` | `Content.type` | `SDS.INPUT_FORMAT` |
| **客户端指定方式** | 连接 Catalog 时隐含 | 选择 API 端点 | 不指定 | 不指定 | 不指定 |
| **服务端区分机制** | Catalog 注册表 + provider 路由 | `table_type` 字段 + 端点路由 | `securable_type` + `data_source_format` | payload_id + 序列化器注册表 | 不区分，靠客户端推断 |
| **存储隔离** | 是（不同 Catalog 可不同 bucket） | 否（共用存储） | 部分（Managed vs External） | 是（服务端只存 blob） | 否 |
| **权限隔离粒度** | Catalog 级 | Namespace 级统一 | 完全统一（跨类型） | Commit/ContentKey 级统一 | Database 级统一 |
| **跨格式原子操作** | 不可能（不同 Catalog） | 不可能 | 不可能 | ✅ 可能（同一 Commit） | 不可能 |

### 7.2 适用场景对比

| 项目 | 隔离策略 | 适用场景 |
|------|---------|---------|
| **Gravitino** | Catalog 级 | 格式差异极大（Iceberg vs Kafka vs MySQL），需要完全不同的协议、配置和 connector |
| **Polaris** | 端点级 | 以 Iceberg 为主，其他格式为附加（Generic Table），共享存储和治理基础设施 |
| **Unity Catalog** | 表属性级 | 格式差异中等（Delta/Parquet/CSV），同一引擎可以处理多种格式，治理需要跨类型统一 |
| **Nessie** | Content Type 级 | 服务端不做格式解析，只存 blob，跨格式原子事务和分支管理是核心需求 |
| **Hive Metastore** | 不隔离 | 传统 Hive 生态，格式由 Storage Descriptor 描述，服务端无格式概念 |

### 7.3 隔离策略的代价

| 隔离策略 | 优势 | 代价 |
|---------|------|------|
| **Catalog 级** | 协议简单（每个 Catalog 一种协议）；配置自洽；无格式冲突 | 同一业务域跨格式资产需要拆到多个 Catalog；引擎需要配置多个 Catalog |
| **端点级** | 共享 Namespace 和治理；统一发现入口 | 端点数量随格式增加；用错端点报错；Generic 表治理能力降级 |
| **表属性级** | 完全灵活的混合格式；统一治理和血缘 | 服务端需要处理所有格式的元数据；扩展表 schema 复杂 |
| **Content Type 级** | 跨格式原子 Commit；服务端极简 | 服务端不解析格式，无法做格式特定的优化；所有逻辑在客户端 |
| **不隔离** | 最简单的模型 | 格式信息散落；无格式感知能力；无法做格式特定的治理 |

---

## 八、对 Quasar V3 的启示

### 8.1 Quasar 的场景特征

| 维度 | Quasar 的情况 |
|------|-------------|
| 自包含性 | Quasar 是独立的 Catalog 服务，不是元数据联邦（区别于 Gravitino） |
| 协议暴露 | 同时暴露 Iceberg REST 和 Lance REST 协议端点 |
| 格式数量 | 当前：Iceberg + Lance；未来可能 + Delta + Model |
| 治理需求 | 当前：基础 CRUD；未来：权限、血缘 |
| 计算引擎 | Iceberg → Spark/Trino/Flink；Lance → Python/Daft/Ray |

### 8.2 关键判断

**Quasar 不适合纯 Catalog 级隔离**，原因是：
- Gravitino 的 Catalog 级隔离是为了对接**完全不同的外部系统**（Hive/JDBC/Kafka），每个系统的连接方式和协议完全不同
- Quasar 是**自包含服务**，所有数据都在自己的 PostgreSQL 中管理，不存在"对接外部 Metastore"的需求
- Iceberg 和 Lance 的格式差异虽然大，但都在 Quasar 的统一管理范围内

**Quasar 也不适合纯 Nessie 模式**，原因是：
- Quasar 需要服务端解析格式语义（Iceberg 的 metadata.json、Lance 的 version 管理）
- 不能做"只存 blob"的极简设计

**最可能适合 Quasar 的是 Polaris 式端点级隔离 + 统一身份层**：
- Domain 作为**存储配置边界**（不同 Domain 可以不同 S3 bucket）
- 同一 Domain 内**不隔离格式**
- 不同格式的资产通过**不同 REST 端点**访问
- 统一身份层（`assets`）支持跨格式查询和治理

### 8.3 待决策问题

1. **Domain 是否绑定格式？**
   - 绑定：Domain 创建时指定 provider（Gravitino 模式）
   - 不绑定：Domain 只是存储配置容器，格式由表决定（Polaris 模式）

2. **同一 Domain 内是否允许混合格式？**
   - 允许：V3 设计更灵活，但端点更复杂
   - 不允许：V3 实现更简单，但用户需要管理多个 Domain

3. **统一入口（Unified API）如何处理混合格式？**
   - 如果 Domain 绑定格式，Unified API 需要按 Domain 过滤
   - 如果 Domain 不绑定格式，Unified API 需要按表级 format 字段过滤

### 8.4 决策结论（Quasar V3 采用）

经讨论确认，Quasar V3 采用以下方案：

1. **Domain（顶层容器）不绑定格式** —— 采用 Polaris 式端点级隔离。Domain 只是存储配置容器，`format` 由资产级别的 `tabular_assets.format` 字段决定。

2. **同一 Domain 内允许混合格式** —— Iceberg 表和 Lance 数据集可以共存于同一 Domain/Namespace 下。不同格式的资产通过不同的 REST 端点访问（`/iceberg/v1/...` vs `/lance/v1/...`）。

3. **Unified API 按表级 format 字段过滤** —— 不依赖 Domain 绑定，通过 `tabular_assets.format` 字段区分格式，返回跨格式的资产列表。

4. **端点行为** —— 各协议端点内部按 `format` 字段过滤，非匹配格式返回 `TableNotFoundException`。

详见 `docs/v3/V3_REQUIREMENTS.md` 第 2.2 节（端点级隔离）、第 4.2 节（层级与命名需求）以及 `docs/v3/V3_DATA_MODEL_DESIGN.md`。

---

*本文档基于公开文档和源码分析整理，部分内部实现细节为合理推测。*
