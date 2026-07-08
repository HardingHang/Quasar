# Quasar V3 官方 REST API 能力清单

> **版本**: V1.0
> **日期**: 2026-05-12
> **状态**: 设计参考
>
> 本文档维护 Quasar V3 需要对齐的官方 REST API 来源、端点能力和 V3 实现范围。
> `V3_DESIGN.md` 只描述 Quasar 的实现选择；官方端点清单以本文档为准。

> ⚠️ **冻结说明（自 V1.2 起）**: 本文档已被 `docs/v4/V4_OFFICIAL_REST_API.md` 取代，仅保留作为 V3 历史记录。后续端点清单与 V4 范围以 v4 版本为准。

---

## 一、官方来源

| 协议 | 官方来源 | 用途 |
|------|----------|------|
| Iceberg REST Catalog | https://iceberg.apache.org/rest-catalog-spec/ | REST Catalog 规范入口 |
| Iceberg OpenAPI YAML | https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml | 端点、请求/响应模型、错误模型权威来源 |
| Iceberg Endpoint Javadoc | https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/rest/Endpoint.html | Java 客户端支持端点常量 |
| Lance REST Namespace Catalog Spec | https://lance.org/format/namespace/rest/catalog-spec/ | Lance REST Namespace 协议说明 |
| Lance REST Namespace Implementation Spec | https://lance.org/format/namespace/rest/impl-spec/ | Lance REST Namespace 路由、错误码和实现约定 |

---

## 二、Quasar Domain 映射规则

### 2.1 Iceberg

Iceberg 官方路径为 `/v1/{prefix}/...`。Quasar V3 将 `Domain.name` 映射为 Iceberg `{prefix}`：

| Quasar | Iceberg |
|--------|---------|
| Domain `prod` | `{prefix}=prod` |
| Namespace `analytics` | `{namespace}=analytics` |
| Table `events` | `{table}=events` |

示例：Quasar 部署基路径为 `/iceberg` 时，原生 Iceberg 客户端访问：

```text
GET /iceberg/v1/prod/namespaces/analytics/tables/events
```

其中 `/iceberg` 是服务部署 base path，不属于 Iceberg 协议操作路径；协议路径仍是 `/v1/{prefix}/...`。

### 2.2 Lance

Lance 官方路径不提供独立的 Domain path segment，所有 Namespace/Table 身份都通过 `{id}` 表达。Quasar V3 将 `Domain.name` 编码为 Lance identifier 的第一段：

| Quasar | Lance identifier parts | 默认 `$` 分隔后的 `{id}` |
|--------|------------------------|--------------------------|
| Domain `prod`, Namespace `analytics` | `["prod", "analytics"]` | `prod$analytics` |
| Domain `prod`, Namespace `analytics`, Table `embeddings` | `["prod", "analytics", "embeddings"]` | `prod$analytics$embeddings` |

示例：Quasar 部署基路径为 `/lance` 时，原生 Lance REST Namespace 客户端访问：

```text
POST /lance/v1/table/prod$analytics$embeddings/describe
```

其中 `/lance` 是服务部署 base path，不属于 Lance 协议操作路径；协议路径仍是 `/v1/table/{id}/...`。

---

## 三、Iceberg REST Catalog 官方端点

### 3.1 Configuration / OAuth

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| GET | `/v1/config` | 获取 catalog 配置、默认值、覆盖值和支持端点列表 | 必须实现 |
| POST | `/v1/oauth/tokens` | OAuth2 token 交换，官方已标记 deprecated | 暂不实现 |

### 3.2 Namespace

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| GET | `/v1/{prefix}/namespaces` | 列出 Namespace | 必须实现 |
| POST | `/v1/{prefix}/namespaces` | 创建 Namespace | 必须实现 |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 加载 Namespace 属性 | 必须实现 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}` | 检查 Namespace 是否存在 | 必须实现 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 删除空 Namespace | 必须实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 更新 Namespace 属性 | 必须实现 |

### 3.3 Table

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| GET | `/v1/{prefix}/namespaces/{namespace}/tables` | 列出表 | 必须实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables` | 创建表或 staged create | 必须实现基础创建 |
| POST | `/v1/{prefix}/namespaces/{namespace}/register` | 注册已有 metadata file 为表 | 暂不实现 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 加载表 metadata | 必须实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 提交表更新（requirements-based 乐观锁 / CAS commit） | 必须实现 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 删除表，可带 `purgeRequested` | 必须实现 catalog drop，purge 可暂不实现 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 检查表是否存在 | 必须实现 |
| POST | `/v1/{prefix}/tables/rename` | 重命名表 | 必须实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | 上报表 metrics | 暂不实现 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` | 加载 vended credentials | 暂不实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign` | S3 SigV4 代理签名（与 vended credentials 配合） | 暂不实现 |

### 3.4 Table Scan Planning

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | 提交服务端 scan planning | 暂不实现 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 获取 scan planning 结果 | 暂不实现 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 取消 scan planning | 暂不实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | 拉取 plan-task token 对应的 FileScanTask 列表（fetchScanTasks 语义） | 暂不实现 |

### 3.5 Transaction

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/{prefix}/transactions/commit` | 原子提交多个表更新 | 暂不实现 |

### 3.6 View

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| GET | `/v1/{prefix}/namespaces/{namespace}/views` | 列出视图 | 暂不实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/views` | 创建视图 | 暂不实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/register-view` | 注册已有 view metadata | 暂不实现 |
| GET | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 加载视图 | 暂不实现 |
| POST | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 替换视图 | 暂不实现 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 删除视图 | 暂不实现 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 检查视图是否存在 | 暂不实现 |
| POST | `/v1/{prefix}/views/rename` | 重命名视图 | 暂不实现 |

---

## 四、Lance REST Namespace 官方端点

### 4.1 Namespace

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/namespace/{id}/create` | 创建 Namespace | 必须实现 |
| GET | `/v1/namespace/{id}/list` | 列出子 Namespace | 必须实现单层列表 |
| POST | `/v1/namespace/{id}/describe` | 查询 Namespace 属性 | 必须实现 |
| POST | `/v1/namespace/{id}/drop` | 删除空 Namespace | 必须实现 |
| POST | `/v1/namespace/{id}/exists` | 检查 Namespace 是否存在 | 必须实现 |

### 4.2 Table 基础操作

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/table/{id}/declare` | 声明表，保留名称和位置 | 必须实现 |
| GET | `/v1/namespace/{id}/table/list` | 列出 Namespace 下的表 | 必须实现 |
| POST | `/v1/table/{id}/describe` | 查询表 metadata，可指定版本 | 必须实现 |
| POST | `/v1/table/{id}/deregister` | 注销表但保留存储数据 | 必须实现 |
| POST | `/v1/table/{id}/drop` | 删除表及其数据 | 必须实现 catalog drop，数据 purge 可按配置处理 |
| POST | `/v1/table/{id}/register` | 注册已有 Lance 表 | 必须实现 |
| POST | `/v1/table/{id}/rename` | 重命名表 | 必须实现 |
| POST | `/v1/table/{id}/exists` | 检查表是否存在 | 必须实现 |
| GET | `/v1/table/list` | 跨 Namespace 列出所有表 | 暂不实现 |
| POST | `/v1/table/{id}/restore` | 恢复到指定版本 | 暂不实现 |
| POST | `/v1/table/{id}/create` | 基于 Arrow stream 创建表 | 暂不实现 |

### 4.3 Table Version

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/table/{id}/version/create` | 创建或注册表版本记录 | 必须实现 |
| GET | `/v1/table/{id}/version/list` | 列出表版本 | 必须实现 |
| POST | `/v1/table/{id}/version/describe` | 查询表版本 | 必须实现 |
| POST | `/v1/table/version/batch-create` | 批量创建版本记录 | 暂不实现 |
| POST | `/v1/table/{id}/version/batch-delete` | 批量删除版本记录 | 暂不实现 |

### 4.4 Table Schema / DML / Query

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/table/{id}/stats` | 查询表统计信息 | 暂不实现 |
| POST | `/v1/table/{id}/schema/metadata` | 更新 schema metadata | 暂不实现 |
| POST | `/v1/table/{id}/add_columns` | 添加列 | 暂不实现 |
| POST | `/v1/table/{id}/alter_columns` | 修改列 | 暂不实现 |
| POST | `/v1/table/{id}/backfill_column` | 回填计算列 | 暂不实现 |
| POST | `/v1/table/{id}/drop_columns` | 删除列 | 暂不实现 |
| POST | `/v1/table/{id}/refresh` | 刷新物化视图 | 暂不实现 |
| POST | `/v1/table/{id}/insert` | 插入数据 | 暂不实现 |
| POST | `/v1/table/{id}/merge-insert` | merge insert/upsert | 暂不实现 |
| POST | `/v1/table/{id}/update` | 更新数据 | 暂不实现 |
| POST | `/v1/table/{id}/delete` | 删除数据 | 暂不实现 |
| POST | `/v1/table/{id}/query` | 查询数据 | 暂不实现 |
| POST | `/v1/table/{id}/count` | 统计行数 | 暂不实现 |
| POST | `/v1/table/{id}/query/explain` | 查询计划 explain | 暂不实现 |
| POST | `/v1/table/{id}/query/analyze` | 查询计划 analyze | 暂不实现 |

### 4.5 Index / Tag / Transaction

| 方法 | 官方路径 | 能力 | V3 范围 |
|------|----------|------|---------|
| POST | `/v1/table/{id}/index/create` | 创建向量索引 | 暂不实现 |
| POST | `/v1/table/{id}/index/create-scalar` | 创建标量索引 | 暂不实现 |
| GET | `/v1/table/{id}/index/list` | 列出索引 | 暂不实现 |
| POST | `/v1/table/{id}/index/{index_name}/stats` | 查询索引统计 | 暂不实现 |
| POST | `/v1/table/{id}/index/{index_name}/drop` | 删除索引 | 暂不实现 |
| GET | `/v1/table/{id}/tag/list` | 列出 tag | 暂不实现 |
| POST | `/v1/table/{id}/tag/{tag_name}/describe` | 查询 tag 指向版本 | 暂不实现 |
| POST | `/v1/table/{id}/tag/create` | 创建 tag | 暂不实现 |
| POST | `/v1/table/{id}/tag/{tag_name}/delete` | 删除 tag | 暂不实现 |
| POST | `/v1/table/{id}/tag/{tag_name}/update` | 更新 tag | 暂不实现 |
| POST | `/v1/transaction/{id}/describe` | 查询事务 | 暂不实现 |
| POST | `/v1/transaction/{id}/alter` | 修改事务状态 | 暂不实现 |

---

## 七、Quasar 自定义管理 API（Unified Domain）

> 本章节端点为 **Quasar-specific**，不属于 Iceberg REST Catalog 或 Lance REST Namespace 官方协议。它们通过 Unified API (`/unified/v1`) 暴露，用于 Domain 生命周期管理。

| 方法 | 路径 | 能力 | V3 范围 |
|------|------|------|---------|
| GET | `/unified/v1/domains` | 列出所有 Domain | 必须实现 |
| POST | `/unified/v1/domains` | 创建 Domain | 必须实现 |
| GET | `/unified/v1/domains/{domain}` | 获取单个 Domain | 必须实现 |
| PATCH | `/unified/v1/domains/{domain}` | 更新 Domain | 必须实现 |
| DELETE | `/unified/v1/domains/{domain}` | 删除空 Domain | 必须实现 |

**设计约束**：
- `DELETE` 仅对空 Domain 生效（无下属 Namespace）；非空删除返回 `409 DomainNotEmpty`。
- `GET /unified/v1/domains/{domain}` 响应中的 `storage_config` 字段必须脱敏（credential / secret 值不得明文暴露）。
- Domain 名称在全局唯一；`name` 字段同时受 `validate_name` 规则约束。

---

## 五、维护规则

1. 修改 `V3_DESIGN.md` 第四章 REST API 范围时，必须同步更新本文档的 V3 范围列。
2. 引入新官方端点时，必须先在本文档登记官方路径和能力，再在设计文档中说明实现策略。
3. Quasar 自定义管理 API 必须明确标注为 Quasar-specific，不得混入官方协议端点表。
4. 官方协议路径不得因 Domain、tenant、environment 等 Quasar 内部概念增加自定义 path segment；这些概念只能映射到官方参数、identifier 或服务部署 base path。

---

## 六、修订记录

### V1.2 (2026-05-18)

- 补遗 `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign`（S3 SigV4 代理签名）端点。
- 微调 commit_table 描述为 "提交表更新（requirements-based 乐观锁 / CAS commit）"。
- 微调 tasks 描述为 "拉取 plan-task token 对应的 FileScanTask 列表（fetchScanTasks 语义）"。
- 顶部追加 V4 迁移 admonition：本文档冻结，V4 范围以 `docs/v4/V4_OFFICIAL_REST_API.md` 为准。
- 本次校对范围仅涵盖 Iceberg 章节（第三章）；Lance、Unified 章节未动。

### V1.1 (2026-05-13)

- 新增 **七、Quasar 自定义管理 API（Unified Domain）** 章节，登记 5 个 Domain 管理端点。
- 明确 V3 路径形态变更：
  - Iceberg `/iceberg/v1/{prefix}/...` 中 `{prefix}` = Domain.name；
  - Lance `/lance/v1/table/{id}/...` 中 `{id}` 第一段 = Domain.name；
  - Unified Namespace/Asset 路由统一挂载到 `/unified/v1/domains/{domain}/...`。

### V1.0

- 新增 Iceberg REST Catalog 与 Lance REST Namespace 官方来源。
- 新增官方端点能力清单与 Quasar V3 实现范围。
- 明确 Iceberg Domain 到 `{prefix}`、Lance Domain 到 `{id}` 第一段的映射规则。
