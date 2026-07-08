# Quasar V4 官方 REST API 能力清单

> **版本**: V1.7
> **日期**: 2026-06-08
> **状态**: V4.2 已实现
>
> 本文档维护 Quasar V4 需要对齐的官方 REST API 来源、端点能力和分小版本实现范围。
> `V4_DESIGN.md` 只描述 Quasar 的实现选择；官方端点清单以本文档为准。

---

## 一、官方基线

V4 的 Iceberg REST Catalog 协议基线固定为 **Apache Iceberg 1.10.x 发布版**。不得以 `main` 分支漂移内容作为 V4.0 必须实现范围；`main` 或 1.11+ 仅可进入后续候选项。

| 协议 / 能力 | 官方来源 | 用途 |
|-------------|----------|------|
| Iceberg REST Catalog Spec | https://iceberg.apache.org/rest-catalog-spec/ | REST Catalog 规范入口 |
| Iceberg OpenAPI YAML 1.10.0 | https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml | REST Catalog 端点、请求/响应模型、错误模型权威来源 |
| Iceberg Endpoint Javadoc 1.10.0 | https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/rest/Endpoint.html | Java 客户端支持端点常量 |
| Iceberg Constant Values 1.10.1 | https://iceberg.apache.org/javadoc/1.10.1/constant-values.html | Java ResourcePaths 常量交叉校验 |
| Iceberg S3 Signer OpenAPI 1.10.0 | https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/aws/src/main/resources/s3-signer-open-api.yaml | S3 remote signing；不属于 REST Catalog 主 OpenAPI |
| iceberg crate | https://docs.rs/iceberg/ | Rust 端 Iceberg 类型与工具库候选 |
| Lance REST Namespace Catalog Spec | https://lance.org/format/namespace/rest/catalog-spec/ | Lance REST Namespace 协议说明 |
| Lance REST Namespace Implementation Spec | https://lance.org/format/namespace/rest/impl-spec/ | Lance REST Namespace 路由、错误码和实现约定 |

**基线口径**:

- Iceberg REST Catalog 1.10.x OpenAPI 共 30 个 operation（含 deprecated `POST /v1/oauth/tokens`）；排除 OAuth 后为 29 个 operation。
- 1.10.x `BaseUpdate` discriminator 共 25 项：6 项 table/view 共用（`assign-uuid`、`upgrade-format-version`、`add-schema`、`set-location`、`set-properties`、`remove-properties`）+ 2 项 view-only（`add-view-version`、`set-current-view-version`）+ 17 项 table-only（其余全部）。`BaseRequirement` 共 9 项：1 项 table/view 共用（`assert-create`）+ 1 项 view-only（`assert-view-uuid`）+ 7 项 table-only（其余全部）。
- 1.10.x 不包含 `POST /v1/{prefix}/namespaces/{namespace}/register-view`。
- 1.10.x 不包含 `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign`；S3 signing 是独立 S3 Signer API，官方路径为 `/v1/aws/s3/sign`。
- OpenAPI 是 REST Catalog 路径权威；Java `ResourcePaths` 常量用于兼容性复核。若二者存在差异，设计文档必须明确主路径与兼容 alias。

---

## 二、Quasar Domain 映射规则

### 2.1 Iceberg

Iceberg 官方路径为 `/v1/{prefix}/...`。Quasar V4 沿用 V3 约定，将 `Domain.name` 映射为 Iceberg `{prefix}`：

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

Lance 官方路径不提供独立的 Domain path segment，所有 Namespace/Table 身份都通过 `{id}` 表达。Quasar V4 沿用 V3 约定，将 `Domain.name` 编码为 Lance identifier 的第一段：

| Quasar | Lance identifier parts | 默认 `$` 分隔后的 `{id}` |
|--------|------------------------|--------------------------|
| Domain `prod`, Namespace `analytics` | `["prod", "analytics"]` | `prod$analytics` |
| Domain `prod`, Namespace `analytics`, Table `embeddings` | `["prod", "analytics", "embeddings"]` | `prod$analytics$embeddings` |

---

## 三、小版本范围规则

| 小版本 | 定位 | 范围承诺 |
|--------|------|----------|
| V4.0 | Spark-ready Iceberg baseline | 必须交付；只纳入 Spark 3.5 + Iceberg 1.10.x E2E 必需路径与 V3 Iceberg 兼容性缺口 |
| V4.1 | REST table capability completion candidate | 后续候选；补齐 table credentials、transactions、非 Spark 必需 table update 能力和 S3 Signer 可选集成 |
| V4.2 | Views and scan planning candidate | 后续候选；补齐 Iceberg view 生命周期与 server-side scan planning |
| V4.3 | Operational hardening candidate | 后续候选；性能、缓存、后台清理、多客户端验证和 Lance/Unified 演进 |

`/v1/config` 的 `endpoints` 字段必须只声明当前服务实际支持的 REST Catalog endpoint；候选小版本端点在未实现前不得出现在 `endpoints` 中。

---

## 四、Iceberg REST Catalog 1.10.x 官方端点

### 4.1 Configuration / OAuth

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| GET | `/v1/config` | 获取 catalog 配置、默认值、覆盖值和支持端点列表 | 必须实现 | 持续维护 |
| POST | `/v1/oauth/tokens` | OAuth2 token 交换，官方已 deprecated | 不实现 | 不实现；生产鉴权另行设计 |

### 4.2 Namespace

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| GET | `/v1/{prefix}/namespaces` | 列出 Namespace | 维持并补测试 | cursor pagination 可在 V4.3 评估 |
| POST | `/v1/{prefix}/namespaces` | 创建 Namespace | 维持并补测试 | 持续维护 |
| GET | `/v1/{prefix}/namespaces/{namespace}` | 加载 Namespace 属性 | 维持并补测试 | 持续维护 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}` | 检查 Namespace 是否存在 | 维持并补测试 | 持续维护 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}` | 删除空 Namespace | 维持并补测试 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/properties` | 更新 Namespace 属性 | 维持并补测试 | 持续维护 |

### 4.3 Table

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| GET | `/v1/{prefix}/namespaces/{namespace}/tables` | 列出表 | 维持并补测试 | cursor pagination 可在 V4.3 评估 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables` | 创建表或 staged create | 必须实现 staged create 修复 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/register` | 注册已有 metadata file 为表 | 必须实现 | 持续维护 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 加载表 metadata | 维持 V3 行为；`If-None-Match` 头部、snapshot 参数化加载视为 V4.3 候选评估 | metadata cache 可在 V4.3 评估 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 提交表更新 | 必须实现 Spark E2E 必需 actions/requirements；未知或未支持项按官方错误模型拒绝 | V4.1 补齐 1.10.x table update 全集 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 删除表，可带 `purgeRequested` | 必须实现 catalog drop；`purgeRequested=true` 至少完成表路径内 metadata/data 清理 | V4.3 完善孤儿文件后台清理 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/tables/{table}` | 检查表是否存在 | 维持并补测试 | 持续维护 |
| POST | `/v1/{prefix}/tables/rename` | 重命名表 | 维持并补测试 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` | 上报表 metrics | 必须实现接收与日志/持久化最小闭环 | V4.3 接入观测指标体系 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials` | 加载 vended credentials | 不进入 V4.0 必须范围 | 推迟到后续安全专项版本；V4.1 不实现 |

### 4.4 Table Commit Actions / Requirements

1.10.x `TableRequirement` 官方集合：

- `assert-create`
- `assert-table-uuid`
- `assert-ref-snapshot-id`
- `assert-last-assigned-field-id`
- `assert-current-schema-id`
- `assert-last-assigned-partition-id`
- `assert-default-spec-id`
- `assert-default-sort-order-id`

1.10.x `TableUpdate` 官方集合：

- `assign-uuid`
- `upgrade-format-version`
- `add-schema`
- `set-current-schema`
- `add-spec`
- `set-default-spec`
- `add-sort-order`
- `set-default-sort-order`
- `add-snapshot`
- `set-snapshot-ref`
- `remove-snapshots`
- `remove-snapshot-ref`
- `set-location`
- `set-properties`
- `remove-properties`
- `set-statistics`
- `remove-statistics`
- `set-partition-statistics`
- `remove-partition-statistics`
- `remove-partition-specs`
- `remove-schemas`
- `add-encryption-key`
- `remove-encryption-key`

V4.0 的最低要求是覆盖 Spark 3.5 + Iceberg 1.10.x E2E 会触发的 requirement/update，并对已知但暂未支持的官方 update 返回清晰错误。V4.1 补齐 `TableUpdate` 全集中的非安全类 actions（statistics 4 项、remove-schemas）；encryption key actions（`add-encryption-key`、`remove-encryption-key`）推迟到后续安全专项版本。

注：1.10.x `BaseUpdate` discriminator 共 25 项；本表列出 17 个 table-only 项 + 6 个 table/view 共用项（`assign-uuid`、`upgrade-format-version`、`add-schema`、`set-location`、`set-properties`、`remove-properties`）；2 个 view-only 项（`add-view-version`、`set-current-view-version`）将在 V4.2 §4.7 View 端点章节单独登记。statistics（含 partition statistics）、remove-schemas 等 5 项 update 不在 V4.0 必须实现范围，归入 V4.1；encryption key 2 项推迟到后续安全专项版本。

### 4.5 Table Scan Planning

| 方法 | OpenAPI 官方路径 | 能力 | V4.0 | 后续候选 |
|------|------------------|------|------|----------|
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | 提交服务端 scan planning | V4.2 已实现 | 持续维护 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 获取 scan planning 结果 | V4.2 已实现 | 持续维护 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 取消 scan planning | V4.2 已实现 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | 拉取 plan-task token 对应的 FileScanTask 列表 | V4.2 已实现 | 持续维护 |

兼容性注意：scan planning 在 OpenAPI 与 Java `ResourcePaths` 常量中存在路径差异。

- **OpenAPI 路径**: `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` / `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` / `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}`
- **Java `ResourcePaths` 常量路径**: `/v1/{prefix}/tables/{table}/plan` / `/v1/{prefix}/tables/{table}/tasks`（无 `namespaces/{namespace}` segment）

V4.2 设计时必须确认 Spark/Java client 实际请求路径，必要时同时支持两者作为 alias。

### 4.6 Transaction

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| POST | `/v1/{prefix}/transactions/commit` | 原子提交多个表更新 | V4.1 已实现 | 持续维护 |

### 4.7 View

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| GET | `/v1/{prefix}/namespaces/{namespace}/views` | 列出视图 | V4.2 已实现 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/views` | 创建视图 | V4.2 已实现 | 持续维护 |
| GET | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 加载视图 | V4.2 已实现 | 持续维护 |
| POST | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 替换视图 | V4.2 已实现 | 持续维护 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 删除视图 | V4.2 已实现 | 持续维护 |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 检查视图是否存在 | V4.2 已实现 | 持续维护 |
| POST | `/v1/{prefix}/views/rename` | 重命名视图 | V4.2 已实现 | 持续维护 |

说明：`POST /v1/{prefix}/namespaces/{namespace}/register-view` 不属于 Iceberg 1.10.x REST Catalog，若后续升级到 1.11+ 再重新评审。

### 4.8 S3 Signer API（非 REST Catalog）

| 方法 | 官方路径 | 能力 | V4.0 | 后续候选 |
|------|----------|------|------|----------|
| POST | `/v1/aws/s3/sign` | 远程签名 S3 请求 | 不实现 | 推迟到后续安全专项版本；V4.1 不实现 |

---

## 五、Lance REST Namespace 官方端点

> **V4 范围说明**: V4.0 维持 V3 现状，仅接受因共享数据模型重构带来的兼容性修复；不扩展新的 Lance 端点。

### 5.1 Namespace

| 方法 | 官方路径 | 能力 | V4.0 |
|------|----------|------|------|
| POST | `/v1/namespace/{id}/create` | 创建 Namespace | 维持 V3 现状 |
| GET | `/v1/namespace/{id}/list` | 列出子 Namespace | 维持 V3 现状 |
| POST | `/v1/namespace/{id}/describe` | 查询 Namespace 属性 | 维持 V3 现状 |
| POST | `/v1/namespace/{id}/drop` | 删除空 Namespace | 维持 V3 现状 |
| POST | `/v1/namespace/{id}/exists` | 检查 Namespace 是否存在 | 维持 V3 现状 |

### 5.2 Table / Version 已实现端点

| 方法 | 官方路径 | 能力 | V4.0 |
|------|----------|------|------|
| POST | `/v1/table/{id}/declare` | 声明表，保留名称和位置 | 维持 V3 现状 |
| GET | `/v1/namespace/{id}/table/list` | 列出 Namespace 下的表 | 维持 V3 现状 |
| POST | `/v1/table/{id}/describe` | 查询表 metadata，可指定版本 | 维持 V3 现状 |
| POST | `/v1/table/{id}/deregister` | 注销表但保留存储数据 | 维持 V3 现状 |
| POST | `/v1/table/{id}/drop` | 删除表及其数据 | 维持 V3 现状 |
| POST | `/v1/table/{id}/register` | 注册已有 Lance 表 | 维持 V3 现状 |
| POST | `/v1/table/{id}/rename` | 重命名表 | 维持 V3 现状 |
| POST | `/v1/table/{id}/exists` | 检查表是否存在 | 维持 V3 现状 |
| POST | `/v1/table/{id}/version/create` | 创建或注册表版本记录 | 维持 V3 现状 |
| GET | `/v1/table/{id}/version/list` | 列出表版本 | 维持 V3 现状 |
| POST | `/v1/table/{id}/version/describe` | 查询表版本 | 维持 V3 现状 |

Lance Table Schema / DML / Query / Index / Tag / Transaction 等 V3 未实现端点继续 defer 到后续主版本或专题版本。

---

## 六、Quasar 自定义管理 API（Unified Domain）

> 本章节端点为 **Quasar-specific**，不属于 Iceberg REST Catalog 或 Lance REST Namespace 官方协议。它们通过 Unified API (`/unified/v1`) 暴露，用于 Domain 生命周期管理。

| 方法 | 路径 | 能力 | V4.0 |
|------|------|------|------|
| GET | `/unified/v1/domains` | 列出所有 Domain | 维持 V3 现状 |
| POST | `/unified/v1/domains` | 创建 Domain | 维持 V3 现状 |
| GET | `/unified/v1/domains/{domain}` | 获取单个 Domain | 维持 V3 现状 |
| PATCH | `/unified/v1/domains/{domain}` | 更新 Domain | 维持 V3 现状 |
| DELETE | `/unified/v1/domains/{domain}` | 删除空 Domain | 维持 V3 现状 |

**约束**:

- `DELETE` 仅对空 Domain 生效；非空删除返回 `409 DomainNotEmpty`。
- `GET /unified/v1/domains/{domain}` 响应中的 `storage_config` 字段必须脱敏。
- 官方协议路径不得因 Domain、tenant、environment 等 Quasar 内部概念增加自定义 path segment。

---

## 七、维护规则

1. 修改 `V4_DESIGN.md` 中 REST API 范围时，必须同步更新本文档对应小版本列。
2. 引入 Iceberg 1.11+ 端点时，必须先在本文档新增官方基线说明，再进入需求或设计文档。
3. Quasar 自定义管理 API 必须明确标注为 Quasar-specific，不得混入官方协议端点表。
4. V4.0 只声明实际已实现 endpoint；候选小版本端点未实现前不得出现在 `/v1/config` 的 `endpoints` 中。
5. 本文档变更必须在末尾 "八、修订记录" 追加版本条目；版本号同步更新文件头部 metadata。

---

## 八、修订记录

### V1.7 (2026-06-08)

- §4.5 scan planning 端点：V4.0 列从"不实现"更新为"V4.2 已实现"，后续候选列更新为"持续维护"。
- §4.7 view 端点：V4.0 列从"不实现"更新为"V4.2 已实现"，后续候选列更新为"持续维护"。
- 文档头部状态从"V4.1 已实现"更新为"V4.2 已实现"。
- 同步更新文档头部版本号为 V1.7、日期为 2026-06-08。

### V1.6 (2026-06-06)

- §一 基线口径修正：`BaseUpdate` 从"23 table + 2 view"修正为"6 table/view 共用 + 2 view-only + 17 table-only"；`BaseRequirement` 从"8 table + 1 view"修正为"1 table/view 共用 + 1 view-only + 7 table-only"。此修正与 iceberg crate 0.9.1 的 `ViewUpdate` enum（8 个 variant）和 Iceberg 1.10.x OpenAPI 的 `BaseRequirement` discriminator（`assert-create` 对 table/view 共用）一致。
- §4.4 注释修正：从"本表列出 23 个 table 适用项"改为"本表列出 17 个 table-only 项 + 6 个 table/view 共用项"，明确共用项归属。
- 同步更新文档头部版本号为 V1.6、日期为 2026-06-06。

### V1.5 (2026-06-06)

- §4.5 scan planning 端点后续候选列调整：从"V4.2 候选"改为"V4.2 需实现"。
- §4.7 view 端点后续候选列调整：从"V4.2 候选"改为"V4.2 需实现"。
- 同步更新文档头部版本号为 V1.5、日期为 2026-06-06。

### V1.4 (2026-06-06)

- §4.6 transactions 端点：V4.0 列从"不实现"更新为"V4.1 已实现"，后续候选列更新为"持续维护"。
- 同步更新文档头部版本号为 V1.4、日期为 2026-06-06。

### V1.3 (2026-06-05)

- §4.3 credentials 端点后续候选列调整：从"V4.1 候选"改为"推迟到后续安全专项版本；V4.1 不实现"。
- §4.4 注释调整：明确 statistics 5 项归入 V4.1，encryption key 2 项推迟到安全专项版本。
- §4.8 S3 Signer 后续候选列调整：从"V4.1 可选"改为"推迟到后续安全专项版本；V4.1 不实现"。
- 同步更新文档头部版本号为 V1.3、日期为 2026-06-05。

### V1.2 (2026-05-19)

- §4.4 TableUpdate 列表补全两个 1.10.x table 适用 update：`set-partition-statistics`、`remove-partition-statistics`；并在 §4.4 末尾追加 ViewUpdate / V4.1 候选范围说明。
- §一 基线口径补充 1.10.x `BaseUpdate`（25 项：6 table/view 共用 + 2 view-only + 17 table-only）与 `BaseRequirement`（9 项：1 table/view 共用 + 1 view-only + 7 table-only）总数声明。
- §4.3 load_table 行措辞收紧为"维持 V3 行为；`If-None-Match` 头部、snapshot 参数化加载视为 V4.3 候选评估"。
- §4.5 scan planning 路径差异显式列出两条 alias（OpenAPI 路径 vs Java `ResourcePaths` 常量路径），便于 V4.2 设计直接采用。

### V1.1 (2026-05-18)

- 固定 Iceberg REST Catalog 官方基线为 Apache Iceberg 1.10.x 发布版，移除 `main` 分支作为 V4 必须实现基线。
- 将 V4 范围拆分为 V4.0 / V4.1 / V4.2 / V4.3，小版本职责分别对应 Spark-ready baseline、table capability completion、views/scan planning、operational hardening。
- 从 REST Catalog 主表移除非 1.10.x endpoint：`POST .../tables/{table}/sign` 与 `POST .../register-view`。
- 增加 S3 Signer API 独立章节，明确 `/v1/aws/s3/sign` 不属于 REST Catalog 主 OpenAPI。
- 修正 1.10.x `TableRequirement` 与 `TableUpdate` 官方集合，删除 `assert-last-sequence-number`、`add-partition-spec`、`remove-sort-orders` 等错误项，补充 `add-spec`、statistics、schema removal、encryption key actions。
- 明确 scan planning OpenAPI 路径与 Java ResourcePaths 常量存在兼容性差异，要求后续设计确认 alias 策略。

### V1.0 (2026-05-18)

- V4 基线文档建立，源自 `docs/v3/V3_OFFICIAL_REST_API.md` V1.2。
