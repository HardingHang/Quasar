# Quasar V4.2 需求分析文档

> **版本**: V1.3
> **日期**: 2026-06-06
> **状态**: 需求评审修订完成，待评审
> **定位**: Views and Scan Planning Candidate
>
> 本文档承担 V4.2 阶段的需求澄清与分析职责。
> 官方端点清单以 `docs/v4/V4_OFFICIAL_REST_API.md` 为唯一权威。
> 设计与实现细节由后续 `V4_2_DESIGN.md` 承接。
>
> **前置依赖**: V4.1 必须完成并通过验收；`V4_2_DESIGN.md` 必须完成并经评审后方可进入实现阶段。

---

## 一、文档边界

| 文档 | 职责 | 规则 |
|------|------|------|
| `README.md` | V4 文档入口与阅读顺序 | 不承载需求细节 |
| `V4_OFFICIAL_REST_API.md` | 官方协议基线、端点清单、小版本范围矩阵 | 不写 Quasar 内部实现方案 |
| `V4_REQUIREMENTS.md` | V4.0~V4.3 总需求基线 | 不写 DDL、具体 Rust 类型替换步骤 |
| `V4_1_REQUIREMENTS.md` | V4.1 需求基线 | 已完成 |
| **本文档** | V4.2 需求澄清与范围定义 | 不写具体实现方案 |
| `V4_2_DESIGN.md`（后续创建） | V4.2 设计方案 | 承接实现策略、数据模型、接口、错误映射、测试设计 |
| `PROGRESS.md` | V4 阶段进度 | V4.2 完成后更新 |

**前置条件**: 进入 V4.2 实现阶段前，V4.1 必须完成并通过验收；`V4_2_DESIGN.md` 必须完成并经评审批准。

---

## 二、前置依赖与准入条件

### 2.1 前置依赖

| 依赖项 | 要求 |
|--------|------|
| V4.1 验收 | 必须完成并通过全部验收标准 |
| `V4_2_DESIGN.md` | 必须完成并经评审批准，方可进入实现阶段 |
| 测试基础设施 | 沿用 V4.0/V4.1 的容器化 Postgres 环境，支持并发测试 |
| Spark 环境 | 验证 View 端点需要 Spark 3.5+ 支持 Iceberg View（Iceberg 1.10.x 已支持） |

### 2.2 准入条件

V4.2 的准入条件：

1. V4.1 完成并通过验收
2. `V4_2_DESIGN.md` 完成，包含 View 生命周期方案、Scan Planning 执行设计、路径兼容 alias 设计
3. 测试矩阵与验收清单明确定义
4. 确认 Spark Iceberg 1.10.x 对 View 端点的实际调用行为（通过集成测试环境验证）

---

## 三、V4.2 定位与范围决策

### 3.1 范围决策背景

V4.2 是 V4 系列的第三个小版本，在 V4.0 "Spark-ready baseline" 和 V4.1 "Table capability completion" 基础上，补齐 Iceberg REST Catalog 的 **View 生命周期管理** 和 **Server-side Scan Planning** 能力。

### 3.2 V4.2 最终范围

| 类别 | 功能 | 说明 |
|------|------|------|
| **Iceberg View 生命周期** | 7 个端点 | list / create / load / replace / drop / head / rename |
| **Server-side Scan Planning** | 4 个端点 | submit plan / fetch plan / cancel plan / fetch tasks |

### 3.3 V4.2 明确不做

1. **`register-view` 端点** — 不属于 Iceberg 1.10.x REST Catalog 官方端点集合，若后续 Iceberg 1.11+ 引入再评估。
2. **View Version 历史深度管理** — 仅支持当前 view version 加载和 replace，不提供历史 version 回滚或审计 API。
3. **Scan Planning 结果持久化** — Plan 结果和 FileScanTask 列表仅在请求会话内有效，不持久化到 PostgreSQL。
4. **Scan Planning 调度优化** — 不引入任务队列、并行度控制、资源限制等高级调度能力。
5. **View 与 Table 的跨资源引用验证** — View SQL 中引用的 Table 存在性验证由客户端负责，服务端不强制校验。

---

## 四、功能详细需求

### 4.1 Iceberg View 生命周期

#### 4.1.1 协议端点

Iceberg 1.10.x REST Catalog 定义以下 View 端点：

| 方法 | 官方路径 | 能力 |
|------|----------|------|
| GET | `/v1/{prefix}/namespaces/{namespace}/views` | 列出 Namespace 下的所有 View |
| POST | `/v1/{prefix}/namespaces/{namespace}/views` | 创建 View |
| GET | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 加载 View metadata |
| POST | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 替换 View（commit update） |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 删除 View |
| HEAD | `/v1/{prefix}/namespaces/{namespace}/views/{view}` | 检查 View 是否存在 |
| POST | `/v1/{prefix}/views/rename` | 重命名 View |

**说明**：`POST /v1/{prefix}/namespaces/{namespace}/register-view` 不属于 Iceberg 1.10.x 官方端点。

#### 4.1.2 View Metadata 结构

View metadata（`ViewMetadata`）遵循 Iceberg Table Format Spec V2 的 View 定义：

```json
{
  "view-uuid": "uuid-string",
  "format-version": 1,
  "location": "s3://bucket/path/to/view/",
  "current-version-id": 1,
  "versions": [
    {
      "version-id": 1,
      "timestamp-ms": 1234567890000,
      "schema-id": 0,
      "default-namespace": ["namespace"],
      "representations": [
        { "type": "sql", "sql": "SELECT * FROM table", "dialect": "spark" }
      ]
    }
  ],
  "version-log": [
    { "version-id": 1, "timestamp-ms": 1234567890000 }
  ],
  "schemas": [
    { "schema-id": 0, "type": "struct", "fields": [...] }
  ],
  "properties": {}
}
```

**注意**：`metadata-location` 不属于 ViewMetadata JSON 文件的内部字段，它是 REST API 响应（`GetViewResponse`）的外部字段，指向存储该 JSON 文件的对象存储路径。这与 Table 的 `LoadTableResponse` 设计一致。

关键结构说明：

| 字段 | 类型 | 描述 |
|------|------|------|
| `view-uuid` | UUID string | View 全局唯一标识 |
| `format-version` | int | View format 版本（当前仅支持 `1`） |
| `location` | string | View 根路径，用于存储 metadata 文件 |
| `current-version-id` | int | 当前活跃 View Version ID |
| `versions` | array of ViewVersion | View Version 列表，受 `version.history.num-entries` 属性控制数量上限 |
| `version-log` | array of ViewHistoryEntry | Version 切换历史记录，每次 `setCurrentVersion` 操作追加一条 |
| `schemas` | array of Schema | View 输出 schema 定义 |
| `properties` | map | View 属性（包含 `version.history.num-entries`） |

**ViewVersion 结构**：

```json
{
  "version-id": int,
  "timestamp-ms": int64,
  "schema-id": int,
  "default-namespace": ["namespace"],
  "default-catalog": "catalog" (optional),
  "representations": [
    { "type": "sql", "sql": "SELECT ...", "dialect": "spark" }
  ],
  "summary": { "app-name": "...", "user": "..." }
}
```

| 字段 | 类型 | 描述 |
|------|------|------|
| `version-id` | int | Version ID（单调递增） |
| `timestamp-ms` | int64 | 创建时间戳 |
| `schema-id` | int | 对应 `schemas` 中的 schema ID |
| `default-namespace` | array of string | SQL 解析时的默认 namespace 上下文 |
| `default-catalog` | string (optional) | SQL 解析时的默认 catalog |
| `representations` | array | SQL 表示形式列表，支持多 dialect |
| `summary` | map | 创建操作的元信息（app-name、user 等） |

**ViewHistoryEntry 结构**：

```json
{
  "version-id": int,
  "timestamp-ms": int64
}
```

记录每次 Version 切换的时间和目标 Version ID，用于审计追踪。

**Version History 管理**:

- Iceberg 1.10.x 已支持自动清理过期 Version
- 属性 `version.history.num-entries` 控制保留数量，默认 `10`
- 清理时按 `version-id` 降序保留，同时保证当前版本一定存在
- `version-log` 同步清理已过期版本的历史条目

#### 4.1.3 功能性要求

**Create View** (`POST .../views`):

- 接收 `CreateViewRequest`（Iceberg OpenAPI 定义的独立请求体），包含 `name`、`location`、`schema`、`sql` / `representations`、`default-namespace`、`default-catalog`（optional）、`properties`。
- Iceberg 1.10.x REST Catalog 中，Create View 端点有独立的 `CreateViewRequest` 结构（与 Replace View 的 `CommitViewRequest` 不同）。内部处理时，Quasar 将 `CreateViewRequest` 的字段转换为 `ViewCreation`（iceberg crate 0.9.1 类型），通过 `ViewMetadataBuilder::from_view_creation()` 构建 ViewMetadata。"View 不存在时才允许创建"的 `assert-create` 语义由 adapter 层在调用 Store 前校验（检查 View 是否已存在），而非显式构造 ViewRequirement。转换逻辑由 `V4_2_DESIGN.md` 定义。
- 生成 `view-uuid`、初始化 `current-version-id=1`。
- 构建 `ViewMetadata` 并写入对象存储。
- 在 PostgreSQL 注册 View 记录（`assets` + 新增 `view_assets` 表，参见 §六）。需同时检查同一 Namespace 下不存在同名 Table（参见 §4.1.5 错误处理）。
- 返回 `GetViewResponse`（包含完整 metadata 和 `metadata-location`）。

**Load View** (`GET .../views/{view}`):

- 通过 PostgreSQL 查询 View 记录，获取 `metadata_location`。
- 从对象存储读取 `ViewMetadata` JSON。
- 返回 `GetViewResponse`。

**Replace View** (`POST .../views/{view}`):

- 接收 `CommitViewRequest`，包含 `requirements` 和 `updates`。
- View commit 机制与 Table commit 类似，采用 CAS 模式。
- **View requirement 校验在 adapter 层执行**（与 Table requirement 一致），Store 层仅负责 CAS `metadata_location` 更新。注意：`ViewRequirement` 类型不存在于 iceberg crate 0.9.1（只有 `TableRequirement`），V4.2 在 adapter 层自定义 `ViewRequirement` enum 并手动实现校验逻辑，与 Table 使用 crate 内置 `TableRequirement::check()` 的模式不同。
- 应用 updates 后生成新 `ViewMetadata`，写入对象存储。
- CAS 更新 PostgreSQL `metadata_location`。

**Drop View** (`DELETE .../views/{view}`):

- 删除 PostgreSQL View 记录。
- 不清理对象存储 metadata 文件（View metadata 文件数量远少于 Table，清理推迟到 V4.3）。
- 返回 `204 No Content`。

**Head View** (`HEAD .../views/{view}`):

- 仅检查 PostgreSQL View 记录存在性。
- 返回 `200 OK`（无 body）或 `404`。

**List Views** (`GET .../views`):

- 返回指定 Namespace 下的 View 名称列表。
- 与 `List Tables` 格式一致：`{"identifiers": [{"namespace": [...], "name": "..."}]}`。

**Rename View** (`POST .../views/rename`):

- 接收 `source` 和 `destination` identifier。
- 在 PostgreSQL 事务中更新 View 的 `namespace_name` 和 `name`。
- 返回 `204 No Content` 或 `404`（source 不存在）。

#### 4.1.4 View Commit Requirements/Updates

**View Requirements**（Iceberg 1.10.x）：

| Requirement | Wire Name | 适用范围 | 描述 |
|-------------|-----------|----------|------|
| AssertCreate | `assert-create` | Table + View 共用 | View 不存在时才允许创建（与 Table `assert-create` 语义一致） |
| AssertViewUUID | `assert-view-uuid` | View-only | 校验 View UUID 匹配 |

**说明**：Iceberg 1.10.x `BaseRequirement` 共 9 项，其中 `assert-create` 是 Table 和 View 共用的 requirement，`assert-view-uuid` 是 View-only requirement。其余 7 项 requirement（如 `assert-table-uuid`、`assert-ref-snapshot-id` 等）仅适用于 Table commit。

**View Updates**（Iceberg 1.10.x）：

Iceberg 1.10.x `BaseUpdate` discriminator 中 View 适用的 update 共 **8 项**（2 项 View-only + 6 项 Table/View 共用）：

| Update | Wire Name | 适用范围 | 描述 |
|--------|-----------|----------|------|
| AssignUuid | `assign-uuid` | Table + View 共用 | 分配 View UUID |
| UpgradeFormatVersion | `upgrade-format-version` | Table + View 共用 | 升级 View format version（注：View format-version 当前仅支持 V1，此 update 无实际升级目标，但 wire name 与 Table 共用） |
| AddSchema | `add-schema` | Table + View 共用 | 添加新 Schema 到 View |
| SetLocation | `set-location` | Table + View 共用 | 更新 View 根路径 |
| SetProperties | `set-properties` | Table + View 共用 | 设置 View 属性 |
| RemoveProperties | `remove-properties` | Table + View 共用 | 移除 View 属性 |
| AddViewVersion | `add-view-version` | View-only | 添加新的 View Version |
| SetCurrentViewVersion | `set-current-view-version` | View-only | 设置当前 View Version ID |

**说明**：以上 8 项与 iceberg crate 0.9.1 `ViewUpdate` enum 的 variant 一致。V4.2 实现策略参照 V4.0/V4.1 对 TableUpdate 的分层方式——V4.2 必须实现 Spark Iceberg 1.10.x View 端点 E2E 会触发的全部 ViewUpdate；对已知但暂未支持的 update 必须返回明确 501 错误，不得静默忽略。具体实现清单由 `V4_2_DESIGN.md` 定义。

#### 4.1.5 错误处理

| 场景 | 响应 | Iceberg error type |
|------|------|--------------------|
| View 不存在 | 404 | `NoSuchViewException` |
| View 已存在 | 409 | `ViewAlreadyExistsException` |
| Namespace 下存在同名 Table | 409 | `AlreadyExistsException`（"Table with same name already exists"） |
| Namespace 不存在 | 404 | `NoSuchNamespaceException` |
| Requirement 不满足 | 409 | `CommitFailedException` |
| View UUID 不匹配 | 409 | `CommitFailedException` |
| Rename destination 已存在 | 409 | `ViewAlreadyExistsException` |

#### 4.1.6 验收标准

- [ ] Create View 成功，返回完整 ViewMetadata。
- [ ] Load View 成功，metadata-location 与创建时一致。
- [ ] Replace View 成功，`current-version-id` 递增。
- [ ] Drop View 成功，PostgreSQL 记录删除。
- [ ] List Views 返回正确名称列表。
- [ ] Rename View 成功，跨 Namespace rename 验证。
- [ ] Head View 存在性检查正确。
- [ ] View commit CAS 冲突测试（并发 replace）。
- [ ] Spark Iceberg 1.10.x 通过 View 端点创建和读取 View（E2E smoke）。

---

### 4.2 Server-side Scan Planning

#### 4.2.1 协议端点

Iceberg 1.10.x REST Catalog 定义以下 Scan Planning 端点：

| 方法 | 官方路径 | 能力 |
|------|----------|------|
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | 提交 Scan Planning 请求 |
| GET | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 获取 Plan 结果 |
| DELETE | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | 取消 Plan |
| POST | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | 拉取 FileScanTask 列表 |

#### 4.2.2 路径兼容性问题

**OpenAPI vs Java ResourcePaths 差异**：

| 端点 | 官方路径（OpenAPI） | Java ResourcePaths 常量路径 |
|------|--------------------|----------------------------|
| Submit Plan | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | `/v1/{prefix}/tables/{table}/plan` |
| Fetch Plan | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | `/v1/{prefix}/tables/{table}/plan/{plan-id}` |
| Cancel Plan | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | `/v1/{prefix}/tables/{table}/plan/{plan-id}` |
| Fetch Tasks | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | `/v1/{prefix}/tables/{table}/tasks` |

**差异说明**：Java `ResourcePaths` 常量路径**不包含** `namespaces/{namespace}` segment，与 OpenAPI 定义不一致。

**V4.2 设计要求**：必须确认 Spark/Java client 实际请求路径。若 Java client 使用 `ResourcePaths` 常量路径，Quasar 必须同时支持两种路径作为 alias。

**设计方案选项**（由 `V4_2_DESIGN.md` 选择）：

1. **Option A**: 主路径使用 OpenAPI 路径，Java 常量路径作为 alias（路由层兼容）。**推荐**——与官方 OpenAPI 一致，后续非 Java 客户端（PyIceberg 等）也使用 OpenAPI 路径。
2. **Option B**: 主路径使用 Java 常量路径，OpenAPI 路径作为 alias。**风险**：与官方 OpenAPI 定义不一致，后续其他客户端可能使用 OpenAPI 路径而非 Java 常量路径。
3. **Option C**: 仅支持 OpenAPI 路径，客户端需适配。**风险**：Java client 使用 ResourcePaths 常量路径时请求会 404，需 Java client 方配合修改。

推荐 Option A（官方 OpenAPI 优先）。

#### 4.2.3 Scan Planning 流程

```
Client                      Quasar Server                Object Store
  |                              |                             |
  | POST /plan (submitPlanRequest)|                             |
  |----------------------------->|                             |
  |                              | Load TableMetadata          |
  |                              |---------------------------> |
  |                              |                             |
  |                              | Plan FileScanTask splits    |
  |                              | (server-side computation)   |
  |                              |                             |
  | Return plan-id + plan-task   |                             |
  |<-----------------------------|                             |
  |                              |                             |
  | POST /tasks (plan-task token)|                             |
  |----------------------------->|                             |
  |                              |                             |
  |                              | Return FileScanTask[]       |
  |<-----------------------------|                             |
  |                              |                             |
```

**关键数据结构**：

**SubmitPlanRequest**:

```json
{
  "snapshot-id": int64,
  "filter": {...},
  "case-sensitive": bool,
  "split-size": int64,
  "num-splits": int,
  "columns": [...],
  "start-snapshot-id": int64 (optional),
  "end-snapshot-id": int64 (optional)
}
```

**SubmitPlanResponse**:

```json
{
  "plan-id": "uuid-string",
  "plan-task": "base64-encoded-token"
}
```

**FetchPlanResultResponse**:

```json
{
  "status": "pending" | "completed" | "failed",
  "plan-id": "uuid-string",
  "plan-task": "base64-encoded-token" (if completed),
  "error": {...} (if failed)
}
```

**FileScanTask**:

```json
{
  "file": {
    "file-path": "string",
    "file-format": "string",
    "partition": {...},
    "record-count": int64,
    "file-size-in-bytes": int64,
    ...
  },
  "start": int64,
  "length": int64
}
```

#### 4.2.4 功能性要求

**Submit Plan**:

- 校验 Table 存在且 format 为 `iceberg`。
- 读取 TableMetadata，定位指定 `snapshot-id` 的 Manifests。
- 根据 `filter`、`split-size`、`columns` 等参数生成 FileScanTask splits。
- 生成 `plan-id`（UUID）和 `plan-task` token（用于后续 `/tasks` 请求）。
- Plan 结果仅在会话内有效，不持久化到 PostgreSQL（V4.2 简化实现）。
- 返回 `SubmitPlanResponse`。

**Fetch Plan Result**:

- 根据 `plan-id` 查询 Plan 状态。
- 状态：`pending`（仍在处理）、`completed`（完成）、`failed`（失败）。
- 完成时返回 `plan-task` token。

**Cancel Plan**:

- 取消指定 `plan-id` 的 Plan（如果仍在处理）。
- 返回 `204 No Content`。

**Fetch Tasks**:

- 接收 `plan-task` token。
- 解析 token，返回 `FileScanTask[]` 列表。
- **`plan-task` token 是自包含的**（base64 encoded JSON），包含 Plan 执行所需的所有参数（snapshot-id、filter、split-size 等）和 FileScanTask splits 结果摘要。服务端收到 token 后根据其内容返回 FileScanTask 列表，具体实现策略（token 内编码完整 task 列表 vs. token 仅包含参数让服务端重新计算 vs. 服务端内存暂存结果、token 仅包含 plan-id）由 `V4_2_DESIGN.md` 选定。V4.2 的功能性约束是：**Plan 结果仅在请求会话内有效，不持久化到 PostgreSQL**。

#### 4.2.5 错误处理

| 场景 | 响应 | Iceberg error type |
|------|------|--------------------|
| Namespace 不存在 | 404 | `NoSuchNamespaceException` |
| Table 不存在 | 404 | `NoSuchTableException` |
| Snapshot 不存在 | 404 | `NoSuchSnapshotException` |
| Plan 不存在 | 404 | 无特定类型，返回 404 |
| Plan 已取消或过期 | 404 | 无特定类型，返回 404 |
| Invalid plan-task token | 400 | `BadRequestException` |

#### 4.2.6 验收标准

- [ ] Submit Plan 成功，返回 `plan-id` 和 `plan-task`。
- [ ] Fetch Tasks 成功，返回 `FileScanTask[]`。
- [ ] Cancel Plan 成功，已取消 Plan 返回 404。
- [ ] Filter 条件正确应用到 Manifest 扫描。
- [ ] Split size 参数正确控制 FileScanTask 粒度。
- [ ] 路径 alias 兼容性测试（OpenAPI 路径 + Java 常量路径）。
- [ ] Spark Iceberg 1.10.x scan planning E2E smoke（若 Spark 实际调用）。

---

## 五、非功能要求

- V4.2 继续不引入生产鉴权语义。
- 客户端错误响应继续遵循 Iceberg error model，敏感信息脱敏。
- 生产代码不得使用 `unwrap()` / `expect()`。
- SQL 继续集中在 storage 查询模块，禁止字符串拼接 SQL。
- 新增或修改测试后同步维护 `docs/TEST_MATRIX.md`。
- `/v1/config` 的 `endpoints` 字段必须按实际已实现能力逐步增加，不得提前声明未实现 endpoint。
- View metadata 文件必须能被 Spark Iceberg 1.10.x client 读回。
- Quasar 写出的 View metadata JSON 必须符合 Iceberg View Format Spec V1 的序列化规范（`ViewRepresentation` 的 `type` discriminator 字段不可缺失），与 Table metadata 的序列化兼容性要求对齐。
- Scan Planning 的 `plan-task` token 必须是自包含的（base64 encoded JSON），避免服务端状态管理复杂度。

---

## 六、数据模型增量（候选）

### 6.1 View Assets 表

**设计决策**：View 是 **Iceberg 特有概念**（Lance REST Namespace 无 View 端点），因此：

- View 作为独立的 `asset_type`（与 Table 平行）
- `view_assets` 扩展表无需 `format` 字段（只有 Iceberg 有 View）
- `assets` 表 UNIQUE 约束自动防止 Table/View 同名冲突

**数据模型变更**：

```sql
-- 1. asset_types registry 扩展
INSERT INTO asset_types (name, comment)
VALUES ('view', 'Iceberg View')
ON CONFLICT DO NOTHING;

-- 2. view_assets 扩展表（Iceberg-only）
CREATE TABLE IF NOT EXISTS view_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    -- 无 format 字段，因为 View 只有 Iceberg 支持
    view_uuid UUID NOT NULL,
    location TEXT NOT NULL,
    current_version_id INT NOT NULL,
    metadata_location TEXT NOT NULL,
    properties JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_view_assets_uuid ON view_assets(view_uuid);

-- 3. Trigger: 确保 view_assets 对应 asset_type='view'
CREATE OR REPLACE FUNCTION ensure_view_asset_type()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM assets
        WHERE id = NEW.asset_id
          AND asset_type = 'view'
    ) THEN
        RAISE EXCEPTION 'view asset % must reference an asset with asset_type=view', NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_view_assets_type_check ON view_assets;
CREATE TRIGGER trg_view_assets_type_check
BEFORE INSERT OR UPDATE OF asset_id ON view_assets
FOR EACH ROW
EXECUTE FUNCTION ensure_view_asset_type();
```

**设计规则**：

- `assets.asset_type = 'view'` 触发 `view_assets` 扩展记录创建。
- `assets` 表现有的 UNIQUE 约束 `(namespace_id, name) WHERE deleted_at IS NULL` 自动防止同名 Table/View 冲突（Iceberg 语义）。
- `view_assets` 无 `format` 字段，因为 Lance 不支持 View。
- `view_uuid` 存储 Iceberg ViewMetadata 中的 UUID，用于 CAS commit 校验。
- `metadata_location` 指向对象存储中的当前 View metadata 文件。
- `current_version_id` 存储 View 创建时的初始 version ID（值为 1）。**注意**：此字段仅作为创建时快照，replace commit 后的实际 `current-version-id` 从对象存储 ViewMetadata JSON 中读取，Store 层的 `commit_view` 方法不更新此字段（与 `tabular_assets` 的 `schema_snapshot` 字段性质类似——快照性质、不保证实时一致）。V4.2 不在 `commit_view` 中同步更新此字段；adapter 层 Load View 时总是从对象存储读取完整 metadata 获取最新 `current-version-id`。此字段的用途仅限于：
  - 初始化时确认 View 已有至少一个 version；
  - 未来可能的快速 version 变化检测（对比 DB 快照值 vs 对象存储实际值）。
- View metadata JSON 不存储在 PostgreSQL（与 Table 一致，仅存 location）。

**与 tabular_assets 的对比**：

| 表 | format 字段 | 原因 |
|----|------------|------|
| `tabular_assets` | ✅ 有 (`iceberg`/`lance`) | Table 有多种格式 |
| `view_assets` | ❌ 无 | View 当前只有 Iceberg |

**格式扩展预留**：若后续 Iceberg 版本（如 1.11+）或 Lance 引入 View 格式变体，`view_assets` 可能需要添加 `format` 字段（与 `tabular_assets` 对称），届时需评估 schema migration。V4.2 不预先添加此字段（YAGNI 原则）。

### 6.2 无 Scan Planning 持久化表

Scan Planning 结果不持久化到 PostgreSQL，仅在请求会话内有效：

- `plan-task` token 是自包含的（base64 encoded JSON），具体内容结构由 `V4_2_DESIGN.md` 选定实现策略后定义（参见 §10.2.3 三种策略）。
- 服务端无需在 PostgreSQL 维护 Plan 状态表。
- 若后续需要 Plan 状态持久化或异步调度，推迟到 V4.3+。

---

## 七、Store trait 增量（候选）

### 7.1 新增 View Store Trait

```rust
// quasar/core/src/store.rs

/// Iceberg View 生命周期管理（V4.2）。
///
/// View requirement 校验在 adapter 层执行（与 Table requirement 一致），
/// Store 层仅负责 View 身份管理和 CAS metadata_location 更新。
#[async_trait]
pub trait IcebergViewStore: AssetStore {
    /// 创建 View（插入 assets + view_assets 双行）。
    ///
    /// `view_uuid` 由 adapter 层生成后传入。
    /// `current_version_id` 初始值为 1（在 adapter 层设置，Store 层仅持久化）。
    async fn create_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        view_uuid: Uuid,
        location: &str,
        metadata_location: &str,
        current_version_id: i32,
        properties: serde_json::Value,
    ) -> Result<View, StoreError>;

    /// 加载 View 记录（读取 assets + view_assets 联合行）。
    async fn get_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<View, StoreError>;

    /// CAS 更新 View metadata_location。
    ///
    /// requirement 校验已在 adapter 层完成，Store 层仅执行 CAS 更新。
    /// 如需同时更新 view_assets 中的 current_version_id 等字段，
    /// 由 V4_2_DESIGN.md 决定是否在此方法中添加额外参数。
    async fn commit_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
        expected_location: &str,
        new_location: &str,
    ) -> Result<(), StoreError>;

    /// 删除 View（依赖 assets FK cascade 自动删除 view_assets）。
    async fn drop_view(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<(), StoreError>;

    /// 列出 Namespace 下所有 View。
    async fn list_views(
        &self,
        domain_name: &str,
        namespace_name: &str,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<ViewIdentifier>, StoreError>;

    /// 重命名 View（复用 AssetStore::rename_asset 逻辑 + view_assets 同步）。
    async fn rename_view(
        &self,
        source_domain: &str,
        source_namespace: &str,
        source_name: &str,
        dest_domain: &str,
        dest_namespace: &str,
        dest_name: &str,
    ) -> Result<(), StoreError>;

    /// 检查 View 是否存在（查询 assets 表 asset_type='view'）。
    async fn view_exists(
        &self,
        domain_name: &str,
        namespace_name: &str,
        view_name: &str,
    ) -> Result<bool, StoreError>;
}

// CatalogStore marker trait 扩展（V4.2）
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
    + IcebergTransactionStore
    + IcebergViewStore  // V4.2 新增
    + Send
    + Sync
{
}
```

**新增类型说明**：

- `View`：需要在 `quasar/core/src/models.rs` 中新增的 model 类型，表示 `(Asset, ViewAsset)` 联合行的投影（与 `TabularAsset` 是 `(Asset, TabularAsset)` 联合行投影的模式一致）。具体字段由 `V4_2_DESIGN.md` 定义。
- `ViewAsset`：需要在 `quasar/core/src/models.rs` 中新增的 model 类型，对应 `view_assets` 表行（与 `TabularAsset` 对应 `tabular_assets` 表的模式一致）。
- `ViewIdentifier`：需要新增的 view 标识类型（结构为 `{ namespace: Vec<String>, name: String }`），语义上与 `TableIdentifier` 相同但类型独立，避免 Table/View 标识混用。

**super-trait 选择说明**：

`IcebergViewStore: AssetStore`（而非 `IcebergViewStore: CatalogStore`）——与 `IcebergPurgeStore: TabularStore` 的模式对齐，最小化 trait 约束。View 通过 `assets` 表管理身份，`AssetStore` 已提供 `create_asset`、`drop_asset`、`rename_asset`、`asset_exists` 等基础方法，`IcebergViewStore` 仅需补充 View 特有的扩展层操作。

### 7.2 Scan Planning 无 Store Trait

Scan Planning 不涉及 PostgreSQL 持久化，所有逻辑在 Adapter 层完成：

- `plan-task` token 是自包含的。
- Adapter handler 直接读取 TableMetadata（通过对象存储）。
- 无需新增 Store trait 方法。

---

## 八、验收清单

V4.2 完成条件：

**前置条件检查**：
- [ ] V4.1 已完成并通过全部验收标准。
- [ ] `V4_2_DESIGN.md` 已完成并经评审批准。

**View 功能验收**：
- [ ] 7 个 View 端点实现并通过集成测试（create / load / replace / drop / list / head / rename）。
- [ ] View commit CAS 冲突测试通过。
- [ ] View metadata 文件能被 Spark Iceberg 1.10.x client 读回。
- [ ] Spark E2E View smoke 测试通过（CREATE VIEW / SELECT FROM VIEW）。

**Scan Planning 功能验收**：
- [ ] Submit Plan / Fetch Plan / Cancel Plan / Fetch Tasks 4 个端点实现并通过测试。
- [ ] 路径 alias 兼容性测试（OpenAPI 路径 + Java ResourcePaths 常量路径）。
- [ ] Filter 和 Split size 参数正确应用。
- [ ] `plan-task` token 自包含，无服务端状态依赖。

**文档与配置同步**：
- [ ] `V4_OFFICIAL_REST_API.md` 的 V4.2 范围列已同步更新。
- [ ] `/v1/config` 实际 `endpoints` 输出包含 View 和 Scan Planning 端点声明。
- [ ] `docs/TEST_MATRIX.md` 已同步记录新增测试。
- [ ] `V4_2_DESIGN.md` 和 `PROGRESS.md` 已按实际实现结果更新。

---

## 九、与 V4 总需求的对应关系

| V4_REQUIREMENTS.md 原始条目 | V4.2 处理方式 |
|---------------------------|--------------|
| §5.2 Iceberg View 生命周期（7 端点） | ✅ 纳入 |
| §5.2 Server-side Scan Planning（4 端点） | ✅ 纳入 |
| §5.2 OpenAPI vs Java ResourcePaths 兼容性 | ✅ 纳入，设计 alias |
| §5.2 `register-view` | ❌ 不纳入（不属于 1.10.x 官方端点） |
| §5.3 Metadata cache | ❌ 不纳入，推迟到 V4.3 |
| §5.3 Cursor pagination | ❌ 不纳入，推迟到 V4.3 |
| §5.3 Orphan cleanup | ❌ 不纳入，推迟到 V4.3 |

---

## 十、已澄清结论

> 以下问题已通过源码调研和文档查阅确认，结论纳入 V4.2 设计约束。

### 10.1 View 相关

#### 10.1.1 Spark Iceberg View E2E 支持

**结论**: ✅ **Spark 3.5 + Iceberg 1.10.x 已支持 REST Catalog View 端点**。

- SparkSessionCatalog 实现 `ViewCatalog` 接口（Spark Connector V2 Catalog API）。
- `SparkCatalog` 同时实现 `ViewCatalog`，支持 `createView`、`dropView`、`listViews`、`loadView` 等操作。
- V4.2 View 端点实现后可通过 Spark E2E 验证（`CREATE VIEW` / `SELECT FROM VIEW`）。

**验证来源**: `SparkSessionCatalog.java`（Iceberg 1.10.0）line ~40-60：

```java
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import ...
    extends BaseCatalog implements CatalogExtension {
    private ViewCatalog asViewCatalog = null;
    ...
    if (icebergCatalog instanceof ViewCatalog) {
        this.asViewCatalog = (ViewCatalog) icebergCatalog;
    }
```

#### 10.1.2 View Version History 管理

**结论**: ✅ **Iceberg 1.10.x 已支持 View Version History 管理，默认保留最近 10 个版本**。

- **配置属性**: `version.history.num-entries`（View 属性）
- **默认值**: `10`（`ViewProperties.VERSION_HISTORY_SIZE_DEFAULT`）
- **清理算法**: `ViewMetadata.expireVersions()` 按 version-id 降序保留 N 个版本，同时保证当前版本一定保留
- **历史记录同步**: `version-log`（`ViewHistoryEntry`）同步清理已过期版本条目

**验证来源**: `ViewProperties.java` + `ViewMetadata.java`（Iceberg 1.10.0）：

```java
// ViewProperties.java
public static final String VERSION_HISTORY_SIZE = "version.history.num-entries";
public static final int VERSION_HISTORY_SIZE_DEFAULT = 10;

// ViewMetadata.java - build() 方法中的清理逻辑
int historySize = PropertyUtil.propertyAsInt(
    properties, ViewProperties.VERSION_HISTORY_SIZE, ViewProperties.VERSION_HISTORY_SIZE_DEFAULT);

if (versions.size() > numVersionsToKeep) {
    retainedVersions = expireVersions(versionsById, numVersionsToKeep, currentVersion);
    retainedHistory = updateHistory(history, retainedVersionIds);
}
```

**V4.2 设计决策**:
- 采用 Iceberg 默认值 `historySize=10`
- `set-properties` / `remove-properties` actions 已覆盖，客户端可自定义 `version.history.num-entries`
- **Version History 清理由 iceberg crate 0.9.1 内置处理**：`ViewMetadataBuilder::build()` 在构建完成时自动调用 `expire_versions()`，V4.2 无需自行实现此逻辑

#### 10.1.3 View 与 Table 同名冲突

**结论**: ✅ **Iceberg 禁止 View 和 Table 在同一 Namespace 下同名**。

- `RESTViewBuilder.replace()` 在创建 View 时显式检查同名 Table 存在性，若存在抛出 `AlreadyExistsException`。
- 反之，创建 Table 时同样需检查同名 View 存在性（客户端责任或服务端校验）。
- **V4.2 数据模型约束**: `assets` 表必须禁止同一 `(domain, namespace, name)` 同时存在 `tabular_assets` 和 `view_assets` 记录。

**验证来源**: `RESTSessionCatalog.java`（Iceberg 1.10.0）RESTViewBuilder 内部类：

```java
@Override
public View replace() {
    if (tableExists(context, identifier)) {
        throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
    }
    return replace(loadView());
}
```

**数据模型约束**: 现有 `assets` 表的 `uq_assets_active_name` partial unique index 已保证同一 `(namespace_id, name)` 下 `deleted_at IS NULL` 的记录唯一，无论 `asset_type` 是 `table` 还是 `view`。因此 **不需要新增额外的 UNIQUE 约束**，现有约束已足够防止 Table/View 同名冲突。

```sql
-- 现有约束（init.sql 中已定义）：
CREATE UNIQUE INDEX uq_assets_active_name
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;
```

由于 Iceberg 明确禁止同名，V4.2 依赖 **现有 `uq_assets_active_name` 索引**（无需区分 format 或新增 constraint）来保证同名冲突约束。

### 10.2 Scan Planning 相关

#### 10.2.1 Spark Scan Planning 调用行为

**结论**: ⚠️ **Spark Iceberg 1.10.x 默认使用 client-side planning，REST scan planning 为可选特性**。

- Spark connector 的 `SparkScanBuilder.buildIcebergBatchScan()` 调用 `scan.planFiles()`（client-side）。
- Iceberg 提供 `PlanningMode` 配置：`LOCAL`（client-side）、`DISTRIBUTED`（server-side）、`AUTO`。
- 表属性 `read.data-planning-mode` / `read.delete-planning-mode` 可配置 planning 模式。
- **默认行为**: Spark 不主动调用 REST `/plan` 端点，需客户端显式配置 `distributed` planning mode。

**验证来源**:
- `TableProperties.java`: `DATA_PLANNING_MODE = "read.data-planning-mode"`
- `PlanningMode.java`: `LOCAL`、`DISTRIBUTED`、`AUTO` 三种模式
- `SparkScanBuilder.java`: 调用 `scan.planFiles()`（本地 planning）

**V4.2 决策**: 实现 Scan Planning 端点但不强制 E2E 测试；通过协议测试覆盖功能正确性。若后续 Spark 配置分布式 planning 后可补充 E2E 验证。

#### 10.2.2 Java ResourcePaths 路径差异

**结论**: ✅ **Java ResourcePaths 常量路径不包含 `namespaces/{namespace}` segment**。

| 端点 | 官方路径（OpenAPI） | Java ResourcePaths 常量 |
|------|--------------------|------------------------|
| Submit Plan | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan` | `/v1/{prefix}/tables/{table}/plan` |
| Fetch Plan | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}` | `/v1/{prefix}/tables/{table}/plan/{plan-id}` |
| Cancel Plan | 同 Fetch Plan | 同 Fetch Plan |
| Fetch Tasks | `/v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks` | `/v1/{prefix}/tables/{table}/tasks` |

**验证来源**: `ResourcePaths.java`（Iceberg 1.10.0）常量定义：

```java
public static final String V1_TABLE_SCAN_PLAN_SUBMIT = "/v1/{prefix}/tables/{table}/plan";
public static final String V1_TABLE_SCAN_PLAN = "/v1/{prefix}/tables/{table}/plan/{plan-id}";
public static final String V1_TABLE_SCAN_PLAN_TASKS = "/v1/{prefix}/tables/{table}/tasks";
```

**V4.2 设计决策**:
- **主路径使用 OpenAPI 路径**（包含 `namespaces/{namespace}`）。
- **同时提供 Java 常量路径 alias**，在路由层兼容。
- 若 Java client 使用 ResourcePaths 常量构建请求，Quasar 必须支持两种路径。

#### 10.2.3 Plan Task Token 格式

**结论**: ⚠️ **Iceberg OpenAPI 未定义 `plan-task` token 内部格式，仅声明为 base64-encoded string**。

- OpenAPI schema: `plan-task: { type: string, description: "base64-encoded token" }`
- 无标准 JSON 结构定义。
- **V4.2 设计**: 采用自包含 JSON 结构 base64 编码。具体 token 内容结构由 `V4_2_DESIGN.md` 选定实现策略后定义：
  - **策略 A**: Token 包含 plan 参数（snapshot-id、filter、split-size 等），`/tasks` 端点收到 token 后重新执行 scan planning 计算（无状态，但计算重复）。
  - **策略 B**: Token 包含编码后的 FileScanTask 列表（无状态，但 token 体积较大）。
  - **策略 C**: 服务端用内存缓存（如 `DashMap<plan-id, Vec<FileScanTask>>`）暂存结果，token 仅包含 plan-id（有状态，但实现简单）。

**V4.2 功能性约束**：不论选择哪种策略，Plan 结果不持久化到 PostgreSQL，仅在请求会话内有效。

**候选 Token 内容结构**（V4_2_DESIGN.md 选定策略后确定）:

```json
{
  "plan-id": "uuid",
  "table-identifier": { "namespace": [...], "name": "..." },
  "snapshot-id": int64,
  "filter": {...},
  "split-size": int64,
  "tasks-summary": { "count": int, "total-size": int64 }
}
```

---

## 十一、风险评估（已更新）

| 风险 | 影响 | 缓解策略 | 状态 |
|------|------|----------|------|
| View/Table 同名冲突 | 数据模型设计需约束 | ✅ 已确认 Iceberg 禁止同名；V4.2 数据模型添加 UNIQUE 约束 | 已解决 |
| Scan Planning 路径兼容性 | 实现路径与客户端请求不一致 | ✅ 已确认 Java ResourcePaths 与 OpenAPI 差异；V4.2 设计双路径 alias | 已解决 |
| Scan Planning Spark 不默认调用 | E2E 验证价值降低 | ⚠️ 已确认 Spark 默认 client-side；V4.2 通过协议测试覆盖，E2E 可选 | 已明确 |
| Spark View E2E 验证 | 验证 Spark View 功能完整性 | ✅ 已确认 Spark 支持 REST View；V4.2 通过 E2E 验证 CREATE/SELECT | 可验证 |
| View Version History 无上限 | Metadata 文件增长 | ⚠️ Iceberg spec 无定义；V4.2 不限制，推迟 V4.3 评估清理 | 已明确 |
| Plan Task Token 格式兼容 | 客户端解析 Token | ⚠️ 无标准格式；V4.2 自定义 JSON base64 编码，通过协议测试覆盖 | 已明确 |

---

## 十二、参考资料

- `docs/v4/V4_OFFICIAL_REST_API.md`
- `docs/v4/V4_1_REQUIREMENTS.md`
- `docs/v4/V4_1_DESIGN.md`
- Apache Iceberg REST Catalog Spec: <https://iceberg.apache.org/rest-catalog-spec/>
- Apache Iceberg 1.10.0 OpenAPI: <https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml>
- Apache Iceberg Views Documentation: <https://iceberg.apache.org/docs/nightly/views/>
- Apache Iceberg 1.10.0 Endpoint Javadoc: <https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/rest/Endpoint.html>
- Apache Iceberg 1.10.1 Constant Values: <https://iceberg.apache.org/javadoc/1.10.1/constant-values.html>

---

## 十三、修订记录

### V1.3 (2026-06-08)

- §4.1.3 Create View 请求体澄清修正：
  - 从"转换为 CommitViewRequest（携带 assert-create requirement）"改为"转换为 ViewCreation（iceberg crate 类型），assert-create 语义由 adapter 层校验"。
  - 理由：CreateViewRequest 和 CommitViewRequest 是不同的 REST 请求体结构；ViewCreation 是 iceberg crate 0.9.1 的内部类型，由 `ViewMetadataBuilder::from_view_creation()` 直接使用。assert-create 语义通过 adapter 层检查 View 是否已存在实现，而非显式构造 ViewRequirement。
- §4.1.3 Replace View requirement 校验层级说明修正：
  - 从"通过 ViewMetadataBuilder 的 check() 方法校验"改为"adapter 层自定义 ViewRequirement enum 并手动实现校验"。
  - 理由：iceberg crate 0.9.1 不提供 ViewRequirement 类型（只有 TableRequirement），ViewRequirement 由 V4.2 adapter 层定义。
- §10.1.2 Version History 管理设计决策修正：
  - 从"实现 expireVersions() 清理逻辑（或在 V4_2_DESIGN.md 中评估是否由 iceberg crate 处理）"改为"Version History 清理由 iceberg crate 0.9.1 内置处理，V4.2 无需自行实现"。
  - 理由：`ViewMetadataBuilder::build()` 在构建完成时自动调用 `expire_versions()`。
- §4.1.4 UpgradeFormatVersion 适用范围注释补充：
  - 注明 View format-version 当前仅支持 V1，此 update 在 View 中无实际升级目标。
- 同步更新文档头部版本号为 V1.3。

### V1.2 (2026-06-06)

- §4.1.4 View Updates/Requirements 表补全：
  - ViewUpdate 从 5 项补全为 8 项（新增 `assign-uuid`、`upgrade-format-version`、`add-schema`），与 iceberg crate 0.9.1 `ViewUpdate` enum 一致。
  - 区分 table/view 共用 vs. view-only 适用范围。
  - 添加实现策略说明（参照 V4.0/V4.1 TableUpdate 分层方式）。
- §4.1.3 Create View 请求体澄清：
  - 明确 Create View 端点接收独立的 `CreateViewRequest`，但内部转换为 `CommitViewRequest`（携带 `assert-create` requirement）统一处理。
  - 补充同名 Table 冲突检查要求。
- §4.1.3 Replace View requirement 校验层级说明：明确 requirement 校验在 adapter 层执行，Store 层仅负责 CAS 更新。
- §4.1.5 View 错误处理补充"Namespace 下存在同名 Table → 409 `AlreadyExistsException`"场景。
- §4.2.5 Scan Planning 错误处理补充"Namespace 不存在 → 404 `NoSuchNamespaceException`"场景。
- §4.1.2 ViewMetadata JSON 示例修正：
  - `representations` 添加 `type: "sql"` discriminator 字段（符合 Iceberg View Format Spec V1 序列化规范）。
  - `metadata-location` 从 ViewMetadata JSON 内部字段移除，标注为 REST 响应层字段。
- §4.2.4/§6.2/§10.2.3 plan-task token 自包含表述统一：
  - 列出三种实现策略（token 编码完整 task 列表 / token 仅含参数重新计算 / 内存缓存），具体方案留给 `V4_2_DESIGN.md`。
  - 明确功能性约束：Plan 结果不持久化到 PostgreSQL。
- §10.1.3 同名冲突约束修正：现有 `uq_assets_active_name` 索引已满足约束，删除错误的 `ALTER TABLE` SQL 示例。
- §6.1 `view_assets` 表：
  - `current_version_id` 字段标注为"创建时快照，replace 后的实际值从对象存储读取"，是否在 commit 中同步更新由设计文档评估。
  - 补充 format 扩展预留说明（YAGNI 原则，不预先添加 format 字段）。
- §7.1 `IcebergViewStore` trait 修改：
  - super-trait 从 `CatalogStore` 改为 `AssetStore`（最小化 trait 约束）。
  - `create_view` 方法添加 `current_version_id` 参数。
  - 添加 `View`、`ViewAsset`、`ViewIdentifier` 新增类型说明。
  - 添加 requirement 校验层级文档注释。
- §4.2.2 路径选项补充 Option B/C 风险说明。
- §4.2.1/§4.2.2/§10.2.2 表头统一为"官方路径（OpenAPI）"。
- §五 非功能要求补充 View metadata 序列化规范约束（`type` discriminator 不可缺失）。
- 同步修正 `V4_OFFICIAL_REST_API.md` §一 `BaseUpdate`/`BaseRequirement` 口径（从"2 项 view 适用"修正为"6 项共用 + 2 项 view-only"；§4.4 注释同步修正）。
- 同步更新文档头部版本号为 V1.2、状态为"需求评审修订完成，待评审"。

### V1.1 (2026-06-06)

- §十"待澄清问题"转换为"已澄清结论"，基于 Iceberg 1.10.0 源码调研确认：
  - **Spark View E2E**: Spark 3.5 + Iceberg 1.10.x 已支持 REST Catalog View 端点。
  - **View/Table 同名冲突**: Iceberg 禁止同名，`RESTViewBuilder.replace()` 检查并抛出 `AlreadyExistsException`。
  - **View Version History**: ✅ 已确认 Iceberg 1.10.x 支持自动清理，属性 `version.history.num-entries`（默认 10）控制保留数量。
  - **Scan Planning 调用**: Spark 默认 client-side planning，REST `/plan` 端点为可选特性（需配置 `distributed` mode）。
  - **Java ResourcePaths 路径**: 确认不包含 `namespaces/{namespace}` segment，V4.2 需设计双路径 alias。
  - **Plan Task Token**: OpenAPI 未定义格式，V4.2 采用自包含 JSON base64 编码。
- §四.1.2 View Metadata 结构修正：
  - 字段名从 `view-history` 改为 OpenAPI 官方命名 `version-log`。
  - 补充 `ViewVersion.representations` 结构（替代单一 `sql` 字段）。
  - 补充 `ViewHistoryEntry` 结构定义。
  - 补充 Version History 管理说明（`version.history.num-entries` 属性）。
- §十一 风险评估更新：添加"状态"列，标注已解决/已明确/可验证。
- §六 数据模型补充 View/Table 同名约束建议（`assets` 表 UNIQUE 约束）。
- §四.2.2 Scan Planning 流程说明 Spark 默认不调用 REST 端点。
- 同步更新文档头部版本号为 V1.1、状态为"需求澄清完成，待评审"。

### V1.0 (2026-06-06)

- 初始版本，基于 V4_REQUIREMENTS.md §5.2 和用户需求澄清形成。
- 确定 V4.2 范围：View 生命周期（7 端点）+ Scan Planning（4 端点）。
- 明确 View metadata 结构和 ViewVersion 定义。
- 明确 Scan Planning 流程和路径兼容性问题。
- 提出 View Assets 数据模型和 IcebergViewStore trait 设计（候选）。
- 列出待澄清问题和风险评估。