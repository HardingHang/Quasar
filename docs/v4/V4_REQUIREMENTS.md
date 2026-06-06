# Quasar V4 需求分析文档

> **版本**: V1.3
> **日期**: 2026-06-05
> **状态**: 设计参考（已按评审意见拆分小版本）
>
> 本文档承担 V4 阶段的需求澄清与分析职责。
> 官方端点清单以 `docs/v4/V4_OFFICIAL_REST_API.md` 为唯一权威。
> 设计与实现细节由后续 `V4_DESIGN.md` 按小版本分别承接。

---

## 一、文档边界

| 文档 | 职责 | 规则 |
|------|------|------|
| `README.md` | V4 文档入口与阅读顺序 | 不承载需求细节；V4.0 启动时创建 |
| `V4_OFFICIAL_REST_API.md` | 官方协议基线、端点清单、小版本范围矩阵 | 不写 Quasar 内部实现方案 |
| `V4_REQUIREMENTS.md` | V4.0 必须交付范围与后续候选需求 | 不写 DDL、具体 Rust 类型替换步骤、代码路径 |
| `V4_DESIGN.md`（后续创建） | V4.0 设计方案 | 承接实现策略、数据模型、接口、错误映射、测试设计 |
| `PROGRESS.md`（后续创建） | V4 阶段进度 | 每个小版本完成后更新；V4.0 启动时创建 |

---

## 二、背景与目标

### 2.1 V3 完成范围回顾

Quasar V3 已完成：

- 数据核心模型：Domain -> Namespace -> Asset，`assets` 身份层与 `tabular_assets` 扩展层分离。
- Store trait 拆分：Domain / Namespace / Asset / Tabular / Version / CAS / Unified 查询等细粒度接口。
- 协议端点：Iceberg REST、Lance REST、Unified Domain API 三套入口。
- Iceberg CAS 提交：通过 `metadata_location` 条件更新保证乐观锁。
- 对象存储：已支持 Iceberg metadata.json 写入与读取。
- 错误处理：`StoreError` 分层、客户端响应脱敏、 transient error 分类已完成。

### 2.2 V4 总目标

V4 的总目标是把 Quasar 从 V3 的“协议骨架可用”推进到“Spark 可稳定接入的 Iceberg REST Catalog”。

| 目标 | V4.0 验收口径 |
|------|---------------|
| Iceberg 1.10.x 协议基线 | 以 Iceberg 1.10.x OpenAPI 为准，V4.0 只实现 Spark E2E 必需 endpoint，不声称全量覆盖 |
| Spark 端到端 | Spark 3.5 + Iceberg 1.10.x + MinIO + Postgres + Quasar 直接跑通基础 DDL/DML |
| V3 Iceberg 缺口闭环 | staged create、register table、commit requirements/actions、purgeRequested、metrics 接收进入 V4.0 |
| 元数据兼容 | Quasar 写出的 metadata.json 可被 Spark Iceberg client 读回 |
| 范围风险控制 | views、scan planning、transactions、vended credentials、S3 signer 下放到候选小版本 |

---

## 三、小版本拆分

### 3.1 小版本总览

| 小版本 | 定位 | 状态 | 验收信号 |
|--------|------|------|----------|
| V4.0 | Spark-ready Iceberg baseline | 必须交付 | Spark 基础 DDL/DML、schema/spec 演化、branch/tag 基础路径、drop purge smoke 全部通过 |
| V4.1 | REST table capability completion | 后续候选 | transactions、vended credentials、TableUpdate 全集、安全边界设计完成并通过集成测试 |
| V4.2 | Views and scan planning | 后续候选 | Iceberg view 生命周期与 server-side scan planning endpoint 通过协议测试 |
| V4.3 | Operational hardening | 后续候选 | metadata cache、orphan cleanup、cursor pagination、多客户端 smoke、观测指标完善 |

### 3.2 版本推进规则

- V4.0 不以“全量 Iceberg REST Catalog”作为验收口径，只以 Spark-ready baseline 作为验收口径。
- V4.1+ 是候选小版本，不阻塞 V4.0 进入实现。
- 每个小版本必须有独立设计文档章节、测试矩阵和进度状态。
- `/v1/config` 的 `endpoints` 字段必须按实际已实现能力逐步增加，不得提前声明候选 endpoint。

---

## 四、V4.0 必须交付范围

### 4.1 官方协议范围

V4.0 必须以 `V4_OFFICIAL_REST_API.md` 中 Iceberg 1.10.x 为官方基线，但实现范围只包含 Spark-ready 必需能力：

| 类别 | V4.0 要求 |
|------|-----------|
| Configuration | `GET /v1/config` 返回实际支持 endpoint；继续支持 `warehouse` 查询参数现有行为 |
| Namespace | 维持 V3 namespace 端点，并补齐关键错误路径测试 |
| Table CRUD | 维持 list/create/load/drop/head/rename；新增 register table；修复 staged create |
| Commit | 覆盖 Spark DDL/DML 会触发的 1.10.x requirements/actions；已知但未支持的官方 action 必须返回清晰错误，不得静默忽略 |
| Metrics | 实现 scan metrics report 的接收与最小持久化或日志化 |
| Purge | `purgeRequested=true` 不再返回 501；至少完成表路径下 metadata/data 的同步清理闭环 |
| Lance / Unified | 维持 V3 现状，只接受共享数据模型重构引发的兼容修复 |

### 4.2 V3 Iceberg 缺口闭环

| # | 项 | V4.0 要求 |
|---|-----|-----------|
| 1 | staged create 路径 `assert-create` 不可达 | create 返回 staged metadata，commit 阶段在事务内验证 `assert-create` 并插入 catalog 记录 |
| 2 | `register` table 未实现 | 支持以外部 `metadata-location` 注册已有 Iceberg 表；不重写 metadata.json |
| 3 | commit requirements 缺失 | 实现 1.10.x `BaseRequirement` 全部 8 项 table 适用断言：`assert-create`、`assert-table-uuid`、`assert-ref-snapshot-id`、`assert-current-schema-id`、`assert-default-spec-id`、`assert-default-sort-order-id`、`assert-last-assigned-field-id`、`assert-last-assigned-partition-id`。1.10.x table requirement 只有 8 项，全集覆盖比挑选 Spark 子集风险更小（避免后续 ALTER TABLE ADD COLUMN / ADD PARTITION FIELD 等路径漏断言）。 |
| 4 | commit actions 缺失 | 分两层覆盖 1.10.x `BaseUpdate` 中 Spark 实际触发的 action：<br>**(a) Spark INSERT/CREATE/ALTER 路径必经（V3 已实现 enum，需修正 wire 名）**：`add-snapshot`、`set-snapshot-ref`、`set-properties`、`remove-properties`、`add-schema`、`set-current-schema`、`add-spec`（V3 当前错误命名 `add-partition-spec`，参见 `quasar/adapter/src/iceberg/table_metadata.rs:150` — `#[serde(rename_all = "kebab-case")]` 从 enum variant `AddPartitionSpec` 自动派生 wire name，与 1.10.x 官方 `add-spec` 不符，V4.0 必须显式 `#[serde(rename = "add-spec")]` 或切换到 iceberg-rust 类型）、`set-default-spec`、`add-sort-order`、`set-default-sort-order`。<br>**(b) Spark 维护与演化路径（V4.0 新增实现）**：`assign-uuid`、`upgrade-format-version`、`remove-snapshots`、`remove-snapshot-ref`、`set-location`、`remove-partition-specs`。<br>对其他 1.10.x table 适用但 V4.0 不实现的 update（statistics 4 项、encryption key 2 项、`remove-schemas`），必须返回明确错误（不得静默忽略），归入 V4.1 候选。 |
| 5 | `purgeRequested=true` 返回 501 | 改为真实清理；失败时返回明确错误并避免 catalog/对象存储状态不可解释 |
| 6 | metrics endpoint 缺失 | 接收 Spark scan report；V4.0 不要求接入 Prometheus 指标后端 |
| 7 | 并发 CAS 测试不足 | 增加至少一个真实并行 commit 冲突集成测试；不要求大规模压测 |
| 8 | 多 warehouse 错误口径不清 | V4.0 仅保持现有单 warehouse 行为；多 warehouse 完整支持推迟到 V4.1+ 评估 |

### 4.3 Spark E2E 范围

| 维度 | V4.0 必测 | V4.0 不强制 |
|------|----------|-------------|
| 基础 DDL/DML | `CREATE TABLE`、`CREATE TABLE IF NOT EXISTS`、`INSERT INTO`、`SELECT`、`DROP TABLE`、DataFrame `.writeTo(...).create()` / `.append()` | UPDATE / DELETE / MERGE 的完整矩阵可进入 V4.1，除非当前 Spark 环境稳定触发 |
| Schema / Spec 演化 | ADD/DROP/RENAME/ALTER COLUMN 基础路径；ADD/DROP PARTITION FIELD smoke（分区 transform V4.0 必测限定为 `identity` + 任一时间型 transform `days(...)` 或 `hours(...)` + `bucket(N, ...)` 各一例） | 其余分区 transform（`years`、`months`、`truncate(N, ...)`）推迟到 V4.1+；所有复杂 transform 组合与历史 snapshot 深度验证 |
| 时间旅行 / Branch / Tag | snapshot read、create branch/tag、drop branch/tag 的 smoke | 并发 branch 写入与跨 branch merge |
| 表维护 | `expire_snapshots` smoke、`DROP TABLE PURGE` | `rewrite_data_files`、`rewrite_manifests`、`remove_orphan_files` 完整闭环 |

### 4.4 非功能要求

- V4.0 不引入生产鉴权语义；因此不得默认签发真实 vended credentials。
- 客户端错误响应继续遵循 Iceberg error model，并保持敏感信息脱敏。
- 生产代码不得使用 `unwrap()` / `expect()`。
- SQL 继续集中在 storage 查询模块，禁止字符串拼接 SQL。
- 修改测试后必须同步维护 `docs/TEST_MATRIX.md`。

---

## 五、后续候选小版本

### 5.1 V4.1: REST Table Capability Completion

候选范围：

- `POST /v1/{prefix}/transactions/commit` 多表原子提交。
- `GET .../tables/{table}/credentials` vended credentials — **推迟到后续安全专项版本**。
- 可选接入 S3 Signer API `/v1/aws/s3/sign` — **推迟到后续安全专项版本**。
- 补齐 1.10.x `TableUpdate` 全集：statistics、remove schemas、encryption key 等非 V4.0 必需 actions。其中 encryption key actions（`add-encryption-key`、`remove-encryption-key`）**推迟到后续安全专项版本**。
- 多 warehouse 的 `NoSuchWarehouse` 行为与配置模型。配置模型扩展推迟到 V4.3+ 评估。

准入条件（V4.1 重新定义）：

- V4.0 完成并通过验收。
- `V4_1_DESIGN.md` 完成，包含事务原子性方案、TableUpdate actions 实现设计、multi-warehouse 参数校验设计。

原准入条件（"完成鉴权/凭证安全边界设计"和"明确 Domain.storage_config 中 credential/secret 的存储、脱敏、轮换与最小权限策略"）推迟到后续独立版本（或 V4.x 安全专项）评估。

### 5.2 V4.2: Views And Scan Planning

候选范围：

- Iceberg 1.10.x view 生命周期：list/create/load/replace/drop/head/rename。
- Server-side scan planning：submit/fetch/cancel/fetch tasks。
- 处理 OpenAPI scan planning 路径与 Java ResourcePaths 常量差异，必要时提供兼容 alias。

不纳入 V4.2：

- `register-view`，除非官方基线升级到 Iceberg 1.11+。

### 5.3 V4.3: Operational Hardening

候选范围：

- metadata cache 与 commit 后失效策略。
- cursor-based pagination 替换大 OFFSET 扫描。
- 对象存储孤儿文件后台清理与保护窗口。
- Prometheus 指标、结构化审计日志、更多 transient error 注入测试。
- PyIceberg / Trino / Flink smoke 验证。
- Lance 新端点与 Unified Asset 创建重新评估。

---

## 六、数据模型与依赖边界

### 6.1 需求层要求

V4.0 的需求层只规定兼容性目标：

- Quasar 写出的 Iceberg metadata JSON 必须能被 Spark Iceberg 1.10.x client 读回。
- Quasar commit 后的 `metadata_location` 必须始终指向当前有效 metadata 文件。
- 对象存储写入与 PostgreSQL CAS 更新之间的失败窗口必须被记录，并在 V4.0 至少给出可诊断错误。
- V4.0 容忍 metadata 写入对象存储成功但 PostgreSQL CAS 失败导致的孤儿 metadata 文件；客户端必须收到 5xx 错误，服务端日志必须包含可定位的对象存储路径与 commit 上下文。孤儿文件的后台清理任务推迟到 V4.3。

### 6.2 设计层候选

以下内容进入 `V4_DESIGN.md`，不在需求文档中定死：

- 是否引入 `iceberg` crate，以及 pin 的具体 patch 版本。
- V3 自研 `TableMetadata` 替换路径。
- wrapper 层、Builder 使用、manifest 读写、缓存与后台任务实现。
- PostgreSQL schema 是否调整及初始化脚本变化。

---

## 七、本版本不做什么

V4.0 明确不做：

1. Iceberg REST Catalog 全量官方端点覆盖声明。
2. Iceberg V3 table-format spec 支持。
3. `POST /v1/oauth/tokens` OAuth token endpoint。
4. Vended credentials 与 S3 signer 的生产级实现。
5. Iceberg views。
6. Server-side scan planning。
7. Multi-table transaction commit。
8. V3 存量数据自动转换工具。
9. Lance 新端点扩展。
10. Unified API 暴露 Asset 创建。

---

## 八、验收清单

V4.0 完成条件：

- `V4_OFFICIAL_REST_API.md` 的 V4.0 范围列与 `/v1/config` 实际 `endpoints` 输出一致。
- Spark E2E V4.0 必测用例通过。
- Quasar 写出的 metadata.json 至少通过一个由 Spark Iceberg 1.10.x client 直接读取并打印 schema/snapshot 的集成测试（保证元数据序列化兼容性）。
- staged create、register table、commit requirements/actions、purge、metrics 均有 adapter 集成测试。
- 至少一个真实并行 CAS 冲突 adapter 测试通过；如沿用 V3 `postgres_embedded` 受限于 `serial_test`，V4.0 必须引入容器化 Postgres 或等价替代方案以解除并发限制。
- `docs/TEST_MATRIX.md` 已同步记录新增或修改测试。
- `V4_DESIGN.md` 和 `PROGRESS.md` 已按实际实现结果更新。

---

## 九、参考资料

- `docs/v4/V4_OFFICIAL_REST_API.md`
- `docs/v3/V3_TEST_GAPS.md`
- `docs/v3/V3_CLOSURE.md`
- `docs/v3/V3_DESIGN.md`
- `docs/EVOLUTION.md`
- Apache Iceberg REST Catalog Spec: <https://iceberg.apache.org/rest-catalog-spec/>
- Apache Iceberg 1.10.0 OpenAPI: <https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml>
- Apache Iceberg 1.10.0 Endpoint Javadoc: <https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/rest/Endpoint.html>

---

## 十、修订记录

### V1.3 (2026-06-05)

- §5.1 V4.1 候选范围调整：credentials、S3 Signer、encryption key actions 明确标注"推迟到后续安全专项版本"。
- §5.1 准入条件重新定义：V4.1 准入条件调整为 V4.0 完成和设计文档评审；原"鉴权/凭证安全边界设计"准入条件推迟到后续独立版本评估。
- 同步更新文档头部版本号为 V1.3、日期为 2026-06-05。

### V1.2 (2026-05-19)

- §一 文档边界 — README.md 与 PROGRESS.md 两行规则列加注 "V4.0 启动时创建"，避免对未存在文件的硬引用。
- §4.2 item 3 commit requirements 改为覆盖 1.10.x `BaseRequirement` 全部 8 项 table 适用断言（含 `assert-last-assigned-field-id`、`assert-last-assigned-partition-id` — Spark ALTER 路径触发）。
- §4.2 item 4 commit actions 拆为两层：(a) V3 已实现 enum、仅需修正 wire name `add-partition-spec` → `add-spec`（V3 bug 位置: `quasar/adapter/src/iceberg/table_metadata.rs:150`）；(b) V4.0 新增实现 `assign-uuid` / `upgrade-format-version` / `remove-snapshots` / `remove-snapshot-ref` / `set-location` / `remove-partition-specs`。其余 1.10.x action（statistics 4 项、encryption key 2 项、`remove-schemas`）归入 V4.1 候选并必须返回明确错误。
- §4.3 Spark E2E 矩阵 — 分区 transform V4.0 必测限定为 `identity` + 时间型（`days`/`hours`）+ `bucket(N)` 各一例；`years`/`months`/`truncate(N)` 推迟到 V4.1+。
- §6.1 需求层要求 — 追加孤儿 metadata 文件容忍口径：V4.0 容忍 PostgreSQL CAS 失败导致的孤儿 metadata；客户端必须收到 5xx 错误，日志包含可定位的对象存储路径与 commit 上下文；后台孤儿清理推迟到 V4.3。
- §八 验收清单 — 新增 "Spark Iceberg 1.10.x client 直接读取 Quasar 写出 metadata.json" 序列化兼容性测试条目；将并发 CAS 测试条目扩充为对测试基础设施的硬约束（容器化 Postgres 或等价替代）。

### V1.1 (2026-05-18)

- 将 V4 从单一“大版本全量交付”拆分为 V4.0 / V4.1 / V4.2 / V4.3。
- 明确 V4.0 为 Spark-ready Iceberg baseline，不再以全量 REST Catalog 端点覆盖作为 V4.0 验收口径。
- 将 transactions、vended credentials、S3 signer、views、scan planning、metadata cache、orphan cleanup 等下放为后续候选小版本。
- 将 `iceberg` crate、wrapper 层、具体 Rust 类型替换等内容降级为设计层候选，不在需求文档中定死。
- 修正 V3 遗留闭环表中的错误官方 action/requirement 口径，删除 `assert-last-sequence-number` 与 `remove-sort-orders` 的 V4.0 必须实现要求。

### V1.0 (2026-05-18)

- 初始版本，V4 需求基线建立。
