# Quasar 开发进度跟踪

> 本文档记录 Quasar 项目的当前开发版本、所处阶段及完成情况。Agent 开发前应先查阅本文档确认当前状态。

---

## 当前版本

**版本：MVP**

MVP（Minimum Viable Product）是 Quasar 的第一个可交付版本，目标是验证核心架构并支持 Lance REST Namespace 和 Iceberg REST Catalog 两套协议。

---

## 当前阶段

| 属性 | 值 |
|------|---|
| **阶段** | S1（Core 层定义） |
| **Phase** | Phase 1（Lance REST Namespace） |
| **状态** | 未开始 |

---

## 阶段总览

### Phase 1：Lance REST Namespace（MVP）

| 阶段 | 名称 | 状态 | 验收标准 |
|------|------|------|---------|
| S0 | 项目骨架 | 已完成 | `cargo build` 成功编译 |
| S1 | Core 层定义 | 未开始 | 所有 trait 和数据结构定义完毕 |
| S2 | Storage 基础 | 未开始 | PostgreSQL 可连接，DDL 可迁移，基础 CRUD 可运行 |
| S3 | Lance Namespace 端点 | 未开始 | `curl` 可操作 Lance Namespace |
| S4 | Lance Table 基础操作 | 未开始 | `curl` 可操作 Lance Table |
| S5 | Lance 版本管理 | 未开始 | `curl` 可注册和查询版本 |
| S6 | 基础设施 | 未开始 | 容器化部署，`/healthz` 和 `/readyz` 正常 |
| S7 | Lance 集成验证 | 未开始 | Lance Python SDK 端到端跑通 |

### Phase 2：Iceberg REST Catalog（MVP）

| 阶段 | 名称 | 状态 | 验收标准 |
|------|------|------|---------|
| S8 | Iceberg Namespace + Table CRUD | 未开始 | `curl` 可操作 Iceberg Namespace 和 Table |
| S9 | Iceberg CAS Commit | 未开始 | 并发 commit 冲突正确返回 409 |
| S10 | Spark 集成验证 | 未开始 | Spark 端到端读写 Iceberg 表成功 |

---

## 已完成的里程碑

- **S0 — 项目骨架**（2026-04-19）
  - Cargo workspace 初始化，4 个 crate 结构就位
  - `cargo build` / `cargo clippy` 通过
  - `quasar-server` 可启动，`/healthz` 返回 `ok`

---

## 修订记录

### V1.1（2026-04-19）

- 更新：S0 状态标记为"已完成"
- 更新：当前阶段推进至 S1
- 新增：已完成里程碑记录 S0

### V1.0（2026-04-19）

- 新增：MVP 版本的阶段总览表（S0~S10）
- 新增：当前阶段状态记录
- 新增：已完成里程碑记录

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。每当完成一个阶段，更新对应阶段的状态为"已完成"。
