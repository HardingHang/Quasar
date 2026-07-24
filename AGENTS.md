# Quasar Agent 开发指南

> 本文档是 Agent 参与 Quasar 项目开发时的**入口指南**。开发前请阅读本指南，按指引查阅相关文档和规范。
>
> **项目文档入口：** 需求与设计权威文档为 `docs/REQUIREMENTS.md` 与 `docs/DESIGN.md`，开发前必读（见下方"开发前必读"）。

---

## 项目简介

**Quasar** 是一个面向 Lakehouse 架构的独立通用 Catalog Service 组件，用 Rust 编写。
- 同时支持 **Iceberg REST Catalog** 和 **Lance REST Namespace** 两套标准协议，另有格式无关的 **Unified API**（管理/发现/治理）
- 核心模型：`Domain → 层级 Namespace → Asset → AssetVersion`，资产类型/格式可注册扩展
- 后端使用 PostgreSQL，无状态设计可水平扩展

---

## 文档目录结构

```
Project/
├── AGENTS.md              # 本文件：Agent 开发入口指南
├── docs/
│   ├── REQUIREMENTS.md    # 权威需求基线（功能/非功能需求、数据模型需求、验收标准）
│   ├── DESIGN.md          # 权威设计基线（架构、数据模型 DDL、Store trait、API、并发控制）
│   ├── DEPLOYMENT.md      # 部署与验证流程
│   ├── TEST_MATRIX.md     # 测试覆盖矩阵
│   │
│   ├── conventions/       # 项目规范
│   │   ├── COMMIT_CONVENTION.md      # Git commit message 格式规范
│   │   ├── CODING_CONVENTION.md      # Rust 编码规范
│   │   └── DEVELOPMENT_CONVENTION.md # 项目开发规范（测试、文档、审查、验收）
│   │
│   └── archive/           # 历史版本文档（mvp / v2 / v3 / v4.0~v4.2），仅作参考
```

---

## 技术栈速查

| 用途 | crate | 备注 |
|------|-------|------|
| HTTP 框架 | `axum` | |
| 异步运行时 | `tokio` | `features = ["full"]` |
| PostgreSQL 驱动 | `tokio-postgres` | |
| 连接池 | `deadpool-postgres` | |
| DB 迁移 | 自研迁移 runner | `storage/migrations/NNNN_*.up.sql` + `schema_migrations` 版本表，启动时按序应用 |
| 序列化 | `serde` + `serde_json` | |
| UUID | `uuid` | `features = ["v4"]` |
| 错误定义 | `thiserror` | |
| 日志 | `tracing` + `tracing-subscriber` | |
| 配置加载 | 手写环境变量读取（`server/src/config.rs`） | `QUASAR_*` 前缀 |
| 对象存储 | `object_store` | 读写 S3/MinIO metadata.json |

---

## Crate 分层

| Crate | 职责 | 关键约束 |
|-------|------|---------|
| `core` | 数据结构 + trait 定义 | **禁止**依赖 axum、tokio-postgres 等框架 crate |
| `storage` | PostgreSQL 实现 `CatalogStore` | 依赖 `core` |
| `adapter` | 协议适配（`lance/` + `iceberg/` + `unified/`） | 依赖 `core`，**不依赖** `storage` |
| `server` | 路由注册、启动、DI 组装 | 依赖所有 crate，负责将 `storage` 注入 `adapter` |

> **关键依赖方向：** `adapter` 只通过 `CatalogStore` trait 操作存储，不知道具体实现是 PostgreSQL。`server` 负责创建 `PgCatalogStore` 并以 `Arc<dyn CatalogStore>`（lance/unified）与 `Arc<dyn IcebergCatalogStore>`（iceberg）注入给 `adapter`。这条约束不得违反。

---

## 重要约束（开发红线）

- **不要**在 `core` crate 中引入任何框架依赖（axum、tokio-postgres 等）
- **不要**在 `adapter` 中直接依赖 `storage`，通过 `Arc<dyn CatalogStore>` 注入
- **不要**在生产代码中使用 `unwrap()` / `expect()`（详见 `CODING_CONVENTION.md`）
- **不要**发明私有 API 路径，端点必须严格遵循上游协议规范
- **不要**使用数据库触发器维护跨行不变量（asset_type 校验、版本链校验、updated_at 均走应用层）
- **不要**使用字符串拼接 SQL，必须使用参数化查询（`$1`、`$2`）
- 命名必须遵循 URL-safe slug 规则 `^[a-z0-9][a-z0-9_-]{0,62}$`（Domain/Namespace 路径段/Asset 名统一）
- `format` 存储在 `assets.format`（FK→formats），协议隔离按此列过滤；**不要**在扩展表重复存储
- 最新版本由 `assets.current_version_key` 确定（CAS 成功后同步更新）；**不要**引入 version_order 之类的冗余排序列
- 数据库变更必须走 `storage/migrations/` 增量迁移，**不要**回退到单文件幂等 init.sql

---

## 开发前必读

按顺序阅读以下文档：

| 顺序 | 文档 | 作用 |
|------|------|------|
| 1 | `docs/REQUIREMENTS.md` | **当前权威**：项目定位、功能/非功能需求、数据模型需求、并发控制、错误处理、验收标准 |
| 2 | `docs/DESIGN.md` | **当前权威**：架构总览、Crate 分层、数据模型 DDL、Store trait、三套协议 API 设计、版本镜像与 CAS 并发控制、错误映射、Feature Flag、Server 组装 |
| 3 | `docs/DEPLOYMENT.md` | 部署与验证流程（最小部署 / Lance 集成 / Spark 集成） |
| 4 | `docs/TEST_MATRIX.md` | 测试覆盖矩阵 |
| 5 | `docs/archive/` | 历史版本文档（MVP/V2/V3/V4.0~V4.2）归档，仅作参考 |

> ⚠️ `docs/ARCHITECTURE.md`、`docs/PROGRESS.md`、`docs/ROADMAP.md`、`docs/EVOLUTION.md` 为早期历史文档，已不再维护，以 `REQUIREMENTS.md` / `DESIGN.md` 为准。

**开发过程中必须遵守的规范：**
- `docs/conventions/COMMIT_CONVENTION.md` — 每次提交前确认 message 格式
- `docs/conventions/CODING_CONVENTION.md` — 编写代码时遵循编码规范

---

## 版本背景

当前代码与文档对应**基线重设计**（Domain → 层级 Namespace → Asset → AssetVersion、版本镜像、Unified API），早期 MVP/V2/V3/V4.x 阶段文档均归档于 `docs/archive/`（含各阶段 S0~S10 的划分与验收记录），仅作历史参考，不作为开发依据。

---

## TDD 开发原则（测试先行）

> 虽然不要求严格的 TDD 流程，但开发时应遵循"测试驱动思维"：先思考测试，再写实现。

### TDD 循环

```
红（写测试 → 失败）→ 绿（写最少代码 → 通过）→ 重构（改进设计）
```

### 测试优先级

| 优先级 | 测试类型 | 覆盖目标 | 编写时机 |
|--------|----------|----------|----------|
| P0 | 单元测试 | `core` crate 模型方法、错误分支、边界条件 | 与被测代码同 commit 或更早 |
| P0 | 集成测试 | 每个端点的成功路径 + 主要错误路径 | 端点实现后立刻补充 |
| P1 | 边缘场景 | 分页边界、并发冲突、空输入、超长输入 | 功能稳定后补充 |
| P2 | 端到端 | Python SDK / Spark 集成验证 | 阶段验收前 |

### 各 crate 测试策略

| Crate | 测试重点 | 测试方式 |
|-------|----------|----------|
| `core` | 模型序列化、枚举变体、错误消息格式 | 单元测试（`#[cfg(test)]`） |
| `storage` | `CatalogStore` trait 每个方法的 CRUD + 错误 + 并发 | 集成测试（`tests/` + 嵌入式 PostgreSQL） |
| `adapter` | 每个端点的 HTTP 状态码、响应格式、错误映射 | 集成测试（`tests/` + Tower `ServiceExt::oneshot`） |
| `server` | 配置解析、健康检查、路由组装 | 集成测试（`tests/`） |

### 红绿重构实践

1. **红**：为新增功能写一个失败的测试（编译失败或断言失败）
2. **绿**：写最少代码让测试通过，不追求完美
3. **重构**：测试通过后优化代码结构，保持测试通过

**Agent 开发时**：若发现已有功能缺少测试，优先补测试再改代码。测试是理解代码行为的最可靠文档。

---

## 规范遵循要求

### 三类规范的关系

| 规范 | 定义 | 文档 |
|------|------|------|
| 开发规范 | 做什么、何时做 | `docs/conventions/DEVELOPMENT_CONVENTION.md` |
| 编码规范 | 怎么写 | `docs/conventions/CODING_CONVENTION.md` |
| 提交规范 | 怎么记 | `docs/conventions/COMMIT_CONVENTION.md` |

### 具体要求

1. **每次提交前**检查 commit message 是否符合提交规范
2. **每次提交前**运行代码格式检查并修复，确保 CI 格式检查通过：
   ```bash
   cargo fmt --all --manifest-path quasar/Cargo.toml
   ```
3. **每次提交前**运行测试确保全部通过，避免 CI 测试失败：
   ```bash
   cargo test --all-features --manifest-path quasar/Cargo.toml
   ```
4. **编写代码时**遵循编码规范
5. **开发功能/模块时**遵循开发规范中的工作流、测试、文档、审查要求
6. **修改设计文档时**在对应文档的"修订记录"末尾追加新条目，按规则增长版本号
7. **实现端点前**先对照**设计规格文档**确认端点路径、请求/响应格式、错误码映射
8. **修改数据模型时**同步更新**数据模型文档**中的 Rust 定义和 DDL
9. **完成一个阶段的全部工作（功能实现 + 测试通过 + 文档更新）后**，更新**进度跟踪文档**，将该阶段状态标记为"已完成"

