# Quasar Agent 开发指南

> 本文档是 Agent 参与 Quasar 项目开发时的**入口指南**。开发前请阅读本指南，按指引查阅相关文档和规范。
>
> **项目进度跟踪：** 当前开发状态和阶段信息请查阅 `docs/PROGRESS.md`。

---

## 项目简介

**Quasar** 是一个面向 Lakehouse 架构的独立通用 Catalog Service 组件，用 Rust 编写。
- 同时支持 **Iceberg REST Catalog** 和 **Lance REST Namespace** 两套标准协议
- 核心模型格式无关（`Namespace → Asset → AssetVersion`）
- 后端使用 PostgreSQL，无状态设计可水平扩展
- MVP 分 Phase 1（Lance）和 Phase 2（Iceberg）两阶段交付

---

## 文档目录结构

```
Project/
├── AGENTS.md              # 本文件：Agent 开发入口指南
├── docs/
│   ├── PROGRESS.md        # 当前开发状态与进度跟踪（版本、阶段、已完成项）
│   ├── ARCHITECTURE.md    # 项目定位、设计目标、双协议架构、Namespace 隔离策略
│   ├── ROADMAP.md         # 版本路线图（V0.x / V1.x 阶段规划）
│   │
│   ├── mvp/               # MVP 设计文档（当前开发目标）
│   │   ├── MVP_REQUIREMENTS.md   # 功能需求、非功能需求、端点清单、关键约束
│   │   ├── DATA_MODEL.md         # 核心实体、PostgreSQL DDL、CatalogStore trait、并发控制
│   │   └── MVP_DESIGN.md         # 完整设计规格：架构、数据模型、端点、接口、开发阶段划分
│   │
│   └── conventions/       # 项目规范
│       ├── COMMIT_CONVENTION.md  # Git commit message 格式规范
│       └── CODING_CONVENTION.md  # Rust 编码规范
```

---

## 技术栈速查

| 用途 | crate | 备注 |
|------|-------|------|
| HTTP 框架 | `axum` | |
| 异步运行时 | `tokio` | `features = ["full"]` |
| PostgreSQL 驱动 | `tokio-postgres` | |
| 连接池 | `deadpool-postgres` | |
| DB 迁移 | `refinery` | |
| 序列化 | `serde` + `serde_json` | |
| UUID | `uuid` | `features = ["v4"]` |
| 错误定义 | `thiserror` | |
| 日志 | `tracing` + `tracing-subscriber` | |
| 配置加载 | `envy`（或 `dotenvy`） | 环境变量 + `.env` |
| 对象存储 | `object_store` | Phase 2 引入，读写 S3/MinIO metadata.json |

---

## Crate 分层

| Crate | 职责 | 关键约束 |
|-------|------|---------|
| `core` | 数据结构 + trait 定义 | **禁止**依赖 axum、tokio-postgres 等框架 crate |
| `storage` | PostgreSQL 实现 `CatalogStore` | 依赖 `core` |
| `adapter` | 协议适配（`lance/` + `iceberg/`） | 依赖 `core`，**不依赖** `storage` |
| `server` | 路由注册、启动、DI 组装 | 依赖所有 crate，负责将 `storage` 注入 `adapter` |

> **关键依赖方向：** `adapter` 只通过 `CatalogStore` trait 操作存储，不知道具体实现是 PostgreSQL。`server` 负责创建 `PgCatalogStore` 并以 `Arc<dyn CatalogStore>` 注入给 `adapter`。这条约束不得违反。

---

## 重要约束（开发红线）

- **不要**在 `core` crate 中引入任何框架依赖（axum、tokio-postgres 等）
- **不要**在 `adapter` 中直接依赖 `storage`，通过 `Arc<dyn CatalogStore>` 注入
- **不要**在生产代码中使用 `unwrap()` / `expect()`（详见 `CODING_CONVENTION.md`）
- **不要**发明私有 API 路径，端点必须严格遵循上游协议规范
- **不要**在 `assets` 表中存储 `format` 字段，format 由所属 `namespace` 决定
- **不要**使用 `ON DELETE CASCADE` 删除 Namespace 下的 Asset（规范要求非空 Namespace 不可删除）
- **不要**在 `properties` 中冗余存储 `current_version`（Lance 由 `SELECT MAX(version)` 实时查询）
- **不要**使用字符串拼接 SQL，必须使用参数化查询（`$1`、`$2`）

---

## 开发前必读

按顺序阅读以下文档：

| 顺序 | 文档 | 作用 |
|------|------|------|
| 1 | `docs/ARCHITECTURE.md` | 理解项目定位、设计目标、双协议架构、Namespace 隔离策略 |
| 2 | `docs/mvp/MVP_REQUIREMENTS.md` | 理解 MVP 范围、功能需求、非功能需求、端点清单 |
| 3 | `docs/mvp/DATA_MODEL.md` | 理解核心实体、DDL、CatalogStore trait、并发控制机制 |
| 4 | `docs/mvp/MVP_DESIGN.md` | 理解完整设计规格，特别关注当前阶段的验收标准 |

**开发过程中必须遵守的规范：**
- `docs/conventions/COMMIT_CONVENTION.md` — 每次提交前确认 message 格式
- `docs/conventions/CODING_CONVENTION.md` — 编写代码时遵循编码规范

---

## 按开发阶段查阅文档

### Phase 1：Lance REST Namespace（S0 ~ S7）

| 阶段 | 主要工作 | 重点参考 |
|------|---------|---------|
| S0 ~ S2 | 项目骨架、Core 定义、Storage 实现 | `DATA_MODEL.md`（实体定义、DDL、trait 签名） |
| S3 ~ S5 | Lance Adapter 端点实现 | `MVP_DESIGN.md` 第 5 章（Lance 端点）、第 9 章（Lance 错误格式 RFC-7807） |
| S6 | 基础设施 | `MVP_DESIGN.md` 第 10 章（配置与部署） |
| S7 | Lance 集成验证 | `MVP_DESIGN.md` S7 验收标准 |

### Phase 2：Iceberg REST Catalog（S8 ~ S10）

| 阶段 | 主要工作 | 重点参考 |
|------|---------|---------|
| S8 | Iceberg Namespace + Table CRUD | `MVP_DESIGN.md` 第 5 章（Iceberg 端点）、第 9 章（Iceberg 错误格式） |
| S9 | Iceberg CAS Commit | `DATA_MODEL.md`（CAS SQL）、`MVP_DESIGN.md` 第 8 章（并发控制） |
| S10 | Spark 集成验证 | `MVP_DESIGN.md` S10 验收标准 |

---

## 规范遵循要求

1. **每次提交前**检查 commit message 是否符合 `docs/conventions/COMMIT_CONVENTION.md`
2. **编写代码时**遵循 `docs/conventions/CODING_CONVENTION.md`
3. **修改设计文档时**在对应文档的"修订记录"末尾追加新条目，按规则增长版本号
4. **实现端点前**先对照 `MVP_DESIGN.md` 确认端点路径、请求/响应格式、错误码映射
5. **修改数据模型时**同步更新 `DATA_MODEL.md` 中的 Rust 定义和 DDL
6. **完成一个阶段的全部工作（功能实现 + 测试通过 + 文档更新）后**，更新 `docs/PROGRESS.md`，将该阶段状态标记为"已完成"

