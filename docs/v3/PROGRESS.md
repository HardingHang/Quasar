# Quasar V3 开发进度

> 本文档跟踪 Quasar V3(Data Core Model 重构)各 Phase 的完成状态。
> 每个 Phase 完成后追加条目;详细设计见 `docs/v3/V3_DESIGN.md`,需求见 `docs/v3/V3_REQUIREMENTS.md`。

---

## 阶段总览

| 阶段 | 名称 | 状态 | 关键提交 | 完成日期 |
|------|------|------|----------|----------|
| **V3-Phase 1** | **基础层:Core 模型 + Schema + SQL 骨架** | **已完成** | `5699400`、`48e0d7e`、`934f5a5`、`85ab58d`、`e5eaeda` | **2026/05/12** |
| V3-Phase 2 | 存储层:Trait 拆分 + PgCatalogStore 重构 | 未开始 | - | - |
| V3-Phase 3 | 适配器层:Domain 路由 + 端点级隔离 + 错误脱敏 | 未开始 | - | - |
| V3-Phase 4 | 集成与验证:Server 组装 + 端到端测试 | 未开始 | - | - |

---

## Phase 1 详情

### 范围

- 重构 `StoreError` 为 V3 九变体形态(`Internal { msg, source }` 等)。
- 重构 Core 模型:新增 `Domain`,改造 `Namespace`/`Asset`/`TabularAsset`/`AssetVersion`/`TabularAssetVersion` 以匹配 V3 Data Core Model。
- 引入 `schema/init.sql` 作为 V3 DDL 真理源,提供 `schema::initialize` 加载入口(尚未接入 `PgCatalogStore`)。
- 引入 `queries.rs` 模块骨架,集中管理 V3 SQL 常量(Phase 2 替换内联 SQL)。
- 添加 `#[ignore]` 集成 smoke 测试,验证 init.sql 在 PostgreSQL 上可执行且幂等。

### 实现策略

采用 **B+ 桥接策略**:V3_DESIGN 第十章 Phase 1 验收原文只要求 core 编译,但为遵守 `COMMIT_CONVENTION` / `AGENTS.md` 红线(每个 commit 跑全 workspace 检查),Phase 1 在 `storage/src/store.rs` 内做最小桥接(占位 `Uuid::nil()`、`asset_subtype` 列映射到 `TabularAsset.format`、`asset_version_id` 列映射到 `version_id`),保持 V1 schema 行为不变,所有桥接处标注 `// TODO(v3-phase2)`。

### Commit 清单

| Commit | 标题 | 影响 |
|--------|------|------|
| `6717486` | `chore(adapter): silence preexisting clippy 1.94 lints in tests` | 解锁 V3 工作流的 clippy 关卡 |
| `5699400` | `refactor(core): restructure StoreError and migrate callsites` | C1 — 9 变体 StoreError + 36 callsite 机械迁移 |
| `48e0d7e` | `refactor(core): align asset/namespace/version models with v3 data core model` | C2 — Core 模型重构 + storage 桥接 |
| `934f5a5` | `feat(storage): add v3 init schema and loader` | C3 — `schema/init.sql` 与 `schema::initialize` |
| `85ab58d` | `feat(storage): scaffold queries module with v3 sql constants` | C4 — `queries.rs` 骨架(Phase 2 启用) |
| `e5eaeda` | `test(storage): add v3 schema init integration smoke test` | C5 — `#[ignore]` 集成 smoke 测试 |

### 验收

- `cargo fmt --all --check`:通过
- `cargo clippy --workspace --all-targets -- -D warnings`:通过
- `cargo build --workspace`:通过
- `cargo test --workspace`:全部通过(`schema_init` 2 个 `#[ignore]` 跳过)
- `cargo test -p quasar-storage --test schema_init -- --ignored`:2 个测试通过

V2 行为契约保持不变:adapter / storage 既有集成测试在 V1 schema runtime 上全绿。

### Phase 2 交接

Phase 2 启动时需立刻清理的 Phase 1 桥接债:

1. 删除 `migrations/` 与 `v1_migrations_archive/`,`PgCatalogStore::new` 改为调用 `schema::initialize`。
2. 把 `storage/src/store.rs` 内所有 `// TODO(v3-phase2)` 标注的 row 映射桥接切到 V3 真实列。
3. 实现 V3 设计的 8 个 trait 拆分(`DomainStore` / `NamespaceStore` / `AssetStore` / `TabularStore` / `VersionStore` / `TabularVersionStore` / `CasCommitStore` / `UnifiedQueryStore`),所有 SQL 替换为 `queries::*` 常量。
4. 删除 `CatalogStore` 单 trait,server 层以 marker trait 组合表达。
