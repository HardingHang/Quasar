# Quasar V3 开发进度

> 本文档跟踪 Quasar V3(Data Core Model 重构)各 Phase 的完成状态。
> 每个 Phase 完成后追加条目;详细设计见 `docs/v3/V3_DESIGN.md`,需求见 `docs/v3/V3_REQUIREMENTS.md`。

---

## 阶段总览

| 阶段 | 名称 | 状态 | 关键提交 | 完成日期 |
|------|------|------|----------|----------|
| **V3-Phase 1** | **基础层:Core 模型 + Schema + SQL 骨架** | **已完成** | `5699400`、`48e0d7e`、`934f5a5`、`85ab58d`、`e5eaeda` | **2026/05/12** |
| **V3-Phase 2** | **存储层:Trait 拆分 + PgCatalogStore 重构** | **已完成** | `d3a9427`、`91d0a9e`、`a055a4a`、`57dea67`、`262c54b` | **2026/05/12** |
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

---

## Phase 2 详情

### 范围

- 删除 `storage/src/v1_migrations_archive/` 与 `storage/src/migrations/`,移除 `refinery` 依赖。
- `PgCatalogStore::migrate()` → `PgCatalogStore::initialize()`,body 调 `schema::initialize`;`storage/src/schema/init.sql` 中 seed `'default'` domain。
- 5 个 row mapper 全部切 V3 列,消除 Phase 1 留下的 7 处 `// TODO(v3-phase2)` 桥接。
- `core/src/store.rs` 拆 8 trait,新增 `DomainPatch`;`CatalogStore` 转为 marker super-trait + blanket impl,保持 `Arc<dyn CatalogStore>` 兼容。
- PgCatalogStore 实现全部 8 trait,所有 SQL 改用 `queries::*` 常量;新增 `asset::CAS_UPDATE_METADATA_LOCATION` / `LIST_UNIFIED` / `GET_UNIFIED` / `UPDATE_PROPERTIES_BY_ID` / `LOOKUP_ID_BY_NAME` 常量。
- ~35 处 adapter src 调用点切到 V3 trait 方法签名,`adapter::DEFAULT_DOMAIN = "default"` 作为 Phase 2 → Phase 3 transitional 兜底。
- `storage/tests/integration.rs` 切到 V3 trait API,新增 7 个 P0 测试覆盖 V3_DESIGN §11.2 矩阵。
- 14 个 adapter/server 测试 fixture 跟随重命名 `.migrate()` → `.initialize()`,并把 `create_asset` / `update_asset_properties` / `cas_update_metadata_location` 等调用同步切到 V3 形态。

### 实现策略

采用 **Phase 2 内置最小 adapter shim** 策略:trait 拆分 commit 同步把 adapter 调用点切到 V3 方法签名,Domain 参数硬编码 `"default"`,V3 schema 中 init.sql seed 此 domain。Phase 3 删除硬编码、改为从请求路径(`{prefix}` / `{id}` / `/domains/{domain}`)提取 domain。adapter handler 状态类型 `Arc<dyn CatalogStore>` 维持不变(`CatalogStore` 现在是 marker super-trait,blanket impl 保证 PgCatalogStore 满足)。

### Commit 清单

| Commit | 标题 | 影响 |
|--------|------|------|
| `d3a9427` | `chore(storage): remove dormant v1 migrations archive` | C1 — 5 文件 62 行删除,无引用者,清理 noise |
| `91d0a9e` | `feat(storage): seed default domain in v3 init schema` | C2 — init.sql 末尾 `INSERT INTO domains ('default')` + 测试断言 |
| `a055a4a` | `refactor(storage): rewrite pgcatalogstore against v3 schema and queries` | C3 — V1 schema → V3 schema 内核切换 + 7 处 TODO 桥接清除 + V2 trait 接口面保持 |
| `57dea67` | `refactor(core,storage,adapter): split catalogstore into 8 v3 traits` | C4 — 8 trait 定义 + PgCatalogStore impl 拆分 + ~35 adapter shim 切换 + 7 个 P0 测试 |
| `262c54b` | `chore(storage): drop refinery dependency` | C5 — Cargo.toml 删除 `refinery` 依赖项 |

### 验收

- `cargo fmt --all --check`:通过
- `cargo clippy --workspace --all-targets -- -D warnings`:通过
- `cargo build --workspace`:通过
- `cargo test --workspace`:260 passed, 0 failed, 6 ignored
  - 5 个 cross-format same-name 测试 `#[ignore]`,留 `TODO(v3-phase3)`(V3 active-name uniqueness 不允许 V2 的同名跨格式 fixture)
  - 1 个 schema_init `#[ignore]` 集成测试需 `--ignored` 触发
- `cargo test -p quasar-storage --test schema_init -- --ignored`:2/2 通过
- `cargo tree -p quasar-storage | grep refinery`:无输出(依赖移除验证)

### Phase 2 测试矩阵覆盖(V3_DESIGN §11.2)

P0 场景新增/迁移到 `storage/tests/integration.rs`:

- ✅ Domain CRUD 完整流程 + 重复创建 → `AlreadyExists` (`test_domain_crud_complete_flow`)
- ✅ schema/init.sql 在空 DB 上执行成功;表/索引/触发器/registry/default domain 存在 (`schema_init::*` `#[ignore]`)
- ✅ 同名 Namespace 跨 Domain 允许 (`test_namespace_same_name_across_domains`)
- ✅ 非空 Domain 删除 → `DomainNotEmpty` (`test_non_empty_domain_delete_returns_conflict`)
- ✅ 非空 Namespace 删除 → `NamespaceNotEmpty` (`test_non_empty_domain_delete_returns_conflict` 同一函数末尾)
- ✅ Asset active 名 namespace 内唯一(跨格式同名 → `AlreadyExists`)(`test_asset_active_uniqueness_within_namespace`)
- ✅ Asset 删除级联 (`test_drop_asset_cascades_tabular_and_versions`)
- ✅ Lance 版本链 (`test_version_commit_and_load`)
- ✅ 跨 Asset previous_version → trigger 23514 → `Conflict` (`test_previous_version_must_belong_to_same_asset`)
- ✅ latest 查询用 `version_order DESC` (`test_get_latest_version_uses_version_order`)
- ✅ Unified 查询 pair (Asset, Option<TabularAsset>) (`test_unified_query_pairs_asset_with_tabular`)
- ✅ CAS 乐观锁失败 → `Conflict` (`test_cas_commit_optimistic_concurrency`)

### Phase 3 交接清单

Phase 3 启动时需要立即清理的 Phase 2 transitional shim:

1. **adapter 路径提取 Domain**:
   - Iceberg `routes()` 改用 `/{prefix}/...`,handler 把 `prefix` 当 domain 传 store(替换硬编码 `DEFAULT_DOMAIN`)
   - Lance `routes()` 从 `{id}` 第一段解析 domain(`prod$analytics$table`),handler 提取后传 store
   - Unified `routes()` 加 `/domains/{domain}/...` 路径层级,handler 从路径取 domain
   - 删除 `adapter/src/lib.rs` 中的 `DEFAULT_DOMAIN` 常量
2. **端点级 format 过滤**:
   - Iceberg `routes()` 内部查询附加 `format='iceberg'`;同名 Lance 表返 `NoSuchTableException`(目前 `iceberg/lance::table_exists` 已用 `get_tabular_asset` 加 format,Phase 3 推广到其他读路径)
   - Lance `routes()` 附加 `format='lance'`
3. **adapter `*/error.rs` 错误脱敏与变体匹配**:
   - 删除 `msg.contains` / `msg.starts_with` 字符串匹配(V3_DESIGN §13)
   - `StoreError::Internal { msg, source }` 客户端响应固定为 "An internal error occurred";详细 `msg`/`source` 通过 tracing 记录
   - 新变体 `DomainNotEmpty` / `NamespaceNotEmpty` / `DatabaseUnavailable` / `Timeout` 映射到协议专属错误
4. **删除 `parse_format` 与 `?format=` 必填**:Unified Asset GET/DELETE/PATCH/rename 全部不再需要 format,Phase 2 仍保留为 transitional 校验
5. **Unified Domain 管理 API 新建**:`/unified/v1/domains*` 路由 + `unified/domain.rs` 新 handler 文件 + storage_config 响应脱敏
6. **Lance version_order 直读**:`unified::version::lance_version_to_response` 与 `lance::version::*` 删 `version_key.parse().unwrap_or(0)` fallback,改为 `version_order.ok_or(...)`
7. **`AssetFormat` / `AssetType` 枚举评估**:adapter 内部仍使用 `AssetFormat` 枚举为路由 dispatch,Phase 3 评估是否保留为 adapter-internal newtype
8. **`object_store` feature 解耦**:`UnifiedConfig` 删 `#[cfg(feature = "iceberg")]`,4 个 Cargo.toml 调整
9. **5 个 `#[ignore]` 跨格式测试**:`adapter/tests/format_isolation.rs` 中 `test_cross_format_list_isolation` / `_drop_` / `_rename_` + `adapter/tests/unified_asset.rs::test_list_assets_order_by_name_then_format` 需要按 V3 active-name uniqueness 重新设计(可能改为测试"second create 返 409"或"endpoint level filter")
10. **"default" domain seed**:Phase 3 完成后,init.sql 中的 `INSERT INTO domains ('default')` 可选删除(取决于产品策略是否预设此 domain)
