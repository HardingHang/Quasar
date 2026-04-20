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
| **阶段** | S6（基础设施） |
| **Phase** | Phase 1（Lance REST Namespace） |
| **状态** | 未开始 |

---

## 阶段总览

### Phase 1：Lance REST Namespace（MVP）

| 阶段 | 名称 | 状态 | 验收标准 |
|------|------|------|---------|
| S0 | 项目骨架 | 已完成 | `cargo build` 成功编译 |
| S1 | Core 层定义 | 已完成 | 所有 trait 和数据结构定义完毕 |
| S2 | Storage 基础 | 已完成 | PostgreSQL 可连接，DDL 可迁移，基础 CRUD 可运行 |
| S3 | Lance Namespace 端点 | 已完成 | `curl` 可操作 Lance Namespace |
| S4 | Lance Table 基础操作 | 已完成 | `curl` 可操作 Lance Table |
| S5 | Lance 版本管理 | 已完成 | `curl` 可注册和查询版本 |
| S6 | 基础设施 | 未开始 | 容器化部署，`/healthz` 和 `/readyz` 正常 |
| S7 | Lance 集成验证 | 未开始 | Lance Python SDK 端到端跑通 |

### Phase 2：Iceberg REST Catalog（MVP）

| 阶段 | 名称 | 状态 | 验收标准 |
|------|------|------|---------|
| S8 | Iceberg Namespace + Table CRUD | 未开始 | `curl` 可操作 Iceberg Namespace 和 Table |
| S9 | Iceberg CAS Commit | 未开始 | 并发 commit 冲突正确返回 409 |
| S10 | Spark 集成验证 | 未开始 | Spark 端到端读写 Iceberg 表成功 |

---

## 当前阶段

| 属性 | 值 |
|------|---|
| **阶段** | S6（基础设施） |
| **Phase** | Phase 1（Lance REST Namespace） |
| **状态** | 未开始 |

---

## 已完成的里程碑

- **S5 — Lance 版本管理**（2026-04-20）
  - `CatalogStore` trait 新增 `create_version` 方法（客户端指定 version_id，区别于 Iceberg CAS 的 `commit_version`）
  - `PgCatalogStore` 实现 `create_version`（INSERT 带 version_id，`UNIQUE(asset_id, version_id)` 约束冲突 → 409）
  - Lance 错误模块扩展：新增 `TableVersionAlreadyExists` 变体
  - Lance 版本管理 3 个端点全部实现：
    - `POST /lance/v1/table/{id}/version/create` — 注册版本（version + manifest_path）
    - `GET /lance/v1/table/{id}/version/list` — 列出版本历史
    - `POST /lance/v1/table/{id}/version/describe` — 获取指定版本详情
  - 测试覆盖（`adapter/tests/lance_version.rs`，6 个）：
    - 创建/描述版本正常流
    - 重复创建返回 409 + TableVersionAlreadyExists
    - 列出版本（v1, v2）
    - 描述不存在的版本返回 404
    - 对不存在的表创建版本返回 404
    - 创建版本后 describe_table 返回 current_version
  - `cargo build` / `cargo clippy` 零警告 / `cargo test` 全通过（27 个测试）

- **S4 — Lance Table 基础操作**（2026-04-20）
  - Lance 错误模块扩展：新增 `TableNotFound`、`TableAlreadyExists`、`TableNotEmpty` 变体
  - `{id}` 路径参数解析：`{namespace}${table}` 格式（如 `prod$users`）
  - Lance Table 8 个端点全部实现：
    - `POST /lance/v1/table/{id}/declare` — 声明表，分配 `location` 存储于 properties
    - `POST /lance/v1/table/{id}/describe` — 描述表（含 location、current_version）
    - `POST /lance/v1/table/{id}/register` — 注册已有表（提供 location）
    - `POST /lance/v1/table/{id}/deregister` — 注销表（保留数据）
    - `POST /lance/v1/table/{id}/drop` — 删除表
    - `POST /lance/v1/table/{id}/exists` — 存在检查
    - `POST /lance/v1/table/{id}/rename` — 重命名表
    - `GET /lance/v1/namespace/{id}/table/list` — 列出 Namespace 下的表
  - 测试覆盖（`adapter/tests/lance_table.rs`，10 个）：
    - 声明/描述表正常流
    - 重复声明返回 409 + TableAlreadyExists
    - 列出 Namespace 下的表
    - 存在检查（true/false）
    - 注册已有表
    - 注销表成功 + 重复注销返回 404
    - 删除表 + 确认不存在
    - 重命名表 + 新旧名称验证
    - 描述不存在的表返回 404 + TableNotFound
    - 在不存在的 Namespace 中声明返回 404
  - `cargo build` / `cargo clippy` 零警告 / `cargo test` 全通过（21 个测试）

- **S3 — Lance Namespace 端点**（2026-04-20）
  - `namespaces` 表约束修正为 `UNIQUE(name, format)`，修复双协议隔离策略
  - `CatalogStore` trait 增加 `AssetFormat` 参数到全部方法
  - Lance RFC-7807 错误模块实现
  - Lance Namespace 5 个端点全部实现（create/list/describe/drop/exists）
  - 测试覆盖（`adapter/tests/lance_namespace.rs`，6 个）：
    - 创建/获取 Namespace 正常流（含 properties）
    - 重复创建返回 409 + RFC-7807 错误体
    - 列出 Namespace 只返回对应格式（Iceberg/Lance 隔离）
    - 存在检查（true/false）
    - 删除成功 + 重复删除返回 404
    - 描述不存在的 Namespace 返回 404 + RFC-7807
  - `cargo build` / `cargo clippy` 零警告 / `cargo test` 全通过

- **S2 — Storage 基础**（2026-04-19）
  - `quasar-storage` 创建 refinery migration `V1__init.sql`（namespaces / assets / asset_versions）
  - `PgCatalogStore` 实现 `CatalogStore` trait 全部方法（含 CAS 冲突检测的 `commit_version`）
  - 测试覆盖（`storage/tests/integration.rs`，5 个）：
    - Namespace CRUD：创建/列出/获取/存在检查/删除
    - 重复创建 Namespace 返回 `AlreadyExists`
    - Asset CRUD：创建/列出/获取/存在检查/重命名/删除
    - 重命名冲突返回 `AlreadyExists`
    - 版本提交与加载：顺序提交 v1→v2，获取当前版本/指定版本/列表
    - 版本冲突：`previous_version_id` 不匹配返回 `Conflict`
    - NotFound 场景：namespace 不存在、asset 不存在、无版本记录
  - `cargo build` / `cargo clippy` 零警告通过

- **S1 — Core 层定义**（2026-04-19）
  - `quasar-core` 定义 `StoreError`、`AssetFormat`、`Namespace`、`Asset`、`AssetVersion`、`AssetCommitUpdate`
  - `CatalogStore` trait 完整签名定义（Namespace / Asset / Version 全生命周期）
  - `cargo build` / `cargo clippy` 零警告通过

- **S0 — 项目骨架**（2026-04-19）
  - Cargo workspace 初始化，4 个 crate 结构就位
  - `cargo build` / `cargo clippy` 通过
  - `quasar-server` 可启动，`/healthz` 返回 `ok`

---

## 测试覆盖总览

| 阶段 | 测试文件 | 测试数 | 覆盖场景 |
|------|---------|-------|---------|
| S2 | `storage/tests/integration.rs` | 5 | Namespace CRUD、Asset CRUD、版本提交/加载/冲突、NotFound |
| S3 | `adapter/tests/lance_namespace.rs` | 6 | Lance Namespace 创建/描述/列表/存在检查/删除、409 冲突、404 NotFound、格式隔离 |
| S4 | `adapter/tests/lance_table.rs` | 10 | Lance Table 声明/描述/列表/注册/注销/删除/存在检查/重命名、409 冲突、404 NotFound |
| S5 | `adapter/tests/lance_version.rs` | 6 | Lance 版本创建/列表/描述、409 版本冲突、404 NotFound、current_version 联动 |

**运行方式：**
```bash
cd quasar
cargo test                      # 全部 27 个测试
cargo test -p quasar-storage    # Storage 层 5 个
cargo test -p quasar-adapter    # Adapter 层 22 个
```

---

## 修订记录

### V1.6（2026-04-20）

- 更新：S5 状态标记为"已完成"
- 更新：当前阶段推进至 S6
- 新增：已完成里程碑记录 S5（含测试覆盖详情）
- 新增：S5 测试覆盖条目至测试覆盖总览表

### V1.5（2026-04-20）

- 更新：S4 状态标记为"已完成"
- 更新：当前阶段推进至 S5
- 新增：已完成里程碑记录 S4（含测试覆盖详情）
- 新增：S4 测试覆盖条目至测试覆盖总览表

### V1.4（2026-04-20）

- 更新：S3 状态标记为"已完成"
- 更新：当前阶段推进至 S4
- 新增：已完成里程碑记录 S3（含测试覆盖详情）
- 新增：测试覆盖总览章节
- 修正：S2/S3 里程碑补充测试覆盖明细

### V1.3（2026-04-19）

- 更新：S2 状态标记为"已完成"
- 更新：当前阶段推进至 S3
- 新增：已完成里程碑记录 S2

### V1.2（2026-04-19）

- 更新：S1 状态标记为"已完成"
- 更新：当前阶段推进至 S2
- 新增：已完成里程碑记录 S1

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
