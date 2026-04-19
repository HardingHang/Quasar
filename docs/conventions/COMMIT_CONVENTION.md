# Quasar Git Commit Message 规范

> 本规范约束 Quasar 项目所有 git commit 的 message 格式，确保提交历史清晰可读，便于追溯变更原因和影响范围。

---

## 格式总览

```
<type>(<scope>): <subject>

<body>

<footer>
```

- 第 1 行（header）为**必填**，不可省略
- `<body>` 和 `<footer>` 为**可选**
- header 总长度不超过 72 字符
- `<subject>` 使用祈使句（现在时），描述**做了什么**而非**做了什么修改**

---

## type（修改类型）

| type | 含义 | 示例场景 |
|------|------|---------|
| `feat` | 新增功能 | 新增 handler、新增端点、新增配置项 |
| `fix` | 修复 bug | 修复 CAS 冲突判断错误、修复路由拼写 |
| `docs` | 文档变更 | 修改需求文档、更新 API 说明、补充注释 |
| `style` | 代码格式调整 | 格式化、删除多余空行、调整 import 顺序 |
| `refactor` | 重构 | 函数提取、模块拆分、重命名变量（无行为变更） |
| `test` | 测试相关 | 新增/修改单元测试、集成测试脚本 |
| `chore` | 杂项 | 依赖升级、CI 配置调整、脚本更新 |
| `deps` | 依赖变更 | Cargo.toml 中添加/升级/移除 crate |

---

## scope（影响范围）

scope 标识本次修改主要涉及的模块。Quasar 为多 crate 项目，scope 应尽量精确。

| scope | 含义 |
|-------|------|
| `core` | `quasar-core` crate（数据结构、trait 定义） |
| `storage` | `quasar-storage` crate（PostgreSQL 实现、迁移脚本） |
| `adapter/iceberg` | Iceberg REST Catalog 适配器 |
| `adapter/lance` | Lance REST Namespace 适配器 |
| `server` | `quasar-server` crate（路由注册、配置、main） |
| `tests` | 测试脚本（集成测试、端到端测试） |
| `ci` | CI/CD 配置（GitHub Actions、Dockerfile） |
| `deps` | 依赖管理（Cargo.toml、lock 文件） |
| `all` | 跨多个 crate 的全局变更 |

---

## subject（简述）

- 不超过 50 字符
- 使用祈使句（命令式），如 "add" 而非 "added" 或 "adds"
- 描述**变更本身**，不解释原因（原因放在 body 中）
- 中英文均可，同一仓库内保持一致即可

**示例：**

```
feat(adapter/lance): add create_namespace handler
fix(storage): handle unique constraint conflict in create_namespace
refactor(core): extract AssetCommitUpdate from CatalogStore trait
docs(MVP): update Iceberg CAS commit flow description
test(adapter/iceberg): add concurrent commit conflict test case
deps(server): add object_store crate for S3 metadata access
```

---

## body（详细说明）

当单次提交涉及复杂逻辑或需要解释**为什么**做这个变更时，在 body 中补充。

- 每行不超过 72 字符
- 用空行与 header 隔开
- 说明**为什么**做，而非**做了什么**（做了什么 header 已说明）
- 涉及 issue 或设计决策时，引用相关文档或讨论

**示例：**

```
feat(adapter/lance): add create_namespace handler

实现 Lance REST Namespace 规范中的 create 端点。
POST /lance/v1/namespace/{id}/create

返回 RFC-7807 格式的错误响应，重复创建时返回 409。
```

---

## footer（脚注）

用于标记破坏性变更、关联 issue、关闭 ticket 等。

| 标记 | 用途 |
|------|------|
| `BREAKING CHANGE:` | 标记不兼容变更，说明迁移方式 |
| `Closes #N` | 关闭关联的 issue 或 PR |

**示例：**

```
refactor(core): rename StoreError::AlreadyExists to Exists

统一错误命名风格，与 Lance 规范中的错误码对齐。

BREAKING CHANGE: StoreError::AlreadyExists 已更名为 Exists，
所有引用此变体的代码需同步更新。
```

---

## 禁止的写法

| 反例 | 问题 |
------|------|
| `update` | 无 type，无 scope，subject 过于笼统 |
| `fix bug` | 无 scope，未说明修复的是哪个 bug |
| `feat: some changes` | subject 无意义，未说明具体做了什么 |
| `fix(adapter): fix the handler` | subject 重复 type，且未说明具体修复了什么 |
| `docs: update` | 无 scope，subject 过于笼统 |

---

## 版本号变更的提交

当修改文档版本号（如 `MVP_DESIGN.md` 从 V1.0 升级到 V1.1）时：

```
docs(MVP): bump DATA_MODEL to V1.1

- 修正 assets 表唯一约束描述
- 补充 NamespaceNotEmpty 状态码说明
```

---

## 多文件小修的情况

如果一次提交修改了多个文件，但都属于同一 scope 下的同类变更：

```
style(adapter): unify error response formatting across handlers

统一 Iceberg 和 Lance 适配器中错误响应的序列化逻辑，
消除重复代码。
```

如果一次提交跨越多个 scope（如同时改了 core 和 storage）：

```
feat(all): add format field to Namespace

- core: 新增 AssetFormat 枚举
- storage: 在 create_namespace 中持久化 format 字段
```

---

## 分支策略

Quasar 采用 **main + dev + feature** 三层分支模型。

### 分支定义

| 分支 | 用途 | 规则 |
|------|------|------|
| `main` | 稳定分支 | 只接受 `dev` 分支的合并，不直接提交代码 |
| `dev` | 开发集成 | 接受 feature 分支合并，阶段性里程碑完成后合并到 `main` |
| `feature/s{N}-{name}` | 阶段开发 | 每个开发阶段一个分支，从 `dev` 切出，完成后 PR 合并到 `dev` |

### feature 分支命名

```
feature/s0-project-skeleton
feature/s1-core-definition
feature/s2-storage-basics
feature/s3-lance-namespace
feature/s4-lance-table
feature/s5-lance-version
feature/s6-infrastructure
feature/s7-lance-integration
feature/s8-iceberg-crud
feature/s9-iceberg-cas
feature/s10-spark-integration
```

### 工作流

1. 从 `dev` 切出 feature 分支：`git checkout -b feature/s3-lance-namespace dev`
2. 在 feature 分支上开发，按本规范提交 commit
3. 阶段完成后，发起 PR 合并到 `dev`
4. Phase 1（S0~S7）全部完成后，`dev` 合并到 `main`，打 tag `v0.1.0`
5. Phase 2（S8~S10）完成后，`dev` 合并到 `main`，打 tag `v1.0.0`

---

## 修订记录

### V1.1

- 新增：分支策略章节（main + dev + feature 三层模型、feature 分支命名规则、工作流）

### V1.0

- 新增：type 分类表（8 种类型）
- 新增：scope 范围表（9 个模块标识）
- 新增：header、body、footer 三段式格式说明
- 新增：示例与反例
- 新增：版本号变更和多 scope 提交的处理方式

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。
