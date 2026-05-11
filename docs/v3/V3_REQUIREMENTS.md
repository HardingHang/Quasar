# Quasar V3 需求分析文档

## 一、背景与目标

### 1.1 V2 遗留问题

Quasar V2 已完成功能开发，但在代码审查和架构复盘过程中，识别出数据模型存在根本性设计缺陷。V2 的问题清单记录在 `docs/v3/V3_PROBLEMS.md` 中，共 15 项，核心集中在：

- **数据模型层面**：`asset_subtype` 语义模糊（同时承担"格式标识"和"资产子类型"），硬编码枚举约束限制扩展，TabularAsset 与非表资产硬绑定
- **存储层层面**：CatalogStore trait 职责膨胀，StoreError 错误粒度太粗且存在信息泄露安全风险，SQL 查询分散无集中管理
- **API 设计层面**：Asset 操作强制要求 `format` 参数，UnifiedConfig 与 iceberg feature 隐式耦合
- **版本设计层面**：`version_key` 与 `version_order` 混用，存在不安全的字符串解析 fallback

### 1.2 V3 核心目标

V3 的核心目标是**彻底重构 V2 的数据模型**，解决上述遗留问题，建立一套**长期稳定、可扩展**的 Data Core Model。具体要求：

1. **Data Core Model 一经确定，未来版本不得进行大改动**。V3 的表结构、关系设计、字段语义必须经得起推敲，为后续版本（V4 及以后）的权限、安全、Git4Data 等功能奠定坚实基础。
2. 所有 V2 的设计缺陷必须在 V3 中得到根治，不能通过打补丁的方式掩盖。
3. 非表资产（如 Model）的扩展必须在 V3 数据模型中留有明确的扩展路径，不能是"伪预留"。

---

## 二、核心架构决策

### 2.1 决策清单

| # | 决策 | 状态 |
|---|------|------|
| 1 | **端点级隔离**：Domain 不绑定格式，不同格式通过不同 REST 端点区分 | 已确认 |
| 2 | **统一资产身份层 + 类型扩展表**：`assets` 承载身份，各类型有独立扩展表 | 已确认 |
| 3 | **3 层架构**：Domain → Namespace → Asset | 已确认 |
| 4 | **Namespace 单层 V3，嵌套后续单独设计** | 已确认 |
| 5 | **同一 Namespace 内资产名唯一**（不区分格式） | 已确认 |
| 6 | **资产类型与表格式可扩展**：新增类型/格式不应要求数据库 schema 迁移 | 已确认 |

### 2.2 端点级隔离

**核心原则：Domain 不绑定格式，格式由 REST 端点路径隐含。**

```
Domain 'prod'
  └── Namespace 'analytics'
        ├── Asset 'events' (format=iceberg)
        │     → /iceberg/v1/prod/namespaces/analytics/tables/events
        └── Asset 'embeddings' (format=lance)
              → /lance/v1/prod/namespaces/analytics/tables/embeddings
```

**选择端点级隔离的考量：**

1. **管理后台需要统一视图**：同一页面需要同时看到 Iceberg 表、Lance 数据集、Model 等所有资产。如果 Domain 绑定格式，管理后台需要跨 Domain 聚合，增加复杂度。
2. **Iceberg 和 Lance 是平等的一等公民**：两者都有完整的协议能力，不存在主次关系。Lance 不应被降级为"Generic Table"（无 update、无 schema 规范）。
3. **基础设施共享**：Iceberg 和 Lance 的元数据都存储在 Quasar 的 PostgreSQL 中，数据文件都在同一个 S3 bucket 下，拆分 Domain 没有技术收益。
4. **用户心智负担**：用户配置一个 Domain 连接即可管理所有资产，不需要为每种格式维护独立的 Domain 配置。

**代价**：
- 每个协议端点内部需要按 `format` 字段过滤，用错端点会返回 `TableNotFoundException`
- 端点过滤逻辑需要在各 handler 中重复实现（可通过中间件抽象）

---

## 三、层级架构设计

### 3.1 3 层架构

```
┌─────────────────────────────────────────────┐
│  第一层：Domain（顶层容器）                   │
│  • 存储配置边界（S3 bucket、warehouse 路径）  │
│  • 权限隔离边界                               │
│  • 多租户隔离边界（如果有）                   │
├─────────────────────────────────────────────┤
│  第二层：Namespace（业务组织）                │
│  • 逻辑分组（如 analytics、marketing、ml）   │
│  • V3 单层，嵌套 Namespace 后续单独设计       │
│  • 同一 Domain 内 Namespace 名唯一            │
├─────────────────────────────────────────────┤
│  第三层：Asset（实际资产）                    │
│  • table、model、fileset、topic 等           │
│  • 同一 Namespace 内资产名唯一（不区分格式）  │
│  • 格式信息在扩展表中（如 tabular_assets.format）│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Data Core Model 需求

本章只定义 V3 Data Core Model 必须满足的业务不变量和演进边界。DDL、索引、外键动作、Rust 结构体草案等设计细节由 `docs/v3/V3_DATA_MODEL_DESIGN.md` 承载。

### 4.1 核心不变量

V3 的 Data Core Model 必须满足以下要求，确保未来版本不需要推翻重建：

1. **资产身份与类型属性分离**：所有资产共享统一身份层；表、模型、文件集等类型的专有字段进入各自扩展层。
2. **格式信息不在资产身份层**：`iceberg`、`lance`、`delta` 等表格式属于表资产属性，不属于所有资产的公共身份。
3. **类型与格式可扩展**：新增资产类型或表格式不应要求修改核心表结构；实现层可以通过注册表、配置或插件机制校验合法值。
4. **版本模型同时保留原生标识与排序语义**：Catalog 必须能记录格式原生版本标识，也必须能按可比较顺序查询 latest；不得通过字符串解析 fallback 伪造版本顺序。
5. **核心完整性由数据库约束兜底**：Domain、Namespace、Asset、Version 之间的身份关系、唯一性和引用完整性不能只依赖应用层约定。
6. **删除必须显式且可控**：Domain / Namespace 这类上层容器不得因普通删除语句隐式级联删除大量下游资产；非空容器删除应返回冲突。
7. **性能约束与完整性约束并重**：必须为外键引用方、主要列表查询和 latest-version 查询提供必要索引；非必要 JSONB / 预留字段索引延后到有明确查询场景时再引入。
8. **敏感配置不得明文暴露**：Domain 级存储配置如涉及 credential，应存储 secret reference 或加密后的配置，并在 API 响应中脱敏。

### 4.2 层级与命名需求

V3 引入三层逻辑层级：

```
Domain → Namespace → Asset
```

- **Domain** 是顶层存储与治理容器，不绑定表格式。
- **Namespace** 是 Domain 内的业务组织单元。V3 只要求单层 Namespace；嵌套 Namespace 属于后续版本设计，不在 V3 DDL 中做半成品预留。
- **Asset** 是 Namespace 内的实际资产身份。V3 采用更严格的命名规则：同一 Namespace 内活动资产名唯一，不按资产类型或表格式放宽。

由于活动资产名在 Namespace 内唯一：

- 协议端点查询非匹配格式资产时返回 not found 语义，例如用 Lance 端点查询 Iceberg 表返回 `TableNotFoundException`。
- 协议端点创建表时，如果同名活动资产已被其他类型或格式占用，应返回冲突语义；具体错误格式按各标准协议适配层定义。
- Unified API 可以在不要求调用方传入格式的情况下定位单个资产。

### 4.3 表资产需求

V3 初始交付仍以表资产为主，必须同时支持 Iceberg 与 Lance：

- 表资产必须记录表格式、数据根路径、当前元数据位置和 schema 快照等表相关字段。
- Iceberg 和 Lance 的协议入口继续保持独立，并在存储查询中按表格式隔离。
- 表格式字段可扩展，不能通过硬编码数据库 CHECK 限死为 Iceberg / Lance。
- 如果后续表格式差异明显扩大，可以在设计层拆分为格式专属扩展表，但 V3 不把该拆分作为需求。

### 4.4 非表资产扩展需求

V3 不要求实现 Model / Fileset / Topic 等非表资产的完整 API，但 Data Core Model 必须提供真实扩展路径：

- 新增非表资产类型时，不应修改资产身份层的核心字段。
- 非表资产应通过独立扩展层承载专有字段。
- 权限、审计、标签、搜索等治理能力应优先绑定到统一资产身份，而不是绑定到某个表格式。

### 4.5 版本模型需求

V3 版本模型必须修复 V2 中 `version_key` 与 `version_order` 使用混乱的问题，但不能把所有格式压缩成单一整数版本号：

- `version_key` 表达格式原生版本标识，必须在同一 Asset 内唯一。
- `version_order` 表达可比较顺序，允许为空；支持自然递增版本的格式应写入该字段。
- latest 查询只能基于明确的排序字段或格式内权威指针，不能从 `version_key` 字符串解析得到 fallback 顺序。
- 版本前驱关系必须保证指向同一 Asset 下的版本；跨 Asset 前驱引用必须被拒绝。
- 表资产版本的元数据位置属于表版本扩展层，不应污染通用版本身份层。

### 4.6 删除与生命周期需求

V3 删除语义必须区分“容器删除”和“资产删除”：

- 非空 Domain 不可删除。
- 非空 Namespace 不可删除。
- 删除 Asset 可以清理该 Asset 下的扩展记录和版本记录，但必须由明确的 Asset 删除 API 触发。
- Unified API 删除 Catalog 记录时不删除或迁移对象存储中的数据文件；标准协议如定义了数据删除语义，则由对应协议适配器单独处理。
- 如果启用软删除，唯一性约束和查询语义必须以“活动资产”为边界，已软删除资产不得继续阻塞名称复用，除非产品明确要求保留占名。

### 4.7 详细设计承载

以下内容不在需求文档中展开，统一放入 `docs/v3/V3_DATA_MODEL_DESIGN.md`：

- DDL 草案
- 外键动作和级联范围
- 必要索引与延迟索引策略
- Rust Core 模型草案
- Store trait 拆分方案
- Domain 到标准协议路径 / prefix 的映射细节

---

## 五、V2 遗留问题闭环

### 5.1 闭环策略

V3_PROBLEMS.md 中的 15 个问题，按 V3 工作范围分为两类：

| 分类 | 问题数 | 处理策略 |
|------|--------|---------|
| **V3 核心解决** | 11 | 在 V3 开发周期内完成，是 V3 的交付物 |
| **V3 暂不解决** | 4 | 记录并 defer，不影响 V3 的核心目标（Data Core Model 重构） |

### 5.2 V3 核心解决（11 个问题）

| 问题 | 描述 | V3 解决方案 |
|------|------|------------|
| 问题 1 | AssetFormat / AssetType 枚举与数据库约束硬编码 | 数据库层取消硬编码 CHECK；Core 存储模型使用可扩展名称或注册表；标准协议适配器仍可保留 Iceberg/Lance 常量 |
| 问题 2 | asset_subtype 语义模糊 | 删除主表 `asset_subtype`，`format` 下放 `tabular_assets.format`；端点级隔离 |
| 问题 3 | TabularAsset 硬绑定 | trait 拆分：通用 `CatalogStore` + `TabularStore` + `VersionedStore` + `CasCommitStore` |
| 问题 4 | CatalogStore 职责膨胀 | 与问题 3 合并解决，trait 拆分 |
| 问题 6 | StoreError 粒度太粗 + 信息泄露 | `Internal` 改为结构体变体 `{msg, source}`；客户端脱敏；新增 `DatabaseUnavailable`、`Timeout` |
| 问题 7 | SQL 查询分散 | `storage/src/queries.rs` 常量模块集中管理 |
| 问题 8 | Asset 操作强制 format 参数 | 端点级隔离：端点路径隐含格式，不需要 `?format=iceberg` |
| 问题 10 | UnifiedConfig 与 iceberg 耦合 | `object_store` 改为公共依赖，移除条件编译 |
| 问题 12 | version_key vs version_order 混用 | 保留原生 `version_key` 与可选 `version_order`，明确语义边界；禁止从字符串解析 fallback 得到排序 |
| 问题 13 | 字符串匹配判断错误类型 | 新增 `NamespaceNotEmpty` 变体，SQL 状态码直接构造 |
| 问题 14 | lance_version_to_response unwrap | 与问题 12 合并解决 |

### 5.3 V3 暂不解决（4 个问题）

| 问题 | 描述 | 不解决理由 | 演进方向 |
|------|------|-----------|---------|
| 问题 5 | 分页 OFFSET 性能隐患 | 不影响数据模型重构 | 后续版本引入 cursor-based 分页 |
| 问题 9 | Unified API 不暴露 Asset 创建 | 资产创建涉及格式特有语义，不影响数据模型 | 后续版本设计格式无关的请求格式 |
| 问题 11 | Iceberg 实时读取对象存储 | 性能优化，不影响正确性和数据模型 | 后续版本引入 metadata 缓存 |
| 问题 15 | 对象存储与数据库非原子性 | 不影响正确性 | 后台清理任务（已记录在 docs/EVOLUTION.md） |

---

## 六、附录：V3 问题闭环映射

| V3_PROBLEMS 编号 | 问题描述 | V3 处理策略 | 归属 |
|-----------------|---------|------------|------|
| 1 | AssetFormat / AssetType 枚举硬编码 | 核心解决数据库与 Core 模型硬约束；热插拔机制后续演进 | 核心解决 |
| 2 | asset_subtype 语义模糊 | 核心解决 | 核心解决 |
| 3 | TabularAsset 硬绑定 | 核心解决 | 核心解决 |
| 4 | CatalogStore 职责膨胀 | 核心解决 | 核心解决 |
| 5 | 分页 OFFSET 性能隐患 | 暂不解决 | 暂不解决 |
| 6 | StoreError 粒度太粗 | 核心解决 | 核心解决 |
| 7 | SQL 查询分散 | 核心解决 | 核心解决 |
| 8 | Asset 操作强制 format 参数 | 核心解决 | 核心解决 |
| 9 | Unified API 不暴露创建 | 暂不解决 | 暂不解决 |
| 10 | UnifiedConfig 与 iceberg 耦合 | 核心解决 | 核心解决 |
| 11 | Iceberg 实时读取对象存储 | 暂不解决 | 暂不解决 |
| 12 | version_key vs version_order | 核心解决 | 核心解决 |
| 13 | 字符串匹配判断错误类型 | 核心解决 | 核心解决 |
| 14 | lance_version_to_response unwrap | 核心解决 | 核心解决 |
| 15 | 对象存储与数据库非原子性 | 暂不解决 | 暂不解决 |

**统计：** 核心解决 11 个，暂不解决 4 个。

---

## 七、遗留事项：Lance 版本管理与 Catalog Service 的一致性

### 7.1 问题背景

Lance 格式的设计哲学是"数据集自包含"——版本管理完全在存储层（S3）完成，通过 `_versions/` 目录和 `_latest.manifest` 文件维护版本历史。这与 Iceberg 的设计根本不同：

| | Iceberg | Lance |
|--|---------|-------|
| **Catalog 角色** | **必需**（没有 `metadata_location` 指针，客户端找不到 metadata.json） | **可选**（客户端自己扫描 `_versions/` 即可发现所有版本） |
| **写入流程** | 客户端 → Catalog Service → 服务端写 S3 | 客户端 → Lance SDK → 直接写 S3 |
| **版本权威** | Catalog Service（DB 中的 `metadata_location` 指针） | S3（`_latest.manifest`） |

在 Quasar 的 Lance 端点中，当前流程是：

1. 客户端通过 Lance SDK 写入数据 → Lance 内部生成 manifest 文件到 S3
2. 客户端再通知 Quasar Catalog Service："我创建了版本 N"
3. Quasar 将版本记录写入 PostgreSQL

**问题**：步骤 1 和步骤 2 之间没有原子性保证。如果步骤 1 成功但步骤 2 失败，S3 上存在版本 N 的 manifest，但 Quasar DB 中没有该版本的记录。

### 7.2 影响分析

#### 场景还原

```
操作 A：客户端写入 Lance 数据
  ├── Lance SDK 生成 42.manifest → 写入 S3
  ├── _latest.manifest 更新为 42
  └── 通知 Quasar Catalog Service
       │
       X── 网络超时 / 服务宕机 / DB 故障
       │
      失败 → DB 中没有版本 42 的记录

S3 状态：版本 41, 42（latest=42）
DB  状态：版本 41（latest=41）
```

#### 后果

| 后果 | 说明 |
|------|------|
| **版本历史断裂** | DB 中版本从 41 跳到 43（如果后续写入成功注册），缺少 42 |
| **静默语义漂移** | 客户端从 DB 获取 "最新版本 = 41"，但 Lance SDK 实际基于 S3 上的 42 写入 43。客户端完全不知情 |
| **管理后台视图不准确** | 管理后台通过 DB 查询版本列表，看不到孤儿版本 |

**关键限制**：Lance SDK 的所有写入操作（`append`、`merge_insert`、`overwrite`）都基于 S3 上的 `_latest.manifest`，**不查询** Catalog Service。因此 Catalog Service 的 DB 记录对 Lance SDK 的写入行为没有影响。

### 7.3 社区动态

#### Lance Issue #6595：并发写入时的静默数据丢失

Lance 社区在生产环境中遇到了更严重的并发问题：当多个 worker 同时追加数据时，某些存储后端（如 Tencent COS）的 `put-if-not-exists` 会静默降级为普通 PUT，导致"后写入者覆盖先写入者"，数据丢失且无错误提示。

#### Lance Discussion #5229：Catalog-Aware Table Commits（RFC 阶段）

Lance 核心贡献者（jackye1995，AWS）于 2025-11-12 发布了重新设计方案，目标让 Lance 提交像 Iceberg 一样经过 Catalog：

- **核心思想**：所有写入必须通过 `CommitTable` 端点，服务端执行实际的 S3 写入
- **访问控制**：写入者凭证禁止直接写 `_versions/` 目录，强制走 Catalog
- **版本解析**：仍基于 Lance manifest 命名方案（保留 Lance 的设计哲学）
- **冲突解决**：服务端 OCC + `conflict_resolver.rs` 规范

**状态**：0 回复，仍在 Ideas 阶段，尚未进入实现。

> 参考：[lance-format/lance Discussion #5229](https://github.com/lance-format/lance/discussions/5229)

#### 其他开源项目的现状

| 项目 | 处理方式 |
|------|---------|
| **Apache Polaris** | 明确标注 `managed_versioning=false`，"No commit coordination: Concurrent writers must be managed at the application level" |
| **Apache Gravitino** | 提供 Lance REST 集成，但写入通过 Spark/Ray → Lance SDK → 直接写 S3，不介入版本协调 |

**结论**：所有支持 Lance 的开源项目都面临相同的问题，当前均未完美解决。

### 7.4 V3 处理策略

V3 **暂不实现** Catalog-Aware Commits，理由：

1. Lance 社区自身尚未实现（RFC 阶段）
2. V3 核心目标是数据模型重构，不是 Lance 协议重写
3. 修改 Lance 客户端行为需要生态配合，超出 V3 范围

**V3 采用的做法**：

1. **维持现状**：客户端写 S3 + Catalog Service 事后记录
2. **参考 Polaris 模式**：诚实标注限制，不假装提供无法保证的强一致性
3. **明确语义**：`create_version` 本质是"注册已存在的版本"，不是"协调写入"
4. **后台清理任务**：定期扫描 S3 `_versions/` 与 DB 的差异（与 V3_PROBLEMS #15 合并处理）

### 7.5 未来演进方向

1. **短期（V3 及之后）**：维持最终一致语义，关注 Lance 社区 RFC 进展
2. **中期**：一旦 Lance 社区实现 Catalog-Aware Commits（`CommitTable` 端点），Quasar 适配：
   - 新增 `POST /lance/v1/table/{id}/commit` 端点
   - 服务端执行 S3 写入 + PostgreSQL 记录（同一事务或协调机制）
   - 客户端不再直接写 `_versions/`
3. **长期**：Lance 端点和 Iceberg 端点统一为"服务端控制写入"模式，Catalog Service 成为所有格式提交的唯一权威

---

**登记日期**：2026-05-11  
**相关文档**：`docs/v3/V3_PROBLEMS.md` 问题 15（对象存储与数据库非原子性）、`docs/EVOLUTION.md`
