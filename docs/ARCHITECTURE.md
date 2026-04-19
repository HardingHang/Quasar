# 项目定位与设计目标

**项目名称：Quasar**

## 寓意

Quasar（类星体）是宇宙中最明亮、能量最集中的天体之一。它象征着元数据底座作为数据生态的核心引力源——无论数据资产分散在何处（Iceberg 表、Lance 表、AI 模型、特征等），都能被 Quasar 清晰索引、高效发现，并以极高的性能响应每一次查询。

## 定位

Quasar 是一个面向 Lakehouse 架构的、独立的通用 Catalog Service 组件。它是可纳管多种数据资产的元数据底座。

## 可纳管的资产类型

包括但不限于：

- Iceberg 表
- Lance 表
- AI 模型
- 特征（Feature）
- 以及后续可能扩展的数据集、指标、标签等其他资产

## 设计目标

1. **协议开放**：对每种资产类型，忠实实现其上游标准协议，而非发明私有 API。Iceberg 表遵循 Iceberg REST Catalog 规范，Lance 表遵循 Lance REST Namespace 规范，确保各自生态的引擎和客户端零改动接入。
2. **模型通用**：核心对象模型不绑定任何单一数据格式，任何资产类型都可以注册到 Catalog 中。各协议适配器将上游规范映射到统一的核心模型。
3. **一致性保障**：对 Iceberg 等需要原子提交的资产类型，通过乐观并发控制保证元数据更新的正确性。
4. **性能优先**：采用 Rust + axum + tokio-postgres 构建，追求低延迟、高并发、小内存占用。
5. **部署简洁**：服务本身无状态，单二进制部署，依赖单一 PostgreSQL 实例即可运行。
6. **可扩展**：通过清晰的 crate 分层，后续新增资产类型或接入协议只需扩展对应 crate，不侵入核心。

## 双协议架构

Quasar 的核心定位是统一管理多种数据格式（Iceberg、Lance 及未来扩展）。每种格式有其独立的标准协议和客户端生态，协议之间在端点语义、错误格式、分页方式、并发控制等方面存在天然差异。为避免不同格式在 Namespace 和 Table 命名上相互干扰，同时允许同一份业务数据以不同格式并存（例如同一数据集既有 Iceberg 表也有 Lance 表），Quasar 在单一进程内为每套协议暴露独立的路径前缀，内部通过统一的存储层和核心模型承载，并用 `format` 字段在逻辑上隔离不同格式的资产空间。

```
                      ┌───────────────────────────────────────┐
                      │            Quasar Server               │
                      │                                       │
 Spark / Trino        │   /iceberg/v1/...                     │
 Flink (Iceberg)  ───►│   ┌─────────────────────┐            │
                      │   │  Iceberg REST        │            │
                      │   │  Catalog Adapter     │──┐         │
                      │   └─────────────────────┘  │         │
                      │                            ▼         │
                      │                    ┌──────────────┐  │
                      │                    │  Core Model   │  │
                      │                    │  + Storage    │  │
                      │                    │  (PostgreSQL) │  │
                      │                    └──────────────┘  │
                      │                            ▲         │
 Lance SDK            │   ┌─────────────────────┐  │         │
 (Python/Rust/Java)───►│   │  Lance REST         │──┘         │
 Spark (Lance)        │   │  Namespace Adapter   │            │
 Ray / Trino          │   └─────────────────────┘            │
                      │   /lance/v1/...                       │
                      └───────────────────────────────────────┘
```

两套协议各自遵循上游规范定义的路径前缀和语义：

- **Iceberg REST Catalog**：路径前缀 `/iceberg/v1/...`，其中 `/v1` 是 Iceberg REST OpenAPI 规范自带的版本前缀。
- **Lance REST Namespace**：路径前缀 `/lance/v1/...`，其中 `/v1` 是 Lance REST Namespace 规范自带的版本前缀。

### Namespace 隔离策略

Quasar 的核心目标是统一纳管多种数据格式，而不同格式的资产在命名上可能重叠。例如，同一业务团队可能同时维护 Iceberg 和 Lance 两种格式的 `users` 表。为避免命名冲突，同时允许各格式客户端在各自协议空间内自由操作而不受其他格式干扰，Quasar 将每套协议的 Namespace/Table 管理设计为逻辑隔离的空间：

- Iceberg 端点下的 Namespace 和 Table 只对 Iceberg 客户端可见。
- Lance 端点下的 Namespace 和 Table 只对 Lance 客户端可见。
- 两个空间中允许存在同名的 Namespace 和 Table，互不干扰。

在核心模型层面，通过一个 `format` 字段（如 `iceberg` / `lance`）区分不同格式的资产，存储在同一个 PostgreSQL 实例中。唯一约束为 `(namespace_name, table_name, format)`。

这种设计带来三个好处：
1. **命名自由**：用户不需要为不同格式的同一张表人为改名；
2. **协议独立**：各协议适配器独立演进，新增格式时不影响现有格式的行为；
3. **扩展简单**：未来新增格式（如 Delta Lake）只需增加一个新的 `format` 枚举值。

## Crate 分层

```
quasar/
├── quasar-core          # 核心领域模型与 trait 定义
│                        # - Namespace / Table 等领域对象（含 format 字段）
│                        # - CatalogStore trait（存储抽象）
│                        # - 不依赖具体框架或存储实现
│
├── quasar-storage       # 存储层实现
│                        # - PostgreSQL 实现 CatalogStore trait
│                        # - 连接池管理（deadpool-postgres）
│                        # - 数据库 schema 迁移（refinery）
│
├── quasar-adapter       # 协议适配层
│   ├── iceberg          # Iceberg REST Catalog 适配
│   │                    # - 将 /iceberg/v1/... 请求映射到 core 层操作
│   │                    # - 查询时自动注入 format=iceberg 过滤条件
│   │                    # - 实现 Iceberg 特有语义（CAS commit 等）
│   └── lance            # Lance REST Namespace 适配
│                        # - 将 /lance/v1/... 请求映射到 core 层操作
│                        # - 查询时自动注入 format=lance 过滤条件
│                        # - 实现 Lance 特有语义（Declare/Register/Version 等）
│
└── quasar-server        # 服务入口
                         # - axum 路由注册与中间件（tracing、错误处理）
                         # - 配置加载（环境变量 / 配置文件）
                         # - Health / Readiness 端点
                         # - main 函数与优雅关机
```