# Quasar V4 文档入口

> **日期**: 2026-05-18
> **状态**: V4 文档结构入口

V4 采用小版本推进，避免把 Iceberg REST Catalog 全量能力、Spark E2E、凭证安全、视图、scan planning 和运维增强压入同一次交付。

## 阅读顺序

| 顺序 | 文档 | 作用 |
|------|------|------|
| 1 | `V4_OFFICIAL_REST_API.md` | 固定 Iceberg 1.10.x 官方 REST API 基线，并列出 V4.0 / V4.1 / V4.2 / V4.3 范围矩阵 |
| 2 | `V4_REQUIREMENTS.md` | 定义 V4.0 必须交付范围、后续候选小版本和验收清单 |
| 3 | `V4_DESIGN.md` | 承接 V4.0 的具体设计、数据模型、接口、错误映射和测试设计 |
| 4 | `V4_1_REQUIREMENTS.md` | V4.1 需求澄清与范围定义（排除鉴权/安全/凭证功能） |
| 5 | `V4_1_DESIGN.md` | 承接 V4.1 的具体设计、数据模型、接口、错误映射和测试设计 |
| 6 | `V4_2_REQUIREMENTS.md` | V4.2 需求澄清与范围定义（Views 和 Scan Planning） |
| 7 | `V4_2_DESIGN.md` | 承接 V4.2 的具体设计、数据模型、接口、错误映射和测试设计 |
| 8 | `PROGRESS.md` | 跟踪 V4 小版本开发进度 |

## 小版本口径

| 小版本 | 定位 |
|--------|------|
| V4.0 | Spark-ready Iceberg baseline |
| V4.1 | REST table capability completion candidate |
| V4.2 | Views and scan planning candidate |
| V4.3 | Operational hardening candidate |

V4.0 只承诺 Spark 3.5 + Iceberg 1.10.x 的关键路径。后续候选小版本必须在进入实现前补充对应设计与测试矩阵。
