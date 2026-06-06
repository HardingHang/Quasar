# Quasar V4.1 需求分析文档

> **版本**: V1.2
> **日期**: 2026-06-05
> **状态**: 需求澄清完成，待设计文档承接
> **定位**: REST Table Capability Completion（排除鉴权/安全/凭证功能）
>
> 本文档承担 V4.1 阶段的需求澄清与分析职责。
> 官方端点清单以 `docs/v4/V4_OFFICIAL_REST_API.md` 为唯一权威。
> 设计与实现细节由后续 `V4_1_DESIGN.md` 承接。
>
> **前置依赖**: V4.0 必须完成并通过验收；`V4_1_DESIGN.md` 必须完成并经评审后方可进入实现阶段。

---

## 一、文档边界

| 文档 | 职责 | 规则 |
|------|------|------|
| `README.md` | V4 文档入口与阅读顺序 | 不承载需求细节 |
| `V4_OFFICIAL_REST_API.md` | 官方协议基线、端点清单、小版本范围矩阵 | 不写 Quasar 内部实现方案 |
| `V4_REQUIREMENTS.md` | V4.0~V4.3 总需求基线 | 不写 DDL、具体 Rust 类型替换步骤 |
| **本文档** | V4.1 需求澄清与范围定义 | 不写具体实现方案 |
| `V4_1_DESIGN.md`（后续创建） | V4.1 设计方案 | 承接实现策略、数据模型、接口、错误映射、测试设计 |
| `PROGRESS.md` | V4 阶段进度 | V4.1 完成后更新 |

**前置条件**: 进入 V4.1 实现阶段前，V4.0 必须完成并通过验收；`V4_1_DESIGN.md` 必须完成并经评审批准。

---

## 二、前置依赖与准入条件

### 2.1 前置依赖

| 依赖项 | 要求 |
|--------|------|
| V4.0 验收 | 必须完成并通过全部验收标准 |
| `V4_1_DESIGN.md` | 必须完成并经评审批准，方可进入实现阶段 |
| 测试基础设施 | 沿用 V4.0 的容器化 Postgres 环境，支持并发测试 |

### 2.2 准入条件重新定义

`V4_REQUIREMENTS.md` §5.1 原定义的准入条件为"完成鉴权/凭证安全边界设计"和"明确 Domain.storage_config 中 credential/secret 的存储、脱敏、轮换与最小权限策略"。

由于 V4.1 明确排除所有鉴权/安全/凭证功能（见 §三），上述准入条件不再适用于 V4.1。原准入条件推迟到后续独立版本（或 V4.x 安全专项）评估。

V4.1 的准入条件调整为：
1. V4.0 完成并通过验收
2. `V4_1_DESIGN.md` 完成，包含事务原子性方案、TableUpdate actions 实现设计、multi-warehouse 参数校验设计
3. 测试矩阵与验收清单明确定义

---

## 三、V4.1 定位与范围决策

### 3.1 范围决策背景

V4.1 原始候选范围包含涉及安全/鉴权/凭证的功能（vended credentials、S3 Signer API、encryption key actions）。经需求澄清，**V4.1 明确排除所有安全/鉴权/凭证相关功能**，这些功能待后续独立版本（或 V4.x 安全专项）评估。

### 3.2 V4.1 最终范围

| 类别 | 功能 | 说明 |
|------|------|------|
| **Multi-table transactions** | `POST /v1/{prefix}/transactions/commit` | 原子性批量提交方案（见 §4.1） |
| **TableUpdate 补全** | Statistics（4 项）+ `remove-schemas` | 补齐 V4.0 返回 501 的非安全类 actions |
| **Multi-warehouse 最小支持** | `NoSuchWarehouse` 错误行为 | 仅处理 warehouse 参数校验，不涉及多后端隔离 |

### 3.3 V4.1 明确不做

1. **Vended credentials**（`GET /tables/{table}/credentials`）— 涉及凭证签发与最小权限策略，属于安全专项。
2. **S3 Signer API**（`/v1/aws/s3/sign`）— 涉及请求签名与密钥管理，属于安全专项。
3. **Encryption key actions**（`add-encryption-key`、`remove-encryption-key`）— 涉及密钥存储与轮换，属于安全专项。
4. **鉴权/认证/授权** — 任何生产级安全边界设计。
5. **多 warehouse 存储后端隔离** — 仅处理参数校验和错误响应，不扩展配置模型支持不同 S3 bucket。

---

## 四、功能详细需求

### 4.1 Multi-table Transactions

#### 4.1.1 协议端点

```
POST /v1/{prefix}/transactions/commit
```

接收 `CommitTransactionRequest`，其中包含多个表的 `TableUpdate` 列表，服务端需按表分组后执行提交。

#### 4.1.2 功能性要求

**原子性要求**：
- 多表提交必须保证原子性：任一表提交失败时，所有表的状态必须保持不变（客户端视角）。
- 提交失败后，必须返回包含失败表名和失败原因的错误响应。
- 对象存储层面可能产生孤儿文件（已写入但未被 catalog 引用的 metadata.json），容忍此现象，孤儿清理推迟到 V4.3。

**一致性约束**：
- 服务端必须保证多表事务的 `metadata_location` 更新在数据库层面的原子性。
- 由于 REST API 的无状态特性，客户端通过独立请求读取各表时无法保证跨表读取一致性。此限制是 REST Catalog 协议的固有特性，非 Quasar 特有。

**具体实现方案**（如对象存储预写顺序、PostgreSQL 事务策略、锁顺序等）由 `V4_1_DESIGN.md` 承接。

#### 4.1.3 错误处理

| 场景 | 响应 |
|------|------|
| 单表 CAS 冲突 | 返回 409，包含冲突表信息 |
| 对象存储写入失败 | 返回 500，不执行任何 DB 操作 |
| 请求中表不存在 | 返回 404，不执行任何 commit |
| 请求格式错误 | 返回 400 |

#### 4.1.4 验收标准

- [ ] 单事务成功提交 2~3 个表的更新，返回 204。
- [ ] 单事务中部分表 CAS 失败时，所有表状态保持不变（原子性验证）。
- [ ] 对象存储写入失败后，不执行任何 DB 操作，返回 500。
- [ ] 设计层面保证事务与单表 commit 使用一致的锁顺序，避免死锁风险。
- [ ] 适配器集成测试覆盖成功路径、对象存储写入失败、单表 CAS 冲突三种场景。

---

### 4.2 TableUpdate 补全

#### 4.2.1 需实现的 Actions

补齐 V4.0 中返回 501 的非安全类 TableUpdate actions，共 **5 项**：

| Action | Wire Name | 描述 |
|--------|-----------|------|
| SetStatistics | `set-statistics` | 设置表级统计信息 |
| RemoveStatistics | `remove-statistics` | 移除表级统计信息 |
| SetPartitionStatistics | `set-partition-statistics` | 设置分区级统计信息 |
| RemovePartitionStatistics | `remove-partition-statistics` | 移除分区级统计信息 |
| RemoveSchemas | `remove-schemas` | 移除指定 schema（保留至少一个） |

#### 4.2.2 功能性要求

**Statistics actions（4 项）**：
- 参考 Iceberg 1.10.x OpenAPI 中 `StatisticsFile` / `PartitionStatisticsFile` 结构。
- `set-*` 时更新 metadata 中 `statistics` / `partition-statistics` 列表。
- `remove-*` 时从列表中移除匹配 `snapshot-id` 的条目。
- 统计信息文件本身（JSON）由客户端生成并上传至对象存储，服务端只更新 metadata 中的引用。
- 统计信息持久化不纳入 V4.1 范围（无需 PostgreSQL 持久化）。
- **行为待设计确认**：若引用的统计信息文件在对象存储中不存在，服务端处理方式（记录警告不阻塞 / 返回错误）需在设计中参考 Iceberg Java 实现确定。

**RemoveSchemas**：
- 接收 `schema-ids` 列表，从 metadata 的 `schemas` 列表中移除对应 ID 的 schema。
- 必须保留至少一个 schema（当前 schema 不可被移除）。
- 若试图移除不存在的 schema ID，静默忽略（与 Iceberg 规范一致）。
- 若移除后只剩一个 schema，需确保该 schema 的 ID 等于 `current-schema-id`。

#### 4.2.3 错误处理

| 场景 | 响应 |
|------|------|
| RemoveSchemas 试图移除当前 schema | 返回 400 |
| RemoveSchemas 移除后无 schema 剩余 | 返回 400 |
| 其他 commit 通用错误 | 复用现有错误映射 |

#### 4.2.4 验收标准

- [ ] 5 项 actions 均能在单表 commit 中正确应用。
- [ ] Statistics actions 支持在 transactions 批量提交中与其他 actions 混合使用。
- [ ] RemoveSchemas 的边界条件（移除当前 schema、移除不存在 ID 静默忽略）均有测试覆盖。
- [ ] `V4_OFFICIAL_REST_API.md` 和 `/v1/config` 的 `endpoints` 字段同步更新。

---

### 4.3 Multi-warehouse 最小支持

#### 4.3.1 需求范围

仅实现**参数校验层**的支持，不扩展数据模型或配置：

- 所有 Iceberg REST 端点（config、namespace、table 等）在携带 `warehouse` 查询参数时，校验该 warehouse 是否存在于服务端配置中。
- 若 warehouse 不存在，返回 `NoSuchWarehouseException` 错误（Iceberg 错误模型）。
- 若 warehouse 存在或无 warehouse 参数，保持现有 V4.0 行为不变。
- 不涉及多 warehouse 的存储后端隔离（不同 warehouse 使用不同 S3 bucket 等）。

**当前配置语义**：V4.1 沿用 V4.0 的单一 warehouse 配置（默认 warehouse）。任何非默认值的 warehouse 参数均返回 `NoSuchWarehouseException`。

#### 4.3.2 错误响应

```json
{
  "error": {
    "message": "Warehouse does not exist: {warehouse}",
    "type": "NoSuchWarehouseException",
    "code": 404
  }
}
```

#### 4.3.3 验收标准

- [ ] 携带不存在的 warehouse 参数访问 config 端点，返回 404 + `NoSuchWarehouseException`。
- [ ] 携带默认 warehouse 参数或无参数，保持现有行为。
- [ ] 所有 Iceberg REST 端点（namespace、table、transactions 等）在携带无效 warehouse 时同样返回 `NoSuchWarehouseException`。

---

## 五、非功能要求

- V4.1 继续不引入生产鉴权语义。
- 客户端错误响应继续遵循 Iceberg error model，敏感信息脱敏。
- 生产代码不得使用 `unwrap()` / `expect()`。
- SQL 继续集中在 storage 查询模块，禁止字符串拼接 SQL。
- 新增或修改测试后同步维护 `docs/TEST_MATRIX.md`。
- `/v1/config` 的 `endpoints` 字段必须按实际已实现能力逐步增加，不得提前声明未实现 endpoint。

---

## 六、验收清单

V4.1 完成条件：

**前置条件检查**：
- [ ] V4.0 已完成并通过全部验收标准。
- [ ] `V4_1_DESIGN.md` 已完成并经评审批准。

**功能验收**：
- [ ] `POST /v1/{prefix}/transactions/commit` 端点实现并通过集成测试（成功路径 + 单表 CAS 冲突回滚 + 对象存储写入失败）。
- [ ] 5 项 TableUpdate actions（statistics 4 项 + remove-schemas）实现并通过集成测试。
- [ ] Multi-warehouse `NoSuchWarehouseException` 错误行为实现并通过测试（覆盖所有 Iceberg REST 端点）。

**文档与配置同步**：
- [ ] `V4_OFFICIAL_REST_API.md` 的 V4.1 范围列已同步更新，反映排除项（credentials、S3 Signer、encryption key）。
- [ ] `/v1/config` 实际 `endpoints` 输出与已实现能力一致（新增 transactions 端点声明）。
- [ ] `docs/TEST_MATRIX.md` 已同步记录新增测试。
- [ ] `V4_1_DESIGN.md` 和 `PROGRESS.md` 已按实际实现结果更新。

---

## 七、与 V4 总需求的对应关系

| V4_REQUIREMENTS.md 原始条目 | V4.1 处理方式 |
|---------------------------|--------------|
| §5.1 `POST /v1/{prefix}/transactions/commit` | ✅ 纳入 |
| §5.1 `GET .../tables/{table}/credentials` | ❌ 排除，推迟到后续安全专项版本 |
| §5.1 S3 Signer API `/v1/aws/s3/sign` | ❌ 排除，推迟到后续安全专项版本 |
| §5.1 TableUpdate 全集（statistics、remove-schemas、encryption key） | ✅ 纳入非安全类（statistics 4 项 + remove-schemas），❌ 排除 encryption key 2 项 |
| §5.1 多 warehouse `NoSuchWarehouse` 行为与配置模型 | ✅ 纳入最小实现（仅错误行为），排除配置模型扩展 |
| §5.1 准入条件（鉴权/凭证安全边界设计） | ❌ 不适用于 V4.1，已重新定义准入条件（见 §二），原条件推迟到后续安全专项版本 |

---

## 八、修订记录

### V1.2 (2026-06-05)

- 新增 §二"前置依赖与准入条件"，明确 V4.0 完成依赖和设计文档评审前置条件。
- 重新定义 V4.1 准入条件，原"鉴权/凭证安全边界设计"准入条件推迟到后续安全专项版本。
- 重写 §4.1.2"实现策略"为"功能性要求"，移除具体技术方案（对象存储预写、PostgreSQL 事务等），改为原子性、一致性等功能性描述。
- 修改 §4.1.4 验收标准：死锁项改为"设计层面保证"；删除模糊的"回滚失败"项。
- 修改 §4.2.2 Statistics 行为：将"服务端仅记录警告"改为"待设计确认"表述。
- 补充 §4.3.1 当前配置语义说明：单一 warehouse（默认），非默认值返回错误。
- 统一 §4.3.3 验收标准范围描述：明确所有 Iceberg REST 端点需处理 warehouse 校验。
- 修正 §4.2.4 RemoveSchemas 验收表述：移除不存在 ID"静默忽略"而非"有测试覆盖"。
- 重构 §六 验收清单：区分前置条件检查与功能验收；添加官方文档同步项；补充 `/v1/config` endpoints 验收要求。
- 更新 §七 对应关系表：补充准入条件偏离说明。
- 统一错误类型术语：将 `NoSuchWarehouse` 改为 `NoSuchWarehouseException`（§4.3.1、§六）。
- 调整章节编号：§二→§三→§四→§五→§六→§七→§八。
- 文档头部添加前置依赖摘要。

### V1.1 (2026-06-05)

- 修正 §3.1 multi-table transactions 的实现描述。
- 将"串行逐表 commit + 撤销 snapshot 回滚"修正为"统一对象存储预写 + PostgreSQL 事务原子提交"。
- 删除错误的"中间状态对其他客户端可见"描述；所有 CAS update 在同一事务中，原子性由 PostgreSQL 保证。
- 回滚机制简化为事务自动回滚，无需手动生成撤销 snapshot。
- 同步修正 §3.1.3 错误处理表和 §3.1.4 验收标准。
- 修正 §3.1.2"与真正 ACID 的差异"段落：区分"服务端原子性"与"客户端跨表读取一致性限制"，不再暗示方案缺乏 ACID 属性。

### V1.0 (2026-05-27)

- 初始版本，基于 V4_REQUIREMENTS.md V1.2 和用户需求澄清会议形成。
- 明确 V4.1 排除鉴权/安全/凭证功能。
- 确定 transactions 采用简化批量提交方案（串行逐表 commit + 失败回滚）。
- 确定 TableUpdate 补全范围为 statistics 4 项 + remove-schemas。
- 确定 multi-warehouse 为最小实现（仅参数校验 + NoSuchWarehouse 错误）。
