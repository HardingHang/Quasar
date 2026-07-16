# Quasar 后续演进清单

> ⚠️ **历史文档**：本文档记录 MVP 之后的演进候选项，部分已实现（如孤儿清理思路见 `docs/DESIGN.md` §8.7），部分仍为候选。
> 当前权威文档：[`docs/REQUIREMENTS.md`](./REQUIREMENTS.md)（需求，含明确不做事项清单）与 [`docs/DESIGN.md`](./DESIGN.md)（设计）。

> 本文档记录 MVP 之后需要解决的能力缺口、对应方案和优先级。
> 每一项在进入开发前应细化验收标准和实现范围。

---

## 对象存储与数据库修改非原子性 → 后台孤儿文件清理

**问题**：`create_table` 和 `commit_table` 的流程是先写对象存储（metadata.json），再更新数据库（metadata_location 指针）。如果 S3 写入成功但数据库操作失败，S3 上会留下未被任何 DB 记录引用的孤儿 metadata 文件。

**影响**：
- 不影响正确性（Iceberg 客户端始终通过 DB 指针定位文件，不会误读孤儿文件）
- 长期累积会浪费对象存储空间

**参考做法**（Polaris / Gravitino 等开源项目的通用方案）：
- 不引入分布式事务（2PC/Saga），复杂度与收益不成正比
- 接受两步写的不一致，通过**后台定期清理任务**删除孤儿文件

**建议方案**：
1. 启动一个 `tokio::spawn` 后台任务，按配置间隔（如每小时）执行
2. 从 DB 查询所有 `metadata_location`（Iceberg 格式），存入 HashSet
3. 遍历对象存储上各表 `metadata/` 目录下的 `.metadata.json` 文件
4. 若文件不在 DB 引用集合中，则删除
5. 提供环境变量控制开关和间隔：`QUASAR_CLEANUP_ENABLED`、`QUASAR_CLEANUP_INTERVAL_SECONDS`

**边界条件**：
- 清理前需保留一定时间窗口（如 24 小时），防止正在进行的读取操作需要访问较旧 metadata
- 只扫描 `metadata/` 子目录，不扫描数据文件（Parquet）
- 清理失败时只打日志，不中断服务
