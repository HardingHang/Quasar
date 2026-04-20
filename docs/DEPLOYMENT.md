# Quasar 部署与验证指南

本文档介绍如何部署 Quasar Catalog Service 以及如何验证其 Lance REST Namespace 功能。

---

## 1. 快速开始（Docker Compose）

### 1.1 最小部署（Server + PostgreSQL）

```bash
cd quasar
docker-compose up -d
```

启动后访问：

```bash
# 健康检查
curl http://localhost:8080/healthz
# {"status": "ok"}

curl http://localhost:8080/readyz
# {"status": "ready", "checks": {"database": "ok"}}

# 列出 Namespace
curl http://localhost:8080/lance/v1/namespace/default/list
# {"namespaces": []}
```

停止：

```bash
docker-compose down
```

### 1.2 Lance 集成环境（Server + PostgreSQL + MinIO）

用于 Lance Python SDK 端到端验证：

```bash
cd quasar
docker-compose -f docker-compose.lance.yml up -d
```

该环境包含：
- **Quasar Server**（端口 8080）
- **PostgreSQL**（内部端口 5432）
- **MinIO**（对象存储，S3 API 端口 9000，Console 端口 9001）

---

## 2. 配置说明

Quasar 通过环境变量进行配置。

### 2.1 必需配置

| 变量名 | 说明 | 示例 |
|--------|------|------|
| `QUASAR_DATABASE_URL` | PostgreSQL 连接串 | `postgres://quasar:quasar@db:5432/quasar` |

### 2.2 服务端配置

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_HOST` | 否 | `0.0.0.0` | HTTP 监听地址 |
| `QUASAR_PORT` | 否 | `8080` | HTTP 监听端口 |
| `QUASAR_LOG_LEVEL` | 否 | `info` | 日志级别（也支持 `RUST_LOG`） |

### 2.3 Warehouse 与对象存储配置（Lance 验证需要）

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_WAREHOUSE_PATH` | 否 | - | Warehouse 根路径，如 `s3://warehouse/` |
| `QUASAR_S3_ENDPOINT` | 否 | - | S3 endpoint，如 `http://minio:9000` |
| `QUASAR_S3_ACCESS_KEY` | 否 | - | S3 access key |
| `QUASAR_S3_SECRET_KEY` | 否 | - | S3 secret key |
| `QUASAR_S3_REGION` | 否 | `us-east-1` | S3 region |
| `QUASAR_S3_ALLOW_HTTP` | 否 | `false` | 是否允许 HTTP（非 HTTPS）S3 连接 |

### 2.4 配置示例

**本地开发（无对象存储）：**

```bash
export QUASAR_DATABASE_URL="postgres://postgres:postgres@localhost:5432/quasar"
export QUASAR_LOG_LEVEL="debug"
cargo run -p quasar-server
```

**Docker Compose Lance 环境：**

```yaml
environment:
  QUASAR_DATABASE_URL: postgres://quasar:quasar@db:5432/quasar
  QUASAR_LOG_LEVEL: info
  QUASAR_WAREHOUSE_PATH: s3://warehouse/
  QUASAR_S3_ENDPOINT: http://minio:9000
  QUASAR_S3_ACCESS_KEY: minioadmin
  QUASAR_S3_SECRET_KEY: minioadmin
  QUASAR_S3_REGION: us-east-1
  QUASAR_S3_ALLOW_HTTP: "true"
```

---

## 3. 部署模式

### 3.1 Docker Compose（推荐）

```yaml
# docker-compose.yml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: quasar
      POSTGRES_PASSWORD: quasar
      POSTGRES_DB: quasar
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U quasar -d quasar"]
      interval: 5s
      timeout: 5s
      retries: 5

  server:
    build: .
    environment:
      QUASAR_DATABASE_URL: postgres://quasar:quasar@db:5432/quasar
    ports:
      - "8080:8080"
    depends_on:
      db:
        condition: service_healthy

volumes:
  pgdata:
```

特点：
- 单命令启动完整服务栈
- PostgreSQL 数据持久化到命名卷
- Server 等待 DB 就绪后才启动

### 3.2 原生二进制

```bash
# 编译
cd quasar
cargo build --release -p quasar-server

# 运行
export QUASAR_DATABASE_URL="postgres://postgres:postgres@localhost:5432/quasar"
./target/release/quasar-server
```

### 3.3 生产环境注意事项

- **数据库迁移**：Server 启动时会自动运行 `refinery` 迁移。多实例部署时建议先由单个实例完成迁移，再扩容。
- **连接池**：当前使用 `deadpool_postgres` 默认配置，生产环境可通过 `QUASAR_DATABASE_URL` 的连接参数调整（如 `?pool.max_size=20`）。
- **对象存储凭证**：`QUASAR_S3_ACCESS_KEY` / `QUASAR_S3_SECRET_KEY` 应通过 secrets manager 注入，避免硬编码。
- **健康检查**：Kubernetes 部署时建议配置：
  - `livenessProbe`: `GET /healthz`
  - `readinessProbe`: `GET /readyz`
- **优雅关闭**：Server 响应 SIGTERM/SIGINT 信号，等待存量请求处理完毕后退出。

---

## 4. Lance 端到端验证

### 4.1 启动环境

```bash
cd quasar
docker-compose -f docker-compose.lance.yml up -d
```

### 4.2 安装 Python 依赖

```bash
pip install requests pyarrow lance boto3
```

### 4.3 运行集成测试

```bash
cd quasar
python tests/lance_integration.py
```

预期输出：

```
==================================================
Quasar S7: Lance Integration Test
==================================================

[Step 0] Waiting for Quasar server...
[Step 0] Creating MinIO bucket 'warehouse'...

[Step 1] Create namespace: prod
[PASS] Create namespace: prod

[Step 2] Declare table: prod.users
[PASS] Declare table: location=s3://warehouse/prod/users/
[PASS] Declare table: storage_options contains endpoint

[Step 3] Write data to location (3 rows)
[PASS] Write data to location (3 rows)

[Step 4] Create table version: v1
[PASS] Create table version: v1

[Step 5] Describe table: verify current_version=1
[PASS] Describe table: current_version=1

[Step 6] Read data back using Lance SDK
[PASS] Read data back: 3 rows
[PASS] Read data back: schema=['id', 'name']

[Step 7] Write second batch (5 rows total)
[PASS] Write second batch (5 rows total)

[Step 8] Create table version: v2
[PASS] Create table version: v2

[Step 9] List versions: verify [1, 2]
[PASS] List versions: [1, 2]

[Step 10] Cleanup: drop table and namespace
[PASS] Drop table + namespace

==================================================
Results: 11 passed, 0 failed
==================================================
```

### 4.4 手动验证（curl + Python）

如果不运行脚本，可以手动逐步验证：

**1. 创建 Namespace：**

```bash
curl -X POST http://localhost:8080/lance/v1/namespace/prod/create
```

**2. 声明表（获取 location 和 storage_options）：**

```bash
curl -X POST http://localhost:8080/lance/v1/table/prod%24users/declare
# {"name": "users", "location": "s3://warehouse/prod/users/",
#  "storage_options": {"endpoint": "http://minio:9000", ...}}
```

**3. Python 写入数据：**

```python
import lance, pyarrow as pa

table = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})

lance.write_dataset(
    table,
    "s3://warehouse/prod/users/",
    storage_options={
        "endpoint": "http://localhost:9000",
        "access_key_id": "minioadmin",
        "secret_access_key": "minioadmin",
        "region": "us-east-1",
        "allow_http": "true",
    },
)
```

**4. 注册版本：**

```bash
curl -X POST http://localhost:8080/lance/v1/table/prod%24users/version/create \
  -H "Content-Type: application/json" \
  -d '{"version": 1, "manifest_path": "s3://warehouse/prod/users/_versions/1.manifest"}'
```

**5. 验证 current_version：**

```bash
curl -X POST http://localhost:8080/lance/v1/table/prod%24users/describe
# {"name": "users", "location": "...", "current_version": 1, ...}
```

**6. Python 读回数据：**

```python
ds = lance.dataset("s3://warehouse/prod/users/", storage_options=...)
print(len(ds.to_table()))  # 3
```

---

## 5. Lance REST API 快速参考

### Namespace

```bash
POST /lance/v1/namespace/{id}/create      # 创建
GET  /lance/v1/namespace/{id}/list        # 列出
POST /lance/v1/namespace/{id}/describe    # 描述
POST /lance/v1/namespace/{id}/drop        # 删除
POST /lance/v1/namespace/{id}/exists      # 存在检查
```

### Table

```bash
POST /lance/v1/table/{id}/declare         # 声明（分配 location）
POST /lance/v1/table/{id}/describe        # 描述（含 current_version）
POST /lance/v1/table/{id}/register        # 注册已有表
POST /lance/v1/table/{id}/deregister      # 注销（保留数据）
POST /lance/v1/table/{id}/drop            # 删除
POST /lance/v1/table/{id}/exists          # 存在检查
POST /lance/v1/table/{id}/rename          # 重命名
GET  /lance/v1/namespace/{id}/table/list  # 列出 Namespace 下的表
```

`{id}` 格式：`{namespace}${table}`（URL 编码 `$` 为 `%24`）。

### Version

```bash
POST /lance/v1/table/{id}/version/create  # 注册版本（客户端指定 version_id）
GET  /lance/v1/table/{id}/version/list    # 列出版本
POST /lance/v1/table/{id}/version/describe # 描述指定版本
```

### Health

```bash
GET /healthz   # Liveness（始终返回 200）
GET /readyz    # Readiness（检查 DB 连通性）
```

---

## 6. 故障排查

| 现象 | 原因 | 解决 |
|------|------|------|
| Server 启动失败，日志显示 "error connecting to server" | PostgreSQL 未就绪 | 确保 DB 先启动并可通过 `QUASAR_DATABASE_URL` 连接 |
| `readyz` 返回 503 | DB 连接池耗尽或网络不通 | 检查 DB 状态，确认连接串正确 |
| `declare_table` 返回 `lance://...` | `QUASAR_WAREHOUSE_PATH` 未设置 | 配置环境变量 |
| Lance Python SDK 无法写入 MinIO | storage_options 不匹配 | 确认 endpoint、access_key、secret_key 正确；若 MinIO 在容器内，endpoint 应为 `http://minio:9000` |
| `docker-compose.lance.yml` server 启动报错 | MinIO bucket 不存在 | 测试脚本会自动创建 bucket；手动验证时需先用 `boto3` 或 MinIO Console 创建 |
| 集成测试失败 "Quasar server did not become ready" | Server 启动慢或端口冲突 | 检查 `docker-compose -f docker-compose.lance.yml logs server`，确认端口 8080 未被占用 |

---

## 7. 版本兼容性

| 组件 | 版本 |
|------|------|
| Rust | 1.86+ |
| PostgreSQL | 14+ |
| Python | 3.10+ |
| Lance (Python) | 0.25+ |
| PyArrow | 16+ |
