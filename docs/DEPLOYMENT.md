# Quasar 部署与验证指南

本文档介绍如何部署 Quasar Catalog Service 以及如何验证其 Lance REST Namespace 和 Iceberg REST Catalog 功能。

---

## 1. 快速开始（Docker Compose）

### 1.1 最小部署（Server + PostgreSQL）

首次使用需先编译并构建镜像：

```bash
cd quasar

# 1. 本地编译 release 二进制
cargo build --release -p quasar-server

# 2. 构建 Docker 镜像
docker build -t quasar-server:latest .

# 3. 启动最小部署
docker compose up -d
```

后续直接启动：

```bash
cd quasar
docker compose up -d
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
docker compose down
```

### 1.2 Lance 集成环境（Server + PostgreSQL + MinIO）

用于 Lance Python SDK 端到端验证，需要先本地编译再启动：

```bash
cd quasar

# 1. 本地编译 release 二进制（复用宿主机的 cargo 缓存）
cargo build --release -p quasar-server

# 2. 构建 Docker 镜像（基于本地二进制，无需容器内重新下载依赖）
docker build -t quasar-server:latest .

# 3. 启动 Lance 集成环境
docker compose -f docker-compose.lance.yml up -d
```

> **说明：** `docker-compose.lance.yml` 使用 `image: quasar-server:latest` 而非 `build: .`，避免容器内重复下载 crates.io 依赖。
>
> 该环境包含：
> - **Quasar Server**（端口 8080）
> - **PostgreSQL**（内部端口 5432）
> - **MinIO**（对象存储，S3 API 端口 9000，Console 端口 9001）

### 1.3 Spark 集成环境（Server + PostgreSQL + MinIO）

用于 Spark 通过 Iceberg REST Catalog 端到端验证。推荐使用**本地预编译快速构建**（约 15-30 秒），避免容器内重复下载 crates.io 依赖：

```bash
cd quasar

# 一键编译 + 打包镜像（复用本地 cargo 缓存）
./scripts/docker-build.sh

# 启动 Spark 集成环境
docker compose -f docker-compose.spark.yml up -d
```

快速构建脚本自动化三步：
1. `cargo build --release -p quasar-server` — 本地编译（复用 cargo 缓存）
2. `cp target/release/quasar-server quasar-server-bin` — 复制二进制
3. `docker build -f Dockerfile.fast -t quasar-server:latest .` — 快速打包（只 COPY 二进制，无 Rust 工具链）

> **说明：** `docker-compose.spark.yml` 与 `docker-compose.lance.yml` 共享相同的基础设施（MinIO + PostgreSQL + Quasar Server）。区别在于测试脚本使用 PySpark + Iceberg 而非 Lance Python SDK。
>
> 该环境包含：
> - **Quasar Server**（端口 8080，启用 Iceberg REST Catalog）
> - **PostgreSQL**（内部端口 5432）
> - **MinIO**（对象存储，S3 API 端口 9000，Console 端口 9001）
> - **spark-test**（可选测试容器，通过 `--profile test` 启动）

---

## 2. Lance 集成环境设计详解

### 2.1 两个部署方案对比

| | 最小部署 `docker-compose.yml` | Lance 集成 `docker-compose.lance.yml` |
|--|---------------------------|----------------------------------------|
| **服务数量** | 2 个（db + server） | 3 个（minio + db + server） |
| **对象存储** | 无 | MinIO（本地 S3） |
| **`declare_table` 返回的 location** | `lance://namespace/table`（虚拟路径） | `s3://warehouse/prod/users/`（真实 S3 路径） |
| **Lance 数据读写** | ❌ 无法写入 | ✅ 通过 S3 API 写入 MinIO |
| **用途** | Catalog CRUD 测试 | 端到端数据读写验证 |

### 2.2 最小部署详解

最小部署只包含两个服务：`db`（PostgreSQL）和 `server`（Quasar）。

#### db 容器（PostgreSQL）

```yaml
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
```

**运行的程序：** `postgres`（PostgreSQL 服务端进程）

`image: postgres:16` 告诉 Docker Compose 从 Docker Hub 拉取官方 `postgres:16` 镜像，不需要自己构建。这个镜像的 Dockerfile 中定义了默认入口：

```dockerfile
# postgres 官方镜像的简化示意
CMD ["postgres"]   # 容器启动时运行 postgres 进程
```

**启动流程：**
1. Docker 检查本地是否有 `postgres:16` 镜像，没有则自动从 Docker Hub 拉取
2. 启动容器时执行镜像中预设的 `CMD ["postgres"]`
3. 传入 `environment` 中的变量，`postgres` 进程用这些值初始化数据库：创建用户 `quasar`、数据库 `quasar`

**数据持久化：**
```yaml
volumes:
  - pgdata:/var/lib/postgresql/data
```
将容器内的 `/var/lib/postgresql/data`（PostgreSQL 存储数据文件的目录）挂载到 Docker 管理的命名卷 `pgdata` 上。即使容器被删除重建，数据仍然存在。

**健康检查：**
```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U quasar -d quasar"]
  interval: 5s
  timeout: 5s
  retries: 5
```
每隔 5 秒在容器内执行 `pg_isready` 命令，检查 PostgreSQL 是否已经可以接受连接。连续 5 次成功才认为服务健康。

#### server 容器（Quasar）

```yaml
server:
  build: .
  environment:
    QUASAR_DATABASE_URL: postgres://quasar:quasar@db:5432/quasar
    QUASAR_LOG_LEVEL: info
  ports:
    - "8080:8080"
  depends_on:
    db:
      condition: service_healthy
```

**运行的程序：** `quasar-server`（编译好的 Rust 二进制文件）

`build: .` 告诉 Docker Compose：**不要拉取现成镜像，用当前目录（`.`）下的 `Dockerfile` 自己构建一个镜像**。

Dockerfile 的核心逻辑：

```dockerfile
# Stage 1: Build（构建阶段）
FROM rust:1.94-slim-bookworm AS builder
COPY . .
RUN cargo build --release -p quasar-server

# Stage 2: Runtime（运行阶段）
FROM debian:bookworm-slim
COPY --from=builder /app/target/release/quasar-server /usr/local/bin/quasar-server
CMD ["quasar-server"]
```

这是**多阶段构建**：
- **Stage 1（builder）**：包含完整的 Rust 编译工具链（约 1GB+），用于编译出二进制文件
- **Stage 2（runtime）**：只保留运行必需的 `ca-certificates`（HTTPS 证书）和编译好的 `quasar-server` 二进制（约 50MB）

最终镜像非常小，只包含能运行 `quasar-server` 的最小环境。

**容器启动时的执行流程：**
1. 加载 `environment` 中定义的环境变量
2. 执行 `CMD ["quasar-server"]` → 启动 Rust 服务端
3. `quasar-server` 内部做的事情：
   - 读取 `QUASAR_DATABASE_URL`，连接到 `db` 服务（Docker 内部网络 DNS 解析 `db` → PostgreSQL 容器 IP）
   - 运行 `refinery` 数据库迁移
   - 绑定 `0.0.0.0:8080`
   - 启动 HTTP 服务

**端口映射：**
```yaml
ports:
  - "8080:8080"
```
将容器内部的 8080 端口暴露到主机的 8080 端口，这样可以在主机上用 `curl http://localhost:8080/...` 访问。

**依赖关系：**
```yaml
depends_on:
  db:
    condition: service_healthy
```
`server` 容器必须在 `db` 容器的健康检查通过后才启动，避免 Quasar 启动时 PostgreSQL 还没准备好导致连接失败。

**最小部署的数据流：**

```
主机 localhost:8080 ◄──────► Quasar Server 容器 :8080
                                      │
                                      │ REST API（curl / Python requests）
                                      │
                                      ▼
                              PostgreSQL 容器 :5432
                              （元数据：namespace / table / version）
```

Quasar Server 是无状态服务，所有数据都存在 PostgreSQL 中。

---

### 2.3 Lance 集成部署详解

Lance 集成部署在最小部署的基础上增加了 **MinIO** 容器，共三个服务。

#### MinIO 容器

```yaml
minio:
  image: minio/minio:latest
  command: server /data --console-address ":9001"
```

**运行的程序：** MinIO Server（Go 编写的 S3 兼容对象存储）

`image: minio/minio:latest` 从 Docker Hub 拉取官方镜像。`command` 覆盖了默认启动参数：
- `server /data`：以 server 模式运行，数据存储在容器内 `/data` 目录
- `--console-address ":9001"`：Web 管理界面监听 9001 端口

**暴露端口：**

| 端口 | 用途 | 主机访问 |
|------|------|---------|
| `9000` | S3 API（程序调用） | `http://localhost:9000` |
| `9001` | Web Console（浏览器管理） | `http://localhost:9001` |

用 `minioadmin/minioadmin` 登录 Console，可查看 bucket 和对象。

**数据持久化：**
```yaml
volumes:
  - minio_data:/data
```
Lance 写入的 Parquet 数据文件、manifest 文件都存储在这个命名卷中。容器重建后数据不丢失。

**健康检查：**
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
```
MinIO 内置了 `/minio/health/live` 端点，curl 返回 200 即认为就绪。

#### PostgreSQL 容器

与最小部署完全一致，详见 2.2 节。在 Lance 集成环境中同样负责存储 namespace、table、version 等元数据。

#### Quasar Server 容器

用同一个 `Dockerfile` 构建，运行的仍然是 `quasar-server` 二进制。关键区别在于**环境变量**：

```yaml
environment:
  # === 基础配置 ===
  QUASAR_DATABASE_URL: postgres://quasar:quasar@db:5432/quasar
  QUASAR_LOG_LEVEL: info

  # === Lance 集成新增：Warehouse + S3 配置 ===
  QUASAR_WAREHOUSE_PATH: s3://warehouse/
  QUASAR_S3_ENDPOINT: http://minio:9000
  QUASAR_S3_ACCESS_KEY: minioadmin
  QUASAR_S3_SECRET_KEY: minioadmin
  QUASAR_S3_REGION: us-east-1
  QUASAR_S3_ALLOW_HTTP: "true"
```

**配置如何影响行为：**

| 环境变量 | 在 Server 中的作用 |
|----------|-------------------|
| `QUASAR_WAREHOUSE_PATH` | `declare_table` 的 location 前缀。`s3://warehouse/` 表示对象存储上的 `warehouse` bucket |
| `QUASAR_S3_ENDPOINT` | S3 服务端点。`http://minio:9000` 通过 Docker 内部网络 DNS 解析到 MinIO 容器 |
| `QUASAR_S3_ACCESS_KEY/SECRET_KEY` | S3 访问凭证，返回给客户端用于直连对象存储 |
| `QUASAR_S3_ALLOW_HTTP` | 允许 HTTP（非 HTTPS）S3 连接。本地 MinIO 无 TLS，必须设为 `true` |

Server 启动时将这些值构建为 `LanceConfig`：
- 有 `warehouse_path` 时，`declare_table` 返回 `s3://warehouse/prod/users/`
- 无 `warehouse_path` 时，返回 `lance://prod/users`（向后兼容）
- `storage_options` 在 `DeclareTableResponse` 中返回给客户端

**依赖关系：**
```yaml
depends_on:
  db:
    condition: service_healthy
  minio:
    condition: service_healthy
```
Quasar Server 等待 **两个** 依赖都就绪后才启动，避免启动时连接失败。

### 2.4 三容器协作关系

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker 内部网络                            │
│                                                             │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐  │
│   │   Quasar    │◄────┤  PostgreSQL │     │    MinIO    │  │
│   │   Server    │     │   (元数据)   │     │  (对象存储)  │  │
│   │  :8080      │     │  :5432      │     │  :9000      │  │
│   └──────┬──────┘     └─────────────┘     └──────▲──────┘  │
│          │                                        │         │
│          │ ① REST API: 注册 namespace/table       │         │
│          │ ② 元数据存入 PostgreSQL                │         │
│          │                                        │         │
│          │ ③ storage_options + location           │         │
│          └────────────────────────────────────────┘         │
│                    (返回给客户端)                            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Python 测试脚本  │
                    │                 │
                    │ ④ Lance Python   │
                    │    SDK 直接读写   │
                    │    s3://...      │
                    └─────────────────┘
```

**数据流说明：**

| 步骤 | 谁发起 | 操作 | 存储位置 |
|------|--------|------|---------|
| ① | Python 脚本 | `POST /lance/v1/namespace/prod/create` | PostgreSQL（namespace 记录） |
| ② | Python 脚本 | `POST /lance/v1/table/prod$users/declare` | PostgreSQL（location + properties） |
| ③ | Quasar Server | 返回 `location` + `storage_options` | —（HTTP 响应给客户端） |
| ④ | Python 脚本 | `lance.write_dataset(uri=location, storage_options=...)` | **MinIO（S3）** |
| ⑤ | Python 脚本 | `POST /lance/v1/table/prod$users/version/create` | PostgreSQL（version 记录） |

**关键设计原则：**
- **Quasar 只存元数据**（namespace、table、version 记录）→ PostgreSQL
- **Lance 数据文件**（Parquet、manifest）→ 直接写入 MinIO/S3
- Quasar 和 Lance Python SDK 各自独立访问存储，`location` 作为两者之间的桥梁

### 2.5 为什么需要 MinIO？

Lance 格式的设计是将数据文件存储在**对象存储**（S3/GCS/Azure）上，而非本地磁盘。

**没有 MinIO 时：**
- `declare_table` 返回 `lance://prod/users`（虚拟 URI）
- Lance Python SDK 无法向该 URI 写入真实数据
- 只能验证 Catalog 的 CRUD，无法验证数据读写

**有了 MinIO 时：**
- `declare_table` 返回 `s3://warehouse/prod/users/`（真实 S3 URI）
- Lance Python SDK 用 `storage_options` 中的凭证直连 MinIO
- 完整验证：写数据 → 注册版本 → 读回数据 → 追加数据

MinIO 在本地提供了与生产环境 S3 兼容的 API，是 Lance 端到端验证的必需组件。

### 2.6 Spark 集成部署详解

Spark 集成部署复用与 Lance 集成完全相同的三个服务（MinIO + PostgreSQL + Quasar Server），额外增加一个可选的 **`spark-test`** 测试容器。

#### spark-test 容器

```yaml
  spark-test:
    image: python:3.11-slim
    profiles: ["test"]
    environment:
      BASE_URL: http://server:8080
      MINIO_ENDPOINT: http://minio:9000
      RUN_MODE: docker
    depends_on:
      server:
        condition: service_started
    volumes:
      - ./tests:/tests:ro
    command: >
      bash -c "
        apt-get update -qq &&
        apt-get install -y -qq default-jre &&
        pip install -q pyspark==3.5.3 requests boto3 &&
        python /tests/spark_iceberg_integration.py
      "
```

**运行的程序：** Python 测试脚本（通过 PySpark 调用 Spark + Iceberg）

**两种运行方式：**

| 方式 | 命令 | 适用场景 |
|------|------|---------|
| **Docker 内运行**（推荐） | `docker compose -f docker-compose.spark.yml --profile test run --rm spark-test` | 宿主机未安装 PySpark/Java |
| **宿主机运行** | `python tests/spark_iceberg_integration.py` | 宿主机已有 PySpark 环境 |

**Docker 内运行时的网络：**
- `BASE_URL=http://server:8080` — 通过 Docker 内部 DNS 访问 Quasar Server
- `MINIO_ENDPOINT=http://minio:9000` — 通过 Docker 内部 DNS 访问 MinIO

**宿主机运行时的网络：**
- REST API: `http://localhost:8080`（端口映射）
- MinIO S3: `http://localhost:9000`（端口映射）

#### Iceberg metadata 在对象存储中的位置

与 Lance 不同，Iceberg 的 **metadata.json 文件由 Quasar Server 直接写入 MinIO**：

```
MinIO (s3://warehouse/)
└── prod/
    └── test/
        └── metadata/
            ├── 00001-<uuid>.metadata.json   ← 初始 metadata（create_table 写入）
            ├── 00002-<uuid>.metadata.json   ← 第一次 commit 后的 metadata
            └── 00003-<uuid>.metadata.json   ← 第二次 commit 后的 metadata
```

每次 `INSERT INTO` 触发 Spark 的 commit 流程：
1. Spark 读取当前 metadata（通过 REST `GET /tables/...`）
2. Spark 写入新的 Parquet 数据文件到 MinIO
3. Spark 构建新的 snapshot 和 metadata
4. Spark 发送 `POST /tables/...` commit 请求
5. Quasar Server 将新的 metadata.json 写入 MinIO
6. Quasar Server CAS 更新 PostgreSQL 中的 metadata_location 指针

#### 三容器协作关系（Iceberg）

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker 内部网络                            │
│                                                             │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐  │
│   │   Quasar    │◄────┤  PostgreSQL │     │    MinIO    │  │
│   │   Server    │     │   (元数据)   │     │  (对象存储)  │  │
│   │  :8080      │     │  :5432      │     │  :9000      │  │
│   └──────┬──────┘     └─────────────┘     └──────▲──────┘  │
│          │                                        │         │
│          │ ① REST API: namespace/table CRUD       │         │
│          │ ② metadata_location 存 PostgreSQL      │         │
│          │ ③ metadata.json 写入 MinIO             │         │
│          │                                        │         │
│          │         ④ Spark 直连 MinIO 读写        │         │
│          │            Parquet 数据文件            │         │
│          │                                        │         │
│          └────────────────────────────────────────┘         │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │              spark-test 容器（可选）                  │   │
│   │                                                      │   │
│   │  ⑤ PySpark Session + Iceberg REST Catalog          │   │
│   │     CREATE TABLE → INSERT → SELECT → DROP           │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**数据流说明：**

| 步骤 | 谁发起 | 操作 | 存储位置 |
|------|--------|------|---------|
| ① | Spark | `POST /iceberg/v1/namespaces` 创建 namespace | PostgreSQL |
| ② | Spark | `POST /iceberg/v1/namespaces/prod/tables` 创建表 | PostgreSQL + MinIO（metadata.json） |
| ③ | Spark | `INSERT INTO iceberg.prod.test` | MinIO（Parquet 数据文件） |
| ④ | Spark | 内部 commit，发送 `POST /tables/...` | MinIO（新 metadata.json）+ PostgreSQL（更新指针） |
| ⑤ | Spark | `SELECT * FROM iceberg.prod.test` | MinIO（读取 Parquet） |

**关键设计原则：**
- **Quasar 管理 metadata 指针**（PostgreSQL 存 `metadata_location`）
- **Quasar 读写 metadata.json**（通过 `object_store` crate 写入 MinIO）
- **Spark 读写数据文件**（Parquet 直接写入 MinIO，不经过 Quasar）
- 两者通过 `location`（表的数据基础路径）协作

---

## 3. 配置说明

Quasar 通过环境变量进行配置。

### 3.1 必需配置

| 变量名 | 说明 | 示例 |
|--------|------|------|
| `QUASAR_DATABASE_URL` | PostgreSQL 连接串 | `postgres://quasar:quasar@db:5432/quasar` |

### 3.2 服务端配置

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_HOST` | 否 | `0.0.0.0` | HTTP 监听地址 |
| `QUASAR_PORT` | 否 | `8080` | HTTP 监听端口 |
| `QUASAR_LOG_LEVEL` | 否 | `info` | 日志级别（也支持 `RUST_LOG`） |

### 3.3 Warehouse 与对象存储配置（Lance / Iceberg 验证需要）

| 变量名 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `QUASAR_WAREHOUSE_PATH` | 否 | - | Warehouse 根路径，如 `s3://warehouse/` |
| `QUASAR_S3_ENDPOINT` | 否 | - | S3 endpoint，如 `http://minio:9000` |
| `QUASAR_S3_ACCESS_KEY` | 否 | - | S3 access key |
| `QUASAR_S3_SECRET_KEY` | 否 | - | S3 secret key |
| `QUASAR_S3_REGION` | 否 | `us-east-1` | S3 region |
| `QUASAR_S3_ALLOW_HTTP` | 否 | `false` | 是否允许 HTTP（非 HTTPS）S3 连接 |

Iceberg 端点使用相同的 S3 配置，`object_store` crate 在 Server 启动时构建 S3 客户端，用于读写 `metadata.json` 文件。

### 3.4 配置示例

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

## 4. 部署模式

### 4.1 Docker Compose（推荐）

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
    image: quasar-server:latest
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

### 4.2 本地预编译快速构建（推荐日常开发）

原 `Dockerfile` 在容器内编译时，需要从零下载所有 Rust 依赖（crates.io），网络慢时需 **10 分钟以上**。

本方案改为**本地编译**（复用 cargo 缓存，约 15 秒）+ **镜像打包**（只 COPY 二进制，约 1 秒）：

```bash
cd quasar
./scripts/docker-build.sh
```

脚本自动化三步：
1. `cargo build --release -p quasar-server` — 本地编译
2. `cp target/release/quasar-server quasar-server-bin` — 复制二进制到 build context
3. `docker build -f Dockerfile.fast -t quasar-server:latest .` — 快速打包
4. 退出时自动清理临时文件

**Dockerfile.fast 原理：**

```dockerfile
FROM debian:bookworm-slim
RUN apt-get install ca-certificates
COPY quasar-server-bin /usr/local/bin/quasar-server
CMD ["quasar-server"]
```

只做两件事：安装 HTTPS 证书、复制已编译好的二进制。没有 Rust 工具链、没有 crate 下载。

**完整流程示例：**

```bash
cd quasar

# 编译 + 打包镜像（约 15-30 秒）
./scripts/docker-build.sh

# 启动最小部署
docker compose up -d

# 或启动 Lance 集成环境
docker compose -f docker-compose.lance.yml up -d

# 或启动 Spark 集成环境
docker compose -f docker-compose.spark.yml up -d
```

### 4.3 原生二进制

```bash
# 编译
cd quasar
cargo build --release -p quasar-server

# 运行
export QUASAR_DATABASE_URL="postgres://postgres:postgres@localhost:5432/quasar"
./target/release/quasar-server
```

### 4.4 生产环境注意事项

- **数据库迁移**：Server 启动时会自动运行 `refinery` 迁移。多实例部署时建议先由单个实例完成迁移，再扩容。
- **连接池**：当前使用 `deadpool_postgres` 默认配置，生产环境可通过 `QUASAR_DATABASE_URL` 的连接参数调整（如 `?pool.max_size=20`）。
- **对象存储凭证**：`QUASAR_S3_ACCESS_KEY` / `QUASAR_S3_SECRET_KEY` 应通过 secrets manager 注入，避免硬编码。
- **健康检查**：Kubernetes 部署时建议配置：
  - `livenessProbe`: `GET /healthz`
  - `readinessProbe`: `GET /readyz`
- **优雅关闭**：Server 响应 SIGTERM/SIGINT 信号，等待存量请求处理完毕后退出。

---

## 5. Lance 端到端验证

### 5.1 启动环境

按 [1.2 节](#12-lance-集成环境server--postgresql--minio) 启动 Lance 集成环境，确保容器状态正常：

```bash
docker compose -f docker-compose.lance.yml ps
```

确认 `quasar-server-1`、`quasar-db-1`、`quasar-minio-1` 均为 `healthy` 状态后即可开始验证。

### 5.2 安装 Python 依赖

```bash
pip install requests pyarrow lance boto3
```

### 5.3 运行集成测试

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
Results: 12 passed, 0 failed
==================================================
```

### 5.4 手动验证（curl + Python）

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

## 5. Spark 端到端验证

### 5.5 启动环境

按 [1.3 节](#13-spark-集成环境server--postgresql--minio) 启动 Spark 集成环境，确保容器状态正常：

```bash
docker compose -f docker-compose.spark.yml ps
```

确认 `quasar-server-1`、`quasar-db-1`、`quasar-minio-1` 均为 `healthy` 状态后即可开始验证。

### 5.6 运行集成测试（方式一：Docker 内运行，推荐）

不需要在宿主机安装 PySpark/Java，所有依赖在容器中自动安装：

```bash
cd quasar
docker compose -f docker-compose.spark.yml --profile test run --rm spark-test
```

预期输出：

```
==================================================
Quasar S10: Spark Iceberg Integration Test
Mode: docker
==================================================

[Step 0] Waiting for Quasar server...
[Step 0] Creating MinIO bucket 'warehouse' via http://minio:9000...

[Step 1] Create namespace: prod
[PASS] Create namespace: prod

[Step 2] Building Spark session with Iceberg REST Catalog...
[PASS] Spark session created

[Step 3] CREATE TABLE iceberg.prod.test (id INT, name STRING)
[PASS] CREATE TABLE iceberg.prod.test

[Step 4] INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')
[PASS] INSERT INTO (2 rows)

[Step 5] SELECT * FROM iceberg.prod.test
[PASS] SELECT * returned 2 rows
[PASS] SELECT * columns: ['id', 'name']

[Step 6] INSERT INTO iceberg.prod.test VALUES (3, 'Charlie')
[PASS] After append: 3 rows

[Step 7] DROP TABLE iceberg.prod.test
[PASS] DROP TABLE iceberg.prod.test

[Step 8] Cleanup: drop namespace 'prod'
[PASS] Drop namespace: prod

==================================================
Results: 9 passed, 0 failed
==================================================
```

### 5.7 运行集成测试（方式二：宿主机运行）

如果宿主机已安装 PySpark 3.5+，可直接在宿主机运行测试脚本：

```bash
pip install pyspark==3.5.3 requests boto3

cd quasar
python tests/spark_iceberg_integration.py
```

### 5.8 手动验证（curl + PySpark）

如果不运行脚本，可以手动逐步验证：

**1. 创建 Namespace：**

```bash
curl -X POST http://localhost:8080/iceberg/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["prod"]}'
```

**2. 配置 PySpark：**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QuasarTest") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://localhost:8080/iceberg") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
    .getOrCreate()
```

**3. 创建表：**

```python
spark.sql("CREATE TABLE iceberg.prod.test (id INT, name STRING) USING iceberg")
```

**4. 插入数据：**

```python
spark.sql("INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')")
```

**5. 查询数据：**

```python
df = spark.sql("SELECT * FROM iceberg.prod.test")
print(df.count())  # 2
```

**6. 验证 metadata.json 在 MinIO 中：**

```bash
# 安装 awscli
pip install awscli

# 列出 metadata 文件
aws s3 ls s3://warehouse/prod/test/metadata/ \
  --endpoint-url http://localhost:9000 \
  --recursive
```

**7. 清理：**

```python
spark.sql("DROP TABLE iceberg.prod.test")
spark.stop()
```

```bash
curl -X DELETE http://localhost:8080/iceberg/v1/namespaces/prod
```

---

## 6. Lance REST API 快速参考

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

## 6.5 Iceberg REST API 快速参考

路径前缀：`/iceberg/v1/...`

### Config

```bash
GET /iceberg/v1/config          # 返回 Catalog 配置
```

### Namespace

```bash
GET    /iceberg/v1/namespaces                       # 列出
POST   /iceberg/v1/namespaces                       # 创建
GET    /iceberg/v1/namespaces/{ns}                  # 描述
DELETE /iceberg/v1/namespaces/{ns}                  # 删除（仅限空 Namespace）
POST   /iceberg/v1/namespaces/{ns}/properties       # 更新 properties
```

### Table

```bash
GET    /iceberg/v1/namespaces/{ns}/tables           # 列出
POST   /iceberg/v1/namespaces/{ns}/tables           # 创建
GET    /iceberg/v1/namespaces/{ns}/tables/{table}   # 加载（含 metadata）
POST   /iceberg/v1/namespaces/{ns}/tables/{table}   # 提交（CAS commit）
DELETE /iceberg/v1/namespaces/{ns}/tables/{table}   # 删除
HEAD   /iceberg/v1/namespaces/{ns}/tables/{table}   # 存在检查
POST   /iceberg/v1/tables/rename                    # 重命名
```

### Health

```bash
GET /healthz   # Liveness（始终返回 200）
GET /readyz    # Readiness（检查 DB 连通性）
```

---

## 7. 故障排查

| 现象 | 原因 | 解决 |
|------|------|------|
| Server 启动失败，日志显示 "error connecting to server" | PostgreSQL 未就绪 | 确保 DB 先启动并可通过 `QUASAR_DATABASE_URL` 连接 |
| `readyz` 返回 503 | DB 连接池耗尽或网络不通 | 检查 DB 状态，确认连接串正确 |
| `declare_table` 返回 `lance://...` | `QUASAR_WAREHOUSE_PATH` 未设置 | 配置环境变量 |
| Lance Python SDK 无法写入 MinIO | storage_options 不匹配 | 确认 endpoint、access_key、secret_key 正确；测试脚本会自动将 `http://minio:9000` 替换为 `http://localhost:9000` 以适配宿主机执行 |
| `docker-compose.lance.yml` server 启动报错 | MinIO bucket 不存在 | 测试脚本会自动创建 bucket；手动验证时需先用 `boto3` 或 MinIO Console 创建 |
| 集成测试失败 "Quasar server did not become ready" | Server 启动慢或端口冲突 | 检查 `docker-compose -f docker-compose.lance.yml logs server`，确认端口 8080 未被占用 |
| Spark `CREATE TABLE` 失败，提示 "NoSuchNamespaceException" | Namespace 未创建 | Spark 不会自动创建 Namespace，需先用 REST `POST /iceberg/v1/namespaces` 创建 |
| Spark commit 失败，提示 "CommitFailedException 409" | CAS 冲突，metadata_location 已变更 | 其他客户端同时提交了更新，Spark 会自动重试 |
| Spark 无法写入 MinIO/S3 | S3A 配置错误 | 确认 `fs.s3a.endpoint`、`access.key`、`secret.key`、`path.style.access` 正确配置 |
| `metadata.json` 不在 MinIO 中 | Quasar Server 未配置 S3 | 确认 `QUASAR_S3_ENDPOINT`、`ACCESS_KEY`、`SECRET_KEY` 已设置，Server 启动日志中应显示 S3 客户端初始化成功 |
| Spark 测试脚本提示 "Java not found" | 宿主机未安装 Java | PySpark 依赖 Java 运行时。宿主机安装 `default-jre` 或使用 Docker 内运行方式 |

---

## 8. 版本兼容性

| 组件 | 版本 |
|------|------|
| Rust | 1.94+ |
| PostgreSQL | 14+ |
| Python | 3.10+ |
| Lance (Python) | 0.25+ |
| PyArrow | 16+ |
| Spark | 3.4+ |
| Iceberg Spark Runtime | 1.5+ |

---

## 9. 修订记录

### V1.2（2026-04-22）

- 新增：Spark 集成验证章节（1.3、2.6、5.5-5.8、6.5），采用与 Lance 相同的**快速构建**方式（`./scripts/docker-build.sh`）
- 新增：`docker-compose.spark.yml` 部署说明，含 `spark-test` 容器（`--profile test`）
- 新增：Iceberg REST API 快速参考（Config / Namespace / Table 端点）
- 新增：Spark 故障排查条目（6 条）
- 更新：`scripts/docker-build.sh` 添加 Spark 集成部署启动提示
- 更新：版本兼容性表，新增 Spark 3.4+ 和 Iceberg Spark Runtime 1.5+
- 更新：文档标题和范围，涵盖 Lance + Iceberg 双协议

### V1.1（2026-04-20）

- 更新：Dockerfile Rust 基础镜像 1.86 → 1.94（`time` crate 要求 1.88+）
- 更新：最小部署和 Lance 集成环境启动流程，增加本地编译 + 镜像构建步骤
- 新增：4.2 节「本地预编译快速构建」及配套 `scripts/docker-build.sh` / `Dockerfile.fast`
- 更新：预期输出 11 passed → 12 passed（清理步骤 PASS）
- 更新：故障排查中 MinIO endpoint 说明（脚本自动替换 `localhost:9000`）
- 更新：版本兼容性 Rust 1.86+ → 1.94+

### V1.0（2026-04-20）

- 初始版本：完整部署与验证指南
