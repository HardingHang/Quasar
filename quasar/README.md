# Quasar

Quasar is a universal Catalog Service for Lakehouse architectures, supporting multiple data formats through standard protocols. It acts as a metadata hub that indexes and manages data assets across Iceberg tables, Lance tables, and future extensible asset types.

## Supported Protocols

| Protocol | Path Prefix | Clients |
|----------|-------------|---------|
| **Iceberg REST Catalog** | `/iceberg/v1/...` | Spark, Trino, Flink |
| **Lance REST Namespace** | `/lance/v1/...` | LanceDB Python SDK, lance-spark, Ray |

## Architecture

```
quasar/
├── core/       # Core domain models and trait definitions
├── storage/    # PostgreSQL storage implementation with migrations
├── adapter/    # Protocol adapters (Iceberg REST, Lance REST)
└── server/     # HTTP server entrypoint (axum)
```

Quasar exposes both protocols on a single port. Clients differentiate by base URI:
- Iceberg clients: `uri=http://host:port/iceberg`
- Lance clients: `uri=http://host:port/lance`

The two protocols maintain independent namespace/table spaces (same names allowed).

## Quick Start

### Prerequisites

- Rust 1.70+
- PostgreSQL 14+
- (Optional) MinIO or S3 for Iceberg metadata storage

### Build

```bash
cd quasar
cargo build --release
```

### Run locally

```bash
# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15

# Set env vars and run
export QUASAR_DATABASE_URL="postgres://postgres:postgres@localhost:5432/quasar"
export QUASAR_WAREHOUSE_PATH="s3://warehouse/"
export QUASAR_S3_ENDPOINT="http://localhost:9000"
export QUASAR_S3_ACCESS_KEY="minioadmin"
export QUASAR_S3_SECRET_KEY="minioadmin"
./target/release/quasar-server
```

### Run with Docker Compose

```bash
# Lance only (with MinIO + PostgreSQL)
docker-compose -f docker-compose.lance.yml up

# Iceberg + Spark integration test
docker-compose -f docker-compose.spark.yml up
```

## API Endpoints

### Infrastructure

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (includes DB check) |
| GET | `/metrics` | Prometheus metrics |

### Iceberg REST Catalog (`/iceberg/v1`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/config` | Catalog configuration |
| GET | `/namespaces` | List namespaces |
| POST | `/namespaces` | Create namespace |
| GET | `/namespaces/{ns}` | Get namespace |
| DELETE | `/namespaces/{ns}` | Drop namespace (must be empty) |
| POST | `/namespaces/{ns}/properties` | Update properties |
| GET | `/namespaces/{ns}/tables` | List tables |
| POST | `/namespaces/{ns}/tables` | Create table |
| GET | `/namespaces/{ns}/tables/{table}` | Load table |
| POST | `/namespaces/{ns}/tables/{table}` | Commit updates (CAS) |
| DELETE | `/namespaces/{ns}/tables/{table}` | Drop table |
| HEAD | `/namespaces/{ns}/tables/{table}` | Check table exists |
| POST | `/tables/rename` | Rename table |

### Lance REST Namespace (`/lance/v1`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/namespace/{id}/create` | Create namespace |
| GET | `/namespace/{id}/list` | List namespaces |
| POST | `/namespace/{id}/describe` | Describe namespace |
| POST | `/namespace/{id}/drop` | Drop namespace |
| POST | `/namespace/{id}/exists` | Check namespace exists |
| GET | `/namespace/{id}/table/list` | List tables |
| POST | `/table/{id}/declare` | Declare table (allocate location) |
| POST | `/table/{id}/describe` | Describe table |
| POST | `/table/{id}/register` | Register existing table |
| POST | `/table/{id}/deregister` | Deregister table |
| POST | `/table/{id}/drop` | Drop table |
| POST | `/table/{id}/exists` | Check table exists |
| POST | `/table/{id}/rename` | Rename table |
| POST | `/table/{id}/version/create` | Create version |
| GET | `/table/{id}/version/list` | List versions |
| POST | `/table/{id}/version/describe` | Describe version |

## Configuration

All configuration is loaded from environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `QUASAR_HOST` | `0.0.0.0` | HTTP bind address |
| `QUASAR_PORT` | `8080` | HTTP port |
| `QUASAR_DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/quasar` | PostgreSQL connection string |
| `QUASAR_LOG_LEVEL` | `info` | Log level (falls back to `RUST_LOG`) |
| `QUASAR_WAREHOUSE_PATH` | — | Default warehouse path (e.g. `s3://bucket/warehouse/`) |
| `QUASAR_S3_ENDPOINT` | — | S3/MinIO endpoint URL |
| `QUASAR_S3_ACCESS_KEY` | — | S3 access key |
| `QUASAR_S3_SECRET_KEY` | — | S3 secret key |
| `QUASAR_S3_REGION` | `us-east-1` | S3 region |
| `QUASAR_S3_ALLOW_HTTP` | `false` | Allow HTTP (not HTTPS) for S3 |
| `QUASAR_DB_MAX_CONNECTIONS` | `10` | PostgreSQL connection pool max size |

## Conditional Compilation

Quasar supports compile-time protocol selection via Cargo feature flags:

```bash
# Default: both protocols
cargo build

# Lance only
cargo build --no-default-features --features lance

# Iceberg only
cargo build --no-default-features --features iceberg

# Infrastructure only (health checks, no catalog protocols)
cargo build --no-default-features
```

## Input Validation

Namespace and table names are validated on creation:
- Length: 1 to 256 characters
- Allowed characters: alphanumeric, `_`, `-`, `.`, `/`
- Must not start with `.` (reserved)
- Must not be empty

## Metrics

The `/metrics` endpoint exposes Prometheus-format metrics:

- `http_requests_total{method,path,status}` — HTTP request counter
- `iceberg_commit_conflicts_total` — Iceberg CAS commit conflicts
- `iceberg_commit_successes_total` — Iceberg CAS commit successes

All HTTP responses include an `x-request-id` header for request tracing.

## Development

### Run tests

```bash
cd quasar
cargo test                      # All tests (requires embedded PostgreSQL)
cargo test -p quasar-core       # Core layer only
cargo test -p quasar-storage    # Storage layer only
cargo test -p quasar-adapter    # Adapter layer only
cargo test -p quasar-server     # Server layer only
```

### Run clippy

```bash
cargo clippy --all-features -- -D warnings
```

### Integration tests

```bash
# Lance Python SDK integration
docker-compose -f docker-compose.lance.yml up -d
python tests/lance_integration.py

# Spark Iceberg integration
docker-compose -f docker-compose.spark.yml up -d
python tests/spark_iceberg_integration.py
```

## Documentation

See `docs/` for detailed design documents:
- `ARCHITECTURE.md` — Architecture overview and design goals
- `mvp/MVP_REQUIREMENTS.md` — MVP functional and non-functional requirements
- `mvp/MVP_DESIGN.md` — MVP design decisions and crate layering
- `DEPLOYMENT.md` — Deployment guide and containerization
- `PROGRESS.md` — Development progress tracking
