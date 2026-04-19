# Quasar

Quasar is a universal Catalog Service for Lakehouse architectures, supporting multiple data formats through standard protocols.

## Supported Protocols

- **Lance REST Namespace** (`/lance/v1/...`)
- **Iceberg REST Catalog** (`/iceberg/v1/...`)

## Architecture

```
quasar/
├── core/       # Core domain models and trait definitions
├── storage/    # PostgreSQL storage implementation
├── adapter/    # Protocol adapters (Iceberg, Lance)
└── server/     # HTTP server entrypoint
```

## Quick Start

```bash
cargo build
./target/debug/quasar-server
```

## Development

See `docs/` for design specifications and development conventions.
