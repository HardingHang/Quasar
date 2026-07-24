-- 0001_init.up.sql — baseline schema (DESIGN §3.4 + §3.5).
--
-- Executed once in its own transaction by the migration runner
-- (src/migrate.rs); the runner records the version in schema_migrations
-- after this script succeeds. Registry seeds are idempotent so a partial
-- manual replay remains safe.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── Registries ─────────────────────────────────────────────────────────────

CREATE TABLE asset_types (
    name TEXT PRIMARY KEY,
    description TEXT,
    category TEXT NOT NULL CHECK (category IN ('tabular', 'view', 'model', 'agent', 'tool', 'mcp_server', 'fileset', 'topic', 'generic')),
    validation_schema JSONB,
    extension_strategy TEXT NOT NULL CHECK (extension_strategy IN ('jsonb', 'dedicated_table', 'reference_only')),
    supports_native_protocol BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE formats (
    name TEXT PRIMARY KEY,
    description TEXT,
    mime_type TEXT,
    serialization_hint TEXT
);

-- ── First layer: Domain ─────────────────────────────────────────────────────

CREATE TABLE domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB,
    storage_type TEXT CHECK (storage_type IN ('s3', 'minio', 'hdfs', 'local')),
    storage_config JSONB,
    warehouse TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Second layer: Namespace ─────────────────────────────────────────────────

CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    path TEXT NOT NULL,
    depth INTEGER NOT NULL,
    comment TEXT,
    properties JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(domain_id, path)
);

-- ── Third layer: Asset ──────────────────────────────────────────────────────

CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL REFERENCES asset_types(name),
    format TEXT REFERENCES formats(name),
    comment TEXT,
    properties JSONB,
    current_version_key TEXT,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_assets_active_name ON assets(namespace_id, name) WHERE deleted_at IS NULL;
CREATE INDEX idx_assets_type_active ON assets(asset_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_assets_namespace ON assets(namespace_id) WHERE deleted_at IS NULL;

-- ── Version layer ───────────────────────────────────────────────────────────

CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_properties JSONB,
    content_inline JSONB,
    content_pointer TEXT,
    previous_version_id UUID REFERENCES asset_versions(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(asset_id, version_key)
);

-- Single-root constraint: at most one root version per asset.
CREATE UNIQUE INDEX uq_asset_versions_root ON asset_versions(asset_id) WHERE previous_version_id IS NULL;

-- ── Tag layer ───────────────────────────────────────────────────────────────

CREATE TABLE asset_tags (
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, tag)
);

-- ── Type extension tables ───────────────────────────────────────────────────

CREATE TABLE tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB
);

CREATE TABLE view_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    view_uuid UUID,
    location TEXT,
    metadata_location TEXT
);

-- ── Migration bookkeeping ───────────────────────────────────────────────────

CREATE TABLE schema_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Iceberg adapter private tables (DESIGN §3.5) ───────────────────────────

CREATE TABLE iceberg_staged_tables (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_name TEXT NOT NULL,
    namespace_path TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_uuid UUID,
    location TEXT NOT NULL,
    metadata_location TEXT,
    metadata_json JSONB,
    properties JSONB,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(domain_name, namespace_path, table_name)
);

CREATE TABLE iceberg_scan_metrics_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    domain_name TEXT NOT NULL,
    namespace_path TEXT NOT NULL,
    table_name TEXT NOT NULL,
    report JSONB NOT NULL,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE iceberg_purge_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_name TEXT NOT NULL,
    namespace_path TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_location TEXT,
    metadata_location TEXT,
    status TEXT NOT NULL CHECK (status IN ('started', 'catalog_dropped', 'completed', 'failed')),
    error_message TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- ── Registry seeds (idempotent) ─────────────────────────────────────────────

INSERT INTO asset_types (name, description, category, extension_strategy, supports_native_protocol)
VALUES
    ('table', 'Tabular dataset', 'tabular', 'dedicated_table', TRUE),
    ('view', 'Logical view over tabular assets', 'view', 'dedicated_table', TRUE)
ON CONFLICT (name) DO NOTHING;

INSERT INTO formats (name, description, mime_type, serialization_hint)
VALUES
    ('iceberg', 'Apache Iceberg table format', 'application/json', 'json'),
    ('lance', 'Lance columnar format', NULL, NULL)
ON CONFLICT (name) DO NOTHING;

INSERT INTO domains (name, comment)
VALUES ('default', 'Default domain')
ON CONFLICT (name) DO NOTHING;
