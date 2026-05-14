-- ============================================================================
-- Quasar V3 initial database schema.
--
-- This file is the single source of truth for V3 catalog tables. It is not a
-- migration script and must not be placed under migrations/; the file is loaded
-- explicitly via `quasar_storage::schema::initialize` when a fresh database is
-- bootstrapped or a test harness rebuilds its schema.
--
-- All statements are idempotent so the script can be re-executed safely:
--   * tables and indexes use `IF NOT EXISTS`
--   * functions use `CREATE OR REPLACE`
--   * triggers are dropped before being (re)created
--   * registry seed rows use `ON CONFLICT DO NOTHING`
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ----------------------------------------------------------------------------
-- Registry tables (V3 §3.3.1)
--
-- Replace hard-coded CHECK constraints with name-keyed registries so new asset
-- types and tabular formats can be introduced without a schema migration.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asset_types (
    name TEXT PRIMARY KEY,
    comment TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tabular_formats (
    name TEXT PRIMARY KEY,
    comment TEXT,
    supports_cas_commit BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO asset_types (name, comment)
VALUES ('table', 'Tabular dataset')
ON CONFLICT DO NOTHING;

INSERT INTO tabular_formats (name, comment, supports_cas_commit)
VALUES
    ('iceberg', 'Apache Iceberg table', TRUE),
    ('lance', 'Lance dataset', FALSE)
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- domains (V3 §3.3.2)
--
-- Top-level governance container. Format is intentionally absent here; format
-- isolation lives on the REST endpoint, not on the domain.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    storage_type TEXT,
    storage_config JSONB NOT NULL DEFAULT '{}',
    warehouse TEXT,
    owner TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed a `default` domain so the Phase 2 storage layer can map the V2 trait
-- surface (no domain parameter) onto V3 schema by hardcoding this name. The
-- row is harmless once Phase 3 wires real domain routing from the request path
-- and may then be deleted by an operator if a default catalog is not desired.
INSERT INTO domains (name, comment)
VALUES ('default', 'Quasar default domain (V3 transitional, removable after Phase 3)')
ON CONFLICT DO NOTHING;

-- ----------------------------------------------------------------------------
-- namespaces (V3 §3.3.3)
--
-- Single-level namespace inside a domain. ON DELETE RESTRICT prevents a domain
-- from being deleted while it still contains namespaces.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(domain_id, name)
);

CREATE INDEX IF NOT EXISTS idx_namespaces_domain ON namespaces(domain_id);

-- ----------------------------------------------------------------------------
-- assets (V3 §3.3.4)
--
-- Generic identity layer for every kind of asset. The active uniqueness
-- constraint targets the (namespace_id, name) pair where deleted_at IS NULL so
-- soft-deleted assets do not block name reuse.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL REFERENCES asset_types(name),
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    deleted_at TIMESTAMPTZ,
    created_by TEXT,
    updated_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_assets_active_name
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_assets_type_active
    ON assets(asset_type)
    WHERE deleted_at IS NULL;

-- ----------------------------------------------------------------------------
-- tabular_assets (V3 §3.3.5)
--
-- Tabular-asset extension layer. `format` references tabular_formats(name).
-- A trigger enforces that the linked Asset must have asset_type='table'.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    format TEXT NOT NULL REFERENCES tabular_formats(name),
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tabular_assets_format_asset
    ON tabular_assets(format, asset_id);

CREATE OR REPLACE FUNCTION ensure_tabular_asset_type()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM assets
        WHERE id = NEW.asset_id
          AND asset_type = 'table'
    ) THEN
        RAISE EXCEPTION 'tabular asset % must reference an asset with asset_type=table', NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tabular_assets_type_check ON tabular_assets;
CREATE TRIGGER trg_tabular_assets_type_check
BEFORE INSERT OR UPDATE OF asset_id ON tabular_assets
FOR EACH ROW
EXECUTE FUNCTION ensure_tabular_asset_type();

-- ----------------------------------------------------------------------------
-- asset_versions (V3 §3.3.6)
--
-- Generic version identity. `version_key` is the format-native version label,
-- `version_order` is the optional comparable ordering. Latest queries must use
-- version_order; deriving order from version_key string parsing is forbidden.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_order BIGINT,
    previous_version_id UUID REFERENCES asset_versions(id) ON DELETE SET NULL,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, version_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_versions_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_asset_versions_previous
    ON asset_versions(previous_version_id);

CREATE OR REPLACE FUNCTION ensure_previous_version_same_asset()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.previous_version_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM asset_versions
           WHERE id = NEW.previous_version_id
             AND asset_id = NEW.asset_id
       ) THEN
        RAISE EXCEPTION 'previous_version_id % must reference a version of the same asset %',
            NEW.previous_version_id, NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_asset_versions_previous_version_check ON asset_versions;
CREATE TRIGGER trg_asset_versions_previous_version_check
BEFORE INSERT OR UPDATE OF asset_id, previous_version_id ON asset_versions
FOR EACH ROW
EXECUTE FUNCTION ensure_previous_version_same_asset();

-- ----------------------------------------------------------------------------
-- tabular_asset_versions (V3 §3.3.7)
--
-- Tabular-version extension. metadata_location is the manifest pointer for
-- this specific version.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tabular_asset_versions (
    version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    metadata_location TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- asset_permissions (V3 §3.3.8)
--
-- Reserved for V4 RBAC. The table exists in V3 so the data model is stable
-- for governance bind-in, but no API exposes it yet.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS asset_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    action TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    UNIQUE(asset_id, subject, action)
);

CREATE INDEX IF NOT EXISTS idx_asset_permissions_asset ON asset_permissions(asset_id);
CREATE INDEX IF NOT EXISTS idx_asset_permissions_subject ON asset_permissions(subject);
