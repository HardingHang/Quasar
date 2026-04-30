CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- namespaces: format-agnostic organization unit
-- ============================================
CREATE TABLE IF NOT EXISTS namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================
-- assets: generic asset registry
-- ============================================
CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('table')),
    asset_subtype TEXT NOT NULL CHECK (asset_subtype IN ('iceberg', 'lance')),
    comment TEXT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(namespace_id, name, asset_type, asset_subtype)
);

CREATE INDEX idx_assets_namespace ON assets(namespace_id);
CREATE INDEX idx_assets_type ON assets(asset_type);
CREATE INDEX idx_assets_namespace_type_subtype ON assets(namespace_id, asset_type, asset_subtype);

-- ============================================
-- tabular_assets: table asset detail table
-- ============================================
CREATE TABLE IF NOT EXISTS tabular_assets (
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    location TEXT NOT NULL,
    metadata_location TEXT,
    schema_snapshot JSONB
);

-- ============================================
-- asset_versions: generic version registry
-- ============================================
CREATE TABLE IF NOT EXISTS asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    version_order BIGINT,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, version_key)
);

CREATE INDEX idx_asset_versions_asset ON asset_versions(asset_id);
CREATE INDEX idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;
CREATE UNIQUE INDEX idx_asset_versions_asset_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;

-- ============================================
-- tabular_asset_versions: table asset version detail
-- ============================================
CREATE TABLE IF NOT EXISTS tabular_asset_versions (
    asset_version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    metadata_location TEXT NOT NULL,
    previous_asset_version_id UUID REFERENCES asset_versions(id)
);
