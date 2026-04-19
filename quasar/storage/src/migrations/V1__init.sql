CREATE TABLE IF NOT EXISTS namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    format TEXT NOT NULL CHECK (format IN ('iceberg', 'lance')),
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(namespace_id, name)
);

CREATE TABLE IF NOT EXISTS asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version_id BIGINT NOT NULL,
    metadata_location TEXT NOT NULL,
    previous_version_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, version_id)
);

CREATE INDEX idx_assets_namespace ON assets(namespace_id);
CREATE INDEX idx_versions_asset ON asset_versions(asset_id);
