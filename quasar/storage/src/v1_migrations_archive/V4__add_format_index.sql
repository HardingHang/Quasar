-- Add composite index on assets for common lookup patterns.
CREATE INDEX IF NOT EXISTS idx_assets_namespace_name ON assets(namespace_id, name);