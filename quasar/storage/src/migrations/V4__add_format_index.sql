-- Add index on namespaces(format) for faster list_namespaces queries.
CREATE INDEX IF NOT EXISTS idx_namespaces_format ON namespaces(format);

-- Add composite index on assets for common lookup patterns.
CREATE INDEX IF NOT EXISTS idx_assets_namespace_name ON assets(namespace_id, name);