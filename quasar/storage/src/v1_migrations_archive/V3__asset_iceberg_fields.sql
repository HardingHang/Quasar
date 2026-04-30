-- Add Iceberg-specific fields to the assets table
ALTER TABLE assets
    ADD COLUMN IF NOT EXISTS location TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS metadata_location TEXT,
    ADD COLUMN IF NOT EXISTS schema_snapshot JSONB;

-- For existing Lance assets: extract location from properties
UPDATE assets
SET location = COALESCE(properties->>'location', '')
WHERE location = '';

-- Clean up 'location' key from properties now that it has its own column
UPDATE assets
SET properties = properties - 'location'
WHERE properties ? 'location';

-- Index for Iceberg metadata lookups
CREATE INDEX IF NOT EXISTS idx_assets_metadata_location ON assets(namespace_id, name);
