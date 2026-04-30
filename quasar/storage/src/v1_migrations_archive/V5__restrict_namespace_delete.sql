-- Enforce the product rule that namespaces can only be deleted when empty.
-- Older migrations created this FK with ON DELETE CASCADE; keep the canonical
-- constraint name so fresh and upgraded databases behave the same.

ALTER TABLE assets
    DROP CONSTRAINT IF EXISTS assets_namespace_id_fkey;

ALTER TABLE assets
    ADD CONSTRAINT assets_namespace_id_fkey
    FOREIGN KEY (namespace_id)
    REFERENCES namespaces(id)
    ON DELETE RESTRICT;
