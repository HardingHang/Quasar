-- Change namespaces unique constraint from (name) to (name, format)
-- to support namespace isolation between Iceberg and Lance formats.

ALTER TABLE namespaces
    DROP CONSTRAINT IF EXISTS namespaces_name_key;

ALTER TABLE namespaces
    ADD CONSTRAINT namespaces_name_format_key UNIQUE (name, format);
