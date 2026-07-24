-- 0001_init.down.sql — manual rollback of the baseline schema (DESIGN §8.4).
-- Development use only; production never runs down migrations automatically.
-- Dropped in dependency order (children before parents).

DROP TABLE IF EXISTS iceberg_purge_operations;
DROP TABLE IF EXISTS iceberg_scan_metrics_reports;
DROP TABLE IF EXISTS iceberg_staged_tables;
DROP TABLE IF EXISTS view_assets;
DROP TABLE IF EXISTS tabular_assets;
DROP TABLE IF EXISTS asset_tags;
DROP TABLE IF EXISTS asset_versions;
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS namespaces;
DROP TABLE IF EXISTS domains;
DROP TABLE IF EXISTS formats;
DROP TABLE IF EXISTS asset_types;
DROP TABLE IF EXISTS schema_migrations;
