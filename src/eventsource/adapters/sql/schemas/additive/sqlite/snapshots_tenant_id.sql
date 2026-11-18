-- Additive fragment: owner tenant of a snapshot (ADR-0113).
-- SQLite has no ADD COLUMN IF NOT EXISTS: this fragment is safe only
-- against a table that does not already have the column. Callers that
-- may re-apply a schema to an existing database must not execute it
-- blindly; the SQLite snapshot store composes its schema with
-- additive=False and guards the ALTER with PRAGMA table_info
-- (adapters/sqlite/snapshots.py, _ensure_tenant_column).
ALTER TABLE snapshots ADD COLUMN tenant_id TEXT;
