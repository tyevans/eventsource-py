-- Additive fragment: owner tenant of a dead letter entry (ADR-0114).
-- SQLite has no ADD COLUMN IF NOT EXISTS: this fragment is safe only
-- against a table that does not already have the column. The runtime
-- SQLite connect path composes its schema with additive=False, so a
-- database created that way does not get this column. Callers that need
-- the column must add it with updates/007_add_dlq_tenant_sqlite.sql.
ALTER TABLE dead_letter_queue ADD COLUMN tenant_id TEXT;

CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id
    ON dead_letter_queue (tenant_id);
