-- =============================================================================
-- Migration: Add tenant_id to dead_letter_queue
-- For: SQLite
-- Version: 007
-- =============================================================================
-- Adds the nullable owner-tenant column to the DLQ table (ADR-0114). Under a
-- tenant scope, DLQ reads and state changes match only rows of the active
-- tenant, and delete_resolved_events is refused. Existing rows get NULL and
-- are invisible under a scope.
--
-- Note: SQLite does not support ADD COLUMN IF NOT EXISTS, and this script is
-- not re-runnable. Run it once on a database that lacks the column. A second
-- run fails with "duplicate column name" at the ALTER.
-- =============================================================================

ALTER TABLE dead_letter_queue ADD COLUMN tenant_id TEXT;

CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id
    ON dead_letter_queue (tenant_id);
