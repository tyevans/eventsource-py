-- =============================================================================
-- Migration: Add tenant_id to dead_letter_queue
-- For: PostgreSQL
-- Version: 007
-- =============================================================================
-- Adds the nullable owner-tenant column to the DLQ table (ADR-0114). Under a
-- tenant scope, DLQ reads and state changes match only rows of the active
-- tenant, and delete_resolved_events is refused. Existing rows get NULL and
-- are invisible under a scope, so backfill them first if a tenant must see
-- them.
--
-- Run before upgrading to a release whose DLQ adapter writes tenant_id.
--
-- Metadata-only change: no rewrite, no backfill, no NOT NULL.
-- Idempotent: safe to run multiple times.
-- =============================================================================

ALTER TABLE dead_letter_queue ADD COLUMN IF NOT EXISTS tenant_id UUID;

CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id
    ON dead_letter_queue (tenant_id);

COMMENT ON COLUMN dead_letter_queue.tenant_id IS 'Owner tenant of the failed event; NULL when untenanted';
