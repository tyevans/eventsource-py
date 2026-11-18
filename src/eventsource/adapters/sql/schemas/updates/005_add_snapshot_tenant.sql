-- =============================================================================
-- Migration: Add tenant_id to snapshots
-- For: PostgreSQL
-- Version: 005
-- =============================================================================
-- REQUIRED before upgrading: the PostgreSQL snapshot store now reads and writes
-- snapshots.tenant_id, and fails with an undefined-column error without it.
--
-- Adds the nullable owner-tenant column to the snapshots table (ADR-0113).
-- Under a tenant scope, a snapshot is used only when its tenant_id equals the
-- active tenant. Existing rows get NULL and are ignored under a scope, so each
-- such aggregate is replayed once and then snapshotted again.
--
-- Metadata-only change: no rewrite, no backfill, no NOT NULL.
-- Idempotent: safe to run multiple times.
-- =============================================================================

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS tenant_id UUID;

COMMENT ON COLUMN snapshots.tenant_id IS 'Owner tenant of the source stream; NULL when untenanted';
