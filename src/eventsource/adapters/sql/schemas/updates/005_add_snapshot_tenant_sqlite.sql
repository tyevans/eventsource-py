-- =============================================================================
-- Migration: Add tenant_id to snapshots
-- For: SQLite
-- Version: 005
-- =============================================================================
-- Adds the nullable owner-tenant column to the snapshots table (ADR-0113).
-- Under a tenant scope, a snapshot is used only when its tenant_id equals the
-- active tenant. Existing rows get NULL and are ignored under a scope.
--
-- Note: SQLite does not support ADD COLUMN IF NOT EXISTS, and this script is
-- not re-runnable. The SQLite snapshot store performs the same addition at
-- open time, guarded by PRAGMA table_info (adapters/sqlite/snapshots.py), so
-- applying this script by hand is optional.
-- =============================================================================

ALTER TABLE snapshots ADD COLUMN tenant_id TEXT;
