-- =============================================================================
-- Migration: Add position tokens to migration bookkeeping tables
-- For: PostgreSQL
-- Version: 003
-- =============================================================================
-- This migration adds opaque position-token columns to
-- migration_position_mappings and tenant_migrations to support
-- token-based checkpoint tracking during store migrations, alongside the
-- existing BIGINT position columns.
--
-- The legacy NOT NULL constraint on source_position / target_position is
-- dropped because new inserts carry only tokens.
--
-- This is an idempotent migration - safe to run multiple times.
-- =============================================================================

-- Add new columns (nullable for backward compatibility)
ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS source_position_token TEXT;

ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS target_position_token TEXT;

-- New inserts carry only tokens; relax the legacy NOT NULL on the int columns
ALTER TABLE migration_position_mappings
ALTER COLUMN source_position DROP NOT NULL;

ALTER TABLE migration_position_mappings
ALTER COLUMN target_position DROP NOT NULL;

-- Each source position maps to exactly one target position, as the legacy
-- UNIQUE (migration_id, source_position) constraint expressed for tokens
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token)
WHERE source_position_token IS NOT NULL;

-- Indexes for token-based lookups
CREATE INDEX IF NOT EXISTS idx_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token);

CREATE INDEX IF NOT EXISTS idx_position_mappings_target_token
ON migration_position_mappings (migration_id, target_position_token);

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_source_position_token TEXT;

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_target_position_token TEXT;

-- Add column comments
COMMENT ON COLUMN migration_position_mappings.source_position_token IS 'Opaque position token in the source event stream';
COMMENT ON COLUMN migration_position_mappings.target_position_token IS 'Opaque position token in the target event stream';
COMMENT ON COLUMN tenant_migrations.last_source_position_token IS 'Opaque position token of the last position copied from the source event stream';
COMMENT ON COLUMN tenant_migrations.last_target_position_token IS 'Opaque position token of the last position copied to the target event stream';
