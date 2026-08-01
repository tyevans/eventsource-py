-- =============================================================================
-- Migration: Add position_token to projection_checkpoints
-- For: PostgreSQL
-- Version: 002
-- =============================================================================
-- This migration adds the position_token column to the projection_checkpoints
-- table to support opaque position-token checkpoint tracking for subscriptions.
--
-- This is an idempotent migration - safe to run multiple times.
-- =============================================================================

-- Add new column (nullable for backward compatibility)
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS position_token TEXT;

-- Create index for position-token-based queries (partial index for non-null values)
CREATE INDEX IF NOT EXISTS idx_checkpoints_position_token
ON projection_checkpoints (position_token)
WHERE position_token IS NOT NULL;

-- Add column comment
COMMENT ON COLUMN projection_checkpoints.position_token IS 'Opaque position token in the event stream';
