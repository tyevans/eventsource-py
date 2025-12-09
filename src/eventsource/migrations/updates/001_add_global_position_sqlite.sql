-- =============================================================================
-- Migration: Add global_position to projection_checkpoints
-- For: SQLite
-- Version: 001
-- =============================================================================
-- This migration adds the global_position column to the projection_checkpoints
-- table to support position-based checkpoint tracking for subscriptions.
--
-- Note: SQLite does not support ADD COLUMN IF NOT EXISTS syntax.
-- This migration assumes the column does not already exist.
-- If running against an already-migrated database, this will fail silently
-- or you should check the schema first.
-- =============================================================================

-- Add new column (nullable for backward compatibility)
-- SQLite allows adding nullable columns directly
ALTER TABLE projection_checkpoints
ADD COLUMN global_position INTEGER;
