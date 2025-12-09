-- =============================================================================
-- Migration: Add global_position to projection_checkpoints
-- For: PostgreSQL
-- Version: 001
-- =============================================================================
-- This migration adds the global_position column to the projection_checkpoints
-- table to support position-based checkpoint tracking for subscriptions.
--
-- This is an idempotent migration - safe to run multiple times.
-- =============================================================================

-- Add new column (nullable for backward compatibility)
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS global_position BIGINT;

-- Create index for position-based queries (partial index for non-null values)
CREATE INDEX IF NOT EXISTS idx_checkpoints_global_position
ON projection_checkpoints(global_position)
WHERE global_position IS NOT NULL;

-- Add column comment
COMMENT ON COLUMN projection_checkpoints.global_position IS 'Global position in the event stream';

-- Update the statistics view to include global_position
CREATE OR REPLACE VIEW projection_checkpoint_stats AS
SELECT
    projection_name,
    last_event_id,
    last_event_type,
    last_processed_at,
    global_position,
    events_processed,
    created_at,
    updated_at,
    EXTRACT(EPOCH FROM (NOW() - last_processed_at)) AS seconds_since_last_event,
    EXTRACT(EPOCH FROM (NOW() - updated_at)) AS seconds_since_update
FROM projection_checkpoints
ORDER BY updated_at DESC;
