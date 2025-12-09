-- =============================================================================
-- Projection Checkpoints Table Schema
-- =============================================================================
-- Tracks the position of each projection in the event stream.
--
-- Projections (read models) process events to build queryable views.
-- Checkpoints enable:
--   - Resumable processing after restarts
--   - Exactly-once processing semantics
--   - Lag monitoring and health checks
--   - Safe rebuilds from specific positions
--
-- PostgreSQL 12+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    -- Projection identifier (unique name)
    -- Examples: "OrderSummaryProjection", "UserAnalyticsProjection"
    projection_name VARCHAR(255) PRIMARY KEY,

    -- Last successfully processed event
    -- NULL if projection has never processed any events
    last_event_id UUID,
    last_event_type VARCHAR(255),
    last_processed_at TIMESTAMP WITH TIME ZONE,

    -- Global position in the event stream
    -- Used for position-based checkpoint tracking
    global_position BIGINT,

    -- Processing statistics
    events_processed BIGINT NOT NULL DEFAULT 0,

    -- Lifecycle timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- Indexes
-- =============================================================================

-- Index for monitoring projection lag by processing time
CREATE INDEX IF NOT EXISTS idx_checkpoints_last_processed
    ON projection_checkpoints (last_processed_at);

-- Index for finding stale projections
CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at
    ON projection_checkpoints (updated_at);

-- Index for position-based queries (partial index for non-null values)
CREATE INDEX IF NOT EXISTS idx_checkpoints_global_position
    ON projection_checkpoints (global_position)
    WHERE global_position IS NOT NULL;

-- =============================================================================
-- Trigger for automatic updated_at
-- =============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_checkpoint_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at on any modification
DROP TRIGGER IF EXISTS trg_checkpoint_updated_at ON projection_checkpoints;
CREATE TRIGGER trg_checkpoint_updated_at
    BEFORE UPDATE ON projection_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION update_checkpoint_timestamp();

-- =============================================================================
-- Helper Functions
-- =============================================================================

-- Function to update checkpoint (UPSERT pattern)
-- Idempotent - safe to call multiple times for the same event
CREATE OR REPLACE FUNCTION update_projection_checkpoint(
    p_projection_name VARCHAR(255),
    p_event_id UUID,
    p_event_type VARCHAR(255)
) RETURNS VOID AS $$
BEGIN
    INSERT INTO projection_checkpoints
        (projection_name, last_event_id, last_event_type,
         last_processed_at, events_processed, created_at, updated_at)
    VALUES
        (p_projection_name, p_event_id, p_event_type,
         NOW(), 1, NOW(), NOW())
    ON CONFLICT (projection_name) DO UPDATE
    SET last_event_id = EXCLUDED.last_event_id,
        last_event_type = EXCLUDED.last_event_type,
        last_processed_at = EXCLUDED.last_processed_at,
        events_processed = projection_checkpoints.events_processed + 1,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to reset a projection checkpoint
-- Used when rebuilding a projection from scratch
CREATE OR REPLACE FUNCTION reset_projection_checkpoint(
    p_projection_name VARCHAR(255)
) RETURNS BOOLEAN AS $$
DECLARE
    deleted BOOLEAN;
BEGIN
    DELETE FROM projection_checkpoints
    WHERE projection_name = p_projection_name;

    GET DIAGNOSTICS deleted = FOUND;
    RETURN deleted;
END;
$$ LANGUAGE plpgsql;

-- Function to get stale projections (not updated recently)
CREATE OR REPLACE FUNCTION get_stale_projections(
    stale_threshold_minutes INTEGER DEFAULT 60
) RETURNS TABLE (
    projection_name VARCHAR(255),
    last_event_id UUID,
    last_processed_at TIMESTAMP WITH TIME ZONE,
    events_processed BIGINT,
    minutes_since_update DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        pc.projection_name,
        pc.last_event_id,
        pc.last_processed_at,
        pc.events_processed,
        EXTRACT(EPOCH FROM (NOW() - pc.updated_at)) / 60 AS minutes_since_update
    FROM projection_checkpoints pc
    WHERE pc.updated_at < NOW() - (stale_threshold_minutes || ' minutes')::INTERVAL
    ORDER BY pc.updated_at ASC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Statistics View
-- =============================================================================

-- View for projection health monitoring
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

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON TABLE projection_checkpoints IS 'Tracks projection positions in the event stream';
COMMENT ON COLUMN projection_checkpoints.projection_name IS 'Unique projection identifier';
COMMENT ON COLUMN projection_checkpoints.last_event_id IS 'Last successfully processed event ID';
COMMENT ON COLUMN projection_checkpoints.last_event_type IS 'Type of the last processed event';
COMMENT ON COLUMN projection_checkpoints.last_processed_at IS 'When the last event was processed';
COMMENT ON COLUMN projection_checkpoints.global_position IS 'Global position in the event stream';
COMMENT ON COLUMN projection_checkpoints.events_processed IS 'Total count of events processed';
COMMENT ON FUNCTION update_projection_checkpoint IS 'Updates checkpoint using UPSERT pattern';
COMMENT ON FUNCTION reset_projection_checkpoint IS 'Resets checkpoint for rebuilding projection';
COMMENT ON FUNCTION get_stale_projections IS 'Returns projections that have not updated recently';
