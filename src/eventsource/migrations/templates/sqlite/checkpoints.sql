-- =============================================================================
-- Projection Checkpoints Table Schema (SQLite)
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
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - No triggers for updated_at (handled in application code)
--   - No stored functions (handled in application code)
--
-- SQLite 3.8+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    -- Projection identifier (unique name)
    -- Examples: "OrderSummaryProjection", "UserAnalyticsProjection"
    projection_name TEXT PRIMARY KEY,

    -- Last successfully processed event (UUID as TEXT)
    -- NULL if projection has never processed any events
    last_event_id TEXT,
    last_event_type TEXT,
    last_processed_at TEXT,

    -- Global position in the event stream
    -- Used for position-based checkpoint tracking
    global_position INTEGER,

    -- Processing statistics
    events_processed INTEGER NOT NULL DEFAULT 0,

    -- Lifecycle timestamps (ISO 8601 format)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
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

-- =============================================================================
-- Trigger for automatic updated_at
-- =============================================================================
-- SQLite supports triggers for automatic timestamp updates

CREATE TRIGGER IF NOT EXISTS trg_checkpoint_updated_at
    AFTER UPDATE ON projection_checkpoints
    FOR EACH ROW
BEGIN
    UPDATE projection_checkpoints
    SET updated_at = datetime('now')
    WHERE projection_name = NEW.projection_name;
END;
