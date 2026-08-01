-- =============================================================================
-- Dead Letter Queue (DLQ) Table Schema (SQLite)
-- =============================================================================
-- Stores events that failed processing after all retry attempts.
--
-- The DLQ serves as a safety net for event processing failures, enabling:
--   - Manual investigation of processing failures
--   - Replay/retry mechanisms after fixes are deployed
--   - Failure monitoring and alerting
--   - Audit trail of processing issues
--
-- Events end up in the DLQ when:
--   1. Projection handler throws an unrecoverable error
--   2. Maximum retry count is exceeded
--   3. Event data is malformed or incompatible
--
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - JSON stored as TEXT (no native JSONB support)
--   - No stored functions (handled in application code)
--
-- SQLite 3.8+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    -- Auto-incrementing ID for ordering and pagination
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Reference to the failed event (UUID as TEXT)
    event_id TEXT NOT NULL,

    -- Projection that failed to process the event
    projection_name TEXT NOT NULL,

    -- Event metadata
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,

    -- Error information
    error_message TEXT NOT NULL,
    error_stacktrace TEXT,

    -- Retry tracking
    retry_count INTEGER NOT NULL DEFAULT 0,

    -- Failure timestamps (ISO 8601 format)
    first_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_failed_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Status management
    -- failed: processing failed, awaiting manual review
    -- retrying: currently being retried
    -- resolved: manually resolved (skip or reprocessed)
    status TEXT NOT NULL DEFAULT 'failed',

    -- Resolution tracking
    resolved_at TEXT,
    resolved_by TEXT,

    -- Ensure valid status values
    CHECK (status IN ('failed', 'retrying', 'resolved')),

    -- Ensure unique combination of event and projection
    -- An event can fail for multiple projections, but only one entry per pair
    UNIQUE (event_id, projection_name)
);

-- =============================================================================
-- Indexes
-- =============================================================================

-- Index for finding failed events by status
CREATE INDEX IF NOT EXISTS idx_dlq_status
    ON dead_letter_queue (status);

-- Index for projection-specific queries
CREATE INDEX IF NOT EXISTS idx_dlq_projection_name
    ON dead_letter_queue (projection_name);

-- Index for finding events by event_id
CREATE INDEX IF NOT EXISTS idx_dlq_event_id
    ON dead_letter_queue (event_id);

-- Index for finding oldest failures
CREATE INDEX IF NOT EXISTS idx_dlq_first_failed_at
    ON dead_letter_queue (first_failed_at);

-- Composite index for projection + status queries
CREATE INDEX IF NOT EXISTS idx_dlq_projection_status
    ON dead_letter_queue (projection_name, status);

-- Partial index for active failures only
CREATE INDEX IF NOT EXISTS idx_dlq_active_failures
    ON dead_letter_queue (first_failed_at)
    WHERE status IN ('failed', 'retrying');

-- Index for cleanup of resolved entries
CREATE INDEX IF NOT EXISTS idx_dlq_resolved_at
    ON dead_letter_queue (resolved_at)
    WHERE status = 'resolved';
