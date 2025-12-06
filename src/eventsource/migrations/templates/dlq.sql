-- =============================================================================
-- Dead Letter Queue (DLQ) Table Schema
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
-- PostgreSQL 12+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    -- Auto-incrementing ID for ordering and pagination
    id BIGSERIAL PRIMARY KEY,

    -- Reference to the failed event
    event_id UUID NOT NULL,

    -- Projection that failed to process the event
    projection_name VARCHAR(255) NOT NULL,

    -- Event metadata
    event_type VARCHAR(255) NOT NULL,
    event_data JSONB NOT NULL,

    -- Error information
    error_message TEXT NOT NULL,
    error_stacktrace TEXT,

    -- Retry tracking
    retry_count INTEGER NOT NULL DEFAULT 0,

    -- Failure timestamps
    first_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Status management
    -- failed: processing failed, awaiting manual review
    -- retrying: currently being retried
    -- resolved: manually resolved (skip or reprocessed)
    status VARCHAR(20) NOT NULL DEFAULT 'failed',

    -- Resolution tracking
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(255),

    -- Ensure valid status values
    CONSTRAINT chk_dlq_status
        CHECK (status IN ('failed', 'retrying', 'resolved')),

    -- Ensure unique combination of event and projection
    -- An event can fail for multiple projections, but only one entry per pair
    CONSTRAINT uq_dlq_event_projection
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

-- =============================================================================
-- Helper Functions
-- =============================================================================

-- Function to add or update a failed event in the DLQ
-- Uses UPSERT pattern - updates retry count if already exists
CREATE OR REPLACE FUNCTION add_to_dlq(
    p_event_id UUID,
    p_projection_name VARCHAR(255),
    p_event_type VARCHAR(255),
    p_event_data JSONB,
    p_error_message TEXT,
    p_error_stacktrace TEXT DEFAULT NULL,
    p_retry_count INTEGER DEFAULT 0
) RETURNS BIGINT AS $$
DECLARE
    result_id BIGINT;
BEGIN
    INSERT INTO dead_letter_queue
        (event_id, projection_name, event_type, event_data,
         error_message, error_stacktrace, retry_count,
         first_failed_at, last_failed_at, status)
    VALUES
        (p_event_id, p_projection_name, p_event_type, p_event_data,
         p_error_message, p_error_stacktrace, p_retry_count,
         NOW(), NOW(), 'failed')
    ON CONFLICT (event_id, projection_name) DO UPDATE
    SET retry_count = EXCLUDED.retry_count,
        last_failed_at = NOW(),
        error_message = EXCLUDED.error_message,
        error_stacktrace = EXCLUDED.error_stacktrace,
        status = 'failed'
    RETURNING id INTO result_id;

    RETURN result_id;
END;
$$ LANGUAGE plpgsql;

-- Function to mark a DLQ entry as resolved
CREATE OR REPLACE FUNCTION resolve_dlq_entry(
    p_dlq_id BIGINT,
    p_resolved_by VARCHAR(255)
) RETURNS BOOLEAN AS $$
DECLARE
    updated BOOLEAN;
BEGIN
    UPDATE dead_letter_queue
    SET status = 'resolved',
        resolved_at = NOW(),
        resolved_by = p_resolved_by
    WHERE id = p_dlq_id
      AND status != 'resolved';

    GET DIAGNOSTICS updated = FOUND;
    RETURN updated;
END;
$$ LANGUAGE plpgsql;

-- Function to mark a DLQ entry as retrying
CREATE OR REPLACE FUNCTION mark_dlq_retrying(
    p_dlq_id BIGINT
) RETURNS BOOLEAN AS $$
DECLARE
    updated BOOLEAN;
BEGIN
    UPDATE dead_letter_queue
    SET status = 'retrying'
    WHERE id = p_dlq_id
      AND status = 'failed';

    GET DIAGNOSTICS updated = FOUND;
    RETURN updated;
END;
$$ LANGUAGE plpgsql;

-- Function to get DLQ statistics
CREATE OR REPLACE FUNCTION get_dlq_stats()
RETURNS TABLE (
    total_failed BIGINT,
    total_retrying BIGINT,
    total_resolved BIGINT,
    affected_projections BIGINT,
    oldest_failure TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*) FILTER (WHERE dlq.status = 'failed') AS total_failed,
        COUNT(*) FILTER (WHERE dlq.status = 'retrying') AS total_retrying,
        COUNT(*) FILTER (WHERE dlq.status = 'resolved') AS total_resolved,
        COUNT(DISTINCT projection_name) FILTER (WHERE dlq.status IN ('failed', 'retrying')) AS affected_projections,
        MIN(first_failed_at) FILTER (WHERE dlq.status IN ('failed', 'retrying')) AS oldest_failure
    FROM dead_letter_queue dlq;
END;
$$ LANGUAGE plpgsql;

-- Function to get failure counts per projection
CREATE OR REPLACE FUNCTION get_dlq_projection_stats()
RETURNS TABLE (
    projection_name VARCHAR(255),
    failure_count BIGINT,
    oldest_failure TIMESTAMP WITH TIME ZONE,
    most_recent_failure TIMESTAMP WITH TIME ZONE,
    avg_retry_count DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        dlq.projection_name,
        COUNT(*) AS failure_count,
        MIN(dlq.first_failed_at) AS oldest_failure,
        MAX(dlq.last_failed_at) AS most_recent_failure,
        AVG(dlq.retry_count)::DOUBLE PRECISION AS avg_retry_count
    FROM dead_letter_queue dlq
    WHERE dlq.status IN ('failed', 'retrying')
    GROUP BY dlq.projection_name
    ORDER BY failure_count DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old resolved entries
CREATE OR REPLACE FUNCTION cleanup_resolved_dlq(
    retention_days INTEGER DEFAULT 30
) RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM dead_letter_queue
    WHERE status = 'resolved'
      AND resolved_at < NOW() - (retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to retry all failed events for a projection
CREATE OR REPLACE FUNCTION retry_projection_dlq(
    p_projection_name VARCHAR(255)
) RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE dead_letter_queue
    SET status = 'retrying'
    WHERE projection_name = p_projection_name
      AND status = 'failed';

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Views
-- =============================================================================

-- View for monitoring active DLQ entries
CREATE OR REPLACE VIEW dlq_active_failures AS
SELECT
    id,
    event_id,
    projection_name,
    event_type,
    error_message,
    retry_count,
    first_failed_at,
    last_failed_at,
    status,
    EXTRACT(EPOCH FROM (NOW() - first_failed_at)) / 3600 AS hours_since_first_failure
FROM dead_letter_queue
WHERE status IN ('failed', 'retrying')
ORDER BY first_failed_at DESC;

-- View for failure summary by projection
CREATE OR REPLACE VIEW dlq_summary AS
SELECT
    projection_name,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
    COUNT(*) FILTER (WHERE status = 'retrying') AS retrying_count,
    MIN(first_failed_at) AS oldest_failure,
    MAX(last_failed_at) AS most_recent_failure
FROM dead_letter_queue
WHERE status IN ('failed', 'retrying')
GROUP BY projection_name;

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON TABLE dead_letter_queue IS 'Stores events that failed processing after all retries';
COMMENT ON COLUMN dead_letter_queue.id IS 'Auto-incrementing ID for ordering';
COMMENT ON COLUMN dead_letter_queue.event_id IS 'Reference to the failed event';
COMMENT ON COLUMN dead_letter_queue.projection_name IS 'Projection that failed to process the event';
COMMENT ON COLUMN dead_letter_queue.event_type IS 'Type of the failed event';
COMMENT ON COLUMN dead_letter_queue.event_data IS 'Full event data for replay';
COMMENT ON COLUMN dead_letter_queue.error_message IS 'Error message from the failure';
COMMENT ON COLUMN dead_letter_queue.error_stacktrace IS 'Full stack trace for debugging';
COMMENT ON COLUMN dead_letter_queue.retry_count IS 'Number of retry attempts made';
COMMENT ON COLUMN dead_letter_queue.status IS 'Current status (failed, retrying, resolved)';
COMMENT ON COLUMN dead_letter_queue.resolved_by IS 'User who resolved the entry';
COMMENT ON FUNCTION add_to_dlq IS 'Adds or updates a failed event in the DLQ';
COMMENT ON FUNCTION resolve_dlq_entry IS 'Marks a DLQ entry as resolved';
COMMENT ON FUNCTION get_dlq_stats IS 'Returns aggregate DLQ statistics';
COMMENT ON FUNCTION cleanup_resolved_dlq IS 'Removes old resolved entries';
