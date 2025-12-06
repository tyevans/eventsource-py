-- =============================================================================
-- Event Outbox Table Schema
-- =============================================================================
-- Implements the transactional outbox pattern for reliable event publishing.
--
-- The outbox pattern ensures events are reliably published even when the
-- message broker (e.g., Redis, RabbitMQ) is temporarily unavailable.
--
-- How it works:
--   1. Events are written to both the events table AND the outbox table
--      in the same database transaction
--   2. A background worker polls the outbox for pending events
--   3. Events are published to the message broker
--   4. Successfully published events are marked as 'published'
--
-- Benefits:
--   - Guaranteed at-least-once delivery
--   - Transactional consistency between state and events
--   - Resilience to broker failures
--   - Decoupled publishing from request path
--
-- PostgreSQL 12+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS event_outbox (
    -- Unique outbox entry identifier
    -- Separate from event_id to allow re-queuing the same event
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Reference to the original event
    -- Note: No FK constraint because events table may be partitioned
    event_id UUID NOT NULL,

    -- Event metadata for filtering and routing
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Multi-tenancy support
    tenant_id UUID,

    -- Serialized event data for publishing
    -- Contains full event payload for reconstruction
    event_data JSONB NOT NULL,

    -- Lifecycle timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,

    -- Error tracking
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    -- Status management
    -- pending: awaiting publishing
    -- published: successfully sent to broker
    -- failed: exceeded retry limit or permanent failure
    status VARCHAR(20) NOT NULL DEFAULT 'pending',

    -- Ensure valid status values
    CONSTRAINT chk_outbox_status
        CHECK (status IN ('pending', 'published', 'failed'))
);

-- =============================================================================
-- Indexes
-- =============================================================================

-- Primary index for outbox worker polling
-- Optimizes: "get oldest pending events"
CREATE INDEX IF NOT EXISTS idx_outbox_status_created
    ON event_outbox (status, created_at)
    WHERE status = 'pending';

-- Partial index for pending events only
-- More efficient than full index for the common query pattern
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON event_outbox (created_at)
    WHERE status = 'pending';

-- Index for looking up events by event_id
-- Useful for idempotency checks and debugging
CREATE INDEX IF NOT EXISTS idx_outbox_event_id
    ON event_outbox (event_id);

-- Index for tenant-specific queries
CREATE INDEX IF NOT EXISTS idx_outbox_tenant_id
    ON event_outbox (tenant_id)
    WHERE tenant_id IS NOT NULL;

-- Index for failed events monitoring
CREATE INDEX IF NOT EXISTS idx_outbox_failed
    ON event_outbox (created_at)
    WHERE status = 'failed';

-- Index for cleanup of published events
CREATE INDEX IF NOT EXISTS idx_outbox_published_at
    ON event_outbox (published_at)
    WHERE status = 'published';

-- =============================================================================
-- Helper Functions
-- =============================================================================

-- Function to get next batch of pending events with row locking
-- Prevents multiple workers from processing the same events
CREATE OR REPLACE FUNCTION get_pending_outbox_events(
    batch_size INTEGER DEFAULT 100,
    max_retries INTEGER DEFAULT 5
) RETURNS SETOF event_outbox AS $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM event_outbox
    WHERE status = 'pending'
      AND retry_count < max_retries
    ORDER BY created_at ASC
    LIMIT batch_size
    FOR UPDATE SKIP LOCKED;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old published events
-- Should be run periodically to prevent table bloat
CREATE OR REPLACE FUNCTION cleanup_published_outbox_events(
    retention_days INTEGER DEFAULT 7
) RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM event_outbox
    WHERE status = 'published'
      AND published_at < NOW() - (retention_days || ' days')::INTERVAL
    RETURNING 1 INTO deleted_count;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to move failed events back to pending for retry
CREATE OR REPLACE FUNCTION retry_failed_outbox_events(
    max_age_hours INTEGER DEFAULT 24
) RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    UPDATE event_outbox
    SET status = 'pending',
        last_error = NULL
    WHERE status = 'failed'
      AND created_at > NOW() - (max_age_hours || ' hours')::INTERVAL;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW event_outbox_stats AS
SELECT
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
    COUNT(*) FILTER (WHERE status = 'published') AS published_count,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
    MIN(created_at) FILTER (WHERE status = 'pending') AS oldest_pending,
    AVG(retry_count) FILTER (WHERE status = 'pending') AS avg_retry_count,
    MAX(retry_count) AS max_retry_count,
    COUNT(*) AS total_count
FROM event_outbox;

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON TABLE event_outbox IS 'Transactional outbox for reliable event publishing';
COMMENT ON COLUMN event_outbox.id IS 'Unique outbox entry identifier';
COMMENT ON COLUMN event_outbox.event_id IS 'Reference to the original event';
COMMENT ON COLUMN event_outbox.event_type IS 'Type of event for routing';
COMMENT ON COLUMN event_outbox.aggregate_id IS 'Aggregate that produced this event';
COMMENT ON COLUMN event_outbox.aggregate_type IS 'Type of aggregate';
COMMENT ON COLUMN event_outbox.tenant_id IS 'Tenant ID for multi-tenant deployments';
COMMENT ON COLUMN event_outbox.event_data IS 'Serialized event data for publishing';
COMMENT ON COLUMN event_outbox.status IS 'Current publishing status';
COMMENT ON COLUMN event_outbox.retry_count IS 'Number of publishing attempts';
COMMENT ON COLUMN event_outbox.last_error IS 'Last error message if publishing failed';
COMMENT ON FUNCTION get_pending_outbox_events IS 'Gets pending events with row locking for concurrent workers';
COMMENT ON FUNCTION cleanup_published_outbox_events IS 'Removes old published events to prevent table bloat';
COMMENT ON FUNCTION retry_failed_outbox_events IS 'Moves failed events back to pending for retry';
