-- =============================================================================
-- Event Outbox Table Schema (SQLite)
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
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - JSON stored as TEXT (no native JSONB support)
--   - No stored functions (handled in application code)
--
-- SQLite 3.8+ recommended.
-- =============================================================================

CREATE TABLE IF NOT EXISTS event_outbox (
    -- Auto-incrementing ID for ordering
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Reference to the original event (UUID as TEXT)
    -- Note: No FK constraint because events table may be partitioned
    event_id TEXT NOT NULL,

    -- Event metadata for filtering and routing
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,

    -- Multi-tenancy support
    tenant_id TEXT,

    -- Serialized event data for publishing
    -- Contains full event payload for reconstruction (JSON as TEXT)
    event_data TEXT NOT NULL,

    -- Lifecycle timestamps (ISO 8601 format)
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    published_at TEXT,

    -- Error tracking
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    -- Status management
    -- pending: awaiting publishing
    -- published: successfully sent to broker
    -- failed: exceeded retry limit or permanent failure
    status TEXT NOT NULL DEFAULT 'pending',

    -- Ensure valid status values
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
