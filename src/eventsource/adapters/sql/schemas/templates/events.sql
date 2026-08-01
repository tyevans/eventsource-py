-- =============================================================================
-- Event Store Table Schema
-- =============================================================================
-- This schema provides the core events table for event sourcing.
-- It supports both single-tenant and multi-tenant deployments.
--
-- Usage:
--   1. For simple deployments, use this non-partitioned version
--   2. For high-volume deployments, see events_partitioned.sql
--
-- PostgreSQL 12+ required for optimal performance.
-- =============================================================================

-- Events Table (Non-Partitioned)
-- ================================
-- This table stores all domain events in the system.
-- Each event represents a fact that occurred in the business domain.

CREATE TABLE IF NOT EXISTS events (
    -- Global position for ordered replay across all streams
    global_position BIGSERIAL PRIMARY KEY,

    -- Unique event identifier (UUID v4 recommended)
    event_id UUID NOT NULL UNIQUE,

    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Event metadata
    event_type VARCHAR(255) NOT NULL,

    -- Multi-tenancy support (optional)
    -- Set to NULL for single-tenant deployments
    tenant_id UUID,

    -- Actor/user who triggered the event
    -- Can be a user ID, system identifier, or service name
    actor_id VARCHAR(255),

    -- Optimistic concurrency control
    -- Version starts at 1 and increments for each event on an aggregate
    version INTEGER NOT NULL,

    -- Event timestamp (when the event occurred in the domain)
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Event payload stored as JSONB for flexible querying
    -- Contains the event-specific data
    payload JSONB NOT NULL,

    -- Technical metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Ensure no two events have the same version for an aggregate
    -- This prevents concurrent modifications and ensures stream integrity
    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);

-- =============================================================================
-- Indexes
-- =============================================================================
-- These indexes optimize common query patterns in event sourcing systems.

-- Index for loading aggregate event streams
-- Primary access pattern: get all events for an aggregate
CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
    ON events (aggregate_id);

-- Index for querying events by aggregate type
-- Useful for projections that process all events of a type
CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
    ON events (aggregate_type);

-- Index for querying events by event type
-- Useful for event-type-specific projections and handlers
CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type);

-- Index for time-based queries
-- Useful for projections catching up, auditing, and replay
CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp);

-- Partial index for multi-tenant queries
-- Only indexes rows where tenant_id is not null (saves space)
CREATE INDEX IF NOT EXISTS idx_events_tenant_id
    ON events (tenant_id)
    WHERE tenant_id IS NOT NULL;

-- Composite index for projection queries
-- Optimizes: "get all events of type X for tenant Y after time Z"
CREATE INDEX IF NOT EXISTS idx_events_type_tenant_timestamp
    ON events (aggregate_type, tenant_id, timestamp);

-- Index for aggregate stream loading with version ordering
-- Optimizes loading full aggregate history in order
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version);

-- =============================================================================
-- Comments for documentation
-- =============================================================================

COMMENT ON TABLE events IS 'Immutable event store containing all domain events';
COMMENT ON COLUMN events.event_id IS 'Globally unique event identifier';
COMMENT ON COLUMN events.aggregate_id IS 'ID of the aggregate this event belongs to';
COMMENT ON COLUMN events.aggregate_type IS 'Type name of the aggregate (e.g., Order, User)';
COMMENT ON COLUMN events.event_type IS 'Fully qualified event type name';
COMMENT ON COLUMN events.tenant_id IS 'Optional tenant ID for multi-tenant deployments';
COMMENT ON COLUMN events.actor_id IS 'ID of actor/user who caused this event';
COMMENT ON COLUMN events.version IS 'Aggregate version (optimistic concurrency)';
COMMENT ON COLUMN events.timestamp IS 'When the event occurred in the domain';
COMMENT ON COLUMN events.payload IS 'Event-specific data in JSONB format';
COMMENT ON COLUMN events.created_at IS 'When the event was persisted to the store';
