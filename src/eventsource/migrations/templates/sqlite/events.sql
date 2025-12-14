-- =============================================================================
-- Event Store Table Schema (SQLite)
-- =============================================================================
-- This schema provides the core events table for event sourcing.
-- It supports both single-tenant and multi-tenant deployments.
--
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - JSON stored as TEXT (no native JSONB support)
--   - global_position uses INTEGER PRIMARY KEY AUTOINCREMENT
--
-- SQLite 3.8+ required for partial index support.
-- =============================================================================

-- Events Table
-- ================================
-- This table stores all domain events in the system.
-- Each event represents a fact that occurred in the business domain.

CREATE TABLE IF NOT EXISTS events (
    -- Global position for ordered replay across all streams
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Unique event identifier (UUID v4 as TEXT)
    event_id TEXT NOT NULL UNIQUE,

    -- Aggregate identification
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,

    -- Event metadata
    event_type TEXT NOT NULL,

    -- Multi-tenancy support (optional)
    -- Set to NULL for single-tenant deployments
    tenant_id TEXT,

    -- Actor/user who triggered the event
    -- Can be a user ID, system identifier, or service name
    actor_id TEXT,

    -- Optimistic concurrency control
    -- Version starts at 1 and increments for each event on an aggregate
    version INTEGER NOT NULL,

    -- Event timestamp (when the event occurred in the domain)
    -- Stored as ISO 8601 TEXT for portability
    timestamp TEXT NOT NULL,

    -- Event payload stored as TEXT (JSON format)
    -- Contains the event-specific data
    payload TEXT NOT NULL,

    -- Technical metadata
    -- Stored as ISO 8601 TEXT
    created_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Ensure no two events have the same version for an aggregate
    -- This prevents concurrent modifications and ensures stream integrity
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
