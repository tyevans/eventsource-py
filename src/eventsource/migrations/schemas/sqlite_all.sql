-- =============================================================================
-- EventSource Library - Complete Database Schema (SQLite)
-- =============================================================================
-- This file contains all tables required for the eventsource library.
--
-- Tables included:
--   1. events - Main event store table
--   2. event_outbox - Transactional outbox for reliable publishing
--   3. projection_checkpoints - Projection position tracking
--   4. dead_letter_queue - Failed event processing storage
--   5. snapshots - Aggregate state snapshots for fast loading
--
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - JSON stored as TEXT (no native JSONB support)
--   - global_position uses INTEGER PRIMARY KEY AUTOINCREMENT
--   - No stored functions (handled in application code)
--
-- Usage:
--   Run this file against a SQLite 3.8+ database to create all tables.
--
-- Generated from individual schema templates.
-- =============================================================================

-- =============================================================================
-- 1. Events Table
-- =============================================================================
-- The core event store table for event sourcing.

CREATE TABLE IF NOT EXISTS events (
    global_position INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    event_type TEXT NOT NULL,
    tenant_id TEXT,
    actor_id TEXT,
    version INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (aggregate_id, aggregate_type, version)
);

-- Events indexes
CREATE INDEX IF NOT EXISTS idx_events_aggregate_id ON events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_type ON events (aggregate_type);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_tenant_id ON events (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_type_tenant_timestamp ON events (aggregate_type, tenant_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version ON events (aggregate_id, aggregate_type, version);

-- =============================================================================
-- 2. Event Outbox Table
-- =============================================================================
-- Implements the transactional outbox pattern for reliable event publishing.

CREATE TABLE IF NOT EXISTS event_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    tenant_id TEXT,
    event_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    published_at TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    CHECK (status IN ('pending', 'published', 'failed'))
);

-- Outbox indexes
CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON event_outbox (status, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_event_id ON event_outbox (event_id);
CREATE INDEX IF NOT EXISTS idx_outbox_tenant_id ON event_outbox (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_failed ON event_outbox (created_at) WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON event_outbox (published_at) WHERE status = 'published';

-- =============================================================================
-- 3. Projection Checkpoints Table
-- =============================================================================
-- Tracks the position of each projection in the event stream.

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name TEXT PRIMARY KEY,
    last_event_id TEXT,
    last_event_type TEXT,
    last_processed_at TEXT,
    global_position INTEGER,
    events_processed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Checkpoint indexes
CREATE INDEX IF NOT EXISTS idx_checkpoints_last_processed ON projection_checkpoints (last_processed_at);
CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at ON projection_checkpoints (updated_at);

-- Auto-update trigger for updated_at
CREATE TRIGGER IF NOT EXISTS trg_checkpoint_updated_at
    AFTER UPDATE ON projection_checkpoints
    FOR EACH ROW
BEGIN
    UPDATE projection_checkpoints
    SET updated_at = datetime('now')
    WHERE projection_name = NEW.projection_name;
END;

-- =============================================================================
-- 4. Dead Letter Queue Table
-- =============================================================================
-- Stores events that failed processing after all retry attempts.

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    projection_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT NOT NULL,
    error_message TEXT NOT NULL,
    error_stacktrace TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    first_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    status TEXT NOT NULL DEFAULT 'failed',
    resolved_at TEXT,
    resolved_by TEXT,
    CHECK (status IN ('failed', 'retrying', 'resolved')),
    UNIQUE (event_id, projection_name)
);

-- DLQ indexes
CREATE INDEX IF NOT EXISTS idx_dlq_status ON dead_letter_queue (status);
CREATE INDEX IF NOT EXISTS idx_dlq_projection_name ON dead_letter_queue (projection_name);
CREATE INDEX IF NOT EXISTS idx_dlq_event_id ON dead_letter_queue (event_id);
CREATE INDEX IF NOT EXISTS idx_dlq_first_failed_at ON dead_letter_queue (first_failed_at);
CREATE INDEX IF NOT EXISTS idx_dlq_projection_status ON dead_letter_queue (projection_name, status);
CREATE INDEX IF NOT EXISTS idx_dlq_active_failures ON dead_letter_queue (first_failed_at) WHERE status IN ('failed', 'retrying');
CREATE INDEX IF NOT EXISTS idx_dlq_resolved_at ON dead_letter_queue (resolved_at) WHERE status = 'resolved';

-- =============================================================================
-- 5. Snapshots Table
-- =============================================================================
-- Stores aggregate state snapshots for performance optimization.
-- Instead of replaying all events, aggregates can be restored from a snapshot.

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (aggregate_id, aggregate_type)
);

-- Snapshots indexes
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup ON snapshots(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type ON snapshots(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version ON snapshots(aggregate_type, schema_version);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON snapshots(created_at);

-- =============================================================================
-- Verification Query
-- =============================================================================
-- Run this to verify all tables were created correctly:
/*
SELECT name, type
FROM sqlite_master
WHERE type = 'table'
  AND name IN ('events', 'event_outbox', 'projection_checkpoints', 'dead_letter_queue', 'snapshots')
ORDER BY name;
*/
