-- =============================================================================
-- EventSource Library - Complete Database Schema
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
-- Usage:
--   Run this file against a PostgreSQL 12+ database to create all tables.
--   For production with high volume, consider using events_partitioned.sql
--   instead of the basic events table.
--
-- Generated from individual schema templates.
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- For UUID generation

BEGIN;

-- =============================================================================
-- 1. Events Table (Non-Partitioned Version)
-- =============================================================================
-- The core event store table. Use events_partitioned.sql for high-volume
-- deployments instead.

CREATE TABLE IF NOT EXISTS events (
    global_position BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    actor_id VARCHAR(255),
    version INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_events_aggregate_version
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

COMMENT ON TABLE events IS 'Immutable event store containing all domain events';

-- =============================================================================
-- 2. Event Outbox Table
-- =============================================================================
-- Implements the transactional outbox pattern for reliable event publishing.

CREATE TABLE IF NOT EXISTS event_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    CONSTRAINT chk_outbox_status CHECK (status IN ('pending', 'published', 'failed'))
);

-- Outbox indexes
CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON event_outbox (status, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON event_outbox (created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_event_id ON event_outbox (event_id);
CREATE INDEX IF NOT EXISTS idx_outbox_tenant_id ON event_outbox (tenant_id) WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_failed ON event_outbox (created_at) WHERE status = 'failed';
CREATE INDEX IF NOT EXISTS idx_outbox_published_at ON event_outbox (published_at) WHERE status = 'published';

COMMENT ON TABLE event_outbox IS 'Transactional outbox for reliable event publishing';

-- =============================================================================
-- 3. Projection Checkpoints Table
-- =============================================================================
-- Tracks the position of each projection in the event stream.

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_event_id UUID,
    last_event_type VARCHAR(255),
    last_processed_at TIMESTAMP WITH TIME ZONE,
    global_position BIGINT,
    events_processed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Checkpoint indexes
CREATE INDEX IF NOT EXISTS idx_checkpoints_last_processed ON projection_checkpoints (last_processed_at);
CREATE INDEX IF NOT EXISTS idx_checkpoints_updated_at ON projection_checkpoints (updated_at);
CREATE INDEX IF NOT EXISTS idx_checkpoints_global_position ON projection_checkpoints (global_position) WHERE global_position IS NOT NULL;

-- Auto-update trigger for updated_at
CREATE OR REPLACE FUNCTION update_checkpoint_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_checkpoint_updated_at ON projection_checkpoints;
CREATE TRIGGER trg_checkpoint_updated_at
    BEFORE UPDATE ON projection_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION update_checkpoint_timestamp();

COMMENT ON TABLE projection_checkpoints IS 'Tracks projection positions in the event stream';

-- =============================================================================
-- 4. Dead Letter Queue Table
-- =============================================================================
-- Stores events that failed processing after all retry attempts.

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    projection_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data JSONB NOT NULL,
    error_message TEXT NOT NULL,
    error_stacktrace TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'failed',
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(255),
    CONSTRAINT chk_dlq_status CHECK (status IN ('failed', 'retrying', 'resolved')),
    CONSTRAINT uq_dlq_event_projection UNIQUE (event_id, projection_name)
);

-- DLQ indexes
CREATE INDEX IF NOT EXISTS idx_dlq_status ON dead_letter_queue (status);
CREATE INDEX IF NOT EXISTS idx_dlq_projection_name ON dead_letter_queue (projection_name);
CREATE INDEX IF NOT EXISTS idx_dlq_event_id ON dead_letter_queue (event_id);
CREATE INDEX IF NOT EXISTS idx_dlq_first_failed_at ON dead_letter_queue (first_failed_at);
CREATE INDEX IF NOT EXISTS idx_dlq_projection_status ON dead_letter_queue (projection_name, status);
CREATE INDEX IF NOT EXISTS idx_dlq_active_failures ON dead_letter_queue (first_failed_at) WHERE status IN ('failed', 'retrying');
CREATE INDEX IF NOT EXISTS idx_dlq_resolved_at ON dead_letter_queue (resolved_at) WHERE status = 'resolved';

COMMENT ON TABLE dead_letter_queue IS 'Stores events that failed processing after all retries';

-- =============================================================================
-- 5. Snapshots Table
-- =============================================================================
-- Stores aggregate state snapshots for fast loading.
-- One snapshot per aggregate (keyed by aggregate_id + aggregate_type).

CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshots_aggregate UNIQUE (aggregate_id, aggregate_type)
);

-- Snapshots indexes
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup
    ON snapshots(aggregate_id, aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots(aggregate_type);
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version
    ON snapshots(aggregate_type, schema_version);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots(created_at);

COMMENT ON TABLE snapshots IS 'Point-in-time aggregate state snapshots for fast loading';

COMMIT;

-- =============================================================================
-- Verification Query
-- =============================================================================
-- Run this to verify all tables were created correctly:
/*
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('events', 'event_outbox', 'projection_checkpoints', 'dead_letter_queue', 'snapshots')
ORDER BY table_name;
*/
