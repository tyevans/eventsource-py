-- =============================================================================
-- Snapshots Table Schema
-- =============================================================================
-- This schema provides the snapshots table for aggregate state caching.
-- Snapshots enable fast aggregate loading by capturing point-in-time state,
-- allowing the system to skip replay of events before the snapshot.
--
-- Design principles:
--   - One snapshot per aggregate (keyed by aggregate_id + aggregate_type)
--   - Upsert semantics on save (newer snapshot replaces older)
--   - Snapshots are optional - missing snapshots trigger full event replay
--   - Schema versioning supports aggregate state evolution
--
-- PostgreSQL 12+ required for optimal performance.
-- =============================================================================

-- Snapshots Table
-- ================================
-- This table stores aggregate state snapshots for fast loading.
-- Each snapshot represents a point-in-time capture of aggregate state.

CREATE TABLE IF NOT EXISTS snapshots (
    -- Surrogate key for internal references
    id BIGSERIAL PRIMARY KEY,

    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Snapshot versioning
    -- version: Aggregate version when this snapshot was taken
    version INTEGER NOT NULL,

    -- schema_version: State schema version for evolution support
    -- Used to detect incompatible schema changes and trigger full replay
    schema_version INTEGER NOT NULL DEFAULT 1,

    -- Serialized state
    -- JSONB enables efficient storage and indexing of aggregate state
    state JSONB NOT NULL,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Constraints
    -- Ensure one snapshot per aggregate (keyed by id + type)
    CONSTRAINT uq_snapshots_aggregate UNIQUE (aggregate_id, aggregate_type)
);

-- =============================================================================
-- Indexes
-- =============================================================================
-- These indexes optimize common query patterns for snapshot operations.

-- Index for primary lookup by aggregate
-- Main access pattern: get snapshot for a specific aggregate
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup
    ON snapshots(aggregate_id, aggregate_type);

-- Index for bulk operations by aggregate type
-- Useful for projections and schema migration bulk invalidation
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots(aggregate_type);

-- Index for schema version queries
-- Enables efficient bulk invalidation during schema migrations
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version
    ON snapshots(aggregate_type, schema_version);

-- Index for cleanup of old snapshots by date
-- Useful for maintenance and cleanup operations
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots(created_at);

-- =============================================================================
-- Comments for documentation
-- =============================================================================

COMMENT ON TABLE snapshots IS 'Point-in-time aggregate state snapshots for fast loading';
COMMENT ON COLUMN snapshots.id IS 'Surrogate primary key for internal references';
COMMENT ON COLUMN snapshots.aggregate_id IS 'UUID of the aggregate this snapshot belongs to';
COMMENT ON COLUMN snapshots.aggregate_type IS 'Type name of the aggregate (e.g., Order, User)';
COMMENT ON COLUMN snapshots.version IS 'Aggregate version when this snapshot was taken';
COMMENT ON COLUMN snapshots.schema_version IS 'Schema version of the aggregate state model';
COMMENT ON COLUMN snapshots.state IS 'Serialized aggregate state as JSONB';
COMMENT ON COLUMN snapshots.created_at IS 'Timestamp when this snapshot was created';
