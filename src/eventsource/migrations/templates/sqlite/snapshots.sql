-- =============================================================================
-- Snapshots Table Schema (SQLite)
-- =============================================================================
-- Snapshots table for aggregate state caching.
-- Purpose: Store point-in-time aggregate state for fast loading.
-- Note: One snapshot per aggregate (latest only); upsert on save.
--
-- SQLite-specific adaptations:
--   - UUID stored as TEXT (36 characters, hyphenated format)
--   - Timestamps stored as TEXT in ISO 8601 format
--   - JSON stored as TEXT (no native JSONB support)
--   - Auto-increment uses INTEGER PRIMARY KEY AUTOINCREMENT
--
-- SQLite 3.8+ required for partial index support.
-- =============================================================================

-- Snapshots Table
-- ================================
-- This table stores aggregate state snapshots for performance optimization.
-- Instead of replaying all events, aggregates can be restored from a snapshot
-- and only replay events since the snapshot was taken.

CREATE TABLE IF NOT EXISTS snapshots (
    -- Surrogate key for internal references
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Aggregate identification
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,

    -- Snapshot versioning
    -- Aggregate version when this snapshot was taken
    version INTEGER NOT NULL,
    -- State schema version for evolution
    schema_version INTEGER NOT NULL DEFAULT 1,

    -- Serialized state (JSON stored as TEXT in SQLite)
    state TEXT NOT NULL,

    -- Metadata (ISO 8601 format)
    created_at TEXT NOT NULL,

    -- Constraints
    -- Ensure one snapshot per aggregate (keyed by id + type)
    UNIQUE (aggregate_id, aggregate_type)
);

-- =============================================================================
-- Indexes
-- =============================================================================
-- These indexes optimize common query patterns for snapshot operations.

-- Index for loading snapshots by aggregate (covered by unique constraint but explicit)
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_lookup
    ON snapshots(aggregate_id, aggregate_type);

-- Index for bulk operations by aggregate type
CREATE INDEX IF NOT EXISTS idx_snapshots_aggregate_type
    ON snapshots(aggregate_type);

-- Index for schema version queries (useful for bulk invalidation)
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version
    ON snapshots(aggregate_type, schema_version);

-- Index for cleanup of old snapshots by date
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at
    ON snapshots(created_at);
