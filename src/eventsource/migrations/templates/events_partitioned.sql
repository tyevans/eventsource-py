-- =============================================================================
-- Event Store Table Schema (Partitioned Version)
-- =============================================================================
-- This schema provides a partitioned events table for high-volume deployments.
-- Uses PostgreSQL native range partitioning by timestamp for:
--   - Improved query performance on time-based queries
--   - Efficient data lifecycle management (archival, deletion)
--   - Better I/O distribution across partition files
--
-- Prerequisites:
--   - PostgreSQL 12+ (native partitioning)
--   - Optional: pg_partman extension for automatic partition management
--   - Optional: pg_cron extension for scheduled maintenance
--
-- When to use partitioning:
--   - Expected event volume > 10M events/month
--   - Need to efficiently archive or purge old events
--   - Time-range queries are common (projections, analytics)
-- =============================================================================

-- Events Table (Partitioned by timestamp)
-- ========================================
-- Note: In partitioned tables, the partition key MUST be included in:
--   1. The primary key
--   2. All unique constraints
-- This is a PostgreSQL requirement for partition pruning.

-- Sequence for global ordering (partitioned tables can't use BIGSERIAL)
CREATE SEQUENCE IF NOT EXISTS events_global_position_seq;

CREATE TABLE IF NOT EXISTS events (
    -- Global position for ordered replay across all streams
    global_position BIGINT NOT NULL DEFAULT nextval('events_global_position_seq'),

    -- Unique event identifier
    event_id UUID NOT NULL,

    -- Aggregate identification
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Event metadata
    event_type VARCHAR(255) NOT NULL,

    -- Multi-tenancy support (optional)
    tenant_id UUID,

    -- Actor/user who triggered the event
    actor_id VARCHAR(255),

    -- Optimistic concurrency control
    version INTEGER NOT NULL,

    -- Event timestamp (partition key)
    -- This determines which partition the event is stored in
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Event payload
    payload JSONB NOT NULL,

    -- Technical metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Primary key MUST include partition key
    PRIMARY KEY (global_position, timestamp),

    -- Unique constraint on event_id MUST include partition key
    CONSTRAINT uq_events_event_id_partitioned UNIQUE (event_id, timestamp),

    -- Unique constraint MUST include partition key
    -- This ensures aggregate version uniqueness within partition boundaries
    CONSTRAINT uq_events_aggregate_version_partitioned
        UNIQUE (aggregate_id, aggregate_type, version, timestamp)
) PARTITION BY RANGE (timestamp);

-- Make sequence owned by table
ALTER SEQUENCE events_global_position_seq OWNED BY events.global_position;

-- =============================================================================
-- Indexes (created on parent, inherited by partitions)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_events_aggregate_id
    ON events (aggregate_id);

CREATE INDEX IF NOT EXISTS idx_events_aggregate_type
    ON events (aggregate_type);

CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type);

CREATE INDEX IF NOT EXISTS idx_events_tenant_id
    ON events (tenant_id)
    WHERE tenant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp);

-- Composite index for projection queries
CREATE INDEX IF NOT EXISTS idx_events_type_tenant_timestamp
    ON events (aggregate_type, tenant_id, timestamp);

-- Index for aggregate stream loading
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version);

-- =============================================================================
-- Partition Creation Examples
-- =============================================================================
-- Choose ONE of the following approaches:

-- OPTION 1: Manual Monthly Partitions
-- ------------------------------------
-- Create partitions manually for each month.
-- Good for development/testing or when pg_partman is not available.

-- Example: Create partitions for 2025
/*
CREATE TABLE events_y2025m01 PARTITION OF events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE events_y2025m02 PARTITION OF events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE events_y2025m03 PARTITION OF events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE events_y2025m04 PARTITION OF events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE events_y2025m05 PARTITION OF events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE events_y2025m06 PARTITION OF events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE events_y2025m07 PARTITION OF events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE events_y2025m08 PARTITION OF events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE events_y2025m09 PARTITION OF events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE events_y2025m10 PARTITION OF events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE events_y2025m11 PARTITION OF events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE events_y2025m12 PARTITION OF events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
*/

-- OPTION 2: Default Partition (Catch-all)
-- ----------------------------------------
-- Creates a default partition for events that don't match any range.
-- Recommended to prevent INSERT failures.

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

-- OPTION 3: Automatic Partitioning with pg_partman
-- -------------------------------------------------
-- Recommended for production. Requires pg_partman extension.
-- Uncomment to use:

/*
-- Enable pg_partman (run as superuser)
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- Configure automatic partition management
SELECT partman.create_parent(
    p_parent_table => 'public.events'::text,
    p_control => 'timestamp'::text,
    p_interval => '1 month'::text,
    p_type => 'range'::text,
    p_premake => 6,  -- Create 6 months of future partitions
    p_automatic_maintenance => 'on'::text
);

-- Configure retention and infinite creation
UPDATE partman.part_config
SET
    infinite_time_partitions = true,
    retention = '24 months'::text,    -- Detach partitions older than 24 months
    retention_keep_table = true,       -- Keep detached tables (for archival)
    retention_keep_index = true
WHERE parent_table = 'public.events';

-- Run initial maintenance
SELECT partman.run_maintenance();
*/

-- OPTION 4: Scheduled Maintenance with pg_cron
-- ---------------------------------------------
-- Runs partition maintenance daily. Requires pg_cron extension.
-- Uncomment to use:

/*
-- Enable pg_cron (run as superuser)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule daily maintenance at 2 AM UTC
SELECT cron.schedule(
    'events-partition-maintenance',
    '0 2 * * *',
    $$CALL partman.run_maintenance_proc()$$
);
*/

-- =============================================================================
-- Helper Functions
-- =============================================================================

-- Function to create a partition for a specific month
CREATE OR REPLACE FUNCTION create_events_partition(
    year INTEGER,
    month INTEGER
) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    partition_name := format('events_y%sm%s', year, lpad(month::text, 2, '0'));
    start_date := make_date(year, month, 1);
    end_date := start_date + interval '1 month';

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF events
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_date,
        end_date
    );

    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to create partitions for a range of months
CREATE OR REPLACE FUNCTION create_events_partitions_for_year(
    target_year INTEGER
) RETURNS SETOF TEXT AS $$
DECLARE
    m INTEGER;
BEGIN
    FOR m IN 1..12 LOOP
        RETURN NEXT create_events_partition(target_year, m);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON TABLE events IS 'Partitioned event store for high-volume deployments';
COMMENT ON FUNCTION create_events_partition(INTEGER, INTEGER) IS 'Creates a monthly partition for the events table';
COMMENT ON FUNCTION create_events_partitions_for_year(INTEGER) IS 'Creates all monthly partitions for a given year';
