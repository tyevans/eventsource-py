-- =============================================================================
-- Migration Schema Template for eventsource-py
-- =============================================================================
-- This schema supports the multi-tenant live migration system.
-- It enables migrating tenant data from a shared event store to a dedicated
-- store without service interruption using a three-phase approach:
--   1. Bulk Copy - Copying historical events
--   2. Dual-Write - Real-time sync, writing to both stores
--   3. Cutover - Brief pause, switching routing
--
-- PostgreSQL 12+ required for advisory lock support and optimal performance.
--
-- Tables:
--   - tenant_migrations: Tracks active and historical migrations
--   - tenant_routing: Stores tenant-to-store routing configuration
--   - migration_position_mappings: Maps source/target positions for checkpoints
--   - migration_audit_log: Audit trail for migration operations (compliance)
-- =============================================================================

-- =============================================================================
-- Table: tenant_migrations
-- Purpose: Tracks active and historical migrations with full state machine
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_migrations (
    -- Primary identifier for the migration
    id UUID PRIMARY KEY,

    -- The tenant being migrated
    tenant_id UUID NOT NULL,

    -- Source and target store identifiers
    source_store_id VARCHAR(255) NOT NULL,
    target_store_id VARCHAR(255) NOT NULL,

    -- Migration phase state machine
    -- Represents the current phase of the migration process
    phase VARCHAR(50) NOT NULL DEFAULT 'pending'
        CHECK (phase IN (
            'pending',      -- Migration created, not started
            'bulk_copy',    -- Copying historical events
            'dual_write',   -- Real-time sync, writing to both stores
            'cutover',      -- Brief pause, switching routing
            'completed',    -- Successfully completed
            'aborted',      -- Operator-initiated cancellation
            'failed'        -- Unrecoverable error
        )),

    -- Progress tracking for monitoring and resumption
    events_total BIGINT DEFAULT 0,
    events_copied BIGINT DEFAULT 0,
    last_source_position BIGINT DEFAULT 0,
    last_target_position BIGINT DEFAULT 0,

    -- Phase timing for performance analysis and debugging
    started_at TIMESTAMP WITH TIME ZONE,
    bulk_copy_started_at TIMESTAMP WITH TIME ZONE,
    bulk_copy_completed_at TIMESTAMP WITH TIME ZONE,
    dual_write_started_at TIMESTAMP WITH TIME ZONE,
    cutover_started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,

    -- Configuration stored as JSON for flexibility
    -- May include: batch_size, timeout settings, retry config, etc.
    config JSONB NOT NULL DEFAULT '{}',

    -- Error tracking for debugging and operational visibility
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    last_error_at TIMESTAMP WITH TIME ZONE,

    -- Pause state for operator control
    is_paused BOOLEAN DEFAULT FALSE,
    paused_at TIMESTAMP WITH TIME ZONE,
    pause_reason TEXT,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255)
);

-- Index for looking up migrations by tenant
CREATE INDEX IF NOT EXISTS idx_tenant_migrations_tenant_id
    ON tenant_migrations(tenant_id);

-- Index for listing active migrations (partial index for efficiency)
CREATE INDEX IF NOT EXISTS idx_tenant_migrations_phase
    ON tenant_migrations(phase)
    WHERE phase NOT IN ('completed', 'aborted', 'failed');

-- Unique constraint: only one active migration per tenant
-- Prevents starting a new migration while one is already in progress
CREATE UNIQUE INDEX IF NOT EXISTS idx_tenant_migrations_active_unique
    ON tenant_migrations(tenant_id)
    WHERE phase NOT IN ('completed', 'aborted', 'failed');

-- Index for completed migrations (for historical queries)
CREATE INDEX IF NOT EXISTS idx_tenant_migrations_completed_at
    ON tenant_migrations(completed_at)
    WHERE completed_at IS NOT NULL;

-- =============================================================================
-- Table: tenant_routing
-- Purpose: Stores tenant-to-store routing configuration for the store router
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_routing (
    -- Tenant identifier (one row per tenant)
    tenant_id UUID PRIMARY KEY,

    -- The store this tenant is routed to
    store_id VARCHAR(255) NOT NULL,

    -- Migration state for routing decisions
    -- Determines how reads/writes should be handled during migration
    migration_state VARCHAR(50) NOT NULL DEFAULT 'normal'
        CHECK (migration_state IN (
            'normal',           -- Route to configured store (no migration)
            'bulk_copy',        -- Route reads to source, writes to source
            'dual_write',       -- Route writes through interceptor (both stores)
            'cutover_paused',   -- Block writes, await cutover completion
            'migrated'          -- Route to new store (migration complete)
        )),

    -- Reference to active migration (if any)
    -- NULL when no migration is active
    active_migration_id UUID REFERENCES tenant_migrations(id) ON DELETE SET NULL,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for store_id lookups (finding all tenants on a store)
CREATE INDEX IF NOT EXISTS idx_tenant_routing_store_id
    ON tenant_routing(store_id);

-- Index for finding tenants by migration state
CREATE INDEX IF NOT EXISTS idx_tenant_routing_migration_state
    ON tenant_routing(migration_state)
    WHERE migration_state != 'normal';

-- =============================================================================
-- Table: migration_position_mappings
-- Purpose: Maps source positions to target positions for checkpoint translation
-- =============================================================================

CREATE TABLE IF NOT EXISTS migration_position_mappings (
    -- Surrogate primary key
    id BIGSERIAL PRIMARY KEY,

    -- The migration this mapping belongs to
    migration_id UUID NOT NULL REFERENCES tenant_migrations(id) ON DELETE CASCADE,

    -- Position mapping: source -> target
    source_position BIGINT NOT NULL,
    target_position BIGINT NOT NULL,

    -- Event ID for correlation and debugging
    event_id UUID NOT NULL,

    -- When this mapping was recorded
    mapped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Unique constraint on source position per migration
    -- Each source position maps to exactly one target position
    CONSTRAINT unique_migration_source_position
        UNIQUE (migration_id, source_position)
);

-- Index for efficient position lookups during checkpoint translation
CREATE INDEX IF NOT EXISTS idx_position_mappings_lookup
    ON migration_position_mappings(migration_id, source_position);

-- Index for finding nearest position (for checkpoint translation)
-- DESC ordering enables efficient "find position <= X" queries
CREATE INDEX IF NOT EXISTS idx_position_mappings_nearest
    ON migration_position_mappings(migration_id, source_position DESC);

-- Index for event_id lookups (debugging and verification)
CREATE INDEX IF NOT EXISTS idx_position_mappings_event_id
    ON migration_position_mappings(event_id);

-- =============================================================================
-- Table: migration_audit_log
-- Purpose: Audit trail for migration operations (compliance requirement)
-- =============================================================================

CREATE TABLE IF NOT EXISTS migration_audit_log (
    -- Surrogate primary key
    id BIGSERIAL PRIMARY KEY,

    -- The migration this log entry belongs to
    migration_id UUID NOT NULL REFERENCES tenant_migrations(id) ON DELETE CASCADE,

    -- Event classification
    event_type VARCHAR(100) NOT NULL
        CHECK (event_type IN (
            'migration_started',
            'phase_changed',
            'migration_paused',
            'migration_resumed',
            'migration_aborted',
            'migration_completed',
            'migration_failed',
            'error_occurred',
            'cutover_initiated',
            'cutover_completed',
            'cutover_rolled_back',
            'verification_started',
            'verification_completed',
            'verification_failed',
            'progress_checkpoint'
        )),

    -- State change tracking
    old_phase VARCHAR(50),
    new_phase VARCHAR(50),

    -- Additional context (structured data for each event type)
    details JSONB,

    -- Who performed the action (system or operator ID)
    operator VARCHAR(255),

    -- When it happened
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Index for querying audit log by migration
CREATE INDEX IF NOT EXISTS idx_migration_audit_migration_id
    ON migration_audit_log(migration_id);

-- Index for time-based queries (compliance reporting)
CREATE INDEX IF NOT EXISTS idx_migration_audit_occurred_at
    ON migration_audit_log(occurred_at);

-- Index for event type filtering
CREATE INDEX IF NOT EXISTS idx_migration_audit_event_type
    ON migration_audit_log(event_type);

-- Composite index for filtered time queries by migration
CREATE INDEX IF NOT EXISTS idx_migration_audit_migration_time
    ON migration_audit_log(migration_id, occurred_at);

-- =============================================================================
-- Trigger Functions for automatic timestamp updates
-- =============================================================================

-- Function to update tenant_migrations.updated_at
CREATE OR REPLACE FUNCTION update_tenant_migrations_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to update tenant_routing.updated_at
CREATE OR REPLACE FUNCTION update_tenant_routing_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Triggers
-- =============================================================================

-- Trigger for tenant_migrations updated_at
DROP TRIGGER IF EXISTS tenant_migrations_updated ON tenant_migrations;
CREATE TRIGGER tenant_migrations_updated
    BEFORE UPDATE ON tenant_migrations
    FOR EACH ROW EXECUTE FUNCTION update_tenant_migrations_timestamp();

-- Trigger for tenant_routing updated_at
DROP TRIGGER IF EXISTS tenant_routing_updated ON tenant_routing;
CREATE TRIGGER tenant_routing_updated
    BEFORE UPDATE ON tenant_routing
    FOR EACH ROW EXECUTE FUNCTION update_tenant_routing_timestamp();

-- =============================================================================
-- Table and Column Comments
-- =============================================================================

COMMENT ON TABLE tenant_migrations IS 'Tracks active and historical tenant migrations between event stores';
COMMENT ON COLUMN tenant_migrations.id IS 'Unique identifier for the migration';
COMMENT ON COLUMN tenant_migrations.tenant_id IS 'The tenant being migrated';
COMMENT ON COLUMN tenant_migrations.source_store_id IS 'Identifier of the source event store';
COMMENT ON COLUMN tenant_migrations.target_store_id IS 'Identifier of the target event store';
COMMENT ON COLUMN tenant_migrations.phase IS 'Current phase of the migration state machine';
COMMENT ON COLUMN tenant_migrations.events_total IS 'Total number of events to migrate';
COMMENT ON COLUMN tenant_migrations.events_copied IS 'Number of events copied so far';
COMMENT ON COLUMN tenant_migrations.last_source_position IS 'Last processed position in source store';
COMMENT ON COLUMN tenant_migrations.last_target_position IS 'Last written position in target store';
COMMENT ON COLUMN tenant_migrations.config IS 'Migration configuration as JSON';
COMMENT ON COLUMN tenant_migrations.is_paused IS 'Whether the migration is currently paused';

COMMENT ON TABLE tenant_routing IS 'Tenant-to-store routing configuration for the TenantStoreRouter';
COMMENT ON COLUMN tenant_routing.tenant_id IS 'Tenant identifier (primary key)';
COMMENT ON COLUMN tenant_routing.store_id IS 'Store this tenant is currently routed to';
COMMENT ON COLUMN tenant_routing.migration_state IS 'Migration state affecting routing behavior';
COMMENT ON COLUMN tenant_routing.active_migration_id IS 'Reference to active migration if any';

COMMENT ON TABLE migration_position_mappings IS 'Maps source positions to target positions for checkpoint translation';
COMMENT ON COLUMN migration_position_mappings.migration_id IS 'Migration this mapping belongs to';
COMMENT ON COLUMN migration_position_mappings.source_position IS 'Position in the source event store';
COMMENT ON COLUMN migration_position_mappings.target_position IS 'Corresponding position in target store';
COMMENT ON COLUMN migration_position_mappings.event_id IS 'Event ID for correlation';

COMMENT ON TABLE migration_audit_log IS 'Audit trail for migration operations (compliance)';
COMMENT ON COLUMN migration_audit_log.migration_id IS 'Migration this audit entry belongs to';
COMMENT ON COLUMN migration_audit_log.event_type IS 'Type of audit event';
COMMENT ON COLUMN migration_audit_log.old_phase IS 'Previous phase (for phase_changed events)';
COMMENT ON COLUMN migration_audit_log.new_phase IS 'New phase (for phase_changed events)';
COMMENT ON COLUMN migration_audit_log.details IS 'Additional structured context';
COMMENT ON COLUMN migration_audit_log.operator IS 'Who initiated the action';
