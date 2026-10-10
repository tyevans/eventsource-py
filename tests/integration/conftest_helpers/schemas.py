"""DDL schema statements for integration test database tables."""

from __future__ import annotations

CHECKPOINTS_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name VARCHAR(255) PRIMARY KEY,
        last_event_id UUID,
        last_event_type VARCHAR(255),
        last_processed_at TIMESTAMPTZ,
        events_processed BIGINT DEFAULT 0,
        global_position BIGINT,
        position_token TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
]

DLQ_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dead_letter_queue (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL,
        projection_name VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        event_data JSONB NOT NULL,
        error_message TEXT NOT NULL,
        error_stacktrace TEXT,
        retry_count INTEGER DEFAULT 0,
        first_failed_at TIMESTAMPTZ NOT NULL,
        last_failed_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(50) DEFAULT 'failed',
        resolved_at TIMESTAMPTZ,
        resolved_by VARCHAR(255),
        UNIQUE(event_id, projection_name)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dlq_status
    ON dead_letter_queue (status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dlq_projection
    ON dead_letter_queue (projection_name)
    """,
]

OUTBOX_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS event_outbox (
        id UUID PRIMARY KEY,
        event_id UUID NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        aggregate_id UUID NOT NULL,
        aggregate_type VARCHAR(255) NOT NULL,
        tenant_id UUID,
        event_data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(50) DEFAULT 'pending',
        published_at TIMESTAMPTZ,
        retry_count INTEGER DEFAULT 0,
        last_error TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_outbox_status
    ON event_outbox (status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_outbox_created
    ON event_outbox (created_at) WHERE status = 'pending'
    """,
]
