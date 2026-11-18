-- =============================================================================
-- Migration: Add event_ids side table to the partitioned events schema
-- For: PostgreSQL (events_partitioned template only)
-- Version: 006
-- =============================================================================
-- REQUIRED for databases created from events_partitioned.sql before this
-- release. The partitioned events table cannot enforce global event_id
-- uniqueness on its own (PostgreSQL requires the partition key in every unique
-- constraint). The event store writes to event_ids when the table exists, so
-- this script creates it and backfills one row per existing event_id.
--
-- Do not run this on the default (non-partitioned) events schema. It does not
-- need to run there.
--
-- After running this script, restart every process that writes events. A store
-- instance checks for event_ids once, on its first append, and keeps that answer
-- for its lifetime. An instance that started earlier never writes to event_ids.
--
-- Existing duplicate event_id values (if any) collapse to one reservation. The
-- event rows themselves are not changed.
--
-- Idempotent: safe to run multiple times. Runs in one transaction.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS event_ids (
    event_id UUID PRIMARY KEY
);

INSERT INTO event_ids (event_id)
SELECT DISTINCT event_id FROM events
ON CONFLICT DO NOTHING;

COMMIT;
