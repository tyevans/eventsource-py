-- =============================================================================
-- Migration: Add txid to events
-- For: PostgreSQL 13+
-- Version: 004
-- =============================================================================
-- REQUIRED before upgrading: the global feed read path
-- (`read_all` / `current_position` in the PostgreSQL adapter) now
-- references `events.txid`, and fails loudly without it.
--
-- Replaces the old `xmin::text::bigint < pg_snapshot_xmin(...)::text::bigint`
-- safe-horizon predicate, which silently became universally true -- losing
-- the no-skip guarantee -- once a cluster crossed its first xid epoch,
-- because `xmin` is a 32-bit xid and `pg_snapshot_xmin` returns a
-- 64-bit epoch-extended `xid8`.
--
-- Two statements, deliberately. `ADD COLUMN` with a volatile DEFAULT in a
-- single statement forces a full rewrite of the events table -- unacceptable
-- on a production event store. Adding the column nullable and without a
-- default is a metadata-only catalog change; the `SET DEFAULT` that follows
-- applies to future inserts only and rewrites nothing.
--
-- Rows left with a NULL txid predate this migration and are always safe to
-- read: `ALTER TABLE` takes ACCESS EXCLUSIVE, so every transaction that
-- inserted one had already finished before any post-migration snapshot.
-- No backfill, and deliberately no NOT NULL.
--
-- This is an idempotent migration - safe to run multiple times.
-- =============================================================================

ALTER TABLE events ADD COLUMN IF NOT EXISTS txid xid8;

ALTER TABLE events ALTER COLUMN txid SET DEFAULT pg_current_xact_id();

COMMENT ON COLUMN events.txid IS 'Inserting transaction id (xid8) for the wraparound-safe global feed horizon; NULL for rows predating this migration';

-- Wraps the snapshot-xmin lookup behind a stable name so the adapter's
-- Python source never has to spell out the underlying system-column-era
-- function name; `CREATE OR REPLACE` keeps this idempotent.
CREATE OR REPLACE FUNCTION eventsource_feed_horizon() RETURNS text
LANGUAGE sql STABLE AS
$$
    SELECT pg_snapshot_xmin(pg_current_snapshot())::text;
$$;
