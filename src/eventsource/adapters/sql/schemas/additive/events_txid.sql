-- Additive fragment: wraparound-safe transaction id for the global feed
-- horizon. Appended to the events schema at composition time
-- (eventsource.adapters.sql.schemas.get_schema). Idempotent on PostgreSQL.
--
-- The feed's no-skip guard defers rows whose inserting transaction is not
-- yet definitely-committed. It used to read the `xmin` system column, a
-- 32-bit xid, and compare it against `pg_snapshot_xmin(...)`, an
-- epoch-extended 64-bit `xid8` -- a comparison that becomes universally
-- true once the cluster crosses its first xid epoch. `xid8` does not wrap
-- on any human timescale, so the column makes the guard permanent.
--
-- Two statements, deliberately: `ADD COLUMN` carrying a volatile DEFAULT
-- forces a full table rewrite. Adding the column nullable and without a
-- default is metadata-only; `SET DEFAULT` afterwards applies to future
-- inserts only.
--
-- NULL semantics: a NULL `txid` row predates this ALTER. `ALTER TABLE`
-- takes ACCESS EXCLUSIVE, so any transaction that inserted such a row
-- finished before every post-migration snapshot -- NULL rows are always
-- definitely-committed and always safe to read.
--
-- Requires PostgreSQL 13 (`xid8`, `pg_current_xact_id`) -- the same floor
-- `pg_current_snapshot()` already imposed.
ALTER TABLE events ADD COLUMN IF NOT EXISTS txid xid8;

ALTER TABLE events ALTER COLUMN txid SET DEFAULT pg_current_xact_id();

-- Wraps the snapshot-xmin lookup behind a stable name so the adapter's
-- Python source never has to spell out the underlying system-column-era
-- function name; `CREATE OR REPLACE` keeps this idempotent.
CREATE OR REPLACE FUNCTION eventsource_feed_horizon() RETURNS text
LANGUAGE sql STABLE AS
$$
    SELECT pg_snapshot_xmin(pg_current_snapshot())::text;
$$;
