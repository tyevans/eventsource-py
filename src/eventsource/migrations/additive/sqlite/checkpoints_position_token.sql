-- Additive fragment: opaque position token for subscription checkpoints.
-- SQLite has no ADD COLUMN IF NOT EXISTS: this fragment is safe only
-- against a table that does not already have the column. Callers that
-- may re-apply a schema to an existing database must guard with
-- PRAGMA table_info (see adapters/sqlite/store.py).
ALTER TABLE projection_checkpoints ADD COLUMN position_token TEXT;
