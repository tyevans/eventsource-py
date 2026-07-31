-- Additive fragment: opaque position tokens for migration bookkeeping.
-- Appended to the migration schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
--
-- The legacy BIGINT position columns are left in place and are neither
-- written nor read by the library after slice (c); they die with their
-- own schema revision, not this one.
ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS source_position_token TEXT;

ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS target_position_token TEXT;

-- New inserts carry only tokens; the legacy NOT NULL on the int columns
-- would otherwise force a fabricated position value.
ALTER TABLE migration_position_mappings
ALTER COLUMN source_position DROP NOT NULL;

ALTER TABLE migration_position_mappings
ALTER COLUMN target_position DROP NOT NULL;

-- Each source position maps to exactly one target position, as the legacy
-- UNIQUE (migration_id, source_position) constraint expressed for tokens.
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token)
WHERE source_position_token IS NOT NULL;

-- Ordering for checkpoint translation is by surrogate id (mappings are
-- recorded in ascending source-position order); these indexes serve the
-- exact-token lookups.
CREATE INDEX IF NOT EXISTS idx_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token);

CREATE INDEX IF NOT EXISTS idx_position_mappings_target_token
ON migration_position_mappings (migration_id, target_position_token);

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_source_position_token TEXT;

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_target_position_token TEXT;
