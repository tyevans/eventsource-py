-- Additive fragment: opaque position token for subscription checkpoints.
-- Appended to the checkpoints schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
--
-- The legacy projection_checkpoints.global_position BIGINT column is left
-- in place and is neither written nor read by the library after slice (b);
-- it dies with its own schema revision, not this one. It is not dropped
-- here: dropping a column is destructive, schemas/checkpoints.sql is under
-- the Do Not Modify rule, and this additive-fragment mechanism exists to
-- add columns, not remove them.
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS position_token TEXT;

CREATE INDEX IF NOT EXISTS idx_checkpoints_position_token
ON projection_checkpoints (position_token)
WHERE position_token IS NOT NULL;
