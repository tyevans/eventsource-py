-- Additive fragment: opaque position token for subscription checkpoints.
-- Appended to the checkpoints schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS position_token TEXT;

CREATE INDEX IF NOT EXISTS idx_checkpoints_position_token
ON projection_checkpoints (position_token)
WHERE position_token IS NOT NULL;
