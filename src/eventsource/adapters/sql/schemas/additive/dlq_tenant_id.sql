-- Additive fragment: owner tenant of a dead letter entry (ADR-0114).
-- Appended to the DLQ schema and the combined schema at composition time
-- (eventsource.adapters.sql.schemas.get_schema). Idempotent on PostgreSQL.
--
-- Nullable and without a default: adding it is metadata-only. Existing rows
-- read as NULL (untenanted) and are invisible under a tenant scope.
--
-- Existing PostgreSQL databases receive this column from
-- updates/007_add_dlq_tenant.sql (operator-applied, like 005).
ALTER TABLE dead_letter_queue ADD COLUMN IF NOT EXISTS tenant_id UUID;

CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id
    ON dead_letter_queue (tenant_id);

COMMENT ON COLUMN dead_letter_queue.tenant_id IS 'Owner tenant of the failed event; NULL when untenanted';
