-- Additive fragment: owner tenant of a snapshot (ADR-0113).
-- Appended to the snapshots schema at composition time
-- (eventsource.adapters.sql.schemas.get_schema). Idempotent on PostgreSQL.
--
-- Nullable and without a default: adding it is metadata-only. Existing rows
-- read as NULL (untenanted) and are ignored under a tenant scope.
--
-- Existing PostgreSQL databases receive this column from
-- updates/005_add_snapshot_tenant.sql (operator-applied, like 002).
ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS tenant_id UUID;

COMMENT ON COLUMN snapshots.tenant_id IS 'Owner tenant of the source stream; NULL when untenanted';
