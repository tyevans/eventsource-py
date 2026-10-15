"""Unit tests for position mappings, audit log, token columns, idempotency, triggers, and comments."""

from eventsource.adapters.sql.schemas import get_schema


class TestMigrationPositionMappingsTable:
    """Tests for migration_position_mappings table schema."""

    def test_position_mappings_table_exists(self):
        schema = get_schema("migration")
        assert "CREATE TABLE IF NOT EXISTS migration_position_mappings" in schema

    def test_position_mappings_has_required_columns(self):
        schema = get_schema("migration")
        required_columns = [
            "id BIGSERIAL PRIMARY KEY",
            "migration_id UUID NOT NULL",
            "source_position BIGINT NOT NULL",
            "target_position BIGINT NOT NULL",
            "event_id UUID NOT NULL",
            "mapped_at TIMESTAMP WITH TIME ZONE",
        ]
        for column in required_columns:
            assert column in schema, f"Missing column definition: {column}"

    def test_position_mappings_foreign_key_cascade(self):
        schema = get_schema("migration")
        assert "REFERENCES tenant_migrations(id) ON DELETE CASCADE" in schema

    def test_position_mappings_unique_constraint(self):
        schema = get_schema("migration")
        assert "unique_migration_source_position" in schema
        assert "UNIQUE (migration_id, source_position)" in schema

    def test_position_mappings_indexes(self):
        schema = get_schema("migration")
        expected_indexes = [
            "idx_position_mappings_lookup",
            "idx_position_mappings_nearest",
            "idx_position_mappings_event_id",
        ]
        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"


class TestMigrationAuditLogTable:
    """Tests for migration_audit_log table schema."""

    def test_audit_log_table_exists(self):
        schema = get_schema("migration")
        assert "CREATE TABLE IF NOT EXISTS migration_audit_log" in schema

    def test_audit_log_has_required_columns(self):
        schema = get_schema("migration")
        required_columns = [
            "id BIGSERIAL PRIMARY KEY",
            "migration_id UUID NOT NULL",
            "event_type VARCHAR(100)",
            "old_phase VARCHAR(50)",
            "new_phase VARCHAR(50)",
            "details JSONB",
            "operator VARCHAR(255)",
            "occurred_at TIMESTAMP WITH TIME ZONE",
        ]
        for column in required_columns:
            assert column in schema, f"Missing column definition: {column}"

    def test_audit_log_event_type_check_constraint(self):
        schema = get_schema("migration")
        event_types = [
            "'migration_started'",
            "'phase_changed'",
            "'migration_paused'",
            "'migration_resumed'",
            "'migration_aborted'",
            "'migration_completed'",
            "'migration_failed'",
            "'error_occurred'",
            "'cutover_initiated'",
            "'cutover_completed'",
            "'cutover_rolled_back'",
            "'verification_started'",
            "'verification_completed'",
            "'verification_failed'",
            "'progress_checkpoint'",
        ]
        for event_type in event_types:
            assert event_type in schema, f"Missing event_type value: {event_type}"

    def test_audit_log_foreign_key_cascade(self):
        schema = get_schema("migration")
        assert "migration_audit_log" in schema
        assert "ON DELETE CASCADE" in schema

    def test_audit_log_indexes(self):
        schema = get_schema("migration")
        expected_indexes = [
            "idx_migration_audit_migration_id",
            "idx_migration_audit_occurred_at",
            "idx_migration_audit_event_type",
            "idx_migration_audit_migration_time",
        ]
        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"


class TestMigrationPositionTokenColumns:
    """Tests for the additive position-token columns (spec §11 risk 4)."""

    def test_position_mappings_has_token_columns(self):
        schema = get_schema("migration")
        assert "source_position_token" in schema
        assert "target_position_token" in schema

    def test_tenant_migrations_has_token_columns(self):
        schema = get_schema("migration")
        assert "last_source_position_token" in schema
        assert "last_target_position_token" in schema


class TestSchemaIdempotency:
    """Tests for schema idempotency (CREATE IF NOT EXISTS)."""

    def test_tables_use_if_not_exists(self):
        schema = get_schema("migration")
        tables = [
            "tenant_migrations",
            "tenant_routing",
            "migration_position_mappings",
            "migration_audit_log",
        ]
        for table in tables:
            assert f"CREATE TABLE IF NOT EXISTS {table}" in schema, (
                f"Table {table} should use IF NOT EXISTS"
            )

    def test_indexes_use_if_not_exists(self):
        schema = get_schema("migration")
        create_index_count = schema.count("CREATE INDEX")
        if_not_exists_count = schema.count("CREATE INDEX IF NOT EXISTS")
        create_unique_index_count = schema.count("CREATE UNIQUE INDEX")
        unique_if_not_exists_count = schema.count("CREATE UNIQUE INDEX IF NOT EXISTS")
        assert create_index_count == if_not_exists_count, (
            "All CREATE INDEX should use IF NOT EXISTS"
        )
        assert create_unique_index_count == unique_if_not_exists_count, (
            "All CREATE UNIQUE INDEX should use IF NOT EXISTS"
        )


class TestSchemaTriggers:
    """Tests for trigger definitions."""

    def test_tenant_migrations_updated_trigger(self):
        schema = get_schema("migration")
        assert "update_tenant_migrations_timestamp" in schema
        assert "tenant_migrations_updated" in schema
        assert "BEFORE UPDATE ON tenant_migrations" in schema

    def test_tenant_routing_updated_trigger(self):
        schema = get_schema("migration")
        assert "update_tenant_routing_timestamp" in schema
        assert "tenant_routing_updated" in schema
        assert "BEFORE UPDATE ON tenant_routing" in schema

    def test_triggers_use_drop_if_exists(self):
        schema = get_schema("migration")
        assert "DROP TRIGGER IF EXISTS tenant_migrations_updated" in schema
        assert "DROP TRIGGER IF EXISTS tenant_routing_updated" in schema


class TestSchemaComments:
    """Tests for schema documentation comments."""

    def test_table_comments_present(self):
        schema = get_schema("migration")
        tables = [
            "tenant_migrations",
            "tenant_routing",
            "migration_position_mappings",
            "migration_audit_log",
        ]
        for table in tables:
            assert f"COMMENT ON TABLE {table}" in schema, f"Missing COMMENT ON TABLE for {table}"

    def test_column_comments_present(self):
        schema = get_schema("migration")
        key_columns = [
            "tenant_migrations.id",
            "tenant_migrations.tenant_id",
            "tenant_migrations.phase",
            "tenant_routing.tenant_id",
            "tenant_routing.store_id",
            "migration_position_mappings.source_position",
            "migration_position_mappings.target_position",
            "migration_audit_log.event_type",
        ]
        for column in key_columns:
            assert f"COMMENT ON COLUMN {column}" in schema, (
                f"Missing COMMENT ON COLUMN for {column}"
            )


__all__ = [
    "TestMigrationAuditLogTable",
    "TestMigrationPositionMappingsTable",
    "TestMigrationPositionTokenColumns",
    "TestSchemaComments",
    "TestSchemaIdempotency",
    "TestSchemaTriggers",
]
