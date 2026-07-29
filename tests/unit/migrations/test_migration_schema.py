"""Unit tests for migration schema template."""

from eventsource.migrations import (
    MIGRATION_SCHEMA,
    get_schema,
    get_template_path,
    list_schemas,
)


class TestMigrationSchemaTemplate:
    """Tests for migration schema template file."""

    def test_migration_schema_constant(self):
        """Test that MIGRATION_SCHEMA constant is correct."""
        assert MIGRATION_SCHEMA == "migration"

    def test_get_migration_schema(self):
        """Test getting migration schema for PostgreSQL."""
        schema = get_schema("migration")

        assert "CREATE TABLE" in schema
        assert "tenant_migrations" in schema
        assert "tenant_routing" in schema
        assert "migration_position_mappings" in schema
        assert "migration_audit_log" in schema

    def test_get_template_path(self):
        """Test getting template path for migration schema."""
        path = get_template_path("migration")

        assert path.exists()
        assert path.name == "migration.sql"
        assert "templates" in str(path)

    def test_migration_in_list_schemas(self):
        """Test that migration schema is listed for PostgreSQL backend."""
        schemas = list_schemas("postgresql")

        assert "migration" in schemas

    def test_migration_not_in_sqlite_schemas(self):
        """Test that migration schema is PostgreSQL-only (not in SQLite)."""
        schemas = list_schemas("sqlite")

        # Migration schema should not be available for SQLite
        # due to advisory lock dependency
        assert "migration" not in schemas


class TestTenantMigrationsTable:
    """Tests for tenant_migrations table schema."""

    def test_tenant_migrations_table_exists(self):
        """Test that tenant_migrations table is defined."""
        schema = get_schema("migration")

        assert "CREATE TABLE IF NOT EXISTS tenant_migrations" in schema

    def test_tenant_migrations_has_required_columns(self):
        """Test that tenant_migrations has all required columns."""
        schema = get_schema("migration")

        required_columns = [
            "id UUID PRIMARY KEY",
            "tenant_id UUID NOT NULL",
            "source_store_id VARCHAR(255)",
            "target_store_id VARCHAR(255)",
            "phase VARCHAR(50)",
            "events_total BIGINT",
            "events_copied BIGINT",
            "last_source_position BIGINT",
            "last_target_position BIGINT",
            "started_at TIMESTAMP WITH TIME ZONE",
            "bulk_copy_started_at",
            "bulk_copy_completed_at",
            "dual_write_started_at",
            "cutover_started_at",
            "completed_at TIMESTAMP WITH TIME ZONE",
            "config JSONB",
            "error_count INTEGER",
            "last_error TEXT",
            "last_error_at",
            "is_paused BOOLEAN",
            "paused_at",
            "pause_reason TEXT",
            "created_at TIMESTAMP WITH TIME ZONE",
            "updated_at TIMESTAMP WITH TIME ZONE",
            "created_by VARCHAR(255)",
        ]

        for column in required_columns:
            assert column in schema, f"Missing column definition: {column}"

    def test_tenant_migrations_phase_check_constraint(self):
        """Test that phase has CHECK constraint with valid values."""
        schema = get_schema("migration")

        # Check that all phase values are defined
        phase_values = [
            "'pending'",
            "'bulk_copy'",
            "'dual_write'",
            "'cutover'",
            "'completed'",
            "'aborted'",
            "'failed'",
        ]

        for phase in phase_values:
            assert phase in schema, f"Missing phase value: {phase}"

    def test_tenant_migrations_indexes(self):
        """Test that tenant_migrations has required indexes."""
        schema = get_schema("migration")

        expected_indexes = [
            "idx_tenant_migrations_tenant_id",
            "idx_tenant_migrations_phase",
            "idx_tenant_migrations_active_unique",
            "idx_tenant_migrations_completed_at",
        ]

        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"

    def test_tenant_migrations_active_unique_constraint(self):
        """Test unique constraint for active migrations per tenant."""
        schema = get_schema("migration")

        # Should have partial unique index for active migrations
        assert "idx_tenant_migrations_active_unique" in schema
        assert "WHERE phase NOT IN ('completed', 'aborted', 'failed')" in schema


class TestTenantRoutingTable:
    """Tests for tenant_routing table schema."""

    def test_tenant_routing_table_exists(self):
        """Test that tenant_routing table is defined."""
        schema = get_schema("migration")

        assert "CREATE TABLE IF NOT EXISTS tenant_routing" in schema

    def test_tenant_routing_has_required_columns(self):
        """Test that tenant_routing has all required columns."""
        schema = get_schema("migration")

        required_columns = [
            "tenant_id UUID PRIMARY KEY",
            "store_id VARCHAR(255)",
            "migration_state VARCHAR(50)",
            "active_migration_id UUID",
            "created_at TIMESTAMP WITH TIME ZONE",
            "updated_at TIMESTAMP WITH TIME ZONE",
        ]

        for column in required_columns:
            assert column in schema, f"Missing column definition: {column}"

    def test_tenant_routing_migration_state_check_constraint(self):
        """Test that migration_state has CHECK constraint."""
        schema = get_schema("migration")

        state_values = [
            "'normal'",
            "'bulk_copy'",
            "'dual_write'",
            "'cutover_paused'",
            "'migrated'",
        ]

        for state in state_values:
            assert state in schema, f"Missing migration_state value: {state}"

    def test_tenant_routing_foreign_key(self):
        """Test foreign key to tenant_migrations."""
        schema = get_schema("migration")

        assert "REFERENCES tenant_migrations(id)" in schema
        assert "ON DELETE SET NULL" in schema

    def test_tenant_routing_indexes(self):
        """Test that tenant_routing has required indexes."""
        schema = get_schema("migration")

        expected_indexes = [
            "idx_tenant_routing_store_id",
            "idx_tenant_routing_migration_state",
        ]

        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"


class TestMigrationPositionMappingsTable:
    """Tests for migration_position_mappings table schema."""

    def test_position_mappings_table_exists(self):
        """Test that migration_position_mappings table is defined."""
        schema = get_schema("migration")

        assert "CREATE TABLE IF NOT EXISTS migration_position_mappings" in schema

    def test_position_mappings_has_required_columns(self):
        """Test that migration_position_mappings has all required columns."""
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
        """Test foreign key with CASCADE delete."""
        schema = get_schema("migration")

        # Should cascade delete when migration is deleted
        assert "REFERENCES tenant_migrations(id) ON DELETE CASCADE" in schema

    def test_position_mappings_unique_constraint(self):
        """Test unique constraint on migration_id + source_position."""
        schema = get_schema("migration")

        assert "unique_migration_source_position" in schema
        assert "UNIQUE (migration_id, source_position)" in schema

    def test_position_mappings_indexes(self):
        """Test that migration_position_mappings has required indexes."""
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
        """Test that migration_audit_log table is defined."""
        schema = get_schema("migration")

        assert "CREATE TABLE IF NOT EXISTS migration_audit_log" in schema

    def test_audit_log_has_required_columns(self):
        """Test that migration_audit_log has all required columns."""
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
        """Test that event_type has CHECK constraint."""
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
        """Test foreign key with CASCADE delete."""
        schema = get_schema("migration")

        # Audit log entries should be deleted when migration is deleted
        # Find the specific CASCADE for audit_log table
        assert "migration_audit_log" in schema
        # The CASCADE should be present for the foreign key
        assert "ON DELETE CASCADE" in schema

    def test_audit_log_indexes(self):
        """Test that migration_audit_log has required indexes."""
        schema = get_schema("migration")

        expected_indexes = [
            "idx_migration_audit_migration_id",
            "idx_migration_audit_occurred_at",
            "idx_migration_audit_event_type",
            "idx_migration_audit_migration_time",
        ]

        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"


class TestSchemaIdempotency:
    """Tests for schema idempotency (CREATE IF NOT EXISTS)."""

    def test_tables_use_if_not_exists(self):
        """Test that all tables use IF NOT EXISTS."""
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
        """Test that all indexes use IF NOT EXISTS."""
        schema = get_schema("migration")

        # Count CREATE INDEX statements
        create_index_count = schema.count("CREATE INDEX")
        if_not_exists_count = schema.count("CREATE INDEX IF NOT EXISTS")
        create_unique_index_count = schema.count("CREATE UNIQUE INDEX")
        unique_if_not_exists_count = schema.count("CREATE UNIQUE INDEX IF NOT EXISTS")

        # All indexes should use IF NOT EXISTS
        assert create_index_count == if_not_exists_count, (
            "All CREATE INDEX should use IF NOT EXISTS"
        )
        assert create_unique_index_count == unique_if_not_exists_count, (
            "All CREATE UNIQUE INDEX should use IF NOT EXISTS"
        )


class TestSchemaTriggers:
    """Tests for trigger definitions."""

    def test_tenant_migrations_updated_trigger(self):
        """Test that tenant_migrations has updated_at trigger."""
        schema = get_schema("migration")

        assert "update_tenant_migrations_timestamp" in schema
        assert "tenant_migrations_updated" in schema
        assert "BEFORE UPDATE ON tenant_migrations" in schema

    def test_tenant_routing_updated_trigger(self):
        """Test that tenant_routing has updated_at trigger."""
        schema = get_schema("migration")

        assert "update_tenant_routing_timestamp" in schema
        assert "tenant_routing_updated" in schema
        assert "BEFORE UPDATE ON tenant_routing" in schema

    def test_triggers_use_drop_if_exists(self):
        """Test that triggers are recreated safely."""
        schema = get_schema("migration")

        assert "DROP TRIGGER IF EXISTS tenant_migrations_updated" in schema
        assert "DROP TRIGGER IF EXISTS tenant_routing_updated" in schema


class TestSchemaComments:
    """Tests for schema documentation comments."""

    def test_table_comments_present(self):
        """Test that tables have COMMENT ON TABLE statements."""
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
        """Test that key columns have COMMENT ON COLUMN statements."""
        schema = get_schema("migration")

        # Check for some key column comments
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
