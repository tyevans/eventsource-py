"""Unit tests for migration schema template, tenant_migrations, and tenant_routing tables."""

from eventsource.adapters.sql.schemas import (
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


__all__ = [
    "TestMigrationSchemaTemplate",
    "TestTenantMigrationsTable",
    "TestTenantRoutingTable",
]
