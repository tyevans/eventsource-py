"""
Integration tests for migration schema on PostgreSQL.

These tests verify the schema can be created and used correctly
on a real PostgreSQL database using testcontainers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.migrations import get_schema

from ..conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


@pytest.fixture
async def migration_schema_engine(postgres_engine: AsyncEngine) -> AsyncEngine:
    """
    Provide PostgreSQL engine with migration schema created.

    Creates the migration schema tables and cleans them up after test.
    """
    schema_sql = get_schema("migration")

    # Use raw asyncpg connection to execute multi-statement SQL script
    # SQLAlchemy's exec_driver_sql doesn't support multiple statements
    async with postgres_engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        await raw_conn.driver_connection.execute(schema_sql)

    yield postgres_engine

    # Cleanup - drop tables in reverse order (due to foreign keys)
    async with postgres_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS migration_audit_log CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS migration_position_mappings CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS tenant_routing CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS tenant_migrations CASCADE"))
        # Clean up functions
        await conn.execute(
            text("DROP FUNCTION IF EXISTS update_tenant_migrations_timestamp CASCADE")
        )
        await conn.execute(text("DROP FUNCTION IF EXISTS update_tenant_routing_timestamp CASCADE"))


class TestMigrationSchemaCreation:
    """Tests for schema creation on PostgreSQL."""

    async def test_schema_creates_all_tables(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that all required tables are created."""
        async with migration_schema_engine.begin() as conn:
            result = await conn.execute(
                text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN (
                    'tenant_migrations',
                    'tenant_routing',
                    'migration_position_mappings',
                    'migration_audit_log'
                )
                ORDER BY table_name
            """)
            )
            tables = [row[0] for row in result.fetchall()]

        assert "migration_audit_log" in tables
        assert "migration_position_mappings" in tables
        assert "tenant_migrations" in tables
        assert "tenant_routing" in tables

    async def test_schema_is_idempotent(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that schema can be applied multiple times safely."""
        # Get the migration schema SQL
        schema_sql = get_schema("migration")

        # Apply schema again - should not raise (uses IF NOT EXISTS)
        async with migration_schema_engine.begin() as conn:
            raw_conn = await conn.get_raw_connection()
            await raw_conn.driver_connection.execute(schema_sql)

        # Verify tables still exist
        async with migration_schema_engine.begin() as conn:
            result = await conn.execute(
                text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN (
                    'tenant_migrations',
                    'tenant_routing',
                    'migration_position_mappings',
                    'migration_audit_log'
                )
            """)
            )
            count = result.scalar()

        assert count == 4


class TestTenantMigrationsTable:
    """Tests for tenant_migrations table operations."""

    async def test_insert_migration(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test inserting a migration record."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, :source_store_id, :target_store_id, :phase
                )
            """),
                {
                    "id": migration_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "shared-store",
                    "target_store_id": "dedicated-store",
                    "phase": "pending",
                },
            )

            result = await conn.execute(
                text("SELECT id, tenant_id, phase FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == migration_id
        assert row[1] == tenant_id
        assert row[2] == "pending"

    async def test_phase_check_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that invalid phase values are rejected."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    text("""
                    INSERT INTO tenant_migrations (
                        id, tenant_id, source_store_id, target_store_id, phase
                    ) VALUES (
                        :id, :tenant_id, 'source', 'target', 'invalid_phase'
                    )
                """),
                    {"id": migration_id, "tenant_id": tenant_id},
                )

            assert (
                "check" in str(exc_info.value).lower() or "violates" in str(exc_info.value).lower()
            )

    async def test_active_migration_unique_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that only one active migration per tenant is allowed."""
        tenant_id = uuid4()
        migration1_id = uuid4()
        migration2_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Insert first active migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration1_id, "tenant_id": tenant_id},
            )

            # Try to insert second active migration for same tenant
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    text("""
                    INSERT INTO tenant_migrations (
                        id, tenant_id, source_store_id, target_store_id, phase
                    ) VALUES (
                        :id, :tenant_id, 'source2', 'target2', 'pending'
                    )
                """),
                    {"id": migration2_id, "tenant_id": tenant_id},
                )

            # Should fail due to unique constraint
            assert (
                "unique" in str(exc_info.value).lower()
                or "duplicate" in str(exc_info.value).lower()
            )

    async def test_completed_migration_allows_new_active(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that completed migrations don't block new active migrations."""
        tenant_id = uuid4()
        migration1_id = uuid4()
        migration2_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Insert completed migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration1_id, "tenant_id": tenant_id},
            )

            # Insert new active migration - should succeed
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source2', 'target2', 'pending'
                )
            """),
                {"id": migration2_id, "tenant_id": tenant_id},
            )

            # Verify both exist
            result = await conn.execute(
                text("SELECT COUNT(*) FROM tenant_migrations WHERE tenant_id = :tenant_id"),
                {"tenant_id": tenant_id},
            )
            count = result.scalar()

        assert count == 2

    async def test_updated_at_trigger(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that updated_at is automatically updated."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Insert migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'pending'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Get initial updated_at
            result = await conn.execute(
                text("SELECT updated_at FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            initial_updated_at = result.scalar()

        # Update in separate transaction (slight delay)
        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                text("""
                UPDATE tenant_migrations SET phase = 'bulk_copy' WHERE id = :id
            """),
                {"id": migration_id},
            )

            result = await conn.execute(
                text("SELECT updated_at FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            new_updated_at = result.scalar()

        assert new_updated_at >= initial_updated_at


class TestTenantRoutingTable:
    """Tests for tenant_routing table operations."""

    async def test_insert_routing(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test inserting a routing record."""
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state
                ) VALUES (
                    :tenant_id, :store_id, :migration_state
                )
            """),
                {
                    "tenant_id": tenant_id,
                    "store_id": "shared-store",
                    "migration_state": "normal",
                },
            )

            result = await conn.execute(
                text(
                    "SELECT tenant_id, store_id, migration_state FROM tenant_routing WHERE tenant_id = :tenant_id"
                ),
                {"tenant_id": tenant_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == tenant_id
        assert row[1] == "shared-store"
        assert row[2] == "normal"

    async def test_migration_state_check_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that invalid migration_state values are rejected."""
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            with pytest.raises(Exception):  # noqa: B017 - DB-specific exception
                await conn.execute(
                    text("""
                    INSERT INTO tenant_routing (
                        tenant_id, store_id, migration_state
                    ) VALUES (
                        :tenant_id, 'store', 'invalid_state'
                    )
                """),
                    {"tenant_id": tenant_id},
                )

    async def test_routing_with_migration_reference(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test routing with active_migration_id reference."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration first
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Create routing with reference
            await conn.execute(
                text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state, active_migration_id
                ) VALUES (
                    :tenant_id, 'source', 'bulk_copy', :migration_id
                )
            """),
                {"tenant_id": tenant_id, "migration_id": migration_id},
            )

            result = await conn.execute(
                text("SELECT active_migration_id FROM tenant_routing WHERE tenant_id = :tenant_id"),
                {"tenant_id": tenant_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == migration_id

    async def test_routing_foreign_key_set_null_on_delete(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that deleting migration sets active_migration_id to NULL."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Create routing with reference
            await conn.execute(
                text("""
                INSERT INTO tenant_routing (
                    tenant_id, store_id, migration_state, active_migration_id
                ) VALUES (
                    :tenant_id, 'target', 'migrated', :migration_id
                )
            """),
                {"tenant_id": tenant_id, "migration_id": migration_id},
            )

            # Delete migration
            await conn.execute(
                text("DELETE FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )

            # Check that routing still exists with NULL active_migration_id
            result = await conn.execute(
                text("SELECT active_migration_id FROM tenant_routing WHERE tenant_id = :tenant_id"),
                {"tenant_id": tenant_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] is None


class TestMigrationPositionMappingsTable:
    """Tests for migration_position_mappings table operations."""

    async def test_insert_position_mapping(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test inserting a position mapping."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration first
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert position mapping
            await conn.execute(
                text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position, event_id
                ) VALUES (
                    :migration_id, :source_position, :target_position, :event_id
                )
            """),
                {
                    "migration_id": migration_id,
                    "source_position": 100,
                    "target_position": 50,
                    "event_id": event_id,
                },
            )

            result = await conn.execute(
                text("""
                SELECT source_position, target_position, event_id
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
            """),
                {"migration_id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == 100
        assert row[1] == 50
        assert row[2] == event_id

    async def test_position_mapping_unique_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test unique constraint on migration_id + source_position."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert first mapping
            await conn.execute(
                text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position, event_id
                ) VALUES (
                    :migration_id, 100, 50, :event_id
                )
            """),
                {"migration_id": migration_id, "event_id": uuid4()},
            )

            # Try to insert duplicate source_position
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    text("""
                    INSERT INTO migration_position_mappings (
                        migration_id, source_position, target_position, event_id
                    ) VALUES (
                        :migration_id, 100, 60, :event_id
                    )
                """),
                    {"migration_id": migration_id, "event_id": uuid4()},
                )

            assert (
                "unique" in str(exc_info.value).lower()
                or "duplicate" in str(exc_info.value).lower()
            )

    async def test_position_mappings_cascade_delete(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that position mappings are deleted when migration is deleted."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert mappings
            for i in range(5):
                await conn.execute(
                    text("""
                    INSERT INTO migration_position_mappings (
                        migration_id, source_position, target_position, event_id
                    ) VALUES (
                        :migration_id, :source_pos, :target_pos, :event_id
                    )
                """),
                    {
                        "migration_id": migration_id,
                        "source_pos": i * 10,
                        "target_pos": i * 5,
                        "event_id": uuid4(),
                    },
                )

            # Delete migration
            await conn.execute(
                text("DELETE FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )

            # Check that mappings were cascade deleted
            result = await conn.execute(
                text(
                    "SELECT COUNT(*) FROM migration_position_mappings WHERE migration_id = :migration_id"
                ),
                {"migration_id": migration_id},
            )
            count = result.scalar()

        assert count == 0


class TestMigrationAuditLogTable:
    """Tests for migration_audit_log table operations."""

    async def test_insert_audit_log_entry(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test inserting an audit log entry."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'pending'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert audit log entry
            await conn.execute(
                text("""
                INSERT INTO migration_audit_log (
                    migration_id, event_type, old_phase, new_phase,
                    details, operator
                ) VALUES (
                    :migration_id, :event_type, :old_phase, :new_phase,
                    :details, :operator
                )
            """),
                {
                    "migration_id": migration_id,
                    "event_type": "migration_started",
                    "old_phase": None,
                    "new_phase": "pending",
                    "details": '{"initiated_by": "system"}',
                    "operator": "test_user",
                },
            )

            result = await conn.execute(
                text("""
                SELECT event_type, new_phase, operator
                FROM migration_audit_log
                WHERE migration_id = :migration_id
            """),
                {"migration_id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == "migration_started"
        assert row[1] == "pending"
        assert row[2] == "test_user"

    async def test_audit_log_event_type_check_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that invalid event_type values are rejected."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'pending'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            with pytest.raises(Exception):  # noqa: B017 - DB-specific exception
                await conn.execute(
                    text("""
                    INSERT INTO migration_audit_log (
                        migration_id, event_type
                    ) VALUES (
                        :migration_id, 'invalid_event_type'
                    )
                """),
                    {"migration_id": migration_id},
                )

    async def test_audit_log_cascade_delete(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that audit logs are deleted when migration is deleted."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert audit log entries
            event_types = [
                "migration_started",
                "phase_changed",
                "migration_completed",
            ]
            for event_type in event_types:
                await conn.execute(
                    text("""
                    INSERT INTO migration_audit_log (
                        migration_id, event_type
                    ) VALUES (
                        :migration_id, :event_type
                    )
                """),
                    {"migration_id": migration_id, "event_type": event_type},
                )

            # Delete migration
            await conn.execute(
                text("DELETE FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )

            # Check that audit logs were cascade deleted
            result = await conn.execute(
                text("SELECT COUNT(*) FROM migration_audit_log WHERE migration_id = :migration_id"),
                {"migration_id": migration_id},
            )
            count = result.scalar()

        assert count == 0

    async def test_audit_log_ordering(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that audit logs can be ordered by occurred_at."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert multiple audit entries
            for event_type in [
                "migration_started",
                "phase_changed",
                "migration_completed",
            ]:
                await conn.execute(
                    text("""
                    INSERT INTO migration_audit_log (
                        migration_id, event_type
                    ) VALUES (
                        :migration_id, :event_type
                    )
                """),
                    {"migration_id": migration_id, "event_type": event_type},
                )

            # Query ordered by occurred_at
            result = await conn.execute(
                text("""
                SELECT event_type FROM migration_audit_log
                WHERE migration_id = :migration_id
                ORDER BY occurred_at ASC
            """),
                {"migration_id": migration_id},
            )
            rows = result.fetchall()

        assert len(rows) == 3
        assert rows[0][0] == "migration_started"
        assert rows[2][0] == "migration_completed"
