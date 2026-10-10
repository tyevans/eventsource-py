"""Tests for migration schema creation and idempotency on PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text

from eventsource.adapters.sql.schemas import get_schema

from ...conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


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
            driver_conn = raw_conn.driver_connection
            assert driver_conn is not None
            await driver_conn.execute(schema_sql)

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
