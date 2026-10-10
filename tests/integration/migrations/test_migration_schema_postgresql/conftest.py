"""Shared pytest fixtures and configuration for migration schema PostgreSQL tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
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


@pytest.fixture
async def migration_schema_engine(postgres_engine: AsyncEngine) -> AsyncGenerator[AsyncEngine]:
    """
    Provide PostgreSQL engine with migration schema created.

    Creates the migration schema tables and cleans them up after test.
    """
    schema_sql = get_schema("migration")

    # Use raw asyncpg connection to execute multi-statement SQL script
    # SQLAlchemy's exec_driver_sql doesn't support multiple statements
    async with postgres_engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        driver_conn = raw_conn.driver_connection
        assert driver_conn is not None
        await driver_conn.execute(schema_sql)

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
