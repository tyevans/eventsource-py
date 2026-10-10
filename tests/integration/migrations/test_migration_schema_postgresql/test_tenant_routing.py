"""Tests for tenant_routing table operations on PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from ...conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


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
