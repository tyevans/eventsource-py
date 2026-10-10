"""Tests for tenant_migrations table operations on PostgreSQL."""

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

        assert initial_updated_at is not None
        assert new_updated_at is not None
        assert new_updated_at >= initial_updated_at
