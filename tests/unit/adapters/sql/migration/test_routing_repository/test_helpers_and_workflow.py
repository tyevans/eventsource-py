"""Unit tests for TenantRoutingRepository row helpers and migration state transition workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import (
    PostgreSQLTenantRoutingRepository,
)
from eventsource.ports.migration.models import TenantMigrationState


class TestPostgreSQLTenantRoutingRepositoryHelpers:
    """Tests for PostgreSQLTenantRoutingRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLTenantRoutingRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLTenantRoutingRepository(mock_conn, enable_tracing=False)

    def test_row_to_routing_with_all_fields(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test converting a row with all fields populated."""
        tenant_id = uuid4()
        migration_id = uuid4()
        created_at = datetime.now(UTC)
        updated_at = datetime.now(UTC)

        row = (
            tenant_id,
            "dedicated",
            "dual_write",
            migration_id,
            created_at,
            updated_at,
        )

        routing = repo._row_to_routing(row)

        assert routing.tenant_id == tenant_id
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE
        assert routing.active_migration_id == migration_id
        assert routing.created_at == created_at
        assert routing.updated_at == updated_at

    def test_row_to_routing_with_null_migration_id(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test converting a row with null migration_id."""
        tenant_id = uuid4()
        created_at = datetime.now(UTC)

        row = (
            tenant_id,
            "shared",
            "normal",
            None,  # No active migration
            created_at,
            created_at,
        )

        routing = repo._row_to_routing(row)

        assert routing.tenant_id == tenant_id
        assert routing.active_migration_id is None
        assert routing.migration_state == TenantMigrationState.NORMAL

    @pytest.mark.parametrize(
        "state_value,expected_state",
        [
            ("normal", TenantMigrationState.NORMAL),
            ("bulk_copy", TenantMigrationState.BULK_COPY),
            ("dual_write", TenantMigrationState.DUAL_WRITE),
            ("cutover_paused", TenantMigrationState.CUTOVER_PAUSED),
            ("migrated", TenantMigrationState.MIGRATED),
        ],
    )
    def test_row_to_routing_all_states(
        self,
        repo: PostgreSQLTenantRoutingRepository,
        state_value: str,
        expected_state: TenantMigrationState,
    ) -> None:
        """Test converting rows with all possible migration states."""
        tenant_id = uuid4()
        now = datetime.now(UTC)

        row = (tenant_id, "shared", state_value, None, now, now)

        routing = repo._row_to_routing(row)

        assert routing.migration_state == expected_state


class TestMigrationStateTransitionWorkflow:
    """Integration-style tests for migration state transition workflow."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_typical_migration_workflow(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test a typical migration state transition workflow."""
        tenant_id = uuid4()
        migration_id = uuid4()

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Step 1: Set initial routing (tenant starts on shared store)
            await repo.set_routing(tenant_id, "shared")

            # Verify the set_routing call
            call_args = mock_conn.execute.call_args_list[0]
            assert call_args[0][1]["store_id"] == "shared"

            # Step 2: Start migration - transition to BULK_COPY
            await repo.set_migration_state(tenant_id, TenantMigrationState.BULK_COPY, migration_id)

            call_args = mock_conn.execute.call_args_list[1]
            assert call_args[0][1]["state"] == "bulk_copy"
            assert call_args[0][1]["migration_id"] == migration_id

            # Step 3: Enter dual-write phase
            await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)

            call_args = mock_conn.execute.call_args_list[2]
            assert call_args[0][1]["state"] == "dual_write"

            # Step 4: Cutover pause
            await repo.set_migration_state(
                tenant_id, TenantMigrationState.CUTOVER_PAUSED, migration_id
            )

            call_args = mock_conn.execute.call_args_list[3]
            assert call_args[0][1]["state"] == "cutover_paused"

            # Step 5: Complete migration
            await repo.set_migration_state(tenant_id, TenantMigrationState.MIGRATED, migration_id)

            call_args = mock_conn.execute.call_args_list[4]
            assert call_args[0][1]["state"] == "migrated"

            # Step 6: Clear migration state (cleanup)
            await repo.clear_migration_state(tenant_id)

            call_args = mock_conn.execute.call_args_list[5]
            assert call_args[0][1]["state"] == "normal"
            assert call_args[0][1]["migration_id"] is None

            # Total of 6 database operations
            assert mock_conn.execute.call_count == 6

    @pytest.mark.asyncio
    async def test_migration_abort_workflow(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test migration abort workflow."""
        tenant_id = uuid4()
        migration_id = uuid4()

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Start migration
            await repo.set_migration_state(tenant_id, TenantMigrationState.BULK_COPY, migration_id)

            # Enter dual-write
            await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)

            # Abort - clear migration state
            await repo.clear_migration_state(tenant_id)

            # Verify final state is NORMAL with no migration_id
            call_args = mock_conn.execute.call_args
            assert call_args[0][1]["state"] == "normal"
            assert call_args[0][1]["migration_id"] is None
