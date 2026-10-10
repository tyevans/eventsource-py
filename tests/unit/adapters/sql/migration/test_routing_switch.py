"""
Tests for atomic routing switch in migration cutover.

Governed by:
- ADR-0111 (Zero-Downtime Live Store Migration)
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- TASK-0007 (Atomic Routing Switch in Migration Cutover)
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import PostgreSQLTenantRoutingRepository
from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports.migration.models import (
    MigrationConfig,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.ports.positions import Position


class TestPostgreSQLTenantRoutingRepositorySwitchRouting:
    """Unit tests for PostgreSQLTenantRoutingRepository.switch_routing."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn,
            enable_tracing=False,
            enable_cache=True,
        )

    @pytest.mark.asyncio
    async def test_switch_routing_executes_atomic_upsert(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Verify switch_routing updates store_id and migration_state atomically."""
        tenant_id = uuid4()
        migration_id = uuid4()
        target_store = "dedicated-store-1"

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_sql_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_sql_conn

            await repo.switch_routing(
                tenant_id=tenant_id,
                store_id=target_store,
                state=TenantMigrationState.MIGRATED,
                migration_id=migration_id,
            )

            mock_sql_conn.execute.assert_called_once()
            call_args = mock_sql_conn.execute.call_args
            params = call_args[0][1]

            assert params["tenant_id"] == tenant_id
            assert params["store_id"] == target_store
            assert params["state"] == TenantMigrationState.MIGRATED.value
            assert params["migration_id"] == migration_id

    @pytest.mark.asyncio
    async def test_switch_routing_invalidates_cache(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Verify switch_routing clears cached routing entry for the tenant."""
        tenant_id = uuid4()
        old_routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="source-store",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        await repo._set_cache(tenant_id, old_routing)
        assert await repo._get_from_cache(tenant_id) is not None

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_sql_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_sql_conn

            await repo.switch_routing(
                tenant_id=tenant_id,
                store_id="target-store",
                state=TenantMigrationState.MIGRATED,
            )

        assert await repo._get_from_cache(tenant_id) is None


class TestAtomicCutoverFailureSimulation:
    """Failure simulation tests verifying no split-brain state occurs on cutover abort."""

    @pytest.mark.asyncio
    async def test_switch_routing_failure_leaves_no_split_brain_state(self) -> None:
        """
        When switch_routing fails at the atomic database transaction boundary,
        neither the store route nor the migration state is partially applied.
        """
        tenant_id = uuid4()
        migration_id = uuid4()
        source_store = "shared-store"
        target_store = "dedicated-store"

        # Stateful mock simulating an atomic transactional repository
        state = {
            "store_id": source_store,
            "migration_state": TenantMigrationState.DUAL_WRITE,
        }

        async def failing_switch(
            t_id, store_id, state=TenantMigrationState.MIGRATED, migration_id=None
        ):
            # Simulate a database failure during atomic transaction execution
            raise RuntimeError("Database connection lost during atomic routing switch")

        mock_repo = MagicMock()
        mock_repo.get_routing = AsyncMock(
            side_effect=lambda t_id: TenantRouting(
                tenant_id=t_id,
                store_id=state["store_id"],
                migration_state=state["migration_state"],
            )
        )
        mock_repo.set_migration_state = AsyncMock(
            side_effect=lambda t_id, st, **kw: state.update({"migration_state": st})
        )
        mock_repo.set_routing = AsyncMock(
            side_effect=lambda t_id, st: state.update({"store_id": st})
        )
        mock_repo.switch_routing = AsyncMock(side_effect=failing_switch)

        mock_lock = AsyncMock()
        mock_lock_mgr = MagicMock()
        mock_lock_mgr.acquire.return_value.__aenter__.return_value = mock_lock

        mock_router = MagicMock()
        mock_router.pause_writes = AsyncMock()
        mock_router.resume_writes = AsyncMock()
        mock_router.clear_dual_write_interceptor = MagicMock()
        mock_router.get_store.return_value = AsyncMock(
            current_position=AsyncMock(return_value=Position(store_id=target_store, key=(100,)))
        )

        mock_lag_tracker = MagicMock(spec=SyncLagTracker)
        mock_lag_tracker.calculate_lag = AsyncMock()
        mock_lag_tracker.current_lag = SyncLag(
            events=0,
            source_position=Position(store_id=source_store, key=(100,)),
            target_position=Position(store_id=target_store, key=(100,)),
            timestamp=datetime.now(UTC),
        )

        cutover = CutoverManager(
            lock_manager=mock_lock_mgr,
            router=mock_router,
            routing_repo=mock_repo,
        )

        result = await cutover.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store,
            config=MigrationConfig(cutover_timeout_ms=500),
        )

        # Cutover must report failure
        assert result.success is False
        assert "Database connection lost" in (result.error_message or "")

        # Crucial split-brain invariant:
        # Route must NOT have moved to target while state was paused or rolled back
        current_routing = await mock_repo.get_routing(tenant_id)
        assert current_routing.store_id == source_store
        assert current_routing.migration_state == TenantMigrationState.DUAL_WRITE
        mock_router.resume_writes.assert_called_once_with(tenant_id)
