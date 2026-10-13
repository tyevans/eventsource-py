"""Tests for cutover execution, success, and failure rollback scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import (
    MigrationConfig,
    TenantMigrationState,
)
from tests.unit.application.migration.test_phase2_integration._events import SampleTestEvent
from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.ports import Position
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryRoutingRepository,
    )


class TestCutoverSuccess:
    """Tests for successful cutover scenarios."""

    @pytest.mark.asyncio
    async def test_cutover_success_when_synced(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test successful cutover when stores are synchronized."""
        migration_id = uuid4()
        last_source_position: Position | None = None

        # Setup: Same events in both stores (fully synced)
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
            result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
            last_source_position = result.position
            await target_store.append(stream, [event], ExpectedVersion.no_stream())

        # Setup routing state
        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        # Register target store
        router.register_store("dedicated", target_store)

        # Create lag tracker
        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=100),
            enable_tracing=False,
        )

        # Create cutover manager
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        # Execute cutover -- `since` the last position already copied, so
        # the lag tracker sees zero events behind.
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=500.0,
            since=last_source_position,
        )

        assert result.success
        assert result.duration_ms > 0

        # Verify routing was updated
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.MIGRATED


class TestCutoverFailureAndRollback:
    """Tests for cutover failure and rollback scenarios."""

    @pytest.mark.asyncio
    async def test_cutover_fails_when_lag_too_high(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when sync lag exceeds threshold."""
        migration_id = uuid4()

        # Setup: Source has many more events than target, and nothing has
        # been copied yet (since=None), so every source event counts as lag.
        for _i in range(200):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target has only 10 events
        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Setup routing state
        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        router.register_store("dedicated", target_store)

        # Create lag tracker with strict threshold
        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=50),  # Strict threshold
            enable_tracing=False,
        )

        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            config=MigrationConfig(cutover_max_lag_events=50),
            timeout_ms=500.0,
        )

        assert result.success is False
        assert result.rolled_back is True
        assert "lag" in result.error_message.lower()

        # Verify routing was rolled back to DUAL_WRITE
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE

    @pytest.mark.asyncio
    async def test_cutover_fails_on_lock_acquisition_failure(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when lock cannot be acquired."""
        migration_id = uuid4()

        # Use failing lock manager
        failing_lock_manager = MockLockManager(should_fail=True)

        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        cutover_manager = CutoverManager(
            lock_manager=failing_lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=500.0,
        )

        assert result.success is False
        assert "lock" in result.error_message.lower()


__all__ = [
    "TestCutoverFailureAndRollback",
    "TestCutoverSuccess",
]
