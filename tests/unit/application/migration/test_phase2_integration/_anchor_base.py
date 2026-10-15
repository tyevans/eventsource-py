"""Base test class for sync lag anchor tests on write-active tenants."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import MigrationConfig, TenantMigrationState
from tests.unit.application.migration.test_phase2_integration._events import SampleTestEvent
from tests.unit.application.migration.test_phase2_integration._helpers import (
    drive_dual_writes,
    seed_copied_prefix,
)
from tests.unit.application.migration.test_phase2_integration._mocks import FlakyTarget

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryRoutingRepository,
    )


class _TestSyncLagAnchorBase:
    @pytest.mark.asyncio
    async def test_healthy_tenant_converges_unbounded(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        interceptor = DualWriteInterceptor(
            source_store, target_store, tenant_id, enable_tracing=False
        )
        await drive_dual_writes(interceptor, tenant_id, 3)
        copied_through = await source_store.current_position()
        assert copied_through is not None
        assert interceptor.first_seen_source_position is not None
        assert copied_through >= interceptor.first_seen_source_position

        await drive_dual_writes(interceptor, tenant_id, 2)
        assert interceptor.first_failed_source_position is None
        assert interceptor.last_synced_source_position is not None

        tracker = SyncLagTracker(
            source_store,
            target_store,
            MigrationConfig(cutover_max_lag_events=3),
            tenant_id,
            enable_tracing=False,
        )
        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == interceptor.last_synced_source_position

        lag = await tracker.calculate_lag(since=anchor)
        assert lag.events == 0
        assert lag.count_is_bounded is False
        assert tracker.is_sync_ready() is True

    @pytest.mark.asyncio
    async def test_failed_prefix_never_reads_as_converged(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        migration_id = uuid4()
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)
        flaky = FlakyTarget(target_store, reject_first=3)
        interceptor = DualWriteInterceptor(source_store, flaky, tenant_id, enable_tracing=False)
        await drive_dual_writes(interceptor, tenant_id, 8)
        assert interceptor.first_failed_source_position is not None

        config = MigrationConfig(cutover_max_lag_events=3)
        tracker = SyncLagTracker(
            source_store, target_store, config, tenant_id, enable_tracing=False
        )
        anchor = interceptor.safe_lag_anchor(copied_through)
        lag = await tracker.calculate_lag(since=anchor)
        assert lag.events >= 3
        assert tracker.is_sync_ready() is False

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        cutover_manager = CutoverManager(lock_manager, router, routing_repo, enable_tracing=False)
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=tracker,
            target_store_id="dedicated",
            config=config,
            timeout_ms=500.0,
            since=anchor,
        )
        assert result.success is False
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id != "dedicated"

    @pytest.mark.asyncio
    async def test_first_failure_watermark_does_not_move_on_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        flaky = FlakyTarget(target_store, reject_first=1)
        interceptor = DualWriteInterceptor(source_store, flaky, tenant_id, enable_tracing=False)
        await drive_dual_writes(interceptor, tenant_id, 1)
        first_failed = interceptor.first_failed_source_position
        assert first_failed is not None

        await drive_dual_writes(interceptor, tenant_id, 5)
        assert interceptor.first_failed_source_position == first_failed
        assert interceptor.last_synced_source_position is not None
        assert interceptor.last_synced_source_position > first_failed
        assert interceptor.safe_lag_anchor(None) is None

    @pytest.mark.asyncio
    async def test_install_window_gap_blocks_cutover(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        migration_id = uuid4()
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)
        for _i in range(3):
            aggregate_id = uuid4()
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
                ExpectedVersion.no_stream(),
            )

        interceptor = DualWriteInterceptor(
            source_store, target_store, tenant_id, enable_tracing=False
        )
        await drive_dual_writes(interceptor, tenant_id, 2)
        assert interceptor.first_failed_source_position is None
        assert interceptor.last_synced_source_position is not None
        assert interceptor.first_seen_source_position is not None

        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == copied_through

        config = MigrationConfig(cutover_max_lag_events=3)
        tracker = SyncLagTracker(
            source_store, target_store, config, tenant_id, enable_tracing=False
        )
        lag = await tracker.calculate_lag(since=anchor)
        assert lag.events >= 3
        assert tracker.is_sync_ready() is False

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        cutover_manager = CutoverManager(lock_manager, router, routing_repo, enable_tracing=False)
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=tracker,
            target_store_id="dedicated",
            config=config,
            timeout_ms=500.0,
            since=anchor,
        )
        assert result.success is False
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id != "dedicated"

    @pytest.mark.asyncio
    async def test_anchor_advances_when_interceptor_covers_the_checkpoint(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        interceptor = DualWriteInterceptor(
            source_store, target_store, tenant_id, enable_tracing=False
        )
        await drive_dual_writes(interceptor, tenant_id, 1)
        checkpoint = interceptor.first_seen_source_position
        assert checkpoint is not None

        await drive_dual_writes(interceptor, tenant_id, 3)
        anchor = interceptor.safe_lag_anchor(checkpoint)
        assert anchor == interceptor.last_synced_source_position
        assert anchor != checkpoint


__all__ = ["_TestSyncLagAnchorBase"]
