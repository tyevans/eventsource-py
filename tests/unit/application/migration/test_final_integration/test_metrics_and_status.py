"""Tests for metrics collection and status streaming during migration."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.application.migration.status_streamer import StatusStreamer, StatusStreamManager

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_final_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_final_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestMetricsCollectionDuringMigration:
    @pytest.mark.asyncio
    async def test_metrics_track_events_copied(self, migration_id: UUID, tenant_id: UUID) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        metrics.record_events_copied(100, rate_events_per_sec=1000.0)
        metrics.record_events_copied(150, rate_events_per_sec=1500.0)
        metrics.record_events_copied(50, rate_events_per_sec=500.0)
        snapshot = metrics.get_snapshot()
        assert (snapshot.events_copied, snapshot.events_copied_rate) == (300, 500.0)

    @pytest.mark.asyncio
    async def test_metrics_track_sync_lag(self, migration_id: UUID, tenant_id: UUID) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        for raw, expected in [(1000, 1000), (500, 500), (0, 0), (-10, 0)]:
            metrics.record_sync_lag(raw)
            assert metrics.current_sync_lag == expected

    @pytest.mark.asyncio
    async def test_metrics_track_phase_durations(self, migration_id: UUID, tenant_id: UUID) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_phase_duration("dual_write", 600.0)
        metrics.record_phase_duration("cutover", 0.05)
        assert metrics.get_snapshot().phase_durations == {
            "bulk_copy": 300.0,
            "dual_write": 600.0,
            "cutover": 0.05,
        }

    @pytest.mark.asyncio
    async def test_metrics_track_cutover_duration(
        self, migration_id: UUID, tenant_id: UUID
    ) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        metrics.record_cutover_duration(45.5, success=True)
        metrics.record_cutover_duration(100.0, success=False)
        assert metrics.get_snapshot().cutover_durations == [45.5, 100.0]

    @pytest.mark.asyncio
    async def test_metrics_track_failures(self, migration_id: UUID, tenant_id: UUID) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        metrics.record_failed_target_write(error_type="timeout")
        metrics.record_failed_target_write(error_type="connection_error")
        metrics.record_failed_target_write()
        metrics.record_verification_failure(failure_type="count_mismatch")
        metrics.record_verification_failure()
        snapshot = metrics.get_snapshot()
        assert (snapshot.failed_target_writes, snapshot.verification_failures) == (3, 2)

    @pytest.mark.asyncio
    async def test_metrics_context_managers(self, migration_id: UUID, tenant_id: UUID) -> None:
        metrics = get_migration_metrics(migration_id=str(migration_id), tenant_id=str(tenant_id))
        with metrics.time_phase("test_phase"):
            await asyncio.sleep(0.01)
        assert metrics.get_snapshot().phase_durations["test_phase"] >= 0.01

        with metrics.time_cutover() as timer:
            await asyncio.sleep(0.01)
            timer.success = True
        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1 and snapshot.cutover_durations[0] >= 10


class TestStatusStreamingDuringMigration:
    @staticmethod
    async def _setup_migration(
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ):
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        return coordinator, migration

    @pytest.mark.asyncio
    async def test_status_streamer_yields_initial_status(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        tenant_id,
    ) -> None:
        coordinator, migration = await self._setup_migration(
            source_store,
            target_store,
            migration_repo,
            routing_repo,
            router,
            lock_manager,
            tenant_id,
        )
        streamer = StatusStreamer(
            coordinator=coordinator, migration_id=migration.id, enable_tracing=False
        )
        statuses = []
        async for status in streamer.stream_status(update_interval=0.1, include_initial=True):
            statuses.append(status)
            break
        assert len(statuses) == 1
        assert (statuses[0].migration_id, statuses[0].tenant_id) == (migration.id, tenant_id)
        await streamer.close()

    @pytest.mark.asyncio
    async def test_status_stream_manager_creates_streamers(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        tenant_id,
    ) -> None:
        coordinator, migration = await self._setup_migration(
            source_store,
            target_store,
            migration_repo,
            routing_repo,
            router,
            lock_manager,
            tenant_id,
        )
        manager = StatusStreamManager(coordinator=coordinator, enable_tracing=False)
        streamer1 = await manager.get_streamer(migration.id)
        assert streamer1 is not None and manager.active_streamers == 1

        streamer2 = await manager.get_streamer(migration.id)
        assert streamer2 is streamer1 and manager.active_streamers == 1

        await manager.close_all()
        assert manager.active_streamers == 0

    @pytest.mark.asyncio
    async def test_status_streamer_subscriber_count(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        tenant_id,
    ) -> None:
        coordinator, migration = await self._setup_migration(
            source_store,
            target_store,
            migration_repo,
            routing_repo,
            router,
            lock_manager,
            tenant_id,
        )
        streamer = StatusStreamer(
            coordinator=coordinator, migration_id=migration.id, enable_tracing=False
        )
        assert streamer.subscriber_count == 0

        async def stream_and_count():
            async for _ in streamer.stream_status(update_interval=0.1):
                break

        task = asyncio.create_task(stream_and_count())
        await asyncio.sleep(0.05)
        await task
        await streamer.close()


__all__ = [  # pragma: allowlist secret
    "TestMetricsCollectionDuringMigration",
    "TestStatusStreamingDuringMigration",
]
