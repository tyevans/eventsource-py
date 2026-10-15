"""Integration tests for sequential delivery and graceful shutdown."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.application.subscriptions.flow_control import FlowController
from eventsource.application.subscriptions.shutdown import ShutdownCoordinator, ShutdownPhase
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    populate_event_store,
)

from .helpers import ConcurrencyTrackingProjection, position_after

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


class TestSequentialDelivery:
    """Delivery is ordered per subscription, and `FlowController` counts it."""

    async def test_flow_controller_tracks_acquisitions_and_releases(self):
        controller = FlowController()

        async def process() -> None:
            async with await controller.acquire():
                await asyncio.sleep(0.01)

        await asyncio.gather(*[asyncio.create_task(process()) for _ in range(25)])
        stats = controller.stats
        assert stats.total_acquisitions == 25 and stats.total_releases == 25
        assert stats.events_in_flight == 0

    async def test_a_slow_subscriber_is_delivered_one_event_at_a_time(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 50)
        projection = ConcurrencyTrackingProjection(process_delay=0.05)
        await subscription_manager.subscribe(projection, SubscriptionConfig(start_from="beginning"))
        await subscription_manager.start()
        success = await projection.wait_for_events(50, timeout=30.0)
        await subscription_manager.stop()
        assert success, f"Expected 50 events, got {projection.event_count}"
        assert projection.max_concurrent == 1, (
            f"Deliveries overlapped: peak concurrency was {projection.max_concurrent}, expected 1"
        )

    async def test_a_burst_does_not_widen_delivery_concurrency(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 100)
        projection = ConcurrencyTrackingProjection(process_delay=0.02)
        config = SubscriptionConfig(start_from="beginning", batch_size=25)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        success = await projection.wait_for_events(100, timeout=30.0)
        await subscription_manager.stop()
        assert success and projection.event_count == 100
        assert projection.max_concurrent == 1, (
            f"Deliveries overlapped under burst: peak concurrency was {projection.max_concurrent}"
        )


class TestGracefulShutdown:
    """Tests for graceful shutdown behavior."""

    async def test_completes_in_flight_events_on_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 30)
        started_events: list[str] = []
        completed_events: list[str] = []

        class TrackingProjection:
            def subscribed_to(self):
                return [SubTestOrderCreated]

            async def handle(self, event):
                started_events.append(str(event.event_id))
                await asyncio.sleep(0.1)
                completed_events.append(str(event.event_id))

        await subscription_manager.subscribe(TrackingProjection())
        await subscription_manager.start()
        await asyncio.sleep(0.2)
        await subscription_manager.stop(timeout=10.0)
        assert len(completed_events) >= len(started_events) - 2

    async def test_saves_checkpoint_on_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()
        await subscription_manager.subscribe(projection, SubscriptionConfig(start_from="beginning"))
        await subscription_manager.start()
        await projection.wait_for_events(50)
        await subscription_manager.stop()
        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None and position >= await position_after(in_memory_event_store, 50)

    async def test_forced_shutdown_after_timeout(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store(in_memory_event_store, 3)

        class LocalSlowProjection:
            def __init__(self):
                self.event_count = 0
                self.handler_started = asyncio.Event()

            def subscribed_to(self):
                return [SubTestOrderCreated]

            async def handle(self, event):
                self.handler_started.set()
                for _ in range(20):
                    await asyncio.sleep(0.05)
                self.event_count += 1

        projection = LocalSlowProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)
        await manager.start()
        await asyncio.wait_for(projection.handler_started.wait(), timeout=2.0)

        start = asyncio.get_event_loop().time()
        await manager.stop(timeout=0.2)
        duration = asyncio.get_event_loop().time() - start
        assert duration < 1.0, f"Stop took too long: {duration:.2f}s"

    async def test_shutdown_coordinator_phases(self):
        coordinator = ShutdownCoordinator(timeout=30.0, drain_timeout=5.0, checkpoint_timeout=2.0)
        phases_seen: list[ShutdownPhase] = []

        async def stop_func():
            phases_seen.append(coordinator.phase)

        async def drain_func() -> int:
            phases_seen.append(coordinator.phase)
            return 5

        async def checkpoint_func() -> int:
            phases_seen.append(coordinator.phase)
            return 1

        result = await coordinator.shutdown(
            stop_func=stop_func, drain_func=drain_func, checkpoint_func=checkpoint_func
        )
        assert result.phase in (ShutdownPhase.STOPPED, ShutdownPhase.FORCED)
        assert ShutdownPhase.STOPPING in phases_seen
        assert ShutdownPhase.DRAINING in phases_seen
        assert ShutdownPhase.CHECKPOINTING in phases_seen

    async def test_shutdown_coordinator_request_shutdown(self):
        coordinator = ShutdownCoordinator()
        assert not coordinator.is_shutting_down
        coordinator.request_shutdown()
        assert coordinator.is_shutting_down
