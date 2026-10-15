"""Integration tests for pause/resume functionality."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    PauseReason,
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    SlowProjection,
    SubTestOrderCreated,
    populate_event_store,
)

from .helpers import PausableProjection

pytestmark = pytest.mark.asyncio


class TestPauseResume:
    """Tests for pause/resume functionality."""

    async def test_pause_stops_processing(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test that pause stops event processing."""
        projection = PausableProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(
            projection, SubscriptionConfig(start_from="beginning"), name="PausableProj"
        )
        await manager.start()
        await asyncio.sleep(0.2)

        assert await manager.pause_subscription("PausableProj") is True
        subscription = manager.get_subscription("PausableProj")
        assert subscription is not None and subscription.state == SubscriptionState.PAUSED

    async def test_resume_continues_from_position(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test that resume continues from the last position."""
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(
            projection, SubscriptionConfig(start_from="beginning"), name="ResumeProj"
        )
        await manager.start()
        await projection.wait_for_events(50, timeout=10.0)
        initial_count = projection.event_count

        await manager.pause_subscription("ResumeProj")
        await populate_event_store(in_memory_event_store, 25, start_index=50)
        for i in range(50, 75):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(), order_number=f"ORD-{i:05d}", amount=100.0 + i
            )
            await in_memory_event_bus.publish([event])

        await manager.resume_subscription("ResumeProj")
        await projection.wait_for_events(initial_count + 25, timeout=10.0)
        await manager.stop()
        assert projection.event_count >= initial_count + 25

    async def test_pause_during_catchup(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test pause during catch-up phase."""
        await populate_event_store(in_memory_event_store, 200)
        projection = SlowProjection(delay=0.05)
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning", batch_size=10)
        await manager.subscribe(projection, config, name="CatchupPauseProj")
        await manager.start()
        await asyncio.sleep(0.3)

        subscription = manager.get_subscription("CatchupPauseProj")
        if subscription and subscription.state == SubscriptionState.CATCHING_UP:
            assert await manager.pause_subscription("CatchupPauseProj") is True
            assert subscription.state == SubscriptionState.PAUSED
            assert subscription.state_before_pause == SubscriptionState.CATCHING_UP
            await manager.resume_subscription("CatchupPauseProj")
            assert subscription.state == SubscriptionState.CATCHING_UP

        await manager.stop()

    async def test_pause_during_live(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test pause during live phase."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(
            projection, SubscriptionConfig(start_from="beginning"), name="LivePauseProj"
        )
        await manager.start()
        await asyncio.sleep(0.2)

        subscription = manager.get_subscription("LivePauseProj")
        assert subscription is not None and subscription.state == SubscriptionState.LIVE
        assert await manager.pause_subscription("LivePauseProj") is True
        assert subscription.state == SubscriptionState.PAUSED
        assert subscription.state_before_pause == SubscriptionState.LIVE

        await manager.resume_subscription("LivePauseProj")
        assert subscription.state == SubscriptionState.LIVE
        await manager.stop()

    async def test_pause_all_subscriptions(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test pausing all subscriptions at once."""
        projections = [CollectingProjection(name=f"AllPause{i}") for i in range(3)]
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"AllPause{i}")

        await manager.start()
        await asyncio.sleep(0.2)
        results = await manager.pause_all()
        assert all(results.values()) and len(manager.paused_subscriptions) == 3
        await manager.stop()

    async def test_resume_all_subscriptions(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test resuming all subscriptions at once."""
        projections = [CollectingProjection(name=f"AllResume{i}") for i in range(3)]
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"AllResume{i}")

        await manager.start()
        await asyncio.sleep(0.2)
        await manager.pause_all()
        assert len(manager.paused_subscriptions) == 3

        results = await manager.resume_all()
        assert all(results.values()) and len(manager.paused_subscriptions) == 0
        await manager.stop()

    async def test_pause_with_reason_tracking(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        """Test that pause reason is tracked correctly."""
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(
            projection, SubscriptionConfig(start_from="beginning"), name="ReasonProj"
        )
        await manager.start()
        await asyncio.sleep(0.2)

        await manager.pause_subscription("ReasonProj", reason=PauseReason.MAINTENANCE)
        subscription = manager.get_subscription("ReasonProj")
        assert subscription is not None and subscription.pause_reason == PauseReason.MAINTENANCE
        await manager.stop()
