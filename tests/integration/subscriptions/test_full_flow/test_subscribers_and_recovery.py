"""Integration tests for multiple concurrent subscribers and error recovery."""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionState
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    populate_event_store,
)

from .helpers import position_after

pytestmark = pytest.mark.asyncio


class TestMultipleConcurrentSubscribers:
    """Tests for multiple concurrent subscribers."""

    async def test_multiple_subscribers_independent_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 50)
        projection1 = CollectingProjection(name="Proj1")
        projection2 = CollectingProjection(name="Proj2")
        projection3 = CollectingProjection(name="Proj3")

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.subscribe(projection3, config, name="Projection3")
        await subscription_manager.start()

        await projection1.wait_for_events(50)
        await projection2.wait_for_events(50)
        await projection3.wait_for_events(50)

        assert projection1.event_count == 50
        assert projection2.event_count == 50
        assert projection3.event_count == 50

    async def test_multiple_subscribers_different_starting_points(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        events = await populate_event_store(in_memory_event_store, 100)
        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionWithCheckpoint",
            position=await position_after(in_memory_event_store, 75),
            event_id=events[74].event_id,
            event_type="TestOrderCreated",
        )

        projection_beginning = CollectingProjection(name="Beginning")
        projection_checkpoint = CollectingProjection(name="Checkpoint")
        config_beginning = SubscriptionConfig(start_from="beginning")
        config_checkpoint = SubscriptionConfig(start_from="checkpoint")

        await subscription_manager.subscribe(
            projection_beginning, config_beginning, name="ProjectionFromBeginning"
        )
        await subscription_manager.subscribe(
            projection_checkpoint, config_checkpoint, name="ProjectionWithCheckpoint"
        )
        await subscription_manager.start()

        await projection_beginning.wait_for_events(100)
        await projection_checkpoint.wait_for_events(25)

        assert projection_beginning.event_count == 100
        assert projection_checkpoint.event_count == 25
        assert projection_checkpoint.events[0].order_number == "ORD-00075"

    async def test_slow_subscriber_does_not_block_others(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 20)
        fast_projection = CollectingProjection(name="Fast")
        slow_projection = SlowProjection(delay=0.05)

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(fast_projection, config, name="FastProjection")
        await subscription_manager.subscribe(slow_projection, config, name="SlowProjection")
        await subscription_manager.start()

        assert await fast_projection.wait_for_events(20, timeout=5.0)
        assert await slow_projection.wait_for_events(20, timeout=15.0)
        assert fast_projection.event_count == 20
        assert slow_projection.event_count == 20


class TestErrorHandlingAndRecovery:
    """Tests for error handling and recovery."""

    async def test_continue_on_error_mode(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 10)
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})
        config = SubscriptionConfig(start_from="beginning", continue_on_error=True)
        await subscription_manager.subscribe(projection, config, name="FailingProjection")
        await subscription_manager.start()
        await asyncio.sleep(0.5)

        assert subscription_manager.is_running
        subscription = subscription_manager.get_subscription("FailingProjection")
        assert subscription is not None and subscription.events_failed > 0

    async def test_stop_on_error_mode(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 10)
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})
        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await subscription_manager.subscribe(projection, config, name="FailingProjection")

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        subscription = subscription_manager.get_subscription("FailingProjection")
        assert subscription is not None and subscription.state == SubscriptionState.ERROR

    async def test_healthy_subscriber_unaffected_by_failing_sibling(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 20)
        healthy_projection = CollectingProjection(name="Healthy")
        failing_projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config_healthy = SubscriptionConfig(start_from="beginning")
        config_failing = SubscriptionConfig(start_from="beginning", continue_on_error=True)

        await subscription_manager.subscribe(
            healthy_projection, config_healthy, name="HealthyProjection"
        )
        await subscription_manager.subscribe(
            failing_projection, config_failing, name="FailingProjection"
        )
        await subscription_manager.start()

        assert await healthy_projection.wait_for_events(20)
        assert healthy_projection.event_count == 20
