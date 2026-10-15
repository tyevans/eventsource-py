"""Integration tests for full subscription lifecycle and checkpoints."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion, Position
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

from .helpers import position_after

pytestmark = pytest.mark.asyncio


class TestFullLifecycle:
    """Tests for complete subscription lifecycle."""

    async def test_full_lifecycle(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()
        subscription = await subscription_manager.subscribe(projection)
        assert subscription.state == SubscriptionState.STARTING

        await subscription_manager.start()
        success = await projection.wait_for_events(50)
        assert success
        await subscription_manager.stop()

        assert projection.event_count == 50
        assert subscription.state == SubscriptionState.STOPPED

    async def test_lifecycle_with_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 25)
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(25)

        for i in range(25, 35):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"ORD-{i:05d}")

        success = await projection.wait_for_events(35)
        assert success
        await subscription_manager.stop()

        assert projection.event_count == 35
        assert not projection.has_duplicates()


class TestCheckpointPersistence:
    """Tests for checkpoint persistence."""

    async def test_checkpoint_persisted_after_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 100)
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(100)
        await subscription_manager.stop()

        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None
        assert position >= await position_after(in_memory_event_store, 100)

    async def test_checkpoint_persisted_periodically(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 200)
        projection = CollectingProjection()
        config = SubscriptionConfig(start_from="beginning", batch_size=50)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(100)

        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None
        assert position >= await position_after(in_memory_event_store, 50)
        await subscription_manager.stop()


class TestResumeAfterRestart:
    """Tests for resuming after restart."""

    async def test_resume_after_restart(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store(in_memory_event_store, 50)
        projection1 = CollectingProjection()
        manager1 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        await manager1.subscribe(projection1, config, name="TestProjection")
        await manager1.start()
        await projection1.wait_for_events(50)
        await manager1.stop()

        for i in range(50, 100):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(), order_number=f"ORD-{i:05d}", amount=100.0 + i
            )
            await in_memory_event_store.append(
                StreamId(aggregate_id=event.aggregate_id, category="TestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )

        projection2 = CollectingProjection()
        manager2 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        resume_config = SubscriptionConfig(start_from="checkpoint")
        await manager2.subscribe(projection2, resume_config, name="TestProjection")
        await manager2.start()
        await projection2.wait_for_events(50)
        await manager2.stop()

        assert projection2.event_count == 50
        assert projection2.events[0].order_number == "ORD-00050"

    async def test_resume_from_beginning_ignores_checkpoint(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await in_memory_checkpoint_repo.save_position(
            subscription_id="TestProjection",
            position=Position(store_id="memory", key=(50,)),
            event_id=uuid4(),
            event_type="TestOrderCreated",
        )
        await populate_event_store(in_memory_event_store, 100)
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="TestProjection")
        await manager.start()
        await projection.wait_for_events(100)
        await manager.stop()

        assert projection.event_count == 100
        assert projection.events[0].order_number == "ORD-00000"


class TestContextManagerUsage:
    """Tests for async context manager usage."""

    async def test_context_manager(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store(in_memory_event_store, 25)
        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)

        async with manager:
            success = await projection.wait_for_events(25, timeout=10.0)
            assert success, f"Expected 25 events, got {projection.event_count}"

        assert projection.event_count == 25
        assert not manager.is_running

    async def test_context_manager_cleanup_on_exception(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        projection = CollectingProjection()
        try:
            async with SubscriptionManager(
                event_store=in_memory_event_store,
                event_bus=in_memory_event_bus,
                checkpoint_repo=in_memory_checkpoint_repo,
            ) as manager:
                await manager.subscribe(projection)
                raise RuntimeError("Test exception")
        except RuntimeError:
            pass

        assert not manager.is_running
