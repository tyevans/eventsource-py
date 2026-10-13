"""Stop/pause responsiveness and tenant isolation tests for LiveRunner."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import LiveRunner
from tests.unit.application.subscriptions.test_live_runner._fixtures import (
    LiveTestEvent,
    MockLiveSubscriber,
    _RecordingFeed,
    append_event,
    wake,
)

# --- Stop-Pause Responsiveness Tests ---


class TestLiveRunnerStopPauseResponsiveness:
    """Test that stop() and pause() take effect on an in-flight drain."""

    @pytest.mark.asyncio
    async def test_stop_halts_an_in_flight_drain(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test stop() during a drain stops delivery before the whole feed is processed."""
        subscriber.handle_delay = 0.05
        await runner.start()

        total_events = 6
        for i in range(total_events):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"e{i}"))

        drain_task = asyncio.create_task(wake(event_bus))
        # Let the drain begin and process roughly one event before stopping.
        await asyncio.sleep(0.06)
        await runner.stop()

        await asyncio.wait_for(drain_task, timeout=2.0)

        assert len(subscriber.handled_events) < total_events

    @pytest.mark.asyncio
    async def test_pause_takes_effect_mid_drain(
        self,
        runner: LiveRunner,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        subscription: Subscription,
        subscriber: MockLiveSubscriber,
    ):
        """Test pause() during a drain blocks further delivery until resume()."""
        subscriber.handle_delay = 0.02
        await runner.start()

        total_events = 6
        for i in range(total_events):
            await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data=f"e{i}"))

        drain_task = asyncio.create_task(wake(event_bus))
        # Let the drain begin and process roughly one event before pausing.
        await asyncio.sleep(0.03)
        await subscription.pause()

        # Wait well past the time an un-paused drain would need to finish
        # all `total_events` (6 * 0.02s = 0.12s) to prove pause(), not mere
        # delay, is what is holding the rest of the batch back.
        await asyncio.sleep(0.3)
        processed_while_paused = len(subscriber.handled_events)
        assert processed_while_paused < total_events, (
            "pause() had no effect -- drain ran to completion while paused"
        )
        assert not drain_task.done(), "drain completed instead of blocking on pause"

        await subscription.resume()
        await asyncio.wait_for(drain_task, timeout=2.0)

        assert len(subscriber.handled_events) == total_events


# --- Tenant Isolation Tests ---
#
# `SubscriptionConfig.tenant_id` documents that, when set, "only events
# belonging to the specified tenant are processed". Catch-up honors this via
# `FeedReadOptions(tenant_id=self.config.tenant_id, ...)` (catchup.py:438).
# The live drain must agree -- a tenant-scoped subscription that behaves
# correctly during catch-up and then reads every tenant's events once live
# is a data-isolation breach, not a latency issue.


class TestLiveRunnerTenantIsolation:
    """Test a tenant-scoped LiveRunner never delivers another tenant's events."""

    @pytest.mark.asyncio
    async def test_tenant_scoped_drain_excludes_other_tenants(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test a tenant-scoped subscription only receives its own tenant's events."""
        own_tenant = uuid4()
        other_tenant = uuid4()

        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
            tenant_id=own_tenant,
        )
        subscription = Subscription(
            name="TenantScopedLive",
            config=config,
            subscriber=subscriber,
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()

        own_event = LiveTestEvent(aggregate_id=uuid4(), data="mine", tenant_id=own_tenant)
        other_event = LiveTestEvent(aggregate_id=uuid4(), data="not-mine", tenant_id=other_tenant)
        await append_event(event_store, own_event)
        await append_event(event_store, other_event)
        await wake(event_bus)

        await asyncio.sleep(0.01)

        handled_data = [e.data for e in subscriber.handled_events]
        assert "not-mine" not in handled_data, (
            "tenant-scoped LiveRunner delivered another tenant's event"
        )
        assert handled_data == ["mine"]

    @pytest.mark.asyncio
    async def test_drain_pushes_tenant_id_into_the_feed_read(
        self,
        event_bus: InMemoryEventBus,
        checkpoint_repo: InMemoryCheckpointRepository,
        event_store: InMemoryEventStore,
        subscriber: MockLiveSubscriber,
    ):
        """Test the bounded feed read itself is tenant-scoped, not just the consumer-side filter.

        `EventFilter` already drops another tenant's events before delivery
        (that is what the previous test observes), which means a delivery-only
        assertion cannot tell an indexed, tenant-scoped read apart from an
        unscoped read that happens to get filtered afterwards. Catch-up pushes
        the filter into the read itself (`FeedReadOptions(tenant_id=...)`,
        catchup.py:438) rather than relying on the consumer -- the live drain
        must match, both so the two runners agree (recurring-defects #1) and
        so backends can use the indexed predicate instead of over-fetching
        every tenant's rows.
        """
        own_tenant = uuid4()
        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
            tenant_id=own_tenant,
        )
        subscription = Subscription(
            name="TenantScopedLiveSpy",
            config=config,
            subscriber=subscriber,
        )
        recording_feed = _RecordingFeed(event_store)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=recording_feed,
            subscription=subscription,
        )
        await runner.start()

        await append_event(
            event_store, LiveTestEvent(aggregate_id=uuid4(), data="mine", tenant_id=own_tenant)
        )
        await wake(event_bus)
        await asyncio.sleep(0.01)

        assert recording_feed.read_all_calls, "read_all was never called"
        for _from_position, options in recording_feed.read_all_calls:
            assert options is not None
            assert options.tenant_id == own_tenant, (
                "read_all called without the subscription's tenant_id -- "
                "the live drain reads every tenant's events from the feed"
            )
