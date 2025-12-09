"""
Unit tests for SubscriptionManager pause/resume functionality (PHASE3-005).

Tests cover:
- pause_subscription() method
- resume_subscription() method
- pause_all() method
- resume_all() method
- paused_subscriptions property
- paused_subscription_names property
- Integration with LiveRunner pause buffer
"""

from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.subscriptions import (
    PauseReason,
    SubscriptionManager,
    SubscriptionState,
)

# --- Sample Event Classes ---


class PauseTestEvent(DomainEvent):
    """Simple test event for pause testing."""

    event_type: str = "PauseTestEvent"
    aggregate_type: str = "PauseAggregate"
    data: str = "test"


# --- Mock Subscribers ---


class MockSubscriber:
    """Mock subscriber that tracks handled events."""

    def __init__(self, name: str | None = None) -> None:
        self.handled_events: list[DomainEvent] = []
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [PauseTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class OrderProjection(MockSubscriber):
    """Named projection for testing."""

    pass


class CustomerProjection(MockSubscriber):
    """Another named projection for testing."""

    pass


# --- Fixtures ---


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore."""
    return InMemoryEventStore(enable_tracing=False)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh InMemoryEventBus."""
    return InMemoryEventBus(enable_tracing=False)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create a fresh InMemoryCheckpointRepository."""
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def manager(
    event_store: InMemoryEventStore,
    event_bus: InMemoryEventBus,
    checkpoint_repo: InMemoryCheckpointRepository,
) -> SubscriptionManager:
    """Create a SubscriptionManager."""
    return SubscriptionManager(event_store, event_bus, checkpoint_repo)


async def add_events_to_store(
    store: InMemoryEventStore,
    count: int,
) -> list[DomainEvent]:
    """Helper to add events to the store."""
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = PauseTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="PauseAggregate",
            events=[event],
            expected_version=0,
        )
        events.append(event)
    return events


# --- Manager Pause Subscription Tests ---


class TestManagerPauseSubscription:
    """Tests for SubscriptionManager.pause_subscription()."""

    @pytest.mark.asyncio
    async def test_pause_running_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pausing a running subscription."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.LIVE

        result = await manager.pause_subscription("MockSubscriber")

        assert result is True
        assert subscription.state == SubscriptionState.PAUSED

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_with_reason(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pausing with specific reason."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        result = await manager.pause_subscription(
            "MockSubscriber",
            reason=PauseReason.MAINTENANCE,
        )

        subscription = manager.get_subscription("MockSubscriber")
        assert result is True
        assert subscription.pause_reason == PauseReason.MAINTENANCE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_nonexistent_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        """Test pausing nonexistent subscription returns False."""
        result = await manager.pause_subscription("NonExistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_pause_not_running_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        """Test pausing subscription that is not running returns False."""
        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        # Not started, so not running

        result = await manager.pause_subscription("MockSubscriber")
        assert result is False

    @pytest.mark.asyncio
    async def test_pause_already_paused_returns_false(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pausing already paused subscription returns False."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")

        # Try to pause again
        result = await manager.pause_subscription("MockSubscriber")
        assert result is False

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_maintains_position(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pause maintains checkpoint position."""
        await add_events_to_store(event_store, 10)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        position_before = subscription.last_processed_position

        await manager.pause_subscription("MockSubscriber")

        assert subscription.last_processed_position == position_before

        await manager.stop()


# --- Manager Resume Subscription Tests ---


class TestManagerResumeSubscription:
    """Tests for SubscriptionManager.resume_subscription()."""

    @pytest.mark.asyncio
    async def test_resume_paused_subscription(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test resuming a paused subscription."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")
        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.PAUSED

        result = await manager.resume_subscription("MockSubscriber")

        assert result is True
        assert subscription.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_nonexistent_subscription_returns_false(
        self,
        manager: SubscriptionManager,
    ):
        """Test resuming nonexistent subscription returns False."""
        result = await manager.resume_subscription("NonExistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_resume_not_paused_subscription_returns_false(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test resuming subscription that is not paused returns False."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        # Not paused
        result = await manager.resume_subscription("MockSubscriber")
        assert result is False

        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_continues_from_position(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test resume continues from paused position."""
        await add_events_to_store(event_store, 10)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        subscription = manager.get_subscription("MockSubscriber")
        position_before = subscription.last_processed_position

        await manager.pause_subscription("MockSubscriber")
        await manager.resume_subscription("MockSubscriber")

        # Position should be preserved (may be same or higher if events arrived)
        assert subscription.last_processed_position >= position_before

        await manager.stop()


# --- Manager Pause All Tests ---


class TestManagerPauseAll:
    """Tests for SubscriptionManager.pause_all()."""

    @pytest.mark.asyncio
    async def test_pause_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pausing all running subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        results = await manager.pause_all()

        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is True

        order_sub = manager.get_subscription("OrderProjection")
        customer_sub = manager.get_subscription("CustomerProjection")
        assert order_sub.state == SubscriptionState.PAUSED
        assert customer_sub.state == SubscriptionState.PAUSED

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_with_reason(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pause_all with specific reason."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_all(reason=PauseReason.BACKPRESSURE)

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.pause_reason == PauseReason.BACKPRESSURE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_skips_non_running(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pause_all skips subscriptions not in running state."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)

        # Only start OrderProjection
        await manager.start(subscription_names=["OrderProjection"])

        results = await manager.pause_all()

        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is False

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_all_empty_manager(
        self,
        manager: SubscriptionManager,
    ):
        """Test pause_all with no subscriptions."""
        results = await manager.pause_all()
        assert results == {}


# --- Manager Resume All Tests ---


class TestManagerResumeAll:
    """Tests for SubscriptionManager.resume_all()."""

    @pytest.mark.asyncio
    async def test_resume_all_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test resuming all paused subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        await manager.pause_all()

        results = await manager.resume_all()

        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is True

        order_sub = manager.get_subscription("OrderProjection")
        customer_sub = manager.get_subscription("CustomerProjection")
        assert order_sub.state == SubscriptionState.LIVE
        assert customer_sub.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_all_skips_non_paused(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test resume_all skips subscriptions not in paused state."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        # Only pause OrderProjection
        await manager.pause_subscription("OrderProjection")

        results = await manager.resume_all()

        assert results["OrderProjection"] is True
        assert results["CustomerProjection"] is False

        await manager.stop()

    @pytest.mark.asyncio
    async def test_resume_all_empty_manager(
        self,
        manager: SubscriptionManager,
    ):
        """Test resume_all with no subscriptions."""
        results = await manager.resume_all()
        assert results == {}


# --- Paused Subscriptions Properties Tests ---


class TestPausedSubscriptionsProperties:
    """Tests for paused_subscriptions and paused_subscription_names properties."""

    @pytest.mark.asyncio
    async def test_paused_subscriptions_property(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test paused_subscriptions returns paused subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        # Pause only OrderProjection
        await manager.pause_subscription("OrderProjection")

        paused = manager.paused_subscriptions
        assert len(paused) == 1
        assert paused[0].name == "OrderProjection"

        await manager.stop()

    @pytest.mark.asyncio
    async def test_paused_subscriptions_empty_when_none_paused(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test paused_subscriptions is empty when no subscriptions are paused."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        paused = manager.paused_subscriptions
        assert len(paused) == 0

        await manager.stop()

    @pytest.mark.asyncio
    async def test_paused_subscription_names_property(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test paused_subscription_names returns names of paused subscriptions."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        await manager.pause_all()

        names = manager.paused_subscription_names
        assert set(names) == {"OrderProjection", "CustomerProjection"}

        await manager.stop()


# --- Pause/Resume Lifecycle Tests ---


class TestPauseResumeLifecycle:
    """Tests for pause/resume lifecycle scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_pause_resume_cycles(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test multiple pause/resume cycles."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        for _ in range(5):
            await manager.pause_subscription("MockSubscriber")
            subscription = manager.get_subscription("MockSubscriber")
            assert subscription.state == SubscriptionState.PAUSED

            await manager.resume_subscription("MockSubscriber")
            assert subscription.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_pause_does_not_affect_other_subscriptions(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test pausing one subscription does not affect others."""
        await add_events_to_store(event_store, 5)

        order_projection = OrderProjection()
        customer_projection = CustomerProjection()

        await manager.subscribe(order_projection)
        await manager.subscribe(customer_projection)
        await manager.start()

        await manager.pause_subscription("OrderProjection")

        order_sub = manager.get_subscription("OrderProjection")
        customer_sub = manager.get_subscription("CustomerProjection")

        assert order_sub.state == SubscriptionState.PAUSED
        assert customer_sub.state == SubscriptionState.LIVE

        await manager.stop()

    @pytest.mark.asyncio
    async def test_stop_while_paused(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test stopping manager while subscriptions are paused."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")

        # Stop should work even when paused
        await manager.stop()

        subscription = manager.get_subscription("MockSubscriber")
        assert subscription.state == SubscriptionState.STOPPED


# --- Health Status with Paused Subscriptions ---


class TestHealthStatusWithPaused:
    """Tests for health status when subscriptions are paused."""

    @pytest.mark.asyncio
    async def test_health_shows_paused_state(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test health status includes paused state."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")

        health = manager.get_health()

        assert health["subscriptions"][0]["state"] == "paused"

        await manager.stop()

    @pytest.mark.asyncio
    async def test_subscription_status_shows_paused(
        self,
        manager: SubscriptionManager,
        event_store: InMemoryEventStore,
    ):
        """Test subscription status shows paused state."""
        await add_events_to_store(event_store, 5)

        subscriber = MockSubscriber()
        await manager.subscribe(subscriber)
        await manager.start()

        await manager.pause_subscription("MockSubscriber")

        statuses = manager.get_all_statuses()
        status = statuses["MockSubscriber"]

        assert status.state == "paused"

        await manager.stop()
