"""
Unit tests for event bus infrastructure.

Tests the EventBus interface, protocols, and InMemoryEventBus implementation.
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.bus.interface import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
)
from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent

# =============================================================================
# Test Event Classes
# =============================================================================


class OrderCreated(DomainEvent):
    """Test event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: str


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    order_number: str
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    order_number: str
    reason: str


# =============================================================================
# Test Handler Classes
# =============================================================================


class RecordingHandler:
    """Test handler that records all events it receives."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.handle_count: int = 0

    async def handle(self, event: DomainEvent) -> None:
        """Record the event."""
        self.handled_events.append(event)
        self.handle_count += 1


class SyncRecordingHandler:
    """Test handler with a synchronous handle method."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.handle_count: int = 0

    def handle(self, event: DomainEvent) -> None:
        """Record the event synchronously."""
        self.handled_events.append(event)
        self.handle_count += 1


class FailingHandler:
    """Test handler that always raises an exception."""

    def __init__(self, error_message: str = "Handler failed intentionally") -> None:
        self.error_message = error_message
        self.call_count: int = 0

    async def handle(self, event: DomainEvent) -> None:
        """Always raise an exception."""
        self.call_count += 1
        raise ValueError(self.error_message)


class SlowHandler:
    """Test handler that takes time to process."""

    def __init__(self, delay: float = 0.1) -> None:
        self.delay = delay
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with delay."""
        await asyncio.sleep(self.delay)
        self.handled_events.append(event)


class SelectiveSubscriber:
    """Test subscriber that only handles specific event types."""

    def __init__(self, event_types: list[type[DomainEvent]]) -> None:
        self._event_types = event_types
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return subscribed event types."""
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle the event."""
        self.handled_events.append(event)


class SampleAsyncEventHandler(AsyncEventHandler):
    """Sample implementation of AsyncEventHandler ABC for testing."""

    def __init__(self, event_types: list[type[DomainEvent]]) -> None:
        self._event_types = event_types
        self.handled_events: list[DomainEvent] = []

    def event_types(self) -> list[type[DomainEvent]]:
        """Return supported event types."""
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle the event."""
        self.handled_events.append(event)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh event bus for testing."""
    return InMemoryEventBus()


@pytest.fixture
def sample_order_created() -> OrderCreated:
    """Create a sample OrderCreated event."""
    return OrderCreated(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        customer_id="customer-123",
    )


@pytest.fixture
def sample_order_shipped() -> OrderShipped:
    """Create a sample OrderShipped event."""
    return OrderShipped(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        tracking_number="TRK-123456",
    )


@pytest.fixture
def sample_order_cancelled() -> OrderCancelled:
    """Create a sample OrderCancelled event."""
    return OrderCancelled(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        reason="Customer request",
    )


# =============================================================================
# Test EventHandler Protocol
# =============================================================================


class TestEventHandlerProtocol:
    """Tests for EventHandler protocol compliance."""

    def test_async_handler_implements_protocol(self) -> None:
        """Test that async handler implements EventHandler protocol."""
        handler = RecordingHandler()
        assert isinstance(handler, EventHandler)

    def test_sync_handler_implements_protocol(self) -> None:
        """Test that sync handler implements EventHandler protocol."""
        handler = SyncRecordingHandler()
        assert isinstance(handler, EventHandler)


# =============================================================================
# Test EventSubscriber Protocol
# =============================================================================


class TestEventSubscriberProtocol:
    """Tests for EventSubscriber protocol compliance."""

    def test_subscriber_implements_protocol(self) -> None:
        """Test that subscriber implements EventSubscriber protocol."""
        subscriber = SelectiveSubscriber([OrderCreated])
        assert isinstance(subscriber, EventSubscriber)

    def test_subscribed_to_returns_event_types(self) -> None:
        """Test that subscribed_to returns correct event types."""
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_types = subscriber.subscribed_to()
        assert len(event_types) == 2
        assert OrderCreated in event_types
        assert OrderShipped in event_types


# =============================================================================
# Test AsyncEventHandler ABC
# =============================================================================


class TestAsyncEventHandlerABC:
    """Tests for AsyncEventHandler abstract base class."""

    def test_can_handle_returns_true_for_supported_types(self) -> None:
        """Test can_handle returns True for supported event types."""
        handler = SampleAsyncEventHandler([OrderCreated, OrderShipped])
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id="cust-1",
        )
        assert handler.can_handle(event) is True

    def test_can_handle_returns_false_for_unsupported_types(self) -> None:
        """Test can_handle returns False for unsupported event types."""
        handler = SampleAsyncEventHandler([OrderCreated])
        event = OrderShipped(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            tracking_number="TRK-1",
        )
        assert handler.can_handle(event) is False

    @pytest.mark.asyncio
    async def test_handle_processes_event(self, sample_order_created: OrderCreated) -> None:
        """Test that handle processes the event."""
        handler = SampleAsyncEventHandler([OrderCreated])
        await handler.handle(sample_order_created)
        assert len(handler.handled_events) == 1
        assert handler.handled_events[0] == sample_order_created


# =============================================================================
# Test InMemoryEventBus - Basic Publishing
# =============================================================================


class TestInMemoryEventBusBasicPublishing:
    """Tests for basic event publishing functionality."""

    @pytest.mark.asyncio
    async def test_publish_to_subscribed_handler(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that events are published to subscribed handlers."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        await event_bus.publish([sample_order_created])

        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_publish_multiple_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test publishing multiple events in sequence."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe(OrderShipped, handler)

        await event_bus.publish([sample_order_created, sample_order_shipped])

        assert handler.handle_count == 2
        assert handler.handled_events[0] == sample_order_created
        assert handler.handled_events[1] == sample_order_shipped

    @pytest.mark.asyncio
    async def test_publish_empty_list(self, event_bus: InMemoryEventBus) -> None:
        """Test publishing empty event list does nothing."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)

        await event_bus.publish([])

        assert handler.handle_count == 0

    @pytest.mark.asyncio
    async def test_publish_to_no_handlers(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test publishing event with no registered handlers doesn't raise."""
        # Should not raise exception
        await event_bus.publish([sample_order_created])


# =============================================================================
# Test InMemoryEventBus - Multiple Handlers
# =============================================================================


class TestInMemoryEventBusMultipleHandlers:
    """Tests for multiple handler scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_event(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that multiple handlers receive the same event."""
        handler1 = RecordingHandler()
        handler2 = RecordingHandler()

        event_bus.subscribe(OrderCreated, handler1)
        event_bus.subscribe(OrderCreated, handler2)

        await event_bus.publish([sample_order_created])

        assert handler1.handle_count == 1
        assert handler2.handle_count == 1
        assert handler1.handled_events[0] == sample_order_created
        assert handler2.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_handlers_execute_concurrently(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that handlers execute concurrently."""
        handler1 = SlowHandler(delay=0.1)
        handler2 = SlowHandler(delay=0.1)

        event_bus.subscribe(OrderCreated, handler1)
        event_bus.subscribe(OrderCreated, handler2)

        # Both handlers should run in parallel, so total time should be ~0.1s not ~0.2s
        import time

        start = time.time()
        await event_bus.publish([sample_order_created])
        elapsed = time.time() - start

        assert elapsed < 0.15  # Should be around 0.1s, not 0.2s
        assert len(handler1.handled_events) == 1
        assert len(handler2.handled_events) == 1


# =============================================================================
# Test InMemoryEventBus - Error Handling
# =============================================================================


class TestInMemoryEventBusErrorHandling:
    """Tests for error handling in event bus."""

    @pytest.mark.asyncio
    async def test_handler_failure_doesnt_affect_others(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that one handler's failure doesn't prevent others from running."""
        failing_handler = FailingHandler()
        recording_handler = RecordingHandler()

        event_bus.subscribe(OrderCreated, failing_handler)
        event_bus.subscribe(OrderCreated, recording_handler)

        # Should not raise exception
        await event_bus.publish([sample_order_created])

        assert failing_handler.call_count == 1
        assert recording_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_error_statistics_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that handler errors are tracked in statistics."""
        failing_handler = FailingHandler()
        event_bus.subscribe(OrderCreated, failing_handler)

        await event_bus.publish([sample_order_created])

        stats = event_bus.get_stats()
        assert stats["handler_errors"] == 1

    @pytest.mark.asyncio
    async def test_successful_handlers_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that successful handler invocations are tracked."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        await event_bus.publish([sample_order_created])

        stats = event_bus.get_stats()
        assert stats["handlers_invoked"] == 1


# =============================================================================
# Test InMemoryEventBus - Sync and Async Handler Support
# =============================================================================


class TestInMemoryEventBusSyncAsyncSupport:
    """Tests for sync and async handler support."""

    @pytest.mark.asyncio
    async def test_sync_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that synchronous handlers work correctly."""
        handler = SyncRecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        await event_bus.publish([sample_order_created])

        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_async_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that asynchronous handlers work correctly."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        await event_bus.publish([sample_order_created])

        assert handler.handle_count == 1
        assert handler.handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_mixed_sync_async_handlers(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that sync and async handlers work together."""
        sync_handler = SyncRecordingHandler()
        async_handler = RecordingHandler()

        event_bus.subscribe(OrderCreated, sync_handler)
        event_bus.subscribe(OrderCreated, async_handler)

        await event_bus.publish([sample_order_created])

        assert sync_handler.handle_count == 1
        assert async_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_callable_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that callable (function) handlers work."""
        handled_events: list[DomainEvent] = []

        async def handler_func(event: DomainEvent) -> None:
            handled_events.append(event)

        event_bus.subscribe(OrderCreated, handler_func)

        await event_bus.publish([sample_order_created])

        assert len(handled_events) == 1
        assert handled_events[0] == sample_order_created

    @pytest.mark.asyncio
    async def test_sync_callable_handler_works(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that synchronous callable handlers work."""
        handled_events: list[DomainEvent] = []

        def handler_func(event: DomainEvent) -> None:
            handled_events.append(event)

        event_bus.subscribe(OrderCreated, handler_func)

        await event_bus.publish([sample_order_created])

        assert len(handled_events) == 1
        assert handled_events[0] == sample_order_created


# =============================================================================
# Test InMemoryEventBus - Wildcard Subscriptions
# =============================================================================


class TestInMemoryEventBusWildcardSubscriptions:
    """Tests for wildcard subscription functionality."""

    @pytest.mark.asyncio
    async def test_wildcard_subscription_receives_all_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test subscribing to all event types."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)

        await event_bus.publish([sample_order_created, sample_order_shipped])

        assert handler.handle_count == 2

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_subscription(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that wildcard and specific subscriptions both receive events."""
        wildcard_handler = RecordingHandler()
        specific_handler = RecordingHandler()

        event_bus.subscribe_to_all_events(wildcard_handler)
        event_bus.subscribe(OrderCreated, specific_handler)

        await event_bus.publish([sample_order_created])

        # Both should receive the event
        assert wildcard_handler.handle_count == 1
        assert specific_handler.handle_count == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_from_all_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
    ) -> None:
        """Test unsubscribing from wildcard subscription."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)

        # Unsubscribe
        result = event_bus.unsubscribe_from_all_events(handler)
        assert result is True

        await event_bus.publish([sample_order_created])

        assert handler.handle_count == 0

    def test_unsubscribe_from_all_events_not_found(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing handler that isn't registered."""
        handler = RecordingHandler()
        result = event_bus.unsubscribe_from_all_events(handler)
        assert result is False

    def test_get_wildcard_subscriber_count(self, event_bus: InMemoryEventBus) -> None:
        """Test getting wildcard subscriber count."""
        handler1 = RecordingHandler()
        handler2 = RecordingHandler()

        assert event_bus.get_wildcard_subscriber_count() == 0

        event_bus.subscribe_to_all_events(handler1)
        assert event_bus.get_wildcard_subscriber_count() == 1

        event_bus.subscribe_to_all_events(handler2)
        assert event_bus.get_wildcard_subscriber_count() == 2


# =============================================================================
# Test InMemoryEventBus - subscribe_all
# =============================================================================


class TestInMemoryEventBusSubscribeAll:
    """Tests for subscribe_all with EventSubscriber."""

    @pytest.mark.asyncio
    async def test_subscribe_all_with_subscriber(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test subscribe_all registers for all declared event types."""
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_bus.subscribe_all(subscriber)

        await event_bus.publish([sample_order_created])
        assert len(subscriber.handled_events) == 1

        await event_bus.publish([sample_order_shipped])
        assert len(subscriber.handled_events) == 2

    @pytest.mark.asyncio
    async def test_subscribe_all_ignores_undeclared_events(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_cancelled: OrderCancelled,
    ) -> None:
        """Test subscribe_all doesn't receive undeclared event types."""
        subscriber = SelectiveSubscriber([OrderCreated])
        event_bus.subscribe_all(subscriber)

        await event_bus.publish([sample_order_created, sample_order_cancelled])

        # Should only have received OrderCreated
        assert len(subscriber.handled_events) == 1
        assert isinstance(subscriber.handled_events[0], OrderCreated)


# =============================================================================
# Test InMemoryEventBus - Unsubscribe
# =============================================================================


class TestInMemoryEventBusUnsubscribe:
    """Tests for unsubscribe functionality."""

    @pytest.mark.asyncio
    async def test_unsubscribe_handler(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test unsubscribing a handler."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        # Verify subscribed
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1

        # Unsubscribe
        result = event_bus.unsubscribe(OrderCreated, handler)
        assert result is True

        # Verify unsubscribed
        await event_bus.publish([sample_order_created])
        assert handler.handle_count == 1  # Should not have increased

    def test_unsubscribe_handler_not_found(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing a handler that isn't registered."""
        handler = RecordingHandler()
        result = event_bus.unsubscribe(OrderCreated, handler)
        assert result is False

    def test_unsubscribe_wrong_event_type(self, event_bus: InMemoryEventBus) -> None:
        """Test unsubscribing from wrong event type."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        result = event_bus.unsubscribe(OrderShipped, handler)
        assert result is False


# =============================================================================
# Test InMemoryEventBus - Background Publishing
# =============================================================================


class TestInMemoryEventBusBackgroundPublishing:
    """Tests for background publishing functionality."""

    @pytest.mark.asyncio
    async def test_background_publish_returns_immediately(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that background publish doesn't block."""
        slow_handler = SlowHandler(delay=0.5)
        event_bus.subscribe(OrderCreated, slow_handler)

        import time

        start = time.time()
        await event_bus.publish([sample_order_created], background=True)
        elapsed = time.time() - start

        # Should return almost immediately
        assert elapsed < 0.1

        # Wait for background task to complete
        await asyncio.sleep(0.6)
        assert len(slow_handler.handled_events) == 1

    @pytest.mark.asyncio
    async def test_background_tasks_tracked(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that background tasks are tracked in statistics."""
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)

        await event_bus.publish([sample_order_created], background=True)

        stats = event_bus.get_stats()
        assert stats["background_tasks_created"] == 1

        # Wait for completion
        await asyncio.sleep(0.3)

        stats = event_bus.get_stats()
        assert stats["background_tasks_completed"] == 1

    @pytest.mark.asyncio
    async def test_get_background_task_count(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test getting active background task count."""
        slow_handler = SlowHandler(delay=0.3)
        event_bus.subscribe(OrderCreated, slow_handler)

        assert event_bus.get_background_task_count() == 0

        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1

        await asyncio.sleep(0.4)
        assert event_bus.get_background_task_count() == 0


# =============================================================================
# Test InMemoryEventBus - Subscriber Management
# =============================================================================


class TestInMemoryEventBusSubscriberManagement:
    """Tests for subscriber management utilities."""

    def test_clear_subscribers(self, event_bus: InMemoryEventBus) -> None:
        """Test clearing all subscribers."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe_to_all_events(handler)

        assert event_bus.get_subscriber_count(OrderCreated) == 1
        assert event_bus.get_wildcard_subscriber_count() == 1

        event_bus.clear_subscribers()

        assert event_bus.get_subscriber_count(OrderCreated) == 0
        assert event_bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_specific_type(self, event_bus: InMemoryEventBus) -> None:
        """Test getting subscriber count for specific event type."""
        handler1 = RecordingHandler()
        handler2 = RecordingHandler()

        assert event_bus.get_subscriber_count(OrderCreated) == 0

        event_bus.subscribe(OrderCreated, handler1)
        assert event_bus.get_subscriber_count(OrderCreated) == 1

        event_bus.subscribe(OrderCreated, handler2)
        assert event_bus.get_subscriber_count(OrderCreated) == 2

        event_bus.subscribe(OrderShipped, handler1)
        assert event_bus.get_subscriber_count(OrderCreated) == 2
        assert event_bus.get_subscriber_count(OrderShipped) == 1

    def test_get_subscriber_count_total(self, event_bus: InMemoryEventBus) -> None:
        """Test getting total subscriber count."""
        handler = RecordingHandler()

        assert event_bus.get_subscriber_count() == 0

        event_bus.subscribe(OrderCreated, handler)
        event_bus.subscribe(OrderShipped, handler)
        event_bus.subscribe(OrderCancelled, handler)

        assert event_bus.get_subscriber_count() == 3


# =============================================================================
# Test InMemoryEventBus - Shutdown
# =============================================================================


class TestInMemoryEventBusShutdown:
    """Tests for event bus shutdown functionality."""

    @pytest.mark.asyncio
    async def test_shutdown_waits_for_background_tasks(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that shutdown waits for background tasks."""
        slow_handler = SlowHandler(delay=0.2)
        event_bus.subscribe(OrderCreated, slow_handler)

        await event_bus.publish([sample_order_created], background=True)
        assert event_bus.get_background_task_count() == 1

        await event_bus.shutdown(timeout=1.0)

        assert len(slow_handler.handled_events) == 1
        assert event_bus.get_background_task_count() == 0

    @pytest.mark.asyncio
    async def test_shutdown_with_no_tasks(self, event_bus: InMemoryEventBus) -> None:
        """Test shutdown with no background tasks."""
        # Should complete immediately without error
        await event_bus.shutdown(timeout=1.0)

    @pytest.mark.asyncio
    async def test_shutdown_timeout(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that shutdown times out for long-running tasks."""
        # Create a very slow handler
        very_slow_handler = SlowHandler(delay=5.0)
        event_bus.subscribe(OrderCreated, very_slow_handler)

        await event_bus.publish([sample_order_created], background=True)

        # Shutdown with short timeout
        import time

        start = time.time()
        await event_bus.shutdown(timeout=0.1)
        elapsed = time.time() - start

        # Should timeout around 0.1s, not wait 5s
        assert elapsed < 0.5


# =============================================================================
# Test InMemoryEventBus - Statistics
# =============================================================================


class TestInMemoryEventBusStatistics:
    """Tests for event bus statistics."""

    @pytest.mark.asyncio
    async def test_events_published_counter(
        self,
        event_bus: InMemoryEventBus,
        sample_order_created: OrderCreated,
        sample_order_shipped: OrderShipped,
    ) -> None:
        """Test that events published counter is accurate."""
        handler = RecordingHandler()
        event_bus.subscribe_to_all_events(handler)

        await event_bus.publish([sample_order_created])
        await event_bus.publish([sample_order_shipped])

        stats = event_bus.get_stats()
        assert stats["events_published"] == 2

    @pytest.mark.asyncio
    async def test_get_stats_returns_copy(
        self, event_bus: InMemoryEventBus, sample_order_created: OrderCreated
    ) -> None:
        """Test that get_stats returns a copy of statistics."""
        handler = RecordingHandler()
        event_bus.subscribe(OrderCreated, handler)

        await event_bus.publish([sample_order_created])

        stats1 = event_bus.get_stats()
        stats1["events_published"] = 999  # Modify the returned dict

        stats2 = event_bus.get_stats()
        assert stats2["events_published"] == 1  # Original unchanged


# =============================================================================
# Test InMemoryEventBus - Thread Safety
# =============================================================================


class TestInMemoryEventBusThreadSafety:
    """Tests for thread safety of subscription operations."""

    def test_subscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        """Test that subscribe is thread-safe."""
        import threading

        handlers: list[RecordingHandler] = []

        def subscribe_handler() -> None:
            handler = RecordingHandler()
            handlers.append(handler)
            event_bus.subscribe(OrderCreated, handler)

        threads = [threading.Thread(target=subscribe_handler) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert event_bus.get_subscriber_count(OrderCreated) == 10

    def test_unsubscribe_from_multiple_threads(self, event_bus: InMemoryEventBus) -> None:
        """Test that unsubscribe is thread-safe."""
        import threading

        handlers = [RecordingHandler() for _ in range(10)]
        for handler in handlers:
            event_bus.subscribe(OrderCreated, handler)

        results: list[bool] = []

        def unsubscribe_handler(h: RecordingHandler) -> None:
            result = event_bus.unsubscribe(OrderCreated, h)
            results.append(result)

        threads = [threading.Thread(target=unsubscribe_handler, args=(h,)) for h in handlers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)
        assert event_bus.get_subscriber_count(OrderCreated) == 0


# =============================================================================
# Test Invalid Handler Types
# =============================================================================


class TestInvalidHandlerTypes:
    """Tests for invalid handler type detection."""

    def test_subscribe_invalid_handler_raises(self, event_bus: InMemoryEventBus) -> None:
        """Test that subscribing invalid handler raises TypeError."""
        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            event_bus.subscribe(OrderCreated, "not_a_handler")  # type: ignore[arg-type]

    def test_subscribe_to_all_invalid_handler_raises(self, event_bus: InMemoryEventBus) -> None:
        """Test that wildcard subscription with invalid handler raises TypeError."""
        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            event_bus.subscribe_to_all_events(123)  # type: ignore[arg-type]


# =============================================================================
# Test Event Ordering
# =============================================================================


class TestEventOrdering:
    """Tests for event ordering guarantees."""

    @pytest.mark.asyncio
    async def test_events_processed_in_order(self, event_bus: InMemoryEventBus) -> None:
        """Test that events are processed in order."""
        order: list[str] = []

        class OrderTrackingHandler:
            async def handle(self, event: DomainEvent) -> None:
                order.append(event.event_type)

        handler = OrderTrackingHandler()
        event_bus.subscribe_to_all_events(handler)

        events = [
            OrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id="cust-1",
            ),
            OrderShipped(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                tracking_number="TRK-1",
            ),
            OrderCancelled(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                reason="Test",
            ),
        ]

        await event_bus.publish(events)

        assert order == ["OrderCreated", "OrderShipped", "OrderCancelled"]
