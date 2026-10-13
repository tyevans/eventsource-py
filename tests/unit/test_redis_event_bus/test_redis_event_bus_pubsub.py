"""Subscription, dispatch, and pubsub integration tests for RedisEventBus."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.redis.bus import (
    RedisEventBus,
    RedisEventBusConfig,
)
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import HandlerDispatchError

from ._fixtures import (
    FailingHandler,
    OrderHandler,
    SampleOrderCreated,
    SampleOrderShipped,
    SamplePaymentReceived,
    SampleSubscriber,
    SyncHandler,
    bus,
    config,
    event_registry,
    mock_redis,
)

__all__ = [
    "TestRedisEventBusDispatch",
    "TestRedisEventBusIntegration",
    "TestRedisEventBusSubscription",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
]


class TestRedisEventBusSubscription:
    """Tests for subscription management."""

    async def test_subscribe(self, bus: RedisEventBus) -> None:
        """Test subscribing a handler to an event type."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1

    async def test_subscribe_multiple_handlers(self, bus: RedisEventBus) -> None:
        """Test subscribing multiple handlers to the same event type."""
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)

        assert bus.get_subscriber_count(SampleOrderCreated) == 2

    async def test_subscribe_to_multiple_events(self, bus: RedisEventBus) -> None:
        """Test subscribing to multiple event types."""
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)
        bus.subscribe(SampleOrderShipped, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1
        assert bus.get_subscriber_count() == 2

    async def test_unsubscribe(self, bus: RedisEventBus) -> None:
        """Test unsubscribing a handler."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        result = bus.unsubscribe(SampleOrderCreated, handler)

        assert result is True
        assert bus.get_subscriber_count(SampleOrderCreated) == 0

    async def test_unsubscribe_not_found(self, bus: RedisEventBus) -> None:
        """Test unsubscribing a handler that wasn't subscribed."""
        handler = OrderHandler()
        result = bus.unsubscribe(SampleOrderCreated, handler)

        assert result is False

    async def test_subscribe_all(self, bus: RedisEventBus) -> None:
        """Test subscribing an EventSubscriber to all its events."""
        subscriber = SampleSubscriber()
        bus.subscribe_all(subscriber)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1

    async def test_subscribe_to_all_events(self, bus: RedisEventBus) -> None:
        """Test wildcard subscription."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)

        assert bus.get_wildcard_subscriber_count() == 1

    async def test_unsubscribe_from_all_events(self, bus: RedisEventBus) -> None:
        """Test unsubscribing from wildcard subscription."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)
        result = bus.unsubscribe_from_all_events(handler)

        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    async def test_clear_subscribers(self, bus: RedisEventBus) -> None:
        """Test clearing all subscribers."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        bus.subscribe_to_all_events(handler)

        bus.clear_subscribers()

        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0


class TestRedisEventBusDispatch:
    """Tests for event dispatching."""

    async def test_dispatch_to_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test dispatching event to handler."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1
        assert handler.handled_events[0].order_number == "ORD-001"

    async def test_dispatch_to_sync_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test dispatching event to sync handler."""
        handler = SyncHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1

    async def test_dispatch_to_wildcard_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test dispatching event to wildcard handler."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)

        event = SamplePaymentReceived(
            aggregate_id=uuid4(),
            amount=100.0,
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1

    async def test_dispatch_to_multiple_handlers(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test dispatching event to multiple handlers."""
        handler1 = OrderHandler()
        handler2 = OrderHandler()
        wildcard_handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)
        bus.subscribe_to_all_events(wildcard_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler1.handled_events) == 1
        assert len(handler2.handled_events) == 1
        assert len(wildcard_handler.handled_events) == 1

    async def test_dispatch_handler_failure(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test that handler failure raises exception."""
        handler = FailingHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(event, "msg-123")

        assert len(exc_info.value.failures) == 1
        failed_name, failed_exc = exc_info.value.failures[0]
        assert isinstance(failed_exc, ValueError)
        assert "Handler failed intentionally" in str(failed_exc)

    async def test_dispatch_handler_failure_updates_stats(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """Test that handler failure updates statistics."""
        handler = FailingHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        initial_errors = bus.stats.handler_errors

        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, "msg-123")

        assert bus.stats.handler_errors == initial_errors + 1

    async def test_dispatch_isolates_handler_errors(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """A failing handler must not prevent other handlers from running.

        All three handlers should be invoked even though the middle one
        raises; the failure is reported once dispatch has run every handler,
        via an aggregate HandlerDispatchError (not the underlying exception
        directly), so the message is still left unacked for redelivery.
        """
        handler1 = OrderHandler()
        failing_handler = FailingHandler()
        handler3 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, failing_handler)
        bus.subscribe(SampleOrderCreated, handler3)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        initial_errors = bus.stats.handler_errors

        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(event, "msg-123")

        assert len(handler1.handled_events) == 1
        assert len(handler3.handled_events) == 1
        assert len(exc_info.value.failures) == 1
        assert bus.stats.handler_errors == initial_errors + 1


class TestRedisEventBusIntegration:
    """Integration-like tests using mocks."""

    async def test_full_publish_consume_cycle(
        self,
        config: RedisEventBusConfig,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ) -> None:
        """Test a full publish-consume cycle."""
        handler = OrderHandler()

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus_instance = RedisEventBus(config=config, event_registry=event_registry)
            await bus_instance.connect()

            # Subscribe
            bus_instance.subscribe(SampleOrderCreated, handler)

            # Publish
            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
            )
            await bus_instance.publish([event])

            # Simulate consuming the event
            message_data = {
                "event_id": str(event.event_id),
                "event_type": "SampleOrderCreated",
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": "Order",
                "occurred_at": datetime.now(UTC).isoformat(),
                "payload": event.model_dump_json(),
            }
            await bus_instance._process_message("msg-123", message_data, "test_consumer")

            # Verify
            assert len(handler.handled_events) == 1
            assert handler.handled_events[0].order_number == "ORD-001"
            assert bus_instance.stats.events_published == 1
            assert bus_instance.stats.events_consumed == 1

            await bus_instance.disconnect()

    async def test_subscriber_receives_events(
        self,
        config: RedisEventBusConfig,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ) -> None:
        """Test that EventSubscriber receives events."""
        subscriber = SampleSubscriber()

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus_instance = RedisEventBus(config=config, event_registry=event_registry)
            await bus_instance.connect()

            bus_instance.subscribe_all(subscriber)

            # Simulate consuming events
            event1 = SampleOrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
            )
            event2 = SampleOrderShipped(
                aggregate_id=uuid4(),
                tracking_number="TRK-001",
            )

            for event in [event1, event2]:
                message_data = {
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_id": str(event.aggregate_id),
                    "aggregate_type": event.aggregate_type,
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "payload": event.model_dump_json(),
                }
                await bus_instance._process_message(
                    f"msg-{event.event_id}", message_data, "test_consumer"
                )

            assert len(subscriber.handled_events) == 2
            assert subscriber.handled_events[0].event_type == "SampleOrderCreated"
            assert subscriber.handled_events[1].event_type == "SampleOrderShipped"

            await bus_instance.disconnect()
