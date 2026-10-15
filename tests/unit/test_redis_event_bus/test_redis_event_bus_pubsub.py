"""Subscription, dispatch, and pubsub integration tests for RedisEventBus."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig
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


def _make_order_created(order_number: str = "ORD-001") -> SampleOrderCreated:
    return SampleOrderCreated(aggregate_id=uuid4(), order_number=order_number, customer_id=uuid4())


class TestRedisEventBusSubscription:
    """Tests for subscription management."""

    async def test_subscribe(self, bus: RedisEventBus) -> None:
        bus.subscribe(SampleOrderCreated, OrderHandler())
        assert bus.get_subscriber_count(SampleOrderCreated) == 1

    async def test_subscribe_multiple_handlers(self, bus: RedisEventBus) -> None:
        bus.subscribe(SampleOrderCreated, OrderHandler())
        bus.subscribe(SampleOrderCreated, OrderHandler())
        assert bus.get_subscriber_count(SampleOrderCreated) == 2

    async def test_subscribe_to_multiple_events(self, bus: RedisEventBus) -> None:
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        bus.subscribe(SampleOrderShipped, handler)
        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1
        assert bus.get_subscriber_count() == 2

    async def test_unsubscribe(self, bus: RedisEventBus) -> None:
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        assert bus.unsubscribe(SampleOrderCreated, handler) is True
        assert bus.get_subscriber_count(SampleOrderCreated) == 0

    async def test_unsubscribe_not_found(self, bus: RedisEventBus) -> None:
        assert bus.unsubscribe(SampleOrderCreated, OrderHandler()) is False

    async def test_subscribe_all(self, bus: RedisEventBus) -> None:
        bus.subscribe_all(SampleSubscriber())
        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1

    async def test_subscribe_to_all_events(self, bus: RedisEventBus) -> None:
        bus.subscribe_to_all_events(OrderHandler())
        assert bus.get_wildcard_subscriber_count() == 1

    async def test_unsubscribe_from_all_events(self, bus: RedisEventBus) -> None:
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)
        assert bus.unsubscribe_from_all_events(handler) is True
        assert bus.get_wildcard_subscriber_count() == 0

    async def test_clear_subscribers(self, bus: RedisEventBus) -> None:
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
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        await bus._dispatch_event(_make_order_created(), "msg-123")
        assert len(handler.handled_events) == 1
        assert handler.handled_events[0].order_number == "ORD-001"

    async def test_dispatch_to_sync_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        handler = SyncHandler()
        bus.subscribe(SampleOrderCreated, handler)
        await bus._dispatch_event(_make_order_created(), "msg-123")
        assert len(handler.handled_events) == 1

    async def test_dispatch_to_wildcard_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)
        event = SamplePaymentReceived(aggregate_id=uuid4(), amount=100.0)
        await bus._dispatch_event(event, "msg-123")
        assert len(handler.handled_events) == 1

    async def test_dispatch_to_multiple_handlers(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        handler1, handler2, wildcard_handler = OrderHandler(), OrderHandler(), OrderHandler()
        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)
        bus.subscribe_to_all_events(wildcard_handler)
        await bus._dispatch_event(_make_order_created(), "msg-123")
        assert len(handler1.handled_events) == 1
        assert len(handler2.handled_events) == 1
        assert len(wildcard_handler.handled_events) == 1

    async def test_dispatch_handler_failure(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        bus.subscribe(SampleOrderCreated, FailingHandler())
        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(_make_order_created(), "msg-123")
        assert len(exc_info.value.failures) == 1
        _failed_name, failed_exc = exc_info.value.failures[0]
        assert isinstance(failed_exc, ValueError)
        assert "Handler failed intentionally" in str(failed_exc)

    async def test_dispatch_handler_failure_updates_stats(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        bus.subscribe(SampleOrderCreated, FailingHandler())
        initial_errors = bus.stats.handler_errors
        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(_make_order_created(), "msg-123")
        assert bus.stats.handler_errors == initial_errors + 1

    async def test_dispatch_isolates_handler_errors(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        """A failing handler must not prevent other handlers from running."""
        handler1, failing_handler, handler3 = OrderHandler(), FailingHandler(), OrderHandler()
        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, failing_handler)
        bus.subscribe(SampleOrderCreated, handler3)
        initial_errors = bus.stats.handler_errors
        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(_make_order_created(), "msg-123")
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
        handler = OrderHandler()
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config, event_registry=event_registry)
            await bus_instance.connect()
            bus_instance.subscribe(SampleOrderCreated, handler)

            event = _make_order_created()
            await bus_instance.publish([event])

            message_data = {
                "event_id": str(event.event_id),
                "event_type": "SampleOrderCreated",
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": "Order",
                "occurred_at": datetime.now(UTC).isoformat(),
                "payload": event.model_dump_json(),
            }
            await bus_instance._process_message("msg-123", message_data, "test_consumer")

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
        subscriber = SampleSubscriber()
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config, event_registry=event_registry)
            await bus_instance.connect()
            bus_instance.subscribe_all(subscriber)

            events = [
                _make_order_created(),
                SampleOrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"),
            ]
            for event in events:
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
