"""Unit tests for event handler protocols and handler types."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from tests.unit.test_event_bus._fixtures import (
    EventHandler,
    FlexibleEventSubscriber,
    OrderCreated,
    OrderShipped,
    RecordingHandler,
    SampleAsyncEventHandler,
    SelectiveSubscriber,
    SyncRecordingHandler,
)


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


class TestEventSubscriberProtocol:
    """Tests for EventSubscriber protocol compliance."""

    def test_subscriber_implements_protocol(self) -> None:
        """Test that subscriber implements FlexibleEventSubscriber protocol."""
        subscriber = SelectiveSubscriber([OrderCreated])
        assert isinstance(subscriber, FlexibleEventSubscriber)

    def test_subscribed_to_returns_event_types(self) -> None:
        """Test that subscribed_to returns correct event types."""
        subscriber = SelectiveSubscriber([OrderCreated, OrderShipped])
        event_types = subscriber.subscribed_to()
        assert len(event_types) == 2
        assert OrderCreated in event_types
        assert OrderShipped in event_types


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


class TestInvalidHandlerTypes:
    """Tests for invalid handler type detection."""

    def test_subscribe_invalid_handler_raises(self, event_bus: InMemoryEventBus) -> None:
        """Test that subscribing invalid handler raises TypeError."""
        with pytest.raises(TypeError, match=r"Handler must have a handle\(\) method"):
            event_bus.subscribe(OrderCreated, "not_a_handler")  # type: ignore[arg-type]

    def test_subscribe_to_all_invalid_handler_raises(self, event_bus: InMemoryEventBus) -> None:
        """Test that wildcard subscription with invalid handler raises TypeError."""
        with pytest.raises(TypeError, match=r"Handler must have a handle\(\) method"):
            event_bus.subscribe_to_all_events(123)  # type: ignore[arg-type]


__all__ = [
    "TestAsyncEventHandlerABC",
    "TestEventHandlerProtocol",
    "TestEventSubscriberProtocol",
    "TestInvalidHandlerTypes",
]
