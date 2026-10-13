"""Unit tests extracted for test_rabbitmq_subscriptions.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
)
from eventsource.domain.event import DomainEvent


class TestRabbitMQSubscriptionManagement:
    """Tests for subscription management in RabbitMQEventBus (P1-009)."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a bus instance for testing."""
        return RabbitMQEventBus()

    def test_subscribe_registers_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribe registers a handler for an event type."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_multiple_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing multiple handlers to the same event type."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_same_handler_twice(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribing the same handler twice adds it twice."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        # Same handler can be added multiple times
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_to_different_event_types(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing handlers to different event types."""

        # Create a test event class
        class TestEvent(DomainEvent):
            pass

        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(TestEvent, handler2)
        assert bus.get_subscriber_count(DomainEvent) == 1
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count() == 2

    def test_unsubscribe_removes_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe removes a handler."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        result = bus.unsubscribe(DomainEvent, handler)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_unsubscribe_returns_false_if_not_found(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe returns False if handler not found."""
        handler = MagicMock()
        result = bus.unsubscribe(DomainEvent, handler)
        assert result is False

    def test_unsubscribe_only_removes_correct_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe only removes the specified handler."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        bus.unsubscribe(DomainEvent, handler1)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_unsubscribe_removes_only_first_occurrence(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe removes only the first occurrence of a handler."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 2
        bus.unsubscribe(DomainEvent, handler)
        # Only one instance should be removed
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_all(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_all subscribes to all declared event types."""

        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        class TestSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestEvent1, TestEvent2]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = TestSubscriber()
        bus.subscribe_all(subscriber)
        assert bus.get_subscriber_count(TestEvent1) == 1
        assert bus.get_subscriber_count(TestEvent2) == 1

    def test_subscribe_all_with_empty_subscribed_to(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_all with subscriber that returns empty list."""

        class EmptySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return []

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = EmptySubscriber()
        bus.subscribe_all(subscriber)
        assert bus.get_subscriber_count() == 0

    def test_subscribe_to_all_events(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_to_all_events adds to wildcard handlers."""
        handler = MagicMock()
        bus.subscribe_to_all_events(handler)
        assert bus.get_wildcard_subscriber_count() == 1

    def test_subscribe_to_all_events_multiple(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing multiple wildcard handlers."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        assert bus.get_wildcard_subscriber_count() == 2

    def test_unsubscribe_from_all_events(self, bus: RabbitMQEventBus) -> None:
        """Test unsubscribe_from_all_events removes wildcard handler."""
        handler = MagicMock()
        bus.subscribe_to_all_events(handler)
        result = bus.unsubscribe_from_all_events(handler)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    def test_unsubscribe_from_all_events_returns_false_if_not_found(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test unsubscribe_from_all_events returns False if not found."""
        handler = MagicMock()
        result = bus.unsubscribe_from_all_events(handler)
        assert result is False

    def test_unsubscribe_from_all_events_only_removes_correct_handler(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test unsubscribe_from_all_events only removes specified handler."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        bus.unsubscribe_from_all_events(handler1)
        assert bus.get_wildcard_subscriber_count() == 1

    def test_clear_subscribers(self, bus: RabbitMQEventBus) -> None:
        """Test clear_subscribers removes all subscriptions."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe_to_all_events(handler2)
        bus.clear_subscribers()
        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_no_event_type(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns total when no event_type provided."""

        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent2, MagicMock())
        assert bus.get_subscriber_count() == 3

    def test_get_subscriber_count_with_event_type(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns count for specific event type."""

        class TestEvent(DomainEvent):
            pass

        bus.subscribe(TestEvent, MagicMock())
        bus.subscribe(DomainEvent, MagicMock())
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_get_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns 0 when no subscribers."""
        assert bus.get_subscriber_count() == 0
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_get_wildcard_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        """Test get_wildcard_subscriber_count returns 0 when no wildcard handlers."""
        assert bus.get_wildcard_subscriber_count() == 0


class TestRabbitMQSubscriptionIntegration:
    """Integration tests for subscription management with HandlerAdapter (P1-009)."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a bus instance for testing."""
        return RabbitMQEventBus()

    def test_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe uses identity comparison via HandlerAdapter equality."""
        # Two different mock objects that might be equal
        handler1 = MagicMock()
        handler2 = MagicMock()
        # MagicMock objects are not equal by default

        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)

        # Unsubscribing handler1 should only remove handler1
        result = bus.unsubscribe(DomainEvent, handler1)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 1

        # Handler2 should still be subscribed
        result = bus.unsubscribe(DomainEvent, handler2)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_wildcard_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe_from_all_events uses identity comparison."""
        handler1 = MagicMock()
        handler2 = MagicMock()

        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)

        result = bus.unsubscribe_from_all_events(handler1)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 1

        result = bus.unsubscribe_from_all_events(handler2)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0
