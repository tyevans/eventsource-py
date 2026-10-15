"""Unit tests extracted for test_rabbitmq_subscriptions.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus
from eventsource.domain.event import DomainEvent


class TestRabbitMQSubscriptionManagement:
    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        return RabbitMQEventBus()

    def test_subscribe_registers_handler(self, bus: RabbitMQEventBus) -> None:
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_multiple_handlers(self, bus: RabbitMQEventBus) -> None:
        handler1, handler2 = MagicMock(), MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_same_handler_twice(self, bus: RabbitMQEventBus) -> None:
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_to_different_event_types(self, bus: RabbitMQEventBus) -> None:
        class TestEvent(DomainEvent):
            pass

        bus.subscribe(DomainEvent, MagicMock())
        bus.subscribe(TestEvent, MagicMock())
        assert bus.get_subscriber_count(DomainEvent) == 1
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count() == 2

    def test_unsubscribe_removes_handler(self, bus: RabbitMQEventBus) -> None:
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        assert bus.unsubscribe(DomainEvent, handler) is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_unsubscribe_returns_false_if_not_found(self, bus: RabbitMQEventBus) -> None:
        assert bus.unsubscribe(DomainEvent, MagicMock()) is False

    def test_unsubscribe_only_removes_correct_handler(self, bus: RabbitMQEventBus) -> None:
        handler1, handler2 = MagicMock(), MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        bus.unsubscribe(DomainEvent, handler1)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_unsubscribe_removes_only_first_occurrence(self, bus: RabbitMQEventBus) -> None:
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 2
        bus.unsubscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_all(self, bus: RabbitMQEventBus) -> None:
        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        class TestSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestEvent1, TestEvent2]

            async def handle(self, event: DomainEvent) -> None:
                pass

        bus.subscribe_all(TestSubscriber())
        assert bus.get_subscriber_count(TestEvent1) == 1
        assert bus.get_subscriber_count(TestEvent2) == 1

    def test_subscribe_all_with_empty_subscribed_to(self, bus: RabbitMQEventBus) -> None:
        class EmptySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return []

            async def handle(self, event: DomainEvent) -> None:
                pass

        bus.subscribe_all(EmptySubscriber())
        assert bus.get_subscriber_count() == 0

    def test_subscribe_to_all_events(self, bus: RabbitMQEventBus) -> None:
        bus.subscribe_to_all_events(MagicMock())
        assert bus.get_wildcard_subscriber_count() == 1

    def test_subscribe_to_all_events_multiple(self, bus: RabbitMQEventBus) -> None:
        bus.subscribe_to_all_events(MagicMock())
        bus.subscribe_to_all_events(MagicMock())
        assert bus.get_wildcard_subscriber_count() == 2

    def test_unsubscribe_from_all_events(self, bus: RabbitMQEventBus) -> None:
        handler = MagicMock()
        bus.subscribe_to_all_events(handler)
        assert bus.unsubscribe_from_all_events(handler) is True
        assert bus.get_wildcard_subscriber_count() == 0

    def test_unsubscribe_from_all_events_returns_false_if_not_found(
        self, bus: RabbitMQEventBus
    ) -> None:
        assert bus.unsubscribe_from_all_events(MagicMock()) is False

    def test_unsubscribe_from_all_events_only_removes_correct_handler(
        self, bus: RabbitMQEventBus
    ) -> None:
        handler1, handler2 = MagicMock(), MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        bus.unsubscribe_from_all_events(handler1)
        assert bus.get_wildcard_subscriber_count() == 1

    def test_clear_subscribers(self, bus: RabbitMQEventBus) -> None:
        bus.subscribe(DomainEvent, MagicMock())
        bus.subscribe_to_all_events(MagicMock())
        bus.clear_subscribers()
        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_no_event_type(self, bus: RabbitMQEventBus) -> None:
        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent2, MagicMock())
        assert bus.get_subscriber_count() == 3

    def test_get_subscriber_count_with_event_type(self, bus: RabbitMQEventBus) -> None:
        class TestEvent(DomainEvent):
            pass

        bus.subscribe(TestEvent, MagicMock())
        bus.subscribe(DomainEvent, MagicMock())
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_get_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        assert bus.get_subscriber_count() == 0
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_get_wildcard_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        assert bus.get_wildcard_subscriber_count() == 0


class TestRabbitMQSubscriptionIntegration:
    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        return RabbitMQEventBus()

    def test_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        handler1, handler2 = MagicMock(), MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        assert bus.unsubscribe(DomainEvent, handler1) is True
        assert bus.get_subscriber_count(DomainEvent) == 1
        assert bus.unsubscribe(DomainEvent, handler2) is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_wildcard_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        handler1, handler2 = MagicMock(), MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        assert bus.unsubscribe_from_all_events(handler1) is True
        assert bus.get_wildcard_subscriber_count() == 1
        assert bus.unsubscribe_from_all_events(handler2) is True
        assert bus.get_wildcard_subscriber_count() == 0
