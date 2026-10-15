"""Tests for Subscriber protocol and protocol compatibility."""

from collections.abc import Sequence
from uuid import uuid4

import pytest

from eventsource.application.subscriptions.subscriber import (
    BaseSubscriber,
    BatchSubscriber,
    Subscriber,
    SyncSubscriber,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_subscriber._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestSubscriberProtocol:
    """Tests for Subscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySubscriber(), Subscriber)

    def test_missing_subscribed_to_fails_check(self) -> None:
        class IncompleteSubscriber:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert not isinstance(IncompleteSubscriber(), Subscriber)

    def test_missing_handle_fails_check(self) -> None:
        class IncompleteSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

        assert not isinstance(IncompleteSubscriber(), Subscriber)

    def test_non_subscriber_fails_check(self) -> None:
        class NotASubscriber:
            def some_method(self) -> None:
                pass

        assert not isinstance(NotASubscriber(), Subscriber)

    @pytest.mark.asyncio
    async def test_subscriber_handles_event(self) -> None:
        handled_events: list[DomainEvent] = []

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

        subscriber = MySubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await subscriber.handle(event)

        assert len(handled_events) == 1
        assert handled_events[0] == event

    def test_subscribed_to_returns_correct_types(self) -> None:
        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        types = MySubscriber().subscribed_to()
        assert len(types) == 2
        assert OrderCreated in types
        assert OrderShipped in types


class TestSyncSubscriberProtocol:
    """Tests for SyncSubscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySyncSubscriber(), SyncSubscriber)

    def test_async_subscriber_matches_sync_protocol(self) -> None:
        class AsyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(AsyncSubscriber(), SyncSubscriber)

    def test_sync_subscriber_handles_event(self) -> None:
        handled_events: list[DomainEvent] = []

        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

        subscriber = MySyncSubscriber()
        subscriber.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(handled_events) == 1


class TestSubscriberProtocolCompatibility:
    """Tests for protocol compatibility with existing patterns."""

    def test_compatible_with_event_subscriber(self) -> None:
        from eventsource.ports.handlers import EventSubscriber

        class MyProjection(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        projection = MyProjection()
        assert isinstance(projection, Subscriber)
        assert isinstance(projection, EventSubscriber)

    def test_base_subscriber_matches_event_subscriber(self) -> None:
        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySubscriber(), Subscriber)


class TestSubscriberDuckTyping:
    """Tests for duck typing with subscriber protocols."""

    @pytest.mark.asyncio
    async def test_function_accepting_subscriber(self) -> None:
        async def process_events(subscriber: Subscriber, events: list[DomainEvent]) -> int:
            count = 0
            for event in events:
                if type(event) in subscriber.subscribed_to():
                    await subscriber.handle(event)
                    count += 1
            return count

        class OrderProjection:
            def __init__(self) -> None:
                self.events: list[DomainEvent] = []

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                self.events.append(event)

        projection = OrderProjection()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"),
        ]
        count = await process_events(projection, events)
        assert count == 1
        assert len(projection.events) == 1

    @pytest.mark.asyncio
    async def test_function_accepting_batch_subscriber(self) -> None:
        async def process_batch(subscriber: BatchSubscriber, events: Sequence[DomainEvent]) -> None:
            await subscriber.handle_batch(events)

        class BulkProjection:
            def __init__(self) -> None:
                self.batches: list[list[DomainEvent]] = []

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                self.batches.append(list(events))

        projection = BulkProjection()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
        ]
        await process_batch(projection, events)
        assert len(projection.batches) == 1
        assert len(projection.batches[0]) == 2
