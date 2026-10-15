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
from eventsource.ports.handlers import EventSubscriber
from tests.unit.application.subscriptions.test_subscriber._fixtures import (
    OrderCreated,
    OrderShipped,
)


class _BasicSubscriber:
    def __init__(self, types: list[type[DomainEvent]] | None = None) -> None:
        self._types = types or [OrderCreated]
        self.events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return self._types

    async def handle(self, event: DomainEvent) -> None:
        self.events.append(event)


class TestSubscriberProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        assert isinstance(_BasicSubscriber(), Subscriber)

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
        sub = _BasicSubscriber([OrderCreated, OrderShipped])
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await sub.handle(event)
        assert sub.events == [event]

    def test_subscribed_to_returns_correct_types(self) -> None:
        sub = _BasicSubscriber([OrderCreated, OrderShipped])
        assert sub.subscribed_to() == [OrderCreated, OrderShipped]


class TestSyncSubscriberProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySyncSubscriber(), SyncSubscriber)

    def test_async_subscriber_matches_sync_protocol(self) -> None:
        assert isinstance(_BasicSubscriber(), SyncSubscriber)

    def test_sync_subscriber_handles_event(self) -> None:
        handled_events: list[DomainEvent] = []

        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

        MySyncSubscriber().handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(handled_events) == 1


class TestSubscriberProtocolCompatibility:
    def test_compatible_with_event_subscriber(self) -> None:
        class MyProjection(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        projection = MyProjection()
        assert isinstance(projection, Subscriber) and isinstance(projection, EventSubscriber)

    def test_base_subscriber_matches_event_subscriber(self) -> None:
        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySubscriber(), Subscriber)


class TestSubscriberDuckTyping:
    @pytest.mark.asyncio
    async def test_function_accepting_subscriber(self) -> None:
        async def process_events(subscriber: Subscriber, events: list[DomainEvent]) -> int:
            count = 0
            for event in events:
                if type(event) in subscriber.subscribed_to():
                    await subscriber.handle(event)
                    count += 1
            return count

        projection = _BasicSubscriber([OrderCreated])
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"),
        ]
        assert await process_events(projection, events) == 1 and len(projection.events) == 1

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
        assert len(projection.batches) == 1 and len(projection.batches[0]) == 2
