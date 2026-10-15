"""Tests for BatchSubscriber protocol and BatchAwareSubscriber base class."""

from collections.abc import Sequence
from uuid import uuid4

import pytest

from eventsource.application.subscriptions.subscriber import (
    BatchAwareSubscriber,
    BatchSubscriber,
    supports_batch_handling,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_subscriber._fixtures import OrderCreated


class TestBatchSubscriberProtocol:
    """Tests for BatchSubscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        class MyBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                pass

        assert isinstance(MyBatchSubscriber(), BatchSubscriber)

    def test_non_batch_subscriber_fails_check(self) -> None:
        class NonBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert not isinstance(NonBatchSubscriber(), BatchSubscriber)

    @pytest.mark.asyncio
    async def test_batch_subscriber_handles_batch(self) -> None:
        handled_batches: list[list[DomainEvent]] = []

        class MyBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                handled_batches.append(list(events))

        subscriber = MyBatchSubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
        ]
        await subscriber.handle_batch(events)
        assert len(handled_batches) == 1
        assert len(handled_batches[0]) == 2

    @pytest.mark.asyncio
    async def test_batch_subscriber_handles_empty_batch(self) -> None:
        call_count = 0

        class MyBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                nonlocal call_count
                call_count += 1

        subscriber = MyBatchSubscriber()
        await subscriber.handle_batch([])
        assert call_count == 1


class TestSupportsBatchHandling:
    """Tests for supports_batch_handling utility function."""

    def test_detects_batch_support(self) -> None:
        class BatchCapable:
            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                pass

        assert supports_batch_handling(BatchCapable())

    def test_rejects_non_batch(self) -> None:
        class NonBatch:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert not supports_batch_handling(NonBatch())

    def test_rejects_non_callable(self) -> None:
        class FakeSubscriber:
            handle_batch = "not a method"

        assert not supports_batch_handling(FakeSubscriber())


class TestBatchAwareSubscriber:
    """Tests for BatchAwareSubscriber base class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError, match="abstract"):
            BatchAwareSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_handle_batch_calls_handle_by_default(self) -> None:
        handled: list[DomainEvent] = []

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                handled.append(event)

        subscriber = MySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-003"),
        ]
        await subscriber.handle_batch(events)
        assert len(handled) == 3

    @pytest.mark.asyncio
    async def test_handle_batch_can_be_overridden(self) -> None:
        batch_calls: list[int] = []

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                raise AssertionError("handle should not be called")

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                batch_calls.append(len(events))

        subscriber = MySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
        ]
        await subscriber.handle_batch(events)
        assert batch_calls == [2]

    @pytest.mark.asyncio
    async def test_handle_batch_with_empty_list(self) -> None:
        handled: list[DomainEvent] = []

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                handled.append(event)

        subscriber = MySubscriber()
        await subscriber.handle_batch([])
        assert len(handled) == 0

    @pytest.mark.asyncio
    async def test_handle_batch_with_error_tracking_success(self) -> None:
        handled: list[DomainEvent] = []

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                handled.append(event)

        subscriber = MySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
        ]
        success_count, failures = await subscriber.handle_batch_with_error_tracking(events)
        assert success_count == 2
        assert len(failures) == 0
        assert len(handled) == 2

    @pytest.mark.asyncio
    async def test_handle_batch_with_error_tracking_partial_failure(self) -> None:
        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                if isinstance(event, OrderCreated) and "fail" in event.order_number:
                    raise ValueError(f"Failed: {event.order_number}")

        subscriber = MySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="fail-002"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-003"),
            OrderCreated(aggregate_id=uuid4(), order_number="fail-004"),
        ]
        success_count, failures = await subscriber.handle_batch_with_error_tracking(events)
        assert success_count == 2
        assert len(failures) == 2
        assert "fail-002" in str(failures[0][1])
        assert "fail-004" in str(failures[1][1])

    @pytest.mark.asyncio
    async def test_handle_batch_with_error_tracking_all_fail(self) -> None:
        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                raise ValueError("Always fails")

        subscriber = MySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002"),
        ]
        success_count, failures = await subscriber.handle_batch_with_error_tracking(events)
        assert success_count == 0
        assert len(failures) == 2

    def test_matches_batch_subscriber_protocol(self) -> None:
        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        assert isinstance(MySubscriber(), BatchSubscriber)
