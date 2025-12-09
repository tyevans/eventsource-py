"""
Unit tests for Subscriber protocol and base classes.

Tests cover:
- Subscriber protocol runtime checking
- SyncSubscriber protocol runtime checking
- BatchSubscriber protocol runtime checking
- BaseSubscriber abstract class
- BatchAwareSubscriber with batch fallback
- FilteringSubscriber with event filtering
- Utility functions for subscriber introspection
"""

from collections.abc import Sequence
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.subscriptions.subscriber import (
    BaseSubscriber,
    BatchAwareSubscriber,
    BatchSubscriber,
    FilteringSubscriber,
    Subscriber,
    SyncSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)


# Sample events for testing
class OrderCreated(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str = Field(default="", description="Cancellation reason")


class TestSubscriberProtocol:
    """Tests for Subscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """Subscriber protocol supports isinstance checks."""

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, Subscriber)

    def test_missing_subscribed_to_fails_check(self) -> None:
        """Object without subscribed_to fails protocol check."""

        class IncompleteSubscriber:
            async def handle(self, event: DomainEvent) -> None:
                pass

        obj = IncompleteSubscriber()
        assert not isinstance(obj, Subscriber)

    def test_missing_handle_fails_check(self) -> None:
        """Object without handle fails protocol check."""

        class IncompleteSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

        obj = IncompleteSubscriber()
        assert not isinstance(obj, Subscriber)

    def test_non_subscriber_fails_check(self) -> None:
        """Arbitrary object fails protocol check."""

        class NotASubscriber:
            def some_method(self) -> None:
                pass

        obj = NotASubscriber()
        assert not isinstance(obj, Subscriber)

    @pytest.mark.asyncio
    async def test_subscriber_handles_event(self) -> None:
        """Subscriber can handle events."""
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
        """subscribed_to returns expected event types."""

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        types = subscriber.subscribed_to()

        assert len(types) == 2
        assert OrderCreated in types
        assert OrderShipped in types


class TestSyncSubscriberProtocol:
    """Tests for SyncSubscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """SyncSubscriber protocol supports isinstance checks."""

        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySyncSubscriber()
        assert isinstance(subscriber, SyncSubscriber)

    def test_async_subscriber_matches_sync_protocol(self) -> None:
        """Async subscriber also matches SyncSubscriber (method exists)."""

        class AsyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        # Note: Runtime protocol check only verifies method existence
        subscriber = AsyncSubscriber()
        assert isinstance(subscriber, SyncSubscriber)

    def test_sync_subscriber_handles_event(self) -> None:
        """Sync subscriber can handle events synchronously."""
        handled_events: list[DomainEvent] = []

        class MySyncSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

        subscriber = MySyncSubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        subscriber.handle(event)

        assert len(handled_events) == 1


class TestBatchSubscriberProtocol:
    """Tests for BatchSubscriber protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """BatchSubscriber protocol supports isinstance checks."""

        class MyBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                pass

        subscriber = MyBatchSubscriber()
        assert isinstance(subscriber, BatchSubscriber)

    def test_non_batch_subscriber_fails_check(self) -> None:
        """Subscriber without handle_batch fails BatchSubscriber check."""

        class NonBatchSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = NonBatchSubscriber()
        assert not isinstance(subscriber, BatchSubscriber)

    @pytest.mark.asyncio
    async def test_batch_subscriber_handles_batch(self) -> None:
        """BatchSubscriber processes batch of events."""
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
        """BatchSubscriber handles empty batch gracefully."""
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
        """Detects subscribers with handle_batch method."""

        class BatchCapable:
            async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
                pass

        assert supports_batch_handling(BatchCapable())

    def test_rejects_non_batch(self) -> None:
        """Returns False for subscribers without handle_batch."""

        class NonBatch:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert not supports_batch_handling(NonBatch())

    def test_rejects_non_callable(self) -> None:
        """Returns False when handle_batch is not callable."""

        class FakeSubscriber:
            handle_batch = "not a method"

        assert not supports_batch_handling(FakeSubscriber())


class TestGetSubscribedEventTypes:
    """Tests for get_subscribed_event_types utility function."""

    def test_returns_event_types(self) -> None:
        """Returns event types from subscribed_to."""

        class MySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        types = get_subscribed_event_types(MySubscriber())

        assert len(types) == 2
        assert OrderCreated in types
        assert OrderShipped in types

    def test_returns_empty_list(self) -> None:
        """Handles subscriber with no subscriptions."""

        class EmptySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return []

            async def handle(self, event: DomainEvent) -> None:
                pass

        types = get_subscribed_event_types(EmptySubscriber())
        assert types == []

    def test_raises_on_invalid_return(self) -> None:
        """Raises ValueError when subscribed_to returns non-list."""

        class InvalidSubscriber:
            def subscribed_to(self) -> tuple:  # type: ignore[override]
                return (OrderCreated,)

            async def handle(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(ValueError, match="must return a list"):
            get_subscribed_event_types(InvalidSubscriber())  # type: ignore


class TestBaseSubscriber:
    """Tests for BaseSubscriber abstract class."""

    def test_cannot_instantiate_directly(self) -> None:
        """BaseSubscriber cannot be instantiated directly."""
        with pytest.raises(TypeError, match="abstract"):
            BaseSubscriber()  # type: ignore[abstract]

    def test_subclass_must_implement_subscribed_to(self) -> None:
        """Subclass missing subscribed_to raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(BaseSubscriber):
                async def handle(self, event: DomainEvent) -> None:
                    pass

            IncompleteSubscriber()

    def test_subclass_must_implement_handle(self) -> None:
        """Subclass missing handle raises TypeError."""

        with pytest.raises(TypeError, match="abstract"):

            class IncompleteSubscriber(BaseSubscriber):
                def subscribed_to(self) -> list[type[DomainEvent]]:
                    return [OrderCreated]

            IncompleteSubscriber()

    @pytest.mark.asyncio
    async def test_complete_subclass_works(self) -> None:
        """Complete BaseSubscriber subclass works correctly."""
        handled: list[DomainEvent] = []

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                handled.append(event)

        subscriber = MySubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        await subscriber.handle(event)

        assert len(handled) == 1

    def test_can_handle_returns_true_for_subscribed_types(self) -> None:
        """can_handle returns True for event types in subscribed_to."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        assert subscriber.can_handle(event) is True

    def test_can_handle_returns_false_for_other_types(self) -> None:
        """can_handle returns False for non-subscribed event types."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        assert subscriber.can_handle(event) is False

    def test_repr_includes_event_types(self) -> None:
        """repr shows subscribed event types."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated, OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        repr_str = repr(subscriber)

        assert "MySubscriber" in repr_str
        assert "OrderCreated" in repr_str
        assert "OrderShipped" in repr_str

    def test_matches_subscriber_protocol(self) -> None:
        """BaseSubscriber subclass matches Subscriber protocol."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, Subscriber)


class TestBatchAwareSubscriber:
    """Tests for BatchAwareSubscriber base class."""

    def test_cannot_instantiate_directly(self) -> None:
        """BatchAwareSubscriber requires subclass implementation."""
        with pytest.raises(TypeError, match="abstract"):
            BatchAwareSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_handle_batch_calls_handle_by_default(self) -> None:
        """Default handle_batch calls handle for each event."""
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
        """Subclass can override handle_batch for bulk processing."""
        batch_calls: list[int] = []

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                # Should not be called when handle_batch is overridden
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
        """handle_batch handles empty list gracefully."""
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
        """handle_batch_with_error_tracking reports success count."""
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
        """handle_batch_with_error_tracking tracks failures."""

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
        """handle_batch_with_error_tracking handles all failures."""

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
        """BatchAwareSubscriber matches BatchSubscriber protocol."""

        class MySubscriber(BatchAwareSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, BatchSubscriber)


class TestFilteringSubscriber:
    """Tests for FilteringSubscriber base class."""

    def test_cannot_instantiate_directly(self) -> None:
        """FilteringSubscriber requires subclass implementation."""
        with pytest.raises(TypeError, match="abstract"):
            FilteringSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_filters_events_with_should_handle(self) -> None:
        """Events are filtered using should_handle."""
        processed: list[DomainEvent] = []

        class MySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                if isinstance(event, OrderCreated):
                    return event.order_number.startswith("VIP")
                return False

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = MySubscriber()

        vip_event = OrderCreated(aggregate_id=uuid4(), order_number="VIP-001")
        regular_event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        await subscriber.handle(vip_event)
        await subscriber.handle(regular_event)

        assert len(processed) == 1
        assert processed[0] == vip_event

    @pytest.mark.asyncio
    async def test_default_should_handle_uses_can_handle(self) -> None:
        """Default should_handle delegates to can_handle."""
        processed: list[DomainEvent] = []

        class MySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = MySubscriber()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await subscriber.handle(created)
        await subscriber.handle(shipped)

        # Only OrderCreated should be processed (in subscribed_to)
        assert len(processed) == 1
        assert isinstance(processed[0], OrderCreated)

    @pytest.mark.asyncio
    async def test_handle_batch_applies_filter(self) -> None:
        """handle_batch filters events before processing."""
        processed: list[DomainEvent] = []

        class VIPOnlySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                if isinstance(event, OrderCreated):
                    return event.order_number.startswith("VIP")
                return False

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = VIPOnlySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="VIP-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="VIP-002"),
        ]

        await subscriber.handle_batch(events)

        assert len(processed) == 2
        assert all(
            isinstance(e, OrderCreated) and e.order_number.startswith("VIP") for e in processed
        )

    @pytest.mark.asyncio
    async def test_filter_by_tenant(self) -> None:
        """Demonstrates tenant-based filtering."""
        processed: list[DomainEvent] = []
        target_tenant = uuid4()

        class TenantSubscriber(FilteringSubscriber):
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                return event.tenant_id == self.tenant_id

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = TenantSubscriber(target_tenant)

        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", tenant_id=target_tenant),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002", tenant_id=uuid4()),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-003", tenant_id=target_tenant),
        ]

        for event in events:
            await subscriber.handle(event)

        assert len(processed) == 2
        assert all(e.tenant_id == target_tenant for e in processed)


class TestSubscriberProtocolCompatibility:
    """Tests for protocol compatibility with existing patterns."""

    def test_compatible_with_event_subscriber(self) -> None:
        """Subscriber is compatible with existing EventSubscriber pattern."""
        from eventsource.protocols import EventSubscriber

        class MyProjection(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        projection = MyProjection()
        # Should match both protocols
        assert isinstance(projection, Subscriber)
        assert isinstance(projection, EventSubscriber)

    def test_base_subscriber_matches_event_subscriber(self) -> None:
        """BaseSubscriber subclass matches EventSubscriber pattern."""

        class MySubscriber(BaseSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = MySubscriber()
        # Runtime protocol check
        assert isinstance(subscriber, Subscriber)
        # Note: EventSubscriber is an ABC, so BaseSubscriber is not a subclass,
        # but the protocol interface is compatible


class TestSubscriberDuckTyping:
    """Tests for duck typing with subscriber protocols."""

    @pytest.mark.asyncio
    async def test_function_accepting_subscriber(self) -> None:
        """Function can accept any Subscriber implementation."""

        async def process_events(subscriber: Subscriber, events: list[DomainEvent]) -> int:
            count = 0
            for event in events:
                if type(event) in subscriber.subscribed_to():
                    await subscriber.handle(event)
                    count += 1
            return count

        class OrderProjection:
            def __init__(self):
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
        """Function can use BatchSubscriber for bulk processing."""

        async def process_batch(subscriber: BatchSubscriber, events: Sequence[DomainEvent]) -> None:
            await subscriber.handle_batch(events)

        class BulkProjection:
            def __init__(self):
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
