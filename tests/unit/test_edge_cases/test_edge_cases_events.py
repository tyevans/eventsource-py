"""Unit tests for event store and event bus edge cases."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import (
    CategoryReadOptions,
    ExpectedVersion,
    ReadDirection,
    StreamReadOptions,
    collect,
)

# --- Test Event Classes ---


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class EdgeTestEvent(DomainEvent):
    """Simple test event."""

    aggregate_type: str = "Test"
    data: str = ""


@register_event(registry=_REGISTRY)
class SampleOrderEvent(DomainEvent):
    """Order event for testing."""

    aggregate_type: str = "Order"
    order_data: str = ""


# --- InMemoryEventStore Edge Cases ---


class TestInMemoryEventStoreEdgeCases:
    """Edge case tests for InMemoryEventStore."""

    @pytest.fixture
    def store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_timestamp_filter(self, store: InMemoryEventStore):
        """Test read_category with a from_timestamp filter."""
        aggregate_id = uuid4()
        old_timestamp = datetime.now(UTC) - timedelta(hours=1)

        event = EdgeTestEvent(aggregate_id=aggregate_id, data="test")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"),
            [event],
            ExpectedVersion.no_stream(),
        )

        # Get events stored after old_timestamp (stored_at, inclusive).
        envelopes = await collect(
            store.read_category("Test", CategoryReadOptions(from_timestamp=old_timestamp))
        )
        assert len(envelopes) == 1

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_tenant_filter(self, store: InMemoryEventStore):
        """Test read_category with a tenant_id filter."""
        aggregate_id = uuid4()
        tenant_id = uuid4()
        other_tenant = uuid4()

        event1 = EdgeTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            data="tenant1",
        )
        event2 = EdgeTestEvent(
            aggregate_id=uuid4(),
            tenant_id=other_tenant,
            data="tenant2",
        )

        await store.append(
            StreamId(aggregate_id=event1.aggregate_id, category="Test"),
            [event1],
            ExpectedVersion.no_stream(),
        )
        await store.append(
            StreamId(aggregate_id=event2.aggregate_id, category="Test"),
            [event2],
            ExpectedVersion.no_stream(),
        )

        # Filter by tenant_id
        envelopes = await collect(
            store.read_category("Test", CategoryReadOptions(tenant_id=tenant_id))
        )
        assert len(envelopes) == 1
        assert envelopes[0].event.tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_read_stream_backward(self, store: InMemoryEventStore):
        """Test reading stream in backward direction."""
        aggregate_id = uuid4()

        events = [EdgeTestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(5)]
        stream = StreamId(aggregate_id=aggregate_id, category="Test")
        await store.append(stream, events, ExpectedVersion.no_stream())

        options = StreamReadOptions(direction=ReadDirection.BACKWARD)
        envelopes = await collect(store.read_stream(stream, options))

        assert len(envelopes) == 5
        # Should be in reverse order (last event first)
        assert "event_4" in envelopes[0].event.data
        assert "event_0" in envelopes[4].event.data

    @pytest.mark.asyncio
    async def test_read_all_stream_position_calculation(self, store: InMemoryEventStore):
        """Test that stream version is correctly calculated in read_all."""
        aggregate_id = uuid4()

        events = [EdgeTestEvent(aggregate_id=aggregate_id, data=f"event_{i}") for i in range(3)]
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="Test"),
            events,
            ExpectedVersion.no_stream(),
        )

        envelopes = await collect(store.read_all())

        assert len(envelopes) == 3
        # Each envelope should have a stream version
        for envelope in envelopes:
            assert envelope.stream_version >= 0

    @pytest.mark.asyncio
    async def test_concurrent_appends(self, store: InMemoryEventStore):
        """Test concurrent appends to different aggregates."""
        aggregate_ids = [uuid4() for _ in range(10)]

        async def append_event(agg_id: UUID):
            event = EdgeTestEvent(aggregate_id=agg_id, data="concurrent")
            await store.append(
                StreamId(aggregate_id=agg_id, category="Test"), [event], ExpectedVersion.no_stream()
            )

        await asyncio.gather(*[append_event(agg_id) for agg_id in aggregate_ids])

        # Verify all events were stored
        for agg_id in aggregate_ids:
            envelopes = await collect(
                store.read_stream(StreamId(aggregate_id=agg_id, category="Test"))
            )
            assert len(envelopes) == 1


# --- InMemoryEventBus Edge Cases ---


class TestInMemoryEventBusEdgeCases:
    """Edge case tests for InMemoryEventBus."""

    @pytest.fixture
    def bus(self) -> InMemoryEventBus:
        return InMemoryEventBus()

    @pytest.mark.asyncio
    async def test_handler_exception_isolation(self, bus: InMemoryEventBus):
        """Test that one handler's exception doesn't affect others."""
        received = []

        async def failing_handler(event: DomainEvent):
            raise RuntimeError("Handler failure")

        async def working_handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, failing_handler)
        bus.subscribe(EdgeTestEvent, working_handler)

        event = EdgeTestEvent(aggregate_id=uuid4(), data="test")
        await bus.publish([event])

        # Working handler should still receive the event
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_publish_empty_event_list(self, bus: InMemoryEventBus):
        """Test publishing an empty list of events."""
        received = []

        async def handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, handler)
        await bus.publish([])

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_sync_handler(self, bus: InMemoryEventBus):
        """Test sync (non-async) handler."""
        received = []

        def sync_handler(event: DomainEvent):
            received.append(event)

        bus.subscribe(EdgeTestEvent, sync_handler)

        event = EdgeTestEvent(aggregate_id=uuid4(), data="test")
        await bus.publish([event])

        assert len(received) == 1

    def test_unsubscribe_returns_false_for_unknown_handler(self, bus: InMemoryEventBus):
        """Test that unsubscribe returns False for unknown handler."""

        async def handler(event: DomainEvent):
            pass

        result = bus.unsubscribe(EdgeTestEvent, handler)
        assert result is False

    def test_subscribe_all_wildcard_handlers(self, bus: InMemoryEventBus):
        """Test wildcard subscription receives all events."""
        received = []

        async def handler(event: DomainEvent):
            received.append(event)

        bus.subscribe_to_all_events(handler)

        # This is just testing the subscription mechanism
        assert bus.get_wildcard_subscriber_count() == 1
