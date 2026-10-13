"""Tests for dual-write interceptor behavior."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from tests.unit.application.migration.test_phase2_integration._events import (
    OrderConfirmed,
    OrderCreated,
)


class TestDualWriteBehavior:
    """Tests for dual-write interceptor behavior."""

    @pytest.mark.asyncio
    async def test_dual_write_writes_to_both_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write interceptor writes to both stores."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write through interceptor
        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert result.new_version == 1

        # Verify event in source
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 1
        assert source_events[0].event.event_id == event.event_id

        # Verify event in target
        target_events = [e async for e in target_store.read_stream(stream)]
        assert len(target_events) == 1
        assert target_events[0].event.event_id == event.event_id

    @pytest.mark.asyncio
    async def test_dual_write_reads_from_source(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that reads during dual-write come from source store."""
        # Pre-populate source with events
        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        await source_store.append(stream, [event], ExpectedVersion.no_stream())

        # Target is empty

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Read through interceptor
        events = [e async for e in interceptor.read_stream(stream)]

        # Should get event from source
        assert len(events) == 1
        assert events[0].event.event_id == event.event_id

    @pytest.mark.asyncio
    async def test_dual_write_handles_target_failure(
        self,
        source_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write handles target store failure gracefully."""

        # Create a target store that will fail on append
        class FailingStore(InMemoryEventStore):
            async def append(self, *args, **kwargs):
                raise RuntimeError("Target store failure")

        failing_target = FailingStore("failing-target")

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=failing_target,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write should succeed (source is authoritative)
        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert result.new_version == 1

        # Verify event in source
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 1

        # Verify failure was recorded
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 1

    @pytest.mark.asyncio
    async def test_dual_write_multiple_events_same_aggregate(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test dual-write with multiple events on same aggregate."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write first event
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append(stream, [event1], ExpectedVersion.no_stream())

        # Write second event
        event2 = OrderConfirmed(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append(stream, [event2], ExpectedVersion.exact(1))

        # Verify both events in both stores
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 2

        target_events = [e async for e in target_store.read_stream(stream)]
        assert len(target_events) == 2


__all__ = ["TestDualWriteBehavior"]
