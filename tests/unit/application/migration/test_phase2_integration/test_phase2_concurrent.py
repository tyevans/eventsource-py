"""Tests for concurrent operations during migration."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from tests.unit.application.migration.test_phase2_integration._events import SampleTestEvent

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.write_pause import WritePauseManager


class TestConcurrentOperations:
    """Tests for concurrent operations during migration."""

    @pytest.mark.asyncio
    async def test_concurrent_writes_during_dual_write(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test concurrent writes during dual-write phase."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        async def write_events(count: int, prefix: str) -> list[UUID]:
            """Write multiple events concurrently."""
            ids = []
            for i in range(count):
                aggregate_id = uuid4()
                ids.append(aggregate_id)

                event = SampleTestEvent(
                    aggregate_id=aggregate_id,
                    tenant_id=tenant_id,
                    value=f"{prefix}-{i}",
                )

                await interceptor.append(
                    StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                    [event],
                    ExpectedVersion.no_stream(),
                )

            return ids

        # Run concurrent writes
        results = await asyncio.gather(
            write_events(5, "writer-1"),
            write_events(5, "writer-2"),
            write_events(5, "writer-3"),
        )

        total_ids = sum(len(r) for r in results)
        assert total_ids == 15

        # Verify all events in source
        source_events = [e async for e in source_store.read_all()]
        assert len(source_events) == 15

        # Verify all events in target
        target_events = [e async for e in target_store.read_all()]
        assert len(target_events) == 15

    @pytest.mark.asyncio
    async def test_write_pause_blocks_concurrent_writers(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that write pause blocks concurrent writers."""
        write_started = asyncio.Event()
        write_completed = asyncio.Event()

        async def writer_task():
            """Task that waits if paused."""
            write_started.set()
            wait_time = await write_pause_manager.wait_if_paused(
                tenant_id,
                timeout=5.0,
            )
            write_completed.set()
            return wait_time

        # Pause writes
        await write_pause_manager.pause_writes(tenant_id)

        # Start writer task
        task = asyncio.create_task(writer_task())

        # Wait for writer to start waiting
        await write_started.wait()
        await asyncio.sleep(0.05)  # Give it time to block

        # Writer should not have completed yet
        assert not write_completed.is_set()

        # Resume writes
        await write_pause_manager.resume_writes(tenant_id)

        # Writer should complete
        wait_time = await task
        assert wait_time > 0
        assert write_completed.is_set()

    @pytest.mark.asyncio
    async def test_multiple_tenants_independent(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test that multiple tenants operate independently."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        interceptor1 = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant1,
            enable_tracing=False,
        )

        interceptor2 = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant2,
            enable_tracing=False,
        )

        # Write events for tenant1
        agg1 = uuid4()
        event1 = SampleTestEvent(aggregate_id=agg1, tenant_id=tenant1, value="tenant1")
        stream1 = StreamId(aggregate_id=agg1, category="SampleAggregate")
        await interceptor1.append(stream1, [event1], ExpectedVersion.no_stream())

        # Write events for tenant2
        agg2 = uuid4()
        event2 = SampleTestEvent(aggregate_id=agg2, tenant_id=tenant2, value="tenant2")
        stream2 = StreamId(aggregate_id=agg2, category="SampleAggregate")
        await interceptor2.append(stream2, [event2], ExpectedVersion.no_stream())

        # Verify both tenants have their events
        events1 = [e async for e in source_store.read_stream(stream1)]
        assert len(events1) == 1
        assert events1[0].event.tenant_id == tenant1

        events2 = [e async for e in source_store.read_stream(stream2)]
        assert len(events2) == 1
        assert events2[0].event.tenant_id == tenant2


__all__ = ["TestConcurrentOperations"]
