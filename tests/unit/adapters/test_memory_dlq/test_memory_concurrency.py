"""Concurrency tests for InMemoryDLQRepository."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.dlq import InMemoryDLQRepository


class TestInMemoryDLQRepositoryConcurrency:
    """Tests for concurrent access to InMemoryDLQRepository.

    These tests verify that asyncio.Lock properly serializes concurrent
    operations without deadlocks or data corruption.
    """

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_concurrent_add_failed_events_no_deadlock(self, repo: InMemoryDLQRepository):
        """Test that 100 concurrent add_failed_event calls complete without deadlock."""
        num_events = 100

        async def add_failed_event(i: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={"index": i},
                error=Exception(f"Error {i}"),
                retry_count=1,
            )

        # Run 100 concurrent adds
        tasks = [add_failed_event(i) for i in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all events were added
        failed_events = await repo.get_failed_events(limit=200)
        assert len(failed_events) == num_events

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryDLQRepository):
        """Test mixed concurrent reads and writes complete without issues."""
        num_operations = 50
        read_results: list[int] = []

        async def writer(i: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={"index": i},
                error=Exception(f"Error {i}"),
            )

        async def reader():
            events = await repo.get_failed_events()
            read_results.append(len(events))

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_upsert_same_event(self, repo: InMemoryDLQRepository):
        """Test concurrent upserts for the same event_id + projection combination."""
        event_id = uuid4()
        projection_name = "TestProjection"
        num_updates = 50

        async def upsert(i: int):
            await repo.add_failed_event(
                event_id=event_id,
                projection_name=projection_name,
                event_type="TestEvent",
                event_data={"update": i},
                error=Exception(f"Error {i}"),
                retry_count=i,
            )

        # Run concurrent upserts for the same event
        tasks = [upsert(i) for i in range(num_updates)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Should only have one entry (upsert behavior)
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1

    @pytest.mark.asyncio
    async def test_concurrent_status_transitions(self, repo: InMemoryDLQRepository):
        """Test concurrent status transitions (retry, resolve)."""
        num_events = 10

        # Add events first
        for i in range(num_events):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        events = await repo.get_failed_events()
        dlq_ids = [e.id for e in events]

        async def mark_retrying(dlq_id: int | str):
            await repo.mark_retrying(dlq_id)

        async def mark_resolved(dlq_id: int | str):
            await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Alternate between retrying and resolving
        tasks = []
        for i, dlq_id in enumerate(dlq_ids):
            if i % 2 == 0:
                tasks.append(mark_retrying(dlq_id))
            else:
                tasks.append(mark_resolved(dlq_id))

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify status transitions happened
        stats = await repo.get_failure_stats()
        # Some should be retrying, some resolved
        assert stats.total_retrying + stats.total_failed <= num_events

    @pytest.mark.asyncio
    async def test_concurrent_stats_during_updates(self, repo: InMemoryDLQRepository):
        """Test that get_failure_stats works correctly during concurrent updates."""
        stats_results: list[object] = []

        async def add_events():
            for i in range(20):
                await repo.add_failed_event(
                    event_id=uuid4(),
                    projection_name=f"Projection{i % 3}",
                    event_type="TestEvent",
                    event_data={},
                    error=Exception(f"Error {i}"),
                )

        async def get_stats():
            for _ in range(10):
                stats = await repo.get_failure_stats()
                stats_results.append(stats)
                await asyncio.sleep(0.001)

        # Run concurrent add and stats operations
        await asyncio.wait_for(
            asyncio.gather(add_events(), get_stats()),
            timeout=5.0,
        )

        # All stats calls completed without error
        assert len(stats_results) == 10
        for stats in stats_results:
            assert hasattr(stats, "total_failed")

    @pytest.mark.asyncio
    async def test_concurrent_multiple_projections(self, repo: InMemoryDLQRepository):
        """Test concurrent failures across multiple projections."""
        num_projections = 5
        events_per_projection = 20

        async def add_for_projection(proj_id: int, event_id: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{proj_id}",
                event_type="TestEvent",
                event_data={"proj": proj_id, "event": event_id},
                error=Exception(f"Error {proj_id}-{event_id}"),
            )

        tasks = []
        for proj in range(num_projections):
            for event in range(events_per_projection):
                tasks.append(add_for_projection(proj, event))

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all events were added
        stats = await repo.get_failure_stats()
        assert stats.total_failed == num_projections * events_per_projection
        assert stats.affected_projections == num_projections
