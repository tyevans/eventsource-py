"""Integration tests verifying components work together."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.positions import Position
from eventsource.testing import InMemoryTestHarness
from tests.unit.testing.test_harness._fixtures import (
    HarnessSampleEvent,
    SampleOrderAggregate,
)


class TestHarnessIntegration:
    """Integration tests verifying components work together."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_event_store_and_bus_work_together(self, harness: InMemoryTestHarness) -> None:
        """Event store and bus can be used together."""
        aggregate_id = uuid4()
        event = HarnessSampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # Store event
        await harness.event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Sample"),
            [event],
            ExpectedVersion.no_stream(),
        )

        # Publish event
        await harness.event_bus.publish([event])

        # Both should reflect the event
        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=aggregate_id, category="Sample")
            )
        ]
        assert len(envelopes) == 1
        assert len(harness.published_events) == 1

    @pytest.mark.asyncio
    async def test_checkpoint_repo_tracks_positions(self, harness: InMemoryTestHarness) -> None:
        """Checkpoint repository tracks positions correctly."""
        event_id = uuid4()

        expected = Position(store_id=harness.event_store.store_id, key=(100,))
        await harness.checkpoint_repo.save_position(
            subscription_id="my-projection",
            position=expected,
            event_id=event_id,
            event_type="HarnessSampleEvent",
        )

        position = await harness.checkpoint_repo.get_position("my-projection")
        assert position == expected

    @pytest.mark.asyncio
    async def test_dlq_repo_stores_failures(self, harness: InMemoryTestHarness) -> None:
        """DLQ repository stores failed events."""
        event_id = uuid4()

        await harness.dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="my-projection",
            event_type="HarnessSampleEvent",
            event_data={"key": "value"},
            error=ValueError("Test error"),
            retry_count=3,
        )

        entries = await harness.dlq_repo.get_failed_events("my-projection")
        assert len(entries) == 1
        assert entries[0].event_id == event_id
        assert entries[0].retry_count == 3

    @pytest.mark.asyncio
    async def test_multiple_aggregates_isolated(self, harness: InMemoryTestHarness) -> None:
        """Multiple aggregates are properly isolated in event store."""
        agg1_id = uuid4()
        agg2_id = uuid4()

        event1 = HarnessSampleEvent(
            aggregate_id=agg1_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = HarnessSampleEvent(
            aggregate_id=agg2_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        await harness.event_store.append(
            StreamId(aggregate_id=agg1_id, category="Sample"),
            [event1],
            ExpectedVersion.no_stream(),
        )
        await harness.event_store.append(
            StreamId(aggregate_id=agg2_id, category="Sample"),
            [event2],
            ExpectedVersion.no_stream(),
        )

        envelopes1 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg1_id, category="Sample")
            )
        ]
        envelopes2 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg2_id, category="Sample")
            )
        ]

        assert len(envelopes1) == 1
        assert len(envelopes2) == 1
        assert envelopes1[0].event.aggregate_id == agg1_id
        assert envelopes2[0].event.aggregate_id == agg2_id

    @pytest.mark.asyncio
    async def test_create_repository_without_snapshots(self, harness: InMemoryTestHarness) -> None:
        """create_repository wires event_store and event_bus without snapshot_store by default."""
        repo = harness.create_repository(SampleOrderAggregate)

        assert repo.snapshot_store is None
        assert repo._enable_tracing is False

        order = SampleOrderAggregate(uuid4())
        order.record_step()
        await repo.save(order)

        harness.assertions.assert_event_count(1)
        harness.assertions.assert_event_published(HarnessSampleEvent)
        loaded = await repo.load(order.aggregate_id)
        assert loaded.version == 1
        assert loaded.state.step_count == 1

    @pytest.mark.asyncio
    async def test_create_repository_with_snapshots_flag(
        self, harness: InMemoryTestHarness
    ) -> None:
        """create_repository(with_snapshots=True) attaches harness.snapshot_store."""
        repo = harness.create_repository(SampleOrderAggregate, with_snapshots=True)

        assert repo.snapshot_store is harness.snapshot_store

        order = SampleOrderAggregate(uuid4())
        order.record_step()
        await repo.save(order)
        await repo.create_snapshot(order)

        snapshot = await harness.snapshot_store.get_snapshot(order.aggregate_id, "Sample")
        assert snapshot is not None
        assert snapshot.version == 1

    @pytest.mark.asyncio
    async def test_create_repository_with_snapshot_threshold(
        self, harness: InMemoryTestHarness
    ) -> None:
        """create_repository(snapshot_threshold=2) attaches snapshot_store and auto-snapshots."""
        repo = harness.create_repository(SampleOrderAggregate, snapshot_threshold=2)

        assert repo.snapshot_store is harness.snapshot_store

        order = SampleOrderAggregate(uuid4())
        order.record_step()
        order.record_step()
        await repo.save(order)

        snapshot = await harness.snapshot_store.get_snapshot(order.aggregate_id, "Sample")
        assert snapshot is not None
        assert snapshot.version == 2
        loaded = await repo.load(order.aggregate_id)
        assert loaded.version == 2
        assert loaded.state.step_count == 2
