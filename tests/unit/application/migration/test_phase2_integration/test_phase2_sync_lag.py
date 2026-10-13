"""Tests for sync lag tracking during dual-write."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import MigrationConfig
from tests.unit.application.migration.test_phase2_integration._events import SampleTestEvent

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.ports import Position


class TestSyncLagTracking:
    """Tests for sync lag tracking during dual-write."""

    @pytest.mark.asyncio
    async def test_sync_lag_calculation(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test sync lag is calculated correctly."""
        # Pre-populate source with more events than target
        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target has only 5 events
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=100),  # exact count, not clamped
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # No copying has happened yet (since=None): every source event is
        # behind, regardless of how many events the target happens to hold.
        lag = await tracker.calculate_lag()

        assert lag is not None
        assert lag.source_position is not None
        assert lag.target_position is not None
        assert lag.events == 10  # nothing copied yet -> all 10 source events are behind

    @pytest.mark.asyncio
    async def test_sync_lag_converged_when_equal(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test sync lag shows converged when the target has caught up."""
        last_source_position: Position | None = None

        # Same events in both stores
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
            result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
            last_source_position = result.position
            await target_store.append(stream, [event], ExpectedVersion.no_stream())

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # `since` is the last position the target has copied; passing the
        # final source position means nothing is behind.
        lag = await tracker.calculate_lag(since=last_source_position)

        assert lag is not None
        assert lag.events == 0
        assert lag.is_converged

    @pytest.mark.asyncio
    async def test_sync_ready_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test is_sync_ready returns True when lag is within threshold."""
        config = MigrationConfig(cutover_max_lag_events=100)

        last_source_position: Position | None = None

        # Copy the first 10 events' worth of position, leave the rest (10
        # more) as lag -- 10 events behind, well within the 100 threshold.
        for i in range(20):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            result = await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            if i == 9:
                last_source_position = result.position

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        await tracker.calculate_lag(since=last_source_position)

        # 10 events lag is within 100 event threshold
        assert tracker.is_sync_ready()

    @pytest.mark.asyncio
    async def test_lag_statistics_tracking(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test lag statistics are properly tracked over time."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=100),  # exact count, not clamped
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Take multiple lag samples, always measured from the start
        # (since=None) -- each new event increases the count behind by one.
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.sample_count == 5
        assert stats.max_lag == 5  # Final lag after 5 events
        assert stats.min_lag == 1  # First lag was 1


__all__ = ["TestSyncLagTracking"]
