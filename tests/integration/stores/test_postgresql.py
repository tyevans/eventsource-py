"""
Integration tests for PostgreSQL Event Store.

These tests verify the actual database operations including:
- Event append and retrieval
- Optimistic locking across connections
- Idempotent event handling
- Concurrent access patterns
- Large event payloads
- Stream reading operations
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource import (
    ExpectedVersion,
    OptimisticLockError,
    PostgreSQLEventStore,
    ReadDirection,
    ReadOptions,
)

from ..conftest import (
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
    skip_if_no_postgres_infra,
)

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestPostgreSQLEventStoreAppend:
    """Tests for event append operations."""

    async def test_append_single_event(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test appending a single event to a new aggregate."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position > 0

    async def test_append_multiple_events(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test appending multiple events to an aggregate."""
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=1,
                name="Test Item",
                quantity=10,
            ),
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=2,
                name="Updated Item",
            ),
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=3,
                quantity=20,
            ),
        ]

        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3

    async def test_append_to_existing_aggregate(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test appending events to an existing aggregate."""
        # First append
        event1 = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=0,
        )

        # Second append
        event2 = TestItemUpdated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event2],
            expected_version=1,
        )

        assert result.success is True
        assert result.new_version == 2

    async def test_append_empty_events_list(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that appending an empty list returns success with same version."""
        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 0


class TestPostgreSQLEventStoreOptimisticLocking:
    """Tests for optimistic locking behavior."""

    async def test_optimistic_lock_error_on_version_conflict(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that version conflicts raise OptimisticLockError."""
        # First append
        event1 = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong expected version
        event2 = TestItemUpdated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )

        with pytest.raises(OptimisticLockError) as exc_info:
            await postgres_event_store.append_events(
                aggregate_id=sample_aggregate_id,
                aggregate_type="TestItem",
                events=[event2],
                expected_version=0,  # Wrong - should be 1
            )

        assert exc_info.value.aggregate_id == sample_aggregate_id
        assert exc_info.value.expected_version == 0
        assert exc_info.value.actual_version == 1

    async def test_expected_version_any(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that ExpectedVersion.ANY skips version check."""
        # First append
        event1 = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=0,
        )

        # Append with ANY (should succeed regardless of version)
        event2 = TestItemUpdated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event2],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True
        assert result.new_version == 2

    async def test_expected_version_no_stream(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that ExpectedVersion.NO_STREAM requires stream to not exist."""
        # Should succeed for new aggregate
        event1 = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=ExpectedVersion.NO_STREAM,
        )
        assert result.success is True

        # Should fail for existing aggregate
        new_agg_id = uuid4()
        event2 = TestItemCreated(
            aggregate_id=new_agg_id,
            aggregate_version=1,
            name="Test Item 2",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="TestItem",
            events=[event2],
            expected_version=0,
        )

        event3 = TestItemUpdated(
            aggregate_id=new_agg_id,
            aggregate_version=2,
            name="Updated Item",
        )
        with pytest.raises(OptimisticLockError):
            await postgres_event_store.append_events(
                aggregate_id=new_agg_id,
                aggregate_type="TestItem",
                events=[event3],
                expected_version=ExpectedVersion.NO_STREAM,
            )

    async def test_expected_version_stream_exists(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that ExpectedVersion.STREAM_EXISTS requires stream to exist."""
        # Should fail for new aggregate
        event1 = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        with pytest.raises(OptimisticLockError):
            await postgres_event_store.append_events(
                aggregate_id=sample_aggregate_id,
                aggregate_type="TestItem",
                events=[event1],
                expected_version=ExpectedVersion.STREAM_EXISTS,
            )

        # Should succeed for existing aggregate
        new_agg_id = uuid4()
        event2 = TestItemCreated(
            aggregate_id=new_agg_id,
            aggregate_version=1,
            name="Test Item 2",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="TestItem",
            events=[event2],
            expected_version=0,
        )

        event3 = TestItemUpdated(
            aggregate_id=new_agg_id,
            aggregate_version=2,
            name="Updated Item",
        )
        result = await postgres_event_store.append_events(
            aggregate_id=new_agg_id,
            aggregate_type="TestItem",
            events=[event3],
            expected_version=ExpectedVersion.STREAM_EXISTS,
        )
        assert result.success is True


class TestPostgreSQLEventStoreRetrieval:
    """Tests for event retrieval operations."""

    async def test_get_events_for_aggregate(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test retrieving all events for an aggregate."""
        # Append events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=1,
                name="Test Item",
                quantity=10,
            ),
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=2,
                name="Updated Item",
            ),
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Retrieve events
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
        )

        assert len(stream.events) == 2
        assert stream.version == 2
        assert stream.aggregate_id == sample_aggregate_id
        assert stream.aggregate_type == "TestItem"

        # Verify event order and types
        assert stream.events[0].event_type == "TestItemCreated"
        assert stream.events[1].event_type == "TestItemUpdated"

    async def test_get_events_from_version(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test retrieving events from a specific version."""
        # Append events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=1,
                name="Test Item",
                quantity=10,
            ),
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=2,
                name="Updated 1",
            ),
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=3,
                name="Updated 2",
            ),
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Retrieve from version 1 (should skip first event)
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
            from_version=1,
        )

        assert len(stream.events) == 2
        assert stream.events[0].aggregate_version == 2
        assert stream.events[1].aggregate_version == 3

    async def test_get_events_empty_aggregate(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test retrieving events for non-existent aggregate."""
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
        )

        assert len(stream.events) == 0
        assert stream.version == 0

    async def test_get_events_by_type(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test retrieving events by aggregate type."""
        # Create events for multiple aggregates
        agg1 = uuid4()
        agg2 = uuid4()

        event1 = TestItemCreated(
            aggregate_id=agg1,
            aggregate_version=1,
            name="Item 1",
            quantity=10,
        )
        event2 = TestItemCreated(
            aggregate_id=agg2,
            aggregate_version=1,
            name="Item 2",
            quantity=20,
        )

        await postgres_event_store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=0,
        )
        await postgres_event_store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestItem",
            events=[event2],
            expected_version=0,
        )

        # Retrieve by type
        events = await postgres_event_store.get_events_by_type("TestItem")

        assert len(events) == 2
        assert all(e.aggregate_type == "TestItem" for e in events)

    async def test_get_events_with_timestamp_filter(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test retrieving events with timestamp filtering."""
        # Create event
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
        )

        # Filter with future timestamp (should return no events)
        future_time = datetime.now(UTC) + timedelta(hours=1)
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
            from_timestamp=future_time,
        )
        assert len(stream.events) == 0

        # Filter with past timestamp (should return the event)
        past_time = datetime.now(UTC) - timedelta(hours=1)
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
            from_timestamp=past_time,
        )
        assert len(stream.events) == 1


class TestPostgreSQLEventStoreIdempotency:
    """Tests for idempotent event handling."""

    async def test_duplicate_event_id_skipped(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that duplicate event IDs are skipped (idempotency)."""
        event_id = uuid4()

        # Create event with specific ID
        event1 = TestItemCreated(
            event_id=event_id,
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        # First append
        result1 = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=0,
        )
        assert result1.new_version == 1

        # Try to append same event again with ANY version
        # The event should be skipped due to idempotency check
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event1],
            expected_version=ExpectedVersion.ANY,
        )

        # Version should still be 1 since duplicate was skipped
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
        )
        assert len(stream.events) == 1

    async def test_event_exists_check(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test checking if an event exists."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        # Before append
        exists_before = await postgres_event_store.event_exists(event.event_id)
        assert exists_before is False

        # After append
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
        )

        exists_after = await postgres_event_store.event_exists(event.event_id)
        assert exists_after is True


class TestPostgreSQLEventStoreConcurrency:
    """Tests for concurrent access patterns."""

    async def test_concurrent_appends_to_different_aggregates(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test concurrent appends to different aggregates succeed."""
        agg_ids = [uuid4() for _ in range(5)]

        async def append_event(agg_id: uuid4) -> None:
            event = TestItemCreated(
                aggregate_id=agg_id,
                aggregate_version=1,
                name=f"Item {agg_id}",
                quantity=10,
            )
            await postgres_event_store.append_events(
                aggregate_id=agg_id,
                aggregate_type="TestItem",
                events=[event],
                expected_version=0,
            )

        # Run all appends concurrently
        await asyncio.gather(*[append_event(agg_id) for agg_id in agg_ids])

        # Verify all succeeded
        for agg_id in agg_ids:
            stream = await postgres_event_store.get_events(agg_id, "TestItem")
            assert len(stream.events) == 1

    async def test_concurrent_appends_to_same_aggregate_conflict(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that concurrent appends to same aggregate cause conflicts."""
        conflict_count = 0

        async def append_event(version: int) -> bool:
            nonlocal conflict_count
            event = TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=version + 1,
                name=f"Item version {version}",
                quantity=10,
            )
            try:
                await postgres_event_store.append_events(
                    aggregate_id=sample_aggregate_id,
                    aggregate_type="TestItem",
                    events=[event],
                    expected_version=version,
                )
                return True
            except OptimisticLockError:
                conflict_count += 1
                return False

        # Try to append 5 events all expecting version 0
        results = await asyncio.gather(
            *[append_event(0) for _ in range(5)],
            return_exceptions=True,
        )

        # Only one should succeed, others should conflict
        successes = sum(1 for r in results if r is True)
        assert successes == 1
        assert conflict_count == 4


class TestPostgreSQLEventStoreStreamReading:
    """Tests for stream reading operations."""

    async def test_read_stream_forward(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test reading stream in forward direction."""
        # Create events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=i * 10,
            )
            for i in range(5)
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Read stream forward
        stream_id = f"{sample_aggregate_id}:TestItem"
        read_events = []
        async for stored_event in postgres_event_store.read_stream(stream_id):
            read_events.append(stored_event)

        assert len(read_events) == 5
        # Verify forward order
        for i, stored_event in enumerate(read_events):
            assert stored_event.stream_position == i + 1

    async def test_read_stream_backward(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test reading stream in backward direction."""
        # Create events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=i * 10,
            )
            for i in range(5)
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Read stream backward
        stream_id = f"{sample_aggregate_id}:TestItem"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        read_events = []
        async for stored_event in postgres_event_store.read_stream(stream_id, options):
            read_events.append(stored_event)

        assert len(read_events) == 5
        # Verify backward order
        assert read_events[0].stream_position == 5
        assert read_events[-1].stream_position == 1

    async def test_read_stream_with_limit(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test reading stream with a limit."""
        # Create events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=i * 10,
            )
            for i in range(10)
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Read with limit
        stream_id = f"{sample_aggregate_id}:TestItem"
        options = ReadOptions(limit=3)
        read_events = []
        async for stored_event in postgres_event_store.read_stream(stream_id, options):
            read_events.append(stored_event)

        assert len(read_events) == 3

    async def test_read_all_events(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test reading all events across all streams."""
        # Create events for multiple aggregates
        agg1, agg2 = uuid4(), uuid4()

        events1 = [
            TestItemCreated(
                aggregate_id=agg1,
                aggregate_version=1,
                name="Item 1",
                quantity=10,
            ),
        ]
        events2 = [
            TestOrderCreated(
                aggregate_id=agg2,
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=100.0,
            ),
        ]

        await postgres_event_store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestItem",
            events=events1,
            expected_version=0,
        )
        await postgres_event_store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestOrder",
            events=events2,
            expected_version=0,
        )

        # Read all
        all_events = []
        async for stored_event in postgres_event_store.read_all():
            all_events.append(stored_event)

        assert len(all_events) == 2
        # Should be in global order
        assert all_events[0].global_position < all_events[1].global_position


class TestPostgreSQLEventStoreLargePayloads:
    """Tests for large event payloads."""

    async def test_large_metadata(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test storing events with large metadata."""
        # Create event with large metadata
        large_metadata = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
            metadata=large_metadata,
        )

        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
        )
        assert result.success is True

        # Retrieve and verify
        stream = await postgres_event_store.get_events(
            sample_aggregate_id,
            aggregate_type="TestItem",
        )
        assert len(stream.events) == 1
        assert stream.events[0].metadata == large_metadata


class TestPostgreSQLEventStoreVersion:
    """Tests for version management operations."""

    async def test_get_stream_version(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test getting stream version."""
        # New aggregate should have version 0
        version = await postgres_event_store.get_stream_version(
            sample_aggregate_id,
            "TestItem",
        )
        assert version == 0

        # Add events
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=10,
            )
            for i in range(3)
        ]
        await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        # Version should now be 3
        version = await postgres_event_store.get_stream_version(
            sample_aggregate_id,
            "TestItem",
        )
        assert version == 3


class TestPostgreSQLEventStoreGlobalPosition:
    """Tests for get_global_position() method."""

    async def test_empty_store_returns_zero(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test that an empty store returns position 0."""
        position = await postgres_event_store.get_global_position()
        assert position == 0

    async def test_single_event_returns_correct_position(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that store returns correct position after one event."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=[event],
            expected_version=0,
        )

        position = await postgres_event_store.get_global_position()
        assert position == result.global_position
        assert position >= 1

    async def test_multiple_events_returns_max_position(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that position reflects highest id after multiple events."""
        events = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=10,
            )
            for i in range(5)
        ]

        result = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events,
            expected_version=0,
        )

        position = await postgres_event_store.get_global_position()
        assert position == result.global_position

    async def test_position_increases_after_append(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_aggregate_id: uuid4,
    ) -> None:
        """Test that position increases after appending more events."""
        # Append first batch
        events1 = [
            TestItemCreated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 1,
                name=f"Item {i}",
                quantity=10,
            )
            for i in range(3)
        ]
        result1 = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events1,
            expected_version=0,
        )

        position1 = await postgres_event_store.get_global_position()
        assert position1 == result1.global_position

        # Append second batch
        events2 = [
            TestItemUpdated(
                aggregate_id=sample_aggregate_id,
                aggregate_version=i + 4,
                name=f"Updated Item {i}",
            )
            for i in range(2)
        ]
        result2 = await postgres_event_store.append_events(
            aggregate_id=sample_aggregate_id,
            aggregate_type="TestItem",
            events=events2,
            expected_version=3,
        )

        position2 = await postgres_event_store.get_global_position()
        assert position2 == result2.global_position
        assert position2 > position1

    async def test_position_across_multiple_aggregates(
        self,
        postgres_event_store: PostgreSQLEventStore,
    ) -> None:
        """Test that position reflects events across all aggregates."""
        agg1 = uuid4()
        agg2 = uuid4()

        # Append to first aggregate
        await postgres_event_store.append_events(
            aggregate_id=agg1,
            aggregate_type="TestItem",
            events=[
                TestItemCreated(
                    aggregate_id=agg1,
                    aggregate_version=1,
                    name="Item 1",
                    quantity=10,
                )
            ],
            expected_version=0,
        )

        # Append to second aggregate
        result = await postgres_event_store.append_events(
            aggregate_id=agg2,
            aggregate_type="TestItem",
            events=[
                TestItemCreated(
                    aggregate_id=agg2,
                    aggregate_version=1,
                    name="Item 2",
                    quantity=20,
                ),
                TestItemUpdated(
                    aggregate_id=agg2,
                    aggregate_version=2,
                    name="Updated Item 2",
                ),
            ],
            expected_version=0,
        )

        position = await postgres_event_store.get_global_position()
        assert position == result.global_position
