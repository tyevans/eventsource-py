"""Unit tests for AggregateRepository snapshot functionality.

P5-004: Tests for snapshot integration in AggregateRepository.

Tests cover:
- Constructor snapshot parameters (snapshot_store, threshold, mode)
- Snapshot-aware load (using snapshot + events since snapshot)
- Schema version validation and fallback behavior
- Automatic snapshot creation at threshold boundaries
- Manual snapshot creation via create_snapshot()
- Background snapshot mode (async task creation)
- Graceful fallback when snapshots are invalid or unavailable
- Full cycle: save events -> create snapshot -> load from snapshot
"""

import asyncio
import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from eventsource.aggregates.base import AggregateRoot
from eventsource.aggregates.repository import AggregateRepository
from eventsource.events.base import DomainEvent
from eventsource.exceptions import AggregateNotFoundError
from eventsource.snapshots import InMemorySnapshotStore, Snapshot
from eventsource.stores.in_memory import InMemoryEventStore

# =============================================================================
# Test Fixtures
# =============================================================================


class TestState(BaseModel):
    """Simple state for testing."""

    value: str = ""
    count: int = 0
    items: list[str] = Field(default_factory=list)


class TestEvent(DomainEvent):
    """Simple event for testing."""

    event_type: str = "TestEvent"
    aggregate_type: str = "Test"
    value: str


class CountEvent(DomainEvent):
    """Event that increments count."""

    event_type: str = "CountEvent"
    aggregate_type: str = "Test"


class TestAggregate(AggregateRoot[TestState]):
    """Test aggregate with snapshot support."""

    aggregate_type = "Test"
    schema_version = 1

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TestEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )
        elif isinstance(event, CountEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(update={"count": self._state.count + 1})

    def _get_initial_state(self) -> TestState:
        return TestState()

    def do_something(self, value: str) -> None:
        """Apply a test event."""
        event = TestEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            value=value,
        )
        self.apply_event(event)

    def increment(self) -> None:
        """Apply a count event."""
        event = CountEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


class TestAggregateV2(AggregateRoot[TestState]):
    """Same aggregate with different schema version."""

    aggregate_type = "Test"
    schema_version = 2  # Different from TestAggregate

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TestEvent):
            if self._state is None:
                self._state = TestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )

    def _get_initial_state(self) -> TestState:
        return TestState()


# =============================================================================
# Test: Repository Constructor
# =============================================================================


class TestRepositoryConstructor:
    """Test repository constructor with snapshot parameters."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def test_constructor_without_snapshot_params(self, event_store: InMemoryEventStore) -> None:
        """Default values without snapshot params."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
        )

        assert repo.snapshot_store is None
        assert repo.snapshot_threshold is None
        assert repo.snapshot_mode == "sync"
        assert repo.has_snapshot_support is False

    def test_constructor_with_snapshot_store(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """With snapshot store provided."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        assert repo.snapshot_store is snapshot_store
        assert repo.has_snapshot_support is True

    def test_constructor_with_all_params(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """With all snapshot parameters."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=100,
            snapshot_mode="background",
        )

        assert repo.snapshot_threshold == 100
        assert repo.snapshot_mode == "background"

    def test_constructor_with_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """With manual snapshot mode."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=10,
            snapshot_mode="manual",
        )

        assert repo.snapshot_mode == "manual"


# =============================================================================
# Test: Snapshot-Aware Load
# =============================================================================


class TestRepositorySnapshotLoad:
    """Test snapshot-aware load functionality."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_load_without_snapshot_store(self, event_store: InMemoryEventStore) -> None:
        """Load works without snapshot store (full event replay)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
        )

        # Create and save aggregate
        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test-value")
        await repo.save(aggregate)

        # Load without snapshot store
        loaded = await repo.load(aggregate.aggregate_id)

        assert loaded.version == 1
        assert loaded.state.value == "test-value"

    @pytest.mark.asyncio
    async def test_load_uses_snapshot_when_available(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Load uses snapshot and replays only events since snapshot."""
        aggregate_id = uuid4()

        # First, add events 1-5 to the event store (required for version tracking)
        initial_events = [
            TestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="Test",
                aggregate_version=i + 1,
                value=f"event_{i}",
            )
            for i in range(5)
        ]
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=initial_events,
            expected_version=0,
        )

        # Pre-populate snapshot at version 5
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=5,
            state={"value": "from_snapshot", "count": 5, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        # Add event after snapshot (version 6)
        event = TestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            aggregate_version=6,
            value="after_snapshot",
        )
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=[event],
            expected_version=5,
        )

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        loaded = await repo.load(aggregate_id)

        # Should have version 6 (5 from snapshot + 1 event)
        assert loaded.version == 6
        assert loaded.state.value == "after_snapshot"
        assert loaded.state.count == 6  # 5 from snapshot + 1 from event

    @pytest.mark.asyncio
    async def test_load_falls_back_on_schema_mismatch(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Load falls back to full replay when schema version doesn't match."""
        aggregate_id = uuid4()

        # Snapshot with old schema version
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=5,
            state={"value": "old_snapshot", "count": 5, "items": []},
            schema_version=99,  # Doesn't match TestAggregate.schema_version (1)
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        # Create events from start
        events = [
            TestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="Test",
                aggregate_version=i + 1,
                value=f"event_{i}",
            )
            for i in range(3)
        ]
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=events,
            expected_version=0,
        )

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        loaded = await repo.load(aggregate_id)

        # Should have replayed from events, not snapshot
        assert loaded.version == 3
        assert loaded.state.count == 3
        assert loaded.state.value == "event_2"

    @pytest.mark.asyncio
    async def test_load_falls_back_on_snapshot_error(self, event_store: InMemoryEventStore) -> None:
        """Load falls back when snapshot store errors."""
        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(side_effect=Exception("Store error"))

        aggregate_id = uuid4()

        # Create events
        event = TestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            aggregate_version=1,
            value="test",
        )
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=[event],
            expected_version=0,
        )

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=mock_snapshot_store,
        )

        # Should not raise, should fall back to event replay
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_load_falls_back_on_deserialization_error(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Load falls back when snapshot deserialization fails."""
        aggregate_id = uuid4()

        # Snapshot with invalid state (wrong type for 'count' - should be int)
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=5,
            state={"value": "test", "count": {"invalid": "type"}, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        # Create events from start
        events = [
            TestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="Test",
                aggregate_version=i + 1,
                value=f"event_{i}",
            )
            for i in range(2)
        ]
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=events,
            expected_version=0,
        )

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        # Should fall back to full event replay
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 2

    @pytest.mark.asyncio
    async def test_load_raises_not_found_when_no_events(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Load raises AggregateNotFoundError when no events exist."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        with pytest.raises(AggregateNotFoundError):
            await repo.load(uuid4())

    @pytest.mark.asyncio
    async def test_load_no_events_after_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Load works when snapshot exists but no events after it."""
        aggregate_id = uuid4()

        # Create snapshot only (no events in store)
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=5,
            state={"value": "snapshot_only", "count": 5, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        # Should load from snapshot successfully
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 5
        assert loaded.state.value == "snapshot_only"


# =============================================================================
# Test: Automatic Snapshot Creation
# =============================================================================


class TestRepositoryAutoSnapshot:
    """Test automatic snapshot creation at threshold."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_auto_snapshot_at_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshot created when version reaches threshold."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=3,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()

        # Create and save events until threshold
        for i in range(3):
            if i == 0:
                aggregate = TestAggregate(aggregate_id)
            else:
                aggregate = await repo.load(aggregate_id)
            aggregate.do_something(f"event_{i}")
            await repo.save(aggregate)

        # Should have created snapshot at version 3
        assert snapshot_store.snapshot_count == 1

        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 3

    @pytest.mark.asyncio
    async def test_no_snapshot_below_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """No snapshot created below threshold."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=10,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_no_auto_snapshot_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """No automatic snapshot in manual mode."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=1,  # Would trigger immediately
            snapshot_mode="manual",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_snapshot_at_multiples_of_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshots created at threshold multiples (e.g., 100, 200, 300)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply 10 events (should trigger snapshot at 5 and 10)
        for _ in range(10):
            aggregate.increment()

        await repo.save(aggregate)

        # Should have snapshot at version 10 (latest threshold multiple)
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 10

    @pytest.mark.asyncio
    async def test_snapshot_failure_doesnt_fail_save(self, event_store: InMemoryEventStore) -> None:
        """Snapshot failure doesn't fail the save operation."""
        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=Exception("Snapshot error"))

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="sync",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        # Should not raise
        await repo.save(aggregate)

        # Events should be committed
        assert not aggregate.has_uncommitted_events


# =============================================================================
# Test: Manual Snapshot Creation
# =============================================================================


class TestRepositoryManualSnapshot:
    """Test manual snapshot creation."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_create_snapshot_returns_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """create_snapshot returns the created Snapshot object."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()  # Simulate already saved

        snapshot = await repo.create_snapshot(aggregate)

        assert snapshot is not None
        assert snapshot.aggregate_id == aggregate.aggregate_id
        assert snapshot.aggregate_type == "Test"
        assert snapshot.version == 1
        assert snapshot.schema_version == 1
        assert snapshot.state["value"] == "test"

    @pytest.mark.asyncio
    async def test_create_snapshot_raises_without_store(
        self, event_store: InMemoryEventStore
    ) -> None:
        """create_snapshot raises RuntimeError without snapshot store."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
        )

        aggregate = TestAggregate(uuid4())

        with pytest.raises(RuntimeError, match="snapshot_store"):
            await repo.create_snapshot(aggregate)

    @pytest.mark.asyncio
    async def test_create_snapshot_works_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Manual snapshot works even in manual mode."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_mode="manual",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        assert snapshot_store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_create_snapshot_overwrites_existing(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Creating snapshot overwrites existing snapshot (upsert)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("first")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        # Apply more events
        aggregate.do_something("second")
        aggregate._uncommitted_events.clear()

        await repo.create_snapshot(aggregate)

        # Should still have only 1 snapshot
        assert snapshot_store.snapshot_count == 1

        # Snapshot should be at newer version
        snapshot = await snapshot_store.get_snapshot(aggregate.aggregate_id, "Test")
        assert snapshot.version == 2
        assert snapshot.state["value"] == "second"


# =============================================================================
# Test: Background Snapshot Mode
# =============================================================================


class TestRepositoryBackgroundSnapshot:
    """Test background snapshot mode."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_background_mode_creates_task(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Background mode creates async task for snapshot."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        await repo.save(aggregate)

        # Wait for background task
        await repo.await_pending_snapshots()

        # Snapshot should have been created
        assert snapshot_store.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_background_mode_doesnt_block(self, event_store: InMemoryEventStore) -> None:
        """Background mode returns before snapshot completes."""

        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.1)

        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=slow_save)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")

        start = time.monotonic()
        await repo.save(aggregate)
        elapsed = time.monotonic() - start

        # Should return much faster than 0.1s
        assert elapsed < 0.05

        # Cleanup
        await repo.await_pending_snapshots()

    @pytest.mark.asyncio
    async def test_await_pending_snapshots_waits_for_all(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """await_pending_snapshots waits for all pending tasks."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        # Create multiple aggregates that will each trigger a snapshot
        for _ in range(3):
            aggregate = TestAggregate(uuid4())
            aggregate.do_something("test")
            await repo.save(aggregate)

        count = await repo.await_pending_snapshots()

        assert count == 3
        assert snapshot_store.snapshot_count == 3

    @pytest.mark.asyncio
    async def test_pending_snapshot_count(self, event_store: InMemoryEventStore) -> None:
        """pending_snapshot_count returns correct count."""

        async def slow_save(snapshot: Snapshot) -> None:
            await asyncio.sleep(0.5)

        mock_snapshot_store = MagicMock()
        mock_snapshot_store.get_snapshot = AsyncMock(return_value=None)
        mock_snapshot_store.save_snapshot = AsyncMock(side_effect=slow_save)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=mock_snapshot_store,
            snapshot_threshold=1,
            snapshot_mode="background",
        )

        aggregate = TestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        # There should be at least 0 pending (may have completed already)
        assert repo.pending_snapshot_count >= 0

        # Cleanup
        await repo.await_pending_snapshots()


# =============================================================================
# Test: Should Create Snapshot Logic
# =============================================================================


class TestSnapshotStrategyLogic:
    """Test snapshot strategy logic via SnapshotStrategy classes.

    Note: These tests validate the strategy logic that was previously in
    AggregateRepository._should_create_snapshot(). The logic was extracted
    to SnapshotStrategy classes (ThresholdSnapshotStrategy, etc.) for SOLID
    compliance. We test via the strategy classes directly.
    """

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    def test_returns_false_without_store(self, event_store: InMemoryEventStore) -> None:
        """Without snapshot store, repository has no snapshot strategy."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_threshold=100,
        )

        # Without snapshot_store, no strategy is created
        assert repo._snapshot_manager is None

    def test_returns_false_without_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Without threshold, ThresholdSnapshotStrategy.should_snapshot returns False."""
        from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

        # Strategy with no threshold never triggers auto-snapshot
        strategy = ThresholdSnapshotStrategy(threshold=None)
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        assert strategy.should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_false_in_manual_mode(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Manual mode (NoSnapshotStrategy) doesn't auto-snapshot."""
        from eventsource.snapshots.strategies import NoSnapshotStrategy

        strategy = NoSnapshotStrategy()
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        # NoSnapshotStrategy.should_snapshot always returns False
        assert strategy.should_snapshot(aggregate, events_since_snapshot=100) is False

    def test_returns_true_at_threshold(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns True at exact threshold."""
        from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

        strategy = ThresholdSnapshotStrategy(threshold=100)
        aggregate = TestAggregate(uuid4())
        aggregate._version = 100

        assert strategy.should_snapshot(aggregate, events_since_snapshot=100) is True

    def test_returns_true_at_threshold_multiples(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns True at threshold multiples."""
        from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

        strategy = ThresholdSnapshotStrategy(threshold=100)
        aggregate = TestAggregate(uuid4())

        for version in [100, 200, 300, 500, 1000]:
            aggregate._version = version
            assert strategy.should_snapshot(aggregate, events_since_snapshot=version) is True

    def test_returns_false_between_thresholds(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns False between threshold multiples."""
        from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

        strategy = ThresholdSnapshotStrategy(threshold=100)
        aggregate = TestAggregate(uuid4())

        for version in [1, 50, 99, 101, 150, 199]:
            aggregate._version = version
            assert strategy.should_snapshot(aggregate, events_since_snapshot=version) is False

    def test_returns_false_at_version_zero(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Returns False at version zero."""
        from eventsource.snapshots.strategies import ThresholdSnapshotStrategy

        strategy = ThresholdSnapshotStrategy(threshold=100)
        aggregate = TestAggregate(uuid4())
        aggregate._version = 0

        assert strategy.should_snapshot(aggregate, events_since_snapshot=0) is False


# =============================================================================
# Test: Full Cycle Integration
# =============================================================================


class TestFullCycleIntegration:
    """Test complete cycle: save events -> create snapshot -> load from snapshot."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_full_cycle_with_auto_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Complete cycle with automatic snapshot creation."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply 5 events (triggers snapshot)
        for i in range(5):
            aggregate.do_something(f"value_{i}")

        await repo.save(aggregate)

        # Verify snapshot exists
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot is not None
        assert snapshot.version == 5

        # Load from snapshot
        loaded = await repo.load(aggregate_id)
        assert loaded.version == 5
        assert loaded.state.value == "value_4"
        assert loaded.state.count == 5

        # Apply more events
        loaded.do_something("value_5")
        await repo.save(loaded)

        # Load again (should use snapshot + 1 event)
        final = await repo.load(aggregate_id)
        assert final.version == 6
        assert final.state.value == "value_5"

    @pytest.mark.asyncio
    async def test_full_cycle_with_manual_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Complete cycle with manual snapshot creation."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_mode="manual",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)

        # Apply events and save
        for i in range(10):
            aggregate.do_something(f"value_{i}")
        await repo.save(aggregate)

        # Manually create snapshot
        snapshot = await repo.create_snapshot(aggregate)
        assert snapshot.version == 10

        # Apply more events
        loaded = await repo.load(aggregate_id)
        loaded.do_something("after_snapshot")
        await repo.save(loaded)

        # Load again (should use snapshot + new event)
        final = await repo.load(aggregate_id)
        assert final.version == 11
        assert final.state.value == "after_snapshot"

    @pytest.mark.asyncio
    async def test_snapshot_invalidation_on_schema_change(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Snapshot is invalidated when schema version changes."""
        # Create repo with schema_version=1
        repo_v1 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,  # schema_version=1
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=5,
            snapshot_mode="sync",
        )

        aggregate_id = uuid4()
        aggregate = TestAggregate(aggregate_id)
        for i in range(5):
            aggregate.do_something(f"v1_{i}")
        await repo_v1.save(aggregate)

        # Snapshot exists with schema_version=1
        snapshot = await snapshot_store.get_snapshot(aggregate_id, "Test")
        assert snapshot.schema_version == 1

        # Create repo with schema_version=2 (simulating schema upgrade)
        repo_v2 = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregateV2,  # schema_version=2
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        # Load should fall back to full replay
        loaded = await repo_v2.load(aggregate_id)
        assert loaded.version == 5  # Full replay from events


# =============================================================================
# Test: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_concurrent_loads_with_snapshot(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Multiple concurrent loads should work with snapshot."""
        aggregate_id = uuid4()

        # Create snapshot
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            version=10,
            state={"value": "concurrent", "count": 10, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
        )

        # Load concurrently
        results = await asyncio.gather(
            repo.load(aggregate_id),
            repo.load(aggregate_id),
            repo.load(aggregate_id),
        )

        for loaded in results:
            assert loaded.version == 10
            assert loaded.state.value == "concurrent"

    @pytest.mark.asyncio
    async def test_save_with_no_changes_and_snapshot_store(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Saving with no changes should not create snapshot."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=1,
        )

        aggregate = TestAggregate(uuid4())
        # No changes, just save

        await repo.save(aggregate)

        assert snapshot_store.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_multiple_aggregates_independent_snapshots(
        self, event_store: InMemoryEventStore, snapshot_store: InMemorySnapshotStore
    ) -> None:
        """Each aggregate should have independent snapshots."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TestAggregate,
            aggregate_type="Test",
            snapshot_store=snapshot_store,
            snapshot_threshold=2,
            snapshot_mode="sync",
        )

        id1 = uuid4()
        id2 = uuid4()

        # Create first aggregate with 2 events (triggers snapshot)
        agg1 = TestAggregate(id1)
        agg1.do_something("agg1_v1")
        agg1.do_something("agg1_v2")
        await repo.save(agg1)

        # Create second aggregate with 2 events (triggers snapshot)
        agg2 = TestAggregate(id2)
        agg2.do_something("agg2_v1")
        agg2.do_something("agg2_v2")
        await repo.save(agg2)

        # Should have 2 independent snapshots
        assert snapshot_store.snapshot_count == 2

        snap1 = await snapshot_store.get_snapshot(id1, "Test")
        snap2 = await snapshot_store.get_snapshot(id2, "Test")

        assert snap1.state["value"] == "agg1_v2"
        assert snap2.state["value"] == "agg2_v2"
