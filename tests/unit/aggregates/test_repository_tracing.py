"""
Unit tests for AggregateRepository tracing functionality.

O11Y-008: Tests for AggregateRepository TracingMixin integration.

Tests cover:
- TracingMixin inheritance
- enable_tracing parameter in constructor
- Span creation for load, save, exists, create_snapshot methods
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
- Backward compatible constructor (default tracing enabled)
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from eventsource.aggregates.base import AggregateRoot
from eventsource.aggregates.repository import AggregateRepository
from eventsource.events.base import DomainEvent
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
    TracingMixin,
)
from eventsource.snapshots import InMemorySnapshotStore, Snapshot
from eventsource.stores.in_memory import InMemoryEventStore

# ============================================================================
# Test Fixtures
# ============================================================================


class TracingTestState(BaseModel):
    """Simple state for tracing tests."""

    value: str = ""
    count: int = 0
    items: list[str] = Field(default_factory=list)


class TracingTestEvent(DomainEvent):
    """Test event for tracing tests."""

    event_type: str = "TracingTestEvent"
    aggregate_type: str = "TracingTest"
    value: str


class TracingTestAggregate(AggregateRoot[TracingTestState]):
    """Test aggregate with tracing support."""

    aggregate_type = "TracingTest"
    schema_version = 1

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TracingTestEvent):
            if self._state is None:
                self._state = TracingTestState()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )

    def _get_initial_state(self) -> TracingTestState:
        return TracingTestState()

    def do_something(self, value: str) -> None:
        """Apply a test event."""
        event = TracingTestEvent(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            value=value,
        )
        self.apply_event(event)


# ============================================================================
# TracingMixin Integration Tests
# ============================================================================


class TestAggregateRepositoryTracingMixin:
    """Tests for AggregateRepository TracingMixin integration."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    def test_inherits_from_tracing_mixin(self):
        """AggregateRepository inherits from TracingMixin."""
        assert issubclass(AggregateRepository, TracingMixin)

    def test_tracing_enabled_by_default(self, event_store: InMemoryEventStore):
        """Tracing is enabled by default when OTEL is available."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
        )

        # Check that tracing was initialized
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")

    def test_tracing_disabled_when_requested(self, event_store: InMemoryEventStore):
        """Tracing can be disabled via constructor parameter."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=False,
        )

        assert repo._enable_tracing is False
        assert repo._tracer is None

    def test_has_tracing_enabled_property(self, event_store: InMemoryEventStore):
        """Repository exposes tracing_enabled property from mixin."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )

        # tracing_enabled is a property from TracingMixin
        assert hasattr(repo, "tracing_enabled")
        assert isinstance(repo.tracing_enabled, bool)

    def test_backward_compatible_constructor(self, event_store: InMemoryEventStore):
        """Constructor without enable_tracing should work (default True)."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
        )
        # Should not raise, tracing defaults to enabled
        assert hasattr(repo, "_enable_tracing")


# ============================================================================
# Span Creation Tests
# ============================================================================


class TestAggregateRepositorySpanCreation:
    """Tests for span creation in AggregateRepository operations."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.fixture
    def traced_repo(self, event_store, mock_tracer):
        """Create a repository with injected mock tracer."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        # Inject mock tracer
        repo._tracer = mock_tracer
        repo._enable_tracing = True
        return repo

    @pytest.fixture
    def traced_repo_with_snapshots(self, event_store, snapshot_store, mock_tracer):
        """Create a repository with snapshots and injected mock tracer."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            snapshot_store=snapshot_store,
            enable_tracing=True,
        )
        # Inject mock tracer
        repo._tracer = mock_tracer
        repo._enable_tracing = True
        return repo

    @pytest.mark.asyncio
    async def test_save_creates_span(self, traced_repo, mock_tracer):
        """save creates a span with correct name."""
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")

        await traced_repo.save(aggregate)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.repository.save"

    @pytest.mark.asyncio
    async def test_save_span_attributes(self, traced_repo, mock_tracer):
        """save span includes correct standard attributes."""
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")

        await traced_repo.save(aggregate)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 1
        assert ATTR_VERSION in attributes
        assert attributes[ATTR_VERSION] == 1

    @pytest.mark.asyncio
    async def test_load_creates_span(self, traced_repo, mock_tracer, event_store):
        """load creates a span with correct name."""
        # First create an aggregate
        aggregate_id = uuid4()
        event = TracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            aggregate_version=1,
            value="test",
        )
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            events=[event],
            expected_version=0,
        )

        await traced_repo.load(aggregate_id)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.repository.load"

    @pytest.mark.asyncio
    async def test_load_span_attributes(self, traced_repo, mock_tracer, event_store):
        """load span includes correct standard attributes."""
        aggregate_id = uuid4()
        event = TracingTestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            aggregate_version=1,
            value="test",
        )
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            events=[event],
            expected_version=0,
        )

        await traced_repo.load(aggregate_id)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, traced_repo, mock_tracer):
        """exists creates a span with correct name."""
        aggregate_id = uuid4()

        await traced_repo.exists(aggregate_id)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.repository.exists"

    @pytest.mark.asyncio
    async def test_exists_span_attributes(self, traced_repo, mock_tracer):
        """exists span includes correct standard attributes."""
        aggregate_id = uuid4()

        await traced_repo.exists(aggregate_id)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"

    @pytest.mark.asyncio
    async def test_create_snapshot_creates_span(self, traced_repo_with_snapshots, mock_tracer):
        """create_snapshot creates a span with correct name."""
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()  # Simulate already saved

        await traced_repo_with_snapshots.create_snapshot(aggregate)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.repository.create_snapshot"

    @pytest.mark.asyncio
    async def test_create_snapshot_span_attributes(self, traced_repo_with_snapshots, mock_tracer):
        """create_snapshot span includes correct standard attributes."""
        aggregate_id = uuid4()
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        await traced_repo_with_snapshots.create_snapshot(aggregate)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "TracingTest"
        assert ATTR_VERSION in attributes
        assert attributes[ATTR_VERSION] == 1


# ============================================================================
# Tracing Disabled Tests
# ============================================================================


class TestAggregateRepositoryTracingDisabled:
    """Tests for AggregateRepository behavior when tracing is disabled."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_save_works_without_tracing(self, event_store):
        """save works correctly when tracing is disabled."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=False,
        )

        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")

        await repo.save(aggregate)

        # Events should be committed
        assert not aggregate.has_uncommitted_events

    @pytest.mark.asyncio
    async def test_load_works_without_tracing(self, event_store):
        """load works correctly when tracing is disabled."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=False,
        )

        # Create and save aggregate
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        # Load should work
        loaded = await repo.load(aggregate.aggregate_id)
        assert loaded.version == 1
        assert loaded.state.value == "test"

    @pytest.mark.asyncio
    async def test_exists_works_without_tracing(self, event_store):
        """exists works correctly when tracing is disabled."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=False,
        )

        aggregate_id = uuid4()

        # Should return False for non-existent aggregate
        assert await repo.exists(aggregate_id) is False

        # Create aggregate
        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        await repo.save(aggregate)

        # Should return True after creating aggregate
        assert await repo.exists(aggregate_id) is True

    @pytest.mark.asyncio
    async def test_create_snapshot_works_without_tracing(self, event_store, snapshot_store):
        """create_snapshot works correctly when tracing is disabled."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            snapshot_store=snapshot_store,
            enable_tracing=False,
        )

        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        snapshot = await repo.create_snapshot(aggregate)

        assert snapshot is not None
        assert snapshot.version == 1


# ============================================================================
# Span Dynamic Attributes Tests
# ============================================================================


class TestAggregateRepositorySpanDynamicAttributes:
    """Tests for dynamic span attributes set during operation."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer, span

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_load_sets_events_replayed_attribute(self, event_store, mock_tracer):
        """load sets events.replayed attribute on span."""
        tracer, span = mock_tracer

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        repo._tracer = tracer
        repo._enable_tracing = True

        # Create events
        aggregate_id = uuid4()
        events = [
            TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_type="TracingTest",
                aggregate_version=i + 1,
                value=f"test_{i}",
            )
            for i in range(3)
        ]
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            events=events,
            expected_version=0,
        )

        await repo.load(aggregate_id)

        # Verify dynamic attributes were set
        span.set_attribute.assert_any_call("events.replayed", 3)
        span.set_attribute.assert_any_call(ATTR_VERSION, 3)

    @pytest.mark.asyncio
    async def test_save_sets_success_attribute(self, event_store, mock_tracer):
        """save sets save.success attribute on span."""
        tracer, span = mock_tracer

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        repo._tracer = tracer
        repo._enable_tracing = True

        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")

        await repo.save(aggregate)

        # Verify dynamic attributes were set
        span.set_attribute.assert_any_call("save.success", True)
        span.set_attribute.assert_any_call("new_version", 1)

    @pytest.mark.asyncio
    async def test_exists_sets_exists_attribute(self, event_store, mock_tracer):
        """exists sets exists attribute on span."""
        tracer, span = mock_tracer

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        repo._tracer = tracer
        repo._enable_tracing = True

        aggregate_id = uuid4()
        await repo.exists(aggregate_id)

        # Verify dynamic attribute was set
        span.set_attribute.assert_any_call("exists", False)

    @pytest.mark.asyncio
    async def test_load_sets_snapshot_attributes_when_snapshot_used(
        self, event_store, snapshot_store, mock_tracer
    ):
        """load sets snapshot attributes when snapshot is used."""
        tracer, span = mock_tracer

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            snapshot_store=snapshot_store,
            enable_tracing=True,
        )
        repo._tracer = tracer
        repo._enable_tracing = True

        aggregate_id = uuid4()

        # Create a snapshot
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TracingTest",
            version=5,
            state={"value": "from_snapshot", "count": 5, "items": []},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await snapshot_store.save_snapshot(snapshot)

        await repo.load(aggregate_id)

        # Verify snapshot attributes were set
        span.set_attribute.assert_any_call("snapshot.used", True)
        span.set_attribute.assert_any_call("snapshot.version", 5)


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestAggregateRepositoryStandardAttributes:
    """Tests for standard attribute usage in AggregateRepository."""

    def test_imports_from_observability_module(self):
        """Verify AggregateRepository imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/aggregates/repository.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "AggregateRepository should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        """Verify AggregateRepository uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/aggregates/repository.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 4, f"Expected at least 4 ATTR_* usages, found {count}"


# ============================================================================
# Multiple Events Tests
# ============================================================================


class TestAggregateRepositoryTracingMultipleEvents:
    """Tests for tracing with multiple events."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore()

    @pytest.mark.asyncio
    async def test_multiple_events_count_attribute(self, event_store, mock_tracer):
        """Event count attribute reflects actual number of events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        repo._tracer = mock_tracer
        repo._enable_tracing = True

        aggregate = TracingTestAggregate(uuid4())
        for i in range(5):
            aggregate.do_something(f"test_{i}")

        await repo.save(aggregate)

        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert attributes[ATTR_EVENT_COUNT] == 5

    @pytest.mark.asyncio
    async def test_no_span_when_no_events_to_save(self, event_store, mock_tracer):
        """No span created when there are no uncommitted events."""
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            aggregate_type="TracingTest",
            enable_tracing=True,
        )
        repo._tracer = mock_tracer
        repo._enable_tracing = True

        aggregate = TracingTestAggregate(uuid4())
        # No events applied

        await repo.save(aggregate)

        # No span should be created for empty events
        mock_tracer.start_as_current_span.assert_not_called()
