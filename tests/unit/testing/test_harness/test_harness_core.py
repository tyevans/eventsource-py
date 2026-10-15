"""Tests for test harness instantiation, properties, tracing, and repr."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.ports.readmodels.model import ReadModel
from eventsource.testing import EventAssertions, InMemoryTestHarness
from tests.unit.testing.test_harness._fixtures import HarnessSampleEvent


class SampleReadModel(ReadModel):
    """Sample read model for testing create_read_model_repository."""

    name: str


class TestHarnessInstantiation:
    """Tests for harness instantiation."""

    def test_harness_can_be_created(self) -> None:
        """InMemoryTestHarness can be instantiated."""
        harness = InMemoryTestHarness()
        assert harness is not None

    def test_harness_creates_all_components(self) -> None:
        """Harness creates all required components on instantiation."""
        harness = InMemoryTestHarness()

        assert harness._event_store is not None
        assert harness._event_bus is not None
        assert harness._checkpoint_repo is not None
        assert harness._dlq_repo is not None
        assert harness._snapshot_store is not None


class TestHarnessProperties:
    """Tests for harness property accessors."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    def test_event_store_type(self, harness: InMemoryTestHarness) -> None:
        """event_store returns InMemoryEventStore."""
        assert isinstance(harness.event_store, InMemoryEventStore)

    def test_event_bus_type(self, harness: InMemoryTestHarness) -> None:
        """event_bus returns InMemoryEventBus."""
        assert isinstance(harness.event_bus, InMemoryEventBus)

    def test_checkpoint_repo_type(self, harness: InMemoryTestHarness) -> None:
        """checkpoint_repo returns InMemoryCheckpointRepository."""
        assert isinstance(harness.checkpoint_repo, InMemoryCheckpointRepository)

    def test_dlq_repo_type(self, harness: InMemoryTestHarness) -> None:
        """dlq_repo returns InMemoryDLQRepository."""
        assert isinstance(harness.dlq_repo, InMemoryDLQRepository)

    def test_snapshot_store_type(self, harness: InMemoryTestHarness) -> None:
        """snapshot_store returns InMemorySnapshotStore."""
        assert isinstance(harness.snapshot_store, InMemorySnapshotStore)

    def test_same_event_store_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """event_store returns same instance on repeated access."""
        assert harness.event_store is harness.event_store

    def test_same_event_bus_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """event_bus returns same instance on repeated access."""
        assert harness.event_bus is harness.event_bus

    def test_same_checkpoint_repo_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """checkpoint_repo returns same instance on repeated access."""
        assert harness.checkpoint_repo is harness.checkpoint_repo

    def test_same_dlq_repo_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """dlq_repo returns same instance on repeated access."""
        assert harness.dlq_repo is harness.dlq_repo

    def test_same_snapshot_store_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """snapshot_store returns same instance on repeated access."""
        assert harness.snapshot_store is harness.snapshot_store

    @pytest.mark.asyncio
    async def test_assertions_property_bound_to_published_events(
        self, harness: InMemoryTestHarness
    ) -> None:
        """assertions returns EventAssertions reflecting current published_events."""
        assert isinstance(harness.assertions, EventAssertions)
        harness.assertions.assert_no_events_published()

        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        harness.assertions.assert_event_count(1)
        assert harness.assertions.assert_event_published(HarnessSampleEvent) == event


class TestHarnessTracingDisabled:
    """Tests for tracing configuration."""

    def test_event_bus_tracing_disabled(self) -> None:
        """Event bus is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.event_bus._enable_tracing is False

    def test_checkpoint_repo_tracing_disabled(self) -> None:
        """Checkpoint repo is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.checkpoint_repo._enable_tracing is False

    def test_dlq_repo_tracing_disabled(self) -> None:
        """DLQ repo is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.dlq_repo._enable_tracing is False

    def test_snapshot_store_tracing_disabled(self) -> None:
        """Snapshot store is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.snapshot_store._enable_tracing is False

    @pytest.mark.asyncio
    async def test_create_read_model_repository_creates_repo_with_tracing_disabled(
        self,
    ) -> None:
        """create_read_model_repository returns an InMemoryReadModelRepository with tracing disabled."""
        harness = InMemoryTestHarness()
        repo = harness.create_read_model_repository(SampleReadModel)

        assert isinstance(repo, InMemoryReadModelRepository)
        assert repo._enable_tracing is False

        model_id = uuid4()
        await repo.save(SampleReadModel(id=model_id, name="Alice"))
        loaded = await repo.get(model_id)
        assert loaded is not None
        assert loaded.name == "Alice"


class TestHarnessRepr:
    """Tests for __repr__ method."""

    def test_repr_shows_counts_initially(self) -> None:
        """__repr__ shows event counts initially."""
        harness = InMemoryTestHarness()
        repr_str = repr(harness)

        assert "InMemoryTestHarness" in repr_str
        assert "published=" in repr_str
        assert "published=0" in repr_str

    @pytest.mark.asyncio
    async def test_repr_shows_published_count(self) -> None:
        """__repr__ shows correct published event count."""
        harness = InMemoryTestHarness()
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        repr_str = repr(harness)

        assert "published=1" in repr_str

    @pytest.mark.asyncio
    async def test_outbox_repo_publish_events_and_len(self) -> None:
        """outbox_repo, publish_events(persist=...), and len(harness) work together."""
        from eventsource.adapters.memory.outbox import InMemoryOutboxRepository

        harness = InMemoryTestHarness()
        assert isinstance(harness.outbox_repo, InMemoryOutboxRepository)
        assert harness.outbox_repo._enable_tracing is False
        assert len(harness) == 0

        await harness.publish_events()
        assert len(harness) == 0

        agg_id = uuid4()
        ev1 = HarnessSampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        ev2 = HarnessSampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)

        await harness.publish_events(ev1)
        assert len(harness) == 1
        assert [e async for e in harness.event_store.read_all()] == []

        await harness.publish_events(ev2, persist=True)
        assert len(harness) == 2
        assert len([e async for e in harness.event_store.read_all()]) == 1

    @pytest.mark.asyncio
    async def test_event_store_len_get_event_count_and_clear(self) -> None:
        """len(event_store), get_event_count(), and clear() track and reset stored events."""
        harness = InMemoryTestHarness()
        assert len(harness.event_store) == 0
        assert await harness.event_store.get_event_count() == 0

        agg_id = uuid4()
        ev1 = HarnessSampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1)
        ev2 = HarnessSampleEvent(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2)
        await harness.append_events(ev1, ev2)

        assert len(harness.event_store) == 2
        assert await harness.event_store.get_event_count() == 2

        harness.event_store.clear()
        assert len(harness.event_store) == 0
        assert await harness.event_store.get_event_count() == 0
        assert [e async for e in harness.event_store.read_all()] == []
        assert not await harness.event_store.event_exists(ev1.event_id)
        await harness.append_events(ev1)
        assert len(harness.event_store) == 1
