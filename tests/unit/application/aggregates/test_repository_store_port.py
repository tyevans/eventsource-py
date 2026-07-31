"""The repository's use of the AggregateStore port.

Pins the four translation rules from the legacy ABC (spec §1.1, §1.2):
exact-version append, the +1 from_version shift after a snapshot,
get_stream_version for existence/version probes, and
OptimisticLockError as the only conflict signal.
"""

from uuid import UUID, uuid4

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.events.base import DomainEvent
from eventsource.exceptions import AggregateNotFoundError, OptimisticLockError
from eventsource.ports import AggregateStore, collect


class CounterState(BaseModel):
    """Simple state for testing."""

    counter_id: UUID
    value: int = 0


class CounterIncremented(DomainEvent):
    """Event for incrementing counter."""

    event_type: str = "CounterIncremented"
    aggregate_type: str = "Counter"
    increment: int = 1


class CounterAggregate(AggregateRoot[CounterState]):
    """Simple counter aggregate for testing."""

    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value + event.increment}
            )

    def increment(self, amount: int = 1) -> None:
        """Command: Increment the counter."""
        event = CounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self.apply_event(event)


def _stream(aggregate_id: UUID) -> StreamId:
    return StreamId(aggregate_id=aggregate_id, category="Counter")


@pytest.mark.asyncio
async def test_memory_store_satisfies_aggregate_store_port() -> None:
    store: AggregateStore = MemoryEventStore()
    assert store is not None


@pytest.mark.asyncio
async def test_save_writes_stream_version_equal_to_event_count() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    aggregate.increment()
    aggregate.increment()
    aggregate.increment()

    await repo.save(aggregate)

    assert await store.get_stream_version(_stream(aggregate_id)) == 3


@pytest.mark.asyncio
async def test_save_twice_uses_correct_expected_version() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    aggregate.increment()
    await repo.save(aggregate)

    aggregate.increment()
    await repo.save(aggregate)

    assert await store.get_stream_version(_stream(aggregate_id)) == 2


@pytest.mark.asyncio
async def test_save_stale_aggregate_raises_optimistic_lock_error() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    aggregate.increment()
    await repo.save(aggregate)

    stale = repo.create_new(aggregate_id)
    stale.increment()

    with pytest.raises(OptimisticLockError):
        await repo.save(stale)


@pytest.mark.asyncio
async def test_save_with_no_uncommitted_events_is_a_noop() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate = repo.create_new(uuid4())

    await repo.save(aggregate)  # must not raise ValueError


@pytest.mark.asyncio
async def test_load_with_no_events_and_no_snapshot_raises_not_found() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )

    with pytest.raises(AggregateNotFoundError):
        await repo.load(uuid4())


@pytest.mark.asyncio
async def test_load_after_snapshot_replays_only_events_after_snapshot_version() -> None:
    store = MemoryEventStore()
    snapshot_store = InMemorySnapshotStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    for _ in range(5):
        aggregate.increment()
    await repo.save(aggregate)

    stream = _stream(aggregate_id)
    events = await collect(store.read_stream(stream))
    assert len(events) == 5  # sanity: full stream present

    # Build a snapshot at version 2 directly, without loading through repo.
    partial = repo.create_new(aggregate_id)
    partial.load_from_history([e.event for e in events[:2]])
    await repo.create_snapshot(partial)

    reloaded = await repo.load(aggregate_id)
    assert reloaded.version == 5


@pytest.mark.asyncio
async def test_exists_false_before_save_true_after() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate_id = uuid4()
    assert await repo.exists(aggregate_id) is False

    aggregate = repo.create_new(aggregate_id)
    aggregate.increment()
    await repo.save(aggregate)

    assert await repo.exists(aggregate_id) is True


@pytest.mark.asyncio
async def test_get_version_zero_for_unknown_and_true_version_after_save() -> None:
    store = MemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
    )
    aggregate_id = uuid4()
    assert await repo.get_version(aggregate_id) == 0

    aggregate = repo.create_new(aggregate_id)
    aggregate.increment()
    aggregate.increment()
    await repo.save(aggregate)

    assert await repo.get_version(aggregate_id) == 2


@settings(max_examples=50, deadline=None)
@given(
    batches=st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=6),
    snapshot_after=st.integers(min_value=0, max_value=10),
)
@pytest.mark.asyncio
async def test_load_reconstructs_version_regardless_of_snapshot_point(
    batches: list[int], snapshot_after: int
) -> None:
    """Loading yields the same version whether or not a snapshot truncates the replay.

    Random batch shapes, random snapshot points: the snapshot's version plus
    the replayed remainder must always equal the total event count. This is
    the executable form of the from_version translation rule -- an off-by-one
    in either direction breaks it for some (batches, snapshot_after) pair.
    """
    store = MemoryEventStore()
    snapshot_store = InMemorySnapshotStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    total = 0
    for batch in batches:
        for _ in range(batch):
            aggregate.increment()
            total += 1
        await repo.save(aggregate)

    snap_point = min(snapshot_after, total)
    if snap_point > 0:
        stream = _stream(aggregate_id)
        events = await collect(store.read_stream(stream))
        partial = repo.create_new(aggregate_id)
        partial.load_from_history([e.event for e in events[:snap_point]])
        await repo.create_snapshot(partial)

    repo2 = AggregateRepository(
        event_store=store,
        aggregate_factory=CounterAggregate,
        snapshot_store=snapshot_store,
        snapshot_mode="manual",
    )
    loaded = await repo2.load(aggregate_id)
    assert loaded.version == total
