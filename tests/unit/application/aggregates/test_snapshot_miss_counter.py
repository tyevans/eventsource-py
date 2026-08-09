"""Tests for the snapshot miss counter (ADR 0017's recorded negative).

Every snapshot failure degrades to a full event replay, which is correct but
invisible: before this counter the only in-band evidence was a log line. Each
test here drives a real read path and asserts the reason it produces is counted
non-zero -- per `.claude/rules/recurring-defects.md` §3, a counter is only
covered by a test that asserts it *non-zero* under the condition it counts.

The distinction the counter exists to draw is permanence: a store outage is
transient and hits every aggregate, while a corrupt row is permanent for one
aggregate and costs a full replay on every load until it is rewritten.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.snapshotting import (
    SnapshotMissReason,
    read_valid_snapshot,
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.ports.snapshots import Snapshot

_REGISTRY = EventRegistry()


class _State(BaseModel):
    value: str = ""


@register_event(registry=_REGISTRY)
class _Happened(DomainEvent):
    aggregate_type: str = "Counted"
    value: str = "x"


class _Counted(AggregateRoot[_State]):
    aggregate_type = "Counted"
    schema_version = 1

    def _apply(self, event: DomainEvent) -> None:
        self._state = _State(value=getattr(event, "value", ""))

    def _get_initial_state(self) -> _State:
        return _State()

    def do(self, value: str) -> None:
        self.apply_event(
            _Happened(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                value=value,
            )
        )


class _UnrestorableCounted(_Counted):
    """Loads its snapshot row fine, then cannot rebuild state from it.

    This is the shape a corrupt payload actually takes with the shipped
    adapters: they return the row intact and the failure surfaces when the
    aggregate rebuilds state.
    """

    def _restore_from_snapshot(self, state: dict, version: int) -> None:
        raise ValueError("state is not restorable")


class _OutageStore(InMemorySnapshotStore):
    """A store that is simply unreachable. Transient, affects everything."""

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        raise RuntimeError("store down")


class _CorruptionSignallingStore(InMemorySnapshotStore):
    """A store that raises the documented type for an unusable payload.

    This is the contract ADR 0017 publishes for `SnapshotStore` implementors;
    no in-tree adapter raises it, so this double stands in for a conforming
    third-party store.
    """

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        raise SnapshotDeserializationError(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
        )


def _snapshot_for(aggregate_id: UUID, *, schema_version: int = 1) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Counted",
        version=1,
        state={"value": "stored"},
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )


@pytest.fixture(autouse=True)
def _clean_counts():
    reset_snapshot_miss_counts()
    yield
    reset_snapshot_miss_counts()


class TestEachReasonIsCountedNonZero:
    """One test per reason, each driving the real read path."""

    async def test_missing_snapshot_is_counted(self):
        result = await read_valid_snapshot(InMemorySnapshotStore(), uuid4(), "Counted", _Counted)

        assert result is None
        assert snapshot_miss_counts()[SnapshotMissReason.MISSING] == 1

    async def test_schema_mismatch_is_counted(self):
        store = InMemorySnapshotStore()
        aggregate_id = uuid4()
        await store.save_snapshot(_snapshot_for(aggregate_id, schema_version=999))

        result = await read_valid_snapshot(store, aggregate_id, "Counted", _Counted)

        assert result is None
        assert snapshot_miss_counts()[SnapshotMissReason.SCHEMA_MISMATCH] == 1

    async def test_store_outage_is_counted_as_store_error(self):
        result = await read_valid_snapshot(_OutageStore(), uuid4(), "Counted", _Counted)

        assert result is None
        assert snapshot_miss_counts()[SnapshotMissReason.STORE_ERROR] == 1

    async def test_store_signalled_corruption_is_counted_separately(self):
        result = await read_valid_snapshot(
            _CorruptionSignallingStore(), uuid4(), "Counted", _Counted
        )

        assert result is None
        counts = snapshot_miss_counts()
        assert counts[SnapshotMissReason.DESERIALIZATION_ERROR] == 1
        # The whole point of the split: corruption must not hide in the
        # transient bucket, because waiting does not fix it.
        assert SnapshotMissReason.STORE_ERROR not in counts

    async def test_state_restore_failure_is_counted(self):
        event_store = InMemoryEventStore(event_registry=_REGISTRY)
        snapshot_store = InMemorySnapshotStore()
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=_UnrestorableCounted,
            snapshot_store=snapshot_store,
        )

        aggregate = _UnrestorableCounted(uuid4())
        aggregate.do("first")
        await repo.save(aggregate)
        await snapshot_store.save_snapshot(_snapshot_for(aggregate.aggregate_id))

        loaded = await repo.load(aggregate.aggregate_id)

        # Control flow is unchanged: the load still succeeds, via full replay.
        assert loaded is not None
        assert loaded.state.value == "first"
        assert snapshot_miss_counts()[SnapshotMissReason.STATE_RESTORE_FAILED] == 1


class TestCounterDoesNotFireOnTheHappyPath:
    async def test_a_usable_snapshot_counts_nothing(self):
        store = InMemorySnapshotStore()
        aggregate_id = uuid4()
        await store.save_snapshot(_snapshot_for(aggregate_id))

        result = await read_valid_snapshot(store, aggregate_id, "Counted", _Counted)

        assert result is not None
        assert snapshot_miss_counts() == {}


class TestInstrumentationOnly:
    async def test_every_reason_still_degrades_to_none(self):
        """No reason changes control flow; all of them fall back to replay."""
        store_with_stale = InMemorySnapshotStore()
        stale_id = uuid4()
        await store_with_stale.save_snapshot(_snapshot_for(stale_id, schema_version=999))

        assert (
            await read_valid_snapshot(InMemorySnapshotStore(), uuid4(), "Counted", _Counted) is None
        )
        assert await read_valid_snapshot(store_with_stale, stale_id, "Counted", _Counted) is None
        assert await read_valid_snapshot(_OutageStore(), uuid4(), "Counted", _Counted) is None
        assert (
            await read_valid_snapshot(_CorruptionSignallingStore(), uuid4(), "Counted", _Counted)
            is None
        )

    async def test_counts_accumulate_across_reads(self):
        """A poisoned row costs a replay on *every* load; the counter shows it."""
        for _ in range(3):
            await read_valid_snapshot(_OutageStore(), uuid4(), "Counted", _Counted)

        assert snapshot_miss_counts()[SnapshotMissReason.STORE_ERROR] == 3
