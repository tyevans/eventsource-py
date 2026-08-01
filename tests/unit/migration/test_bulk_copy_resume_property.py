"""Property: a bulk copy resumed after a crash converges on a source-equal target.

Spec-mandated (store-retirement slice (c), FRD ss7): a resume that re-reads
from an inclusive `last_source_position` re-appends events the target
already has. The copy is only correct because that re-append raises
`DuplicateEventError` and is counted as already-copied rather than as a
failure. This property drives arbitrary stream shapes, batch sizes and
crash points through two `BulkCopier` runs -- one that is cut off partway
and a second that resumes from the persisted checkpoint -- and asserts the
target converges to exactly the source's events, in order, with no
duplicates.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.migration.bulk_copier import BulkCopier
from eventsource.migration.consistency import ConsistencyVerifier
from eventsource.migration.models import Migration, MigrationConfig, MigrationPhase, PositionMapping
from eventsource.migration.position_mapper import PositionMapper
from eventsource.ports import ExpectedVersion, FeedReadOptions, Position


class ResumePropertyEvent(DomainEvent):
    """Minimal event for the resume property; only identity/order matters."""

    event_type: str = "ResumePropertyEvent"
    aggregate_type: str = "ResumePropertyAggregate"


class FakePositionMappingRepository:
    """Minimal in-memory double for `PositionMappingRepository`.

    Only `create` is exercised: `BulkCopier`'s per-event append path calls
    `PositionMapper.record_mapping`, which calls `repo.create` and nothing
    else. Mappings are appended in call order, which is ascending
    source-position order given `BulkCopier` streams the source feed in
    order -- the precondition the real repositories document.
    """

    def __init__(self) -> None:
        self.mappings: list[PositionMapping] = []

    async def create(self, mapping: PositionMapping) -> int:
        self.mappings.append(mapping)
        return len(self.mappings)


async def _append_streams(
    store: InMemoryEventStore,
    tenant_id: UUID,
    streams: list[int],
) -> None:
    """Append `streams[i]` events to the i-th distinct stream, in order."""
    for count in streams:
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="ResumePropertyAggregate")
        events = [
            ResumePropertyEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
            for _ in range(count)
        ]
        await store.append(stream, events, ExpectedVersion.no_stream())


def _make_migration(
    migration_id: UUID,
    tenant_id: UUID,
    batch_size: int,
    events_total: int,
    *,
    events_copied: int = 0,
    last_source_position: Position | None = None,
    last_target_position: Position | None = None,
) -> Migration:
    return Migration(
        id=migration_id,
        tenant_id=tenant_id,
        source_store_id="source",
        target_store_id="target",
        phase=MigrationPhase.BULK_COPY,
        config=MigrationConfig(batch_size=batch_size, max_bulk_copy_rate=1_000_000),
        events_total=events_total,
        events_copied=events_copied,
        last_source_position=last_source_position,
        last_target_position=last_target_position,
    )


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    streams=st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=6),
    batch_size=st.integers(min_value=1, max_value=4),
    crash_after=st.integers(min_value=0, max_value=20),
)
async def test_bulk_copy_resumes_to_a_source_equal_target(
    streams: list[int], batch_size: int, crash_after: int
) -> None:
    """Random stream shapes, batch sizes and crash points: a resumed copy
    converges on a target whose streams equal the source's.

    A resume that re-reads from an inclusive position re-appends events the
    target already has; the copy is only correct because that re-append
    raises DuplicateEventError and is counted as already-copied. Remove that
    handler and this property fails immediately.
    """
    tenant_id = uuid4()
    migration_id = uuid4()
    events_total = sum(streams)
    crash_after = min(crash_after, events_total)

    source_store = InMemoryEventStore("source")
    target_store = InMemoryEventStore("target")
    await _append_streams(source_store, tenant_id, streams)

    mapping_repo = FakePositionMappingRepository()
    mapper = PositionMapper(mapping_repo)

    # First (interrupted) run: consume progress until at least crash_after
    # events have been persisted to the checkpoint, then stop consuming the
    # generator -- simulating a crash. No further batches are processed.
    migration1 = _make_migration(migration_id, tenant_id, batch_size, events_total)
    copier1 = BulkCopier(
        source_store=source_store,
        target_store=target_store,
        migration_repo=AsyncMock(),
        position_mapper=mapper,
        enable_tracing=False,
    )

    checkpoint_events_copied = 0
    checkpoint_source_position: Position | None = None
    checkpoint_target_position: Position | None = None

    async for progress in copier1.run(migration1):
        checkpoint_events_copied = progress.events_copied
        checkpoint_source_position = progress.last_source_position
        checkpoint_target_position = progress.last_target_position
        if progress.events_copied >= crash_after:
            break

    # Second run: resume from the persisted checkpoint over the same stores.
    migration2 = _make_migration(
        migration_id,
        tenant_id,
        batch_size,
        events_total,
        events_copied=checkpoint_events_copied,
        last_source_position=checkpoint_source_position,
        last_target_position=checkpoint_target_position,
    )
    copier2 = BulkCopier(
        source_store=source_store,
        target_store=target_store,
        migration_repo=AsyncMock(),
        position_mapper=mapper,
        enable_tracing=False,
    )

    async for _ in copier2.run(migration2):
        pass

    verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)
    report = await verifier.verify_tenant_consistency(tenant_id)
    assert report.is_consistent, report.violations

    target_event_ids = [
        envelope.event.event_id
        async for envelope in target_store.read_all(None, FeedReadOptions(tenant_id=tenant_id))
    ]
    assert len(target_event_ids) == len(set(target_event_ids))
    assert len(target_event_ids) == events_total

    # The position mapper is engaged for every append in this test (a
    # mapper is always configured, so BulkCopier never takes the
    # no-mapper batch fast path), and a duplicate append records no
    # mapping -- so exactly one mapping should exist per event that
    # ultimately landed in the target, covering it exactly once.
    mapped_event_ids = [mapping.event_id for mapping in mapping_repo.mappings]
    assert len(mapped_event_ids) == len(set(mapped_event_ids))
    assert set(mapped_event_ids) == set(target_event_ids)
    assert len(mapping_repo.mappings) == events_total

    # Mappings are recorded in the order BulkCopier appends them, which is
    # ascending source-position order -- the precondition the real
    # PositionMappingRepository implementations document.
    mapped_source_positions = [mapping.source_position for mapping in mapping_repo.mappings]
    assert mapped_source_positions == sorted(mapped_source_positions)
