"""Bulk copy must verify an already-present event before counting it as copied.

Blackbox: an in-memory source feeds a real in-memory target through
`BulkCopier.run`. The target is pre-seeded with an event that shares the
source event's event_id. The copy must succeed only when the stored event
matches the source in stream, version, and payload.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from tests.unit.application.migration.test_bulk_copier._fixtures import (
    _REGISTRY,
    TestEvent,
    create_migration,
    create_test_events,
    make_event_generator,
)


def _copier(source_events: list, target: InMemoryEventStore) -> BulkCopier:
    source = MagicMock(read_all=make_event_generator(source_events))
    return BulkCopier(source, target, AsyncMock(), enable_tracing=False)


async def _run_last_progress(copier: BulkCopier, tenant_id, events_total: int):
    migration = create_migration(tenant_id=tenant_id, events_total=events_total, batch_size=100)
    progress = [p async for p in copier.run(migration)]
    return progress[-1]


class TestDuplicateEventVerification:
    async def test_identical_event_already_in_target_counts_as_copied(self) -> None:
        tenant_id = uuid4()
        source_events = create_test_events(1, tenant_id)
        envelope = source_events[0]
        target = InMemoryEventStore("target", event_registry=_REGISTRY)
        # The target already holds this exact event at the same stream and version.
        await target.append(envelope.stream_id, [envelope.event], ExpectedVersion.no_stream())

        final = await _run_last_progress(_copier(source_events, target), tenant_id, 1)

        assert final.events_copied == 1

    async def test_same_event_id_with_different_payload_raises(self) -> None:
        tenant_id = uuid4()
        source_events = create_test_events(1, tenant_id)
        envelope = source_events[0]
        target = InMemoryEventStore("target", event_registry=_REGISTRY)
        tampered = TestEvent(
            event_id=envelope.event.event_id,
            aggregate_id=envelope.stream_id.aggregate_id,
            tenant_id=tenant_id,
            value="not the source payload",
        )
        await target.append(envelope.stream_id, [tampered], ExpectedVersion.no_stream())

        with pytest.raises(BulkCopyError, match="payload differs"):
            await _run_last_progress(_copier(source_events, target), tenant_id, 1)

    async def test_same_event_id_in_another_stream_raises(self) -> None:
        tenant_id = uuid4()
        source_events = create_test_events(1, tenant_id)
        envelope = source_events[0]
        target = InMemoryEventStore("target", event_registry=_REGISTRY)
        other_stream = StreamId(aggregate_id=uuid4(), category="TestAggregate")
        elsewhere = TestEvent(
            event_id=envelope.event.event_id,
            aggregate_id=other_stream.aggregate_id,
            tenant_id=tenant_id,
        )
        await target.append(other_stream, [elsewhere], ExpectedVersion.no_stream())

        with pytest.raises(BulkCopyError, match="already exists in the target but differs"):
            await _run_last_progress(_copier(source_events, target), tenant_id, 1)
