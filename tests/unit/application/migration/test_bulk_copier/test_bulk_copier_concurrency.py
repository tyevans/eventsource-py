"""Unit tests for BulkCopier concurrency and live mirror overlap."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import EventEnvelope, ExpectedVersion
from tests.unit.application.migration.test_bulk_copier._fixtures import (
    _REGISTRY,
    TestEvent,
    _pos,
)


class TestBulkCopierOverlapWithLiveMirror:
    """The copy pass now runs with the dual-write interceptor installed,
    so the copier and the mirror can race on the same stream. The copier
    must tolerate events the mirror already landed without ever skipping
    events the mirror did not."""

    @pytest.mark.asyncio
    async def test_mixed_group_falls_back_per_event_and_copies_absent_events(self) -> None:
        """A batched group straddling an already-present event must not be
        skipped wholesale: the absent events still reach the target, in
        source stream order."""
        target_store = InMemoryEventStore("target", event_registry=_REGISTRY)
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        mirrored = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value="mirrored")
        absent = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value="absent")

        # A dual-write mirror already landed the first event.
        await target_store.append(stream, [mirrored], ExpectedVersion.no_stream())

        envelopes = [
            EventEnvelope(
                event=mirrored,
                stream_id=stream,
                stream_version=1,
                position=_pos("source", 1),
                stored_at=datetime.now(UTC),
            ),
            EventEnvelope(
                event=absent,
                stream_id=stream,
                stream_version=2,
                position=_pos("source", 2),
                stored_at=datetime.now(UTC),
            ),
        ]

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        last_pos = await copier._write_batch(uuid4(), tenant_id, envelopes)

        assert last_pos is not None
        assert await target_store.get_stream_version(stream) == 2
        target_ids = [env.event.event_id async for env in target_store.read_stream(stream)]
        assert target_ids == [mirrored.event_id, absent.event_id]

    @pytest.mark.asyncio
    async def test_conflicting_append_rechecks_and_confirms_duplicate(self) -> None:
        """A mirror landing between the version read and the append raises
        OptimisticLockError (not DuplicateEventError); the copier re-reads
        the version and the retry confirms the duplicate. No mapping is
        recorded for the event this run never appended."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(side_effect=[0, 1])
        target_store.append = AsyncMock(
            side_effect=[
                OptimisticLockError(aggregate_id, 0, 1),
                DuplicateEventError("the mirror landed this very event"),
            ]
        )
        position_mapper = AsyncMock()

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )

        envelope = EventEnvelope(
            event=event,
            stream_id=stream,
            stream_version=1,
            position=_pos("source", 1),
            stored_at=datetime.now(UTC),
        )

        last_pos = await copier._write_batch(uuid4(), tenant_id, [envelope])

        assert last_pos is None
        assert target_store.append.call_count == 2
        # The retry used the re-read version.
        retry_expected = target_store.append.call_args.args[2]
        assert retry_expected == ExpectedVersion.exact(1)
        position_mapper.record_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_sustained_conflict_fails_the_copy_honestly(self) -> None:
        """A stream that keeps moving under the copier past the retry bound
        fails the copy rather than spinning or skipping."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=OptimisticLockError(aggregate_id, 0, 1))

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        envelope = EventEnvelope(
            event=event,
            stream_id=stream,
            stream_version=1,
            position=_pos("source", 1),
            stored_at=datetime.now(UTC),
        )

        with pytest.raises(BulkCopyError, match="kept moving under the copier"):
            await copier._write_batch(uuid4(), tenant_id, [envelope])
