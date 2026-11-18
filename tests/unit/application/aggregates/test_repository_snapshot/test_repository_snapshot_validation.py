"""Load-side validation of snapshots against the event stream (TASK-0469).

A snapshot is only trusted when it matches the requested aggregate identity
and does not claim a version beyond the stream head. A snapshot that fails
either check is a miss: load replays the stream from version 0.

This module covers the snapshot-ahead-of-stream check. Identity and
empty-stream checks live in ``test_repository_snapshot_validation_guards``.
"""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.snapshotting import (
    SnapshotMissReason,
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
)
from tests.unit.application.aggregates.test_repository_snapshot._validation_support import (
    LOGGER_NAME,
    append_events,
    build_repo,
    make_snapshot,
    new_event_store,
)


@pytest.fixture(autouse=True)
def _reset_miss_counts() -> None:
    reset_snapshot_miss_counts()


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return new_event_store()


class TestSnapshotAheadOfStream:
    @pytest.mark.asyncio
    async def test_forged_future_version_is_replayed_not_trusted(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 999))

        loaded = await build_repo(event_store, snapshots).load(aggregate_id)

        assert loaded.version == 2
        assert loaded.state is not None
        assert loaded.state.value == "event_1"
        assert loaded.state.count == 2
        assert snapshot_miss_counts()[SnapshotMissReason.VERSION_AHEAD_OF_STREAM] == 1

    @pytest.mark.asyncio
    async def test_save_succeeds_after_forged_snapshot_is_rejected(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 999))
        repo = build_repo(event_store, snapshots)

        aggregate = await repo.load(aggregate_id)
        aggregate.do_something("after-forgery")
        await repo.save(aggregate)

        reloaded = await build_repo(event_store).load(aggregate_id)
        assert reloaded.version == 3
        assert reloaded.state is not None
        assert reloaded.state.value == "after-forgery"

    @pytest.mark.asyncio
    async def test_miss_logs_warning_without_state_payload(
        self, event_store: InMemoryEventStore, caplog: pytest.LogCaptureFixture
    ) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 2)
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(make_snapshot(aggregate_id, 999))

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            await build_repo(event_store, snapshots).load(aggregate_id)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert str(aggregate_id) in warnings[0].getMessage()
        assert "forged-state-marker" not in caplog.text

    @pytest.mark.asyncio
    async def test_snapshot_at_stream_head_is_still_used(
        self, event_store: InMemoryEventStore
    ) -> None:
        aggregate_id = uuid4()
        await append_events(event_store, aggregate_id, 5)
        snapshots = InMemorySnapshotStore(enable_tracing=False)
        await snapshots.save_snapshot(
            make_snapshot(
                aggregate_id, 5, state={"value": "from-snapshot", "count": 5, "items": []}
            )
        )

        loaded = await build_repo(event_store, snapshots).load(aggregate_id)

        assert (loaded.version, loaded.state.value) == (5, "from-snapshot")  # type: ignore[union-attr]
        assert snapshot_miss_counts() == {}
