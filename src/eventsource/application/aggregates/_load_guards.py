"""Trust checks applied to a stream and its snapshot during AggregateRepository.load.

The logger keeps the public name of the repository query module, so existing
log filters continue to match these warnings.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.aggregates.snapshotting import (
    SnapshotMissReason,
    record_snapshot_miss,
)
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.observability.error_summary import error_summary

if TYPE_CHECKING:
    from eventsource.domain import StreamId
    from eventsource.ports.snapshots import Snapshot
    from eventsource.ports.store import AggregateStore

logger = logging.getLogger("eventsource.application.aggregates.repository_query")


def discard_snapshot_ahead_of_stream(
    aggregate_type: str, aggregate_id: UUID, snapshot_version: int, head: int
) -> None:
    """Record and log a snapshot that claims a version past the stream head."""
    record_snapshot_miss(SnapshotMissReason.VERSION_AHEAD_OF_STREAM, aggregate_type)
    logger.warning(
        "Snapshot for %s/%s is at version %d but the stream head is %d. Ignoring snapshot and replaying from the start.",
        aggregate_type, aggregate_id, snapshot_version, head,
    )  # fmt: skip


async def resolve_empty_tail(
    event_store: AggregateStore,
    stream: StreamId,
    aggregate_id: UUID,
    aggregate_type: str,
    snapshot: Snapshot,
) -> tuple[Snapshot | None, list[Any]]:
    """Confirm a snapshot whose tail read returned no events.

    An empty tail proves nothing about the head: confirm it with one lookup.
    Raises AggregateNotFoundError when the stream has no events. Returns the
    snapshot unchanged when it is at or before the head. Otherwise it discards
    the snapshot and returns a full replay of the stream.
    """
    head = await event_store.get_stream_version(stream)
    if head == 0:
        raise AggregateNotFoundError(aggregate_id, aggregate_type)
    if snapshot.version <= head:
        return snapshot, []
    discard_snapshot_ahead_of_stream(aggregate_type, aggregate_id, snapshot.version, head)
    return None, [env.event async for env in event_store.read_stream(stream)]


def restore_snapshot_or_fall_back(
    aggregate: Any, snapshot: Snapshot, aggregate_type: str, aggregate_id: UUID
) -> bool:
    """Restore ``aggregate`` from ``snapshot``. Return False when restore fails.

    A failure is recorded as a miss and logged; the caller then replays the
    full stream into a fresh aggregate.
    """
    try:
        aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
    except Exception as e:
        record_snapshot_miss(SnapshotMissReason.STATE_RESTORE_FAILED, aggregate_type)
        # No exc_info: the traceback repeats the message, which can echo snapshot state.
        logger.warning(
            "Failed to restore from snapshot for %s/%s: %s. Falling back to full event replay.",
            aggregate_type, aggregate_id, error_summary(e),
        )  # fmt: skip
        return False
    return True


def assert_owned(
    aggregate_type: str, aggregate_id: UUID, events: list[Any], owner: UUID | None
) -> None:
    """Raise AggregateNotFoundError unless every event belongs to ``owner``.

    ``owner`` is None when no tenant scope is active, and then nothing is
    checked. An untenanted event fails the check under a scope (fail closed).
    """
    if owner is None or all(getattr(event, "tenant_id", None) == owner for event in events):
        return
    logger.warning(
        "Stream %s/%s has events outside tenant %s. Treating it as not found.",
        aggregate_type, aggregate_id, owner,
    )  # fmt: skip
    raise AggregateNotFoundError(aggregate_id, aggregate_type)
