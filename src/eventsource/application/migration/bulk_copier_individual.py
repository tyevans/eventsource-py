"""Per-event copy with duplicate verification for BulkCopier."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import (
    EventEnvelope,
    ExpectedVersion,
    FullEventStore,
    Position,
    StreamReadOptions,
    collect,
)

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper

logger = logging.getLogger(__name__)


class BulkCopierIndividualMixin:
    """Mixin appending one stream's events one at a time, tolerating a live mirror."""

    _target: FullEventStore
    _position_mapper: PositionMapper | None

    _MAX_APPEND_CONFLICT_RETRIES = 3

    async def _copy_stream_events_individually(
        self,
        migration_id: UUID,
        stream: StreamId,
        agg_events: list[EventEnvelope],
    ) -> Position | None:
        """Append one stream's events one at a time, tolerating a live mirror."""
        try:
            current_version = await self._target.get_stream_version(stream)
        except Exception:
            current_version = 0
        last_target_position: Position | None = None

        for envelope in agg_events:
            result = None
            for _attempt in range(self._MAX_APPEND_CONFLICT_RETRIES + 1):
                try:
                    result = await self._target.append(
                        stream,
                        [envelope.event],
                        ExpectedVersion.exact(current_version),
                    )
                    break
                except DuplicateEventError:
                    await self._verify_duplicate(migration_id, envelope)
                    break
                except OptimisticLockError:
                    current_version = await self._target.get_stream_version(stream)
            else:
                raise BulkCopyError(
                    migration_id,
                    envelope.position,
                    f"stream {stream.render()} kept moving under the copier: "
                    f"append conflicted {self._MAX_APPEND_CONFLICT_RETRIES + 1} times",
                )

            if result is None:
                continue

            current_version += 1
            last_target_position = result.position

            if (
                self._position_mapper is not None
                and envelope.position is not None
                and result.position is not None
            ):
                await self._position_mapper.record_mapping(
                    migration_id,
                    envelope.position,
                    result.position,
                    envelope.event.event_id,
                )

        return last_target_position

    async def _verify_duplicate(self, migration_id: UUID, envelope: EventEnvelope) -> None:
        """Accept a duplicate event only if the target holds the same event at the same place.

        The target has no read-by-event-id port, so the check reads the source
        envelope's stream version back from the target. Raises BulkCopyError on
        any difference in stream, version, event id, event type, or payload.
        """
        version = envelope.stream_version
        options = StreamReadOptions(from_version=version, to_version=version)
        stored = await collect(self._target.read_stream(envelope.stream_id, options))
        mismatch = _describe_mismatch(envelope, stored[0] if stored else None)
        if mismatch is not None:
            raise BulkCopyError(
                migration_id,
                envelope.position,
                f"event {envelope.event.event_id} already exists in the target but differs: "
                f"{mismatch}",
            )
        logger.debug(
            "Event %s already present in target with identical content; counted as copied",
            envelope.event.event_id,
        )


def _describe_mismatch(source: EventEnvelope, stored: EventEnvelope | None) -> str | None:
    """Return why `stored` is not the same event as `source`, or None when they match."""
    stream = source.stream_id.render()
    if stored is None:
        return f"stream {stream} has no event at version {source.stream_version}"
    if stored.stream_id != source.stream_id:
        return f"stored in stream {stored.stream_id.render()}, expected {stream}"
    if stored.event.event_id != source.event.event_id:
        return f"version {source.stream_version} holds event {stored.event.event_id}"
    if stored.event.event_type != source.event.event_type:
        return f"event type {stored.event.event_type!r} != {source.event.event_type!r}"
    if stored.event.model_dump(mode="json") != source.event.model_dump(mode="json"):
        return "payload differs"
    return None


__all__ = ["BulkCopierIndividualMixin"]
