"""Writer mixin for batch writing and stream copy operations in BulkCopier."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.observability import Tracer
from eventsource.ports import (
    EventEnvelope,
    ExpectedVersion,
    FullEventStore,
    Position,
)

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper

logger = logging.getLogger(__name__)


class BulkCopierWriterMixin:
    """Mixin providing batch writing and individual stream event copy for BulkCopier."""

    _tracer: Tracer
    _target: FullEventStore
    _position_mapper: PositionMapper | None

    # Bounded retries for the copier-vs-mirror append race. With the
    # interceptor mirroring at exact pre-append source versions, a mirror
    # can only land on a fully converged stream, so a conflict here
    # resolves within a re-read or two; sustained conflict means something
    # other than the mirror is writing the target and the copy must fail
    # honestly rather than spin.
    _MAX_APPEND_CONFLICT_RETRIES = 3

    async def _write_batch(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        events: list[EventEnvelope],
    ) -> Position | None:
        """
        Write a batch of events to target store.

        Groups events by stream and writes them in order to maintain
        proper versioning in the target store.

        With a `position_mapper` configured, events are appended one at a
        time so that each append's `AppendResult.position` -- the exact
        target position of that single event -- can be recorded. Without
        one, each stream's events are appended as a single batch, falling
        back to per-event appends when the batch straddles events that
        are already present (see `_copy_stream_events_individually`).

        An event already present in the target (`DuplicateEventError`)
        counts as already-copied and the copy continues; this is what
        makes a resumed copy converge.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant UUID.
            events: List of EventEnvelope instances to write.

        Returns:
            The last target position written, or None if nothing was
            appended (every event was already present).
        """
        with self._tracer.span(
            "eventsource.bulk_copier.write_batch",
            {
                "migration.id": str(migration_id),
                "tenant_id": str(tenant_id),
                "batch_size": len(events),
            },
        ):
            # Group events by stream for proper versioning
            # Events may come from different streams
            streams: dict[tuple[UUID, str], list[EventEnvelope]] = {}

            for envelope in events:
                key = (envelope.stream_id.aggregate_id, envelope.stream_id.category)
                if key not in streams:
                    streams[key] = []
                streams[key].append(envelope)

            last_target_position: Position | None = None

            # Write each stream's events. A group is never empty -- it only
            # exists because an event was added to it -- so the adapters'
            # empty-batch ValueError cannot fire here.
            for (aggregate_id, aggregate_type), agg_events in streams.items():
                stream = StreamId(aggregate_id=aggregate_id, category=aggregate_type)

                if self._position_mapper is None:
                    try:
                        current_version = await self._target.get_stream_version(stream)
                    except Exception:
                        current_version = 0
                    try:
                        result = await self._target.append(
                            stream,
                            [e.event for e in agg_events],
                            ExpectedVersion.exact(current_version),
                        )
                        last_target_position = result.position
                        continue
                    except DuplicateEventError:
                        # At least one event of the group is already in
                        # the target (a dual-write mirror landed it, or a
                        # previous run copied it). The group may still
                        # contain events that are NOT there -- skipping it
                        # wholesale would advance the checkpoint over
                        # events that never reached the target -- so fall
                        # through to per-event appends.
                        logger.debug(
                            "Stream %s batch straddles already-present events; retrying per event",
                            stream.render(),
                        )
                    except OptimisticLockError:
                        # A dual-write mirror advanced the stream between
                        # the version read and this append; re-read and
                        # append per event.
                        logger.debug(
                            "Stream %s advanced concurrently during batch append; "
                            "retrying per event",
                            stream.render(),
                        )

                pos = await self._copy_stream_events_individually(migration_id, stream, agg_events)
                if pos is not None:
                    last_target_position = pos

            logger.debug(
                "Wrote batch of %d events for migration %s",
                len(events),
                migration_id,
            )
            return last_target_position

    async def _copy_stream_events_individually(
        self,
        migration_id: UUID,
        stream: StreamId,
        agg_events: list[EventEnvelope],
    ) -> Position | None:
        """Append one stream's events one at a time, tolerating a live mirror.

        Used for the per-event (position-mapping) path and as the fallback
        when a batched append collides with events a dual-write mirror has
        already delivered. Per event:

        - `DuplicateEventError`: already in the target; counts as copied.
          No position mapping is recorded -- the duplicate sits at a
          target position this run never observed, so a mapping would be
          invented. The stream version did not move (nothing appended),
          so the local version bookkeeping stands.
        - `OptimisticLockError`: a mirror advanced the stream between the
          version read and this append. Re-read the version and retry;
          the retry raises `DuplicateEventError` if the mirror landed
          this very event. Bounded by `_MAX_APPEND_CONFLICT_RETRIES`.

        Returns:
            The last target position written, or None if every event was
            already present.

        Raises:
            BulkCopyError: If a conflict persists past the retry bound.
        """
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
                    logger.debug(
                        "Event %s already present in target; counting as copied",
                        envelope.event.event_id,
                    )
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
            # Appends within a run are already in ascending target order,
            # so the last one wins.
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


__all__ = [
    "BulkCopierWriterMixin",
]
