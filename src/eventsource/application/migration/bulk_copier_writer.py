"""Writer mixin for batch writing and stream copy operations in BulkCopier."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.bulk_copier_individual import BulkCopierIndividualMixin
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


class BulkCopierWriterMixin(BulkCopierIndividualMixin):
    """Mixin providing batch writing and individual stream event copy for BulkCopier."""

    _tracer: Tracer
    _target: FullEventStore
    _position_mapper: PositionMapper | None

    async def _write_batch(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        events: list[EventEnvelope],
    ) -> Position | None:
        """Write a batch of events grouped by stream to target store, returning last position."""
        with self._tracer.span(
            "eventsource.bulk_copier.write_batch",
            {
                "migration.id": str(migration_id),
                "tenant_id": str(tenant_id),
                "batch_size": len(events),
            },
        ):
            streams: dict[tuple[UUID, str], list[EventEnvelope]] = {}
            for envelope in events:
                key = (envelope.stream_id.aggregate_id, envelope.stream_id.category)
                streams.setdefault(key, []).append(envelope)

            last_target_position: Position | None = None
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
                        logger.debug(
                            "Stream %s batch straddles already-present events; retrying per event",
                            stream.render(),
                        )
                    except OptimisticLockError:
                        logger.debug(
                            "Stream %s advanced concurrently during batch append; retrying per event",
                            stream.render(),
                        )

                pos = await self._copy_stream_events_individually(migration_id, stream, agg_events)
                if pos is not None:
                    last_target_position = pos

            logger.debug("Wrote batch of %d events for migration %s", len(events), migration_id)
            return last_target_position


__all__ = ["BulkCopierWriterMixin"]
