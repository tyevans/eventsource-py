"""Reader mixin for streaming and counting tenant events in bulk copy operations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from uuid import UUID

from eventsource.observability import Tracer
from eventsource.ports import EventEnvelope, FeedReadOptions, FullEventStore, Position

logger = logging.getLogger(__name__)


class BulkCopierReaderMixin:
    """Mixin providing event reading and counting operations for BulkCopier."""

    _tracer: Tracer
    _source: FullEventStore

    async def _count_tenant_events(self, tenant_id: UUID) -> int:
        """
        Count total events for tenant in source store.

        Args:
            tenant_id: Tenant UUID to count events for.

        Returns:
            Total number of events for the tenant.
        """
        with self._tracer.span(
            "eventsource.bulk_copier.count_events",
            {"tenant_id": str(tenant_id)},
        ):
            count = 0

            async for _ in self._source.read_all(None, FeedReadOptions(tenant_id=tenant_id)):
                count += 1

            logger.debug("Counted %d events for tenant %s", count, tenant_id)
            return count

    async def _stream_tenant_events(
        self,
        tenant_id: UUID,
        from_position: Position | None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Stream events for tenant from source store.

        Args:
            tenant_id: Tenant UUID.
            from_position: Position to start strictly after; None starts at
                the head of the feed. `Position` reads are strictly-after,
                which matches the legacy exclusive predicate exactly, so
                resume semantics are preserved.

        Yields:
            EventEnvelope instances in global position order.
        """
        async for envelope in self._source.read_all(
            from_position,
            FeedReadOptions(tenant_id=tenant_id),
        ):
            yield envelope


__all__ = [
    "BulkCopierReaderMixin",
]
