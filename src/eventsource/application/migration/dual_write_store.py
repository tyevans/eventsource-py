"""Event store port implementation mixin for DualWriteInterceptor."""

from __future__ import annotations

import logging
import sys
from collections.abc import AsyncIterator, Sequence
from typing import cast
from uuid import UUID

from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_TENANT_ID,
    Tracer,
)
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
)


def _get_logger() -> logging.Logger:
    mod = sys.modules.get("eventsource.application.migration.dual_write")
    if mod is not None and hasattr(mod, "logger"):
        return cast(logging.Logger, mod.logger)
    return logging.getLogger("eventsource.application.migration.dual_write")


class DualWriteStoreMixin:
    """Mixin implementing the FullEventStore port for DualWriteInterceptor."""

    _tracer: Tracer
    _source: FullEventStore
    _target: FullEventStore
    _tenant_id: UUID
    _migration_id: UUID | None
    _max_failure_history: int
    _first_seen_source_position: Position | None
    _last_synced_source_position: Position | None
    _dual_write_success_count: int
    _unabsorbed_failure_positions: list[Position]
    _failure_positions_saturated: bool

    def _record_sync_failure(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: Sequence[DomainEvent],
        error: Exception,
        source_position: Position | None,
    ) -> None:
        raise NotImplementedError

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        """
        Append events to both source and target stores.

        Writes to source store first (authoritative), then attempts to write
        to target store. Source failures propagate to the caller. Target
        failures are recorded but don't fail the operation.

        The mirror does NOT forward the caller's `expected`: it appends to
        the target with the exact stream version the source held before
        this append (derived from the source result). The mirror therefore
        lands only when the target stream has fully converged with the
        source, which is what keeps the overlap with a running bulk-copy
        pass safe: a mirror can never leapfrog events the copier has not
        yet delivered, so the target's stream order always matches the
        source's -- even for callers appending with `any`. A mirror
        refused for non-convergence is recorded as a failure and the
        event reaches the target through the copy pass instead.

        Args:
            stream: Identity of the stream to append to.
            events: Events to append.
            expected: Optimistic-concurrency expectation.

        Returns:
            AppendResult from the source store write.

        Raises:
            OptimisticLockError: If the source store's version check fails.
            ValueError: If events list is empty.
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        with self._tracer.span(
            "eventsource.dual_write.append",
            {
                ATTR_AGGREGATE_ID: str(stream.aggregate_id),
                ATTR_AGGREGATE_TYPE: stream.category,
                ATTR_TENANT_ID: str(self._tenant_id),
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected.kind,
            },
        ):
            # Step 1: Write to source (authoritative).
            source_result = await self._source.append(stream, events, expected)

            # Coverage starts at the first append handled, successful mirror or not.
            if self._first_seen_source_position is None and source_result.position is not None:
                self._first_seen_source_position = source_result.position

            # Step 2: Write to target (best-effort)
            try:
                mirror_expected = ExpectedVersion.exact(source_result.new_version - len(events))
                await self._target.append(stream, events, mirror_expected)
                self._dual_write_success_count += len(events)
                if source_result.position is not None:
                    self._last_synced_source_position = source_result.position
                _get_logger().debug(
                    f"Dual-write success for tenant {self._tenant_id}, stream {stream.render()}"
                )
            except Exception as e:
                _get_logger().warning(
                    f"Target write failed for tenant {self._tenant_id}, "
                    f"stream {stream.render()}: {e}"
                )
                if self._migration_id is not None:
                    get_migration_metrics(
                        str(self._migration_id),
                        str(self._tenant_id),
                    ).record_failed_target_write(error_type=type(e).__name__)
                if source_result.position is not None:
                    if len(self._unabsorbed_failure_positions) >= self._max_failure_history:
                        self._failure_positions_saturated = True
                    else:
                        self._unabsorbed_failure_positions.append(source_result.position)
                self._record_sync_failure(
                    aggregate_id=stream.aggregate_id,
                    aggregate_type=stream.category,
                    events=events,
                    error=e,
                    source_position=source_result.position,
                )

            return source_result

    async def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """Read a category from the source store."""
        async for envelope in self._source.read_category(category, options):
            yield envelope

    async def event_exists(self, event_id: UUID) -> bool:
        """Check if an event exists in the source store."""
        return await self._source.event_exists(event_id)

    async def get_stream_version(self, stream: StreamId) -> int:
        """Get the current version of a stream from the source store."""
        return await self._source.get_stream_version(stream)

    async def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """Read events from a stream in the source store."""
        async for envelope in self._source.read_stream(stream, options):
            yield envelope

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """Read the global feed from the source store."""
        async for envelope in self._source.read_all(from_position, options):
            yield envelope

    async def current_position(self) -> Position | None:
        """Get the current global-feed position of the SOURCE store."""
        return await self._source.current_position()


__all__ = [
    "DualWriteStoreMixin",
]
