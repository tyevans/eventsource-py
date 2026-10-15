"""Driving projections over the global event feed to rebuild them.

Deliberately small and explicit rather than a subscription runner: this is
what a *rebuild* looks like, and a rebuild is a foreground operation someone
is waiting on. `ProjectionCoordinator` polls on a timer for live catch-up,
which is the other job.

## A poison event must not wedge the rebuild

`CheckpointTrackingProjection.handle` retries, writes to the DLQ, and then
re-raises so a *live subscription* stops rather than checkpointing past a
failure. A rebuild wants the opposite: stopping means one bad event denies the
projection every event after it. So `replay` catches, records, and continues.
`ReplayReport.failed` is a count rather than a bool, and `ReplayReport.failures`
carries the position, event id, event type, rejecting projection name, and
exception itself. `strict=True` stops at the first rejection and raises
`ReplayFailedError` carrying the same `ReplayFailure`.

## Why the failure list is bounded

Each `ReplayFailure` holds a live exception and its `__traceback__`.
`max_failures` caps what the report retains, `ReplayReport.failures_truncated`
counts what the cap dropped, and `on_failure=` fires for *every* failure
regardless of the cap so callers can stream them outside memory.

## Scoping the read

`tenant_id=` and `aggregate_type=` forward `FeedReadOptions` to the adapter,
pushing the filter into the store query rather than filtering in Python after
delivery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from eventsource.domain.exceptions import ProjectionError
from eventsource.ports.envelopes import FeedReadOptions

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from uuid import UUID

    from eventsource.ports.handlers import EventSubscriber
    from eventsource.ports.positions import Position
    from eventsource.ports.store import GlobalEventFeed

__all__ = [
    "MAX_EVENTS_PER_REPLAY",
    "MAX_FAILURES_PER_REPLAY",
    "REPLAY_BATCH_SIZE",
    "ReplayFailedError",
    "ReplayFailure",
    "ReplayReport",
    "replay",
]

#: Envelopes one feed read will pull into memory at a time. Bounded on the
#: read because in-tree adapters materialize a batch before yielding.
REPLAY_BATCH_SIZE = 1000

#: Events one `replay` call will read before raising RuntimeError to guard
#: against a non-advancing adapter cursor.
MAX_EVENTS_PER_REPLAY = 10_000_000

#: Failures one `ReplayReport` retains before counting the rest in
#: `failures_truncated`.
MAX_FAILURES_PER_REPLAY = 1000


@dataclass(frozen=True, slots=True)
class ReplayFailure:
    """One projection's refusal of one event.

    `projection` is the rejecting projection's class name rather than the live
    object so the report does not hold a handle into the read model.
    """

    position: Position | None
    event_id: UUID
    event_type: str
    projection: str
    error: Exception


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """What one `replay` call did.

    `failed` is derived from `failures` by distinct `event_id` (since
    `position` is optional on feedless stores and two projections can reject
    the same event). It is a lower bound whenever `failures_truncated > 0`.
    """

    applied: int
    last_position: Position | None
    failures: tuple[ReplayFailure, ...] = field(default_factory=tuple)
    failures_truncated: int = 0

    @property
    def failed(self) -> int:
        """Events at least one retained failure names (distinct by `event_id`)."""
        return len({failure.event_id for failure in self.failures})

    @property
    def succeeded(self) -> bool:
        """Whether the replay completed without any failures."""
        return len(self.failures) == 0 and self.failures_truncated == 0


class ReplayFailedError(ProjectionError):
    """A `strict=True` replay hit an event a projection rejected.

    Rooted in `ProjectionError` and carries the `ReplayFailure` on `.failure`.
    """

    def __init__(self, *, failure: ReplayFailure) -> None:
        self.failure = failure
        super().__init__(
            failure.projection,
            failure.event_id,
            f"rejected {failure.event_type} at {failure.position}: {failure.error}",
        )


async def replay(
    feed: GlobalEventFeed,
    projections: Sequence[EventSubscriber] | EventSubscriber,
    *,
    from_position: Position | None = None,
    tenant_id: UUID | None = None,
    aggregate_type: str | None = None,
    strict: bool = False,
    batch_size: int = REPLAY_BATCH_SIZE,
    max_events: int = MAX_EVENTS_PER_REPLAY,
    max_failures: int = MAX_FAILURES_PER_REPLAY,
    on_failure: Callable[[ReplayFailure], None] | None = None,
) -> ReplayReport:
    """Read the feed from `from_position` and fold it into every projection.

    - `from_position` is exclusive (`None` reads from the beginning).
    - `tenant_id` and `aggregate_type` push filter options down to the store.
    - `strict=True` raises `ReplayFailedError` on the first rejection.
    - `max_failures` caps retained `ReplayFailure` items (`failures_truncated`
      counts dropped ones); `on_failure` is invoked for every failure before
      any strict raise.
    """
    projection_list: tuple[EventSubscriber, ...] = (
        (cast("EventSubscriber", projections),)
        if hasattr(projections, "handle")
        else tuple(projections)
    )
    applied = seen = truncated = 0
    failures: list[ReplayFailure] = []
    last_position: Position | None = None
    cursor = from_position

    while True:
        options = FeedReadOptions(
            tenant_id=tenant_id,
            aggregate_type=aggregate_type,
            limit=batch_size,
        )
        batch_count = 0

        async for envelope in feed.read_all(cursor, options):
            batch_count += 1
            seen += 1
            if seen > max_events:
                raise RuntimeError(
                    f"replay read more than {max_events} events without the feed "
                    f"ending; the adapter's cursor is probably not advancing "
                    f"(last position: {last_position})"
                )
            if envelope.position is not None:
                last_position = envelope.position
                cursor = envelope.position

            rejected = False
            for projection in projection_list:
                try:
                    await projection.handle(envelope.event)
                except Exception as exc:
                    failure = ReplayFailure(
                        position=envelope.position,
                        event_id=envelope.event.event_id,
                        event_type=envelope.event.event_type,
                        projection=type(projection).__name__,
                        error=exc,
                    )
                    if on_failure is not None:
                        on_failure(failure)
                    if strict:
                        raise ReplayFailedError(failure=failure) from exc
                    if len(failures) < max_failures:
                        failures.append(failure)
                    else:
                        truncated += 1
                    rejected = True
            if not rejected:
                applied += 1

        if batch_count < batch_size or (batch_count > 0 and cursor is None):
            break

    return ReplayReport(
        applied=applied,
        last_position=last_position,
        failures=tuple(failures),
        failures_truncated=truncated,
    )
