"""Driving projections over the global event feed to rebuild them.

Foreground rebuild runner over `GlobalEventFeed`. By default, `replay` catches,
records, and continues past projection errors so one poison event does not deny
subsequent events (`strict=True` raises `ReplayFailedError` on the first error).
`max_failures` bounds retained `ReplayFailure` items (`failures_truncated`
counts dropped ones), while `on_failure` receives every failure.
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

REPLAY_BATCH_SIZE = 1000
MAX_EVENTS_PER_REPLAY = 10_000_000
MAX_FAILURES_PER_REPLAY = 1000


@dataclass(frozen=True, slots=True)
class ReplayFailure:
    """One projection's refusal of one event (`projection` is the class name)."""

    position: Position | None
    event_id: UUID
    event_type: str
    projection: str
    error: Exception


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """Summary of a `replay` call (`failed` counts distinct event_ids in `failures`)."""

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

    def raise_for_failures(self) -> None:
        """Raise ReplayFailedError if any failure occurred during replay."""
        if self.succeeded:
            return
        if self.failures:
            first = self.failures[0]
            raise ReplayFailedError(failure=first) from first.error
        raise RuntimeError(f"replay failed with {self.failures_truncated} truncated failure(s)")


class ReplayFailedError(ProjectionError):
    """Raised when a `strict=True` replay hits an event rejected by a projection."""

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
    """Read the feed from `from_position` (exclusive) and fold into every projection."""
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
