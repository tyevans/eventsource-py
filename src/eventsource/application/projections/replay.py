"""Driving projections over the global event feed to rebuild them."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from eventsource.domain.exceptions import ProjectionError
from eventsource.ports.envelopes import FeedReadOptions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from uuid import UUID

    from eventsource.domain.event import DomainEvent
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

REPLAY_BATCH_SIZE, MAX_EVENTS_PER_REPLAY, MAX_FAILURES_PER_REPLAY = 1000, 10_000_000, 1000


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
        return len({failure.event_id for failure in self.failures})

    @property
    def total_processed(self) -> int:
        return self.applied + self.failed

    @property
    def succeeded(self) -> bool:
        return len(self.failures) == 0 and self.failures_truncated == 0

    def __bool__(self) -> bool:
        return self.succeeded

    def raise_for_failures(self) -> None:
        if self.succeeded:
            return
        if self.failures:
            raise ReplayFailedError(failure=self.failures[0]) from self.failures[0].error
        raise RuntimeError(f"replay failed with {self.failures_truncated} truncated failure(s)")


class ReplayFailedError(ProjectionError):
    """Raised when a `strict=True` replay hits an event rejected by a projection."""

    def __init__(self, *, failure: ReplayFailure) -> None:
        self.failure = failure
        msg = f"rejected {failure.event_type} at {failure.position}: {failure.error}"
        super().__init__(failure.projection, failure.event_id, msg)


async def replay(
    feed: GlobalEventFeed,
    projections: Iterable[EventSubscriber] | EventSubscriber,
    *,
    from_position: Position | None = None,
    tenant_id: UUID | None = None,
    aggregate_type: str | None = None,
    event_types: Iterable[str | type[DomainEvent]] | None = None,
    strict: bool = False,
    batch_size: int = REPLAY_BATCH_SIZE,
    max_events: int = MAX_EVENTS_PER_REPLAY,
    max_failures: int = MAX_FAILURES_PER_REPLAY,
    on_failure: Callable[[ReplayFailure], None] | None = None,
) -> ReplayReport:
    """Read the feed from `from_position` (exclusive) and fold into every projection."""
    subs = (
        (cast("EventSubscriber", projections),)
        if hasattr(projections, "handle")
        else tuple(projections)
    )
    allowed = (
        frozenset(
            t if isinstance(t, str) else getattr(t, "event_type", t.__name__) for t in event_types
        )
        if event_types is not None
        else None
    )
    applied, seen, truncated, failures = 0, 0, 0, list[ReplayFailure]()
    last_position, cursor = None, from_position
    options = FeedReadOptions(tenant_id=tenant_id, aggregate_type=aggregate_type, limit=batch_size)

    while True:
        batch_count = 0
        async for env in feed.read_all(cursor, options):
            batch_count, seen = batch_count + 1, seen + 1
            if seen > max_events:
                raise RuntimeError(
                    f"replay read more than {max_events} events without the feed ending; "
                    f"the adapter's cursor is probably not advancing (last position: {last_position})"
                )
            pos, ev = env.position, env.event
            if pos is not None:
                last_position = cursor = pos
            if allowed is not None and ev.event_type not in allowed:
                continue

            rejected = False
            for sub in subs:
                try:
                    await sub.handle(ev)
                except Exception as exc:
                    err = ReplayFailure(pos, ev.event_id, ev.event_type, type(sub).__name__, exc)
                    if on_failure is not None:
                        on_failure(err)
                    if strict:
                        raise ReplayFailedError(failure=err) from exc
                    if len(failures) < max_failures:
                        failures.append(err)
                    else:
                        truncated += 1
                    rejected = True
            if not rejected:
                applied += 1

        if batch_count < batch_size or (batch_count > 0 and cursor is None):
            break

    return ReplayReport(applied, last_position, tuple(failures), truncated)
