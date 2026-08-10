"""Driving projections over the global event feed to rebuild them.

Deliberately small and explicit rather than a subscription runner: this is
what a *rebuild* looks like, and a rebuild is a foreground operation someone
is waiting on. `ProjectionCoordinator` polls on a timer for live catch-up,
which is the other job.

## A poison event must not wedge the rebuild

`CheckpointTrackingProjection.handle` retries, writes to the DLQ, and then
re-raises -- the re-raise is what tells a *live subscription* to stop and not
checkpoint past a failure. A rebuild wants the opposite: the failure is
already recorded, and stopping means one bad event denies the projection
every event after it. So `replay` catches, records, and continues.
`ReplayReport.failed` is how the caller finds out, and it is a count rather
than a bool so "some events failed" cannot be mistaken for "none did" by a
truthiness check.

**A count on its own is safe and useless in the same breath.** An operator
told "3 events failed to replay" has no path from that message to the poison
event if the exception was discarded inside the `except`.
`ReplayReport.failures` carries it -- position, event id, event type, the
rejecting projection, and the exception itself. A caller can always turn
detail into a raise; no caller can turn a count back into detail, which is
why the detail is the part that has to live here.

`strict=True` is that raise, offered because it is the common case rather
than because it is hard: it stops at the first rejection and raises
`ReplayFailedError` carrying the same `ReplayFailure`.

## Why the failure list is bounded

Each `ReplayFailure` holds a live exception, so its `__traceback__` -- and
through it every frame's locals -- stays reachable for as long as the report
does. An unbounded list over a rebuild that fails a million times is a memory
hazard, not a diagnostic. `max_failures` caps what the report retains, and
`ReplayReport.failures_truncated` counts what the cap dropped, because a
silent truncation would reproduce the exact defect `failures` exists to fix:
an operator told "N failed" who cannot reach the Nth. `on_failure=` fires for
*every* failure regardless of the cap, so a caller who wants all of them can
stream them somewhere that is not memory.

## Scoping the read

`replay` reads the whole feed by default. `tenant_id=` and `aggregate_type=`
forward a `FeedReadOptions` to the adapter, which pushes the filter into the
query -- so a shared store rebuilds one tenant, or one category, with an
indexed read rather than scanning everything else and discarding it in
Python. This is narrower than `TenantFilter` on the projection, which is
applied *after* delivery and therefore costs the read either way.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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

#: Envelopes one feed read will pull into memory at a time.
#:
#: A rebuild wants the whole log, but not resident at once: the in-tree
#: adapters materialize a feed read fully before yielding the first envelope,
#: so the bound has to be on the *read*, not on how the events are consumed.
#: This is the peak allocation of a replay, so it trades memory against the
#: number of round trips rather than against correctness -- every batch is
#: folded before the next is read, and `from_position` is exclusive, so no
#: event is seen twice and none is skipped.
REPLAY_BATCH_SIZE = 1000

#: Events one `replay` call will read before giving up.
#:
#: The feed is adapter-supplied and the loop's exit depends on it: a cursor
#: that failed to advance would turn this into a hang, and a hang in CI reads
#: as infrastructure trouble and gets retried rather than investigated. Ten
#: million is far above any real rebuild and far below forever.
MAX_EVENTS_PER_REPLAY = 10_000_000

#: Failures one `ReplayReport` will retain before counting the rest.
#:
#: A retained failure pins an exception and its traceback, so this is a memory
#: bound as much as a readability one. A thousand named failures is already
#: more than anyone reads; past that the useful signal is the count, and
#: `on_failure=` is the way to keep the rest.
MAX_FAILURES_PER_REPLAY = 1000


@dataclass(frozen=True, slots=True)
class ReplayFailure:
    """One projection's refusal of one event.

    `projection` is the rejecting projection's class name rather than the
    object: a report is something an operator reads or logs, and holding the
    live projection would make it a handle into the read model.
    """

    #: `Position | None` because that is what an envelope promises. In every
    #: adapter here it is set, and a failure without one is barely actionable
    #: -- but narrowing it with an assert would turn an adapter's quirk into a
    #: crash on the path whose whole job is not to crash.
    position: Position | None
    event_id: UUID
    #: The event's own `event_type`, not `type(event).__name__` -- the same
    #: string except where a wire name is deliberately pinned, and the report
    #: has to name the one that was stored.
    event_type: str
    projection: str
    error: Exception


@dataclass(frozen=True, slots=True)
class ReplayReport:
    """What one `replay` call did.

    `failed` is *derived* from `failures` rather than counted alongside it.
    Two projections can reject the same event, and the two numbers then
    differ -- `failed` answers "how much of the log did not make it into the
    read models", which is per event, while `failures` has one entry per
    refusal because that is what names the projection to fix. Counting both
    independently is how they drift apart; deriving one means they cannot.

    The per-event derivation keys on `event_id`, the one identifier every
    failure carries. `position` would be the intuitive key and is the wrong
    one: it is optional by contract, so a feedless store would collapse an
    entire failed rebuild into a count of one.

    The derivation is only exact while nothing was dropped, so `failed` is a
    lower bound whenever `failures_truncated` is non-zero. That is the honest
    reading of a capped list, and the reason the cap is reported rather than
    applied quietly.
    """

    applied: int
    last_position: Position | None
    failures: tuple[ReplayFailure, ...] = field(default_factory=tuple)
    #: Failures that occurred but are not in `failures`, because the
    #: `max_failures` cap was reached. Non-zero means the report is a sample:
    #: pass `on_failure=` to capture all of them.
    failures_truncated: int = 0

    @property
    def failed(self) -> int:
        """Events at least one *retained* failure names.

        Distinct by `event_id`, not by `position`: `position` is
        `Position | None`, and a feedless store sets it on nothing, so
        deduping on it would fold every failure of such a rebuild into one
        and report `1` for any number of failed events. `event_id` is always
        present and identifies the event at least as precisely.

        A lower bound when `failures_truncated` is non-zero.
        """
        return len({failure.event_id for failure in self.failures})


class ReplayFailedError(ProjectionError):
    """A `strict=True` replay hit an event a projection rejected.

    Rooted in `ProjectionError` -- the domain-ring "a projection failed to
    process an event" error -- rather than in a hierarchy of its own: that is
    exactly what happened, and `except ProjectionError` should catch a strict
    rebuild's stop for the same reason it catches a live projection's. The
    infrastructure taxonomy in `ports/exceptions.py` is the wrong home:
    nothing about a store, bus, or lock failed here.

    Carries the `ReplayFailure` rather than a message about one: the whole
    point of strict mode is that the caller can act on the specific event,
    and re-deriving it by parsing `str(exc)` is not acting on it.
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
    projections: Sequence[EventSubscriber],
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

    `from_position` is exclusive, matching `read_all`: `None` means from the
    very beginning, which is what a rebuild wants.

    `tenant_id` and `aggregate_type` narrow the *read*, not the delivery:
    they are pushed down into the adapter's query, so rebuilding one tenant
    -- or one aggregate type -- out of a shared store does not pay for
    everything else. Leaving both `None` reads the whole feed and sends no
    `FeedReadOptions` at all, which is what `read_all` documents as
    unfiltered.

    `strict=True` raises `ReplayFailedError` on the first rejection instead
    of carrying on. The default is the rebuild's behaviour -- one bad event
    must not deny the projection every event after it -- and strict is for a
    test or a first deployment, where a silent partial rebuild is most costly
    and least visible.

    `max_failures` caps how many `ReplayFailure` records the report retains;
    the rest are counted in `ReplayReport.failures_truncated`. The cap exists
    because a retained failure pins a live exception and its traceback, and
    it is *reported* because truncating silently would recreate the defect
    `failures` exists to fix -- an operator told "N failed" with no way to
    reach the Nth. `on_failure` is called for every failure whether or not it
    was retained, so a caller who needs all of them can write them out as
    they happen; it is called before a `strict=True` raise, and an exception
    raised by the hook itself is left to propagate.

    An event that every projection ignores still counts as applied -- it was
    delivered and nothing rejected it. An event that any projection rejects
    counts as failed once, however many projections rejected it, because the
    count answers "how much of the log did not make it into the read models";
    `failures` carries one entry per rejection, so the projection to fix is
    named.
    """
    applied = seen = truncated = 0
    failures: list[ReplayFailure] = []
    last_position: Position | None = None
    cursor = from_position

    # Read in bounded batches rather than one open-ended pass. A rebuild does
    # want every event, but not all of them resident at once: the in-tree
    # adapters materialize a feed read fully before yielding the first
    # envelope, so an unbounded read is an unbounded allocation regardless of
    # how the events are consumed. `max_events` cannot protect against that --
    # it is downstream of the materialization and only fires once envelopes
    # are already in hand.
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
            for projection in projections:
                try:
                    await projection.handle(envelope.event)
                except Exception as exc:
                    # Re-raising here is what would wedge the rebuild, so the
                    # exception is deliberately not narrowed: a projection may
                    # raise anything, and "this event did not apply" is the only
                    # distinction a rebuild can act on. It is *recorded* rather
                    # than swallowed -- see `ReplayFailure`.
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

        # A short batch means the feed is exhausted. A full batch that
        # advanced no position would loop forever, so it is treated as
        # exhausted too -- the same non-advancing-cursor condition
        # `max_events` guards, caught one batch earlier.
        if batch_count < batch_size or (batch_count > 0 and cursor is None):
            break

    return ReplayReport(
        applied=applied,
        last_position=last_position,
        failures=tuple(failures),
        failures_truncated=truncated,
    )
