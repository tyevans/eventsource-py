"""`replay` folds the global feed into projections without wedging on poison.

The cases here are the ones a rebuild driver actually gets wrong: stopping at
the first bad event, conflating "events that failed" with "rejections",
losing the exception inside the `except`, filtering in Python instead of in
the query, and truncating the failure list quietly.

`RecordingFeed` is here because two of those are invisible in the results.
Filtering client-side and pushing the filter into the adapter's query produce
*the same answers* -- `.claude/rules/recurring-defects.md` §4 exactly -- so a
test that only checked which events landed would pass against the workaround
it exists to replace. What `replay` asked the adapter for is the query plan,
in the only form a port exposes one.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.projections.replay import (
    REPLAY_BATCH_SIZE,
    ReplayFailedError,
    ReplayFailure,
    ReplayReport,
    replay,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.domain.exceptions import ProjectionError
from eventsource.domain.stream_id import StreamId
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.positions import ExpectedVersion

if TYPE_CHECKING:
    from eventsource.ports.positions import Position

TENANT_A = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
TENANT_B = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"


@register_event(registry=_REGISTRY)
class InvoiceIssued(DomainEvent):
    aggregate_type: str = "Invoice"


class Collecting:
    """A subscriber that records what it was handed."""

    def __init__(self) -> None:
        self.seen: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.seen.append(event)


class RejectsEverything:
    """A subscriber that refuses everything, with a distinguishable error."""

    def __init__(self, message: str) -> None:
        self._message = message

    async def handle(self, event: DomainEvent) -> None:
        raise RuntimeError(self._message)


class RejectsOne:
    """Rejects a single named event id and applies the rest."""

    def __init__(self, poison: UUID) -> None:
        self._poison = poison
        self.seen: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        if event.event_id == self._poison:
            raise ValueError("poison")
        self.seen.append(event)


class RecordingFeed:
    """A `GlobalEventFeed` that remembers how it was asked."""

    def __init__(self, inner: InMemoryEventStore) -> None:
        self._inner = inner
        self.calls: list[tuple[Position | None, FeedReadOptions | None]] = []

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        self.calls.append((from_position, options))
        return self._inner.read_all(from_position, options)

    async def current_position(self) -> Position | None:
        return await self._inner.current_position()


class NonAdvancingFeed:
    """The hang `max_events` exists to turn into a failure.

    An adapter whose cursor never advances re-yields forever. Left alone that
    is an infinite loop, and a hang in CI reads as infrastructure trouble and
    gets retried rather than investigated.
    """

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        while True:
            yield _envelope(OrderPlaced(aggregate_id=uuid4()))

    async def current_position(self) -> Position | None:
        return None


class PositionlessFeed:
    """A feed whose envelopes carry no position.

    `EventEnvelope.position` is `Position | None`, and `ReplayFailure` keeps it
    that way rather than asserting -- a feedless store is a documented case,
    and the rebuild path's whole job is not to crash. Anything deriving a
    count from `position` therefore has to cope with `None` repeating.
    """

    def __init__(self, events: Sequence[DomainEvent]) -> None:
        self._events = events

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        for event in self._events:
            yield _envelope(event)

    async def current_position(self) -> Position | None:
        return None


def _envelope(event: DomainEvent) -> EventEnvelope:
    return EventEnvelope(
        event=event,
        stream_id=StreamId(event.aggregate_id, event.aggregate_type),
        stream_version=1,
        position=None,
        stored_at=datetime.now(UTC),
    )


async def _append(store: InMemoryEventStore, event: DomainEvent) -> None:
    await store.append(
        StreamId(event.aggregate_id, event.aggregate_type),
        [event],
        ExpectedVersion.no_stream(),
    )


async def _store_with(*events: DomainEvent) -> InMemoryEventStore:
    store = InMemoryEventStore(event_registry=_REGISTRY)
    for event in events:
        await _append(store, event)
    return store


class TestAPoisonEventDoesNotStopTheRebuild:
    """The whole reason this is not a subscription runner."""

    async def test_events_after_the_poison_are_still_applied(self) -> None:
        first, poison, last = (OrderPlaced(aggregate_id=uuid4()) for _ in range(3))
        store = await _store_with(first, poison, last)
        projection = RejectsOne(poison.event_id)

        report = await replay(store, [projection])

        assert [e.event_id for e in projection.seen] == [first.event_id, last.event_id]
        assert report.applied == 2
        assert report.failed == 1

    async def test_a_clean_replay_reports_no_failures(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [Collecting()])

        assert report.failures == ()
        assert report.failed == 0
        assert report.failures_truncated == 0

    async def test_the_failure_names_the_event_and_the_projection(self) -> None:
        """A count alone gives an operator no route to the poison event."""
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)

        report = await replay(store, [RejectsOne(poison.event_id)])

        (failure,) = report.failures
        assert failure.event_id == poison.event_id
        assert failure.event_type == "OrderPlaced"
        assert failure.projection == "RejectsOne"
        assert failure.position is not None

    async def test_the_failure_carries_the_exception_itself(self) -> None:
        """A message has to be parsed back into what it names; the exception
        already has it."""
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)

        report = await replay(store, [RejectsOne(poison.event_id)])

        (failure,) = report.failures
        assert isinstance(failure.error, ValueError)
        assert failure.error.__traceback__ is not None


class TestFailedCountsEventsAndFailuresCountRejections:
    """The two numbers are allowed to differ, and here they do.

    Both projections reject the one event, so a `failed` counted alongside
    `failures` -- or derived as `len(failures)` -- would say two events failed
    when the log holds one.
    """

    async def test_two_projections_rejecting_one_event_is_one_failed_event(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))

        report = await replay(
            store,
            [RejectsEverything("left"), RejectsEverything("right")],
        )

        assert report.failed == 1
        assert len(report.failures) == 2
        assert {str(f.error) for f in report.failures} == {"left", "right"}

    async def test_nothing_is_applied_when_every_projection_rejects(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [RejectsEverything("no")])
        assert report.applied == 0

    async def test_an_event_no_projection_handles_still_counts_as_applied(self) -> None:
        """Delivered and not rejected is the whole test for applied."""
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [])
        assert report.applied == 1

    async def test_failed_counts_events_when_the_feed_supplies_no_positions(self) -> None:
        """A position-less feed must not collapse every failure into one.

        `ReplayFailure.position` is `Position | None` by contract, so a
        derivation that dedupes on it counts every `None` as the same event
        and reports one failure for a whole failed rebuild. `failed` answers
        "how many events did not reach the read models"; three events that
        each failed are three, whether or not the adapter numbers them.
        """
        events = [OrderPlaced(aggregate_id=uuid4()) for _ in range(3)]
        feed = PositionlessFeed(events)

        report = await replay(feed, [RejectsEverything("no")])

        assert [f.position for f in report.failures] == [None, None, None]
        assert report.failed == 3

    async def test_one_positionless_event_rejected_twice_is_still_one_event(self) -> None:
        """The dedupe still has to dedupe -- fixing the undercount must not
        turn `failed` into `len(failures)`."""
        feed = PositionlessFeed([OrderPlaced(aggregate_id=uuid4())])

        report = await replay(feed, [RejectsEverything("left"), RejectsEverything("right")])

        assert len(report.failures) == 2
        assert report.failed == 1

    def test_failed_cannot_be_supplied_independently_of_the_failures(self) -> None:
        """Restoring a `failed` argument is the change that would let the two
        drift apart again, and it would look like a convenience."""
        with pytest.raises(TypeError):
            ReplayReport(applied=1, failed=9, last_position=None)  # type: ignore[call-arg]


class TestStrictRaisesOnTheFirstRejection:
    async def test_it_raises_carrying_the_failure(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)

        with pytest.raises(ReplayFailedError) as raised:
            await replay(store, [RejectsOne(poison.event_id)], strict=True)

        failure = raised.value.failure
        assert failure.event_id == poison.event_id
        assert failure.event_type == "OrderPlaced"
        assert isinstance(failure.error, ValueError)
        assert raised.value.__cause__ is failure.error

    async def test_it_stops_rather_than_carrying_on(self) -> None:
        """A strict replay that raised and still folded the third event would
        be a louder default, not a stop."""
        first, poison, last = (OrderPlaced(aggregate_id=uuid4()) for _ in range(3))
        store = await _store_with(first, poison, last)
        collecting = Collecting()

        with pytest.raises(ReplayFailedError):
            await replay(store, [RejectsOne(poison.event_id), collecting], strict=True)

        assert [e.event_id for e in collecting.seen] == [first.event_id]

    async def test_a_clean_log_is_unaffected_by_strict(self) -> None:
        """Strict changes nothing when nothing fails -- so a caller can leave
        it on."""
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(3)))

        lenient = await replay(store, [Collecting()])
        strict = await replay(store, [Collecting()], strict=True)

        assert (lenient.applied, lenient.failed) == (strict.applied, strict.failed) == (3, 0)

    def test_the_error_is_rooted_in_the_library_hierarchy(self) -> None:
        """`except ProjectionError` must catch a strict rebuild's stop: it is
        a projection failing to process an event, which is what that error
        already means."""
        assert issubclass(ReplayFailedError, ProjectionError)


class TestTheFailureListIsBoundedAndSaysSo:
    """A silent truncation reproduces the defect `failures` exists to fix.

    `failures_truncated` is the honesty: an operator who sees it non-zero
    knows the report is a sample rather than the whole story.
    """

    async def test_failures_are_capped_at_max_failures(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))

        report = await replay(store, [RejectsEverything("no")], max_failures=2)

        assert len(report.failures) == 2

    async def test_the_dropped_failures_are_counted_rather_than_vanishing(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))

        report = await replay(store, [RejectsEverything("no")], max_failures=2)

        assert report.failures_truncated == 3

    async def test_nothing_is_truncated_when_the_cap_is_not_reached(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(2)))

        report = await replay(store, [RejectsEverything("no")], max_failures=10)

        assert report.failures_truncated == 0
        assert len(report.failures) == 2

    async def test_on_failure_fires_for_every_failure_including_dropped_ones(self) -> None:
        """The hook is the escape hatch from the cap: a caller who wants all
        of them streams them out instead of holding them in memory."""
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))
        streamed: list[ReplayFailure] = []

        report = await replay(
            store,
            [RejectsEverything("no")],
            max_failures=2,
            on_failure=streamed.append,
        )

        assert len(streamed) == 5
        assert report.failures_truncated == 3
        # The dropped ones are reachable through the hook and only there.
        retained = {f.event_id for f in report.failures}
        assert {f.event_id for f in streamed} - retained

    async def test_on_failure_fires_before_a_strict_raise(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        streamed: list[ReplayFailure] = []

        with pytest.raises(ReplayFailedError):
            await replay(
                store,
                [RejectsOne(poison.event_id)],
                strict=True,
                on_failure=streamed.append,
            )

        assert [f.event_id for f in streamed] == [poison.event_id]


class TestTheReadIsBounded:
    """The adapters materialize a feed read before yielding the first
    envelope, so an unbounded read is an unbounded allocation however the
    events are consumed. The bound has to be on the read itself."""

    async def test_every_read_carries_a_limit(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)

        await replay(feed, [Collecting()])

        assert feed.calls
        for _, options in feed.calls:
            assert options is not None
            assert options.limit == REPLAY_BATCH_SIZE

    async def test_a_feed_longer_than_one_batch_is_read_in_several(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))
        feed = RecordingFeed(store)
        collecting = Collecting()

        report = await replay(feed, [collecting], batch_size=2)

        # 2 + 2 + 1: the short final batch is what ends the loop.
        assert len(feed.calls) == 3
        assert [options.limit for _, options in feed.calls if options] == [2, 2, 2]
        # Every event still arrives, exactly once and in order.
        assert len(collecting.seen) == 5
        assert report.applied == 5

    async def test_each_batch_resumes_after_the_previous_one(self) -> None:
        """`from_position` is exclusive, so no event is seen twice or skipped."""
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(4)))
        feed = RecordingFeed(store)
        collecting = Collecting()

        await replay(feed, [collecting], batch_size=2)

        cursors = [from_position for from_position, _ in feed.calls]
        assert cursors[0] is None
        assert all(cursor is not None for cursor in cursors[1:])
        assert len({event.event_id for event in collecting.seen}) == 4

    async def test_an_empty_feed_reads_once_and_stops(self) -> None:
        feed = RecordingFeed(InMemoryEventStore(event_registry=_REGISTRY))

        report = await replay(feed, [Collecting()])

        assert len(feed.calls) == 1
        assert report.applied == 0
        assert report.last_position is None


class TestScopingReachesTheAdaptersQuery:
    """Filtering in Python would give the same answers, so assert the ask."""

    async def test_the_tenant_is_forwarded_as_feed_read_options(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A))
        feed = RecordingFeed(store)

        await replay(feed, [Collecting()], tenant_id=TENANT_A)

        ((_, options),) = feed.calls
        assert options is not None
        assert options.tenant_id == TENANT_A
        assert options.aggregate_type is None

    async def test_the_aggregate_type_is_forwarded_as_feed_read_options(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)

        await replay(feed, [Collecting()], aggregate_type="Order")

        ((_, options),) = feed.calls
        assert options is not None
        assert options.aggregate_type == "Order"
        assert options.tenant_id is None

    async def test_both_filters_travel_together(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A))
        feed = RecordingFeed(store)

        await replay(feed, [Collecting()], tenant_id=TENANT_A, aggregate_type="Order")

        ((_, options),) = feed.calls
        assert options is not None
        assert (options.tenant_id, options.aggregate_type) == (TENANT_A, "Order")

    async def test_no_filter_is_sent_when_nothing_is_named(self) -> None:
        """A whole-feed rebuild must not send a filter an adapter could
        interpret as narrowing the read.

        The options object itself is always sent now, because every read
        carries a batch `limit` -- but its filter fields stay `None`, which
        all three adapters guard with `is not None` before adding a clause.
        An unset filter and no options at all are the same read."""
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)

        await replay(feed, [Collecting()])

        ((_, options),) = feed.calls
        assert options is not None
        assert options.tenant_id is None
        assert options.aggregate_type is None
        assert options.limit is not None

    async def test_from_position_is_forwarded_unchanged(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(2)))
        first = [envelope async for envelope in store.read_all()][0]
        feed = RecordingFeed(store)

        report = await replay(feed, [Collecting()], from_position=first.position)

        ((sent, _),) = feed.calls
        assert sent == first.position
        # Exclusive, matching `read_all`: the event at that position is behind us.
        assert report.applied == 1


class TestTheScopedReplaySeesOnlyThatSlice:
    """The behavioural half: forwarding the options and the adapter honouring
    them are two claims, and each can fail without the other."""

    async def test_only_the_named_tenants_events_are_applied(self) -> None:
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A),
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_B),
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_B),
        )

        report = await replay(store, [Collecting()], tenant_id=TENANT_A)

        assert report.applied == 1

    async def test_only_the_named_aggregate_types_events_are_applied(self) -> None:
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4()),
            InvoiceIssued(aggregate_id=uuid4()),
            InvoiceIssued(aggregate_id=uuid4()),
        )

        report = await replay(store, [Collecting()], aggregate_type="Invoice")

        assert report.applied == 2

    async def test_the_unscoped_replay_still_sees_everything(self) -> None:
        """The counterpart the scoped assertions need: a scoped result of 1 is
        the filter and not an empty log."""
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A),
            InvoiceIssued(aggregate_id=uuid4(), tenant_id=TENANT_B),
            InvoiceIssued(aggregate_id=uuid4(), tenant_id=TENANT_B),
        )

        report = await replay(store, [Collecting()])

        assert report.applied == 3

    async def test_the_last_position_is_the_last_matching_event(self) -> None:
        """A caller checkpoints on `last_position`. Under a filter it has to
        be a position the *filtered* read reached, or the next scoped replay
        resumes past events it never saw."""
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A),
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_B),
        )

        scoped = await replay(store, [Collecting()], tenant_id=TENANT_A)
        whole = await replay(store, [Collecting()])

        assert scoped.last_position is not None
        assert whole.last_position is not None
        assert scoped.last_position < whole.last_position

    async def test_an_empty_feed_reports_no_last_position(self) -> None:
        report = await replay(InMemoryEventStore(event_registry=_REGISTRY), [Collecting()])
        assert report.last_position is None
        assert report.applied == 0


class TestMaxEventsGuardsAgainstANonAdvancingCursor:
    async def test_it_raises_rather_than_hanging(self) -> None:
        with pytest.raises(RuntimeError, match="cursor is probably not advancing"):
            await replay(NonAdvancingFeed(), [Collecting()], max_events=10)

    async def test_a_feed_that_ends_within_the_bound_is_unaffected(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(3)))

        report = await replay(store, [Collecting()], max_events=3)

        assert report.applied == 3
