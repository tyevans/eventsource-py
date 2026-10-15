"""Tests for failure accounting, poison event handling, and error limits in replay."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.application.projections.replay import (
    ReplayFailedError,
    ReplayFailure,
    ReplayReport,
    replay,
)
from eventsource.domain.exceptions import ProjectionError
from tests.unit.application.projections.test_replay._fixtures import (
    Collecting,
    OrderPlaced,
    PositionlessFeed,
    RejectsEverything,
    RejectsOne,
    _store_with,
)


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


class TestSingleProjectionAndSucceededReport:
    """Callers with a single projection can pass it directly without wrapping
    in a list, and `report.succeeded` summarizes whether zero failures occurred."""

    async def test_replay_accepts_a_bare_single_projection(self) -> None:
        first = OrderPlaced(aggregate_id=uuid4())
        second = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(first, second)
        projection = Collecting()

        report = await replay(store, projection)

        assert [e.event_id for e in projection.seen] == [first.event_id, second.event_id]
        assert report.applied == 2
        assert report.succeeded is True

    async def test_succeeded_is_false_when_a_failure_is_retained_or_truncated(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)

        failed_report = await replay(store, RejectsOne(poison.event_id))
        truncated_report = await replay(store, RejectsOne(poison.event_id), max_failures=0)

        assert failed_report.succeeded is False
        assert truncated_report.failures == ()
        assert truncated_report.failures_truncated == 1
        assert truncated_report.succeeded is False
