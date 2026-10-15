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
        assert report.applied == 2 and report.failed == 1

    async def test_a_clean_replay_reports_no_failures(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [Collecting()])
        assert report.failures == () and report.failed == 0 and report.failures_truncated == 0

    async def test_the_failure_names_the_event_and_the_projection(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        report = await replay(store, [RejectsOne(poison.event_id)])
        (failure,) = report.failures
        assert failure.event_id == poison.event_id
        assert failure.event_type == "OrderPlaced"
        assert failure.projection == "RejectsOne"
        assert failure.position is not None

    async def test_the_failure_carries_the_exception_itself(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        report = await replay(store, [RejectsOne(poison.event_id)])
        (failure,) = report.failures
        assert isinstance(failure.error, ValueError)
        assert failure.error.__traceback__ is not None


class TestFailedCountsEventsAndFailuresCountRejections:
    """`failed` counts distinct events while `failures` counts rejections."""

    async def test_two_projections_rejecting_one_event_is_one_failed_event(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [RejectsEverything("left"), RejectsEverything("right")])
        assert report.failed == 1 and len(report.failures) == 2
        assert {str(f.error) for f in report.failures} == {"left", "right"}

    async def test_nothing_is_applied_when_every_projection_rejects(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [RejectsEverything("no")])
        assert report.applied == 0

    async def test_an_event_no_projection_handles_still_counts_as_applied(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        report = await replay(store, [])
        assert report.applied == 1

    async def test_failed_counts_events_when_the_feed_supplies_no_positions(self) -> None:
        feed = PositionlessFeed([OrderPlaced(aggregate_id=uuid4()) for _ in range(3)])
        report = await replay(feed, [RejectsEverything("no")])
        assert [f.position for f in report.failures] == [None, None, None]
        assert report.failed == 3

    async def test_one_positionless_event_rejected_twice_is_still_one_event(self) -> None:
        feed = PositionlessFeed([OrderPlaced(aggregate_id=uuid4())])
        report = await replay(feed, [RejectsEverything("left"), RejectsEverything("right")])
        assert len(report.failures) == 2 and report.failed == 1

    def test_failed_cannot_be_supplied_independently_of_the_failures(self) -> None:
        with pytest.raises(TypeError):
            ReplayReport(applied=1, failed=9, last_position=None)  # type: ignore[call-arg]


class TestStrictRaisesOnTheFirstRejection:
    async def test_it_raises_carrying_the_failure(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        with pytest.raises(ReplayFailedError) as raised:
            await replay(store, [RejectsOne(poison.event_id)], strict=True)
        failure = raised.value.failure
        assert failure.event_id == poison.event_id and failure.event_type == "OrderPlaced"
        assert isinstance(failure.error, ValueError) and raised.value.__cause__ is failure.error

    async def test_it_stops_rather_than_carrying_on(self) -> None:
        first, poison, last = (OrderPlaced(aggregate_id=uuid4()) for _ in range(3))
        store = await _store_with(first, poison, last)
        collecting = Collecting()
        with pytest.raises(ReplayFailedError):
            await replay(store, [RejectsOne(poison.event_id), collecting], strict=True)
        assert [e.event_id for e in collecting.seen] == [first.event_id]

    async def test_a_clean_log_is_unaffected_by_strict(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(3)))
        lenient = await replay(store, [Collecting()])
        strict = await replay(store, [Collecting()], strict=True)
        assert (lenient.applied, lenient.failed) == (strict.applied, strict.failed) == (3, 0)

    def test_the_error_is_rooted_in_the_library_hierarchy(self) -> None:
        assert issubclass(ReplayFailedError, ProjectionError)


class TestTheFailureListIsBoundedAndSaysSo:
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
        assert report.failures_truncated == 0 and len(report.failures) == 2

    async def test_on_failure_fires_for_every_failure_including_dropped_ones(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))
        streamed: list[ReplayFailure] = []
        report = await replay(
            store, [RejectsEverything("no")], max_failures=2, on_failure=streamed.append
        )
        assert len(streamed) == 5 and report.failures_truncated == 3
        retained = {f.event_id for f in report.failures}
        assert {f.event_id for f in streamed} - retained

    async def test_on_failure_fires_before_a_strict_raise(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        streamed: list[ReplayFailure] = []
        with pytest.raises(ReplayFailedError):
            await replay(
                store, [RejectsOne(poison.event_id)], strict=True, on_failure=streamed.append
            )
        assert [f.event_id for f in streamed] == [poison.event_id]


class TestSingleProjectionAndSucceededReport:
    async def test_replay_accepts_a_bare_single_projection(self) -> None:
        first, second = OrderPlaced(aggregate_id=uuid4()), OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(first, second)
        projection = Collecting()
        report = await replay(store, projection)
        assert [e.event_id for e in projection.seen] == [first.event_id, second.event_id]
        assert report.applied == 2 and report.succeeded is True
        report.raise_for_failures()

    async def test_succeeded_and_raise_for_failures_when_retained_or_truncated(self) -> None:
        poison = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(poison)
        failed_report = await replay(store, RejectsOne(poison.event_id))
        truncated_report = await replay(store, RejectsOne(poison.event_id), max_failures=0)
        assert failed_report.succeeded is False
        with pytest.raises(ReplayFailedError) as exc_info:
            failed_report.raise_for_failures()
        assert exc_info.value.failure.event_id == poison.event_id
        assert truncated_report.failures == () and truncated_report.failures_truncated == 1
        assert truncated_report.succeeded is False
        with pytest.raises(RuntimeError, match="1 truncated failure"):
            truncated_report.raise_for_failures()
