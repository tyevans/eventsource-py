"""Tests for bounded reads, cursor guards, and query scoping in replay."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.projections.replay import REPLAY_BATCH_SIZE, replay
from tests.unit.application.projections.test_replay._fixtures import (
    _REGISTRY,
    TENANT_A,
    TENANT_B,
    Collecting,
    InvoiceIssued,
    NonAdvancingFeed,
    OrderPlaced,
    RecordingFeed,
    _store_with,
)


class TestTheReadIsBounded:
    """Every feed read carries a batch limit to avoid unbounded materialization."""

    async def test_every_read_carries_a_limit(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)
        await replay(feed, [Collecting()])
        assert feed.calls
        for _, options in feed.calls:
            assert options is not None and options.limit == REPLAY_BATCH_SIZE

    async def test_a_feed_longer_than_one_batch_is_read_in_several(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(5)))
        feed, collecting = RecordingFeed(store), Collecting()
        report = await replay(feed, [collecting], batch_size=2)
        assert len(feed.calls) == 3
        assert [options.limit for _, options in feed.calls if options] == [2, 2, 2]
        assert len(collecting.seen) == 5 and report.applied == 5

    async def test_each_batch_resumes_after_the_previous_one(self) -> None:
        """`from_position` is exclusive, so no event is seen twice or skipped."""
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(4)))
        feed, collecting = RecordingFeed(store), Collecting()
        await replay(feed, [collecting], batch_size=2)
        cursors = [from_position for from_position, _ in feed.calls]
        assert cursors[0] is None and all(cursor is not None for cursor in cursors[1:])
        assert len({event.event_id for event in collecting.seen}) == 4

    async def test_an_empty_feed_reads_once_and_stops(self) -> None:
        feed = RecordingFeed(InMemoryEventStore(event_registry=_REGISTRY))
        report = await replay(feed, [Collecting()])
        assert len(feed.calls) == 1 and report.applied == 0 and report.last_position is None


class TestScopingReachesTheAdaptersQuery:
    """Filtering in Python would give the same answers, so assert the ask."""

    async def test_the_tenant_is_forwarded_as_feed_read_options(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A))
        feed = RecordingFeed(store)
        await replay(feed, [Collecting()], tenant_id=TENANT_A)
        ((_, options),) = feed.calls
        assert options is not None
        assert options.tenant_id == TENANT_A and options.aggregate_type is None

    async def test_the_aggregate_type_is_forwarded_as_feed_read_options(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)
        await replay(feed, [Collecting()], aggregate_type="Order")
        ((_, options),) = feed.calls
        assert options is not None
        assert options.aggregate_type == "Order" and options.tenant_id is None

    async def test_both_filters_travel_together(self) -> None:
        store = await _store_with(OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A))
        feed = RecordingFeed(store)
        await replay(feed, [Collecting()], tenant_id=TENANT_A, aggregate_type="Order")
        ((_, options),) = feed.calls
        assert options is not None
        assert (options.tenant_id, options.aggregate_type) == (TENANT_A, "Order")

    async def test_no_filter_is_sent_when_nothing_is_named(self) -> None:
        """Whole-feed rebuild sends options with batch limit and None filter fields."""
        store = await _store_with(OrderPlaced(aggregate_id=uuid4()))
        feed = RecordingFeed(store)
        await replay(feed, [Collecting()])
        ((_, options),) = feed.calls
        assert options is not None
        assert options.tenant_id is None and options.aggregate_type is None
        assert options.limit is not None

    async def test_from_position_is_forwarded_unchanged(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(2)))
        first = [envelope async for envelope in store.read_all()][0]
        feed = RecordingFeed(store)
        report = await replay(feed, [Collecting()], from_position=first.position)
        ((sent, _),) = feed.calls
        assert sent == first.position and report.applied == 1


class TestTheScopedReplaySeesOnlyThatSlice:
    """Verify the adapter honors the forwarded read scoping options."""

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
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A),
            InvoiceIssued(aggregate_id=uuid4(), tenant_id=TENANT_B),
            InvoiceIssued(aggregate_id=uuid4(), tenant_id=TENANT_B),
        )
        report = await replay(store, [Collecting()])
        assert report.applied == 3

    async def test_the_last_position_is_the_last_matching_event(self) -> None:
        """Filtered replay reports the last matching event position."""
        store = await _store_with(
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_A),
            OrderPlaced(aggregate_id=uuid4(), tenant_id=TENANT_B),
        )
        scoped = await replay(store, [Collecting()], tenant_id=TENANT_A)
        whole = await replay(store, [Collecting()])
        assert scoped.last_position is not None and whole.last_position is not None
        assert scoped.last_position < whole.last_position

    async def test_an_empty_feed_reports_no_last_position(self) -> None:
        report = await replay(InMemoryEventStore(event_registry=_REGISTRY), [Collecting()])
        assert report.last_position is None and report.applied == 0

    async def test_event_types_filters_by_string_or_class_while_advancing_position(self) -> None:
        order1 = OrderPlaced(aggregate_id=uuid4())
        invoice = InvoiceIssued(aggregate_id=uuid4())
        order2 = OrderPlaced(aggregate_id=uuid4())
        store = await _store_with(order1, invoice, order2)
        whole = await replay(store, [Collecting()])

        by_name, by_cls = Collecting(), Collecting()
        rep_name = await replay(store, [by_name], event_types=["InvoiceIssued"])
        rep_cls = await replay(store, [by_cls], event_types=(InvoiceIssued,))

        assert [e.event_id for e in by_name.seen] == [invoice.event_id]
        assert [e.event_id for e in by_cls.seen] == [invoice.event_id]
        assert rep_name.applied == rep_cls.applied == 1
        assert rep_name.last_position == rep_cls.last_position == whole.last_position


class TestMaxEventsGuardsAgainstANonAdvancingCursor:
    async def test_it_raises_rather_than_hanging(self) -> None:
        with pytest.raises(RuntimeError, match="cursor is probably not advancing"):
            await replay(NonAdvancingFeed(), [Collecting()], max_events=10)

    async def test_a_feed_that_ends_within_the_bound_is_unaffected(self) -> None:
        store = await _store_with(*(OrderPlaced(aggregate_id=uuid4()) for _ in range(3)))
        report = await replay(store, [Collecting()], max_events=3)
        assert report.applied == 3
