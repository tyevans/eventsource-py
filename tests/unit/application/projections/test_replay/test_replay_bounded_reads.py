"""Tests for bounded reads, cursor guards, and query scoping in replay."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.projections.replay import (
    REPLAY_BATCH_SIZE,
    replay,
)
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
