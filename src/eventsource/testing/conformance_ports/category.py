"""Conformance suite for the `CategoryQuery` port."""

import asyncio
from abc import ABC, abstractmethod
from datetime import timedelta, timezone
from typing import Protocol
from uuid import uuid4

import pytest

from eventsource.ports import CategoryReadOptions, ExpectedVersion, collect
from eventsource.ports.store import CategoryQuery, EventAppender
from eventsource.testing.conformance_ports._fixtures import ConformanceEvent, make_stream


class _AppenderCategory(EventAppender, CategoryQuery, Protocol):
    """Adapter surface needed by this suite: append plus category reads."""


class CategoryQueryConformance(ABC):
    """Conformance suite for `CategoryQuery` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `CategoryQuery` + `EventAppender`."""
        raise NotImplementedError

    async def test_only_named_category_returned(self, store: _AppenderCategory) -> None:
        stream_a = make_stream(category="CategoryA", aggregate_id=uuid4())
        stream_b = make_stream(category="CategoryB", aggregate_id=uuid4())
        await store.append(
            stream_a, [ConformanceEvent(aggregate_id=stream_a.aggregate_id)], ExpectedVersion.any_()
        )
        await store.append(
            stream_b, [ConformanceEvent(aggregate_id=stream_b.aggregate_id)], ExpectedVersion.any_()
        )
        await store.append(
            stream_a, [ConformanceEvent(aggregate_id=stream_a.aggregate_id)], ExpectedVersion.any_()
        )

        envelopes = await collect(store.read_category("CategoryA"))
        assert len(envelopes) == 2
        assert all(e.stream_id.category == "CategoryA" for e in envelopes)

    async def test_ordered_by_stored_at(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Ordered")
        for i in range(4):
            ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))
            await store.append(stream, [ev], ExpectedVersion.any_())

        envelopes = await collect(store.read_category("Ordered"))
        stored_ats = [e.stored_at for e in envelopes]
        assert stored_ats == sorted(stored_ats)

    async def test_from_timestamp_honored(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Timestamped")
        early_ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload="early")
        await store.append(stream, [early_ev], ExpectedVersion.any_())
        early_envelopes = await collect(store.read_category("Timestamped"))
        cutoff = early_envelopes[0].stored_at + timedelta(milliseconds=1)

        await asyncio.sleep(0.01)

        late_ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload="late")
        await store.append(stream, [late_ev], ExpectedVersion.any_())

        opts = CategoryReadOptions(from_timestamp=cutoff)
        envelopes = await collect(store.read_category("Timestamped", opts))
        assert [e.event.payload for e in envelopes] == ["late"]  # type: ignore[attr-defined]

    async def test_from_timestamp_compares_instants_not_offsets(
        self, store: _AppenderCategory
    ) -> None:
        """The same instant expressed in another timezone must filter the same."""
        stream = make_stream(category="Offsets")
        early_ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload="early")
        await store.append(stream, [early_ev], ExpectedVersion.any_())
        early_envelopes = await collect(store.read_category("Offsets"))
        cutoff = early_envelopes[0].stored_at + timedelta(milliseconds=1)

        await asyncio.sleep(0.01)

        late_ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload="late")
        await store.append(stream, [late_ev], ExpectedVersion.any_())

        shifted = cutoff.astimezone(timezone(timedelta(hours=5)))
        envelopes = await collect(
            store.read_category("Offsets", CategoryReadOptions(from_timestamp=shifted))
        )
        assert [e.event.payload for e in envelopes] == ["late"]  # type: ignore[attr-defined]

    async def test_from_timestamp_is_inclusive(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Inclusive")
        ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload="boundary")
        await store.append(stream, [ev], ExpectedVersion.any_())
        envelopes = await collect(store.read_category("Inclusive"))
        boundary_stored_at = envelopes[0].stored_at

        envelopes = await collect(
            store.read_category("Inclusive", CategoryReadOptions(from_timestamp=boundary_stored_at))
        )
        assert [e.event.payload for e in envelopes] == ["boundary"]  # type: ignore[attr-defined]

    async def test_tenant_filter_honored(self, store: _AppenderCategory) -> None:
        tenant_a, tenant_b = uuid4(), uuid4()
        stream = make_stream(category="Tenanted")
        ev_a = ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload="1")
        ev_b = ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_b, payload="2")
        await store.append(stream, [ev_a], ExpectedVersion.any_())
        await store.append(stream, [ev_b], ExpectedVersion.any_())

        envelopes = await collect(
            store.read_category("Tenanted", CategoryReadOptions(tenant_id=tenant_a))
        )
        assert [e.event.payload for e in envelopes] == ["1"]  # type: ignore[attr-defined]

    async def test_limit_honored(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Limited")
        for i in range(5):
            ev = ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))
            await store.append(stream, [ev], ExpectedVersion.any_())

        envelopes = await collect(store.read_category("Limited", CategoryReadOptions(limit=2)))
        assert len(envelopes) == 2

    async def test_limit_from_timestamp_and_tenant_combined(self, store: _AppenderCategory) -> None:
        tenant_a, tenant_b = uuid4(), uuid4()
        stream = make_stream(category="Combo")
        ev_0 = ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload="0")
        await store.append(stream, [ev_0], ExpectedVersion.any_())
        early_envelopes = await collect(store.read_category("Combo"))
        cutoff = early_envelopes[0].stored_at

        await asyncio.sleep(0.01)

        for i in range(1, 5):
            ev = ConformanceEvent(
                aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload=str(i)
            )
            await store.append(stream, [ev], ExpectedVersion.any_())
        ev_x = ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_b, payload="x")
        await store.append(stream, [ev_x], ExpectedVersion.any_())

        opts = CategoryReadOptions(
            from_timestamp=cutoff + timedelta(milliseconds=1), tenant_id=tenant_a, limit=2
        )
        envelopes = await collect(store.read_category("Combo", opts))
        assert [e.event.payload for e in envelopes] == ["1", "2"]  # type: ignore[attr-defined]

    async def test_limit_with_batch_ties_returns_earliest_in_stream_order(
        self, store: _AppenderCategory
    ) -> None:
        """Batch ties on storage timestamp must break by stream/insertion order."""
        stream = make_stream(category="Determinism")
        batch = [
            ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i)) for i in range(5)
        ]
        await store.append(stream, batch, ExpectedVersion.any_())

        envelopes = await collect(store.read_category("Determinism", CategoryReadOptions(limit=3)))
        assert [e.event.payload for e in envelopes] == ["0", "1", "2"]  # type: ignore[attr-defined]
