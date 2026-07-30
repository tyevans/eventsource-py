"""Conformance suite for the `CategoryQuery` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance
implementing both `CategoryQuery` and `EventAppender` (appending is the
only way to get events into the store to read back).
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import timedelta
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
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id)],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_b,
            [ConformanceEvent(aggregate_id=stream_b.aggregate_id)],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream_a,
            [ConformanceEvent(aggregate_id=stream_a.aggregate_id)],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(store.read_category("CategoryA"))

        assert len(envelopes) == 2
        assert all(e.stream_id.category == "CategoryA" for e in envelopes)

    async def test_ordered_by_stored_at(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Ordered")
        for i in range(4):
            await store.append(
                stream,
                [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))],
                ExpectedVersion.any_(),
            )

        envelopes = await collect(store.read_category("Ordered"))

        stored_ats = [e.stored_at for e in envelopes]
        assert stored_ats == sorted(stored_ats)

    async def test_from_timestamp_honored(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Timestamped")
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, payload="early")],
            ExpectedVersion.any_(),
        )
        early_envelopes = await collect(store.read_category("Timestamped"))
        cutoff = early_envelopes[0].stored_at + timedelta(milliseconds=1)

        # Ensure real wall-clock separation past the cutoff regardless of
        # the platform's clock resolution.
        await asyncio.sleep(0.01)

        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, payload="late")],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(
            store.read_category("Timestamped", CategoryReadOptions(from_timestamp=cutoff))
        )

        assert [e.event.payload for e in envelopes] == ["late"]  # type: ignore[attr-defined]

    async def test_from_timestamp_is_inclusive(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Inclusive")
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, payload="boundary")],
            ExpectedVersion.any_(),
        )
        envelopes = await collect(store.read_category("Inclusive"))
        boundary_stored_at = envelopes[0].stored_at

        envelopes = await collect(
            store.read_category("Inclusive", CategoryReadOptions(from_timestamp=boundary_stored_at))
        )

        assert [e.event.payload for e in envelopes] == ["boundary"]  # type: ignore[attr-defined]

    async def test_tenant_filter_honored(self, store: _AppenderCategory) -> None:
        tenant_a = uuid4()
        tenant_b = uuid4()
        stream = make_stream(category="Tenanted")
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload="1")],
            ExpectedVersion.any_(),
        )
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_b, payload="2")],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(
            store.read_category("Tenanted", CategoryReadOptions(tenant_id=tenant_a))
        )

        assert [e.event.payload for e in envelopes] == ["1"]  # type: ignore[attr-defined]

    async def test_limit_honored(self, store: _AppenderCategory) -> None:
        stream = make_stream(category="Limited")
        for i in range(5):
            await store.append(
                stream,
                [ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i))],
                ExpectedVersion.any_(),
            )

        envelopes = await collect(store.read_category("Limited", CategoryReadOptions(limit=2)))

        assert len(envelopes) == 2

    async def test_limit_from_timestamp_and_tenant_combined(self, store: _AppenderCategory) -> None:
        tenant_a = uuid4()
        tenant_b = uuid4()
        stream = make_stream(category="Combo")
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload="0")],
            ExpectedVersion.any_(),
        )
        early_envelopes = await collect(store.read_category("Combo"))
        cutoff = early_envelopes[0].stored_at

        await asyncio.sleep(0.01)

        for i in range(1, 5):
            await store.append(
                stream,
                [
                    ConformanceEvent(
                        aggregate_id=stream.aggregate_id, tenant_id=tenant_a, payload=str(i)
                    )
                ],
                ExpectedVersion.any_(),
            )
        await store.append(
            stream,
            [ConformanceEvent(aggregate_id=stream.aggregate_id, tenant_id=tenant_b, payload="x")],
            ExpectedVersion.any_(),
        )

        envelopes = await collect(
            store.read_category(
                "Combo",
                CategoryReadOptions(
                    from_timestamp=cutoff + timedelta(milliseconds=1),
                    tenant_id=tenant_a,
                    limit=2,
                ),
            )
        )

        assert [e.event.payload for e in envelopes] == ["1", "2"]  # type: ignore[attr-defined]

    async def test_limit_with_batch_ties_returns_earliest_in_stream_order(
        self, store: _AppenderCategory
    ) -> None:
        """Pins the tie-break: a batch appended in one call shares a storage
        timestamp on some adapters (sqlite/postgresql stamp one `now` per
        `append()` call), so `limit` must fall back to a stable secondary
        order (position / insertion order) rather than an arbitrary one.
        """
        stream = make_stream(category="Determinism")
        batch = [
            ConformanceEvent(aggregate_id=stream.aggregate_id, payload=str(i)) for i in range(5)
        ]
        await store.append(stream, batch, ExpectedVersion.any_())

        envelopes = await collect(store.read_category("Determinism", CategoryReadOptions(limit=3)))

        assert [e.event.payload for e in envelopes] == ["0", "1", "2"]  # type: ignore[attr-defined]
