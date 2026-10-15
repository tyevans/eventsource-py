"""Property-based tests for InMemoryEventStore category index and bisect lookups."""

from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import (
    CategoryReadOptions,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    PositionForeignError,
    ReadDirection,
    StreamReadOptions,
    collect,
)

_REGISTRY = EventRegistry()
_CATEGORIES = ("Order", "Invoice", "Customer", "Shipment")
_TENANTS = (None, UUID("00000000-0000-0000-0000-000000000001"), UUID("00000000-0000-0000-0000-000000000002"))  # fmt: skip


@register_event(registry=_REGISTRY)
class PropEvent(DomainEvent):
    aggregate_type: str = "Order"
    payload: int = 0


_append_spec = st.tuples(
    st.sampled_from(_CATEGORIES),
    st.integers(min_value=0, max_value=2),  # stream slot within category
    st.sampled_from(_TENANTS),
    st.integers(min_value=1, max_value=3),  # batch size
)


async def _populate_store(
    specs: list[tuple[str, int, UUID | None, int]],
) -> tuple[InMemoryEventStore, dict[tuple[str, int], StreamId]]:
    store = InMemoryEventStore(event_registry=_REGISTRY)
    streams: dict[tuple[str, int], StreamId] = {}
    seq = 0
    for category, slot, tenant_id, batch_size in specs:
        stream = streams.setdefault((category, slot), StreamId(aggregate_id=uuid4(), category=category))  # fmt: skip
        batch = [
            PropEvent(
                aggregate_id=stream.aggregate_id,
                aggregate_type=category,
                tenant_id=tenant_id,
                payload=seq + idx,
            )  # fmt: skip
            for idx in range(batch_size)
        ]
        seq += batch_size
        await store.append(stream, batch, ExpectedVersion.any_())
    return store, streams


@given(
    specs=st.lists(_append_spec, min_size=0, max_size=20),
    query_category=st.sampled_from((*_CATEGORIES, "AbsentCategory")),
    query_tenant=st.sampled_from(_TENANTS),
    timestamp_idx=st.one_of(st.none(), st.integers(min_value=0, max_value=30)),
    limit=st.one_of(st.none(), st.integers(min_value=0, max_value=15)),
)
async def test_read_category_matches_linear_filter_over_read_all(
    specs: list[tuple[str, int, UUID | None, int]],
    query_category: str,
    query_tenant: UUID | None,
    timestamp_idx: int | None,
    limit: int | None,
) -> None:
    store, _ = await _populate_store(specs)
    all_envelopes = await collect(store.read_all())
    from_timestamp = (
        all_envelopes[timestamp_idx % len(all_envelopes)].stored_at
        if timestamp_idx is not None and all_envelopes
        else None
    )
    options = CategoryReadOptions(tenant_id=query_tenant, from_timestamp=from_timestamp, limit=limit)  # fmt: skip
    actual = await collect(store.read_category(query_category, options))

    expected = [e for e in all_envelopes if e.stream_id.category == query_category]
    if query_tenant is not None:
        expected = [e for e in expected if e.event.tenant_id == query_tenant]
    if from_timestamp is not None:
        expected = [e for e in expected if e.stored_at >= from_timestamp]
    expected = sorted(expected, key=lambda e: (e.stored_at, e.position.key if e.position else ()))
    if limit is not None:
        expected = expected[: max(0, limit)]
    assert actual == expected


@given(
    specs=st.lists(_append_spec, min_size=0, max_size=20),
    query_category=st.one_of(st.none(), st.sampled_from((*_CATEGORIES, "AbsentCategory"))),
    query_tenant=st.sampled_from(_TENANTS),
    after_offset=st.one_of(st.none(), st.integers(min_value=0, max_value=65)),
    limit=st.one_of(st.none(), st.integers(min_value=0, max_value=15)),
)
async def test_read_all_bisect_and_category_index_matches_linear_filter(
    specs: list[tuple[str, int, UUID | None, int]],
    query_category: str | None,
    query_tenant: UUID | None,
    after_offset: int | None,
    limit: int | None,
) -> None:
    store, _ = await _populate_store(specs)
    full = await collect(store.read_all())
    from_position = Position(store_id=store.store_id, key=(after_offset,)) if after_offset is not None else None  # fmt: skip
    options = FeedReadOptions(tenant_id=query_tenant, aggregate_type=query_category, limit=limit)
    actual = await collect(store.read_all(from_position=from_position, options=options))

    expected = list(full)
    if from_position is not None:
        expected = [e for e in expected if e.position is not None and e.position > from_position]
    if query_tenant is not None:
        expected = [e for e in expected if e.event.tenant_id == query_tenant]
    if query_category is not None:
        expected = [e for e in expected if e.stream_id.category == query_category]
    if limit is not None:
        expected = expected[: max(0, limit)]
    assert actual == expected


@given(
    n_events=st.integers(min_value=0, max_value=25),
    from_version=st.one_of(st.none(), st.integers(min_value=0, max_value=30)),
    to_version=st.one_of(st.none(), st.integers(min_value=0, max_value=30)),
    direction=st.sampled_from([ReadDirection.FORWARD, ReadDirection.BACKWARD]),
    limit=st.one_of(st.none(), st.integers(min_value=0, max_value=30)),
)
async def test_read_stream_bisect_matches_linear_filter(
    n_events: int,
    from_version: int | None,
    to_version: int | None,
    direction: ReadDirection,
    limit: int | None,
) -> None:
    store = InMemoryEventStore(event_registry=_REGISTRY)
    stream = StreamId(aggregate_id=uuid4(), category="Order")
    if n_events > 0:
        events = [PropEvent(aggregate_id=stream.aggregate_id, payload=i) for i in range(n_events)]
        await store.append(stream, events, ExpectedVersion.no_stream())
    full_stream = await collect(store.read_stream(stream))
    options = StreamReadOptions(direction=direction, from_version=from_version, to_version=to_version, limit=limit)  # fmt: skip
    actual = await collect(store.read_stream(stream, options))

    expected = list(full_stream)
    if from_version is not None:
        expected = [e for e in expected if e.stream_version >= from_version]
    if to_version is not None:
        expected = [e for e in expected if e.stream_version <= to_version]
    if direction == ReadDirection.BACKWARD:
        expected = list(reversed(expected))
    if limit is not None:
        expected = expected[: max(0, limit)]
    assert actual == expected


async def test_read_all_rejects_foreign_position() -> None:
    store = InMemoryEventStore(store_id="store-a", event_registry=_REGISTRY)
    foreign = Position(store_id="store-b", key=(1,))
    with pytest.raises(PositionForeignError):
        await collect(store.read_all(from_position=foreign))


async def test_contains_stream_exists_and_single_event_append() -> None:
    store = InMemoryEventStore(event_registry=_REGISTRY)
    stream = StreamId(aggregate_id=uuid4(), category="Order")
    event = PropEvent(aggregate_id=stream.aggregate_id, payload=42)
    assert event.event_id not in store
    assert event not in store
    assert "invalid" not in store
    assert await store.stream_exists(stream) is False
    result = await store.append(stream, event)
    assert result.new_version == 1
    assert event.event_id in store
    assert event in store
    assert "invalid" not in store
    assert await store.stream_exists(stream) is True
