"""Unit and property tests for published_of_type on InMemoryEventBus and RecordingEventBus."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.testing.recording import RecordingEventBus


class OrderEvent(DomainEvent):
    aggregate_type: str = "Order"


class OrderCreated(OrderEvent):
    customer_id: str = "cust-1"


class OrderShipped(OrderEvent):
    tracking_number: str = "trk-1"


class PaymentReceived(DomainEvent):
    aggregate_type: str = "Payment"
    amount_cents: int = 100


async def test_published_of_type_empty_bus() -> None:
    mem_bus = InMemoryEventBus(enable_tracing=False)
    rec_bus = RecordingEventBus(InMemoryEventBus(enable_tracing=False))

    assert mem_bus.published_of_type(OrderCreated) == []
    assert rec_bus.published_of_type(OrderCreated) == []


async def test_published_of_type_filters_and_infers_static_type() -> None:
    mem_bus = InMemoryEventBus(enable_tracing=False)
    rec_bus = RecordingEventBus(mem_bus)

    created_1 = OrderCreated(aggregate_id=uuid4(), customer_id="c1")
    shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="t1")
    payment = PaymentReceived(aggregate_id=uuid4(), amount_cents=500)
    created_2 = OrderCreated(aggregate_id=uuid4(), customer_id="c2")

    await rec_bus.publish([created_1, shipped, payment, created_2])

    mem_created: list[OrderCreated] = mem_bus.published_of_type(OrderCreated)
    rec_created: list[OrderCreated] = rec_bus.published_of_type(OrderCreated)

    assert mem_created == [created_1, created_2]
    assert rec_created == [created_1, created_2]
    assert [e.customer_id for e in rec_created] == ["c1", "c2"]

    mem_shipped: list[OrderShipped] = mem_bus.published_of_type(OrderShipped)
    rec_shipped: list[OrderShipped] = rec_bus.published_of_type(OrderShipped)
    assert mem_shipped == [shipped]
    assert rec_shipped == [shipped]


async def test_published_of_type_respects_subclass_hierarchy() -> None:
    mem_bus = InMemoryEventBus(enable_tracing=False)
    rec_bus = RecordingEventBus(mem_bus)

    created = OrderCreated(aggregate_id=uuid4())
    shipped = OrderShipped(aggregate_id=uuid4())
    payment = PaymentReceived(aggregate_id=uuid4())

    await rec_bus.publish([created, payment, shipped])

    assert mem_bus.published_of_type(OrderEvent) == [created, shipped]
    assert rec_bus.published_of_type(OrderEvent) == [created, shipped]
    assert mem_bus.published_of_type(DomainEvent) == [created, payment, shipped]
    assert rec_bus.published_of_type(DomainEvent) == [created, payment, shipped]


async def test_clear_published_events_resets_published_of_type() -> None:
    mem_bus = InMemoryEventBus(enable_tracing=False)
    rec_bus = RecordingEventBus(mem_bus)

    await rec_bus.publish([OrderCreated(aggregate_id=uuid4())])
    assert len(mem_bus.published_of_type(OrderCreated)) == 1
    assert len(rec_bus.published_of_type(OrderCreated)) == 1

    mem_bus.clear_published_events()
    rec_bus.clear_published_events()

    assert mem_bus.published_of_type(OrderCreated) == []
    assert rec_bus.published_of_type(OrderCreated) == []


_EVENT_FACTORIES = (
    lambda: OrderCreated(aggregate_id=uuid4()),
    lambda: OrderShipped(aggregate_id=uuid4()),
    lambda: PaymentReceived(aggregate_id=uuid4()),
)

_TARGET_TYPES: tuple[type[DomainEvent], ...] = (
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    OrderEvent,
    DomainEvent,
)


@given(
    kind_indices=st.lists(st.integers(min_value=0, max_value=2), max_size=25),
    target_idx=st.integers(min_value=0, max_value=len(_TARGET_TYPES) - 1),
)
def test_property_published_of_type_matches_isinstance_in_publish_order(
    kind_indices: list[int],
    target_idx: int,
) -> None:
    events: list[DomainEvent] = [_EVENT_FACTORIES[idx]() for idx in kind_indices]
    target_cls = _TARGET_TYPES[target_idx]

    mem_bus = InMemoryEventBus(enable_tracing=False)
    rec_bus = RecordingEventBus(mem_bus)

    asyncio.run(rec_bus.publish(events))

    expected_mem = [e for e in mem_bus.published_events if isinstance(e, target_cls)]
    expected_rec = [e for e in rec_bus.published_events if isinstance(e, target_cls)]

    assert mem_bus.published_of_type(target_cls) == expected_mem
    assert rec_bus.published_of_type(target_cls) == expected_rec
