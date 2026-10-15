"""Unit tests for SubscriberRegistry and concurrent projection execution."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.projections.base import Projection
from eventsource.application.projections.coordinator import ProjectionRegistry, SubscriberRegistry
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
    OrderShipped,
)


class _SimpleSubscriber(EventSubscriber):
    def __init__(
        self,
        event_types: list[type[DomainEvent]] | None = None,
        sink: list[DomainEvent] | None = None,
        fail: bool = False,
    ) -> None:
        self._event_types = event_types or [OrderCreated]
        self._sink = sink
        self._fail = fail

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        if self._fail:
            raise ValueError("Subscriber failed")
        if self._sink is not None:
            self._sink.append(event)


class TestSubscriberRegistry:
    @pytest.fixture
    def registry(self) -> SubscriberRegistry:
        return SubscriberRegistry()

    def test_register_and_fluent_chaining(self, registry: SubscriberRegistry) -> None:
        sub1, sub2 = _SimpleSubscriber(), _SimpleSubscriber()
        returned = registry.register(sub1, sub2)
        assert returned is registry
        assert registry.subscribers == [sub1, sub2]
        assert registry.get_subscriber_count() == 2

    def test_init_with_subscribers_and_unregister(self) -> None:
        subscriber = _SimpleSubscriber()
        registry = SubscriberRegistry(subscribers=[subscriber])
        assert registry.get_subscribers_for(OrderCreated) == [subscriber]
        assert registry.unregister(subscriber) is True
        assert registry.get_subscriber_count() == 0
        assert registry.get_subscribers_for(OrderCreated) == []
        assert registry.unregister(subscriber) is False

    def test_get_subscribers_for_event_type(self, registry: SubscriberRegistry) -> None:
        order_sub = _SimpleSubscriber([OrderCreated])
        shipping_sub = _SimpleSubscriber([OrderShipped])
        registry.register(order_sub, shipping_sub)
        assert registry.get_subscribers_for(OrderCreated) == [order_sub]
        assert registry.get_subscribers_for(OrderShipped) == [shipping_sub]

    @pytest.mark.asyncio
    async def test_dispatch_to_interested_subscribers(self, registry: SubscriberRegistry) -> None:
        order_events: list[DomainEvent] = []
        shipping_events: list[DomainEvent] = []
        registry.register(
            _SimpleSubscriber([OrderCreated], sink=order_events),
            _SimpleSubscriber([OrderShipped], sink=shipping_events),
        )
        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await registry.dispatch(created)
        await registry.dispatch(shipped)
        assert order_events == [created]
        assert shipping_events == [shipped]

    @pytest.mark.asyncio
    async def test_dispatch_handles_errors(
        self, registry: SubscriberRegistry, caplog: pytest.LogCaptureFixture
    ) -> None:
        good_events: list[DomainEvent] = []
        registry.register(
            _SimpleSubscriber([OrderCreated], fail=True),
            _SimpleSubscriber([OrderCreated], sink=good_events),
        )
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(good_events) == 1
        assert any("Error in subscriber" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: SubscriberRegistry) -> None:
        events: list[DomainEvent] = []
        registry.register(_SimpleSubscriber([OrderCreated], sink=events))
        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")
        await registry.dispatch_many((event1, event2))
        assert events == [event1, event2]

    def test_container_dunders_and_clear(self, registry: SubscriberRegistry) -> None:
        assert bool(registry) is True
        assert len(registry) == 0
        sub1, sub2 = _SimpleSubscriber([OrderCreated]), _SimpleSubscriber([OrderShipped])
        registry.register(sub1)
        assert len(registry) == 1
        assert sub1 in registry and sub2 not in registry
        assert list(registry) == [sub1]
        registry.clear()
        assert len(registry) == 0
        assert sub1 not in registry
        assert registry.get_subscribers_for(OrderCreated) == []


class TestConcurrentExecution:
    @pytest.mark.asyncio
    async def test_projections_run_concurrently(self) -> None:
        execution_order: list[str] = []
        execution_lock = asyncio.Lock()

        class SlowProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                async with execution_lock:
                    execution_order.append("slow_start")
                await asyncio.sleep(0.1)
                async with execution_lock:
                    execution_order.append("slow_end")

            async def reset(self) -> None:
                pass

        class FastProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                async with execution_lock:
                    execution_order.append("fast_start")
                    execution_order.append("fast_end")

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry()
        registry.register_projection(SlowProjection())
        registry.register_projection(FastProjection())
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))

        _slow_start_idx = execution_order.index("slow_start")
        slow_end_idx = execution_order.index("slow_end")
        fast_start_idx = execution_order.index("fast_start")
        assert fast_start_idx < slow_end_idx

    @pytest.mark.asyncio
    async def test_registry_dispatch_respects_max_concurrency(self) -> None:
        in_flight = 0
        peak_in_flight = 0
        peak_lock = asyncio.Lock()

        class TrackedProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                nonlocal in_flight, peak_in_flight
                async with peak_lock:
                    in_flight += 1
                    peak_in_flight = max(peak_in_flight, in_flight)
                await asyncio.sleep(0.05)
                async with peak_lock:
                    in_flight -= 1

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry(max_concurrency=3)
        for _ in range(10):
            registry.register_projection(TrackedProjection())
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert peak_in_flight <= 3
        assert peak_in_flight == 3

    @pytest.mark.asyncio
    async def test_subscriber_registry_dispatch_respects_max_concurrency(self) -> None:
        in_flight = 0
        peak_in_flight = 0
        peak_lock = asyncio.Lock()

        class TrackedSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                nonlocal in_flight, peak_in_flight
                async with peak_lock:
                    in_flight += 1
                    peak_in_flight = max(peak_in_flight, in_flight)
                await asyncio.sleep(0.05)
                async with peak_lock:
                    in_flight -= 1

        registry = SubscriberRegistry(max_concurrency=2)
        for _ in range(8):
            registry.register(TrackedSubscriber())
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert peak_in_flight <= 2
        assert peak_in_flight == 2
