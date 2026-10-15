"""Unit tests for SubscriberRegistry and concurrent projection execution."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.projections.base import Projection
from eventsource.application.projections.coordinator import (
    ProjectionRegistry,
    SubscriberRegistry,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber
from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestSubscriberRegistry:
    @pytest.fixture
    def registry(self) -> SubscriberRegistry:
        return SubscriberRegistry()

    def test_register_subscriber(self, registry: SubscriberRegistry) -> None:
        class TestSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = TestSubscriber()
        registry.register(subscriber)
        assert subscriber in registry.subscribers
        assert registry.get_subscriber_count() == 1

    def test_unregister_subscriber(self, registry: SubscriberRegistry) -> None:
        class TestSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = TestSubscriber()
        registry.register(subscriber)
        result = registry.unregister(subscriber)
        assert result is True
        assert registry.get_subscriber_count() == 0

    def test_get_subscribers_for_event_type(self, registry: SubscriberRegistry) -> None:
        class OrderSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                pass

        class ShippingSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                pass

        order_sub = OrderSubscriber()
        shipping_sub = ShippingSubscriber()
        registry.register(order_sub)
        registry.register(shipping_sub)

        order_subs = registry.get_subscribers_for(OrderCreated)
        shipping_subs = registry.get_subscribers_for(OrderShipped)
        assert order_sub in order_subs
        assert shipping_sub not in order_subs
        assert shipping_sub in shipping_subs

    @pytest.mark.asyncio
    async def test_dispatch_to_interested_subscribers(self, registry: SubscriberRegistry) -> None:
        order_events: list[DomainEvent] = []
        shipping_events: list[DomainEvent] = []

        class OrderSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                order_events.append(event)

        class ShippingSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderShipped]

            async def handle(self, event: DomainEvent) -> None:
                shipping_events.append(event)

        registry.register(OrderSubscriber())
        registry.register(ShippingSubscriber())

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await registry.dispatch(created)
        await registry.dispatch(shipped)

        assert len(order_events) == 1
        assert len(shipping_events) == 1
        assert order_events[0] == created
        assert shipping_events[0] == shipped

    @pytest.mark.asyncio
    async def test_dispatch_handles_errors(
        self, registry: SubscriberRegistry, caplog: pytest.LogCaptureFixture
    ) -> None:
        good_events: list[DomainEvent] = []

        class FailingSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                raise ValueError("Subscriber failed")

        class GoodSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                good_events.append(event)

        registry.register(FailingSubscriber())
        registry.register(GoodSubscriber())
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        assert len(good_events) == 1
        assert any("Error in subscriber" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: SubscriberRegistry) -> None:
        events: list[DomainEvent] = []

        class TestSubscriber(EventSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

        registry.register(TestSubscriber())
        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")
        await registry.dispatch_many([event1, event2])

        assert len(events) == 2
        assert events[0] == event1
        assert events[1] == event2


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
                async with execution_lock:
                    execution_order.append("fast_end")

            async def reset(self) -> None:
                pass

        registry = ProjectionRegistry()
        registry.register_projection(SlowProjection())
        registry.register_projection(FastProjection())
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

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
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

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
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        assert peak_in_flight <= 2
        assert peak_in_flight == 2
