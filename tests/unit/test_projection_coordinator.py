"""
Unit tests for projection coordinator and registries.

Tests cover:
- ProjectionRegistry registration and dispatch
- ProjectionCoordinator event distribution
- SubscriberRegistry event filtering
- Concurrent execution
- Error handling
"""

import asyncio
from uuid import uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.projections.base import EventHandlerBase, Projection
from eventsource.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,
)
from eventsource.protocols import EventSubscriber


# Sample events for testing
class OrderCreated(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class TestProjectionRegistry:
    """Tests for ProjectionRegistry."""

    @pytest.fixture
    def registry(self) -> ProjectionRegistry:
        """Create a fresh registry."""
        return ProjectionRegistry()

    def test_register_projection(self, registry: ProjectionRegistry) -> None:
        """Can register a projection."""

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        projection = TestProjection()
        registry.register_projection(projection)

        assert projection in registry.projections
        assert registry.get_projection_count() == 1

    def test_register_multiple_projections(self, registry: ProjectionRegistry) -> None:
        """Can register multiple projections."""

        class Projection1(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        class Projection2(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        registry.register_projection(Projection1())
        registry.register_projection(Projection2())

        assert registry.get_projection_count() == 2

    def test_register_handler(self, registry: ProjectionRegistry) -> None:
        """Can register an event handler."""

        class TestHandler(EventHandlerBase):
            def can_handle(self, event: DomainEvent) -> bool:
                return True

            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = TestHandler()
        registry.register_handler(handler)

        assert handler in registry.handlers
        assert registry.get_handler_count() == 1

    def test_unregister_projection(self, registry: ProjectionRegistry) -> None:
        """Can unregister a projection."""

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        projection = TestProjection()
        registry.register_projection(projection)
        assert registry.get_projection_count() == 1

        result = registry.unregister_projection(projection)
        assert result is True
        assert registry.get_projection_count() == 0

    def test_unregister_nonexistent_projection(self, registry: ProjectionRegistry) -> None:
        """Unregistering nonexistent projection returns False."""

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        projection = TestProjection()
        result = registry.unregister_projection(projection)
        assert result is False

    @pytest.mark.asyncio
    async def test_dispatch_to_all_projections(self, registry: ProjectionRegistry) -> None:
        """dispatch sends event to all projections."""
        events1: list[DomainEvent] = []
        events2: list[DomainEvent] = []

        class Projection1(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events1.append(event)

            async def reset(self) -> None:
                events1.clear()

        class Projection2(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events2.append(event)

            async def reset(self) -> None:
                events2.clear()

        registry.register_projection(Projection1())
        registry.register_projection(Projection2())

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        assert len(events1) == 1
        assert len(events2) == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_handlers(self, registry: ProjectionRegistry) -> None:
        """dispatch sends event to handlers that can handle it."""
        handled_events: list[DomainEvent] = []

        class SelectiveHandler(EventHandlerBase):
            def can_handle(self, event: DomainEvent) -> bool:
                return isinstance(event, OrderCreated)

            async def handle(self, event: DomainEvent) -> None:
                handled_events.append(event)

        registry.register_handler(SelectiveHandler())

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await registry.dispatch(created)
        await registry.dispatch(shipped)

        # Only OrderCreated should be handled
        assert len(handled_events) == 1
        assert handled_events[0] == created

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: ProjectionRegistry) -> None:
        """dispatch_many processes events in order."""
        events: list[DomainEvent] = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                events.clear()

        registry.register_projection(TestProjection())

        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")
        event3 = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await registry.dispatch_many([event1, event2, event3])

        assert len(events) == 3
        assert events[0] == event1
        assert events[1] == event2
        assert events[2] == event3

    @pytest.mark.asyncio
    async def test_dispatch_handles_errors_gracefully(
        self, registry: ProjectionRegistry, caplog: pytest.LogCaptureFixture
    ) -> None:
        """dispatch logs errors but continues with other projections."""
        good_events: list[DomainEvent] = []

        class FailingProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                raise ValueError("Projection failed")

            async def reset(self) -> None:
                pass

        class GoodProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                good_events.append(event)

            async def reset(self) -> None:
                good_events.clear()

        registry.register_projection(FailingProjection())
        registry.register_projection(GoodProjection())

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await registry.dispatch(event)

        # GoodProjection should still receive the event
        assert len(good_events) == 1

        # Error should be logged
        assert any("Error in projection" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_reset_all(self, registry: ProjectionRegistry) -> None:
        """reset_all resets all projections."""
        reset_called = [False, False]

        class Projection1(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                reset_called[0] = True

        class Projection2(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                reset_called[1] = True

        registry.register_projection(Projection1())
        registry.register_projection(Projection2())

        await registry.reset_all()

        assert reset_called[0] is True
        assert reset_called[1] is True


class TestProjectionCoordinator:
    """Tests for ProjectionCoordinator."""

    @pytest.fixture
    def registry(self) -> ProjectionRegistry:
        """Create a registry with projections."""
        return ProjectionRegistry()

    @pytest.fixture
    def coordinator(self, registry: ProjectionRegistry) -> ProjectionCoordinator:
        """Create a coordinator."""
        return ProjectionCoordinator(registry=registry)

    @pytest.mark.asyncio
    async def test_dispatch_events(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """dispatch_events dispatches to registry."""
        events: list[DomainEvent] = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                events.clear()

        registry.register_projection(TestProjection())

        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")

        count = await coordinator.dispatch_events([event1, event2])

        assert count == 2
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_rebuild_all(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """rebuild_all resets and replays events."""
        reset_called = False
        events: list[DomainEvent] = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                nonlocal reset_called
                reset_called = True
                events.clear()

        registry.register_projection(TestProjection())

        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")

        count = await coordinator.rebuild_all([event1, event2])

        assert count == 2
        assert reset_called is True
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_rebuild_projection(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """rebuild_projection resets single projection and replays."""
        events: list[DomainEvent] = []
        reset_called = False

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                nonlocal reset_called
                reset_called = True
                events.clear()

        projection = TestProjection()
        registry.register_projection(projection)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        count = await coordinator.rebuild_projection(projection, [event])

        assert count == 1
        assert reset_called is True
        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_catchup(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """catchup processes events without reset."""
        events: list[DomainEvent] = []
        reset_called = False

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                nonlocal reset_called
                reset_called = True

        projection = TestProjection()
        registry.register_projection(projection)

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        count = await coordinator.catchup(projection, [event])

        assert count == 1
        assert reset_called is False  # No reset for catchup
        assert len(events) == 1

    def test_get_projection_info(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """get_projection_info returns projection info."""

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        registry.register_projection(TestProjection())

        info = coordinator.get_projection_info()

        assert len(info) == 1
        assert info[0]["name"] == "TestProjection"

    @pytest.mark.asyncio
    async def test_health_check(
        self,
        registry: ProjectionRegistry,
        coordinator: ProjectionCoordinator,
    ) -> None:
        """health_check returns status."""

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        class TestHandler(EventHandlerBase):
            def can_handle(self, event: DomainEvent) -> bool:
                return True

            async def handle(self, event: DomainEvent) -> None:
                pass

        registry.register_projection(TestProjection())
        registry.register_handler(TestHandler())

        health = await coordinator.health_check()

        assert health["status"] == "healthy"
        assert health["projection_count"] == 1
        assert health["handler_count"] == 1
        assert "TestProjection" in health["projections"]
        assert "TestHandler" in health["handlers"]


class TestSubscriberRegistry:
    """Tests for SubscriberRegistry."""

    @pytest.fixture
    def registry(self) -> SubscriberRegistry:
        """Create a fresh subscriber registry."""
        return SubscriberRegistry()

    def test_register_subscriber(self, registry: SubscriberRegistry) -> None:
        """Can register a subscriber."""

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
        """Can unregister a subscriber."""

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
        """get_subscribers_for returns subscribers for event type."""

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
        """dispatch only sends to interested subscribers."""
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
        """dispatch logs errors and continues."""
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

        # GoodSubscriber should still receive event
        assert len(good_events) == 1

        # Error logged
        assert any("Error in subscriber" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: SubscriberRegistry) -> None:
        """dispatch_many processes events in order."""
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
    """Tests for concurrent execution behavior."""

    @pytest.mark.asyncio
    async def test_projections_run_concurrently(self) -> None:
        """Projections execute concurrently within dispatch."""
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

        # Both should start before slow finishes
        _slow_start_idx = execution_order.index("slow_start")
        slow_end_idx = execution_order.index("slow_end")
        fast_start_idx = execution_order.index("fast_start")

        # Fast should start while slow is running
        assert fast_start_idx < slow_end_idx
