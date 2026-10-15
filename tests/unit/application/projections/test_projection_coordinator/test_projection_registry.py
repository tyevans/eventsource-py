"""Unit tests for ProjectionRegistry."""

from uuid import uuid4

import pytest

from eventsource.application.projections.base import EventHandlerBase, Projection
from eventsource.application.projections.coordinator import ProjectionRegistry
from eventsource.domain.event import DomainEvent
from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestProjectionRegistry:
    @pytest.fixture
    def registry(self) -> ProjectionRegistry:
        return ProjectionRegistry()

    def test_register_projection(self, registry: ProjectionRegistry) -> None:
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
        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        assert registry.unregister_projection(TestProjection()) is False

    @pytest.mark.asyncio
    async def test_dispatch_to_all_projections(self, registry: ProjectionRegistry) -> None:
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
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(events1) == 1
        assert len(events2) == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_handlers(self, registry: ProjectionRegistry) -> None:
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
        assert len(handled_events) == 1
        assert handled_events[0] == created

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: ProjectionRegistry) -> None:
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
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(good_events) == 1
        assert any("Error in projection" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_reset_all(self, registry: ProjectionRegistry) -> None:
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
