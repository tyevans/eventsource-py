"""Unit tests for ProjectionRegistry and ProjectionCoordinator container helpers."""

from uuid import uuid4

import pytest

from eventsource.application.projections.base import EventHandlerBase, Projection
from eventsource.application.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
)
from eventsource.domain.event import DomainEvent
from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
    OrderShipped,
)


class _RecordingProjection(Projection):
    def __init__(self) -> None:
        self.events: list[DomainEvent] = []
        self.reset_called = False

    async def handle(self, event: DomainEvent) -> None:
        self.events.append(event)

    async def reset(self) -> None:
        self.events.clear()
        self.reset_called = True


class _SelectiveHandler(EventHandlerBase):
    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    def can_handle(self, event: DomainEvent) -> bool:
        return isinstance(event, OrderCreated)

    async def handle(self, event: DomainEvent) -> None:
        self.events.append(event)


class TestProjectionRegistry:
    @pytest.fixture
    def registry(self) -> ProjectionRegistry:
        return ProjectionRegistry()

    def test_register_projection(self, registry: ProjectionRegistry) -> None:
        projection = _RecordingProjection()
        registry.register_projection(projection)
        assert projection in registry.projections
        assert registry.get_projection_count() == 1

    def test_register_multiple_projections(self, registry: ProjectionRegistry) -> None:
        registry.register_projection(_RecordingProjection())
        registry.register_projection(_RecordingProjection())
        assert registry.get_projection_count() == 2

    def test_register_handler(self, registry: ProjectionRegistry) -> None:
        handler = _SelectiveHandler()
        registry.register_handler(handler)
        assert handler in registry.handlers
        assert registry.get_handler_count() == 1

    def test_unregister_projection(self, registry: ProjectionRegistry) -> None:
        projection = _RecordingProjection()
        registry.register_projection(projection)
        assert registry.get_projection_count() == 1
        assert registry.unregister_projection(projection) is True
        assert registry.get_projection_count() == 0

    def test_unregister_nonexistent_projection(self, registry: ProjectionRegistry) -> None:
        assert registry.unregister_projection(_RecordingProjection()) is False

    def test_unregister_dispatches_projection_and_handler(
        self, registry: ProjectionRegistry
    ) -> None:
        proj = _RecordingProjection()
        handler = _SelectiveHandler()
        registry.register(proj, handler)
        assert registry.unregister(proj) is True
        assert registry.unregister(proj) is False
        assert registry.unregister(handler) is True
        assert registry.unregister(handler) is False
        with pytest.raises(TypeError, match="Expected Projection or EventHandlerBase"):
            registry.unregister("not-a-projection")  # type: ignore[arg-type]

    def test_clear_and_container_dunders(self, registry: ProjectionRegistry) -> None:
        assert bool(registry) is True
        assert len(registry) == 0
        proj = _RecordingProjection()
        handler = _SelectiveHandler()
        assert proj not in registry
        assert handler not in registry
        registry.register(proj, handler)
        assert len(registry) == 2
        assert proj in registry
        assert handler in registry
        registry.clear()
        assert len(registry) == 0
        assert bool(registry) is True
        assert proj not in registry
        assert handler not in registry

    @pytest.mark.asyncio
    async def test_dispatch_to_all_projections(self, registry: ProjectionRegistry) -> None:
        p1, p2 = _RecordingProjection(), _RecordingProjection()
        registry.register_projection(p1)
        registry.register_projection(p2)
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(p1.events) == 1
        assert len(p2.events) == 1

    @pytest.mark.asyncio
    async def test_dispatch_to_handlers(self, registry: ProjectionRegistry) -> None:
        handler = _SelectiveHandler()
        registry.register_handler(handler)
        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await registry.dispatch(created)
        await registry.dispatch(shipped)
        assert handler.events == [created]

    @pytest.mark.asyncio
    async def test_dispatch_many(self, registry: ProjectionRegistry) -> None:
        proj = _RecordingProjection()
        registry.register_projection(proj)
        event1 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        event2 = OrderCreated(aggregate_id=uuid4(), order_number="ORD-002")
        event3 = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await registry.dispatch_many((event1, event2, event3))
        assert proj.events == [event1, event2, event3]

    @pytest.mark.asyncio
    async def test_dispatch_handles_errors_gracefully(
        self, registry: ProjectionRegistry, caplog: pytest.LogCaptureFixture
    ) -> None:
        class FailingProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                raise ValueError("Projection failed")

            async def reset(self) -> None:
                pass

        good = _RecordingProjection()
        registry.register_projection(FailingProjection())
        registry.register_projection(good)
        await registry.dispatch(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(good.events) == 1
        assert any("Error in projection" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_reset_all(self, registry: ProjectionRegistry) -> None:
        p1, p2 = _RecordingProjection(), _RecordingProjection()
        registry.register_projection(p1)
        registry.register_projection(p2)
        await registry.reset_all()
        assert p1.reset_called is True
        assert p2.reset_called is True

    @pytest.mark.asyncio
    async def test_coordinator_register_dunders_and_sequence_events(self) -> None:
        coordinator = ProjectionCoordinator()
        assert bool(coordinator) is True
        assert len(coordinator) == 0
        proj = _RecordingProjection()
        handler = _SelectiveHandler()
        assert coordinator.register(proj, handler) is coordinator
        assert len(coordinator) == 2
        ev = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        assert await coordinator.dispatch_events((ev,)) == 1
        assert await coordinator.rebuild_all((ev,)) == 1
        assert await coordinator.rebuild_projection(proj, (ev,)) == 1
        assert await coordinator.catchup(proj, (ev,)) == 1
        assert len(proj.events) == 2
