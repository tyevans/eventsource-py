"""Unit tests for ProjectionCoordinator lifecycle and management."""

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
)


def _orders(*numbers: str) -> list[DomainEvent]:
    return [OrderCreated(aggregate_id=uuid4(), order_number=n) for n in numbers]


class TestProjectionCoordinator:
    """Tests for ProjectionCoordinator."""

    @pytest.fixture
    def registry(self) -> ProjectionRegistry:
        return ProjectionRegistry()

    @pytest.fixture
    def coordinator(self, registry: ProjectionRegistry) -> ProjectionCoordinator:
        return ProjectionCoordinator(registry=registry)

    @pytest.mark.asyncio
    async def test_dispatch_events(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
        events: list[DomainEvent] = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                events.append(event)

            async def reset(self) -> None:
                events.clear()

        registry.register_projection(TestProjection())
        count = await coordinator.dispatch_events(_orders("ORD-001", "ORD-002"))
        assert count == 2 and len(events) == 2

    @pytest.mark.asyncio
    async def test_rebuild_all(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
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
        count = await coordinator.rebuild_all(_orders("ORD-001", "ORD-002"))
        assert count == 2 and reset_called is True and len(events) == 2

    @pytest.mark.asyncio
    async def test_rebuild_projection(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
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
        count = await coordinator.rebuild_projection(projection, _orders("ORD-001"))
        assert count == 1 and reset_called is True and len(events) == 1

    @pytest.mark.asyncio
    async def test_catchup(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
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
        count = await coordinator.catchup(projection, _orders("ORD-001"))
        assert count == 1 and reset_called is False and len(events) == 1

    def test_get_projection_info(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                pass

            async def reset(self) -> None:
                pass

        registry.register_projection(TestProjection())
        info = coordinator.get_projection_info()
        assert len(info) == 1 and info[0]["name"] == "TestProjection"

    @pytest.mark.asyncio
    async def test_health_check(
        self, registry: ProjectionRegistry, coordinator: ProjectionCoordinator
    ) -> None:
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
        assert health["projection_count"] == 1 and health["handler_count"] == 1
        assert "TestProjection" in health["projections"] and "TestHandler" in health["handlers"]

    @pytest.mark.asyncio
    async def test_registry_register_and_coordinator_sequence_init(self) -> None:
        seen: list[str] = []

        class TestProjection(Projection):
            async def handle(self, event: DomainEvent) -> None:
                seen.append("proj")

            async def reset(self) -> None:
                pass

        class TestHandler(EventHandlerBase):
            def can_handle(self, event: DomainEvent) -> bool:
                return True

            async def handle(self, event: DomainEvent) -> None:
                seen.append("handler")

        proj, handler = TestProjection(), TestHandler()
        reg = ProjectionRegistry(projections=[proj], handlers=[handler])
        assert reg.get_projection_count() == 1 and reg.get_handler_count() == 1

        with pytest.raises(TypeError, match="Expected Projection or EventHandlerBase"):
            reg.register("invalid")  # type: ignore[arg-type]

        coord = ProjectionCoordinator([proj, handler])
        count = await coord.dispatch_events(_orders("ORD-001"))
        assert count == 1 and sorted(seen) == ["handler", "proj"]
        assert ProjectionCoordinator().registry.get_projection_count() == 0
