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
