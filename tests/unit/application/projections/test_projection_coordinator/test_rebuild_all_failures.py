"""Blackbox tests: ProjectionCoordinator.rebuild_all reports projection failures."""

from uuid import uuid4

import pytest

from eventsource.application.projections.base import Projection
from eventsource.application.projections.coordinator import ProjectionCoordinator
from eventsource.application.projections.coordinator_projection import ProjectionRebuildError
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import EventSourceError
from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
)


def _orders(*numbers: str) -> list[DomainEvent]:
    return [OrderCreated(aggregate_id=uuid4(), order_number=n) for n in numbers]


class _AlwaysFailing(Projection):
    async def handle(self, event: DomainEvent) -> None:
        raise RuntimeError("read model write rejected")

    async def reset(self) -> None:
        return None


class _Recording(Projection):
    def __init__(self) -> None:
        self.seen: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.seen.append(event)

    async def reset(self) -> None:
        self.seen.clear()


class _FailsOnSecond(Projection):
    def __init__(self) -> None:
        self.calls = 0

    async def handle(self, event: DomainEvent) -> None:
        self.calls += 1
        if self.calls == 2:
            raise ValueError("second event rejected")

    async def reset(self) -> None:
        self.calls = 0


@pytest.mark.asyncio
async def test_rebuild_all_raises_when_projection_always_fails() -> None:
    coordinator = ProjectionCoordinator([_AlwaysFailing()])

    with pytest.raises(ProjectionRebuildError) as exc_info:
        await coordinator.rebuild_all(_orders("ORD-1", "ORD-2", "ORD-3"))

    assert exc_info.value.failure_count == 3
    assert isinstance(exc_info.value, EventSourceError)
    cause = exc_info.value.__cause__
    assert isinstance(cause, RuntimeError)
    assert "read model write rejected" in str(cause)


@pytest.mark.asyncio
async def test_rebuild_all_returns_count_when_projections_succeed() -> None:
    projection = _Recording()
    coordinator = ProjectionCoordinator([projection])

    count = await coordinator.rebuild_all(_orders("ORD-1", "ORD-2", "ORD-3"))

    assert count == 3
    assert len(projection.seen) == 3


@pytest.mark.asyncio
async def test_rebuild_all_dispatches_every_event_before_raising() -> None:
    healthy = _Recording()
    coordinator = ProjectionCoordinator([_FailsOnSecond(), healthy])

    with pytest.raises(ProjectionRebuildError) as exc_info:
        await coordinator.rebuild_all(_orders("ORD-1", "ORD-2", "ORD-3"))

    assert exc_info.value.failure_count == 1
    assert len(healthy.seen) == 3
    assert isinstance(exc_info.value.__cause__, ValueError)


@pytest.mark.asyncio
async def test_dispatch_events_still_does_not_raise_on_projection_failure() -> None:
    coordinator = ProjectionCoordinator([_AlwaysFailing()])

    assert await coordinator.dispatch_events(_orders("ORD-1", "ORD-2", "ORD-3")) == 3
