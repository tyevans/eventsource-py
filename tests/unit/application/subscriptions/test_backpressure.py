"""Unit tests for FlowController's in-flight tracking and drain latch."""

import asyncio

import pytest

from eventsource.application.subscriptions import (
    FlowControlContext,
    FlowController,
    FlowControlStats,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.delay: float = 0.0

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [DomainEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        self.handled_events.append(event)


@pytest.fixture
def mock_subscriber() -> MockSubscriber:
    return MockSubscriber()


@pytest.fixture
def flow_controller() -> FlowController:
    return FlowController()


class TestFlowControlStats:
    """Tests for FlowControlStats dataclass."""

    def test_default_values(self):
        stats = FlowControlStats()
        assert stats.events_in_flight == 0
        assert stats.total_acquisitions == 0
        assert stats.total_releases == 0

    def test_to_dict(self):
        stats = FlowControlStats(events_in_flight=5, total_acquisitions=100, total_releases=95)
        result = stats.to_dict()
        assert result["events_in_flight"] == 5
        assert result["total_acquisitions"] == 100
        assert result["total_releases"] == 95


class TestFlowControllerAcquireRelease:
    """Tests for acquire and release operations."""

    async def test_acquire_returns_context(self, flow_controller: FlowController):
        context = await flow_controller.acquire()
        assert isinstance(context, FlowControlContext)
        await flow_controller.release()

    async def test_acquire_increments_in_flight(self, flow_controller: FlowController):
        assert flow_controller.in_flight == 0
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()

    async def test_release_decrements_in_flight(self, flow_controller: FlowController):
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()
        assert flow_controller.in_flight == 0

    async def test_context_manager_releases_on_exit(self, flow_controller: FlowController):
        async with await flow_controller.acquire():
            assert flow_controller.in_flight == 1
        assert flow_controller.in_flight == 0

    async def test_context_manager_releases_on_exception(self, flow_controller: FlowController):
        with pytest.raises(ValueError):
            async with await flow_controller.acquire():
                raise ValueError("Test error")
        assert flow_controller.in_flight == 0

    async def test_multiple_acquires(self, flow_controller: FlowController):
        contexts = [await flow_controller.acquire() for _ in range(5)]
        assert flow_controller.in_flight == 5
        for _context in contexts:
            await flow_controller.release()
        assert flow_controller.in_flight == 0


class TestFlowControllerStatistics:
    """Tests for FlowController statistics tracking."""

    async def test_total_acquisitions_tracked(self, flow_controller: FlowController):
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()
        assert flow_controller.stats.total_acquisitions == 5

    async def test_total_releases_tracked(self, flow_controller: FlowController):
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()
        assert flow_controller.stats.total_releases == 5

    async def test_reset_stats(self, flow_controller: FlowController):
        for _ in range(5):
            await flow_controller.acquire()
        await flow_controller.release()
        flow_controller.reset_stats()
        assert flow_controller.stats.total_acquisitions == 0
        assert flow_controller.stats.total_releases == 0
        assert flow_controller.stats.events_in_flight == 4

    async def test_stats_snapshot(self, flow_controller: FlowController):
        await flow_controller.acquire()
        stats1 = flow_controller.stats
        await flow_controller.acquire()
        stats2 = flow_controller.stats
        assert stats1.events_in_flight == 1
        assert stats2.events_in_flight == 2


class TestConcurrentProcessing:
    """Tests for concurrent acquire/release under load."""

    async def test_no_deadlocks_under_load(self):
        controller = FlowController()
        processed = 0
        lock = asyncio.Lock()

        async def worker():
            nonlocal processed
            async with await controller.acquire():
                await asyncio.sleep(0.001)
                async with lock:
                    processed += 1

        await asyncio.wait_for(asyncio.gather(*[worker() for _ in range(50)]), timeout=5.0)
        assert processed == 50


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_flow_control_module(self):
        from eventsource.application.subscriptions.flow_control import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None

    def test_import_from_subscriptions_package(self):
        from eventsource.application.subscriptions import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None


class TestFlowControlContext:
    """Tests for FlowControlContext class."""

    async def test_context_enter_returns_self(self, flow_controller: FlowController):
        context = await flow_controller.acquire()
        async with context as entered:
            assert entered is context

    async def test_context_exit_releases(self, flow_controller: FlowController):
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await context.__aexit__(None, None, None)
        assert flow_controller.in_flight == 0

    async def test_context_exit_with_exception(self, flow_controller: FlowController):
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await context.__aexit__(ValueError, ValueError("test"), None)
        assert flow_controller.in_flight == 0
