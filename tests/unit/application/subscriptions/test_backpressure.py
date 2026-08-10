"""
Unit tests for FlowController's in-flight tracking and drain latch.

FlowController no longer limits concurrency -- both subscription runners
deliver events sequentially, so acquire() never blocks. These tests cover
what remains: slot accounting and the FlowControlContext protocol. Drain
behavior (`wait_for_drain`) is covered in test_drain.py.
"""

import asyncio

import pytest

from eventsource.application.subscriptions import (
    FlowControlContext,
    FlowController,
    FlowControlStats,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import EventSubscriber

# Test fixtures


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.delay: float = 0.0  # Optional processing delay

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [DomainEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        self.handled_events.append(event)


@pytest.fixture
def mock_subscriber() -> MockSubscriber:
    """Create a mock subscriber."""
    return MockSubscriber()


@pytest.fixture
def flow_controller() -> FlowController:
    """Create a flow controller for testing."""
    return FlowController()


# FlowControlStats Tests


class TestFlowControlStats:
    """Tests for FlowControlStats dataclass."""

    def test_default_values(self):
        """Test default values are initialized correctly."""
        stats = FlowControlStats()
        assert stats.events_in_flight == 0
        assert stats.total_acquisitions == 0
        assert stats.total_releases == 0

    def test_to_dict(self):
        """Test to_dict conversion."""
        stats = FlowControlStats(
            events_in_flight=5,
            total_acquisitions=100,
            total_releases=95,
        )
        result = stats.to_dict()

        assert result["events_in_flight"] == 5
        assert result["total_acquisitions"] == 100
        assert result["total_releases"] == 95


# FlowController Acquire/Release Tests


class TestFlowControllerAcquireRelease:
    """Tests for acquire and release operations."""

    async def test_acquire_returns_context(self, flow_controller: FlowController):
        """Test acquire returns FlowControlContext."""
        context = await flow_controller.acquire()
        assert isinstance(context, FlowControlContext)
        await flow_controller.release()

    async def test_acquire_increments_in_flight(self, flow_controller: FlowController):
        """Test acquire increments in_flight count."""
        assert flow_controller.in_flight == 0
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()

    async def test_release_decrements_in_flight(self, flow_controller: FlowController):
        """Test release decrements in_flight count."""
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()
        assert flow_controller.in_flight == 0

    async def test_context_manager_releases_on_exit(self, flow_controller: FlowController):
        """Test context manager releases slot on exit."""
        async with await flow_controller.acquire():
            assert flow_controller.in_flight == 1
        assert flow_controller.in_flight == 0

    async def test_context_manager_releases_on_exception(self, flow_controller: FlowController):
        """Test context manager releases slot on exception."""
        with pytest.raises(ValueError):
            async with await flow_controller.acquire():
                raise ValueError("Test error")
        assert flow_controller.in_flight == 0

    async def test_multiple_acquires(self, flow_controller: FlowController):
        """Test multiple concurrent acquires -- acquire() never blocks."""
        contexts = []
        for _ in range(5):
            contexts.append(await flow_controller.acquire())
        assert flow_controller.in_flight == 5

        for _context in contexts:
            await flow_controller.release()
        assert flow_controller.in_flight == 0


# FlowController Statistics Tests


class TestFlowControllerStatistics:
    """Tests for FlowController statistics tracking."""

    async def test_total_acquisitions_tracked(self, flow_controller: FlowController):
        """Test total_acquisitions is tracked."""
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()

        assert flow_controller.stats.total_acquisitions == 5

    async def test_total_releases_tracked(self, flow_controller: FlowController):
        """Test total_releases is tracked."""
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()

        assert flow_controller.stats.total_releases == 5

    async def test_reset_stats(self, flow_controller: FlowController):
        """Test reset_stats clears statistics."""
        for _ in range(5):
            await flow_controller.acquire()
        await flow_controller.release()

        flow_controller.reset_stats()

        assert flow_controller.stats.total_acquisitions == 0
        assert flow_controller.stats.total_releases == 0
        # in_flight should be preserved
        assert flow_controller.stats.events_in_flight == 4

    async def test_stats_snapshot(self, flow_controller: FlowController):
        """Test stats property returns current snapshot."""
        await flow_controller.acquire()
        stats1 = flow_controller.stats

        await flow_controller.acquire()
        stats2 = flow_controller.stats

        # Stats should reflect current state
        assert stats1.events_in_flight == 1
        assert stats2.events_in_flight == 2


# Concurrent Processing Tests


class TestConcurrentProcessing:
    """Tests for concurrent acquire/release under load."""

    async def test_no_deadlocks_under_load(self):
        """Test no deadlocks occur under concurrent load."""
        controller = FlowController()

        processed = 0
        lock = asyncio.Lock()

        async def worker():
            nonlocal processed
            async with await controller.acquire():
                await asyncio.sleep(0.001)
                async with lock:
                    processed += 1

        # Should complete without deadlock
        await asyncio.wait_for(
            asyncio.gather(*[worker() for _ in range(50)]),
            timeout=5.0,
        )

        assert processed == 50


# Module Import Tests


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_flow_control_module(self):
        """Test imports from flow_control module."""
        from eventsource.application.subscriptions.flow_control import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.application.subscriptions import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None


# FlowControlContext Tests


class TestFlowControlContext:
    """Tests for FlowControlContext class."""

    async def test_context_enter_returns_self(self, flow_controller: FlowController):
        """Test __aenter__ returns self."""
        context = await flow_controller.acquire()
        async with context as entered:
            assert entered is context

    async def test_context_exit_releases(self, flow_controller: FlowController):
        """Test __aexit__ releases slot."""
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1

        await context.__aexit__(None, None, None)
        assert flow_controller.in_flight == 0

    async def test_context_exit_with_exception(self, flow_controller: FlowController):
        """Test __aexit__ releases even with exception info."""
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1

        # Simulate exception exit
        await context.__aexit__(ValueError, ValueError("test"), None)
        assert flow_controller.in_flight == 0
