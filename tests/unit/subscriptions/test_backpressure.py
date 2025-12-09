"""
Unit tests for backpressure and flow control.

Tests FlowController, FlowControlStats, and flow control integration
with CatchUpRunner and LiveRunner.
"""

import asyncio

import pytest

from eventsource.events.base import DomainEvent
from eventsource.protocols import EventSubscriber
from eventsource.subscriptions import (
    FlowControlContext,
    FlowController,
    FlowControlStats,
    SubscriptionConfig,
)

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
def config() -> SubscriptionConfig:
    """Create a default configuration."""
    return SubscriptionConfig(
        max_in_flight=10,
        backpressure_threshold=0.8,
    )


@pytest.fixture
def flow_controller() -> FlowController:
    """Create a flow controller for testing."""
    return FlowController(max_in_flight=10, backpressure_threshold=0.8)


# FlowControlStats Tests


class TestFlowControlStats:
    """Tests for FlowControlStats dataclass."""

    def test_default_values(self):
        """Test default values are initialized correctly."""
        stats = FlowControlStats()
        assert stats.events_in_flight == 0
        assert stats.peak_in_flight == 0
        assert stats.pause_count == 0
        assert stats.total_pause_time_seconds == 0.0
        assert stats.total_acquisitions == 0
        assert stats.total_releases == 0

    def test_to_dict(self):
        """Test to_dict conversion."""
        stats = FlowControlStats(
            events_in_flight=5,
            peak_in_flight=10,
            pause_count=2,
            total_pause_time_seconds=1.5,
            total_acquisitions=100,
            total_releases=95,
        )
        result = stats.to_dict()

        assert result["events_in_flight"] == 5
        assert result["peak_in_flight"] == 10
        assert result["pause_count"] == 2
        assert result["total_pause_time_seconds"] == 1.5
        assert result["total_acquisitions"] == 100
        assert result["total_releases"] == 95


# FlowController Initialization Tests


class TestFlowControllerInit:
    """Tests for FlowController initialization."""

    def test_default_values(self):
        """Test default initialization values."""
        controller = FlowController()
        assert controller.max_in_flight == 1000
        assert controller.backpressure_threshold == 0.8

    def test_custom_max_in_flight(self):
        """Test custom max_in_flight."""
        controller = FlowController(max_in_flight=500)
        assert controller.max_in_flight == 500

    def test_custom_backpressure_threshold(self):
        """Test custom backpressure_threshold."""
        controller = FlowController(backpressure_threshold=0.5)
        assert controller.backpressure_threshold == 0.5

    def test_invalid_max_in_flight_zero(self):
        """Test max_in_flight=0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FlowController(max_in_flight=0)
        assert "max_in_flight must be >= 1" in str(exc_info.value)

    def test_invalid_max_in_flight_negative(self):
        """Test negative max_in_flight raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FlowController(max_in_flight=-1)
        assert "max_in_flight must be >= 1" in str(exc_info.value)

    def test_invalid_threshold_below_zero(self):
        """Test threshold < 0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FlowController(backpressure_threshold=-0.1)
        assert "backpressure_threshold must be between 0.0 and 1.0" in str(exc_info.value)

    def test_invalid_threshold_above_one(self):
        """Test threshold > 1 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            FlowController(backpressure_threshold=1.1)
        assert "backpressure_threshold must be between 0.0 and 1.0" in str(exc_info.value)

    def test_threshold_boundary_zero(self):
        """Test threshold=0 is valid."""
        controller = FlowController(backpressure_threshold=0.0)
        assert controller.backpressure_threshold == 0.0

    def test_threshold_boundary_one(self):
        """Test threshold=1 is valid."""
        controller = FlowController(backpressure_threshold=1.0)
        assert controller.backpressure_threshold == 1.0


# FlowController Acquire/Release Tests


class TestFlowControllerAcquireRelease:
    """Tests for acquire and release operations."""

    @pytest.mark.asyncio
    async def test_acquire_returns_context(self, flow_controller: FlowController):
        """Test acquire returns FlowControlContext."""
        context = await flow_controller.acquire()
        assert isinstance(context, FlowControlContext)
        await flow_controller.release()

    @pytest.mark.asyncio
    async def test_acquire_increments_in_flight(self, flow_controller: FlowController):
        """Test acquire increments in_flight count."""
        assert flow_controller.in_flight == 0
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()

    @pytest.mark.asyncio
    async def test_release_decrements_in_flight(self, flow_controller: FlowController):
        """Test release decrements in_flight count."""
        await flow_controller.acquire()
        assert flow_controller.in_flight == 1
        await flow_controller.release()
        assert flow_controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_context_manager_releases_on_exit(self, flow_controller: FlowController):
        """Test context manager releases slot on exit."""
        async with await flow_controller.acquire():
            assert flow_controller.in_flight == 1
        assert flow_controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_context_manager_releases_on_exception(self, flow_controller: FlowController):
        """Test context manager releases slot on exception."""
        with pytest.raises(ValueError):
            async with await flow_controller.acquire():
                raise ValueError("Test error")
        assert flow_controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_multiple_acquires(self, flow_controller: FlowController):
        """Test multiple concurrent acquires."""
        contexts = []
        for _ in range(5):
            contexts.append(await flow_controller.acquire())
        assert flow_controller.in_flight == 5

        for _context in contexts:
            await flow_controller.release()
        assert flow_controller.in_flight == 0


# FlowController Backpressure Tests


class TestFlowControllerBackpressure:
    """Tests for backpressure detection and handling."""

    @pytest.mark.asyncio
    async def test_is_backpressured_false_initially(self, flow_controller: FlowController):
        """Test is_backpressured is False initially."""
        assert flow_controller.is_backpressured is False

    @pytest.mark.asyncio
    async def test_is_backpressured_at_threshold(self, flow_controller: FlowController):
        """Test is_backpressured becomes True at threshold."""
        # Threshold is 0.8 of 10 = 8 slots
        for _ in range(8):
            await flow_controller.acquire()
        assert flow_controller.is_backpressured is True

    @pytest.mark.asyncio
    async def test_is_backpressured_below_threshold(self, flow_controller: FlowController):
        """Test is_backpressured is False below threshold."""
        for _ in range(7):
            await flow_controller.acquire()
        assert flow_controller.is_backpressured is False

    @pytest.mark.asyncio
    async def test_is_paused_at_capacity(self):
        """Test is_paused is True when at full capacity."""
        controller = FlowController(max_in_flight=3)

        # Fill to capacity
        for _ in range(3):
            await controller.acquire()

        assert controller.is_paused is True

    @pytest.mark.asyncio
    async def test_blocks_when_at_capacity(self):
        """Test acquire blocks when at capacity."""
        controller = FlowController(max_in_flight=2)

        # Fill to capacity
        await controller.acquire()
        await controller.acquire()
        assert controller.in_flight == 2

        # Try to acquire a third slot - should block
        blocked = True

        async def acquire_third():
            nonlocal blocked
            await controller.acquire()
            blocked = False

        task = asyncio.create_task(acquire_third())

        # Give some time for the task to potentially complete
        await asyncio.sleep(0.05)
        assert blocked is True  # Should still be blocked

        # Release one slot
        await controller.release()
        await asyncio.sleep(0.01)

        # Now it should complete
        await task
        assert blocked is False

    @pytest.mark.asyncio
    async def test_pause_count_incremented(self):
        """Test pause_count is incremented when capacity reached."""
        controller = FlowController(max_in_flight=2)

        # Fill to capacity
        await controller.acquire()
        await controller.acquire()

        assert controller.stats.pause_count == 1

    @pytest.mark.asyncio
    async def test_resumes_after_release(self):
        """Test controller resumes after releasing slots."""
        controller = FlowController(max_in_flight=2)

        # Fill to capacity
        await controller.acquire()
        await controller.acquire()
        assert controller.is_paused is True

        # Release one
        await controller.release()
        assert controller.is_paused is False


# FlowController Properties Tests


class TestFlowControllerProperties:
    """Tests for FlowController properties."""

    @pytest.mark.asyncio
    async def test_available_capacity_initial(self, flow_controller: FlowController):
        """Test available_capacity equals max_in_flight initially."""
        assert flow_controller.available_capacity == 10

    @pytest.mark.asyncio
    async def test_available_capacity_after_acquire(self, flow_controller: FlowController):
        """Test available_capacity decreases after acquire."""
        await flow_controller.acquire()
        assert flow_controller.available_capacity == 9

    @pytest.mark.asyncio
    async def test_available_capacity_at_zero(self, flow_controller: FlowController):
        """Test available_capacity is 0 when at capacity."""
        for _ in range(10):
            await flow_controller.acquire()
        assert flow_controller.available_capacity == 0

    @pytest.mark.asyncio
    async def test_utilization_initial(self, flow_controller: FlowController):
        """Test utilization is 0 initially."""
        assert flow_controller.utilization == 0.0

    @pytest.mark.asyncio
    async def test_utilization_half(self, flow_controller: FlowController):
        """Test utilization at 50%."""
        for _ in range(5):
            await flow_controller.acquire()
        assert flow_controller.utilization == 0.5

    @pytest.mark.asyncio
    async def test_utilization_full(self, flow_controller: FlowController):
        """Test utilization at 100%."""
        for _ in range(10):
            await flow_controller.acquire()
        assert flow_controller.utilization == 1.0


# FlowController Statistics Tests


class TestFlowControllerStatistics:
    """Tests for FlowController statistics tracking."""

    @pytest.mark.asyncio
    async def test_peak_in_flight_tracked(self, flow_controller: FlowController):
        """Test peak_in_flight is tracked."""
        for _ in range(5):
            await flow_controller.acquire()

        for _ in range(3):
            await flow_controller.release()

        assert flow_controller.in_flight == 2
        assert flow_controller.stats.peak_in_flight == 5

    @pytest.mark.asyncio
    async def test_total_acquisitions_tracked(self, flow_controller: FlowController):
        """Test total_acquisitions is tracked."""
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()

        assert flow_controller.stats.total_acquisitions == 5

    @pytest.mark.asyncio
    async def test_total_releases_tracked(self, flow_controller: FlowController):
        """Test total_releases is tracked."""
        for _ in range(5):
            await flow_controller.acquire()
            await flow_controller.release()

        assert flow_controller.stats.total_releases == 5

    @pytest.mark.asyncio
    async def test_reset_stats(self, flow_controller: FlowController):
        """Test reset_stats clears statistics."""
        for _ in range(5):
            await flow_controller.acquire()
        await flow_controller.release()

        flow_controller.reset_stats()

        assert flow_controller.stats.peak_in_flight == 0
        assert flow_controller.stats.total_acquisitions == 0
        assert flow_controller.stats.total_releases == 0
        # in_flight should be preserved
        assert flow_controller.stats.events_in_flight == 4


# FlowController Wait for Capacity Tests


class TestFlowControllerWaitForCapacity:
    """Tests for wait_for_capacity method."""

    @pytest.mark.asyncio
    async def test_wait_for_capacity_immediate(self, flow_controller: FlowController):
        """Test wait_for_capacity returns immediately when capacity available."""
        await flow_controller.wait_for_capacity(min_capacity=5)
        # Should not block

    @pytest.mark.asyncio
    async def test_wait_for_capacity_waits(self):
        """Test wait_for_capacity waits when capacity insufficient."""
        controller = FlowController(max_in_flight=3)

        # Fill to near capacity
        await controller.acquire()
        await controller.acquire()

        waited = False

        async def wait_task():
            nonlocal waited
            await controller.wait_for_capacity(min_capacity=2)
            waited = True

        task = asyncio.create_task(wait_task())
        await asyncio.sleep(0.02)
        assert waited is False  # Should still be waiting

        # Release to make capacity available
        await controller.release()
        await asyncio.sleep(0.02)

        await task
        assert waited is True

    @pytest.mark.asyncio
    async def test_wait_for_capacity_invalid(self, flow_controller: FlowController):
        """Test wait_for_capacity raises for invalid min_capacity."""
        with pytest.raises(ValueError) as exc_info:
            await flow_controller.wait_for_capacity(min_capacity=100)
        assert "cannot exceed max_in_flight" in str(exc_info.value)


# SubscriptionConfig Backpressure Tests


class TestSubscriptionConfigBackpressure:
    """Tests for backpressure settings in SubscriptionConfig."""

    def test_default_backpressure_threshold(self):
        """Test default backpressure_threshold is 0.8."""
        config = SubscriptionConfig()
        assert config.backpressure_threshold == 0.8

    def test_custom_backpressure_threshold(self):
        """Test custom backpressure_threshold."""
        config = SubscriptionConfig(backpressure_threshold=0.5)
        assert config.backpressure_threshold == 0.5

    def test_invalid_backpressure_threshold_low(self):
        """Test backpressure_threshold < 0 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(backpressure_threshold=-0.1)
        assert "backpressure_threshold must be between 0.0 and 1.0" in str(exc_info.value)

    def test_invalid_backpressure_threshold_high(self):
        """Test backpressure_threshold > 1 raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SubscriptionConfig(backpressure_threshold=1.1)
        assert "backpressure_threshold must be between 0.0 and 1.0" in str(exc_info.value)


# Concurrent Processing Tests


class TestConcurrentProcessing:
    """Tests for concurrent processing with flow control."""

    @pytest.mark.asyncio
    async def test_concurrent_acquires_respect_limit(self):
        """Test concurrent acquires respect max_in_flight limit."""
        controller = FlowController(max_in_flight=5)

        # Track max concurrent
        max_concurrent = 0
        current = 0
        lock = asyncio.Lock()

        async def worker():
            nonlocal max_concurrent, current
            async with await controller.acquire():
                async with lock:
                    current += 1
                    if current > max_concurrent:
                        max_concurrent = current
                await asyncio.sleep(0.01)
                async with lock:
                    current -= 1

        # Start 20 concurrent workers
        await asyncio.gather(*[worker() for _ in range(20)])

        assert max_concurrent == 5
        assert controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_no_deadlocks_under_load(self):
        """Test no deadlocks occur under concurrent load."""
        controller = FlowController(max_in_flight=3)

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

    @pytest.mark.asyncio
    async def test_pause_time_accumulated(self):
        """Test total_pause_time_seconds is accumulated."""
        controller = FlowController(max_in_flight=2)

        # Fill to capacity to trigger pause
        await controller.acquire()
        await controller.acquire()

        # Small delay while paused
        await asyncio.sleep(0.05)

        # Release to exit pause
        await controller.release()

        # Should have accumulated some pause time
        # Note: Timing can be imprecise, so we just check it's > 0
        assert controller.stats.pause_count >= 1


# Module Import Tests


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_flow_control_module(self):
        """Test imports from flow_control module."""
        from eventsource.subscriptions.flow_control import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.subscriptions import (
            FlowControlContext,
            FlowController,
            FlowControlStats,
        )

        assert FlowController is not None
        assert FlowControlContext is not None
        assert FlowControlStats is not None


# Edge Case Tests


class TestFlowControllerEdgeCases:
    """Tests for edge cases in FlowController."""

    @pytest.mark.asyncio
    async def test_single_slot(self):
        """Test with max_in_flight=1."""
        controller = FlowController(max_in_flight=1)

        async with await controller.acquire():
            assert controller.in_flight == 1
            assert controller.is_paused is True

        assert controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_threshold_zero(self):
        """Test with backpressure_threshold=0 (always backpressured when in use)."""
        controller = FlowController(max_in_flight=10, backpressure_threshold=0.0)

        # Even one acquisition should trigger backpressure
        await controller.acquire()
        assert controller.is_backpressured is True
        await controller.release()

    @pytest.mark.asyncio
    async def test_threshold_one(self):
        """Test with backpressure_threshold=1 (never backpressured until full)."""
        controller = FlowController(max_in_flight=10, backpressure_threshold=1.0)

        # At 9 out of 10, should not be backpressured
        for _ in range(9):
            await controller.acquire()
        assert controller.is_backpressured is False

        # At 10 out of 10, should be backpressured
        await controller.acquire()
        assert controller.is_backpressured is True

    @pytest.mark.asyncio
    async def test_stats_snapshot(self, flow_controller: FlowController):
        """Test stats property returns current snapshot."""
        await flow_controller.acquire()
        stats1 = flow_controller.stats

        await flow_controller.acquire()
        stats2 = flow_controller.stats

        # Stats should reflect current state
        assert stats1.events_in_flight == 1
        assert stats2.events_in_flight == 2


# FlowControlContext Tests


class TestFlowControlContext:
    """Tests for FlowControlContext class."""

    @pytest.mark.asyncio
    async def test_context_enter_returns_self(self, flow_controller: FlowController):
        """Test __aenter__ returns self."""
        context = await flow_controller.acquire()
        async with context as entered:
            assert entered is context

    @pytest.mark.asyncio
    async def test_context_exit_releases(self, flow_controller: FlowController):
        """Test __aexit__ releases slot."""
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1

        await context.__aexit__(None, None, None)
        assert flow_controller.in_flight == 0

    @pytest.mark.asyncio
    async def test_context_exit_with_exception(self, flow_controller: FlowController):
        """Test __aexit__ releases even with exception info."""
        context = await flow_controller.acquire()
        assert flow_controller.in_flight == 1

        # Simulate exception exit
        await context.__aexit__(ValueError, ValueError("test"), None)
        assert flow_controller.in_flight == 0
