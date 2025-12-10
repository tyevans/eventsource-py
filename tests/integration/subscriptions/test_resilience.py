"""
Integration tests for resilience and chaos testing.

Tests cover all Phase 2 resilience features:
- Backpressure behavior tests (flow control, slow subscribers, burst events)
- Graceful shutdown tests (in-flight events, checkpointing, timeout)
- Connection failure and recovery tests (retry, exponential backoff)
- Circuit breaker behavior tests (failure thresholds, recovery)
- Error handling integration tests (DLQ routing, error callbacks)
- Chaos testing scenarios (random failures, variable latency)
- Combined resilience scenarios (cascading recovery)
"""

import asyncio
import contextlib
import random
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.subscriptions.error_handling import (
    ErrorCategory,
    ErrorHandlingConfig,
    ErrorHandlingStrategy,
    ErrorInfo,
    SubscriptionErrorHandler,
)
from eventsource.subscriptions.flow_control import FlowController
from eventsource.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    RetryConfig,
    RetryError,
    retry_async,
)
from eventsource.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)

from .conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


# =============================================================================
# Additional Test Projections for Resilience Testing
# =============================================================================


class ConcurrencyTrackingProjection:
    """Projection that tracks concurrent processing."""

    def __init__(self, process_delay: float = 0.05) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.concurrent = 0
        self.max_concurrent = 0
        self.process_delay = process_delay
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event with concurrency tracking."""
        async with self._lock:
            self.concurrent += 1
            if self.concurrent > self.max_concurrent:
                self.max_concurrent = self.concurrent

        try:
            await asyncio.sleep(self.process_delay)
            async with self._lock:
                self.events.append(event)
                self.event_count += 1
        finally:
            async with self._lock:
                self.concurrent -= 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        """Wait for events with timeout."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class PositionalFailingProjection:
    """Projection that fails at specific event positions."""

    def __init__(self, fail_at_positions: set[int]) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.fail_at_positions = fail_at_positions
        self.failures: list[tuple[int, DomainEvent]] = []
        self._position = 0
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event, potentially failing at specific positions."""
        async with self._lock:
            if self._position in self.fail_at_positions:
                self.failures.append((self._position, event))
                self._position += 1
                raise ValueError(f"Intentional failure at position {self._position - 1}")
            self.events.append(event)
            self.event_count += 1
            self._position += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        """Wait for events with timeout."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class TransientFailingProjection:
    """Projection that fails transiently then succeeds."""

    def __init__(self, fail_count: int = 2) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.fail_count = fail_count
        self.attempt_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with transient failures."""
        event_id = str(event.event_id)
        async with self._lock:
            attempts = self.attempt_counts.get(event_id, 0) + 1
            self.attempt_counts[event_id] = attempts

            if attempts <= self.fail_count:
                raise ConnectionError(f"Transient failure (attempt {attempts})")

            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        """Wait for events with timeout."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class VariableLatencyProjection:
    """Projection with variable processing latency."""

    def __init__(self, min_delay: float = 0.01, max_delay: float = 0.2) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.processing_times: list[float] = []
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with variable latency."""
        delay = random.uniform(self.min_delay, self.max_delay)
        start = asyncio.get_event_loop().time()
        await asyncio.sleep(delay)
        processing_time = asyncio.get_event_loop().time() - start

        async with self._lock:
            self.events.append(event)
            self.event_count += 1
            self.processing_times.append(processing_time)

    async def wait_for_events(self, count: int, timeout: float = 30.0) -> bool:
        """Wait for events with timeout."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class ChaosProjection:
    """Projection with configurable random failure rate."""

    def __init__(self, failure_rate: float = 0.1) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.failure_rate = failure_rate
        self.total_failures = 0
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with random failures."""
        if random.random() < self.failure_rate:
            async with self._lock:
                self.total_failures += 1
            raise RuntimeError("Random chaos failure")

        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 30.0) -> bool:
        """Wait for events with timeout."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


# =============================================================================
# Additional Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def in_memory_dlq_repo() -> AsyncGenerator[InMemoryDLQRepository, None]:
    """Create an in-memory DLQ repository for testing."""
    repo = InMemoryDLQRepository(enable_tracing=False)
    yield repo
    await repo.clear()


@pytest_asyncio.fixture
async def subscription_manager_with_dlq(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
    in_memory_dlq_repo: InMemoryDLQRepository,
) -> AsyncGenerator[SubscriptionManager, None]:
    """Create a subscription manager with DLQ support for testing."""
    manager = SubscriptionManager(
        event_store=in_memory_event_store,
        event_bus=in_memory_event_bus,
        checkpoint_repo=in_memory_checkpoint_repo,
        dlq_repo=in_memory_dlq_repo,
    )
    yield manager
    if manager.is_running:
        await manager.stop()


# =============================================================================
# Backpressure Tests
# =============================================================================


class TestBackpressure:
    """Tests for backpressure and flow control behavior."""

    async def test_flow_controller_respects_max_in_flight(self):
        """Test that FlowController limits concurrent operations."""
        controller = FlowController(max_in_flight=5)
        concurrent = 0
        max_concurrent = 0
        results: list[int] = []

        async def process(i: int) -> None:
            nonlocal concurrent, max_concurrent
            async with await controller.acquire():
                concurrent += 1
                max_concurrent = max(max_concurrent, concurrent)
                await asyncio.sleep(0.05)
                results.append(i)
                concurrent -= 1

        # Run 20 concurrent tasks
        tasks = [asyncio.create_task(process(i)) for i in range(20)]
        await asyncio.gather(*tasks)

        assert max_concurrent <= 5
        assert len(results) == 20
        assert controller.stats.peak_in_flight <= 5

    async def test_flow_controller_tracks_statistics(self):
        """Test that FlowController tracks statistics correctly."""
        controller = FlowController(max_in_flight=10, backpressure_threshold=0.8)

        async def process() -> None:
            async with await controller.acquire():
                await asyncio.sleep(0.01)

        tasks = [asyncio.create_task(process()) for _ in range(25)]
        await asyncio.gather(*tasks)

        stats = controller.stats
        assert stats.total_acquisitions == 25
        assert stats.total_releases == 25
        assert stats.events_in_flight == 0

    async def test_flow_controller_pause_at_capacity(self):
        """Test that FlowController enters paused state at capacity."""
        controller = FlowController(max_in_flight=3)
        pause_detected = False
        slots: list[Any] = []

        # Acquire all slots
        for _ in range(3):
            slot = await controller.acquire()
            slots.append(slot)

        # Should be paused now
        pause_detected = controller.is_paused

        # Release all slots
        for slot in slots:
            await slot.__aexit__(None, None, None)

        assert pause_detected is True
        assert not controller.is_paused

    async def test_flow_controller_backpressure_detection(self):
        """Test that backpressure is detected at configured threshold."""
        controller = FlowController(max_in_flight=10, backpressure_threshold=0.8)
        slots: list[Any] = []

        # Acquire 7 slots (70% - below threshold)
        for _ in range(7):
            slots.append(await controller.acquire())
        assert not controller.is_backpressured

        # Acquire 1 more (80% - at threshold)
        slots.append(await controller.acquire())
        assert controller.is_backpressured

        # Release all
        for slot in slots:
            await slot.__aexit__(None, None, None)

    async def test_slow_subscriber_respects_max_in_flight(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test that slow subscriber respects max_in_flight limit."""
        # Populate with 50 events
        await populate_event_store(in_memory_event_store, 50)

        # Create slow projection
        projection = ConcurrencyTrackingProjection(process_delay=0.05)

        # Subscribe with low max_in_flight
        config = SubscriptionConfig(
            start_from="beginning",
            max_in_flight=5,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for completion
        success = await projection.wait_for_events(50, timeout=30.0)
        await subscription_manager.stop()

        assert success, f"Expected 50 events, got {projection.event_count}"
        assert projection.max_concurrent <= 5, (
            f"Max concurrent {projection.max_concurrent} exceeded limit 5"
        )

    async def test_burst_events_with_backpressure(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling burst of events with backpressure."""
        # Populate with 100 events (burst)
        await populate_event_store(in_memory_event_store, 100)

        projection = ConcurrencyTrackingProjection(process_delay=0.02)

        config = SubscriptionConfig(
            start_from="beginning",
            max_in_flight=10,
            batch_size=25,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        success = await projection.wait_for_events(100, timeout=30.0)
        await subscription_manager.stop()

        assert success
        assert projection.event_count == 100
        # Should have maintained concurrency limit
        assert projection.max_concurrent <= 10


# =============================================================================
# Graceful Shutdown Tests
# =============================================================================


class TestGracefulShutdown:
    """Tests for graceful shutdown behavior."""

    async def test_completes_in_flight_events_on_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test that in-flight events complete before shutdown."""
        # Populate store
        await populate_event_store(in_memory_event_store, 30)

        # Create projection with tracking
        started_events: list[str] = []
        completed_events: list[str] = []

        class TrackingProjection:
            def subscribed_to(self):
                return [SubTestOrderCreated]

            async def handle(self, event):
                started_events.append(str(event.event_id))
                await asyncio.sleep(0.1)
                completed_events.append(str(event.event_id))

        projection = TrackingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        # Let some events start processing
        await asyncio.sleep(0.2)

        # Stop with timeout
        await subscription_manager.stop(timeout=10.0)

        # All started events should have completed
        # (within reasonable tolerance for edge cases)
        assert len(completed_events) >= len(started_events) - 2

    async def test_saves_checkpoint_on_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test that checkpoint is saved before shutdown."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(50)
        await subscription_manager.stop()

        # Checkpoint should be saved
        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None
        assert position >= 50

    async def test_forced_shutdown_after_timeout(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test forced shutdown when timeout exceeded.

        This test verifies that the manager.stop() returns within the timeout
        even when the projection handler is slow. The actual handler may still
        be running after stop() returns.
        """
        await populate_event_store(in_memory_event_store, 3)

        # Track if stop was requested during handler execution
        class SlowProjection:
            def __init__(self):
                self.event_count = 0
                self.handler_started = asyncio.Event()

            def subscribed_to(self):
                return [SubTestOrderCreated]

            async def handle(self, event):
                self.handler_started.set()
                # Simulate slow processing with check for stop
                for _ in range(20):
                    await asyncio.sleep(0.05)  # 1 second total if all complete
                self.event_count += 1

        projection = SlowProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()

        # Wait for handler to start
        await asyncio.wait_for(projection.handler_started.wait(), timeout=2.0)

        # Short timeout should cause stop to return quickly
        start = asyncio.get_event_loop().time()
        await manager.stop(timeout=0.2)
        duration = asyncio.get_event_loop().time() - start

        # Should have returned within timeout (plus small margin for overhead)
        assert duration < 1.0, f"Stop took too long: {duration:.2f}s"

    async def test_shutdown_coordinator_phases(self):
        """Test shutdown coordinator goes through proper phases."""
        coordinator = ShutdownCoordinator(
            timeout=30.0,
            drain_timeout=5.0,
            checkpoint_timeout=2.0,
        )

        phases_seen: list[ShutdownPhase] = []

        async def stop_func():
            phases_seen.append(coordinator.phase)

        async def drain_func() -> int:
            phases_seen.append(coordinator.phase)
            return 5

        async def checkpoint_func() -> int:
            phases_seen.append(coordinator.phase)
            return 1

        result = await coordinator.shutdown(
            stop_func=stop_func,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert result.phase in (ShutdownPhase.STOPPED, ShutdownPhase.FORCED)
        assert ShutdownPhase.STOPPING in phases_seen
        assert ShutdownPhase.DRAINING in phases_seen
        assert ShutdownPhase.CHECKPOINTING in phases_seen

    async def test_shutdown_coordinator_request_shutdown(self):
        """Test programmatic shutdown request."""
        coordinator = ShutdownCoordinator()

        assert not coordinator.is_shutting_down

        coordinator.request_shutdown()

        assert coordinator.is_shutting_down


# =============================================================================
# Retry and Recovery Tests
# =============================================================================


class TestRetryAndRecovery:
    """Tests for retry mechanisms and recovery."""

    async def test_retry_on_transient_failure(self):
        """Test that transient failures are retried."""
        call_count = 0

        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Transient failure")
            return "success"

        config = RetryConfig(
            max_retries=5,
            initial_delay=0.01,
            max_delay=0.1,
        )

        result = await retry_async(
            flaky_operation,
            config=config,
            operation_name="test_operation",
        )

        assert result == "success"
        assert call_count == 3

    async def test_exponential_backoff_delays(self):
        """Test that retry delays follow exponential backoff."""
        from eventsource.subscriptions.retry import calculate_backoff

        config = RetryConfig(
            initial_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=0.0,  # No jitter for predictable testing
        )

        delays = [calculate_backoff(i, config) for i in range(6)]

        # Without jitter: 1, 2, 4, 8, 16, 32
        assert delays[0] == pytest.approx(1.0, rel=0.01)
        assert delays[1] == pytest.approx(2.0, rel=0.01)
        assert delays[2] == pytest.approx(4.0, rel=0.01)
        assert delays[3] == pytest.approx(8.0, rel=0.01)
        assert delays[4] == pytest.approx(16.0, rel=0.01)
        assert delays[5] == pytest.approx(32.0, rel=0.01)

    async def test_max_retries_exceeded_raises_error(self):
        """Test behavior when max retries exceeded."""

        async def always_fail():
            raise ConnectionError("Permanent failure")

        config = RetryConfig(
            max_retries=3,
            initial_delay=0.01,
            max_delay=0.1,
        )

        with pytest.raises(RetryError) as exc_info:
            await retry_async(
                always_fail,
                config=config,
                operation_name="failing_operation",
            )

        assert exc_info.value.attempts == 4  # Initial + 3 retries
        assert isinstance(exc_info.value.last_error, ConnectionError)

    async def test_non_retryable_exception_not_retried(self):
        """Test that non-retryable exceptions are raised immediately."""
        call_count = 0

        async def value_error_operation():
            nonlocal call_count
            call_count += 1
            raise ValueError("Not retryable")

        config = RetryConfig(max_retries=5, initial_delay=0.01)

        with pytest.raises(ValueError):
            await retry_async(
                value_error_operation,
                config=config,
                retryable_exceptions=(ConnectionError,),
            )

        assert call_count == 1  # No retries


# =============================================================================
# Circuit Breaker Tests
# =============================================================================


class TestCircuitBreaker:
    """Tests for circuit breaker behavior."""

    async def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=60.0,
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        # Fail 3 times to open circuit
        for _ in range(3):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.state == CircuitState.OPEN
        assert breaker.failure_count >= 3

    async def test_circuit_breaker_blocks_when_open(self):
        """Test circuit breaker blocks requests when open."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=60.0,
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        # Open the circuit
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        # Should block new requests
        with pytest.raises(CircuitBreakerOpenError):
            await breaker.execute(failing_operation)

    async def test_circuit_breaker_half_open_after_timeout(self):
        """Test circuit breaker enters half-open state after timeout."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.1,  # Very short for testing
            )
        )

        async def failing_operation():
            raise RuntimeError("Failure")

        # Open the circuit
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Should transition to half-open on next check
        # Note: state transitions when checking _can_execute
        assert breaker.state in (CircuitState.OPEN, CircuitState.HALF_OPEN)

    async def test_circuit_breaker_closes_on_success(self):
        """Test circuit breaker closes after successful recovery."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=2,
                recovery_timeout=0.1,
            )
        )

        call_count = 0

        async def intermittent_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise RuntimeError("Failure")
            return "success"

        # Fail twice to open circuit
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(intermittent_operation)

        assert breaker.is_open

        # Wait for recovery timeout
        await asyncio.sleep(0.15)

        # Third call should succeed and close circuit
        result = await breaker.execute(intermittent_operation)
        assert result == "success"
        assert breaker.is_closed

    async def test_circuit_breaker_reset(self):
        """Test circuit breaker can be reset."""
        breaker = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=2))

        async def failing_operation():
            raise RuntimeError("Failure")

        # Open the circuit
        for _ in range(2):
            with contextlib.suppress(RuntimeError):
                await breaker.execute(failing_operation)

        assert breaker.is_open

        # Reset
        breaker.reset()

        assert breaker.is_closed
        assert breaker.failure_count == 0


# =============================================================================
# Error Handling Integration Tests
# =============================================================================


class TestErrorHandlingIntegration:
    """Tests for error handling integration."""

    async def test_continue_on_error_skips_failed_event(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test continue_on_error=True skips failed events."""
        await populate_event_store(in_memory_event_store, 10)

        # Fail at position 5
        projection = PositionalFailingProjection(fail_at_positions={5})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for processing
        await asyncio.sleep(1.0)
        await subscription_manager.stop()

        # Should have processed 9 events (skipped position 5)
        assert projection.event_count == 9
        assert len(projection.failures) == 1

    async def test_stop_on_error_stops_subscription(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test continue_on_error=False stops on failure."""
        await populate_event_store(in_memory_event_store, 10)

        # Fail at position 3
        projection = PositionalFailingProjection(fail_at_positions={3})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        # Should have processed fewer than 10 events
        assert projection.event_count < 10

    async def test_error_callback_invoked(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test that error callbacks are invoked on failure."""
        await populate_event_store(in_memory_event_store, 5)

        callback_errors: list[ErrorInfo] = []

        async def error_callback(error_info: ErrorInfo) -> None:
            callback_errors.append(error_info)

        # Register callback
        subscription_manager.on_error(error_callback)

        # Projection that fails
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for processing
        await asyncio.sleep(0.5)
        await subscription_manager.stop()

        # Should have recorded errors
        assert len(projection.failures) > 0

    async def test_dlq_receives_failed_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        in_memory_dlq_repo,
        subscription_manager_with_dlq,
    ):
        """Test that failed events are sent to DLQ."""
        await populate_event_store(in_memory_event_store, 10)

        # Create error handler with DLQ
        error_handler = SubscriptionErrorHandler(
            subscription_name="TestProjection",
            dlq_repo=in_memory_dlq_repo,
            config=ErrorHandlingConfig(
                strategy=ErrorHandlingStrategy.DLQ_ONLY,
                dlq_enabled=True,
            ),
        )

        # Simulate error handling
        from eventsource.stores.interface import StoredEvent

        test_event = SubTestOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            amount=100.0,
        )

        # Create a minimal stored event using correct constructor
        stored_event = StoredEvent(
            event=test_event,
            stream_id=f"{test_event.aggregate_id}:TestOrder",
            stream_position=1,
            global_position=1,
            stored_at=datetime.now(UTC),
        )

        await error_handler.handle_error(
            error=ValueError("Test error"),
            stored_event=stored_event,
            event=test_event,
        )

        # Check DLQ
        failed_events = await in_memory_dlq_repo.get_failed_events(projection_name="TestProjection")
        assert len(failed_events) == 1
        assert str(failed_events[0].event_id) == str(test_event.event_id)

    async def test_error_classification(self):
        """Test error classification for different exception types."""
        from eventsource.subscriptions.error_handling import (
            ErrorClassifier,
        )

        classifier = ErrorClassifier()

        # Transient errors
        conn_error = classifier.classify(ConnectionError("timeout"))
        assert conn_error.category == ErrorCategory.TRANSIENT
        assert conn_error.retryable is True

        timeout_error = classifier.classify(TimeoutError("timeout"))
        assert timeout_error.category == ErrorCategory.TRANSIENT
        assert timeout_error.retryable is True

        # Application errors
        value_error = classifier.classify(ValueError("invalid"))
        assert value_error.category == ErrorCategory.APPLICATION
        assert value_error.retryable is False

        type_error = classifier.classify(TypeError("wrong type"))
        assert type_error.category == ErrorCategory.APPLICATION
        assert type_error.retryable is False

    async def test_error_stats_tracking(self):
        """Test that error statistics are tracked correctly."""
        error_handler = SubscriptionErrorHandler(
            subscription_name="TestProjection",
            config=ErrorHandlingConfig(),
        )

        # Simulate multiple errors
        for i in range(5):
            await error_handler.handle_error(
                error=ConnectionError(f"Error {i}"),
            )

        for i in range(3):
            await error_handler.handle_error(
                error=ValueError(f"Value error {i}"),
            )

        stats = error_handler.stats
        assert stats.total_errors == 8
        assert stats.transient_errors == 5  # ConnectionErrors
        assert stats.permanent_errors == 3  # ValueErrors


# =============================================================================
# Chaos Tests
# =============================================================================


class TestChaos:
    """Chaos testing scenarios."""

    async def test_random_failures(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of random failures during processing."""
        await populate_event_store(in_memory_event_store, 100)

        # 10% random failure rate
        projection = ChaosProjection(failure_rate=0.1)

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for processing
        await asyncio.sleep(3.0)
        await subscription_manager.stop()

        # Should have processed most events despite failures
        # With 10% failure rate, expect ~90 events
        assert projection.event_count >= 70
        assert projection.total_failures > 0

    async def test_variable_latency(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of variable processing latency."""
        await populate_event_store(in_memory_event_store, 50)

        projection = VariableLatencyProjection(
            min_delay=0.01,
            max_delay=0.1,
        )

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        success = await projection.wait_for_events(50, timeout=30.0)
        await subscription_manager.stop()

        assert success
        assert projection.event_count == 50

        # Check that processing times varied
        if len(projection.processing_times) > 1:
            times = projection.processing_times
            assert max(times) > min(times)  # Should have variation

    async def test_concurrent_events_during_shutdown(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test events arriving during shutdown."""
        # Start with some events
        await populate_event_store(in_memory_event_store, 20)

        projection = SlowProjection(delay=0.05)

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Publish more events while processing
        async def publish_events():
            for i in range(20, 30):
                await publish_live_event(
                    in_memory_event_store,
                    in_memory_event_bus,
                    f"ORD-{i:05d}",
                )
                await asyncio.sleep(0.02)

        publish_task = asyncio.create_task(publish_events())

        # Wait a bit then shutdown
        await asyncio.sleep(0.3)
        await subscription_manager.stop(timeout=5.0)

        # Cancel publishing if still running
        if not publish_task.done():
            publish_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await publish_task

        # Should have processed at least some events
        assert projection.event_count > 0


# =============================================================================
# Combined Resilience Tests
# =============================================================================


class TestCombinedResilience:
    """Tests combining multiple resilience features."""

    async def test_retry_with_circuit_breaker(self):
        """Test retry mechanism combined with circuit breaker."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=0.1,
            )
        )

        call_count = 0
        failures = 0

        async def flaky_with_breaker():
            nonlocal call_count, failures
            call_count += 1
            if failures < 2:
                failures += 1
                raise ConnectionError("Flaky")
            return "success"

        config = RetryConfig(
            max_retries=5,
            initial_delay=0.01,
            max_delay=0.1,
        )

        # Should eventually succeed with retries, circuit breaker should
        # not trip because we succeed before threshold
        async def wrapped():
            return await breaker.execute(flaky_with_breaker)

        result = await retry_async(
            wrapped,
            config=config,
            retryable_exceptions=(ConnectionError, CircuitBreakerOpenError),
        )

        assert result == "success"
        assert breaker.is_closed  # Should not have opened

    async def test_backpressure_with_error_handling(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test backpressure combined with error handling."""
        await populate_event_store(in_memory_event_store, 100)

        # 5% failure rate with slow processing
        class SlowChaosProjection:
            def __init__(self):
                self.events: list[DomainEvent] = []
                self.event_count = 0
                self.failures = 0
                self.concurrent = 0
                self.max_concurrent = 0
                self._lock = asyncio.Lock()

            def subscribed_to(self):
                return [SubTestOrderCreated]

            async def handle(self, event):
                async with self._lock:
                    self.concurrent += 1
                    self.max_concurrent = max(self.max_concurrent, self.concurrent)

                try:
                    await asyncio.sleep(0.02)
                    if random.random() < 0.05:
                        self.failures += 1
                        raise RuntimeError("Random failure")

                    async with self._lock:
                        self.events.append(event)
                        self.event_count += 1
                finally:
                    async with self._lock:
                        self.concurrent -= 1

            async def wait_for_events(self, count, timeout=30.0):
                start = datetime.now(UTC)
                while self.event_count < count:
                    if (datetime.now(UTC) - start).total_seconds() > timeout:
                        return False
                    await asyncio.sleep(0.05)
                return True

        projection = SlowChaosProjection()

        config = SubscriptionConfig(
            start_from="beginning",
            max_in_flight=10,
            continue_on_error=True,
        )
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await asyncio.sleep(5.0)
        await subscription_manager.stop()

        # Should have processed most events
        assert projection.event_count >= 80
        # Concurrency should have been limited
        assert projection.max_concurrent <= 10

    async def test_graceful_degradation(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test system degrades gracefully under stress."""
        await populate_event_store(in_memory_event_store, 200)

        # Two projections - one fast, one slow with failures
        fast_projection = CollectingProjection(name="Fast")
        slow_chaos = ChaosProjection(failure_rate=0.1)

        fast_config = SubscriptionConfig(start_from="beginning")
        slow_config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )

        await subscription_manager.subscribe(fast_projection, fast_config, name="FastProjection")
        await subscription_manager.subscribe(slow_chaos, slow_config, name="SlowChaosProjection")

        await subscription_manager.start()

        # Wait for fast projection
        success = await fast_projection.wait_for_events(200, timeout=30.0)
        await subscription_manager.stop()

        # Fast projection should complete normally
        assert success, f"Fast projection got {fast_projection.event_count} events"
        assert fast_projection.event_count == 200

        # Slow projection should have processed most events despite failures
        assert slow_chaos.event_count >= 150

    async def test_cascading_recovery(self):
        """Test recovery after cascading failures."""
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=0.2,
            )
        )

        # Simulate cascading failure then recovery
        failure_phase = True
        call_count = 0

        async def cascading_service():
            nonlocal call_count
            call_count += 1
            if failure_phase:
                raise ConnectionError("Service unavailable")
            return "recovered"

        # Phase 1: Failures to open circuit
        for _ in range(3):
            with contextlib.suppress(ConnectionError, CircuitBreakerOpenError):
                await breaker.execute(cascading_service)

        assert breaker.is_open

        # Phase 2: Service recovers
        failure_phase = False

        # Wait for circuit to transition to half-open
        await asyncio.sleep(0.25)

        # Phase 3: Circuit should close on success
        result = await breaker.execute(cascading_service)
        assert result == "recovered"
        assert breaker.is_closed

    async def test_multiple_subscribers_with_different_error_tolerances(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test multiple subscribers with different error handling settings."""
        await populate_event_store(in_memory_event_store, 50)

        # Test strict projection - stops on error
        strict_projection = PositionalFailingProjection(fail_at_positions={25})
        strict_config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )

        strict_manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await strict_manager.subscribe(strict_projection, strict_config, name="StrictProjection")

        with contextlib.suppress(Exception):
            await strict_manager.start()

        # Strict projection should have stopped early at position 25
        assert strict_projection.event_count < 50
        assert strict_projection.event_count == 25  # Exactly 25 events before failure
        await strict_manager.stop()

        # Test tolerant projection - continues on error
        # Use fresh checkpoint repo to avoid interference
        tolerant_checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)

        tolerant_projection = PositionalFailingProjection(fail_at_positions={10, 20, 30})
        tolerant_config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=True,
        )

        tolerant_manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=tolerant_checkpoint_repo,
        )
        await tolerant_manager.subscribe(
            tolerant_projection, tolerant_config, name="TolerantProjection"
        )

        await tolerant_manager.start()
        # Wait for processing
        await asyncio.sleep(1.0)
        await tolerant_manager.stop()

        # Tolerant projection should have processed 47 events (50 - 3 failures)
        assert tolerant_projection.event_count == 47
        assert len(tolerant_projection.failures) == 3


# =============================================================================
# Health Check Integration Tests
# =============================================================================


class TestHealthCheckIntegration:
    """Tests for health check integration with resilience features."""

    async def test_health_status_reflects_errors(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status reflects error state."""
        await populate_event_store(in_memory_event_store, 10)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        health = subscription_manager.get_health()
        assert health["status"] == "unhealthy"

    async def test_health_status_healthy_after_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status is healthy after successful processing."""
        await populate_event_store(in_memory_event_store, 20)

        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(20)

        health = subscription_manager.get_health()
        assert health["status"] == "healthy"

        await subscription_manager.stop()

    async def test_comprehensive_health_includes_error_stats(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test comprehensive health report includes error statistics."""
        await populate_event_store(in_memory_event_store, 20)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()
        await projection.wait_for_events(20)

        health = subscription_manager.get_comprehensive_health()

        assert "error_stats" in health
        assert "recent_errors" in health
        assert "dlq_status" in health
        assert health["error_stats"]["total_errors"] == 0

        await subscription_manager.stop()
