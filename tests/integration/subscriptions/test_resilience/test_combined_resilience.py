"""Integration tests combining multiple resilience features."""

from __future__ import annotations

import asyncio
import contextlib
import random
from datetime import UTC, datetime

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager
from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    RetryConfig,
    retry_async,
)
from eventsource.domain.event import DomainEvent
from tests.integration.subscriptions.conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    populate_event_store,
)

from .helpers import ChaosProjection, PositionalFailingProjection

pytestmark = [pytest.mark.asyncio, pytest.mark.slow]


class TestCombinedResilience:
    async def test_retry_with_circuit_breaker(self):
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=0.1)
        )
        call_count = failures = 0

        async def flaky_with_breaker():
            nonlocal call_count, failures
            call_count += 1
            if failures < 2:
                failures += 1
                raise ConnectionError("Flaky")
            return "success"

        config = RetryConfig(max_retries=5, initial_delay=0.01, max_delay=0.1)

        async def wrapped():
            return await breaker.execute(flaky_with_breaker)

        result = await retry_async(
            wrapped,
            config=config,
            retryable_exceptions=(ConnectionError, CircuitBreakerOpenError),
        )
        assert result == "success" and breaker.is_closed

    async def test_slow_failing_subscriber_still_drains(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 100)

        class SlowChaosProjection:
            def __init__(self):
                self.events: list[DomainEvent] = []
                self.event_count = self.failures = self.concurrent = self.max_concurrent = 0
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
        config = SubscriptionConfig(start_from="beginning", continue_on_error=True)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await asyncio.sleep(5.0)
        await subscription_manager.stop()
        assert projection.event_count >= 80 and projection.max_concurrent <= 10

    async def test_graceful_degradation(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        await populate_event_store(in_memory_event_store, 200)
        fast_projection = CollectingProjection(name="Fast")
        slow_chaos = ChaosProjection(failure_rate=0.1)
        fast_config = SubscriptionConfig(start_from="beginning")
        slow_config = SubscriptionConfig(start_from="beginning", continue_on_error=True)
        await subscription_manager.subscribe(fast_projection, fast_config, name="FastProjection")
        await subscription_manager.subscribe(slow_chaos, slow_config, name="SlowChaosProjection")
        await subscription_manager.start()
        success = await fast_projection.wait_for_events(200, timeout=30.0)
        await subscription_manager.stop()
        assert success, f"Fast projection got {fast_projection.event_count} events"
        assert fast_projection.event_count == 200 and slow_chaos.event_count >= 150

    async def test_cascading_recovery(self):
        breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=3, recovery_timeout=0.2)
        )
        failure_phase = True
        call_count = 0

        async def cascading_service():
            nonlocal call_count
            call_count += 1
            if failure_phase:
                raise ConnectionError("Service unavailable")
            return "recovered"

        for _ in range(3):
            with contextlib.suppress(ConnectionError, CircuitBreakerOpenError):
                await breaker.execute(cascading_service)
        assert breaker.is_open
        failure_phase = False
        await asyncio.sleep(0.25)
        result = await breaker.execute(cascading_service)
        assert result == "recovered" and breaker.is_closed

    async def test_multiple_subscribers_with_different_error_tolerances(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store(in_memory_event_store, 50)
        strict_projection = PositionalFailingProjection(fail_at_positions={25})
        strict_config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        strict_manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await strict_manager.subscribe(strict_projection, strict_config, name="StrictProjection")
        with contextlib.suppress(Exception):
            await strict_manager.start()
        assert strict_projection.event_count == 25
        await strict_manager.stop()

        tolerant_checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        tolerant_projection = PositionalFailingProjection(fail_at_positions={10, 20, 30})
        tolerant_config = SubscriptionConfig(start_from="beginning", continue_on_error=True)
        tolerant_manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=tolerant_checkpoint_repo,
        )
        await tolerant_manager.subscribe(
            tolerant_projection, tolerant_config, name="TolerantProjection"
        )
        await tolerant_manager.start()
        await asyncio.sleep(1.0)
        await tolerant_manager.stop()
        assert (tolerant_projection.event_count, len(tolerant_projection.failures)) == (47, 3)
