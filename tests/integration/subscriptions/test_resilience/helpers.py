"""Helper projections and utility functions for resilience tests."""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.domain.event import DomainEvent
from tests.integration.subscriptions.conftest import SubTestOrderCreated

if TYPE_CHECKING:
    from eventsource.ports.event_store import EventStore


async def position_after(store: EventStore, n: int):
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position


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
        async with self._lock:
            self.concurrent += 1
            self.max_concurrent = max(self.max_concurrent, self.concurrent)

        try:
            await asyncio.sleep(self.process_delay)
            async with self._lock:
                self.events.append(event)
                self.event_count += 1
        finally:
            async with self._lock:
                self.concurrent -= 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
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
        async with self._lock:
            if self._position in self.fail_at_positions:
                self.failures.append((self._position, event))
                self._position += 1
                raise ValueError(f"Intentional failure at position {self._position - 1}")
            self.events.append(event)
            self.event_count += 1
            self._position += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
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
        event_id = str(event.event_id)
        async with self._lock:
            attempts = self.attempt_counts.get(event_id, 0) + 1
            self.attempt_counts[event_id] = attempts
            if attempts <= self.fail_count:
                raise ConnectionError(f"Transient failure (attempt {attempts})")
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
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
        delay = random.uniform(self.min_delay, self.max_delay)
        start = asyncio.get_event_loop().time()
        await asyncio.sleep(delay)
        processing_time = asyncio.get_event_loop().time() - start
        async with self._lock:
            self.events.append(event)
            self.event_count += 1
            self.processing_times.append(processing_time)

    async def wait_for_events(self, count: int, timeout: float = 30.0) -> bool:
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
        if random.random() < self.failure_rate:
            async with self._lock:
                self.total_failures += 1
            raise RuntimeError("Random chaos failure")
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 30.0) -> bool:
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True
