"""Failure injection infrastructure and events for migration chaos tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum, auto
from uuid import UUID

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    StreamReadOptions,
)

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class ChaosTestEvent(DomainEvent):
    """Test event for chaos testing."""

    aggregate_type: str = "ChaosAggregate"
    value: str = "test"
    sequence: int = 0


class FailureMode(Enum):
    """Types of failures that can be injected."""

    NONE = auto()
    FAIL_APPEND = auto()
    FAIL_READ = auto()
    FAIL_ALL = auto()
    DELAY_APPEND = auto()
    DELAY_READ = auto()
    INTERMITTENT = auto()
    FAIL_AFTER_N_OPS = auto()
    TIMEOUT = auto()


@dataclass
class FailureConfig:
    """Configuration for failure injection."""

    mode: FailureMode = FailureMode.NONE
    fail_rate: float = 0.0
    delay_seconds: float = 0.0
    fail_after_n: int = 0
    timeout_seconds: float = 5.0
    error_message: str = "Injected failure"
    operation_count: int = field(default=0, init=False)
    failure_count: int = field(default=0, init=False)


class InjectedFailureError(Exception):
    """Exception raised when a failure is injected."""

    def __init__(self, message: str, failure_mode: FailureMode):
        super().__init__(message)
        self.failure_mode = failure_mode


class FailureInjectableStore:
    """`FullEventStore`-shaped wrapper around `InMemoryEventStore` that can inject failures."""

    def __init__(self, inner_store: InMemoryEventStore | None = None) -> None:
        self._inner = inner_store or InMemoryEventStore(event_registry=_REGISTRY)
        self._failure_config = FailureConfig()
        self._is_available = True
        self._available_event = asyncio.Event()
        self._available_event.set()

    @property
    def inner_store(self) -> InMemoryEventStore:
        return self._inner

    def set_failure(self, config: FailureConfig) -> None:
        self._failure_config = config

    def clear_failure(self) -> None:
        self._failure_config = FailureConfig()

    def simulate_network_partition(self) -> None:
        self._is_available = False
        self._available_event.clear()

    def heal_network_partition(self) -> None:
        self._is_available = True
        self._available_event.set()

    async def wait_for_availability(self, timeout: float = 5.0) -> bool:
        try:
            await asyncio.wait_for(self._available_event.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    async def _maybe_fail(self, operation: str) -> None:
        config = self._failure_config
        config.operation_count += 1
        if not self._is_available:
            raise InjectedFailureError(
                f"Network partition: store unavailable for {operation}",
                FailureMode.FAIL_ALL,
            )
        if config.mode == FailureMode.NONE:
            return

        should_fail = False
        delay = 0.0
        if (
            config.mode == FailureMode.FAIL_ALL
            or config.mode == FailureMode.FAIL_APPEND
            and operation == "append"
            or config.mode == FailureMode.FAIL_READ
            and operation == "read"
        ):
            should_fail = True
        elif (
            config.mode == FailureMode.DELAY_APPEND
            and operation == "append"
            or config.mode == FailureMode.DELAY_READ
            and operation == "read"
        ):
            delay = config.delay_seconds
        elif config.mode == FailureMode.INTERMITTENT:
            import random

            should_fail = random.random() < config.fail_rate
        elif config.mode == FailureMode.FAIL_AFTER_N_OPS:
            should_fail = config.operation_count > config.fail_after_n
        elif config.mode == FailureMode.TIMEOUT:
            await asyncio.sleep(config.timeout_seconds)
            should_fail = True

        if delay > 0:
            await asyncio.sleep(delay)
        if should_fail:
            config.failure_count += 1
            raise InjectedFailureError(
                f"{config.error_message} (operation={operation}, count={config.failure_count})",
                config.mode,
            )

    async def append(
        self, stream: StreamId, events: list[DomainEvent], expected: ExpectedVersion
    ) -> AppendResult:
        await self._maybe_fail("append")
        return await self._inner.append(stream, events, expected)

    def read_stream(
        self, stream: StreamId, options: StreamReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_stream(stream, options)

    async def _do_read_stream(
        self, stream: StreamId, options: StreamReadOptions | None
    ) -> AsyncIterator[EventEnvelope]:
        await self._maybe_fail("read")
        async for envelope in self._inner.read_stream(stream, options):
            yield envelope

    async def get_stream_version(self, stream: StreamId) -> int:
        await self._maybe_fail("read")
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id: UUID) -> bool:
        await self._maybe_fail("read")
        return await self._inner.event_exists(event_id)

    def read_all(
        self, from_position: Position | None = None, options: FeedReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_all(from_position, options)

    async def _do_read_all(
        self, from_position: Position | None, options: FeedReadOptions | None
    ) -> AsyncIterator[EventEnvelope]:
        await self._maybe_fail("read")
        async for envelope in self._inner.read_all(from_position, options):
            yield envelope

    async def current_position(self) -> Position | None:
        await self._maybe_fail("read")
        return await self._inner.current_position()

    def read_category(
        self, category: str, options: CategoryReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        return self._do_read_category(category, options)

    async def _do_read_category(
        self, category: str, options: CategoryReadOptions | None
    ) -> AsyncIterator[EventEnvelope]:
        await self._maybe_fail("read")
        async for envelope in self._inner.read_category(category, options):
            yield envelope
