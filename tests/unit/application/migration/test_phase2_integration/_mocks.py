"""Mock objects and store wrappers for phase 2 migration integration tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from eventsource.adapters.memory import InMemoryEventStore


@dataclass
class MockLockInfo:
    """Mock lock info for testing."""

    key: str
    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class MockLockManager:
    """
    Mock implementation of PostgreSQLLockManager for testing.

    Can be configured to succeed or fail lock acquisition.
    """

    def __init__(
        self,
        *,
        should_fail: bool = False,
        fail_after: int | None = None,
    ) -> None:
        self._should_fail = should_fail
        self._fail_after = fail_after
        self._acquire_count = 0
        self._held_locks: dict[str, MockLockInfo] = {}

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: float | None = None,
    ) -> AsyncIterator[MockLockInfo]:
        """Acquire advisory lock."""
        self._acquire_count += 1

        if self._should_fail:
            from eventsource.ports.exceptions import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")

        if self._fail_after is not None and self._acquire_count > self._fail_after:
            from eventsource.ports.exceptions import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure after threshold")

        lock_info = MockLockInfo(key=key)
        self._held_locks[key] = lock_info
        try:
            yield lock_info
        finally:
            self._held_locks.pop(key, None)

    async def try_acquire(self, key: str) -> MockLockInfo | None:
        """Try to acquire lock without blocking."""
        if self._should_fail:
            return None

        if key in self._held_locks:
            return None

        return MockLockInfo(key=key)

    async def release(self, key: str) -> bool:
        """Release advisory lock."""
        if key in self._held_locks:
            del self._held_locks[key]
            return True
        return False


class FlakyTarget:
    """`FullEventStore` wrapper that rejects the first `reject_first` appends.

    Composition, not inheritance -- it stands in wherever a
    `FullEventStore` is expected and lets the wrapped store see only the
    appends it does not reject.
    """

    def __init__(self, inner: InMemoryEventStore, *, reject_first: int = 0) -> None:
        self._inner = inner
        self._reject_first = reject_first
        self.append_calls = 0

    async def append(self, stream, events, expected):  # type: ignore[no-untyped-def]
        self.append_calls += 1
        if self.append_calls <= self._reject_first:
            raise RuntimeError("target unavailable")
        return await self._inner.append(stream, events, expected)

    def read_stream(self, stream, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_stream(stream, options)

    async def get_stream_version(self, stream):  # type: ignore[no-untyped-def]
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id):  # type: ignore[no-untyped-def]
        return await self._inner.event_exists(event_id)

    def read_all(self, from_position=None, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_all(from_position, options)

    async def current_position(self):  # type: ignore[no-untyped-def]
        return await self._inner.current_position()

    def read_category(self, category, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_category(category, options)


class GatedFeedStore:
    """`FullEventStore` wrapper whose second `read_all` call blocks mid-stream.

    The bulk copier's first `read_all` is the counting pass and its second
    is the copy pass; gating the second lets a test write live events while
    the copy pass is provably mid-flight, then release it. Every other
    operation passes straight through.
    """

    def __init__(self, inner: InMemoryEventStore, *, gate_after: int = 3) -> None:
        self._inner = inner
        self._gate_after = gate_after
        self._read_all_calls = 0
        self.mid_pass = asyncio.Event()
        self.resume_gate = asyncio.Event()

    async def append(self, stream, events, expected):  # type: ignore[no-untyped-def]
        return await self._inner.append(stream, events, expected)

    def read_stream(self, stream, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_stream(stream, options)

    async def get_stream_version(self, stream):  # type: ignore[no-untyped-def]
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id):  # type: ignore[no-untyped-def]
        return await self._inner.event_exists(event_id)

    def read_all(self, from_position=None, options=None):  # type: ignore[no-untyped-def]
        self._read_all_calls += 1
        if self._read_all_calls == 2:
            return self._gated_read(from_position, options)
        return self._inner.read_all(from_position, options)

    async def _gated_read(self, from_position, options):  # type: ignore[no-untyped-def]
        yielded = 0
        async for envelope in self._inner.read_all(from_position, options):
            yield envelope
            yielded += 1
            if yielded == self._gate_after:
                self.mid_pass.set()
                await self.resume_gate.wait()

    async def current_position(self):  # type: ignore[no-untyped-def]
        return await self._inner.current_position()

    def read_category(self, category, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_category(category, options)


__all__ = [
    "FlakyTarget",
    "GatedFeedStore",
    "MockLockInfo",
    "MockLockManager",
]
