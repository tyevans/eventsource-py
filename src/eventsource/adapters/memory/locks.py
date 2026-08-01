"""In-process lock manager implementing the DistributedLock port.

**This is a test double, not a distributed lock.** It excludes only
coroutines running in one asyncio event loop in one process. It offers no
cross-process or cross-connection exclusion, no release on crash (a killed
process takes its lock table with it), and no fairness or FIFO ordering
among waiters. Use it in unit tests that need a `DistributedLock` and
nothing more; use
`eventsource.adapters.postgresql.locks.PostgreSQLLockManager` anywhere two
processes must coordinate. See ADR 0029 and ADR 0023.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from eventsource.domain.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.ports.locks import LockInfo


class InMemoryLockManager:
    """Single-process `LockManager` backed by a dict and an `asyncio.Condition`."""

    def __init__(self, *, holder_id: str | None = None) -> None:
        self._holder_id = holder_id
        self._held: dict[str, LockInfo] = {}
        self._condition = asyncio.Condition()

    @staticmethod
    def _key_to_lock_id(key: str) -> int:
        """63-bit lock id for `key`, identical to the PostgreSQL adapter's."""
        hash_bytes = hashlib.sha256(key.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big") & 0x7FFFFFFFFFFFFFFF

    def _make_info(self, key: str) -> LockInfo:
        return LockInfo(
            key=key,
            lock_id=self._key_to_lock_id(key),
            acquired_at=datetime.now(UTC),
            holder_id=self._holder_id,
        )

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AsyncIterator[LockInfo]:
        """Acquire `key`, releasing it on context exit.

        `retry_interval` is accepted for port compatibility and ignored: this
        implementation waits on a condition variable rather than polling.
        """
        info = await self._acquire(key, timeout)
        try:
            yield info
        finally:
            await self._release(key)

    async def _acquire(self, key: str, timeout: float | None) -> LockInfo:
        async def wait_and_take() -> LockInfo:
            async with self._condition:
                await self._condition.wait_for(lambda: key not in self._held)
                info = self._make_info(key)
                self._held[key] = info
                return info

        if timeout is None:
            return await wait_and_take()
        try:
            return await asyncio.wait_for(wait_and_take(), timeout)
        except TimeoutError as exc:
            raise LockAcquisitionError(
                key=key,
                reason=f"Timeout after {timeout}s",
                timeout=timeout,
            ) from exc

    async def _release(self, key: str) -> None:
        async with self._condition:
            self._held.pop(key, None)
            self._condition.notify_all()

    async def try_acquire(self, key: str) -> LockInfo | None:
        async with self._condition:
            if key in self._held:
                return None
            info = self._make_info(key)
            self._held[key] = info
            return info

    async def release(self, key: str) -> None:
        async with self._condition:
            if key not in self._held:
                raise LockNotHeldError(key)
            del self._held[key]
            self._condition.notify_all()

    async def is_held(self, key: str) -> bool:
        async with self._condition:
            return key in self._held

    async def release_all(self) -> int:
        async with self._condition:
            released = len(self._held)
            self._held.clear()
            self._condition.notify_all()
            return released

    @property
    def held_lock_count(self) -> int:
        return len(self._held)


__all__ = ["InMemoryLockManager"]
