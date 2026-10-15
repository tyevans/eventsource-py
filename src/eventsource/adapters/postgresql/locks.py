"""
PostgreSQL advisory lock utilities for distributed coordination.

Implements `eventsource.ports.locks.LockManager`. The cross-process exclusion,
release-on-connection-loss, and PostgreSQL-fairness guarantees documented
below are this adapter's, not the port's -- see ADR 0029.

Advisory locks are application-level locks that:
- Are independent of table/row locks
- Persist for session duration (until released or disconnected)
- Support non-blocking acquisition attempts
- Are automatically released on connection close

Usage:
    >>> lock_manager = PostgreSQLLockManager(session_factory)
    >>> async with lock_manager.acquire("migration:tenant-abc"):
    ...     await perform_cutover()
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.adapters.postgresql.locks_helpers import (
    acquire_advisory_lock_session,
    key_to_lock_id,
    release_advisory_lock_session,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_LOCK_ACQUIRED,
    ATTR_LOCK_ID,
    ATTR_LOCK_KEY,
    ATTR_LOCK_TIMEOUT,
)
from eventsource.ports.exceptions import LockNotHeldError
from eventsource.ports.locks import LockInfo

logger = logging.getLogger(__name__)


class PostgreSQLLockManager:
    """
    Manages PostgreSQL advisory locks for distributed coordination.

    Maintains a dedicated database session per held lock because PostgreSQL
    advisory locks are session-level and persist until released or disconnected.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        holder_id: str | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the lock manager with a SQLAlchemy async session factory."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._session_factory = session_factory
        self._holder_id = holder_id
        self._held_locks: dict[str, tuple[AsyncSession, int]] = {}
        self._lock = asyncio.Lock()

    _key_to_lock_id = staticmethod(key_to_lock_id)

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AsyncIterator[LockInfo]:
        """Acquire an advisory lock as a context manager, releasing on exit."""
        lock_id = self._key_to_lock_id(key)

        with self._tracer.span(
            "eventsource.lock.acquire",
            {
                ATTR_LOCK_KEY: key,
                ATTR_LOCK_ID: lock_id,
                ATTR_LOCK_TIMEOUT: timeout if timeout is not None else -1,
            },
        ) as span:
            try:
                session = await self._acquire_lock(key, lock_id, timeout, retry_interval)
            except BaseException:
                if span is not None:
                    span.set_attribute(ATTR_LOCK_ACQUIRED, False)
                raise

            if span is not None:
                span.set_attribute(ATTR_LOCK_ACQUIRED, True)

            lock_info = LockInfo(
                key=key,
                lock_id=lock_id,
                acquired_at=datetime.now(UTC),
                holder_id=self._holder_id,
            )

            try:
                async with self._lock:
                    self._held_locks[key] = (session, lock_id)
                logger.debug("Acquired advisory lock: key=%s, lock_id=%d", key, lock_id)
                yield lock_info
            finally:
                await self._release_lock(key, session, lock_id)

    async def _acquire_lock(
        self,
        key: str,
        lock_id: int,
        timeout: float | None,
        retry_interval: float,
    ) -> AsyncSession:
        """Internal method to acquire the lock."""
        return await acquire_advisory_lock_session(
            self._session_factory, key, lock_id, timeout, retry_interval
        )

    async def _release_lock(
        self,
        key: str,
        session: AsyncSession,
        lock_id: int,
    ) -> None:
        """Internal method to release the lock."""
        with self._tracer.span(
            "eventsource.lock.release",
            {ATTR_LOCK_KEY: key, ATTR_LOCK_ID: lock_id},
        ):
            try:
                await release_advisory_lock_session(key, session, lock_id)
            finally:
                async with self._lock:
                    self._held_locks.pop(key, None)
                await session.close()

    async def try_acquire(self, key: str) -> LockInfo | None:
        """Try to acquire a lock without blocking; returns LockInfo or None."""
        lock_id = self._key_to_lock_id(key)
        session = self._session_factory()

        try:
            result = await session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
            if not result.scalar():
                await session.close()
                return None

            async with self._lock:
                self._held_locks[key] = (session, lock_id)

            logger.debug("Acquired advisory lock (try): key=%s, lock_id=%d", key, lock_id)
            return LockInfo(
                key=key,
                lock_id=lock_id,
                acquired_at=datetime.now(UTC),
                holder_id=self._holder_id,
            )
        except Exception as e:
            await session.close()
            logger.error("Error attempting to acquire lock: key=%s, error=%s", key, e)
            raise

    async def release(self, key: str) -> None:
        """Release a previously acquired lock, or raise LockNotHeldError."""
        async with self._lock:
            if key not in self._held_locks:
                raise LockNotHeldError(key)
            session, lock_id = self._held_locks[key]

        await self._release_lock(key, session, lock_id)

    async def is_held(self, key: str) -> bool:
        """Check if a lock is currently held by this manager."""
        async with self._lock:
            return key in self._held_locks

    async def release_all(self) -> int:
        """Release all locks held by this manager and return the released count."""
        async with self._lock:
            keys = list(self._held_locks.keys())

        released = 0
        for key in keys:
            try:
                await self.release(key)
                released += 1
            except LockNotHeldError:
                pass
            except Exception as e:
                logger.warning("Error releasing lock during release_all: key=%s, error=%s", key, e)

        return released

    @property
    def held_lock_count(self) -> int:
        """Get the number of locks currently held by this manager."""
        return len(self._held_locks)


__all__ = ["PostgreSQLLockManager"]
