"""
PostgreSQL advisory lock utilities for distributed coordination.

Advisory locks are application-level locks that:
- Are independent of table/row locks
- Persist for session duration (until released or disconnected)
- Support non-blocking acquisition attempts
- Are automatically released on connection close

Usage:
    >>> lock_manager = PostgreSQLLockManager(session_factory)
    >>> async with lock_manager.acquire("migration:tenant-abc"):
    ...     # Critical section - only one holder at a time
    ...     await perform_cutover()
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LockInfo:
    """
    Information about an acquired lock.

    Attributes:
        key: The string key used to identify the lock
        lock_id: The numeric PostgreSQL lock ID (derived from key hash)
        acquired_at: When the lock was acquired
        holder_id: Optional identifier for the lock holder (for debugging)
    """

    key: str
    lock_id: int
    acquired_at: datetime
    holder_id: str | None = None


class LockAcquisitionError(Exception):
    """
    Raised when a lock cannot be acquired.

    Attributes:
        key: The lock key that could not be acquired
        reason: Description of why acquisition failed
        timeout: The timeout value if timeout was the cause
    """

    def __init__(
        self,
        key: str,
        reason: str,
        timeout: float | None = None,
    ):
        self.key = key
        self.reason = reason
        self.timeout = timeout
        super().__init__(f"Failed to acquire lock '{key}': {reason}")


class LockNotHeldError(Exception):
    """
    Raised when attempting to release a lock not held.

    Attributes:
        key: The lock key that was not held
    """

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Lock '{key}' is not held by this session")


class PostgreSQLLockManager:
    """
    Manages PostgreSQL advisory locks for distributed coordination.

    Advisory locks are useful for:
    - Coordinating cutover operations across multiple instances
    - Preventing concurrent migrations for the same tenant
    - Ensuring exclusive access to critical operations

    The lock manager maintains a separate database session for each held lock,
    which is necessary because PostgreSQL advisory locks are session-level:
    they persist until explicitly released or the session ends.

    Example:
        >>> lock_manager = PostgreSQLLockManager(session_factory)
        >>>
        >>> # Blocking acquisition
        >>> async with lock_manager.acquire("cutover:tenant-123"):
        ...     await perform_cutover()
        >>>
        >>> # Non-blocking with timeout
        >>> try:
        ...     async with lock_manager.acquire(
        ...         "cutover:tenant-123",
        ...         timeout=5.0,
        ...     ):
        ...         await perform_cutover()
        ... except LockAcquisitionError:
        ...     print("Another instance is performing cutover")

    Note:
        Each lock uses a dedicated session/connection. Consider connection pool
        sizing when using multiple concurrent locks.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        holder_id: str | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the lock manager.

        Args:
            session_factory: SQLAlchemy async session factory for database access
            holder_id: Optional identifier for this lock holder (for debugging)
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing.
                          Ignored if tracer is explicitly provided.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._session_factory = session_factory
        self._holder_id = holder_id
        self._held_locks: dict[str, tuple[AsyncSession, int]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key_to_lock_id(key: str) -> int:
        """
        Convert a string key to a 64-bit lock ID.

        Uses SHA-256 hash truncated to 63 bits (PostgreSQL bigint is signed).
        The 63-bit limit ensures the result fits in a PostgreSQL bigint without
        overflow issues.

        Args:
            key: String key to hash

        Returns:
            63-bit positive integer lock ID

        Example:
            >>> PostgreSQLLockManager._key_to_lock_id("migration:tenant-abc")
            4526372985471823495  # Example value
        """
        hash_bytes = hashlib.sha256(key.encode()).digest()
        # Use first 8 bytes, mask to 63 bits for signed bigint
        lock_id = int.from_bytes(hash_bytes[:8], byteorder="big") & 0x7FFFFFFFFFFFFFFF
        return lock_id

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AsyncIterator[LockInfo]:
        """
        Acquire an advisory lock as a context manager.

        The lock is automatically released when the context exits, whether
        normally or due to an exception.

        Args:
            key: String key identifying the lock (e.g., "migration:tenant-abc")
            timeout: Maximum seconds to wait for lock (None = wait forever)
            retry_interval: Seconds between retry attempts when using timeout

        Yields:
            LockInfo with lock details

        Raises:
            LockAcquisitionError: If lock cannot be acquired within timeout

        Example:
            >>> async with lock_manager.acquire("cutover:tenant-123", timeout=5.0):
            ...     await perform_cutover()
        """
        lock_id = self._key_to_lock_id(key)

        with self._tracer.span(
            "eventsource.lock.acquire",
            {
                "lock.key": key,
                "lock.id": lock_id,
                "lock.timeout": timeout if timeout is not None else -1,
            },
        ):
            session = await self._acquire_lock(key, lock_id, timeout, retry_interval)

            lock_info = LockInfo(
                key=key,
                lock_id=lock_id,
                acquired_at=datetime.now(UTC),
                holder_id=self._holder_id,
            )

            try:
                async with self._lock:
                    self._held_locks[key] = (session, lock_id)

                logger.debug(
                    "Acquired advisory lock: key=%s, lock_id=%d",
                    key,
                    lock_id,
                )

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
        """
        Internal method to acquire the lock.

        Args:
            key: String key identifying the lock
            lock_id: Numeric lock ID
            timeout: Maximum seconds to wait (None = wait forever)
            retry_interval: Seconds between retry attempts

        Returns:
            AsyncSession holding the lock

        Raises:
            LockAcquisitionError: If lock cannot be acquired within timeout
        """
        session = self._session_factory()

        try:
            if timeout is None:
                # Blocking acquisition - wait indefinitely
                await session.execute(
                    text("SELECT pg_advisory_lock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                return session

            # Non-blocking with timeout - retry until acquired or timeout
            loop = asyncio.get_event_loop()
            deadline = loop.time() + timeout

            while True:
                result = await session.execute(
                    text("SELECT pg_try_advisory_lock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                acquired = result.scalar()

                if acquired:
                    return session

                if loop.time() >= deadline:
                    await session.close()
                    raise LockAcquisitionError(
                        key=key,
                        reason=f"Timeout after {timeout}s",
                        timeout=timeout,
                    )

                await asyncio.sleep(retry_interval)

        except LockAcquisitionError:
            raise
        except Exception as e:
            await session.close()
            raise LockAcquisitionError(
                key=key,
                reason=f"Database error: {e}",
            ) from e

    async def _release_lock(
        self,
        key: str,
        session: AsyncSession,
        lock_id: int,
    ) -> None:
        """
        Internal method to release the lock.

        Args:
            key: String key identifying the lock
            session: Session holding the lock
            lock_id: Numeric lock ID
        """
        with self._tracer.span(
            "eventsource.lock.release",
            {
                "lock.key": key,
                "lock.id": lock_id,
            },
        ):
            try:
                await session.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"),
                    {"lock_id": lock_id},
                )
                logger.debug(
                    "Released advisory lock: key=%s, lock_id=%d",
                    key,
                    lock_id,
                )
            except Exception as e:
                logger.warning(
                    "Error releasing advisory lock: key=%s, error=%s",
                    key,
                    e,
                )
            finally:
                async with self._lock:
                    self._held_locks.pop(key, None)
                await session.close()

    async def try_acquire(
        self,
        key: str,
    ) -> LockInfo | None:
        """
        Try to acquire a lock without blocking.

        This method attempts to acquire the lock immediately and returns
        None if the lock is already held by another session.

        Args:
            key: String key identifying the lock

        Returns:
            LockInfo if acquired, None if lock is held by another session

        Note:
            Caller is responsible for calling release() when done.

        Example:
            >>> lock_info = await lock_manager.try_acquire("cutover:tenant-123")
            >>> if lock_info:
            ...     try:
            ...         await perform_cutover()
            ...     finally:
            ...         await lock_manager.release("cutover:tenant-123")
            ... else:
            ...     print("Lock already held")
        """
        lock_id = self._key_to_lock_id(key)
        session = self._session_factory()

        try:
            result = await session.execute(
                text("SELECT pg_try_advisory_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
            acquired = result.scalar()

            if not acquired:
                await session.close()
                return None

            async with self._lock:
                self._held_locks[key] = (session, lock_id)

            logger.debug(
                "Acquired advisory lock (try): key=%s, lock_id=%d",
                key,
                lock_id,
            )

            return LockInfo(
                key=key,
                lock_id=lock_id,
                acquired_at=datetime.now(UTC),
                holder_id=self._holder_id,
            )

        except Exception as e:
            await session.close()
            logger.error(
                "Error attempting to acquire lock: key=%s, error=%s",
                key,
                e,
            )
            raise

    async def release(self, key: str) -> None:
        """
        Release a previously acquired lock.

        Args:
            key: String key identifying the lock

        Raises:
            LockNotHeldError: If the lock is not held by this manager

        Example:
            >>> lock_info = await lock_manager.try_acquire("cutover:tenant-123")
            >>> if lock_info:
            ...     try:
            ...         await perform_cutover()
            ...     finally:
            ...         await lock_manager.release("cutover:tenant-123")
        """
        async with self._lock:
            if key not in self._held_locks:
                raise LockNotHeldError(key)
            session, lock_id = self._held_locks[key]

        await self._release_lock(key, session, lock_id)

    async def is_held(self, key: str) -> bool:
        """
        Check if a lock is currently held by this manager.

        Args:
            key: String key identifying the lock

        Returns:
            True if lock is held, False otherwise

        Example:
            >>> if await lock_manager.is_held("cutover:tenant-123"):
            ...     print("Lock is held")
        """
        async with self._lock:
            return key in self._held_locks

    async def release_all(self) -> int:
        """
        Release all locks held by this manager.

        Useful for cleanup on shutdown or error recovery.

        Returns:
            Number of locks released

        Example:
            >>> released = await lock_manager.release_all()
            >>> print(f"Released {released} locks")
        """
        async with self._lock:
            keys = list(self._held_locks.keys())

        released = 0
        for key in keys:
            try:
                await self.release(key)
                released += 1
            except LockNotHeldError:
                # Lock was already released (race condition)
                pass
            except Exception as e:
                logger.warning(
                    "Error releasing lock during release_all: key=%s, error=%s",
                    key,
                    e,
                )

        return released

    @property
    def held_lock_count(self) -> int:
        """
        Get the number of locks currently held by this manager.

        Returns:
            Number of held locks

        Note:
            This property is not async-safe; use with caution in
            concurrent code.
        """
        return len(self._held_locks)


def migration_lock_key(tenant_id: UUID, operation: str = "migration") -> str:
    """
    Create a lock key for migration operations.

    Provides a consistent naming convention for migration-related locks.

    Args:
        tenant_id: Tenant UUID
        operation: Operation type (migration, cutover, etc.)

    Returns:
        Lock key string in format "{operation}:{tenant_id}"

    Example:
        >>> key = migration_lock_key(tenant_id, "cutover")
        >>> async with lock_manager.acquire(key):
        ...     await perform_cutover()
    """
    return f"{operation}:{tenant_id}"
