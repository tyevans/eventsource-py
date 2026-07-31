"""Distributed lock port.

Pure boundary interface: stdlib, typing, dataclasses, datetime, uuid only.
No sqlalchemy, no observability, no implementation code.

The contract splits along its two real consumer groups (ISP, ADR 0019):
`migration/cutover.py` and `migration/coordinator.py` acquire and release
individual locks (`DistributedLock`), while bulk lifecycle over everything
one manager holds (`LockRegistry`) is a shutdown-and-introspection concern
for whoever owns the manager.

Scope of the promise (ADR 0029, amending ADR 0023): these protocols describe
the *shape of the dependency* -- mutual exclusion among callers sharing one
manager instance, plus the error contract. They do **not** promise
cross-process exclusion, release on crash, or fairness. Those are
PostgreSQL-specific guarantees documented on
`eventsource.adapters.postgresql.locks.PostgreSQLLockManager` and pinned only
by its integration tests.
"""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


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


class DistributedLock(Protocol):
    """Acquire/release mutual exclusion on a string key."""

    def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AbstractAsyncContextManager[LockInfo]:
        """Acquire `key`, releasing it when the returned context exits.

        Declared as a plain method returning an async context manager rather
        than as `async def ... -> AsyncIterator[LockInfo]`: implementations
        decorate their `acquire` with `@asynccontextmanager`, whose runtime
        type is exactly `AbstractAsyncContextManager`, and the async-generator
        spelling would fail to type against it.

        Raises:
            LockAcquisitionError: if the lock cannot be acquired within `timeout`.
        """
        ...

    async def try_acquire(self, key: str) -> LockInfo | None:
        """Acquire `key` without blocking; `None` if it is already held.

        The caller owns the release.
        """
        ...

    async def release(self, key: str) -> None:
        """Release a lock previously taken with `try_acquire`.

        Raises:
            LockNotHeldError: if this manager does not hold `key`.
        """
        ...

    async def is_held(self, key: str) -> bool:
        """Is `key` currently held by this manager?"""
        ...


class LockRegistry(Protocol):
    """Bulk lifecycle over everything one manager instance holds."""

    async def release_all(self) -> int:
        """Release every held lock; returns the number released."""
        ...

    @property
    def held_lock_count(self) -> int:
        """Number of locks currently held by this manager."""
        ...


class LockManager(DistributedLock, LockRegistry, Protocol):
    """Composed convenience protocol: both capabilities in one object."""


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


__all__ = [
    "DistributedLock",
    "LockInfo",
    "LockManager",
    "LockRegistry",
    "migration_lock_key",
]
