"""
Distributed lock utilities for eventsource.

Provides PostgreSQL advisory lock support for coordinating
operations across multiple application instances.

Advisory locks are useful for:
- Coordinating cutover operations during tenant migration
- Preventing concurrent migrations for the same tenant
- Ensuring exclusive access to critical operations

Example:
    >>> from eventsource.locks import PostgreSQLLockManager, migration_lock_key
    >>>
    >>> lock_manager = PostgreSQLLockManager(session_factory)
    >>>
    >>> # Blocking acquisition
    >>> async with lock_manager.acquire("cutover:tenant-123"):
    ...     await perform_cutover()
    >>>
    >>> # Non-blocking with timeout
    >>> try:
    ...     async with lock_manager.acquire("cutover:tenant-123", timeout=5.0):
    ...         await perform_cutover()
    ... except LockAcquisitionError:
    ...     print("Another instance is performing cutover")
    >>>
    >>> # Using migration_lock_key helper
    >>> key = migration_lock_key(tenant_id, "cutover")
    >>> async with lock_manager.acquire(key):
    ...     await perform_cutover()
"""

from eventsource.locks.postgresql import (
    LockAcquisitionError,
    LockInfo,
    LockNotHeldError,
    PostgreSQLLockManager,
    migration_lock_key,
)

__all__ = [
    "LockAcquisitionError",
    "LockInfo",
    "LockNotHeldError",
    "PostgreSQLLockManager",
    "migration_lock_key",
]
