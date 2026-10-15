"""Low-level PostgreSQL advisory lock session and hashing helpers."""

from __future__ import annotations

import asyncio
import hashlib
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.ports.exceptions import LockAcquisitionError

logger = logging.getLogger(__name__)


def key_to_lock_id(key: str) -> int:
    """
    Convert a string key to a 64-bit lock ID.

    Uses SHA-256 hash truncated to 63 bits (PostgreSQL bigint is signed).
    The 63-bit limit ensures the result fits in a PostgreSQL bigint without
    overflow issues.
    """
    hash_bytes = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(hash_bytes[:8], byteorder="big") & 0x7FFFFFFFFFFFFFFF


async def acquire_advisory_lock_session(
    session_factory: async_sessionmaker[AsyncSession],
    key: str,
    lock_id: int,
    timeout: float | None,
    retry_interval: float,
) -> AsyncSession:
    """
    Open a dedicated session and acquire a PostgreSQL advisory lock.

    Args:
        session_factory: SQLAlchemy async session factory
        key: String key identifying the lock
        lock_id: Numeric lock ID
        timeout: Maximum seconds to wait (None = wait forever)
        retry_interval: Seconds between retry attempts

    Returns:
        AsyncSession holding the lock

    Raises:
        LockAcquisitionError: If lock cannot be acquired within timeout
    """
    session = session_factory()

    try:
        if timeout is None:
            await session.execute(
                text("SELECT pg_advisory_lock(:lock_id)"),
                {"lock_id": lock_id},
            )
            return session

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


async def release_advisory_lock_session(
    key: str,
    session: AsyncSession,
    lock_id: int,
) -> None:
    """Execute pg_advisory_unlock on the session, logging any error."""
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


__all__ = [
    "acquire_advisory_lock_session",
    "key_to_lock_id",
    "release_advisory_lock_session",
]
