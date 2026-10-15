"""Integration tests for PostgreSQL advisory lock acquisition, exclusion, and timeouts."""

from __future__ import annotations

import pytest

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.ports.exceptions import LockAcquisitionError

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class TestBasicLockAcquisition:
    """Tests for basic lock acquisition and release."""

    async def test_acquire_and_release_via_context_manager(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test acquiring and releasing a lock via context manager."""
        key = "test:lock:1"

        async with lock_manager.acquire(key) as lock_info:
            assert lock_info.key == key
            assert lock_info.lock_id > 0
            assert lock_info.holder_id == "test-worker"
            assert await lock_manager.is_held(key)

        assert not await lock_manager.is_held(key)

    async def test_acquire_same_lock_twice_sequentially(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test acquiring the same lock twice in sequence."""
        key = "test:lock:2"

        async with lock_manager.acquire(key):
            pass

        async with lock_manager.acquire(key):
            pass

    async def test_try_acquire_returns_lock_info(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test try_acquire returns LockInfo when successful."""
        key = "test:lock:3"

        lock_info = await lock_manager.try_acquire(key)
        assert lock_info is not None
        assert lock_info.key == key
        assert lock_info.lock_id > 0

        await lock_manager.release(key)

    async def test_explicit_release(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test explicit release after try_acquire."""
        key = "test:lock:4"

        lock_info = await lock_manager.try_acquire(key)
        assert lock_info is not None
        assert await lock_manager.is_held(key)

        await lock_manager.release(key)
        assert not await lock_manager.is_held(key)


class TestLockExclusion:
    """Tests for lock exclusion between sessions."""

    async def test_second_manager_blocked_on_same_lock(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that a second manager cannot acquire a held lock."""
        key = "test:exclusion:1"

        async with lock_manager.acquire(key):
            lock_info = await second_lock_manager.try_acquire(key)
            assert lock_info is None

    async def test_second_manager_can_acquire_after_release(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that second manager can acquire after first releases."""
        key = "test:exclusion:2"

        async with lock_manager.acquire(key):
            pass

        lock_info = await second_lock_manager.try_acquire(key)
        assert lock_info is not None
        await second_lock_manager.release(key)

    async def test_different_keys_do_not_block(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that different keys do not block each other."""
        key_1 = "test:exclusion:3a"
        key_2 = "test:exclusion:3b"

        async with lock_manager.acquire(key_1):
            lock_info = await second_lock_manager.try_acquire(key_2)
            assert lock_info is not None
            await second_lock_manager.release(key_2)


class TestLockTimeout:
    """Tests for lock acquisition with timeout."""

    async def test_timeout_raises_error_when_lock_held(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that timeout raises LockAcquisitionError."""
        key = "test:timeout:1"

        async with lock_manager.acquire(key):
            with pytest.raises(LockAcquisitionError) as exc_info:
                async with second_lock_manager.acquire(key, timeout=0.2):
                    pass

            assert exc_info.value.key == key
            assert exc_info.value.timeout == 0.2
            assert "Timeout" in exc_info.value.reason

    async def test_timeout_succeeds_when_lock_available(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that timeout succeeds when lock is available."""
        key = "test:timeout:2"

        async with lock_manager.acquire(key, timeout=1.0) as lock_info:
            assert lock_info.key == key

    async def test_acquire_with_short_retry_interval(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test lock acquisition with short retry interval."""
        key = "test:timeout:3"

        async with lock_manager.acquire(key):
            with pytest.raises(LockAcquisitionError):
                async with second_lock_manager.acquire(
                    key,
                    timeout=0.3,
                    retry_interval=0.05,
                ):
                    pass
