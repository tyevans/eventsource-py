"""
Integration tests for PostgreSQL advisory lock utilities.

These tests require a real PostgreSQL database and verify:
- Lock acquisition and release
- Lock exclusion between sessions
- Timeout behavior
- Context manager cleanup
- Concurrent lock operations
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from eventsource.locks import (
    LockAcquisitionError,
    LockNotHeldError,
    PostgreSQLLockManager,
    migration_lock_key,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

# Mark all tests in this module as integration tests requiring PostgreSQL
pytestmark = [pytest.mark.integration, pytest.mark.postgres]


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def lock_manager(
    postgres_session_factory: async_sessionmaker[AsyncSession],
) -> PostgreSQLLockManager:
    """Provide a PostgreSQLLockManager for testing."""
    return PostgreSQLLockManager(
        postgres_session_factory,
        holder_id="test-worker",
        enable_tracing=False,
    )


@pytest_asyncio.fixture
async def second_lock_manager(
    postgres_session_factory: async_sessionmaker[AsyncSession],
) -> PostgreSQLLockManager:
    """Provide a second PostgreSQLLockManager for concurrent testing."""
    return PostgreSQLLockManager(
        postgres_session_factory,
        holder_id="test-worker-2",
        enable_tracing=False,
    )


# =============================================================================
# Basic Lock Acquisition Tests
# =============================================================================


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

        # Lock should be released after context
        assert not await lock_manager.is_held(key)

    async def test_acquire_same_lock_twice_sequentially(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test acquiring the same lock twice in sequence."""
        key = "test:lock:2"

        async with lock_manager.acquire(key):
            pass

        # Should be able to acquire again after release
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

        # Clean up
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


# =============================================================================
# Lock Exclusion Tests
# =============================================================================


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
            # Second manager should not be able to acquire
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
            pass  # Lock released here

        # Second manager should now be able to acquire
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
            # Second manager should be able to acquire different key
            lock_info = await second_lock_manager.try_acquire(key_2)
            assert lock_info is not None
            await second_lock_manager.release(key_2)


# =============================================================================
# Timeout Tests
# =============================================================================


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
            # Second manager should timeout
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


# =============================================================================
# Context Manager Cleanup Tests
# =============================================================================


class TestContextManagerCleanup:
    """Tests for context manager cleanup behavior."""

    async def test_lock_released_on_exception(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that lock is released when exception occurs in context."""
        key = "test:cleanup:1"

        with pytest.raises(ValueError):
            async with lock_manager.acquire(key):
                raise ValueError("Test error")

        # Lock should be released despite exception
        assert not await lock_manager.is_held(key)

    async def test_lock_released_on_cancellation(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that lock is released on task cancellation."""
        key = "test:cleanup:2"
        lock_held = False

        async def hold_lock() -> None:
            nonlocal lock_held
            async with lock_manager.acquire(key):
                lock_held = True
                await asyncio.sleep(10)  # Will be cancelled

        task = asyncio.create_task(hold_lock())
        await asyncio.sleep(0.1)  # Let task acquire lock
        assert lock_held

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # Give time for cleanup
        await asyncio.sleep(0.1)

        # Lock should be released
        assert not await lock_manager.is_held(key)


# =============================================================================
# Release All Tests
# =============================================================================


class TestReleaseAll:
    """Tests for release_all functionality."""

    async def test_release_all_releases_multiple_locks(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that release_all releases all held locks."""
        keys = ["test:release_all:1", "test:release_all:2", "test:release_all:3"]

        # Acquire multiple locks
        for key in keys:
            await lock_manager.try_acquire(key)

        assert lock_manager.held_lock_count == 3

        # Release all
        released = await lock_manager.release_all()
        assert released == 3
        assert lock_manager.held_lock_count == 0

        # Verify all released
        for key in keys:
            assert not await lock_manager.is_held(key)

    async def test_release_all_on_empty_manager(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test release_all when no locks are held."""
        released = await lock_manager.release_all()
        assert released == 0


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling behavior."""

    async def test_release_unheld_lock_raises_error(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that releasing an unheld lock raises LockNotHeldError."""
        with pytest.raises(LockNotHeldError) as exc_info:
            await lock_manager.release("nonexistent:lock")

        assert exc_info.value.key == "nonexistent:lock"

    async def test_is_held_false_for_unheld_lock(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that is_held returns False for unheld locks."""
        assert not await lock_manager.is_held("nonexistent:lock")


# =============================================================================
# Concurrent Operation Tests
# =============================================================================


class TestConcurrentOperations:
    """Tests for concurrent lock operations."""

    async def test_concurrent_acquire_attempts(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test concurrent acquisition attempts."""
        key = "test:concurrent:1"
        results: list[bool] = []

        async def try_acquire_and_hold(manager: PostgreSQLLockManager) -> bool:
            lock_info = await manager.try_acquire(key)
            if lock_info:
                await asyncio.sleep(0.1)
                await manager.release(key)
                return True
            return False

        # Start both acquisitions concurrently
        result_1, result_2 = await asyncio.gather(
            try_acquire_and_hold(lock_manager),
            try_acquire_and_hold(second_lock_manager),
        )

        # Only one should succeed at a time
        # (one gets lock immediately, other fails try_acquire)
        results = [result_1, result_2]
        assert True in results  # At least one succeeded

    async def test_waiting_for_lock_release(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that blocking acquire waits for lock release."""
        key = "test:concurrent:2"
        acquired_order: list[str] = []

        async def hold_briefly() -> None:
            async with lock_manager.acquire(key):
                acquired_order.append("first")
                await asyncio.sleep(0.2)

        async def wait_for_lock() -> None:
            await asyncio.sleep(0.05)  # Start slightly after
            async with second_lock_manager.acquire(key):
                acquired_order.append("second")

        await asyncio.gather(hold_briefly(), wait_for_lock())

        assert acquired_order == ["first", "second"]


# =============================================================================
# Migration Lock Key Tests
# =============================================================================


class TestMigrationLockKeyIntegration:
    """Integration tests for migration_lock_key helper."""

    async def test_migration_lock_key_creates_valid_lock(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that migration_lock_key creates lockable keys."""
        from uuid import uuid4

        tenant_id = uuid4()
        key = migration_lock_key(tenant_id, "cutover")

        async with lock_manager.acquire(key) as lock_info:
            assert lock_info.key == f"cutover:{tenant_id}"

    async def test_different_operations_different_locks(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that different operations create different locks."""
        from uuid import uuid4

        tenant_id = uuid4()
        key_migration = migration_lock_key(tenant_id, "migration")
        key_cutover = migration_lock_key(tenant_id, "cutover")

        async with lock_manager.acquire(key_migration):  # noqa: SIM117
            # Should be able to acquire different operation for same tenant
            async with second_lock_manager.acquire(key_cutover):
                pass
