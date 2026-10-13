"""Integration tests for PostgreSQL advisory lock cleanup, concurrency, and migration keys."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.ports.exceptions import LockNotHeldError
from eventsource.ports.locks import migration_lock_key

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


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
                await asyncio.sleep(10)

        task = asyncio.create_task(hold_lock())
        await asyncio.sleep(0.1)
        assert lock_held

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        await asyncio.sleep(0.1)

        assert not await lock_manager.is_held(key)


class TestReleaseAll:
    """Tests for release_all functionality."""

    async def test_release_all_releases_multiple_locks(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that release_all releases all held locks."""
        keys = ["test:release_all:1", "test:release_all:2", "test:release_all:3"]

        for key in keys:
            await lock_manager.try_acquire(key)

        assert lock_manager.held_lock_count == 3

        released = await lock_manager.release_all()
        assert released == 3
        assert lock_manager.held_lock_count == 0

        for key in keys:
            assert not await lock_manager.is_held(key)

    async def test_release_all_on_empty_manager(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test release_all when no locks are held."""
        released = await lock_manager.release_all()
        assert released == 0


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


class TestConcurrentOperations:
    """Tests for concurrent lock operations."""

    async def test_concurrent_acquire_attempts(
        self,
        lock_manager: PostgreSQLLockManager,
        second_lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test concurrent acquisition attempts."""
        key = "test:concurrent:1"

        async def try_acquire_and_hold(manager: PostgreSQLLockManager) -> bool:
            lock_info = await manager.try_acquire(key)
            if lock_info:
                await asyncio.sleep(0.1)
                await manager.release(key)
                return True
            return False

        result_1, result_2 = await asyncio.gather(
            try_acquire_and_hold(lock_manager),
            try_acquire_and_hold(second_lock_manager),
        )

        results = [result_1, result_2]
        assert True in results

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
            await asyncio.sleep(0.05)
            async with second_lock_manager.acquire(key):
                acquired_order.append("second")

        await asyncio.gather(hold_briefly(), wait_for_lock())

        assert acquired_order == ["first", "second"]


class TestMigrationLockKeyIntegration:
    """Integration tests for migration_lock_key helper."""

    async def test_migration_lock_key_creates_valid_lock(
        self,
        lock_manager: PostgreSQLLockManager,
    ) -> None:
        """Test that migration_lock_key creates lockable keys."""
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
        tenant_id = uuid4()
        key_migration = migration_lock_key(tenant_id, "migration")
        key_cutover = migration_lock_key(tenant_id, "cutover")

        async with lock_manager.acquire(key_migration):  # noqa: SIM117
            async with second_lock_manager.acquire(key_cutover):
                pass
