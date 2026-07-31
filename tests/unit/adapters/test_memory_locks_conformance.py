"""Conformance tests for InMemoryLockManager against the DistributedLock port suite."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import InMemoryLockManager
from eventsource.testing.conformance_ports import DistributedLockConformance


class TestMemoryLockManager(DistributedLockConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryLockManager]:
        manager = InMemoryLockManager(holder_id="conformance")
        yield manager
        await manager.release_all()

    async def test_lock_ids_are_stable_for_a_key(self, store: InMemoryLockManager) -> None:
        first = await store.try_acquire("stable")
        assert first is not None
        await store.release("stable")
        second = await store.try_acquire("stable")
        assert second is not None
        assert first.lock_id == second.lock_id
