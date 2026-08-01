"""Conformance suite for the `DistributedLock` port.

Subclass and provide a `store` fixture yielding a fresh lock manager.

Pins only the intersection every backend honestly meets: exclusion among
callers of one manager instance, the context-manager release contract, and
the two error types. **Deliberately not pinned here** -- cross-process or
cross-connection exclusion, release on crash, fairness/FIFO ordering among
waiters, and the numeric value of `LockInfo.lock_id`. Those are
PostgreSQL-specific and stay in
`tests/integration/locks/test_postgresql_locks_integration.py`.
"""

from abc import ABC, abstractmethod

import pytest

from eventsource.domain.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.ports.locks import LockManager


class DistributedLockConformance(ABC):
    """Conformance suite for `DistributedLock` + `LockRegistry` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh manager implementing `LockManager`."""
        raise NotImplementedError

    async def test_acquire_yields_info_for_the_requested_key(self, store: LockManager) -> None:
        async with store.acquire("alpha") as info:
            assert info.key == "alpha"

    async def test_is_held_inside_and_outside_the_block(self, store: LockManager) -> None:
        async with store.acquire("alpha"):
            assert await store.is_held("alpha") is True
        assert await store.is_held("alpha") is False

    async def test_acquire_releases_on_exception_and_propagates_it(
        self, store: LockManager
    ) -> None:
        sentinel = RuntimeError("boom")
        with pytest.raises(RuntimeError) as caught:
            async with store.acquire("alpha"):
                raise sentinel
        assert caught.value is sentinel
        assert await store.is_held("alpha") is False

    async def test_try_acquire_succeeds_on_free_key_and_fails_on_held(
        self, store: LockManager
    ) -> None:
        first = await store.try_acquire("alpha")
        assert first is not None
        assert first.key == "alpha"
        assert await store.try_acquire("alpha") is None

    async def test_release_makes_the_key_acquirable_again(self, store: LockManager) -> None:
        assert await store.try_acquire("alpha") is not None
        await store.release("alpha")
        assert await store.try_acquire("alpha") is not None

    async def test_release_of_a_never_held_key_raises(self, store: LockManager) -> None:
        with pytest.raises(LockNotHeldError):
            await store.release("never-held")

    async def test_acquire_with_timeout_on_a_held_key_raises(self, store: LockManager) -> None:
        assert await store.try_acquire("alpha") is not None
        with pytest.raises(LockAcquisitionError) as caught:
            async with store.acquire("alpha", timeout=0.05):
                pass  # pragma: no cover - acquisition must fail
        assert caught.value.timeout == 0.05

    async def test_different_keys_are_independent(self, store: LockManager) -> None:
        async with store.acquire("alpha"):
            async with store.acquire("beta") as beta:
                assert beta.key == "beta"
            assert await store.is_held("alpha") is True

    async def test_release_all_returns_the_count_and_empties_the_registry(
        self, store: LockManager
    ) -> None:
        assert await store.try_acquire("alpha") is not None
        assert await store.try_acquire("beta") is not None
        assert store.held_lock_count == 2
        assert await store.release_all() == 2
        assert store.held_lock_count == 0


__all__ = ["DistributedLockConformance"]
