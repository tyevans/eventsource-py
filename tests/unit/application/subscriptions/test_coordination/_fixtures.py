"""Shared fixtures and mocks for coordination tests."""

import pytest

from eventsource.adapters.memory.coordination import (
    SharedLeaderState,
)
from eventsource.application.subscriptions.coordination import (
    WorkRedistributionCoordinator,
)
from eventsource.ports.coordination import (
    LeaderChangeCallback,
)


class MockLeaderElector:
    """Mock implementation for testing protocol compliance."""

    @property
    def identity(self) -> str:
        return "test-instance"

    @property
    def is_leader(self) -> bool:
        return False

    @property
    def current_leader(self) -> str | None:
        return None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        return True

    async def release(self) -> None:
        pass

    async def renew(self) -> bool:
        return True

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        pass

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        return True


class IncompleteElector:
    """Missing required methods."""

    @property
    def identity(self) -> str:
        return "test"


class MockElector:
    """Mock leader elector with full state tracking for workflow testing."""

    def __init__(self, identity: str) -> None:
        self._identity = identity
        self._is_leader = False
        self._callbacks: list[LeaderChangeCallback] = []

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def current_leader(self) -> str | None:
        return self._identity if self._is_leader else None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        if not self._is_leader:
            self._is_leader = True
            for cb in self._callbacks:
                await cb(True)
            return True
        return True  # Already leader

    async def release(self) -> None:
        if self._is_leader:
            self._is_leader = False
            for cb in self._callbacks:
                await cb(False)

    async def renew(self) -> bool:
        return self._is_leader

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        self._callbacks.append(callback)

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        if callback in self._callbacks:
            self._callbacks.remove(callback)
            return True
        return False


@pytest.fixture
def mock_leader_elector() -> MockLeaderElector:
    """Fixture providing a basic MockLeaderElector."""
    return MockLeaderElector()


@pytest.fixture
def shared_leader_state() -> SharedLeaderState:
    """Fixture providing a fresh SharedLeaderState."""
    return SharedLeaderState()


@pytest.fixture
def work_coordinator() -> WorkRedistributionCoordinator:
    """Fixture providing a fresh WorkRedistributionCoordinator."""
    return WorkRedistributionCoordinator(instance_id="worker-1")
