"""Conformance tests for `InMemoryLeaderElector` against the port suite.

Two bindings, one per mode the adapter documents: single-instance (always
wins, no contention expressible) and simulated multi-instance over a
shared `SharedLeaderState`.
"""

import pytest

from eventsource.adapters.memory.coordination import InMemoryLeaderElector, SharedLeaderState
from eventsource.testing.conformance_ports import LeaderElectorConformance


class TestInMemoryLeaderElectorSingleInstance(LeaderElectorConformance):
    @pytest.fixture
    def elector(self) -> object:
        return InMemoryLeaderElector("worker-1")

    @pytest.fixture
    def rival(self) -> object | None:
        return None  # single-instance mode: every elector always wins


class TestInMemoryLeaderElectorSharedState(LeaderElectorConformance):
    @pytest.fixture
    def state(self) -> SharedLeaderState:
        return SharedLeaderState()

    @pytest.fixture
    def elector(self, state: SharedLeaderState) -> object:
        return InMemoryLeaderElector("worker-1", shared_state=state)

    @pytest.fixture
    def rival(self, state: SharedLeaderState) -> object | None:
        return InMemoryLeaderElector("worker-2", shared_state=state)
