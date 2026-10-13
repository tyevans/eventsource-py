"""Fixtures and test doubles for subscription drain tests."""

import asyncio
from unittest.mock import MagicMock

import pytest

from eventsource.application.subscriptions.manager import SubscriptionManager
from eventsource.application.subscriptions.shutdown import ShutdownCoordinator


class MockFlowController:
    """Mock FlowController for testing."""

    def __init__(self, in_flight: int = 0, drain_delay: float = 0):
        self._in_flight = in_flight
        self._drain_delay = drain_delay

    @property
    def in_flight(self) -> int:
        return self._in_flight

    async def wait_for_drain(self, timeout: float) -> int:
        if self._drain_delay > timeout:
            await asyncio.sleep(timeout)
            return self._in_flight
        await asyncio.sleep(self._drain_delay)
        return 0


class MockCoordinator:
    """Mock TransitionCoordinator for testing."""

    def __init__(self, flow_controller: MockFlowController | None = None):
        self._flow_controller = flow_controller

    @property
    def flow_controller(self) -> MockFlowController | None:
        return self._flow_controller


@pytest.fixture
def manager():
    """Create a SubscriptionManager with mocked dependencies."""
    mgr = MagicMock(spec=SubscriptionManager)
    mgr._shutdown_coordinator = ShutdownCoordinator(drain_timeout=5.0)
    mgr._lifecycle = MagicMock()
    mgr._lifecycle.coordinators = {}
    mgr._registry = {}
    return mgr
