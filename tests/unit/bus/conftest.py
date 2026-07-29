"""Pytest fixtures and setup for bus tests."""

import pytest

from eventsource.events.registry import default_registry


@pytest.fixture(scope="session", autouse=True)
def _register_test_events() -> None:
    """Register test events to the default registry."""
    from tests.unit.bus.test_base import BaseBusEvent

    default_registry.register(BaseBusEvent, "BaseBusEvent")
