"""
Tests for the EventBus conformance test suite.

This module demonstrates how to use EventBusConformanceSuite by running it
against the in-memory EventBus implementation. The EventStore half of this
suite was retired in favor of `eventsource.testing.conformance_ports`
(see `tests/unit/adapters/test_memory_conformance.py` and
`test_sqlite_conformance.py` for the store-side runs).
"""

from typing import Any
from uuid import UUID

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.ports.bus import EventBus
from eventsource.testing.conformance import EventBusConformanceSuite


# Test event for conformance testing
class ConformanceTestEvent(DomainEvent):
    """Simple event for conformance testing."""

    aggregate_type: str = "ConformanceTest"
    test_data: str = "test"


class InMemoryEventBusConformance(EventBusConformanceSuite):
    """Conformance tests for InMemoryEventBus."""

    def create_bus(self) -> InMemoryEventBus:
        """Create a fresh InMemoryEventBus instance."""
        return InMemoryEventBus(enable_tracing=False)

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        """Create a test event."""
        return ConformanceTestEvent(
            aggregate_id=aggregate_id,
            test_data="test",
        )

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        """Create a subscriber for ConformanceTestEvent."""

        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [ConformanceTestEvent]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        """Drain background tasks before checking delivery."""
        assert isinstance(bus, InMemoryEventBus)
        await bus.shutdown(timeout=5.0)


# EventBus conformance tests - all inherited from base suite
class TestInMemoryEventBusConformance(InMemoryEventBusConformance):
    """Run all EventBus conformance tests against InMemoryEventBus."""

    pass


# Additional tests to verify the conformance suite pattern itself


@pytest.mark.asyncio
async def test_event_bus_conformance_suite_works() -> None:
    """Smoke test that EventBus conformance suite runs successfully."""
    suite = InMemoryEventBusConformance()
    await suite.test_publish_and_subscribe_roundtrip()
