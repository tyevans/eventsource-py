"""
Tests for conformance test suites.

This module demonstrates how to use the EventStoreConformanceSuite and
EventBusConformanceSuite by running them against the in-memory implementations.
"""

from uuid import UUID

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.testing.conformance import (
    EventBusConformanceSuite,
    EventStoreConformanceSuite,
)


# Test event for conformance testing
class ConformanceTestEvent(DomainEvent):
    """Simple event for conformance testing."""

    event_type: str = "ConformanceTestEvent"
    aggregate_type: str = "ConformanceTest"
    test_data: str = "test"


class InMemoryEventStoreConformance(EventStoreConformanceSuite):
    """Conformance tests for InMemoryEventStore."""

    def create_store(self) -> InMemoryEventStore:
        """Create a fresh InMemoryEventStore instance."""
        return InMemoryEventStore(enable_tracing=False)

    def create_test_event(self, aggregate_id: UUID, version: int = 1) -> DomainEvent:
        """Create a test event."""
        return ConformanceTestEvent(
            aggregate_id=aggregate_id,
            aggregate_version=version,
            test_data=f"test-{version}",
        )


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


# EventStore conformance tests - all inherited from base suite
class TestInMemoryEventStoreConformance(InMemoryEventStoreConformance):
    """Run all EventStore conformance tests against InMemoryEventStore."""

    pass


# EventBus conformance tests - all inherited from base suite
class TestInMemoryEventBusConformance(InMemoryEventBusConformance):
    """Run all EventBus conformance tests against InMemoryEventBus."""

    pass


# Additional tests to verify the conformance suite pattern itself


@pytest.mark.asyncio
async def test_conformance_suite_can_be_extended():
    """Verify that conformance suites can be extended with custom tests."""

    class ExtendedStoreConformance(InMemoryEventStoreConformance):
        async def test_custom_behavior(self):
            """Custom test added by subclass."""
            store = self.create_store()
            # Add custom test logic here
            assert store is not None

    suite = ExtendedStoreConformance()
    await suite.test_custom_behavior()


@pytest.mark.asyncio
async def test_event_bus_conformance_suite_works():
    """Smoke test that EventBus conformance suite runs successfully."""
    suite = InMemoryEventBusConformance()
    await suite.test_publish_and_subscribe_roundtrip()


@pytest.mark.asyncio
async def test_event_store_conformance_suite_works():
    """Smoke test that EventStore conformance suite runs successfully."""
    suite = InMemoryEventStoreConformance()
    await suite.test_append_and_get_roundtrip()
