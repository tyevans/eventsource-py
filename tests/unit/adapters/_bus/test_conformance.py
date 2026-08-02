"""InMemoryEventBus conformance to the EventBus contract."""

from typing import Any
from uuid import UUID

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.ports.bus import EventBus
from eventsource.testing.conformance import EventBusConformanceSuite


class ConformanceEvent(DomainEvent):
    aggregate_type: str = "Conformance"


class TestInMemoryEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against InMemoryEventBus."""

    def create_bus(self) -> EventBus:
        return InMemoryEventBus()

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return ConformanceEvent(aggregate_id=aggregate_id)

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [ConformanceEvent]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        """Drain background tasks before checking delivery."""
        # Cast to InMemoryEventBus to access shutdown
        assert isinstance(bus, InMemoryEventBus)
        await bus.shutdown(timeout=5.0)


def test_suite_is_actually_collected() -> None:
    """Guard against the suite silently going unrun again.

    EventBusConformanceSuite sat unused in the codebase because nothing
    subclassed it. This asserts the subclass exposes every contract test.
    """
    contract_tests = {name for name in dir(EventBusConformanceSuite) if name.startswith("test_")}

    assert len(contract_tests) == 9, (
        f"expected 9 contract tests, found {len(contract_tests)}: {sorted(contract_tests)}"
    )
    for name in contract_tests:
        assert hasattr(TestInMemoryEventBusConformance, name)
