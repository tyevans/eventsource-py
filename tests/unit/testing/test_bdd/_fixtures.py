"""Test fixtures, models, and domain events for BDD helper tests."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event
from eventsource.testing import InMemoryTestHarness

# =============================================================================
# Test Domain Events
# =============================================================================


@register_event
class SampleCreated(DomainEvent):
    """Test event for sample creation."""

    aggregate_type: str = "Sample"
    name: str


@register_event
class SampleUpdated(DomainEvent):
    """Test event for sample update."""

    aggregate_type: str = "Sample"
    new_value: int


@register_event
class SampleDeleted(DomainEvent):
    """Test event for sample deletion."""

    aggregate_type: str = "Sample"


@register_event
class OtherAggregateCreated(DomainEvent):
    """Test event for a different aggregate type."""

    aggregate_type: str = "OtherAggregate"
    description: str


# =============================================================================
# Test Aggregate
# =============================================================================


class SampleState(BaseModel):
    """State model for test aggregate."""

    name: str = ""
    value: int = 0


class SampleAggregate(AggregateRoot[SampleState]):
    """Test aggregate for BDD helper tests."""

    aggregate_type: str = "Sample"

    def _get_initial_state(self) -> SampleState:
        """Return initial state."""
        return SampleState()

    def _apply(self, event: DomainEvent) -> None:
        """Apply event to state."""
        if isinstance(event, SampleCreated):
            self._state = SampleState(name=event.name, value=0)
        elif isinstance(event, SampleUpdated) and self._state:
            self._state = self._state.model_copy(update={"value": event.new_value})

    def create(self, name: str) -> None:
        """Create the sample."""
        event = SampleCreated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            name=name,
        )
        self._raise_event(event)

    def update(self, new_value: int) -> None:
        """Update the sample."""
        event = SampleUpdated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            new_value=new_value,
        )
        self._raise_event(event)

    def delete(self) -> None:
        """Delete the sample."""
        event = SampleDeleted(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self._raise_event(event)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def harness() -> InMemoryTestHarness:
    """Create a fresh test harness."""
    return InMemoryTestHarness()


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a test aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_aggregate(aggregate_id: UUID) -> SampleAggregate:
    """Create a test aggregate."""
    return SampleAggregate(aggregate_id)
