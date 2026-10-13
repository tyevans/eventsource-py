"""Fixtures and sample event models for EventAssertions tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.testing.assertions import EventAssertions


class SampleCreated(DomainEvent):
    """Test event for sample creation."""

    aggregate_type: str = "Sample"
    name: str


class SampleUpdated(DomainEvent):
    """Test event for sample update."""

    aggregate_type: str = "Sample"
    new_value: int


class SampleDeleted(DomainEvent):
    """Test event for sample deletion."""

    aggregate_type: str = "Sample"


class OtherEvent(DomainEvent):
    """Another test event type."""

    aggregate_type: str = "Other"
    data: str


@pytest.fixture
def empty_assertions() -> EventAssertions:
    """Create assertions with no events."""
    return EventAssertions([])


@pytest.fixture
def single_event_assertions() -> EventAssertions:
    """Create assertions with a single event."""
    event = SampleCreated(
        aggregate_id=uuid4(),
        aggregate_type="Sample",
        aggregate_version=1,
        name="test-sample",
    )
    return EventAssertions([event])


@pytest.fixture
def multi_event_assertions() -> EventAssertions:
    """Create assertions with multiple events."""
    agg_id = uuid4()
    events = [
        SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="test-sample",
        ),
        SampleUpdated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
            new_value=42,
        ),
        SampleDeleted(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=3,
        ),
    ]
    return EventAssertions(events)
