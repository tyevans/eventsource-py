"""Shared sample event for InMemoryOutboxRepository unit tests."""

from eventsource.domain.event import DomainEvent


class SampleEvent(DomainEvent):
    """Sample event for outbox tests."""

    aggregate_type: str = "SampleAggregate"
    test_field: str = "test_value"
