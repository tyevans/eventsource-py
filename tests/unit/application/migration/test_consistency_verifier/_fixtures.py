"""Fixtures and test event models for ConsistencyVerifier tests."""

from eventsource.domain.event import DomainEvent


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False
    aggregate_type: str = "TestAggregate"
    value: str = "test"
