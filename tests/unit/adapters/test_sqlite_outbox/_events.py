"""Sample events for SQLite outbox tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent


class TestSampleEvent(DomainEvent):
    """Sample event for repository tests."""

    __test__ = False
    aggregate_type: str = "TestSampleAggregate"
    data: str = "test_data"


class SampleEvent(DomainEvent):
    """Sample event for the standalone SQLite outbox tests."""

    aggregate_type: str = "SampleAggregate"
    test_field: str = "test_value"
