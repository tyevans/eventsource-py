"""Helper models and events for SQLite repository tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent


class TestSampleEvent(DomainEvent):
    """Sample event for repository tests."""

    __test__ = False

    aggregate_type: str = "TestSampleAggregate"
    data: str = "test_data"
