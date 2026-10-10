"""Pytest fixtures for AggregateRepository tests."""

from __future__ import annotations

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    MockEventPublisher,
    OrderAggregate,
)


@pytest.fixture
def event_store() -> InMemoryEventStore:
    """Create a fresh InMemoryEventStore for each test."""
    return InMemoryEventStore()


@pytest.fixture
def counter_repository(event_store: InMemoryEventStore) -> AggregateRepository[CounterAggregate]:
    """Create a repository for CounterAggregate."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
    )


@pytest.fixture
def order_repository(event_store: InMemoryEventStore) -> AggregateRepository[OrderAggregate]:
    """Create a repository for OrderAggregate."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
    )


@pytest.fixture
def mock_publisher() -> MockEventPublisher:
    """Create a mock event publisher."""
    return MockEventPublisher()


@pytest.fixture
def counter_repository_with_publisher(
    event_store: InMemoryEventStore, mock_publisher: MockEventPublisher
) -> AggregateRepository[CounterAggregate]:
    """Create a repository with event publisher."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=CounterAggregate,
        event_publisher=mock_publisher,
    )
