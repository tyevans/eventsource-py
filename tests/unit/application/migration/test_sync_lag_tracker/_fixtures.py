"""Shared fixtures and helper functions for SyncLagTracker tests."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import ExpectedVersion, Position
from eventsource.ports.migration.models import MigrationConfig

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class LagTestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False

    aggregate_type: str = "LagTestAggregate"
    data: str = "test"


def sid(category: str = "LagTestAggregate") -> StreamId:
    """Build a fresh StreamId for tests."""
    return StreamId(aggregate_id=uuid4(), category=category)


async def seed_events(store: InMemoryEventStore, count: int) -> Position | None:
    """Append `count` events (one per stream) to `store`.

    Returns the position of the last appended event, or None if count == 0.
    """
    last_position: Position | None = None
    for _ in range(count):
        stream = sid()
        result = await store.append(
            stream, [LagTestEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
        )
        last_position = result.position
    return last_position


@pytest.fixture
def source_store() -> InMemoryEventStore:
    """Create an empty source event store."""
    return InMemoryEventStore(store_id="source", event_registry=_REGISTRY)


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create an empty target event store."""
    return InMemoryEventStore(store_id="target", event_registry=_REGISTRY)


@pytest.fixture
def config() -> MigrationConfig:
    """Create a migration config with default thresholds."""
    return MigrationConfig(cutover_max_lag_events=100)


@pytest.fixture
def strict_config() -> MigrationConfig:
    """Create a migration config with strict threshold."""
    return MigrationConfig(cutover_max_lag_events=10)


@pytest.fixture
def tracker(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    config: MigrationConfig,
) -> SyncLagTracker:
    """Create a SyncLagTracker with real in-memory stores, source 50 events ahead."""
    return SyncLagTracker(
        source_store=source_store,
        target_store=target_store,
        config=config,
        enable_tracing=False,
    )
