"""Shared fixtures and test helpers for bulk copier tests."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports import AppendResult, EventEnvelope, Position
from eventsource.ports.migration.models import Migration, MigrationConfig, MigrationPhase

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class TestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False
    aggregate_type: str = "TestAggregate"
    value: str = "test"


def _pos(store_id: str, *key: int | str) -> Position:
    return Position(store_id=store_id, key=key)


def create_test_events(
    count: int,
    tenant_id: UUID,
    aggregate_id: UUID | None = None,
    start: int = 1,
    store_id: str = "source",
    stream_id: StreamId | None = None,
) -> list[EventEnvelope]:
    """Create a list of EventEnvelope instances for testing."""
    events = []
    agg_id = aggregate_id or uuid4()
    actual_stream_id = stream_id or StreamId(aggregate_id=agg_id, category="TestAggregate")
    for i in range(count):
        event = TestEvent(
            aggregate_id=agg_id,
            tenant_id=tenant_id,
            value=f"test_{i}",
        )
        envelope = EventEnvelope(
            event=event,
            stream_id=actual_stream_id,
            stream_version=i + 1,
            position=_pos(store_id, start + i),
            stored_at=datetime.now(UTC),
        )
        events.append(envelope)
    return events


def make_event_generator(events: list[EventEnvelope]) -> Any:
    """Create an async generator callable returning the given events."""

    async def event_generator(
        from_position: Position | None = None, options: Any = None
    ) -> AsyncIterator[EventEnvelope]:
        for event in events:
            yield event

    return event_generator


async def empty_event_generator(
    from_position: Position | None = None, options: Any = None
) -> AsyncIterator[EventEnvelope]:
    """Async generator yielding no events."""
    return
    yield


def make_append_result(stream: StreamId, version: int, target_pos: int) -> AppendResult:
    """Create an AppendResult with position at target_pos."""
    return AppendResult(stream=stream, new_version=version, position=_pos("target", target_pos))


def create_migration(
    id: UUID | None = None,
    tenant_id: UUID | None = None,
    source_store_id: str = "source",
    target_store_id: str = "target",
    phase: MigrationPhase = MigrationPhase.BULK_COPY,
    events_total: int = 0,
    events_copied: int = 0,
    last_source_position: Position | None = None,
    last_target_position: Position | None = None,
    batch_size: int = 10,
    max_bulk_copy_rate: int = 10000,
) -> Migration:
    """Create a Migration model with customizable fields."""
    return Migration(
        id=id or uuid4(),
        tenant_id=tenant_id or uuid4(),
        source_store_id=source_store_id,
        target_store_id=target_store_id,
        phase=phase,
        events_total=events_total,
        events_copied=events_copied,
        last_source_position=last_source_position,
        last_target_position=last_target_position,
        config=MigrationConfig(batch_size=batch_size, max_bulk_copy_rate=max_bulk_copy_rate),
    )
