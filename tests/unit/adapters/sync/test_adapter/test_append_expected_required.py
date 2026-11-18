"""SyncEventStoreAdapter.append requires `expected`, matching the async port."""

from __future__ import annotations

import inspect
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.adapters.sync import SyncEventStoreAdapter
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports import ExpectedVersion
from eventsource.ports.store import EventAppender


class ExpectedRequiredEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "ExpectedRequired"


@pytest.fixture
def sync_store() -> SyncEventStoreAdapter:
    registry = EventRegistry()
    registry.register(ExpectedRequiredEvent)
    return SyncEventStoreAdapter(InMemoryEventStore(event_registry=registry))


def test_sync_append_expected_has_no_default() -> None:
    parameter = inspect.signature(SyncEventStoreAdapter.append).parameters["expected"]
    assert parameter.default is inspect.Parameter.empty


def test_sync_append_expected_matches_port_requirement() -> None:
    port_parameter = inspect.signature(EventAppender.append).parameters["expected"]
    sync_parameter = inspect.signature(SyncEventStoreAdapter.append).parameters["expected"]
    assert port_parameter.default is inspect.Parameter.empty
    assert sync_parameter.default is port_parameter.default


def test_sync_append_without_expected_is_rejected(sync_store: SyncEventStoreAdapter) -> None:
    agg_id = uuid4()
    stream = StreamId(aggregate_id=agg_id, category="ExpectedRequired")
    event = ExpectedRequiredEvent(aggregate_id=agg_id)
    with pytest.raises(TypeError):
        sync_store.append(stream, event)  # type: ignore[call-arg]
    assert sync_store.get_stream_version(stream) == 0


def test_sync_append_with_explicit_any_succeeds(sync_store: SyncEventStoreAdapter) -> None:
    agg_id = uuid4()
    stream = StreamId(aggregate_id=agg_id, category="ExpectedRequired")
    event = ExpectedRequiredEvent(aggregate_id=agg_id)
    result = sync_store.append(stream, event, ExpectedVersion.any_())
    assert result.new_version == 1
