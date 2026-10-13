"""Test fixtures and helpers for TenantStoreRouter tests."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.router import TenantStoreRouter
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import AppendResult, Position


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False

    aggregate_type: str = "TestAggregate"
    data: str = "test"


def sid(aggregate_id=None, category: str = "TestAggregate") -> StreamId:
    """Build a StreamId for tests."""
    return StreamId(aggregate_id=aggregate_id or uuid4(), category=category)


class AsyncIteratorMock:
    """Helper to create async iterators for mocking."""

    def __init__(self, items: list):
        self.items = items
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


async def async_generator_mock(items: list):
    """Helper to create async generators for mocking."""
    for item in items:
        yield item


def create_mock_store(store_id: str = "store", position_key: tuple = (100,)) -> MagicMock:
    """Create a mock FullEventStore with proper async iterator support."""
    store = MagicMock()
    store.append = AsyncMock(
        side_effect=lambda stream, events, expected: AppendResult(
            stream=stream,
            new_version=1,
            position=Position(store_id=store_id, key=(1,)),
        )
    )
    store.event_exists = AsyncMock(return_value=False)
    store.get_stream_version = AsyncMock(return_value=0)
    store.current_position = AsyncMock(return_value=Position(store_id=store_id, key=position_key))

    def mock_read_stream(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_all(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_category(*args, **kwargs):
        return async_generator_mock([])

    store.read_stream = mock_read_stream
    store.read_all = mock_read_all
    store.read_category = mock_read_category
    return store


@pytest.fixture
def mock_default_store() -> MagicMock:
    """Create a mock default event store."""
    return create_mock_store(store_id="default", position_key=(100,))


@pytest.fixture
def mock_dedicated_store() -> MagicMock:
    """Create a mock dedicated event store."""
    return create_mock_store(store_id="dedicated", position_key=(50,))


@pytest.fixture
def mock_routing_repo() -> AsyncMock:
    """Create a mock routing repository."""
    repo = AsyncMock()
    repo.get_routing = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def router(
    mock_default_store: MagicMock,
    mock_routing_repo: AsyncMock,
) -> TenantStoreRouter:
    """Create a router with mock dependencies."""
    return TenantStoreRouter(
        default_store=mock_default_store,
        routing_repo=mock_routing_repo,
        enable_tracing=False,
    )
