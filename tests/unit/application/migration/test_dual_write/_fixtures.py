"""Test fixtures and helpers for DualWriteInterceptor unit tests."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.dual_write import DualWriteInterceptor
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


def create_mock_store(store_id: str = "store", position_key: tuple = (100,)) -> MagicMock:
    """Create a mock FullEventStore with proper async support."""
    store = MagicMock()
    store.append = AsyncMock(
        side_effect=lambda stream, events, expected: AppendResult(
            stream=stream,
            new_version=len(events),
            position=Position(store_id=store_id, key=(1,)),
        )
    )
    store.event_exists = AsyncMock(return_value=False)
    store.get_stream_version = AsyncMock(return_value=0)
    store.current_position = AsyncMock(return_value=Position(store_id=store_id, key=position_key))

    # For async generators, we need to return the generator itself, not a coroutine
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


async def async_generator_mock(items: list):
    """Helper to create async generators for mocking."""
    for item in items:
        yield item


@pytest.fixture
def source_store() -> MagicMock:
    """Create a mock source event store."""
    return create_mock_store(store_id="source", position_key=(100,))


@pytest.fixture
def target_store() -> MagicMock:
    """Create a mock target event store."""
    return create_mock_store(store_id="target", position_key=(50,))


@pytest.fixture
def tenant_id() -> UUID:
    """Create a tenant ID for testing."""
    return uuid4()


@pytest.fixture
def interceptor(
    source_store: MagicMock,
    target_store: MagicMock,
    tenant_id: UUID,
) -> DualWriteInterceptor:
    """Create a DualWriteInterceptor with mock dependencies."""
    return DualWriteInterceptor(
        source_store=source_store,
        target_store=target_store,
        tenant_id=tenant_id,
        enable_tracing=False,
    )


__all__ = [  # pragma: allowlist secret
    "TestEvent",
    "async_generator_mock",
    "create_mock_store",
    "interceptor",
    "sid",
    "source_store",
    "target_store",
    "tenant_id",
]
