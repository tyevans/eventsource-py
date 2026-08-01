"""Conformance tests for InMemoryEventStore against the port suites."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import InMemoryEventStore, InMemorySnapshotStore
from eventsource.testing.conformance_ports import (
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    SnapshotStoreConformance,
    StreamReaderConformance,
)


class TestMemoryAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemoryCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore()


class TestMemorySnapshotStore(SnapshotStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemorySnapshotStore]:
        yield InMemorySnapshotStore()
