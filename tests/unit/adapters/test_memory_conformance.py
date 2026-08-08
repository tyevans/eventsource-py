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
from eventsource.testing.conformance_ports._fixtures import make_conformance_registry


class TestMemoryAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore(event_registry=make_conformance_registry())


class TestMemoryStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore(event_registry=make_conformance_registry())


class TestMemoryEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore(event_registry=make_conformance_registry())


class TestMemoryGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore(event_registry=make_conformance_registry())


class TestMemoryCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryEventStore]:
        yield InMemoryEventStore(event_registry=make_conformance_registry())


class TestMemorySnapshotStore(SnapshotStoreConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemorySnapshotStore]:
        yield InMemorySnapshotStore()
