"""Conformance tests for MemoryEventStore against the port suites."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import MemoryEventStore
from eventsource.testing.conformance_ports import (
    AppenderConformance,
    CategoryQueryConformance,
    EventLookupConformance,
    GlobalFeedConformance,
    StreamReaderConformance,
)


class TestMemoryAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryGlobalFeed(GlobalFeedConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryCategoryQuery(CategoryQueryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()
