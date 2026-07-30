"""Conformance suite for the `EventLookup` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance
implementing both `EventLookup` and `EventAppender` (appending is the
only way to get an event into the store for the "exists" case).
"""

from abc import ABC, abstractmethod
from typing import Protocol
from uuid import uuid4

import pytest

from eventsource.ports import ExpectedVersion
from eventsource.ports.store import EventAppender, EventLookup
from eventsource.testing.conformance_ports._fixtures import make_event, make_stream


class _AppenderLookup(EventAppender, EventLookup, Protocol):
    """Adapter surface needed by this suite: append plus event lookup."""


class EventLookupConformance(ABC):
    """Conformance suite for `EventLookup` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `EventLookup` + `EventAppender`."""
        raise NotImplementedError

    async def test_exists_after_append(self, store: _AppenderLookup) -> None:
        stream = make_stream()
        event = make_event(stream.aggregate_id)
        await store.append(stream, [event], ExpectedVersion.no_stream())

        assert await store.event_exists(event.event_id) is True

    async def test_not_exists_before_append(self, store: _AppenderLookup) -> None:
        event = make_event(uuid4())

        assert await store.event_exists(event.event_id) is False

    async def test_unknown_uuid_returns_false(self, store: _AppenderLookup) -> None:
        assert await store.event_exists(uuid4()) is False
