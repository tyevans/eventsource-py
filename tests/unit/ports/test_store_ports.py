"""Tests for the five store port protocols and the collect helper."""

import pytest

from eventsource.ports import collect
from eventsource.ports.store import (
    CategoryQuery,
    EventAppender,
    EventLookup,
    FullEventStore,
    GlobalEventFeed,
    StreamReader,
)


async def _agen(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_collect_drains_iterator() -> None:
    assert await collect(_agen([1, 2, 3])) == [1, 2, 3]


def test_ports_are_importable_and_distinct() -> None:
    # NOTE: adjusted from the brief's `issubclass(FullEventStore, EventAppender)`.
    # These are plain (non-@runtime_checkable) Protocols, so typing special-cases
    # issubclass() against them to always raise TypeError, even for a class that
    # genuinely inherits from the protocol in its MRO. We verify the composition
    # statically instead: the classes are distinct objects, and FullEventStore's
    # MRO actually includes each component protocol.
    ports = {EventAppender, StreamReader, EventLookup, GlobalEventFeed, CategoryQuery}
    assert len(ports) == 5
    assert EventAppender in FullEventStore.__mro__
    assert StreamReader in FullEventStore.__mro__
    assert EventLookup in FullEventStore.__mro__
    assert GlobalEventFeed in FullEventStore.__mro__
    assert CategoryQuery in FullEventStore.__mro__
