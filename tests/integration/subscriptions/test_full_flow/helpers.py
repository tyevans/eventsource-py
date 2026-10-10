"""Helper utilities for subscription full flow integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports.event_store import EventStore


async def position_after(store: EventStore, n: int):
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position
