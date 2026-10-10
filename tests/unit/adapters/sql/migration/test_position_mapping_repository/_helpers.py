"""Common helpers and fixtures for position mapping repository tests."""

from __future__ import annotations

from eventsource.ports.positions import Position


def pos(n: int, store: str = "src") -> Position:
    """Build a Position for a given store, keyed by a single int."""
    return Position(store_id=store, key=(n,))
