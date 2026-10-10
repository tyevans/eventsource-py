"""Common helpers for checkpoint position tests."""

from __future__ import annotations

from eventsource.ports.positions import Position


def pos(n: int) -> Position:
    """Build a token for a test store, standing in for an int position."""
    return Position(store_id="test", key=(n,))
