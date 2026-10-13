"""
Models and utility functions for subscription event filtering.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent

_WILDCARD_CHARS = frozenset("*?[")


def is_wildcard_pattern(pattern: str) -> bool:
    """Return True if the pattern contains fnmatch wildcard characters."""
    return any(ch in _WILDCARD_CHARS for ch in pattern)


@dataclass
class FilterStats:
    """
    Statistics for event filtering.

    Tracks how many events were evaluated and how many matched/skipped
    to help with debugging and monitoring filter effectiveness.

    Attributes:
        events_evaluated: Total events checked against filter
        events_matched: Events that passed the filter
        events_skipped: Events that were filtered out
    """

    events_evaluated: int = 0
    events_matched: int = 0
    events_skipped: int = 0

    @property
    def match_rate(self) -> float:
        """
        Calculate the match rate as a percentage.

        Returns:
            Percentage of events that matched (0.0 to 1.0)
        """
        if self.events_evaluated == 0:
            return 1.0
        return self.events_matched / self.events_evaluated

    def record_match(self) -> None:
        """Record that an event matched the filter."""
        self.events_evaluated += 1
        self.events_matched += 1

    def record_skip(self) -> None:
        """Record that an event was filtered out."""
        self.events_evaluated += 1
        self.events_skipped += 1

    def to_dict(self) -> dict[str, int | float]:
        """Convert to dictionary for serialization."""
        return {
            "events_evaluated": self.events_evaluated,
            "events_matched": self.events_matched,
            "events_skipped": self.events_skipped,
            "match_rate": round(self.match_rate, 4),
        }


def matches_event_type(
    event: DomainEvent,
    event_types: tuple[type[DomainEvent], ...] | None,
) -> bool:
    """
    Utility function to check if event matches given types.

    This is a standalone function for cases where creating a full
    EventFilter is overkill.

    Args:
        event: The event to check
        event_types: Event types to match, or None for all

    Returns:
        True if event type matches or no types specified
    """
    if event_types is None:
        return True
    return type(event) in event_types


def matches_pattern(event_type_name: str, pattern: str) -> bool:
    """
    Check if an event type name matches a wildcard pattern.

    Uses fnmatch-style pattern matching.

    Args:
        event_type_name: The event type name to check
        pattern: The wildcard pattern

    Returns:
        True if the name matches the pattern
    """
    if not is_wildcard_pattern(pattern):
        return event_type_name == pattern
    return fnmatch.fnmatch(event_type_name, pattern)


__all__ = [
    "FilterStats",
    "is_wildcard_pattern",
    "matches_event_type",
    "matches_pattern",
]
