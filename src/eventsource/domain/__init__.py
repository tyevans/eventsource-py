"""Entities ring. Pure: stdlib + pydantic only.

TRANSITION: DomainEvent/EventRegistry still live in eventsource.events and
count as this ring until sub-project 3 moves them.
"""

from eventsource.domain.stream_id import CATEGORY_PATTERN, StreamId

__all__ = ["CATEGORY_PATTERN", "StreamId"]
