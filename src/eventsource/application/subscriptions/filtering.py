"""
Event filtering for subscription event delivery (exact event types, fnmatch patterns,
aggregate types, and tenant IDs).
"""

from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.filtering_models import (
    FilterStats,
    is_wildcard_pattern,
    matches_event_type,
    matches_pattern,
)
from eventsource.ports.subscribers import get_subscribed_event_types

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.envelopes import EventEnvelope
    from eventsource.ports.subscribers import Subscriber

logger = logging.getLogger(__name__)


@dataclass
class EventFilter:
    """
    Filters events based on event type, fnmatch pattern, aggregate type, and tenant ID.

    If no filters are configured (all None), the filter matches all events.
    """

    event_types: tuple[type[DomainEvent], ...] | None = None
    event_type_patterns: tuple[str, ...] | None = None
    aggregate_types: tuple[str, ...] | None = None
    tenant_id: UUID | None = None

    _event_type_set: set[type[DomainEvent]] | None = field(default=None, init=False, repr=False)
    _aggregate_type_set: set[str] | None = field(default=None, init=False, repr=False)
    _exact_patterns: frozenset[str] = field(default_factory=frozenset, init=False, repr=False)
    _compiled_patterns: tuple[re.Pattern[str], ...] = field(default=(), init=False, repr=False)
    _has_any_filter: bool = field(default=False, init=False, repr=False)
    _stats: FilterStats = field(default_factory=FilterStats, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize internal sets and pre-compiled patterns for fast lookups."""
        if self.event_types:
            self._event_type_set = set(self.event_types)
        if self.aggregate_types:
            self._aggregate_type_set = set(self.aggregate_types)
        if self.event_type_patterns is not None:
            exact: list[str] = []
            compiled: list[re.Pattern[str]] = []
            for pattern in self.event_type_patterns:
                if is_wildcard_pattern(pattern):
                    compiled.append(re.compile(fnmatch.translate(pattern)))
                else:
                    exact.append(pattern)
            self._exact_patterns = frozenset(exact)
            self._compiled_patterns = tuple(compiled)

        self._has_any_filter = bool(
            self._event_type_set is not None
            or self.event_type_patterns is not None
            or self._aggregate_type_set is not None
            or self.tenant_id is not None
        )

    @classmethod
    def from_config(cls, config: SubscriptionConfig) -> EventFilter:
        """Create filter from subscription configuration."""
        return cls(
            event_types=config.event_types,
            aggregate_types=config.aggregate_types,
            tenant_id=config.tenant_id,
        )

    @classmethod
    def from_subscriber(cls, subscriber: Subscriber) -> EventFilter:
        """Create filter from subscriber's subscribed_to() method."""
        event_types = get_subscribed_event_types(subscriber)
        return cls(event_types=tuple(event_types)) if event_types else cls()

    @classmethod
    def from_config_and_subscriber(
        cls,
        config: SubscriptionConfig,
        subscriber: Subscriber,
    ) -> EventFilter:
        """Create filter from config, falling back to subscriber's subscribed_to()."""
        event_types = config.event_types
        if event_types is None:
            subscribed = get_subscribed_event_types(subscriber)
            if subscribed:
                event_types = tuple(subscribed)

        return cls(
            event_types=event_types,
            aggregate_types=config.aggregate_types,
            tenant_id=config.tenant_id,
        )

    @classmethod
    def from_patterns(cls, *patterns: str) -> EventFilter:
        """Create filter from event type name patterns."""
        return cls(event_type_patterns=patterns)

    @property
    def is_configured(self) -> bool:
        """Check if the filter has any criteria configured."""
        return (
            self.event_types is not None
            or self.event_type_patterns is not None
            or self.aggregate_types is not None
            or self.tenant_id is not None
        )

    @property
    def stats(self) -> FilterStats:
        """Get filter statistics."""
        return self._stats

    def matches(self, event: DomainEvent) -> bool:
        """Check if an event matches all filter criteria."""
        if not self._has_any_filter:
            self._stats.record_match()
            return True

        if (
            not self._matches_event_type(event)
            or not self._matches_event_type_pattern(event)
            or not self._matches_aggregate_type(event)
            or not self._matches_tenant_id(event)
        ):
            self._stats.record_skip()
            return False

        self._stats.record_match()
        return True

    def matches_envelope(self, envelope: EventEnvelope) -> bool:
        """Check if an event envelope matches all filter criteria."""
        if not self._has_any_filter:
            self._stats.record_match()
            return True
        return self.matches(envelope.event)

    def _matches_event_type(self, event: DomainEvent) -> bool:
        """Check if event matches configured event types."""
        if self._event_type_set is None:
            return True
        return type(event) in self._event_type_set

    def _matches_event_type_pattern(self, event: DomainEvent) -> bool:
        """Check if event type name matches any configured exact or wildcard pattern."""
        if self.event_type_patterns is None:
            return True

        event_type_name = event.event_type
        if event_type_name in self._exact_patterns:
            return True
        if not self._compiled_patterns:
            return False
        return any(
            pattern.match(event_type_name) is not None for pattern in self._compiled_patterns
        )

    def _matches_aggregate_type(self, event: DomainEvent) -> bool:
        """Check if event's aggregate type is in configured types."""
        if self._aggregate_type_set is None:
            return True
        return event.aggregate_type in self._aggregate_type_set

    def _matches_tenant_id(self, event: DomainEvent) -> bool:
        """Check if event's tenant_id matches the configured tenant_id."""
        if self.tenant_id is None:
            return True
        return event.tenant_id == self.tenant_id

    @property
    def event_type_names(self) -> list[str] | None:
        """Get event type names for query optimization, or None if no types configured."""
        if self._event_type_set is None:
            return None
        names = [event_type.event_type_name() for event_type in self._event_type_set]
        return names if names else None

    def reset_stats(self) -> None:
        """Reset filter statistics."""
        self._stats = FilterStats()

    def __repr__(self) -> str:
        """String representation of the filter."""
        parts = []
        if self.event_types:
            parts.append(f"event_types={[t.__name__ for t in self.event_types]}")
        if self.event_type_patterns:
            parts.append(f"patterns={list(self.event_type_patterns)}")
        if self.aggregate_types:
            parts.append(f"aggregates={list(self.aggregate_types)}")
        if self.tenant_id:
            parts.append(f"tenant_id={self.tenant_id}")

        return f"EventFilter({', '.join(parts)})" if parts else "EventFilter(all)"


__all__ = [
    "EventFilter",
    "FilterStats",
    "matches_event_type",
    "matches_pattern",
]
