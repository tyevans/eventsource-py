"""
Event filtering for subscriptions.

This module provides filtering capabilities for subscription event delivery,
allowing subscribers to specify which events they want to receive based on:
- Exact event type matching
- Wildcard pattern matching (e.g., "Order*" matches OrderCreated, OrderShipped)
- Aggregate type filtering

The filter is used by both CatchUpRunner and LiveRunner to ensure consistent
filtering behavior across catch-up and live modes.

Example:
    >>> from eventsource.application.subscriptions.filtering import EventFilter
    >>>
    >>> # Create filter from subscriber
    >>> filter = EventFilter.from_subscriber(my_projection)
    >>>
    >>> # Check if event matches
    >>> if filter.matches(event):
    ...     await subscriber.handle(event)
    >>>
    >>> # Pattern matching
    >>> filter = EventFilter(event_type_patterns=("Order*", "Payment*"))
    >>> filter.matches(order_created_event)  # True
    >>> filter.matches(user_registered_event)  # False
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
    Filters events based on type, aggregate, and tenant criteria.

    The EventFilter supports:
    - Exact event type matching via event type classes
    - Pattern matching on event type names (e.g., "Order*")
    - Aggregate type filtering
    - Tenant ID filtering for multi-tenant systems

    Pattern matching uses fnmatch-style patterns:
    - "*" matches everything
    - "Order*" matches "OrderCreated", "OrderShipped", etc.
    - "User*Event" matches "UserCreatedEvent", "UserUpdatedEvent", etc.
    - "?" matches any single character

    If no filters are configured (all None), the filter matches all events.

    Attributes:
        event_types: Exact event type classes to match (optional)
        event_type_patterns: Wildcard patterns for event type names (optional)
        aggregate_types: Aggregate types to match (optional)
        tenant_id: Tenant ID to filter events by (optional).
            When specified, only events belonging to the specified tenant
            are matched. Useful for tenant-specific subscriptions during
            multi-tenant migrations.
    """

    # Exact event type class matching
    event_types: tuple[type[DomainEvent], ...] | None = None

    # Pattern matching on event type names
    event_type_patterns: tuple[str, ...] | None = None

    # Aggregate type matching
    aggregate_types: tuple[str, ...] | None = None

    # Tenant ID filtering
    tenant_id: UUID | None = None

    # Internal state
    _event_type_set: set[type[DomainEvent]] | None = field(default=None, init=False, repr=False)
    _aggregate_type_set: set[str] | None = field(default=None, init=False, repr=False)
    _exact_patterns: frozenset[str] = field(default_factory=frozenset, init=False, repr=False)
    _compiled_patterns: tuple[re.Pattern[str], ...] = field(default=(), init=False, repr=False)
    _has_any_filter: bool = field(default=False, init=False, repr=False)
    _stats: FilterStats = field(default_factory=FilterStats, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize internal sets and pre-compiled patterns for fast lookups.

        Note: We only create sets for non-empty tuples. Empty tuples for
        event_types and aggregate_types are treated as "no filter"
        (pass-through) rather than "match nothing".
        """
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
        """
        Create filter from subscription configuration.

        Args:
            config: Subscription configuration

        Returns:
            EventFilter instance
        """
        return cls(
            event_types=config.event_types,
            aggregate_types=config.aggregate_types,
            tenant_id=config.tenant_id,
        )

    @classmethod
    def from_subscriber(cls, subscriber: Subscriber) -> EventFilter:
        """
        Create filter from subscriber's subscribed_to() method.

        Args:
            subscriber: The subscriber to get event types from

        Returns:
            EventFilter instance
        """
        event_types = get_subscribed_event_types(subscriber)
        if event_types:
            return cls(event_types=tuple(event_types))
        return cls()  # No filtering

    @classmethod
    def from_config_and_subscriber(
        cls,
        config: SubscriptionConfig,
        subscriber: Subscriber,
    ) -> EventFilter:
        """
        Create filter from config, falling back to subscriber.

        The config's event_types takes precedence. If not specified,
        uses the subscriber's subscribed_to() method.

        Args:
            config: Subscription configuration
            subscriber: The subscriber to fall back to

        Returns:
            EventFilter instance
        """
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
        """
        Create filter from event type name patterns.

        Args:
            *patterns: Wildcard patterns for event type names

        Returns:
            EventFilter instance
        """
        return cls(event_type_patterns=patterns)

    @property
    def is_configured(self) -> bool:
        """
        Check if the filter has any criteria configured.

        Returns:
            True if any filter criteria is set
        """
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
        """
        Check if an event matches all filter criteria.

        Args:
            event: The event to check

        Returns:
            True if the event matches, False otherwise
        """
        if not self._has_any_filter:
            self._stats.record_match()
            return True

        if not self._matches_event_type(event):
            self._stats.record_skip()
            return False

        if not self._matches_event_type_pattern(event):
            self._stats.record_skip()
            return False

        if not self._matches_aggregate_type(event):
            self._stats.record_skip()
            return False

        if not self._matches_tenant_id(event):
            self._stats.record_skip()
            return False

        self._stats.record_match()
        return True

    def matches_envelope(self, envelope: EventEnvelope) -> bool:
        """
        Check if an event envelope matches all filter criteria.

        Fast-paths when no filter criteria are active.

        Args:
            envelope: The event envelope to check

        Returns:
            True if the envelope's event matches, False otherwise
        """
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
        """
        Check if event type name matches any configured pattern.

        Checks exact pattern names in O(1) via `_exact_patterns` first, then
        evaluates pre-compiled wildcard regexes in `_compiled_patterns`.
        """
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
        """
        Get event type names for query optimization.

        Returns:
            List of event type name strings, or None if no types configured
        """
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
            type_names = [t.__name__ for t in self.event_types]
            parts.append(f"event_types={type_names}")
        if self.event_type_patterns:
            parts.append(f"patterns={list(self.event_type_patterns)}")
        if self.aggregate_types:
            parts.append(f"aggregates={list(self.aggregate_types)}")
        if self.tenant_id:
            parts.append(f"tenant_id={self.tenant_id}")

        if not parts:
            return "EventFilter(all)"

        return f"EventFilter({', '.join(parts)})"


__all__ = [
    "EventFilter",
    "FilterStats",
    "matches_event_type",
    "matches_pattern",
]
