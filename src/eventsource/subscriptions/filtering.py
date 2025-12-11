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
    >>> from eventsource.subscriptions.filtering import EventFilter
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
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent
    from eventsource.subscriptions.config import SubscriptionConfig
    from eventsource.subscriptions.subscriber import Subscriber


logger = logging.getLogger(__name__)


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

    Example:
        >>> # Match specific event types
        >>> filter = EventFilter(event_types=(OrderCreated, OrderShipped))
        >>>
        >>> # Match patterns
        >>> filter = EventFilter(event_type_patterns=("Order*",))
        >>>
        >>> # Match aggregate types
        >>> filter = EventFilter(aggregate_types=("Order", "Payment"))
        >>>
        >>> # Match specific tenant
        >>> from uuid import UUID
        >>> filter = EventFilter(tenant_id=UUID("12345678-1234-5678-1234-567812345678"))
        >>>
        >>> # Combine filters (all must match)
        >>> filter = EventFilter(
        ...     event_type_patterns=("*Created",),
        ...     aggregate_types=("Order",),
        ...     tenant_id=my_tenant_id,
        ... )
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
    _stats: FilterStats = field(default_factory=FilterStats, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize internal sets for fast lookups.

        Note: We only create sets for non-empty tuples. Empty tuples are treated
        as "no filter" (pass-through) rather than "match nothing". This is safer
        as it avoids accidentally filtering all events.
        """
        if self.event_types:
            self._event_type_set = set(self.event_types)
        if self.aggregate_types:
            self._aggregate_type_set = set(self.aggregate_types)

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
        event_types = subscriber.subscribed_to()
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
            # Fall back to subscriber's declared types
            subscribed = subscriber.subscribed_to()
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

        Convenience method for creating pattern-based filters.

        Args:
            *patterns: Wildcard patterns for event type names

        Returns:
            EventFilter instance

        Example:
            >>> filter = EventFilter.from_patterns("Order*", "Payment*")
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

        The filter returns True if:
        - No filters are configured (pass-through), OR
        - The event matches ALL configured criteria

        For a match:
        - event_types: Event type must be in the set
        - event_type_patterns: Event type name must match at least one pattern
        - aggregate_types: Event's aggregate type must be in the set
        - tenant_id: Event's tenant_id must match the configured tenant_id

        Args:
            event: The event to check

        Returns:
            True if the event matches, False otherwise
        """
        # Fast path: no filtering configured
        if not self.is_configured:
            self._stats.record_match()
            return True

        # Check event type matching (exact types)
        if not self._matches_event_type(event):
            self._stats.record_skip()
            return False

        # Check event type patterns
        if not self._matches_event_type_pattern(event):
            self._stats.record_skip()
            return False

        # Check aggregate type
        if not self._matches_aggregate_type(event):
            self._stats.record_skip()
            return False

        # Check tenant ID
        if not self._matches_tenant_id(event):
            self._stats.record_skip()
            return False

        self._stats.record_match()
        return True

    def _matches_event_type(self, event: DomainEvent) -> bool:
        """
        Check if event matches configured event types.

        Args:
            event: The event to check

        Returns:
            True if matches or no event types configured
        """
        if self._event_type_set is None:
            return True
        return type(event) in self._event_type_set

    def _matches_event_type_pattern(self, event: DomainEvent) -> bool:
        """
        Check if event type name matches any configured pattern.

        Uses fnmatch for wildcard pattern matching:
        - "*" matches everything
        - "Order*" matches "OrderCreated", "OrderShipped", etc.
        - "?" matches any single character

        Args:
            event: The event to check

        Returns:
            True if matches any pattern or no patterns configured
        """
        if self.event_type_patterns is None:
            return True

        event_type_name = event.event_type
        for pattern in self.event_type_patterns:
            if fnmatch.fnmatch(event_type_name, pattern):
                return True
        return False

    def _matches_aggregate_type(self, event: DomainEvent) -> bool:
        """
        Check if event's aggregate type is in configured types.

        Args:
            event: The event to check

        Returns:
            True if matches or no aggregate types configured
        """
        if self._aggregate_type_set is None:
            return True
        return event.aggregate_type in self._aggregate_type_set

    def _matches_tenant_id(self, event: DomainEvent) -> bool:
        """
        Check if event's tenant_id matches the configured tenant_id.

        Args:
            event: The event to check

        Returns:
            True if tenant_id matches or no tenant_id configured
        """
        if self.tenant_id is None:
            return True
        return event.tenant_id == self.tenant_id

    @property
    def event_type_names(self) -> list[str] | None:
        """
        Get event type names for query optimization.

        Returns list of event type names from configured event types,
        which can be used to optimize queries at the event store level.

        Returns:
            List of event type name strings, or None if no types configured
        """
        if self._event_type_set is None:
            return None

        names = []
        for event_type in self._event_type_set:
            # Get event_type from class attribute
            if hasattr(event_type, "model_fields"):
                # Pydantic model
                event_type_field = event_type.model_fields.get("event_type")
                if event_type_field and event_type_field.default:
                    names.append(event_type_field.default)

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
    return fnmatch.fnmatch(event_type_name, pattern)


__all__ = [
    "EventFilter",
    "FilterStats",
    "matches_event_type",
    "matches_pattern",
]
