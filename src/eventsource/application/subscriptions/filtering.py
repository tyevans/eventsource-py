"""Event filtering for subscription event delivery (event types, patterns, aggregates, tenants)."""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.subscriptions.filtering_models import (
    FilterStats,
    is_wildcard_pattern,
    matches_event_type,
    matches_pattern,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.subscribers import get_subscribed_event_types

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.ports.envelopes import EventEnvelope
    from eventsource.ports.subscribers import Subscriber


@dataclass
class EventFilter:
    """Filters events based on event type, fnmatch pattern, aggregate type, and tenant ID."""

    event_types: Iterable[type[DomainEvent]] | None = None
    event_type_patterns: Iterable[str] | None = None
    aggregate_types: Iterable[str] | None = None
    tenant_id: UUID | None = None

    _event_type_set: set[type[DomainEvent]] | None = field(default=None, init=False, repr=False)
    _aggregate_type_set: set[str] | None = field(default=None, init=False, repr=False)
    _exact_patterns: frozenset[str] = field(default_factory=frozenset, init=False, repr=False)
    _compiled_patterns: tuple[re.Pattern[str], ...] = field(default=(), init=False, repr=False)
    _has_any_filter: bool = field(default=False, init=False, repr=False)
    _stats: FilterStats = field(default_factory=FilterStats, init=False, repr=False)

    def __post_init__(self) -> None:
        for attr in ("event_types", "event_type_patterns", "aggregate_types"):
            val = getattr(self, attr)
            if val is not None and not isinstance(val, tuple):
                setattr(self, attr, tuple(val))

        if self.event_types:
            self._event_type_set = set(self.event_types)
        if self.aggregate_types:
            self._aggregate_type_set = set(self.aggregate_types)
        if self.event_type_patterns is not None:
            pats = self.event_type_patterns
            self._exact_patterns = frozenset(p for p in pats if not is_wildcard_pattern(p))
            self._compiled_patterns = tuple(
                re.compile(fnmatch.translate(p)) for p in pats if is_wildcard_pattern(p)
            )
        self._has_any_filter = any(x is not None for x in (self._event_type_set, self.event_type_patterns, self._aggregate_type_set, self.tenant_id))  # fmt: skip

    @classmethod
    def from_config(cls, config: SubscriptionConfig) -> EventFilter:
        return cls(config.event_types, aggregate_types=config.aggregate_types, tenant_id=config.tenant_id)  # fmt: skip

    @classmethod
    def from_subscriber(cls, subscriber: Subscriber) -> EventFilter:
        event_types = get_subscribed_event_types(subscriber)
        return cls(event_types=tuple(event_types)) if event_types else cls()

    @classmethod
    def from_config_and_subscriber(
        cls, config: SubscriptionConfig, subscriber: Subscriber
    ) -> EventFilter:
        event_types = config.event_types
        if event_types is None:
            subscribed = get_subscribed_event_types(subscriber)
            event_types = tuple(subscribed) if subscribed else None
        return cls(event_types, aggregate_types=config.aggregate_types, tenant_id=config.tenant_id)

    @classmethod
    def from_patterns(cls, *patterns: str) -> EventFilter:
        return cls(event_type_patterns=patterns)

    @property
    def is_configured(self) -> bool:
        return any(x is not None for x in (self.event_types, self.event_type_patterns, self.aggregate_types, self.tenant_id))  # fmt: skip

    @property
    def stats(self) -> FilterStats:
        return self._stats

    def matches(self, event: DomainEvent) -> bool:
        matched = not self._has_any_filter or (
            self._matches_event_type(event)
            and self._matches_event_type_pattern(event)
            and self._matches_aggregate_type(event)
            and self._matches_tenant_id(event)
        )
        if matched:
            self._stats.record_match()
        else:
            self._stats.record_skip()
        return matched

    def matches_envelope(self, envelope: EventEnvelope) -> bool:
        return self.matches(envelope.event)

    def __call__(self, item: DomainEvent | EventEnvelope) -> bool:
        return self.matches(item) if isinstance(item, DomainEvent) else self.matches_envelope(item)

    def filter[TItem: (DomainEvent, EventEnvelope)](self, items: Iterable[TItem]) -> list[TItem]:
        return [item for item in items if self(item)]

    def _matches_event_type(self, event: DomainEvent) -> bool:
        return self._event_type_set is None or type(event) in self._event_type_set

    def _matches_event_type_pattern(self, event: DomainEvent) -> bool:
        if self.event_type_patterns is None:
            return True
        name = event.event_type
        return name in self._exact_patterns or any(p.match(name) for p in self._compiled_patterns)

    def _matches_aggregate_type(self, event: DomainEvent) -> bool:
        return self._aggregate_type_set is None or event.aggregate_type in self._aggregate_type_set

    def _matches_tenant_id(self, event: DomainEvent) -> bool:
        return self.tenant_id is None or event.tenant_id == self.tenant_id

    @property
    def event_type_names(self) -> list[str] | None:
        return (
            [et.event_type_name() for et in self._event_type_set] if self._event_type_set else None
        )

    def reset_stats(self) -> None:
        self._stats = FilterStats()

    def __repr__(self) -> str:
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


__all__ = ["EventFilter", "FilterStats", "matches_event_type", "matches_pattern"]
