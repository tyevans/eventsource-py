"""Fluent builder for creating test domain events with minimal boilerplate."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent


class EventBuilder[TEvent: DomainEvent]:
    """Fluent builder for creating test events with minimal boilerplate."""

    def __init__(self, event_class: type[TEvent], **initial_fields: Any) -> None:
        """Initialize builder for the given DomainEvent subclass."""
        if not (isinstance(event_class, type) and issubclass(event_class, DomainEvent)):
            raise TypeError(f"event_class must be a DomainEvent subclass, got {event_class}")

        self._event_class = event_class
        self._fields: dict[str, Any] = {
            "aggregate_id": uuid4(),
            "aggregate_version": 1,
            **initial_fields,
        }

    def with_aggregate_id(self, aggregate_id: UUID) -> "EventBuilder[TEvent]":
        """Set the aggregate_id field and return self for chaining."""
        self._fields["aggregate_id"] = aggregate_id
        return self

    def with_event_id(self, event_id: UUID) -> "EventBuilder[TEvent]":
        """Set the event_id field and return self for chaining."""
        self._fields["event_id"] = event_id
        return self

    def with_tenant_id(self, tenant_id: UUID) -> "EventBuilder[TEvent]":
        """Set the tenant_id field for multi-tenant scenarios and return self."""
        self._fields["tenant_id"] = tenant_id
        return self

    def with_version(self, version: int) -> "EventBuilder[TEvent]":
        """Set the aggregate_version field and return self for chaining."""
        self._fields["aggregate_version"] = version
        return self

    def with_occurred_at(self, occurred_at: datetime) -> "EventBuilder[TEvent]":
        """Set the occurred_at timestamp and return self for chaining."""
        self._fields["occurred_at"] = occurred_at
        return self

    def with_correlation_id(self, correlation_id: UUID) -> "EventBuilder[TEvent]":
        """Set the correlation_id for event tracking and return self."""
        self._fields["correlation_id"] = correlation_id
        return self

    def with_causation_id(self, causation_id: UUID) -> "EventBuilder[TEvent]":
        """Set the causation_id to link event chains and return self."""
        self._fields["causation_id"] = causation_id
        return self

    def with_actor_id(self, actor_id: str) -> "EventBuilder[TEvent]":
        """Set the actor_id field and return self for chaining."""
        self._fields["actor_id"] = actor_id
        return self

    def with_metadata(self, metadata: dict[str, Any]) -> "EventBuilder[TEvent]":
        """Set the metadata dictionary and return self for chaining."""
        self._fields["metadata"] = metadata
        return self

    def with_field(self, name: str, value: Any) -> "EventBuilder[TEvent]":
        """Set an arbitrary field by name and return self for chaining."""
        self._fields[name] = value
        return self

    def with_fields(self, **kwargs: Any) -> "EventBuilder[TEvent]":
        """Set multiple fields at once and return self for chaining."""
        self._fields.update(kwargs)
        return self

    def clone(self, **overrides: Any) -> "EventBuilder[TEvent]":
        """Return a new EventBuilder copying existing fields with optional overrides."""
        return EventBuilder(self._event_class, **{**self._fields, **overrides})

    def build(self, **overrides: Any) -> TEvent:
        """Build and return a validated event instance with optional field overrides."""
        return self._event_class(**{**self._fields, **overrides})

    def __call__(self, **overrides: Any) -> TEvent:
        """Build and return a validated event instance with optional field overrides."""
        return self.build(**overrides)

    def build_many(
        self,
        count: int,
        *,
        start_version: int | None = None,
    ) -> list[TEvent]:
        """Build and return a list of events with sequential aggregate versions."""
        if count < 0:
            raise ValueError("count must be >= 0")
        if count == 0:
            return []
        base_version = (
            start_version
            if start_version is not None
            else int(self._fields.get("aggregate_version", 1))
        )
        base_fields = (
            {k: v for k, v in self._fields.items() if k != "event_id"}
            if count > 1 and "event_id" in self._fields
            else self._fields
        )
        return [
            self._event_class(**{**base_fields, "aggregate_version": base_version + i})
            for i in range(count)
        ]

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"EventBuilder({self._event_class.__name__}, fields={list(self._fields.keys())})"


__all__ = ["EventBuilder"]
