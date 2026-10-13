"""Sample event models and fixtures for EventBuilder tests."""

from uuid import UUID

from eventsource.domain.event import DomainEvent


class SampleEvent(DomainEvent):
    """Sample event for testing EventBuilder."""

    aggregate_type: str = "Sample"
    customer_id: UUID
    amount: float


class MinimalEvent(DomainEvent):
    """Minimal event with only required base fields."""

    aggregate_type: str = "Minimal"


__all__ = [  # pragma: allowlist secret
    "MinimalEvent",
    "SampleEvent",
]
