"""Subscriber protocols: the contract event subscribers implement for subscription management."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


@runtime_checkable
class Subscriber(Protocol):
    """Protocol for async event subscribers."""

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        ...

    async def handle(self, event: "DomainEvent") -> None:
        """Handle a single domain event asynchronously."""
        ...


@runtime_checkable
class SyncSubscriber(Protocol):
    """Protocol for synchronous event subscribers."""

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        ...

    def handle(self, event: "DomainEvent") -> None:
        """Handle a single domain event synchronously."""
        ...


@runtime_checkable
class BatchSubscriber(Protocol):
    """Protocol for subscribers that support batch event processing."""

    def subscribed_to(self) -> list[type["DomainEvent"]]:
        """Return list of event types this subscriber handles."""
        ...

    async def handle_batch(self, events: Sequence["DomainEvent"]) -> None:
        """Handle a batch of domain events asynchronously."""
        ...


def supports_batch_handling(subscriber: object) -> bool:
    """Check if a subscriber has a callable handle_batch method."""
    return hasattr(subscriber, "handle_batch") and callable(
        getattr(subscriber, "handle_batch", None)
    )


def get_subscribed_event_types(subscriber: Subscriber) -> list[type["DomainEvent"]]:
    """Get and validate the event types a subscriber handles."""
    event_types = subscriber.subscribed_to()
    if not isinstance(event_types, list):
        raise ValueError(f"subscribed_to() must return a list, got {type(event_types).__name__}")
    return event_types


__all__ = [
    "Subscriber",
    "SyncSubscriber",
    "BatchSubscriber",
    "supports_batch_handling",
    "get_subscribed_event_types",
]
