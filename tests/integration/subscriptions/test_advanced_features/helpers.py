"""Helper classes, events, and functions for advanced features integration tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event
from tests.integration.subscriptions.conftest import (
    SubTestOrderCancelled,
    SubTestOrderCreated,
    SubTestOrderShipped,
)

if TYPE_CHECKING:
    from eventsource.ports.event_store import EventStore


async def position_after(store: EventStore, n: int):
    """The token of the nth (1-based) event in the feed."""
    envelopes = [e async for e in store.read_all()]
    position = envelopes[n - 1].position
    assert position is not None
    return position


class AdvTestUserRegistered(DomainEvent):
    """Test event for user registration in advanced tests."""

    aggregate_type: str = "AdvTestUser"
    username: str = ""


@register_event
class AdvTestUserUpdated(DomainEvent):
    """Test event for user updates in advanced tests."""

    aggregate_type: str = "AdvTestUser"
    field: str = ""


@register_event
class AdvTestPaymentReceived(DomainEvent):
    """Test event for payments in advanced tests."""

    aggregate_type: str = "AdvTestPayment"
    amount: float = 0.0


class FilteredProjection:
    """Projection that only subscribes to specific event types.

    Used for testing event type filtering functionality.
    """

    def __init__(self, event_types: list[type[DomainEvent]] | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self._event_types = event_types or []
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event by collecting it."""
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        """Wait until the specified number of events are collected."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class PausableProjection:
    """Projection that can track pause/resume events.

    Used for testing pause/resume functionality.
    """

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.events_during_pause: list[DomainEvent] = []
        self.is_tracking_pause = False
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event by collecting it."""
        async with self._lock:
            self.events.append(event)
            self.event_count += 1
            if self.is_tracking_pause:
                self.events_during_pause.append(event)

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        """Wait until the specified number of events are collected."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True

    def start_tracking_pause(self) -> None:
        """Start tracking events received during pause."""
        self.is_tracking_pause = True
        self.events_during_pause = []

    def stop_tracking_pause(self) -> None:
        """Stop tracking events received during pause."""
        self.is_tracking_pause = False
