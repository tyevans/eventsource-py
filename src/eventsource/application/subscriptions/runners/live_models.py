"""
Models and helper classes for the LiveRunner.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.domain.event import DomainEvent


@dataclass
class LiveRunnerStats:
    """
    Statistics for live event processing.

    Attributes:
        events_received: Total envelopes read from the global feed while draining
        events_processed: Events successfully processed by subscriber
        events_skipped_filtered: Events skipped due to event type filtering
        events_failed: Events that failed during processing
    """

    events_received: int = 0
    events_processed: int = 0
    events_skipped_filtered: int = 0
    events_failed: int = 0


class _LiveEventHandler:
    """
    Internal handler wrapper for event bus subscription.

    This class wraps a runner or callback to provide a handler interface
    compatible with the EventBus subscription mechanism.
    """

    def __init__(self, runner: Any) -> None:
        """
        Initialize the handler wrapper.

        Args:
            runner: The LiveRunner (or callback) to route events to
        """
        self._runner = runner

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle an event from the event bus.

        Routes the event to the LiveRunner for processing.

        Args:
            event: The event to handle
        """
        await self._runner._handle_live_event(event)


__all__ = [
    "LiveRunnerStats",
    "_LiveEventHandler",
]
