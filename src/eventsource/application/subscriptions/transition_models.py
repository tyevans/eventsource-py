"""
Data models and phases for subscription transitions.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- TASK-0006 (Reconcile Dropped Live Events on Transition)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports.positions import Position


class TransitionPhase(Enum):
    """
    Phases of the catch-up to live transition.

    The transition follows this sequence:
    1. NOT_STARTED: Initial state
    2. INITIAL_CATCHUP: Getting watermark and preparing
    3. LIVE_SUBSCRIBED: Live runner started in buffer mode
    4. FINAL_CATCHUP: Catching up to watermark position
    5. PROCESSING_BUFFER: Processing buffered live events
    6. LIVE: Now processing live events directly
    7. FAILED: Transition failed with error
    """

    NOT_STARTED = "not_started"
    INITIAL_CATCHUP = "initial_catchup"
    LIVE_SUBSCRIBED = "live_subscribed"
    FINAL_CATCHUP = "final_catchup"
    PROCESSING_BUFFER = "processing_buffer"
    LIVE = "live"
    FAILED = "failed"


@dataclass(frozen=True)
class TransitionResult:
    """
    Result of a transition operation.

    Provides statistics and outcome information for the catch-up
    to live transition.

    Attributes:
        success: True if transition completed successfully
        catchup_events_processed: Number of events processed during catch-up
        buffer_events_processed: Number of events processed from the global
            feed once buffering ends (everything past the catch-up watermark)
        final_position: Last processed global-feed position, None if nothing
            has been processed
        phase_reached: The phase reached when transition ended
        error: Exception if transition failed, None otherwise
    """

    success: bool
    catchup_events_processed: int
    buffer_events_processed: int
    final_position: Position | None
    phase_reached: TransitionPhase
    error: Exception | None = None


__all__ = [
    "TransitionPhase",
    "TransitionResult",
]
