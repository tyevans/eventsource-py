"""Transition coordinator module for catch-up to live event transition.

Backward-compatible facade re-exporting TransitionCoordinator, TransitionPhase,
TransitionResult, and StartFromResolver.
"""

from __future__ import annotations

from eventsource.application.subscriptions.resolver import StartFromResolver
from eventsource.application.subscriptions.transition_coordinator import (
    TransitionCoordinator,
)
from eventsource.application.subscriptions.transition_models import (
    TransitionPhase,
    TransitionResult,
)

__all__ = [
    "StartFromResolver",
    "TransitionCoordinator",
    "TransitionPhase",
    "TransitionResult",
]
