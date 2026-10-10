"""
Catch-up runner for reading historical events from the event store.

The CatchUpRunner reads events in batches from the event store,
delivers them to the subscriber, and persists checkpoints according
to the configured strategy.

This module provides:
- CatchUpResult: Result of a catch-up operation
- CatchUpRunner: Runner for historical event processing
"""

from eventsource.application.subscriptions.runners.catchup_result import (
    CatchUpResult,
    _BatchOutcome,
)
from eventsource.application.subscriptions.runners.catchup_runner import CatchUpRunner

__all__ = [
    "CatchUpResult",
    "CatchUpRunner",
    "_BatchOutcome",
]
