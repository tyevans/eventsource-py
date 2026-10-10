"""Live runner module for real-time event processing.

Backward-compatible facade exporting LiveRunner and related models.
"""

from __future__ import annotations

from eventsource.application.subscriptions.runners.live_models import (
    LiveRunnerStats,
    _LiveEventHandler,
)
from eventsource.application.subscriptions.runners.live_runner import LiveRunner

__all__ = [
    "LiveRunner",
    "LiveRunnerStats",
    "_LiveEventHandler",
]
