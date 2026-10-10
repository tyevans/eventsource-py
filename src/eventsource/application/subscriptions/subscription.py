"""Subscription module managing state, position tracking, and statistics.

Backward-compatible facade re-exporting Subscription and related models.
"""

from __future__ import annotations

from eventsource.application.subscriptions.models import (
    VALID_TRANSITIONS,
    BatchHandler,
    EventHandler,
    PauseReason,
    RecentErrorInfo,
    SubscriptionState,
    SubscriptionStatus,
    is_valid_transition,
    render_position,
)
from eventsource.application.subscriptions.subscription_core import Subscription

__all__ = [
    "render_position",
    "SubscriptionState",
    "SubscriptionStatus",
    "Subscription",
    "is_valid_transition",
    "VALID_TRANSITIONS",
    "EventHandler",
    "BatchHandler",
    "RecentErrorInfo",
    "PauseReason",
]
