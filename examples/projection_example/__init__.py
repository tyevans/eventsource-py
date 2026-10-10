"""
Projection Example with SubscriptionManager

This example demonstrates:
- Building read models (projections) from event streams
- SubscriptionManager for coordinated catch-up and live subscriptions
- Checkpoint tracking for resumable processing
- Multiple projections with different read models

The SubscriptionManager is the recommended pattern for production use as it:
- Catches up on historical events from the event store
- Seamlessly transitions to live events from the event bus
- Tracks checkpoints for resumable processing
- Supports multiple concurrent projections

Run with: python -m examples.projection_example
"""

from __future__ import annotations

from examples.projection_example.domain import (
    OrderAggregate,
    OrderCancelled,
    OrderDelivered,
    OrderPlaced,
    OrderShipped,
    OrderState,
)
from examples.projection_example.projections import (
    CustomerStatsProjection,
    DailyRevenueProjection,
    OrderListProjection,
)
from examples.projection_example.runner import main

__all__ = [
    "CustomerStatsProjection",
    "DailyRevenueProjection",
    "OrderAggregate",
    "OrderCancelled",
    "OrderDelivered",
    "OrderListProjection",
    "OrderPlaced",
    "OrderShipped",
    "OrderState",
    "main",
]
