"""Global metrics registry and factory functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.application.subscriptions.metrics_types import reset_meter

if TYPE_CHECKING:
    from eventsource.application.subscriptions.metrics_subscription import (
        SubscriptionMetrics,
    )

# Global metrics registry for tracking all subscription metrics instances
_metrics_registry: dict[str, SubscriptionMetrics] = {}


def get_metrics(
    subscription_name: str,
    enable_metrics: bool = True,
) -> SubscriptionMetrics:
    """
    Get or create metrics instance for a subscription.

    Creates a new SubscriptionMetrics instance if one doesn't exist
    for the given subscription name, or returns the existing one.

    Args:
        subscription_name: Name of the subscription
        enable_metrics: Whether to enable metrics (default True)

    Returns:
        SubscriptionMetrics instance for the subscription
    """
    from eventsource.application.subscriptions.metrics_subscription import (
        SubscriptionMetrics,
    )

    if subscription_name not in _metrics_registry:
        _metrics_registry[subscription_name] = SubscriptionMetrics(
            subscription_name=subscription_name,
            enable_metrics=enable_metrics,
        )
    return _metrics_registry[subscription_name]


def clear_metrics_registry() -> None:
    """
    Clear the metrics registry.

    Useful for testing to reset state between tests.
    """
    _metrics_registry.clear()
    reset_meter()


__all__ = [
    "_metrics_registry",
    "clear_metrics_registry",
    "get_metrics",
]
