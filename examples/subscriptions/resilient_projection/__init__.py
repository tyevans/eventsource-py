"""Resilient Subscription Manager Example - Production-Ready Projection."""

from .domain import (
    PaymentAggregate,
    PaymentFailed,
    PaymentReceived,
    PaymentRefunded,
    PaymentState,
)
from .projection import (
    PaymentAnalyticsProjection,
    on_any_error,
    on_critical_error,
    on_transient_error,
)
from .runner import main

__all__ = [
    "PaymentAggregate",
    "PaymentAnalyticsProjection",
    "PaymentFailed",
    "PaymentReceived",
    "PaymentRefunded",
    "PaymentState",
    "main",
    "on_any_error",
    "on_critical_error",
    "on_transient_error",
]
