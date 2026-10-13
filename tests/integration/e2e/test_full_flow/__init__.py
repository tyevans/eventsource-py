"""End-to-end integration tests for complete event sourcing workflows."""

from __future__ import annotations

from ._models import OrderSummary
from .test_order_projection import (
    TestCommandToProjectionFlow,
    TestOrderProjection,
)
from .test_rehydration_and_locking import (
    TestAggregateRehydration,
    TestMultiAggregateScenarios,
    TestOptimisticLocking,
)
from .test_streams_and_outbox import (
    TestEventStreamOperations,
    TestOutboxIntegration,
    TestProjectionRebuilding,
)

__all__ = [  # pragma: allowlist secret
    "OrderSummary",
    "TestAggregateRehydration",
    "TestCommandToProjectionFlow",
    "TestEventStreamOperations",
    "TestMultiAggregateScenarios",
    "TestOptimisticLocking",
    "TestOrderProjection",
    "TestOutboxIntegration",
    "TestProjectionRebuilding",
]
