"""Unit tests for AggregateRepository."""

from __future__ import annotations

from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterState,
    MockEventPublisher,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
)
from tests.unit.application.aggregates.test_repository.test_repository_loading import (
    TestCreateNewMethod,
    TestExistsMethod,
    TestGetOrRaise,
    TestGetVersionMethod,
    TestLoadAggregate,
    TestLoadOrCreate,
    TestRepositoryInitialization,
)
from tests.unit.application.aggregates.test_repository.test_repository_mutation import (
    TestEventPublishing,
    TestOptimisticConcurrency,
    TestSaveAggregate,
)
from tests.unit.application.aggregates.test_repository.test_repository_scenarios import (
    TestComplexScenarios,
    TestEdgeCases,
    TestGenericTyping,
    TestImports,
)

__all__ = [
    "CounterAggregate",
    "CounterDecremented",
    "CounterIncremented",
    "CounterState",
    "MockEventPublisher",
    "OrderAggregate",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "OrderState",
    "TestComplexScenarios",  # pragma: allowlist secret
    "TestCreateNewMethod",  # pragma: allowlist secret
    "TestEdgeCases",
    "TestEventPublishing",  # pragma: allowlist secret
    "TestExistsMethod",
    "TestGenericTyping",
    "TestGetOrRaise",
    "TestGetVersionMethod",  # pragma: allowlist secret
    "TestImports",
    "TestLoadAggregate",  # pragma: allowlist secret
    "TestLoadOrCreate",
    "TestOptimisticConcurrency",  # pragma: allowlist secret
    "TestRepositoryInitialization",  # pragma: allowlist secret
    "TestSaveAggregate",  # pragma: allowlist secret
]
