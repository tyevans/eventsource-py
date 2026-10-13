"""Unit tests for AggregateRoot and DeclarativeAggregate."""

from __future__ import annotations

from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
    LenientCounterAggregate,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
)
from tests.unit.domain.test_aggregate_root.test_commands import (
    TestCommandMethods,
    TestComplexStateManagement,
    TestLoadFromHistory,
)
from tests.unit.domain.test_aggregate_root.test_declarative import (
    TestDeclarativeAggregate,
    TestStrictUnregisteredDefault,
)
from tests.unit.domain.test_aggregate_root.test_events import (
    TestEventApplication,
    TestIntegrationWithDomainEvent,
    TestRaiseEventMethod,
    TestUncommittedEventManagement,
    TestVersionTracking,
)
from tests.unit.domain.test_aggregate_root.test_exceptions import (
    TestEventVersionErrorException,
    TestUnhandledEventErrorException,
)
from tests.unit.domain.test_aggregate_root.test_initialization import (
    TestAggregateEquality,
    TestAggregateRepr,
    TestAggregateRootInitialization,
    TestAggregateTypeRequired,
    TestEdgeCases,
    TestImmutableStatePatterns,
)
from tests.unit.domain.test_aggregate_root.test_unregistered_events import (
    TestUnregisteredEventHandling,
)
from tests.unit.domain.test_aggregate_root.test_version_validation import (
    TestEventVersionValidation,
)

__all__ = [
    "CounterAggregate",
    "CounterDecremented",
    "CounterIncremented",
    "CounterNamed",
    "CounterReset",
    "CounterState",
    "DeclarativeCounterAggregate",
    "LenientCounterAggregate",
    "OrderAggregate",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "OrderState",
    "TestAggregateEquality",
    "TestAggregateRepr",
    "TestAggregateRootInitialization",
    "TestAggregateTypeRequired",
    "TestCommandMethods",
    "TestComplexStateManagement",
    "TestDeclarativeAggregate",
    "TestEdgeCases",
    "TestEventApplication",
    "TestEventVersionErrorException",
    "TestEventVersionValidation",
    "TestImmutableStatePatterns",
    "TestIntegrationWithDomainEvent",
    "TestLoadFromHistory",
    "TestRaiseEventMethod",
    "TestStrictUnregisteredDefault",
    "TestUncommittedEventManagement",
    "TestUnhandledEventErrorException",
    "TestUnregisteredEventHandling",
    "TestVersionTracking",
]
