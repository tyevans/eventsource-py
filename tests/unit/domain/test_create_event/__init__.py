"""Barrel export facade for create_event tests."""

from tests.unit.domain.test_create_event._fixtures import (
    DeclarativeOrderAggregate,
    OrderAggregate,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    OrderState,
)
from tests.unit.domain.test_create_event.test_create_event_aggregate import (
    TestCreateEventWithDeclarativeAggregate,
)
from tests.unit.domain.test_create_event.test_create_event_edge_cases import (
    TestCreateEventEdgeCases,
    TestCreateEventPerformance,
)
from tests.unit.domain.test_create_event.test_create_event_order import (
    TestBackwardCompatibility,
    TestCreateEventApplication,
    TestCreateEventAutoPopulation,
    TestCreateEventOverrides,
    TestCreateEventReturnValue,
    TestCreateEventTenantContext,
)

__all__ = [
    "DeclarativeOrderAggregate",
    "OrderAggregate",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "OrderState",
    "TestBackwardCompatibility",
    "TestCreateEventApplication",
    "TestCreateEventAutoPopulation",
    "TestCreateEventEdgeCases",
    "TestCreateEventOverrides",
    "TestCreateEventPerformance",
    "TestCreateEventReturnValue",
    "TestCreateEventTenantContext",
    "TestCreateEventWithDeclarativeAggregate",
]
