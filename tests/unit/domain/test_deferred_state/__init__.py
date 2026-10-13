"""Public barrel export facade for test_deferred_state."""

from tests.unit.domain.test_deferred_state._fixtures import (
    ExtractionCompleted,
    ExtractionProcess,
    ExtractionRequested,
    ExtractionState,
    Order,
    OrderCreated,
    OrderShipped,
    OrderState,
)
from tests.unit.domain.test_deferred_state.test_deferred_aggregate import (
    TestDeferredStateAggregate,
    TestDeferredStateAggregateWithReplay,
    TestDeferredStateWithInheritance,
)
from tests.unit.domain.test_deferred_state.test_edge_cases import (
    TestEdgeCases,
    TestStatePropertyConsistency,
)
from tests.unit.domain.test_deferred_state.test_exceptions import (
    TestAggregateNotCreatedError,
    TestAggregateNotCreatedErrorFromAggregate,
)
from tests.unit.domain.test_deferred_state.test_traditional_aggregate import (
    TestBackwardCompatibility,
    TestTraditionalAggregate,
)

__all__ = [
    "ExtractionCompleted",
    "ExtractionProcess",
    "ExtractionRequested",
    "ExtractionState",
    "Order",
    "OrderCreated",
    "OrderShipped",
    "OrderState",
    "TestAggregateNotCreatedError",
    "TestAggregateNotCreatedErrorFromAggregate",
    "TestBackwardCompatibility",
    "TestDeferredStateAggregate",
    "TestDeferredStateAggregateWithReplay",
    "TestDeferredStateWithInheritance",
    "TestEdgeCases",
    "TestStatePropertyConsistency",
    "TestTraditionalAggregate",
]
