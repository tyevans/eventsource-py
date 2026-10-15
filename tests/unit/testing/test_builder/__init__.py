"""Unit tests for EventBuilder modular package."""

from tests.unit.testing.test_builder._fixtures import (
    MinimalEvent,
    SampleEvent,
)
from tests.unit.testing.test_builder.test_builder_build import (
    TestEventBuilderBuild,
)
from tests.unit.testing.test_builder.test_builder_chaining import (
    TestEventBuilderClone,
    TestEventBuilderMethodChaining,
    TestEventBuilderRepr,
    TestEventBuilderTypeSafety,
)
from tests.unit.testing.test_builder.test_builder_methods import (
    TestEventBuilderInit,
    TestEventBuilderWithActorId,
    TestEventBuilderWithAggregateId,
    TestEventBuilderWithCausationId,
    TestEventBuilderWithCorrelationId,
    TestEventBuilderWithEventId,
    TestEventBuilderWithField,
    TestEventBuilderWithFields,
    TestEventBuilderWithMetadata,
    TestEventBuilderWithOccurredAt,
    TestEventBuilderWithTenantId,
    TestEventBuilderWithVersion,
)
from tests.unit.testing.test_builder.test_builder_scenarios import (
    TestEventBuilderEdgeCases,
    TestEventBuilderIntegrationScenarios,
)

__all__ = [  # pragma: allowlist secret
    "MinimalEvent",
    "SampleEvent",
    "TestEventBuilderBuild",
    "TestEventBuilderClone",
    "TestEventBuilderEdgeCases",
    "TestEventBuilderInit",
    "TestEventBuilderIntegrationScenarios",
    "TestEventBuilderMethodChaining",
    "TestEventBuilderRepr",
    "TestEventBuilderTypeSafety",
    "TestEventBuilderWithActorId",
    "TestEventBuilderWithAggregateId",
    "TestEventBuilderWithCausationId",
    "TestEventBuilderWithCorrelationId",
    "TestEventBuilderWithEventId",
    "TestEventBuilderWithField",
    "TestEventBuilderWithFields",
    "TestEventBuilderWithMetadata",
    "TestEventBuilderWithOccurredAt",
    "TestEventBuilderWithTenantId",
    "TestEventBuilderWithVersion",
]
