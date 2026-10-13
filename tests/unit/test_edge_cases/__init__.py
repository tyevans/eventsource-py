"""Unit tests for edge cases across various components."""

from __future__ import annotations

from tests.unit.test_edge_cases.test_edge_cases_concurrency import (
    TestConcurrentAccess,
    TestSerializationEdgeCases,
)
from tests.unit.test_edge_cases.test_edge_cases_events import (
    EdgeTestEvent,
    SampleOrderEvent,
    TestInMemoryEventBusEdgeCases,
    TestInMemoryEventStoreEdgeCases,
)
from tests.unit.test_edge_cases.test_edge_cases_repositories import (
    TestCheckpointRepositoryEdgeCases,
    TestDLQRepositoryEdgeCases,
    TestOutboxRepositoryEdgeCases,
)

__all__ = [  # pragma: allowlist secret
    "EdgeTestEvent",
    "SampleOrderEvent",
    "TestCheckpointRepositoryEdgeCases",
    "TestConcurrentAccess",
    "TestDLQRepositoryEdgeCases",
    "TestInMemoryEventBusEdgeCases",
    "TestInMemoryEventStoreEdgeCases",
    "TestOutboxRepositoryEdgeCases",
    "TestSerializationEdgeCases",
]
