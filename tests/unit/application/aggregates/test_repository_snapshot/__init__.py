"""Unit tests for AggregateRepository snapshot functionality."""

from __future__ import annotations

from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    CountEvent,
    TestAggregate,
    TestAggregateV2,
    TestEvent,
    TestState,
)
from tests.unit.application.aggregates.test_repository_snapshot.test_repository_snapshot_auto import (
    TestRepositoryAutoSnapshot,
    TestRepositoryManualSnapshot,
)
from tests.unit.application.aggregates.test_repository_snapshot.test_repository_snapshot_background import (
    TestRepositoryBackgroundSnapshot,
    TestSnapshotPolicyLogic,
)
from tests.unit.application.aggregates.test_repository_snapshot.test_repository_snapshot_integration import (
    TestEdgeCases,
    TestFullCycleIntegration,
)
from tests.unit.application.aggregates.test_repository_snapshot.test_repository_snapshot_load import (
    TestRepositoryConstructor,
    TestRepositorySnapshotLoad,
)

__all__ = [
    "CountEvent",
    "TestAggregate",
    "TestAggregateV2",
    "TestEdgeCases",
    "TestEvent",
    "TestFullCycleIntegration",  # pragma: allowlist secret
    "TestRepositoryAutoSnapshot",  # pragma: allowlist secret
    "TestRepositoryBackgroundSnapshot",  # pragma: allowlist secret
    "TestRepositoryConstructor",  # pragma: allowlist secret
    "TestRepositoryManualSnapshot",  # pragma: allowlist secret
    "TestRepositorySnapshotLoad",  # pragma: allowlist secret
    "TestSnapshotPolicyLogic",  # pragma: allowlist secret
    "TestState",
]
