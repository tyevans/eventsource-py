"""Unit tests for AggregateRoot snapshot methods."""

from __future__ import annotations

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    DeclarativeOrderAggregate,
    ItemAdded,
    OrderAggregate,
    OrderCreated,
    OrderItem,
    OrderState,
    SimpleAggregate,
    SimpleState,
    StatusChanged,
    ValueSet,
    VersionedAggregate,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_edge_cases import (
    TestEdgeCases,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_restore_from_snapshot import (
    TestRestoreFromSnapshot,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_restore_then_replay import (
    TestRestoreThenReplay,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_round_trip import (
    TestRoundTrip,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_schema_version import (
    TestSchemaVersion,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_serialize_state import (
    TestSerializeState,
)
from tests.unit.domain.test_aggregate_snapshot_methods.test_state_type import (
    TestGetStateType,
)

__all__ = [
    "DeclarativeOrderAggregate",
    "ItemAdded",
    "OrderAggregate",
    "OrderCreated",
    "OrderItem",
    "OrderState",
    "SimpleAggregate",
    "SimpleState",
    "StatusChanged",
    "TestEdgeCases",
    "TestGetStateType",
    "TestRestoreFromSnapshot",
    "TestRestoreThenReplay",
    "TestRoundTrip",
    "TestSchemaVersion",
    "TestSerializeState",
    "ValueSet",
    "VersionedAggregate",
]
