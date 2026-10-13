"""Tests for aggregate schema_version attribute."""

from uuid import uuid4

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    OrderAggregate,
    SimpleAggregate,
    SimpleState,
    VersionedAggregate,
)


class TestSchemaVersion:
    """Test schema_version class attribute behavior."""

    def test_default_schema_version_is_one(self) -> None:
        """Default schema_version should be 1 when not explicitly set."""
        assert SimpleAggregate.schema_version == 1

    def test_schema_version_can_be_overridden(self) -> None:
        """Custom schema_version should be respected."""
        assert VersionedAggregate.schema_version == 5
        assert OrderAggregate.schema_version == 1

    def test_schema_version_accessible_on_instance(self) -> None:
        """schema_version should be accessible on aggregate instances."""
        aggregate = OrderAggregate(uuid4())
        assert aggregate.schema_version == 1

        versioned = VersionedAggregate(uuid4())
        assert versioned.schema_version == 5

    def test_different_aggregates_have_independent_versions(self) -> None:
        """Each aggregate class should have its own schema_version."""
        # SimpleAggregate uses default (1)
        # VersionedAggregate explicitly sets 5
        # OrderAggregate explicitly sets 1
        assert SimpleAggregate.schema_version == 1
        assert VersionedAggregate.schema_version == 5
        assert OrderAggregate.schema_version == 1

        # Modifying one should not affect others
        class TestAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Test"
            schema_version = 99

            def _get_initial_state(self) -> SimpleState:
                return SimpleState()

            def _apply(self, event: DomainEvent) -> None:
                pass

        assert TestAggregate.schema_version == 99
        assert SimpleAggregate.schema_version == 1  # Unchanged
