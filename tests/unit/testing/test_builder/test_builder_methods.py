"""Tests for EventBuilder initialization and individual with_* builder methods."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import SampleEvent


class TestEventBuilderInit:
    def test_accepts_domain_event_subclass(self) -> None:
        assert EventBuilder(SampleEvent)._event_class is SampleEvent

    def test_rejects_non_domain_event_class(self) -> None:
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder(dict)  # type: ignore[type-var]

    def test_rejects_non_class(self) -> None:
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder("not a class")  # type: ignore[arg-type]

    def test_rejects_none(self) -> None:
        with pytest.raises(TypeError, match="must be a DomainEvent subclass"):
            EventBuilder(None)  # type: ignore[arg-type]

    def test_rejects_domain_event_base_class(self) -> None:
        assert EventBuilder(DomainEvent)._event_class is DomainEvent

    def test_auto_generates_aggregate_id(self) -> None:
        assert isinstance(EventBuilder(SampleEvent)._fields.get("aggregate_id"), UUID)

    def test_auto_generates_default_version(self) -> None:
        assert EventBuilder(SampleEvent)._fields["aggregate_version"] == 1


class TestEventBuilderWithAggregateId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_aggregate_id(uuid4()) is builder

    def test_sets_aggregate_id(self) -> None:
        agg_id = uuid4()
        assert EventBuilder(SampleEvent).with_aggregate_id(agg_id)._fields["aggregate_id"] == agg_id

    def test_overwrites_auto_generated_id(self) -> None:
        builder = EventBuilder(SampleEvent)
        original_id, new_id = builder._fields["aggregate_id"], uuid4()
        assert builder.with_aggregate_id(new_id)._fields["aggregate_id"] == new_id != original_id


class TestEventBuilderWithEventId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_event_id(uuid4()) is builder

    def test_sets_event_id(self) -> None:
        event_id = uuid4()
        assert EventBuilder(SampleEvent).with_event_id(event_id)._fields["event_id"] == event_id


class TestEventBuilderWithTenantId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_tenant_id(uuid4()) is builder

    def test_sets_tenant_id(self) -> None:
        tenant_id = uuid4()
        assert EventBuilder(SampleEvent).with_tenant_id(tenant_id)._fields["tenant_id"] == tenant_id


class TestEventBuilderWithVersion:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_version(5) is builder

    def test_sets_aggregate_version(self) -> None:
        assert EventBuilder(SampleEvent).with_version(42)._fields["aggregate_version"] == 42

    def test_overwrites_default_version(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder._fields["aggregate_version"] == 1
        assert builder.with_version(10)._fields["aggregate_version"] == 10


class TestEventBuilderWithOccurredAt:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_occurred_at(datetime.now(UTC)) is builder

    def test_sets_occurred_at(self) -> None:
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert (
            EventBuilder(SampleEvent).with_occurred_at(timestamp)._fields["occurred_at"]
            == timestamp
        )


class TestEventBuilderWithCorrelationId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_correlation_id(uuid4()) is builder

    def test_sets_correlation_id(self) -> None:
        correlation_id = uuid4()
        builder = EventBuilder(SampleEvent).with_correlation_id(correlation_id)
        assert builder._fields["correlation_id"] == correlation_id


class TestEventBuilderWithCausationId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_causation_id(uuid4()) is builder

    def test_sets_causation_id(self) -> None:
        causation_id = uuid4()
        builder = EventBuilder(SampleEvent).with_causation_id(causation_id)
        assert builder._fields["causation_id"] == causation_id


class TestEventBuilderWithActorId:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_actor_id("user-123") is builder

    def test_sets_actor_id(self) -> None:
        assert (
            EventBuilder(SampleEvent).with_actor_id("system:cron")._fields["actor_id"]
            == "system:cron"
        )


class TestEventBuilderWithMetadata:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_metadata({"key": "value"}) is builder

    def test_sets_metadata(self) -> None:
        metadata = {"trace_id": "abc123", "source": "api"}
        assert EventBuilder(SampleEvent).with_metadata(metadata)._fields["metadata"] == metadata

    def test_overwrites_existing_metadata(self) -> None:
        builder = EventBuilder(SampleEvent).with_metadata({"key1": "value1"})
        assert builder.with_metadata({"key2": "value2"})._fields["metadata"] == {"key2": "value2"}


class TestEventBuilderWithField:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_field("customer_id", uuid4()) is builder

    def test_sets_arbitrary_field(self) -> None:
        customer_id = uuid4()
        builder = EventBuilder(SampleEvent).with_field("customer_id", customer_id)
        assert builder._fields["customer_id"] == customer_id

    def test_sets_multiple_fields_separately(self) -> None:
        customer_id = uuid4()
        builder = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id)
            .with_field("amount", 99.99)
        )
        assert builder._fields["customer_id"] == customer_id and builder._fields["amount"] == 99.99

    def test_overwrites_existing_field(self) -> None:
        builder = EventBuilder(SampleEvent).with_field("amount", 100.0).with_field("amount", 200.0)
        assert builder._fields["amount"] == 200.0


class TestEventBuilderWithFields:
    def test_returns_self(self) -> None:
        builder = EventBuilder(SampleEvent)
        assert builder.with_fields(customer_id=uuid4(), amount=50.0) is builder

    def test_sets_multiple_fields(self) -> None:
        customer_id = uuid4()
        builder = EventBuilder(SampleEvent).with_fields(customer_id=customer_id, amount=75.50)
        assert builder._fields["customer_id"] == customer_id and builder._fields["amount"] == 75.50

    def test_overwrites_existing_fields(self) -> None:
        builder = EventBuilder(SampleEvent).with_field("amount", 100.0)
        assert builder.with_fields(amount=200.0, customer_id=uuid4())._fields["amount"] == 200.0


__all__ = [  # pragma: allowlist secret
    "TestEventBuilderInit",
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
