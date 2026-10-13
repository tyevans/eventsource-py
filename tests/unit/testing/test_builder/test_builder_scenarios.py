"""Edge case and integration scenario tests for EventBuilder."""

from datetime import UTC, datetime
from uuid import uuid4

from eventsource.testing import EventBuilder
from tests.unit.testing.test_builder._fixtures import SampleEvent


class TestEventBuilderEdgeCases:
    """Edge case tests for EventBuilder."""

    def test_empty_metadata(self) -> None:
        """Builder handles empty metadata dictionary."""
        event = (
            EventBuilder(SampleEvent)
            .with_metadata({})
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )
        assert event.metadata == {}

    def test_field_overwrite_order(self) -> None:
        """Later field settings overwrite earlier ones."""
        customer_id_1 = uuid4()
        customer_id_2 = uuid4()

        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id_1)
            .with_field("customer_id", customer_id_2)
            .with_field("amount", 100.0)
            .build()
        )
        assert event.customer_id == customer_id_2

    def test_with_fields_overwrites_with_field(self) -> None:
        """with_fields can overwrite fields set with with_field."""
        event = (
            EventBuilder(SampleEvent)
            .with_field("amount", 100.0)
            .with_fields(customer_id=uuid4(), amount=200.0)
            .build()
        )
        assert event.amount == 200.0

    def test_zero_amount(self) -> None:
        """Builder handles zero values correctly."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=0.0).build()
        assert event.amount == 0.0

    def test_negative_amount(self) -> None:
        """Builder handles negative values correctly (if event allows)."""
        event = EventBuilder(SampleEvent).with_fields(customer_id=uuid4(), amount=-50.0).build()
        assert event.amount == -50.0

    def test_builder_immutability_not_enforced(self) -> None:
        """Builder is mutable - same builder can create multiple events."""
        builder = EventBuilder(SampleEvent)
        builder.with_fields(customer_id=uuid4(), amount=100.0)

        event1 = builder.build()
        builder.with_field("amount", 200.0)
        event2 = builder.build()

        assert event1.amount == 100.0
        assert event2.amount == 200.0

    def test_none_values_for_optional_fields(self) -> None:
        """Builder can set None for optional fields explicitly."""
        event = (
            EventBuilder(SampleEvent)
            .with_field("tenant_id", None)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )
        assert event.tenant_id is None


class TestEventBuilderIntegrationScenarios:
    """Integration-style tests showing real-world usage patterns."""

    def test_typical_test_event_creation(self) -> None:
        """Typical test scenario: create event with minimal required fields."""
        customer_id = uuid4()
        event = (
            EventBuilder(SampleEvent)
            .with_field("customer_id", customer_id)
            .with_field("amount", 99.99)
            .build()
        )

        assert event.customer_id == customer_id
        assert event.amount == 99.99
        assert event.aggregate_id is not None
        assert event.event_id is not None
        assert event.occurred_at is not None
        assert event.aggregate_version == 1

    def test_event_chain_creation(self) -> None:
        """Create related events with correlation/causation tracking."""
        correlation_id = uuid4()
        agg_id = uuid4()

        event1 = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_correlation_id(correlation_id)
            .with_version(1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        event2 = (
            EventBuilder(SampleEvent)
            .with_aggregate_id(agg_id)
            .with_correlation_id(correlation_id)
            .with_causation_id(event1.event_id)
            .with_version(2)
            .with_fields(customer_id=uuid4(), amount=50.0)
            .build()
        )

        assert event2.correlation_id == event1.correlation_id
        assert event2.causation_id == event1.event_id
        assert event2.aggregate_version > event1.aggregate_version

    def test_multi_tenant_event_creation(self) -> None:
        """Create events for different tenants."""
        tenant_1 = uuid4()
        tenant_2 = uuid4()

        event1 = (
            EventBuilder(SampleEvent)
            .with_tenant_id(tenant_1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        event2 = (
            EventBuilder(SampleEvent)
            .with_tenant_id(tenant_2)
            .with_fields(customer_id=uuid4(), amount=200.0)
            .build()
        )

        assert event1.tenant_id == tenant_1
        assert event2.tenant_id == tenant_2

    def test_historical_event_creation(self) -> None:
        """Create events with specific timestamps for replay testing."""
        past_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC)

        event = (
            EventBuilder(SampleEvent)
            .with_occurred_at(past_time)
            .with_version(1)
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        assert event.occurred_at == past_time

    def test_event_with_full_audit_trail(self) -> None:
        """Create event with complete audit information."""
        event = (
            EventBuilder(SampleEvent)
            .with_actor_id("user:admin@example.com")
            .with_metadata(
                {
                    "ip_address": "192.168.1.1",
                    "user_agent": "TestClient/1.0",
                    "request_id": "req-12345",
                }
            )
            .with_fields(customer_id=uuid4(), amount=100.0)
            .build()
        )

        assert event.actor_id == "user:admin@example.com"
        assert event.metadata["ip_address"] == "192.168.1.1"
        assert event.metadata["user_agent"] == "TestClient/1.0"
        assert event.metadata["request_id"] == "req-12345"


__all__ = [  # pragma: allowlist secret
    "TestEventBuilderEdgeCases",
    "TestEventBuilderIntegrationScenarios",
]
