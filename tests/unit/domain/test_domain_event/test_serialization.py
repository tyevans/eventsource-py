"""Tests for domain event serialization and multi-tenancy."""

import json
from datetime import UTC, datetime
from uuid import uuid4

from tests.unit.domain.test_domain_event._fixtures import OrderCreated


class TestDomainEventSerialization:
    """Tests for event serialization and deserialization."""

    def test_to_dict_returns_json_serializable(self) -> None:
        """to_dict() returns a JSON-serializable dictionary."""
        aggregate_id = uuid4()
        customer_id = uuid4()

        event = OrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
            total_amount=99.99,
        )

        data = event.to_dict()

        # Should be serializable to JSON
        json_str = json.dumps(data)
        assert isinstance(json_str, str)

        # UUIDs should be strings
        assert isinstance(data["event_id"], str)
        assert isinstance(data["aggregate_id"], str)
        assert isinstance(data["correlation_id"], str)
        assert data["aggregate_id"] == str(aggregate_id)

        # Datetime should be ISO string
        assert isinstance(data["occurred_at"], str)

    def test_from_dict_creates_event(self) -> None:
        """from_dict() creates event from dictionary."""
        aggregate_id = uuid4()
        customer_id = uuid4()
        event_id = uuid4()
        correlation_id = uuid4()
        occurred_at = datetime.now(UTC)

        data = {
            "event_id": str(event_id),
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": occurred_at.isoformat(),
            "aggregate_id": str(aggregate_id),
            "aggregate_type": "Order",
            "aggregate_version": 1,
            "tenant_id": None,
            "actor_id": "user:123",
            "correlation_id": str(correlation_id),
            "causation_id": None,
            "metadata": {},
            "order_number": "ORD-001",
            "customer_id": str(customer_id),
            "total_amount": 99.99,
        }

        event = OrderCreated.from_dict(data)

        assert event.event_id == event_id
        assert event.aggregate_id == aggregate_id
        assert event.order_number == "ORD-001"
        assert event.customer_id == customer_id
        assert event.actor_id == "user:123"

    def test_serialization_roundtrip(self) -> None:
        """Event survives serialization roundtrip."""
        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=uuid4(),
            actor_id="user:123",
            metadata={"trace_id": "abc123"},
        )

        # Serialize to dict
        data = original.to_dict()

        # Deserialize back
        restored = OrderCreated.from_dict(data)

        # All fields should match
        assert restored.event_id == original.event_id
        assert restored.event_type == original.event_type
        assert restored.aggregate_id == original.aggregate_id
        assert restored.aggregate_type == original.aggregate_type
        assert restored.order_number == original.order_number
        assert restored.customer_id == original.customer_id
        assert restored.total_amount == original.total_amount
        assert restored.tenant_id == original.tenant_id
        assert restored.actor_id == original.actor_id
        assert restored.correlation_id == original.correlation_id
        assert restored.causation_id == original.causation_id
        assert restored.metadata == original.metadata

    def test_json_roundtrip(self) -> None:
        """Event survives full JSON roundtrip."""
        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        # Full JSON roundtrip
        json_str = json.dumps(original.to_dict())
        data = json.loads(json_str)
        restored = OrderCreated.from_dict(data)

        assert restored.event_id == original.event_id
        assert restored.order_number == original.order_number


class TestDomainEventMultiTenancy:
    """Tests for multi-tenancy support."""

    def test_tenant_id_is_optional(self) -> None:
        """tenant_id can be omitted for single-tenant use."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        assert event.tenant_id is None

    def test_tenant_id_can_be_set(self) -> None:
        """tenant_id can be set for multi-tenant use."""
        tenant_id = uuid4()

        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=tenant_id,
        )

        assert event.tenant_id == tenant_id

    def test_tenant_id_survives_serialization(self) -> None:
        """tenant_id is properly serialized and deserialized."""
        tenant_id = uuid4()

        original = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            tenant_id=tenant_id,
        )

        data = original.to_dict()
        restored = OrderCreated.from_dict(data)

        assert restored.tenant_id == tenant_id
