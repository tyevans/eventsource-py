"""Tests for the transactional outbox payload builder."""

import json
from datetime import UTC, datetime
from uuid import uuid4

from eventsource.domain.event import DomainEvent
from eventsource.ports import outbox_event_data


class OrderPlaced(DomainEvent):
    """Test event."""

    aggregate_type: str = "Order"
    quantity: int = 1


class TestOutboxEventData:
    """Tests for outbox_event_data builder."""

    def test_returns_exactly_six_keys(self) -> None:
        """Returned dict has exactly the six required keys."""
        event = OrderPlaced(aggregate_id=uuid4())
        result = outbox_event_data(event)

        expected_keys = {
            "event_id",
            "aggregate_id",
            "aggregate_type",
            "tenant_id",
            "occurred_at",
            "payload",
        }
        assert set(result.keys()) == expected_keys

    def test_event_id_is_string(self) -> None:
        """event_id is converted to string."""
        event_id = uuid4()
        event = OrderPlaced(aggregate_id=uuid4(), event_id=event_id)
        result = outbox_event_data(event)

        assert result["event_id"] == str(event_id)
        assert isinstance(result["event_id"], str)

    def test_aggregate_id_is_string(self) -> None:
        """aggregate_id is converted to string."""
        aggregate_id = uuid4()
        event = OrderPlaced(aggregate_id=aggregate_id)
        result = outbox_event_data(event)

        assert result["aggregate_id"] == str(aggregate_id)
        assert isinstance(result["aggregate_id"], str)

    def test_aggregate_type_preserved(self) -> None:
        """aggregate_type is preserved as-is."""
        event = OrderPlaced(aggregate_id=uuid4())
        result = outbox_event_data(event)

        assert result["aggregate_type"] == "Order"

    def test_occurred_at_is_isoformat(self) -> None:
        """occurred_at is converted to ISO format string."""
        occurred_at = datetime(2026, 7, 31, 12, 30, 45, 123456, tzinfo=UTC)
        event = OrderPlaced(aggregate_id=uuid4(), occurred_at=occurred_at)
        result = outbox_event_data(event)

        assert result["occurred_at"] == occurred_at.isoformat()
        assert isinstance(result["occurred_at"], str)

    def test_tenant_id_when_set(self) -> None:
        """tenant_id is converted to string when set."""
        tenant_id = uuid4()
        event = OrderPlaced(aggregate_id=uuid4(), tenant_id=tenant_id)
        result = outbox_event_data(event)

        assert result["tenant_id"] == str(tenant_id)
        assert isinstance(result["tenant_id"], str)

    def test_tenant_id_when_none(self) -> None:
        """tenant_id is None when event has no tenant_id."""
        event = OrderPlaced(aggregate_id=uuid4(), tenant_id=None)
        result = outbox_event_data(event)

        assert result["tenant_id"] is None

    def test_payload_from_model_dump_json(self) -> None:
        """payload equals event.model_dump(mode='json')."""
        event = OrderPlaced(aggregate_id=uuid4(), quantity=42)
        result = outbox_event_data(event)

        expected_payload = event.model_dump(mode="json")
        assert result["payload"] == expected_payload

    def test_payload_is_json_safe(self) -> None:
        """Result survives json.dumps with no custom encoder."""
        event = OrderPlaced(aggregate_id=uuid4(), quantity=42)
        result = outbox_event_data(event)

        # Should not raise; stdlib json encoder handles all values
        serialized = json.dumps(result)
        assert isinstance(serialized, str)

        # Round-trip to verify structure is preserved
        deserialized = json.loads(serialized)
        assert deserialized["event_id"] == result["event_id"]
        assert deserialized["payload"]["quantity"] == 42
