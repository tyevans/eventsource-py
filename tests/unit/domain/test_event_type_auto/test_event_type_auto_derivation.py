"""Unit tests for automatic event_type derivation and backward compatibility."""

from uuid import UUID, uuid4

from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class TestEventTypeAutoDerivation:
    """Tests for automatic event_type derivation from class name."""

    def test_derives_event_type_from_class_name(self) -> None:
        """event_type defaults to class name when not explicitly set."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            customer_id: UUID

        event = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=uuid4(),
        )
        assert event.event_type == "OrderCreated"

    def test_derives_different_class_names(self) -> None:
        """Different classes get different auto-derived event_types."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        class OrderShipped(DomainEvent):
            aggregate_type: str = "Order"

        class PaymentReceived(DomainEvent):
            aggregate_type: str = "Payment"

        created = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        shipped = OrderShipped(aggregate_id=uuid4(), aggregate_version=1)
        payment = PaymentReceived(aggregate_id=uuid4(), aggregate_version=1)

        assert created.event_type == "OrderCreated"
        assert shipped.event_type == "OrderShipped"
        assert payment.event_type == "PaymentReceived"

    def test_class_with_additional_fields(self) -> None:
        """Auto-derivation works with classes that have additional fields."""

        class ItemAdded(DomainEvent):
            aggregate_type: str = "Order"
            item_name: str = Field(..., description="Item name")
            quantity: int = Field(default=1, ge=1)
            price: float = Field(..., ge=0)

        event = ItemAdded(
            aggregate_id=uuid4(),
            aggregate_version=1,
            item_name="Widget",
            price=9.99,
        )
        assert event.event_type == "ItemAdded"
        assert event.item_name == "Widget"
        assert event.quantity == 1
        assert event.price == 9.99


class TestExplicitEventTypePreservation:
    """Tests for backward compatibility with explicit event_type."""

    def test_explicit_event_type_preserved(self) -> None:
        """Explicit event_type takes precedence over auto-derivation."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            event_type: str = "order.created.v2"
            suppress_event_type_warning = True

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "order.created.v2"

    def test_explicit_event_type_matching_class_name(self) -> None:
        """Explicit event_type matching class name works without warning."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreated"

    def test_legacy_event_with_explicit_type_unchanged(self) -> None:
        """Existing events with explicit event_type work unchanged."""

        class LegacyOrderEvent(DomainEvent):
            aggregate_type: str = "Order"
            event_type: str = "OrderCreated"  # Legacy explicit type
            customer_id: UUID
            suppress_event_type_warning = True

        event = LegacyOrderEvent(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=uuid4(),
        )
        assert event.event_type == "OrderCreated"


class TestBackwardCompatibility:
    """Tests ensuring backward compatibility with existing code."""

    def test_existing_events_with_explicit_type_work(self) -> None:
        """Existing events that explicitly set event_type continue to work."""

        # This is how events were defined before auto-derivation
        class LegacyOrderCreated(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"
            order_number: str
            customer_id: UUID

        event = LegacyOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )
        # Should use the explicit value
        assert event.event_type == "OrderCreated"

    def test_serialization_unchanged(self) -> None:
        """Serialized format remains the same."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        data = event.to_dict()

        # event_type should be in serialized data
        assert "event_type" in data
        assert data["event_type"] == "OrderCreated"

    def test_can_construct_with_event_type_kwarg(self) -> None:
        """Can still pass event_type as keyword argument."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        # Explicitly passing event_type should work and override
        event = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            event_type="custom_override",
        )
        assert event.event_type == "custom_override"


class TestIntegrationWithEventRegistry:
    """Tests for integration with event registry."""

    def test_auto_derived_type_works_with_registry(self) -> None:
        """Auto-derived event_type works correctly with EventRegistry."""
        registry = EventRegistry()

        class AutoTypedEvent(DomainEvent):
            aggregate_type: str = "Test"

        registry.register(AutoTypedEvent)

        # Should be registered under class name
        assert "AutoTypedEvent" in registry
        assert registry.get("AutoTypedEvent") is AutoTypedEvent

    def test_explicit_type_works_with_registry(self) -> None:
        """Explicit event_type works correctly with EventRegistry."""
        registry = EventRegistry()

        class ExplicitTypedEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "explicit.typed.event"
            suppress_event_type_warning = True

        registry.register(ExplicitTypedEvent)

        # Should be registered under explicit type
        assert "explicit.typed.event" in registry
        assert registry.get("explicit.typed.event") is ExplicitTypedEvent
