"""
Comprehensive unit tests for automatic event_type derivation.

Tests cover:
- Auto-derivation from class name when event_type not explicitly set
- Explicit event_type preservation (backward compatibility)
- Dict/JSON construction with auto-derivation
- Warning behavior when event_type differs from class name
- Warning suppression via suppress_event_type_warning
- Inheritance and subclass behavior
- Edge cases (empty string, special characters, etc.)

This feature (DX-009) reduces boilerplate by auto-deriving event_type from the
class name, eliminating the need for repetitive `event_type = "ClassName"` in
every event class.
"""

import logging
from uuid import UUID, uuid4

import pytest
from pydantic import Field, ValidationError

from eventsource.events.base import DomainEvent


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
            event_type: str = "OrderCreated"  # Matches class name

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


class TestDictConstruction:
    """Tests for event_type behavior with dict construction."""

    def test_dict_construction_uses_class_name(self) -> None:
        """Creating from dict uses class name for event_type if not provided."""

        class ItemAdded(DomainEvent):
            aggregate_type: str = "Order"
            item_name: str

        event = ItemAdded.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Order",
                "aggregate_version": 1,
                "item_name": "Widget",
            }
        )
        assert event.event_type == "ItemAdded"

    def test_dict_with_explicit_event_type_preserves_it(self) -> None:
        """Dict with explicit event_type preserves the provided value."""

        class ItemAdded(DomainEvent):
            aggregate_type: str = "Order"
            item_name: str

        event = ItemAdded.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Order",
                "aggregate_version": 1,
                "item_name": "Widget",
                "event_type": "custom_event_type",
            }
        )
        assert event.event_type == "custom_event_type"

    def test_from_dict_method_works(self) -> None:
        """from_dict class method respects auto-derivation."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated.from_dict(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Order",
                "aggregate_version": 1,
            }
        )
        assert event.event_type == "OrderCreated"

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """Serialization roundtrip preserves event_type."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            customer_id: UUID

        original = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=uuid4(),
        )
        assert original.event_type == "OrderCreated"

        # Roundtrip
        data = original.to_dict()
        restored = OrderCreated.from_dict(data)
        assert restored.event_type == "OrderCreated"
        assert restored.event_id == original.event_id

    def test_dict_construction_with_explicit_class_default(self) -> None:
        """Dict construction respects explicit class event_type default."""

        class VersionedEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "versioned.event.v3"
            suppress_event_type_warning = True

        # No event_type in dict - should use class default
        event = VersionedEvent.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Test",
                "aggregate_version": 1,
            }
        )
        assert event.event_type == "versioned.event.v3"


class TestEventTypeMismatchWarning:
    """Tests for mismatch warning behavior."""

    def test_warning_logged_on_mismatch(self, caplog: pytest.LogCaptureFixture) -> None:
        """Warning logged when event_type differs from class name."""
        with caplog.at_level(logging.WARNING, logger="eventsource.events.base"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"
                event_type: str = "OrderCreatedEvent"  # Different from class name

        assert "OrderCreated" in caplog.text
        assert "OrderCreatedEvent" in caplog.text
        assert "differs from class name" in caplog.text

    def test_warning_suppressed(self, caplog: pytest.LogCaptureFixture) -> None:
        """Warning suppressed with suppress_event_type_warning=True."""
        with caplog.at_level(logging.WARNING, logger="eventsource.events.base"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"
                event_type: str = "legacy_order_created"
                suppress_event_type_warning = True

        assert "differs from class name" not in caplog.text

    def test_no_warning_when_matching(self, caplog: pytest.LogCaptureFixture) -> None:
        """No warning when event_type matches class name."""
        with caplog.at_level(logging.WARNING, logger="eventsource.events.base"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"
                event_type: str = "OrderCreated"  # Same as class name

        assert "differs from class name" not in caplog.text

    def test_no_warning_for_auto_derived(self, caplog: pytest.LogCaptureFixture) -> None:
        """No warning when event_type is auto-derived."""
        with caplog.at_level(logging.WARNING, logger="eventsource.events.base"):

            class AutoDerivedEvent(DomainEvent):
                aggregate_type: str = "Test"
                # No explicit event_type

        assert "differs from class name" not in caplog.text


class TestInheritanceBehavior:
    """Tests for inheritance and subclass behavior."""

    def test_subclasses_get_own_event_type(self) -> None:
        """Each subclass gets its own auto-derived event_type."""

        class BaseOrderEvent(DomainEvent):
            aggregate_type: str = "Order"

        class OrderCreated(BaseOrderEvent):
            customer_id: UUID

        class OrderShipped(BaseOrderEvent):
            tracking_number: str

        created = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=uuid4(),
        )
        shipped = OrderShipped(
            aggregate_id=uuid4(),
            aggregate_version=2,
            tracking_number="TRACK123",
        )

        assert created.event_type == "OrderCreated"
        assert shipped.event_type == "OrderShipped"

    def test_intermediate_base_class_gets_own_type(self) -> None:
        """Intermediate base classes also get auto-derived event_type."""

        class BaseEvent(DomainEvent):
            aggregate_type: str = "Base"

        class MiddleEvent(BaseEvent):
            middle_field: str = "middle"

        class FinalEvent(MiddleEvent):
            final_field: str = "final"

        middle = MiddleEvent(aggregate_id=uuid4(), aggregate_version=1)
        final = FinalEvent(aggregate_id=uuid4(), aggregate_version=1)

        assert middle.event_type == "MiddleEvent"
        assert final.event_type == "FinalEvent"

    def test_subclass_can_override_parent_event_type(self) -> None:
        """Subclass can override parent's explicit event_type."""

        class ParentEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "parent_event"
            suppress_event_type_warning = True

        class ChildEvent(ParentEvent):
            event_type: str = "child_event"
            suppress_event_type_warning = True

        parent = ParentEvent(aggregate_id=uuid4(), aggregate_version=1)
        child = ChildEvent(aggregate_id=uuid4(), aggregate_version=1)

        assert parent.event_type == "parent_event"
        assert child.event_type == "child_event"


class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_string_event_type_triggers_auto(self) -> None:
        """Empty string event_type is replaced with class name."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            event_type: str = ""  # Empty string

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        # Empty string field default means auto-derivation applies
        assert event.event_type == "OrderCreated"

    def test_frozen_event(self) -> None:
        """Events remain frozen/immutable after auto-derivation."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreated"

        # Should not be able to modify
        with pytest.raises((ValidationError, AttributeError)):
            event.event_type = "modified"  # type: ignore[misc]

    def test_special_characters_in_class_name(self) -> None:
        """Classes with underscores work correctly."""

        class My_Custom_Event(DomainEvent):  # noqa: N801
            aggregate_type: str = "Test"

        event = My_Custom_Event(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "My_Custom_Event"

    def test_numeric_suffix_in_class_name(self) -> None:
        """Classes with numeric suffixes work correctly."""

        class OrderCreatedV2(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreatedV2(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreatedV2"

    def test_long_class_name(self) -> None:
        """Long class names work correctly."""

        class VeryLongEventClassNameForTestingPurposesOnly(DomainEvent):
            aggregate_type: str = "Test"

        event = VeryLongEventClassNameForTestingPurposesOnly(
            aggregate_id=uuid4(),
            aggregate_version=1,
        )
        assert event.event_type == "VeryLongEventClassNameForTestingPurposesOnly"

    def test_class_model_fields_updated(self) -> None:
        """model_fields default is updated correctly."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        field_info = OrderCreated.model_fields.get("event_type")
        assert field_info is not None
        assert field_info.default == "OrderCreated"

    def test_dict_construction_with_empty_string_value(self) -> None:
        """Dict with empty string event_type uses class name."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Order",
                "aggregate_version": 1,
                "event_type": "",  # Empty string
            }
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


class TestSuppressEventTypeWarningAttribute:
    """Tests for the suppress_event_type_warning class attribute."""

    def test_suppress_warning_default_is_false(self) -> None:
        """Default value of suppress_event_type_warning is False."""

        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert TestEvent.suppress_event_type_warning is False

    def test_suppress_warning_can_be_set_true(self) -> None:
        """suppress_event_type_warning can be set to True."""

        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "different_name"
            suppress_event_type_warning = True

        assert TestEvent.suppress_event_type_warning is True

    def test_suppress_warning_is_class_variable(self) -> None:
        """suppress_event_type_warning is a class variable, not instance."""

        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            suppress_event_type_warning = True

        event = TestEvent(aggregate_id=uuid4(), aggregate_version=1)

        # Should not be in instance dict (serialized data)
        data = event.to_dict()
        assert "suppress_event_type_warning" not in data

    def test_suppress_warning_inherited_but_can_override(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Subclasses inherit suppress_event_type_warning but can override."""
        with caplog.at_level(logging.WARNING, logger="eventsource.events.base"):

            class ParentEvent(DomainEvent):
                aggregate_type: str = "Test"
                suppress_event_type_warning = True

            class ChildEventWithWarning(ParentEvent):
                event_type: str = "different"
                suppress_event_type_warning = False  # Override to show warning

        assert "ChildEventWithWarning" in caplog.text
        assert "differs from class name" in caplog.text


class TestIntegrationWithEventRegistry:
    """Tests for integration with event registry."""

    def test_auto_derived_type_works_with_registry(self) -> None:
        """Auto-derived event_type works correctly with EventRegistry."""
        from eventsource.events.registry import EventRegistry

        registry = EventRegistry()

        class AutoTypedEvent(DomainEvent):
            aggregate_type: str = "Test"

        registry.register(AutoTypedEvent)

        # Should be registered under class name
        assert "AutoTypedEvent" in registry
        assert registry.get("AutoTypedEvent") is AutoTypedEvent

    def test_explicit_type_works_with_registry(self) -> None:
        """Explicit event_type works correctly with EventRegistry."""
        from eventsource.events.registry import EventRegistry

        registry = EventRegistry()

        class ExplicitTypedEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "explicit.typed.event"
            suppress_event_type_warning = True

        registry.register(ExplicitTypedEvent)

        # Should be registered under explicit type
        assert "explicit.typed.event" in registry
        assert registry.get("explicit.typed.event") is ExplicitTypedEvent


class TestModelValidatorBehavior:
    """Tests specifically for the model_validator behavior."""

    def test_model_validator_does_not_mutate_input(self) -> None:
        """Model validator creates copy, doesn't mutate input dict."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        input_data = {
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Order",
            "aggregate_version": 1,
        }
        original_keys = set(input_data.keys())

        OrderCreated.model_validate(input_data)

        # Original dict should not have been modified
        assert set(input_data.keys()) == original_keys
        assert "event_type" not in input_data

    def test_model_validator_with_explicit_field_default(self) -> None:
        """Model validator respects explicit field default."""

        class ExplicitDefault(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "explicit_default_value"
            suppress_event_type_warning = True

        # When event_type not in dict, field default should be used
        event = ExplicitDefault.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Test",
                "aggregate_version": 1,
            }
        )
        assert event.event_type == "explicit_default_value"

    def test_model_validator_with_provided_value(self) -> None:
        """Model validator preserves provided event_type value."""

        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        event = TestEvent.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Test",
                "aggregate_version": 1,
                "event_type": "provided_value",
            }
        )
        assert event.event_type == "provided_value"
