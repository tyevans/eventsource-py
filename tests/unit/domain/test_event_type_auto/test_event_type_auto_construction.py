"""Unit tests for dict construction, model validation, and edge cases."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.domain.event import DomainEvent


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
        expected = "VeryLongEventClassNameForTestingPurposesOnly"  # pragma: allowlist secret
        assert event.event_type == expected

    def test_class_model_fields_updated(self) -> None:
        """event_type_name() derives the correct type from class name."""

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        # Use event_type_name() — the canonical source of truth,
        # not the mutated model_fields
        assert OrderCreated.event_type_name() == "OrderCreated"

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
