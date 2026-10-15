"""Unit tests for dict construction, model validation, and edge cases."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.domain.event import DomainEvent


class TestDictConstruction:
    """Tests for event_type behavior with dict construction."""

    def test_dict_construction_uses_class_name(self) -> None:
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
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated.from_dict(
            {"aggregate_id": str(uuid4()), "aggregate_type": "Order", "aggregate_version": 1}
        )
        assert event.event_type == "OrderCreated"

    def test_to_dict_from_dict_roundtrip(self) -> None:
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            customer_id: UUID

        original = OrderCreated(aggregate_id=uuid4(), aggregate_version=1, customer_id=uuid4())
        assert original.event_type == "OrderCreated"
        restored = OrderCreated.from_dict(original.to_dict())
        assert restored.event_type == "OrderCreated"
        assert restored.event_id == original.event_id

    def test_dict_construction_with_explicit_class_default(self) -> None:
        class VersionedEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "versioned.event.v3"
            suppress_event_type_warning = True

        event = VersionedEvent.model_validate(
            {"aggregate_id": str(uuid4()), "aggregate_type": "Test", "aggregate_version": 1}
        )
        assert event.event_type == "versioned.event.v3"


class TestModelValidatorBehavior:
    """Tests specifically for the model_validator behavior."""

    def test_model_validator_does_not_mutate_input(self) -> None:
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        input_data = {
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Order",
            "aggregate_version": 1,
        }
        original_keys = set(input_data.keys())
        OrderCreated.model_validate(input_data)
        assert set(input_data.keys()) == original_keys
        assert "event_type" not in input_data

    def test_model_validator_with_explicit_field_default(self) -> None:
        class ExplicitDefault(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "explicit_default_value"
            suppress_event_type_warning = True

        event = ExplicitDefault.model_validate(
            {"aggregate_id": str(uuid4()), "aggregate_type": "Test", "aggregate_version": 1}
        )
        assert event.event_type == "explicit_default_value"

    def test_model_validator_with_provided_value(self) -> None:
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
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"
            event_type: str = ""

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreated"

    def test_frozen_event(self) -> None:
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreated"
        with pytest.raises((ValidationError, AttributeError)):
            event.event_type = "modified"  # type: ignore[misc]

    def test_special_characters_in_class_name(self) -> None:
        class My_Custom_Event(DomainEvent):  # noqa: N801
            aggregate_type: str = "Test"

        event = My_Custom_Event(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "My_Custom_Event"

    def test_numeric_suffix_in_class_name(self) -> None:
        class OrderCreatedV2(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreatedV2(aggregate_id=uuid4(), aggregate_version=1)
        assert event.event_type == "OrderCreatedV2"

    def test_long_class_name(self) -> None:
        class VeryLongEventClassNameForTestingPurposesOnly(DomainEvent):
            aggregate_type: str = "Test"

        event = VeryLongEventClassNameForTestingPurposesOnly(
            aggregate_id=uuid4(), aggregate_version=1
        )
        expected = "VeryLongEventClassNameForTestingPurposesOnly"  # pragma: allowlist secret
        assert event.event_type == expected

    def test_class_model_fields_updated(self) -> None:
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        assert OrderCreated.event_type_name() == "OrderCreated"

    def test_dict_construction_with_empty_string_value(self) -> None:
        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated.model_validate(
            {
                "aggregate_id": str(uuid4()),
                "aggregate_type": "Order",
                "aggregate_version": 1,
                "event_type": "",
            }
        )
        assert event.event_type == "OrderCreated"
