"""Unit tests for warning behavior, suppression, and inheritance hierarchies."""

import logging
from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class TestEventTypeMismatchWarning:
    """Tests for mismatch warning behavior."""

    def test_warning_logged_on_mismatch(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="eventsource.domain.event"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"
                event_type: str = "OrderCreatedEvent"

        assert "OrderCreated" in caplog.text
        assert "OrderCreatedEvent" in caplog.text
        assert "differs from class name" in caplog.text

    def test_warning_suppressed(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="eventsource.domain.event"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"
                event_type: str = "legacy_order_created"
                suppress_event_type_warning = True

        assert "differs from class name" not in caplog.text

    def test_no_warning_when_matching(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="eventsource.domain.event"):

            class OrderCreated(DomainEvent):
                aggregate_type: str = "Order"

        assert "differs from class name" not in caplog.text

    def test_no_warning_for_auto_derived(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="eventsource.domain.event"):

            class AutoDerivedEvent(DomainEvent):
                aggregate_type: str = "Test"

        assert "differs from class name" not in caplog.text


class TestSuppressEventTypeWarningAttribute:
    """Tests for the suppress_event_type_warning class attribute."""

    def test_suppress_warning_default_is_false(self) -> None:
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert TestEvent.suppress_event_type_warning is False

    def test_suppress_warning_can_be_set_true(self) -> None:
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            event_type: str = "different_name"
            suppress_event_type_warning = True

        assert TestEvent.suppress_event_type_warning is True

    def test_suppress_warning_is_class_variable(self) -> None:
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            suppress_event_type_warning = True

        event = TestEvent(aggregate_id=uuid4(), aggregate_version=1)
        data = event.to_dict()
        assert "suppress_event_type_warning" not in data

    def test_suppress_warning_inherited_but_can_override(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="eventsource.domain.event"):

            class ParentEvent(DomainEvent):
                aggregate_type: str = "Test"
                suppress_event_type_warning = True

            class ChildEventWithWarning(ParentEvent):
                event_type: str = "different"
                suppress_event_type_warning = False

        assert "ChildEventWithWarning" in caplog.text
        assert "differs from class name" in caplog.text


class TestInheritanceBehavior:
    """Tests for inheritance and subclass behavior."""

    def test_subclasses_get_own_event_type(self) -> None:
        class BaseOrderEvent(DomainEvent):
            aggregate_type: str = "Order"

        class OrderCreated(BaseOrderEvent):
            customer_id: UUID

        class OrderShipped(BaseOrderEvent):
            tracking_number: str

        created = OrderCreated(aggregate_id=uuid4(), aggregate_version=1, customer_id=uuid4())
        shipped = OrderShipped(
            aggregate_id=uuid4(), aggregate_version=2, tracking_number="TRACK123"
        )
        assert created.event_type == "OrderCreated"
        assert shipped.event_type == "OrderShipped"

    def test_intermediate_base_class_gets_own_type(self) -> None:
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


class TestSubclassingDoesNotCorruptParent:
    def test_parent_field_default_survives_subclassing(self) -> None:
        class ParentEvent(DomainEvent):
            aggregate_type: str = "P"

        class ChildEvent(ParentEvent):
            pass

        assert ParentEvent.event_type_name() == "ParentEvent"
        assert ChildEvent.event_type_name() == "ChildEvent"
        event = ParentEvent(aggregate_id=uuid4())
        assert event.event_type == "ParentEvent"

    def test_domainevent_base_default_untouched(self) -> None:
        class AnythingEvent(DomainEvent):
            aggregate_type: str = "A"

        assert DomainEvent.model_fields["event_type"].default == ""

    def test_registry_key_matches_wire_value_after_subclassing(self) -> None:
        registry = EventRegistry()

        class OrderPlaced(DomainEvent):
            aggregate_type: str = "Order"

        class OrderPlacedSpecial(OrderPlaced):
            pass

        registry.register(OrderPlaced)
        registry.register(OrderPlacedSpecial)
        assert registry.get("OrderPlaced") is OrderPlaced
        assert registry.get("OrderPlacedSpecial") is OrderPlacedSpecial

    def test_explicit_wire_name_inherited_by_subclass(self) -> None:
        class LegacyEvent(DomainEvent):
            event_type: str = "legacy.v1"
            suppress_event_type_warning = True
            aggregate_type: str = "L"

        class LegacyChildEvent(LegacyEvent):
            pass

        assert LegacyEvent.event_type_name() == "legacy.v1"
        assert LegacyChildEvent.event_type_name() == "legacy.v1"
