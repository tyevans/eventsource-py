"""Tests for duplicate registration, clear/unregister, decorator registration, error messages, and schema versioning."""

from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import (
    DuplicateEventTypeError,
    EventRegistry,
    EventTypeNotFoundError,
    register_event,
)
from tests.unit.domain.test_event_registry._fixtures import OrderCreated, OrderShipped


class TestDuplicateRegistration:
    def test_same_class_same_type_is_idempotent(self) -> None:
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderCreated)
        assert len(registry) == 1

    def test_different_class_same_type_raises_error(self) -> None:
        registry = EventRegistry([OrderCreated])

        class DuplicateOrder(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"

        with pytest.raises(DuplicateEventTypeError) as exc_info:
            registry.register(DuplicateOrder)

        error = exc_info.value
        assert error.event_type == "OrderCreated"
        assert error.existing_class is OrderCreated and error.new_class is DuplicateOrder
        assert "already registered" in str(error)


class TestBulkAndClassRegistration:
    def test_init_with_events_registers_all(self) -> None:
        registry = EventRegistry([OrderCreated, OrderShipped])
        assert len(registry) == 2
        assert OrderCreated in registry and OrderShipped in registry

    def test_register_all_fluent_chaining(self) -> None:
        registry = EventRegistry()
        returned = registry.register_all(OrderCreated, OrderShipped)
        assert returned is registry
        assert registry.contains(OrderCreated) and registry.contains(OrderShipped)

    def test_class_based_contains_and_unregister(self) -> None:
        registry = EventRegistry([OrderCreated, OrderShipped])
        assert OrderCreated in registry and registry.contains(OrderCreated)
        assert registry.unregister(OrderCreated) is True
        assert OrderCreated not in registry and not registry.contains(OrderCreated)
        assert registry.unregister(OrderCreated) is False


class TestRegistryClearAndUnregister:
    def test_clear_removes_all_registrations(self) -> None:
        registry = EventRegistry([OrderCreated, OrderShipped])
        registry.clear()
        assert len(registry) == 0 and registry.list_types() == []

    def test_unregister_removes_specific_type(self) -> None:
        registry = EventRegistry([OrderCreated, OrderShipped])
        assert registry.unregister("OrderCreated") is True
        assert "OrderCreated" not in registry and "OrderShipped" in registry

    def test_unregister_returns_false_for_unknown(self) -> None:
        assert EventRegistry().unregister("NonExistent") is False


class TestDecoratorRegistration:
    def test_decorator_without_parentheses(self) -> None:
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "TestEvent" in registry and registry.get("TestEvent") is TestEvent

    def test_decorator_with_parentheses(self) -> None:
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "TestEvent" in registry

    def test_decorator_with_explicit_type(self) -> None:
        registry = EventRegistry()

        @register_event(event_type="custom.test.event", registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "custom.test.event" in registry and "TestEvent" not in registry

    def test_decorator_returns_original_class(self) -> None:
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            custom_field: str = "value"

        event = TestEvent(aggregate_id=uuid4(), custom_field="test")
        assert event.custom_field == "test"


class TestErrorMessages:
    def test_not_found_error_is_helpful(self) -> None:
        registry = EventRegistry([OrderCreated, OrderShipped])
        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get("BadgeIssued")
        err_str = str(exc_info.value)
        assert all(
            s in err_str
            for s in ("BadgeIssued", "OrderCreated", "OrderShipped", "Did you forget to register")
        )

    def test_not_found_error_empty_registry(self) -> None:
        registry = EventRegistry()
        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get("SomeEvent")
        assert "SomeEvent" in str(exc_info.value) and "none" in str(exc_info.value)

    def test_duplicate_error_is_helpful(self) -> None:
        registry = EventRegistry([OrderCreated])

        class DuplicateOrder(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"

        with pytest.raises(DuplicateEventTypeError) as exc_info:
            registry.register(DuplicateOrder)
        err_str = str(exc_info.value)
        assert all(s in err_str for s in ("OrderCreated", "already registered", "DuplicateOrder"))


class TestVersionedEventSchemas:
    def test_register_versioned_events(self) -> None:
        registry = EventRegistry()

        class OrderCreatedV1(DomainEvent):
            event_type: str = "OrderCreated"
            event_version: int = 1
            aggregate_type: str = "Order"
            order_number: str

        class OrderCreatedV2(DomainEvent):
            event_type: str = "OrderCreated.v2"
            event_version: int = 2
            aggregate_type: str = "Order"
            order_number: str
            order_items: list[str] = []

        registry.register_all(OrderCreatedV1, OrderCreatedV2)
        assert registry.get("OrderCreated") is OrderCreatedV1
        assert registry.get("OrderCreated.v2") is OrderCreatedV2

    def test_same_event_different_versions_same_type_name(self) -> None:
        registry = EventRegistry()

        class OrderCreatedV1(DomainEvent):
            event_type: str = "OrderCreated"
            event_version: int = 1
            aggregate_type: str = "Order"

        class OrderCreatedV2(DomainEvent):
            event_type: str = "OrderCreated"
            event_version: int = 2
            aggregate_type: str = "Order"

        registry.register(OrderCreatedV1)
        with pytest.raises(DuplicateEventTypeError):
            registry.register(OrderCreatedV2)
