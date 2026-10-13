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
from tests.unit.domain.test_event_registry._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestDuplicateRegistration:
    """Tests for duplicate registration handling."""

    def test_same_class_same_type_is_idempotent(self) -> None:
        """Registering same class with same type is idempotent."""
        registry = EventRegistry()

        registry.register(OrderCreated)
        registry.register(OrderCreated)  # Should not raise

        assert len(registry) == 1

    def test_different_class_same_type_raises_error(self) -> None:
        """Registering different class with same type raises error."""
        registry = EventRegistry()

        # Create a class with same event_type as OrderCreated
        class DuplicateOrder(DomainEvent):
            event_type: str = "OrderCreated"  # Same as OrderCreated
            aggregate_type: str = "Order"

        registry.register(OrderCreated)

        with pytest.raises(DuplicateEventTypeError) as exc_info:
            registry.register(DuplicateOrder)

        error = exc_info.value
        assert error.event_type == "OrderCreated"
        assert error.existing_class is OrderCreated
        assert error.new_class is DuplicateOrder
        assert "already registered" in str(error)


class TestRegistryClearAndUnregister:
    """Tests for clear and unregister operations."""

    def test_clear_removes_all_registrations(self) -> None:
        """clear removes all registrations."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        registry.clear()

        assert len(registry) == 0
        assert registry.list_types() == []

    def test_unregister_removes_specific_type(self) -> None:
        """unregister removes specific event type."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        result = registry.unregister("OrderCreated")

        assert result is True
        assert "OrderCreated" not in registry
        assert "OrderShipped" in registry

    def test_unregister_returns_false_for_unknown(self) -> None:
        """unregister returns False for unknown type."""
        registry = EventRegistry()

        result = registry.unregister("NonExistent")

        assert result is False


class TestDecoratorRegistration:
    """Tests for decorator-based registration."""

    def test_decorator_without_parentheses(self) -> None:
        """Decorator works without parentheses."""
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "TestEvent" in registry
        assert registry.get("TestEvent") is TestEvent

    def test_decorator_with_parentheses(self) -> None:
        """Decorator works with empty parentheses."""
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "TestEvent" in registry

    def test_decorator_with_explicit_type(self) -> None:
        """Decorator works with explicit event_type."""
        registry = EventRegistry()

        @register_event(event_type="custom.test.event", registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        assert "custom.test.event" in registry
        assert "TestEvent" not in registry

    def test_decorator_returns_original_class(self) -> None:
        """Decorator returns the original class unchanged."""
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"
            custom_field: str = "value"

        # Class should be usable
        event = TestEvent(
            aggregate_id=uuid4(),
            custom_field="test",
        )
        assert event.custom_field == "test"


class TestErrorMessages:
    """Tests for error message quality."""

    def test_not_found_error_is_helpful(self) -> None:
        """EventTypeNotFoundError has helpful message."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        try:
            registry.get("BadgeIssued")
            pytest.fail("Should have raised")
        except EventTypeNotFoundError as e:
            # Error should contain useful information
            assert "BadgeIssued" in str(e)  # The missing type
            assert "OrderCreated" in str(e)  # Available types
            assert "OrderShipped" in str(e)  # Available types
            assert "Did you forget to register" in str(e)  # Helpful hint

    def test_not_found_error_empty_registry(self) -> None:
        """Error message is helpful even with empty registry."""
        registry = EventRegistry()

        try:
            registry.get("SomeEvent")
            pytest.fail("Should have raised")
        except EventTypeNotFoundError as e:
            assert "SomeEvent" in str(e)
            assert "none" in str(e)  # Shows no types available

    def test_duplicate_error_is_helpful(self) -> None:
        """DuplicateEventTypeError has helpful message."""
        registry = EventRegistry()

        class DuplicateOrder(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"

        registry.register(OrderCreated)

        try:
            registry.register(DuplicateOrder)
            pytest.fail("Should have raised")
        except DuplicateEventTypeError as e:
            assert "OrderCreated" in str(e)
            assert "already registered" in str(e)
            assert "DuplicateOrder" in str(e)


class TestVersionedEventSchemas:
    """Tests for versioned event schema support."""

    def test_register_versioned_events(self) -> None:
        """Can register different versions of events with different type names."""
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
            order_items: list[str] = []  # New field in v2

        registry.register(OrderCreatedV1)
        registry.register(OrderCreatedV2)

        assert registry.get("OrderCreated") is OrderCreatedV1
        assert registry.get("OrderCreated.v2") is OrderCreatedV2

    def test_same_event_different_versions_same_type_name(self) -> None:
        """Cannot register different versions with same type name."""
        registry = EventRegistry()

        class OrderCreatedV1(DomainEvent):
            event_type: str = "OrderCreated"
            event_version: int = 1
            aggregate_type: str = "Order"

        class OrderCreatedV2(DomainEvent):
            event_type: str = "OrderCreated"  # Same type name!
            event_version: int = 2
            aggregate_type: str = "Order"

        registry.register(OrderCreatedV1)

        with pytest.raises(DuplicateEventTypeError):
            registry.register(OrderCreatedV2)
