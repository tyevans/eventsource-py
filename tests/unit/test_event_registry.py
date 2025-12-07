"""
Comprehensive unit tests for the EventRegistry.

Tests cover:
- Event registration (programmatic and decorator-based)
- Event lookup (get, get_or_none, contains)
- Type resolution (from field, class name, explicit)
- Duplicate registration handling
- Thread safety
- Registry isolation
- Convenience functions
- Error handling
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

import pytest
from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.events.registry import (
    DuplicateEventTypeError,
    EventRegistry,
    EventTypeNotFoundError,
    default_registry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    register_event,
)


# Test event classes for the tests
class OrderCreated(DomainEvent):
    """Sample event with explicit event_type default."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"

    order_number: str = Field(..., description="Unique order number")
    customer_id: UUID = Field(..., description="Customer who placed the order")


class OrderShipped(DomainEvent):
    """Sample event with explicit event_type default."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str = Field(..., description="Shipping tracking number")


class PaymentReceived(DomainEvent):
    """Sample event with explicit event_type default."""

    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"

    amount: float = Field(..., description="Payment amount")


class EventWithoutTypeDefault(DomainEvent):
    """Event that doesn't set event_type default - will use class name."""

    aggregate_type: str = "Test"


class TestEventRegistryBasicOperations:
    """Tests for basic registry operations."""

    def test_create_empty_registry(self) -> None:
        """Can create an empty registry."""
        registry = EventRegistry()

        assert len(registry) == 0
        assert registry.list_types() == []

    def test_register_event_class(self) -> None:
        """Can register an event class."""
        registry = EventRegistry()

        registry.register(OrderCreated)

        assert len(registry) == 1
        assert "OrderCreated" in registry

    def test_register_event_with_explicit_type(self) -> None:
        """Can register an event class with explicit type name."""
        registry = EventRegistry()

        registry.register(OrderCreated, "order.created")

        assert "order.created" in registry
        assert "OrderCreated" not in registry

    def test_register_returns_event_class(self) -> None:
        """Register returns the event class for chaining."""
        registry = EventRegistry()

        result = registry.register(OrderCreated)

        assert result is OrderCreated

    def test_register_multiple_events(self) -> None:
        """Can register multiple event classes."""
        registry = EventRegistry()

        registry.register(OrderCreated)
        registry.register(OrderShipped)
        registry.register(PaymentReceived)

        assert len(registry) == 3
        assert "OrderCreated" in registry
        assert "OrderShipped" in registry
        assert "PaymentReceived" in registry


class TestEventRegistryLookup:
    """Tests for event lookup operations."""

    def test_get_registered_event(self) -> None:
        """Can get a registered event class."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        result = registry.get("OrderCreated")

        assert result is OrderCreated

    def test_get_unregistered_event_raises_error(self) -> None:
        """Getting unregistered event raises EventTypeNotFoundError."""
        registry = EventRegistry()

        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get("NonExistentEvent")

        assert exc_info.value.event_type == "NonExistentEvent"
        assert "Unknown event type" in str(exc_info.value)
        assert "Did you forget to register" in str(exc_info.value)

    def test_get_error_includes_available_types(self) -> None:
        """Error message includes list of available types."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get("NonExistent")

        error_msg = str(exc_info.value)
        assert "OrderCreated" in error_msg
        assert "OrderShipped" in error_msg

    def test_get_or_none_returns_class_when_found(self) -> None:
        """get_or_none returns class when found."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        result = registry.get_or_none("OrderCreated")

        assert result is OrderCreated

    def test_get_or_none_returns_none_when_not_found(self) -> None:
        """get_or_none returns None when not found."""
        registry = EventRegistry()

        result = registry.get_or_none("NonExistent")

        assert result is None

    def test_contains_returns_true_for_registered(self) -> None:
        """contains returns True for registered event."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        assert registry.contains("OrderCreated") is True
        assert ("OrderCreated" in registry) is True

    def test_contains_returns_false_for_unregistered(self) -> None:
        """contains returns False for unregistered event."""
        registry = EventRegistry()

        assert registry.contains("NonExistent") is False
        assert ("NonExistent" in registry) is False

    def test_list_types_returns_sorted_types(self) -> None:
        """list_types returns sorted list of type names."""
        registry = EventRegistry()
        registry.register(PaymentReceived)
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        types = registry.list_types()

        assert types == ["OrderCreated", "OrderShipped", "PaymentReceived"]

    def test_list_classes_returns_classes_in_sorted_order(self) -> None:
        """list_classes returns classes sorted by type name."""
        registry = EventRegistry()
        registry.register(PaymentReceived)
        registry.register(OrderCreated)

        classes = registry.list_classes()

        assert classes == [OrderCreated, PaymentReceived]


class TestEventTypeResolution:
    """Tests for event type name resolution."""

    def test_resolves_from_pydantic_field_default(self) -> None:
        """Resolves type from Pydantic field default."""
        registry = EventRegistry()

        registry.register(OrderCreated)

        assert "OrderCreated" in registry

    def test_resolves_from_class_name_without_default(self) -> None:
        """Falls back to class name when no field default."""
        registry = EventRegistry()

        registry.register(EventWithoutTypeDefault)

        assert "EventWithoutTypeDefault" in registry

    def test_explicit_type_overrides_default(self) -> None:
        """Explicit type name overrides field default."""
        registry = EventRegistry()

        registry.register(OrderCreated, "custom.order.created")

        assert "custom.order.created" in registry
        assert "OrderCreated" not in registry


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
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        assert "TestEvent" in registry
        assert registry.get("TestEvent") is TestEvent

    def test_decorator_with_parentheses(self) -> None:
        """Decorator works with empty parentheses."""
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        assert "TestEvent" in registry

    def test_decorator_with_explicit_type(self) -> None:
        """Decorator works with explicit event_type."""
        registry = EventRegistry()

        @register_event(event_type="custom.test.event", registry=registry)
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"  # Ignored
            aggregate_type: str = "Test"

        assert "custom.test.event" in registry
        assert "TestEvent" not in registry

    def test_decorator_returns_original_class(self) -> None:
        """Decorator returns the original class unchanged."""
        registry = EventRegistry()

        @register_event(registry=registry)
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"
            custom_field: str = "value"

        # Class should be usable
        event = TestEvent(
            aggregate_id=uuid4(),
            custom_field="test",
        )
        assert event.custom_field == "test"


class TestDefaultRegistry:
    """Tests for the default module-level registry."""

    def setup_method(self) -> None:
        """Clean up default registry before each test."""
        default_registry.clear()

    def teardown_method(self) -> None:
        """Clean up default registry after each test."""
        default_registry.clear()

    def test_decorator_uses_default_registry(self) -> None:
        """Decorator without registry parameter uses default."""

        @register_event
        class TestEvent(DomainEvent):
            event_type: str = "TestEventDefault"
            aggregate_type: str = "Test"

        assert "TestEventDefault" in default_registry
        assert get_event_class("TestEventDefault") is TestEvent

    def test_get_event_class_uses_default_registry(self) -> None:
        """get_event_class function uses default registry."""
        default_registry.register(OrderCreated)

        result = get_event_class("OrderCreated")

        assert result is OrderCreated

    def test_get_event_class_or_none_uses_default_registry(self) -> None:
        """get_event_class_or_none function uses default registry."""
        default_registry.register(OrderCreated)

        assert get_event_class_or_none("OrderCreated") is OrderCreated
        assert get_event_class_or_none("NonExistent") is None

    def test_is_event_registered_uses_default_registry(self) -> None:
        """is_event_registered function uses default registry."""
        default_registry.register(OrderCreated)

        assert is_event_registered("OrderCreated") is True
        assert is_event_registered("NonExistent") is False

    def test_list_registered_events_uses_default_registry(self) -> None:
        """list_registered_events function uses default registry."""
        default_registry.register(OrderCreated)
        default_registry.register(OrderShipped)

        types = list_registered_events()

        assert "OrderCreated" in types
        assert "OrderShipped" in types


class TestRegistryIsolation:
    """Tests for registry isolation (important for testing)."""

    def test_separate_registries_are_isolated(self) -> None:
        """Separate registry instances are isolated."""
        registry1 = EventRegistry()
        registry2 = EventRegistry()

        registry1.register(OrderCreated)
        registry2.register(OrderShipped)

        assert "OrderCreated" in registry1
        assert "OrderCreated" not in registry2
        assert "OrderShipped" not in registry1
        assert "OrderShipped" in registry2

    def test_clear_does_not_affect_other_registries(self) -> None:
        """Clearing one registry does not affect others."""
        registry1 = EventRegistry()
        registry2 = EventRegistry()

        registry1.register(OrderCreated)
        registry2.register(OrderCreated)

        registry1.clear()

        assert len(registry1) == 0
        assert len(registry2) == 1


class TestThreadSafety:
    """Tests for thread safety of the registry."""

    def test_concurrent_registration(self) -> None:
        """Concurrent registrations are thread-safe."""
        registry = EventRegistry()
        errors: list[Exception] = []

        def register_events(event_classes: list[type[DomainEvent]]) -> None:
            try:
                for event_class in event_classes:
                    registry.register(event_class)
            except Exception as e:
                errors.append(e)

        # Create unique event classes for each thread
        event_classes_per_thread: list[list[type[DomainEvent]]] = []
        for i in range(5):
            thread_events: list[type[DomainEvent]] = []
            for j in range(10):
                # Create unique class dynamically
                class_name = f"Event_{i}_{j}"

                class ThreadEvent(DomainEvent):
                    event_type: str = class_name
                    aggregate_type: str = "Test"

                ThreadEvent.__name__ = class_name
                thread_events.append(ThreadEvent)
            event_classes_per_thread.append(thread_events)

        # Run registrations concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(register_events, events) for events in event_classes_per_thread
            ]
            for f in futures:
                f.result()

        # No errors should have occurred
        assert len(errors) == 0
        # All events should be registered
        assert len(registry) == 50

    def test_concurrent_lookup(self) -> None:
        """Concurrent lookups are thread-safe."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        results: list[type[DomainEvent]] = []
        errors: list[Exception] = []

        def lookup_events() -> None:
            try:
                for _ in range(100):
                    results.append(registry.get("OrderCreated"))
                    results.append(registry.get("OrderShipped"))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=lookup_events) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No errors should have occurred
        assert len(errors) == 0
        # All lookups should have succeeded
        assert len(results) == 2000

    def test_concurrent_registration_and_lookup(self) -> None:
        """Concurrent registration and lookup are thread-safe."""
        registry = EventRegistry()
        errors: list[Exception] = []
        lookup_results: list[type[DomainEvent] | None] = []

        # Pre-register some events
        registry.register(OrderCreated)

        def register_events() -> None:
            try:
                for i in range(50):
                    class_name = f"NewEvent_{i}"

                    class NewEvent(DomainEvent):
                        event_type: str = class_name
                        aggregate_type: str = "Test"

                    NewEvent.__name__ = class_name
                    registry.register(NewEvent)
            except Exception as e:
                errors.append(e)

        def lookup_events() -> None:
            try:
                for _ in range(100):
                    # Look up known event
                    lookup_results.append(registry.get_or_none("OrderCreated"))
                    # Try to list types
                    registry.list_types()
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=register_events),
            threading.Thread(target=lookup_events),
            threading.Thread(target=lookup_events),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No errors should have occurred
        assert len(errors) == 0


class TestRegistryIteration:
    """Tests for registry iteration."""

    def test_iterate_over_registry(self) -> None:
        """Can iterate over registry event type names."""
        registry = EventRegistry()
        registry.register(OrderCreated)
        registry.register(OrderShipped)

        types = list(registry)

        assert "OrderCreated" in types
        assert "OrderShipped" in types
        assert len(types) == 2

    def test_len_returns_count(self) -> None:
        """len() returns number of registered types."""
        registry = EventRegistry()

        assert len(registry) == 0

        registry.register(OrderCreated)
        assert len(registry) == 1

        registry.register(OrderShipped)
        assert len(registry) == 2

    def test_empty_registry_is_truthy(self) -> None:
        """Empty registry is truthy (important for 'registry or default' pattern)."""
        registry = EventRegistry()

        assert len(registry) == 0
        assert bool(registry) is True
        # This is important - registry should be truthy even when empty
        # so that 'registry or default_registry' works correctly
        assert registry or registry == "fallback"


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


class TestEventDeserialization:
    """Tests demonstrating event deserialization use case."""

    def test_deserialize_event_from_storage(self) -> None:
        """Can deserialize events from storage using registry."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        # Simulate data from storage/database
        stored_data = {
            "event_id": str(uuid4()),
            "event_type": "OrderCreated",
            "event_version": 1,
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Order",
            "aggregate_version": 1,
            "tenant_id": None,
            "actor_id": "user:123",
            "correlation_id": str(uuid4()),
            "causation_id": None,
            "occurred_at": "2024-01-15T12:00:00Z",
            "metadata": {},
            "order_number": "ORD-001",
            "customer_id": str(uuid4()),
        }

        # Use registry to get class and deserialize
        event_class = registry.get(stored_data["event_type"])
        event = event_class.model_validate(stored_data)

        assert isinstance(event, OrderCreated)
        assert event.order_number == "ORD-001"
        assert event.event_type == "OrderCreated"

    def test_deserialize_unknown_event_type_fails(self) -> None:
        """Deserializing unknown event type fails with helpful error."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        stored_data = {
            "event_type": "UnknownEvent",
            # ... other fields
        }

        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get(stored_data["event_type"])

        assert "UnknownEvent" in str(exc_info.value)
        assert "OrderCreated" in str(exc_info.value)  # Shows available


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
