"""Tests for event registry basic operations, lookup, resolution, and iteration."""

import pytest

from eventsource.domain.event_registry import (
    EventRegistry,
    EventTypeNotFoundError,
)
from tests.unit.domain.test_event_registry._fixtures import (
    EventWithoutTypeDefault,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
)


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
