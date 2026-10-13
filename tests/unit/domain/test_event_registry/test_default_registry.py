"""Tests for the default module-level registry."""

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import (
    default_registry,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    register_event,
)
from tests.unit.domain.test_event_registry._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestDefaultRegistry:
    """Tests for the default module-level registry."""

    def setup_method(self) -> None:
        """Snapshot the default registry, then clear it so the test starts empty.

        `default_registry` is process-global and populated at import time by
        every `@register_event` in the tree. Clearing it without restoring
        leaks an empty registry into every test that runs afterwards -- which
        now fails at append time, because the in-memory stores resolve event
        types through it.
        """
        self._saved = {
            event_type: default_registry.get(event_type)
            for event_type in default_registry.list_types()
        }
        default_registry.clear()

    def teardown_method(self) -> None:
        """Restore exactly the registrations that existed before this test."""
        default_registry.clear()
        for event_type, event_class in self._saved.items():
            default_registry.register(event_class, event_type)

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
