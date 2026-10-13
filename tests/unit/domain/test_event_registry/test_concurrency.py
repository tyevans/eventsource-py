"""Tests for registry isolation and thread safety."""

import threading
from concurrent.futures import ThreadPoolExecutor

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from tests.unit.domain.test_event_registry._fixtures import (
    OrderCreated,
    OrderShipped,
)


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
