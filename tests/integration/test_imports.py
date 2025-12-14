"""Test that all modules can be imported without circular import errors."""


def test_no_circular_imports():
    """Verify all public modules import cleanly."""
    # Core modules

    # Aggregates

    # Handler modules (formerly had circular dependency)

    # Projection modules

    # All imports succeeded = no circular dependencies


def test_handlers_registry_imports_from_handlers():
    """Verify registry uses the new decorator location."""
    import inspect

    from eventsource.handlers import registry

    source = inspect.getsource(registry)
    assert "from eventsource.handlers.decorators import" in source


def test_handlers_init_no_lazy_import():
    """Verify handlers/__init__.py has no lazy import workaround."""
    import inspect

    from eventsource import handlers

    source = inspect.getsource(handlers)
    # Should not have __getattr__ for lazy imports anymore
    assert "__getattr__" not in source, (
        "handlers/__init__.py should not have __getattr__ lazy import workaround"
    )


def test_direct_imports_work():
    """Verify direct imports from handlers work without circular import."""
    from eventsource.handlers import HandlerInfo, HandlerRegistry, handles

    # Verify they are the actual classes/functions
    assert callable(handles)
    assert hasattr(HandlerRegistry, "__init__")
    assert hasattr(HandlerInfo, "__init__")


def test_top_level_import_matches_handlers_import():
    """Verify top-level and handlers imports resolve to same objects."""
    from eventsource import handles
    from eventsource.handlers import handles as h2

    assert handles is h2


def test_projections_import_matches_handlers_import():
    """Verify projections re-export resolves to same object as handlers."""
    from eventsource.handlers import handles as h1
    from eventsource.projections import handles as h2

    assert h1 is h2


def test_all_decorator_utilities_accessible():
    """Verify all decorator utilities are accessible from handlers."""
    from eventsource.handlers import (
        get_handled_event_type,
        handles,
        is_event_handler,
    )

    # Verify they work
    assert callable(handles)
    assert callable(get_handled_event_type)
    assert callable(is_event_handler)

    # Test basic functionality
    from eventsource.events.base import DomainEvent

    class TestEvent(DomainEvent):
        pass

    @handles(TestEvent)
    def test_handler(event):
        pass

    assert is_event_handler(test_handler)
    assert get_handled_event_type(test_handler) is TestEvent
    assert not is_event_handler(lambda: None)
    assert get_handled_event_type(lambda: None) is None
