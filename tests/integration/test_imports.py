"""Test that all modules can be imported without circular import errors."""


def test_no_circular_imports():
    """Verify all public modules import cleanly."""
    # Core modules

    # Aggregates

    # Handler modules (formerly had circular dependency)

    # Projection modules

    # All imports succeeded = no circular dependencies


def test_handler_registry_imports_from_domain_decorators():
    """Verify HandlerRegistry uses the domain decorator location."""
    import inspect

    from eventsource.application.projections import handlers

    source = inspect.getsource(handlers)
    assert "from eventsource.domain.decorators import" in source


def test_direct_imports_work():
    """Verify direct imports resolve without circular import."""
    from eventsource.application.projections.handlers import HandlerInfo, HandlerRegistry
    from eventsource.domain.decorators import handles

    # Verify they are the actual classes/functions
    assert callable(handles)
    assert hasattr(HandlerRegistry, "__init__")
    assert hasattr(HandlerInfo, "__init__")


def test_top_level_import_matches_domain_decorators_import():
    """Verify top-level and domain.decorators imports resolve to same objects."""
    from eventsource import handles
    from eventsource.domain.decorators import handles as h2

    assert handles is h2


def test_all_decorator_utilities_accessible():
    """Verify all decorator utilities are accessible from domain.decorators."""
    from eventsource.domain.decorators import (
        get_handled_event_type,
        handles,
        is_event_handler,
    )

    # Verify they work
    assert callable(handles)
    assert callable(get_handled_event_type)
    assert callable(is_event_handler)

    # Test basic functionality
    from eventsource.domain.event import DomainEvent

    class TestEvent(DomainEvent):
        pass

    @handles(TestEvent)
    def test_handler(event):
        pass

    assert is_event_handler(test_handler)
    assert get_handled_event_type(test_handler) is TestEvent
    assert not is_event_handler(lambda: None)
    assert get_handled_event_type(lambda: None) is None
