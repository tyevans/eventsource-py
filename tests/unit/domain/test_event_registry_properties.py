"""
Property-based tests for EventRegistry invariants.

Verifies registration, lookup (get / get_or_none), membership checks
(contains / __contains__), sorted listing (list_types / list_classes),
duplicate detection, unregistration, and clear semantics across
randomized sets of event type names.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import DuplicateEventTypeError, EventTypeNotFoundError


class SampleEventA(DomainEvent):
    """Primary sample event class for property testing."""

    aggregate_type: str = "SampleAggregate"


class SampleEventB(DomainEvent):
    """Distinct sample event class for duplicate conflict testing."""

    aggregate_type: str = "SampleAggregate"


event_type_strategy = st.text(
    alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="._-"),
    min_size=1,
    max_size=40,
)

event_type_set_strategy = st.sets(event_type_strategy, min_size=0, max_size=25)


@given(type_names=event_type_set_strategy, missing_name=event_type_strategy)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_registry_registration_lookup_and_clear_invariants(
    type_names: set[str],
    missing_name: str,
) -> None:
    """Registering a set of event type names preserves get, get_or_none,
    membership, sorted list_types, and clear invariants."""
    registry = EventRegistry()
    assert len(registry) == 0
    assert bool(registry) is True

    for name in type_names:
        returned = registry.register(SampleEventA, event_type=name)
        assert returned is SampleEventA
        # Idempotent re-registration of the exact same class is a no-op
        assert registry.register(SampleEventA, event_type=name) is SampleEventA

    assert len(registry) == len(type_names)
    assert registry.list_types() == sorted(type_names)
    assert set(registry) == type_names
    assert len(registry.list_classes()) == len(type_names)

    for name in type_names:
        assert registry.contains(name) is True
        assert (name in registry) is True
        assert registry.get(name) is SampleEventA
        assert registry.get_or_none(name) is SampleEventA

    if missing_name not in type_names:
        assert registry.contains(missing_name) is False
        assert (missing_name in registry) is False
        assert registry.get_or_none(missing_name) is None
        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get(missing_name)
        assert exc_info.value.event_type == missing_name

    registry.clear()
    assert len(registry) == 0
    assert registry.list_types() == []
    for name in type_names:
        assert registry.contains(name) is False
        assert registry.get_or_none(name) is None


@given(type_name=event_type_strategy)
@settings(max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_registry_duplicate_conflict_and_unregister_invariants(
    type_name: str,
) -> None:
    """Registering a different class under an existing type name raises
    DuplicateEventTypeError until the type name is unregistered."""
    registry = EventRegistry()
    registry.register(SampleEventA, event_type=type_name)

    with pytest.raises(DuplicateEventTypeError) as exc_info:
        registry.register(SampleEventB, event_type=type_name)
    assert exc_info.value.event_type == type_name

    assert registry.unregister(type_name) is True
    assert registry.unregister(type_name) is False
    assert registry.contains(type_name) is False

    # After unregistering, SampleEventB can be registered under type_name
    registry.register(SampleEventB, event_type=type_name)
    assert registry.get(type_name) is SampleEventB
