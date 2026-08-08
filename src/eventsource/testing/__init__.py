"""
Test utilities for eventsource-py.

This module provides testing helpers to reduce boilerplate when writing
tests for event-sourced applications.

Components:
    EventBuilder: Fluent builder for creating test events with minimal boilerplate
    InMemoryTestHarness: Pre-configured in-memory infrastructure for fast tests
    EventAssertions: Domain-specific test assertions with clear error messages
    BDD helpers: given_events, when_command, then_event_published for readable tests
    DeciderScenario: given/when/then over a decider's pure functions, no I/O
    RecordingEventBus: an event bus that records what was published
    Port conformance suites: AppenderConformance and friends -- subclass one to
        prove a custom backend satisfies the port contract. See
        docs/guides/validate-custom-backend.md.

Example:
    >>> from eventsource.testing import EventBuilder, InMemoryTestHarness
    >>> from eventsource.testing import given_events, when_command, then_event_published
    >>>
    >>> # Create a test harness with in-memory infrastructure
    >>> harness = InMemoryTestHarness()
    >>>
    >>> # Build test events easily
    >>> event = EventBuilder(OrderCreated).with_aggregate_id(order_id).build()
    >>>
    >>> # Use BDD-style assertions
    >>> await given_events(harness, [event])
    >>> await when_command(harness, ShipOrderCommand(order_id=order_id))
    >>> then_event_published(harness, OrderShipped)

Note:
    This module is optional and intended for test code only. It should not
    be imported in production code paths.
"""

from eventsource.testing import conformance_ports
from eventsource.testing.assertions import EventAssertions
from eventsource.testing.bdd import (
    DeciderScenario,
    given_events,
    then_event_count,
    then_event_published,
    then_event_sequence,
    then_no_events_published,
    when_command,
)
from eventsource.testing.builder import EventBuilder
from eventsource.testing.conformance import EventBusConformanceSuite

# Re-export every port conformance suite. Starred deliberately: the suite roster
# lives in exactly one place (conformance_ports.__all__), so a suite added there
# is reachable from `eventsource.testing` with no second list to keep in sync.
from eventsource.testing.conformance_ports import *  # noqa: F403
from eventsource.testing.harness import InMemoryTestHarness
from eventsource.testing.recording import RecordingEventBus

__all__ = [
    # Core classes
    "EventBuilder",
    "InMemoryTestHarness",
    "EventAssertions",
    "RecordingEventBus",
    # BDD helpers
    "given_events",
    "when_command",
    "then_event_published",
    "then_no_events_published",
    "then_event_sequence",
    "then_event_count",
    "DeciderScenario",
    # Conformance test suite
    "EventBusConformanceSuite",
    # Port conformance suites are appended below, from the single roster in
    # conformance_ports.__all__ -- do not list them here.
    *conformance_ports.__all__,
]
