"""
Shared test fixtures for the eventsource library.

This module provides reusable test fixtures including:
- Test event types (TestCreated, TestUpdated, TestDeleted)
- Test state models (CounterState, OrderState)
- Test aggregate implementations (CounterAggregate, OrderAggregate)
- Event factory for flexible test data creation

Usage:
    from tests.fixtures import (
        CounterState,
        OrderState,
        CounterIncremented,
        CounterDecremented,
        CounterNamed,
        CounterReset,
        OrderCreated,
        OrderItemAdded,
        OrderShipped,
        CounterAggregate,
        DeclarativeCounterAggregate,
        OrderAggregate,
        create_event,
    )
"""

from tests.fixtures.aggregates import (
    CounterAggregate,
    CounterState,
    DeclarativeCounterAggregate,
    OrderAggregate,
    OrderState,
)
from tests.fixtures.events import (
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    OrderCancelled,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    SampleEvent,
    UserRegistered,
    create_event,
)

__all__ = [
    # Events
    "CounterIncremented",
    "CounterDecremented",
    "CounterNamed",
    "CounterReset",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "OrderCancelled",
    "SampleEvent",
    "UserRegistered",
    # States
    "CounterState",
    "OrderState",
    # Aggregates
    "CounterAggregate",
    "DeclarativeCounterAggregate",
    "OrderAggregate",
    # Factories
    "create_event",
]
