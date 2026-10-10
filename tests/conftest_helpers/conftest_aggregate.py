"""Aggregate fixtures for pytest test suite."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from tests.fixtures import (
    CounterAggregate,
    DeclarativeCounterAggregate,
    OrderAggregate,
)


@pytest.fixture
def aggregate_id() -> UUID:
    """
    Provide a random aggregate ID.

    Returns:
        A new random UUID for use as an aggregate identifier.
    """
    return uuid4()


@pytest.fixture
def counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a fresh CounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new CounterAggregate at version 0.
    """
    return CounterAggregate(aggregate_id)


@pytest.fixture
def declarative_counter_aggregate(aggregate_id: UUID) -> DeclarativeCounterAggregate:
    """
    Provide a fresh DeclarativeCounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new DeclarativeCounterAggregate at version 0.
    """
    return DeclarativeCounterAggregate(aggregate_id)


@pytest.fixture
def populated_counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a CounterAggregate with initial state.

    The counter is incremented by 10, giving it:
    - version = 1
    - state.value = 10

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A CounterAggregate with one event applied.
    """
    aggregate = CounterAggregate(aggregate_id)
    aggregate.increment(10)
    return aggregate


@pytest.fixture
def order_aggregate(aggregate_id: UUID) -> OrderAggregate:
    """
    Provide a fresh OrderAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new OrderAggregate at version 0.
    """
    return OrderAggregate(aggregate_id)


@pytest.fixture
def populated_order_aggregate(
    aggregate_id: UUID,
    customer_id: UUID,
) -> OrderAggregate:
    """
    Provide an OrderAggregate with items.

    The order has:
    - customer_id set
    - 2 items: Widget A ($10) and Widget B ($15)
    - total = 25.0
    - version = 3

    Args:
        aggregate_id: The aggregate ID fixture.
        customer_id: The customer ID fixture.

    Returns:
        An OrderAggregate with creation and item events applied.
    """
    aggregate = OrderAggregate(aggregate_id)
    aggregate.create(customer_id)
    aggregate.add_item("Widget A", Decimal("10.00"))
    aggregate.add_item("Widget B", Decimal("15.00"))
    return aggregate


__all__ = [
    "aggregate_id",
    "counter_aggregate",
    "declarative_counter_aggregate",
    "order_aggregate",
    "populated_counter_aggregate",
    "populated_order_aggregate",
]
