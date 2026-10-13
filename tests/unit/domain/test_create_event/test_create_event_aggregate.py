"""Tests for create_event() with DeclarativeAggregate."""

from uuid import uuid4

import pytest

from tests.unit.domain.test_create_event._fixtures import DeclarativeOrderAggregate


class TestCreateEventWithDeclarativeAggregate:
    """Tests for create_event() with DeclarativeAggregate."""

    @pytest.fixture
    def order(self) -> DeclarativeOrderAggregate:
        return DeclarativeOrderAggregate(uuid4())

    def test_create_event_works_with_declarative(self, order: DeclarativeOrderAggregate) -> None:
        """create_event works with DeclarativeAggregate."""
        customer_id = uuid4()
        event = order.create(customer_id=customer_id)

        assert event.aggregate_id == order.aggregate_id
        assert event.aggregate_type == "Order"
        assert event.aggregate_version == 1
        assert event.customer_id == customer_id
        assert order.state is not None
        assert order.state.status == "created"

    def test_version_increments_correctly(self, order: DeclarativeOrderAggregate) -> None:
        """Version increments correctly with DeclarativeAggregate."""
        order.create(customer_id=uuid4())
        assert order.version == 1

        order.ship(tracking_number="TRACK-DECL")
        assert order.version == 2
