"""Tests for edge cases and state property consistency."""

from uuid import uuid4

from tests.unit.domain.test_deferred_state._fixtures import (
    ExtractionProcess,
    Order,
)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_deferred_aggregate_with_empty_history(self) -> None:
        """Deferred aggregate with empty history has no state."""
        process = ExtractionProcess(uuid4())
        process.load_from_history([])

        assert process.is_created is False
        assert process.state_or_none is None
        assert process.version == 0

    def test_multiple_aggregates_independent(self) -> None:
        """Multiple aggregate instances are independent."""
        process1 = ExtractionProcess(uuid4())
        process2 = ExtractionProcess(uuid4())

        process1.request(page_id=uuid4(), config={"key": "1"})

        assert process1.is_created is True
        assert process2.is_created is False

    def test_class_attribute_is_class_var(self) -> None:
        """requires_creation_event is a class variable, not instance."""
        process1 = ExtractionProcess(uuid4())
        process2 = ExtractionProcess(uuid4())

        # Same class attribute
        assert process1.requires_creation_event is process2.requires_creation_event
        assert ExtractionProcess.requires_creation_event is True

    def test_traditional_aggregate_class_attribute(self) -> None:
        """Traditional aggregate has requires_creation_event=False."""
        order1 = Order(uuid4())
        order2 = Order(uuid4())

        assert order1.requires_creation_event is False
        assert order2.requires_creation_event is False
        assert Order.requires_creation_event is False


class TestStatePropertyConsistency:
    """Tests for state property behavior consistency."""

    def test_state_and_state_or_none_consistent_when_created(self) -> None:
        """state and state_or_none return same value when created."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()
        process.request(page_id=page_id, config={})

        assert process.state == process.state_or_none
        assert process.state is process.state_or_none

    def test_state_and_state_or_none_consistent_traditional_after_events(self) -> None:
        """state and state_or_none return same value for traditional aggregate after events."""
        order = Order(uuid4())
        order.create(uuid4())

        assert order.state == order.state_or_none
        assert order.state is order.state_or_none

    def test_state_and_state_or_none_both_none_before_events(self) -> None:
        """state and state_or_none both return None for traditional aggregate before events."""
        order = Order(uuid4())

        # Both should be None before events
        assert order.state is None
        assert order.state_or_none is None
        assert order.state is order.state_or_none

    def test_is_created_consistent_with_state_or_none(self) -> None:
        """is_created is consistent with state_or_none being not None."""
        process = ExtractionProcess(uuid4())

        assert (process.state_or_none is not None) == process.is_created

        process.request(page_id=uuid4(), config={})

        assert (process.state_or_none is not None) == process.is_created

    def test_is_created_consistent_traditional(self) -> None:
        """is_created is consistent for traditional aggregates."""
        order = Order(uuid4())

        assert (order.state_or_none is not None) == order.is_created

        order.create(uuid4())

        assert (order.state_or_none is not None) == order.is_created
