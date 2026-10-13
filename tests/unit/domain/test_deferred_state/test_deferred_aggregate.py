"""Tests for aggregates with requires_creation_event=True (deferred state)."""

from uuid import uuid4

import pytest

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.exceptions import AggregateNotCreatedError
from tests.unit.domain.test_deferred_state._fixtures import (
    ExtractionCompleted,
    ExtractionProcess,
    ExtractionRequested,
    ExtractionState,
    OrderState,
)


class TestDeferredStateAggregate:
    """Tests for aggregates with requires_creation_event=True."""

    def test_state_raises_before_creation(self) -> None:
        """Accessing state before creation raises error."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert "ExtractionProcess" in str(exc_info.value)
        assert "has not been created" in str(exc_info.value)

    def test_state_works_after_creation(self) -> None:
        """Accessing state after creation works."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={})

        assert process.state.page_id == page_id
        assert process.state.status == "requested"

    def test_state_or_none_returns_none_before_creation(self) -> None:
        """state_or_none returns None before creation."""
        process = ExtractionProcess(uuid4())
        assert process.state_or_none is None

    def test_state_or_none_returns_state_after_creation(self) -> None:
        """state_or_none returns state after creation."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={})

        assert process.state_or_none is not None
        assert process.state_or_none.page_id == page_id

    def test_is_created_false_before_creation(self) -> None:
        """is_created returns False before any events."""
        process = ExtractionProcess(uuid4())
        assert process.is_created is False

    def test_is_created_true_after_creation(self) -> None:
        """is_created returns True after events applied."""
        process = ExtractionProcess(uuid4())
        process.request(page_id=uuid4(), config={})
        assert process.is_created is True

    def test_get_initial_state_returns_none(self) -> None:
        """_get_initial_state returns None for deferred state."""
        process = ExtractionProcess(uuid4())
        assert process._get_initial_state() is None

    def test_multiple_events_after_creation(self) -> None:
        """Can apply multiple events after creation."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={"key": "value"})
        assert process.state.status == "requested"

        process.complete(result="success")
        assert process.state.status == "completed"
        assert process.version == 2

    def test_version_zero_before_creation(self) -> None:
        """Version is 0 before any events."""
        process = ExtractionProcess(uuid4())
        assert process.version == 0

    def test_version_increments_after_creation(self) -> None:
        """Version increments with each event."""
        process = ExtractionProcess(uuid4())

        process.request(page_id=uuid4(), config={})
        assert process.version == 1

        process.complete(result="done")
        assert process.version == 2

    def test_uncommitted_events_tracked(self) -> None:
        """Uncommitted events are tracked correctly."""
        process = ExtractionProcess(uuid4())

        event1 = process.request(page_id=uuid4(), config={})
        event2 = process.complete(result="done")

        assert len(process.uncommitted_events) == 2
        assert event1 in process.uncommitted_events
        assert event2 in process.uncommitted_events


class TestDeferredStateAggregateWithReplay:
    """Tests for deferred state aggregates with event replay."""

    def test_load_from_history(self) -> None:
        """Can load aggregate from event history."""
        aggregate_id = uuid4()
        page_id = uuid4()

        # Create events as if from event store
        event1 = ExtractionRequested(
            aggregate_id=aggregate_id,
            aggregate_type="Extraction",
            aggregate_version=1,
            page_id=page_id,
            config={"key": "value"},
        )
        event2 = ExtractionCompleted(
            aggregate_id=aggregate_id,
            aggregate_type="Extraction",
            aggregate_version=2,
            result="success",
        )

        # Load from history
        process = ExtractionProcess(aggregate_id)
        process.load_from_history([event1, event2])

        assert process.is_created is True
        assert process.state.page_id == page_id
        assert process.state.status == "completed"
        assert process.version == 2
        assert len(process.uncommitted_events) == 0


class TestDeferredStateWithInheritance:
    """Tests for deferred state with aggregate inheritance."""

    def test_subclass_inherits_requires_creation_event(self) -> None:
        """Subclass can inherit requires_creation_event."""

        class BaseExtraction(DeclarativeAggregate[ExtractionState]):
            aggregate_type = "Extraction"
            requires_creation_event = True

            def _get_initial_state(self) -> ExtractionState | None:
                return None

        class SpecialExtraction(BaseExtraction):
            pass

        process = SpecialExtraction(uuid4())
        assert process.requires_creation_event is True
        assert process.is_created is False

    def test_subclass_can_override_requires_creation_event(self) -> None:
        """Subclass can override requires_creation_event."""

        class BaseAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "Base"
            requires_creation_event = True

            def _get_initial_state(self) -> OrderState | None:
                return None

        class TraditionalSubclass(BaseAggregate):
            requires_creation_event = False

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

        agg = TraditionalSubclass(uuid4())
        assert agg.requires_creation_event is False
        # State is still None until events are applied (existing library behavior)
        assert agg.is_created is False
        assert agg.state is None
        # But _get_initial_state() returns a valid state when called
        assert agg._get_initial_state() is not None
