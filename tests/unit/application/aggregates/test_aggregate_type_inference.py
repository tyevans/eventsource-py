"""Unit tests for AggregateRepository type inference (DX-006)."""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.event import DomainEvent


class OrderState(BaseModel):
    """Simple state for testing."""

    order_id: UUID
    status: str = "draft"


class OrderCreated(DomainEvent):
    """Event for testing."""

    aggregate_type: str = "Order"


class OrderAggregate(DeclarativeAggregate[OrderState]):
    """Aggregate with properly configured aggregate_type."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(order_id=self.aggregate_id, status="created")


class AggregateWithoutType(DeclarativeAggregate[OrderState]):
    """Aggregate that relies on default 'Unknown' aggregate_type."""

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        pass


class AggregateWithEmptyType(DeclarativeAggregate[OrderState]):
    """Aggregate with empty string aggregate_type."""

    aggregate_type = ""

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        pass


class AggregateWithUnknownType(AggregateRoot[OrderState]):
    """Aggregate using base class default 'Unknown'."""

    aggregate_type = "Unknown"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        pass


class InvoiceAggregate(DeclarativeAggregate[OrderState]):
    """Another aggregate with configured type for multiple repo tests."""

    aggregate_type = "Invoice"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        pass


class TestAggregateTypeInference:
    """Tests for automatic aggregate_type inference."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        return MagicMock()

    def test_infers_from_factory_attribute(self, mock_store: MagicMock) -> None:
        """aggregate_type is inferred from factory.aggregate_type."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=OrderAggregate
        )
        assert repo.aggregate_type == "Order"

    def test_raises_when_default_unknown(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate uses default 'Unknown'."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(event_store=mock_store, aggregate_factory=AggregateWithoutType)

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithoutType" in error_msg
        assert "aggregate_type =" in error_msg

    def test_raises_when_explicit_unknown(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate_type is explicitly 'Unknown'."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(event_store=mock_store, aggregate_factory=AggregateWithUnknownType)

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithUnknownType" in error_msg

    def test_raises_when_empty_string(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate_type is empty string."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(event_store=mock_store, aggregate_factory=AggregateWithEmptyType)

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithEmptyType" in error_msg

    def test_error_message_includes_class_name(self, mock_store: MagicMock) -> None:
        """Error message should include the aggregate class name."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(event_store=mock_store, aggregate_factory=AggregateWithoutType)

        error_msg = str(exc_info.value)
        assert "AggregateWithoutType" in error_msg
        assert "aggregate_type =" in error_msg

    def test_different_aggregates_infer_correctly(self, mock_store: MagicMock) -> None:
        """Different aggregate types should infer their own types."""
        order_repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=OrderAggregate
        )
        invoice_repo: AggregateRepository[InvoiceAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=InvoiceAggregate
        )
        assert order_repo.aggregate_type == "Order"
        assert invoice_repo.aggregate_type == "Invoice"


class TestPropertyAccess:
    """Tests for aggregate_type property access."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        return MagicMock()

    def test_aggregate_type_property_returns_inferred(self, mock_store: MagicMock) -> None:
        """Property returns correctly inferred type."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=OrderAggregate
        )
        assert repo.aggregate_type == "Order"


class TestEdgeCases:
    """Tests for edge cases in type inference."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        return MagicMock()

    def test_aggregate_type_with_special_characters(self, mock_store: MagicMock) -> None:
        """aggregate_type with special characters should work."""

        class SpecialAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "Order_V2-Beta"

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        repo: AggregateRepository[SpecialAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=SpecialAggregate
        )
        assert repo.aggregate_type == "Order_V2-Beta"

    def test_aggregate_type_with_whitespace_only_rejected(self, mock_store: MagicMock) -> None:
        """aggregate_type with only whitespace is still valid (not empty)."""

        class WhitespaceAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "   "

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        repo: AggregateRepository[WhitespaceAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=WhitespaceAggregate
        )
        assert repo.aggregate_type == "   "

    def test_aggregate_type_inference_with_inheritance(self, mock_store: MagicMock) -> None:
        """Type inference should work with inherited aggregate_type."""

        class BaseOrderAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "BaseOrder"

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        class DerivedOrderAggregate(BaseOrderAggregate):
            pass

        repo: AggregateRepository[DerivedOrderAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=DerivedOrderAggregate
        )
        assert repo.aggregate_type == "BaseOrder"

    def test_aggregate_type_override_in_derived(self, mock_store: MagicMock) -> None:
        """Derived class can override aggregate_type."""

        class BaseOrderAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "BaseOrder"

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        class DerivedOrderAggregate(BaseOrderAggregate):
            aggregate_type = "DerivedOrder"

        repo: AggregateRepository[DerivedOrderAggregate] = AggregateRepository(
            event_store=mock_store, aggregate_factory=DerivedOrderAggregate
        )
        assert repo.aggregate_type == "DerivedOrder"


class TestInferenceMethodDirectly:
    """Tests for the _infer_aggregate_type method directly."""

    @pytest.fixture
    def repo_with_order(self) -> AggregateRepository[OrderAggregate]:
        return AggregateRepository(event_store=MagicMock(), aggregate_factory=OrderAggregate)

    def test_infer_returns_correct_type(
        self, repo_with_order: AggregateRepository[OrderAggregate]
    ) -> None:
        """_infer_aggregate_type returns correct type for valid factory."""
        assert repo_with_order._infer_aggregate_type(OrderAggregate) == "Order"

    def test_infer_raises_for_unknown(
        self, repo_with_order: AggregateRepository[OrderAggregate]
    ) -> None:
        """_infer_aggregate_type raises for Unknown type."""
        with pytest.raises(ValueError) as exc_info:
            repo_with_order._infer_aggregate_type(AggregateWithUnknownType)
        assert "Cannot infer aggregate_type" in str(exc_info.value)

    def test_infer_raises_for_empty(
        self, repo_with_order: AggregateRepository[OrderAggregate]
    ) -> None:
        """_infer_aggregate_type raises for empty string type."""
        with pytest.raises(ValueError) as exc_info:
            repo_with_order._infer_aggregate_type(AggregateWithEmptyType)
        assert "Cannot infer aggregate_type" in str(exc_info.value)
