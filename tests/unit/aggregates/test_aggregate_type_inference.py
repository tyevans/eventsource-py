"""
Unit tests for AggregateRepository type inference (DX-006).

Tests cover:
- Inference of aggregate_type from factory.aggregate_type attribute
- Explicit aggregate_type parameter overrides inference
- ValueError raised when inference fails
- Rejection of "Unknown" and empty string as inferred values
- Backward compatibility with existing code
"""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.aggregates.repository import AggregateRepository
from eventsource.events.base import DomainEvent

# =============================================================================
# Test fixtures: State models and Aggregates
# =============================================================================


class OrderState(BaseModel):
    """Simple state for testing."""

    order_id: UUID
    status: str = "draft"


class OrderCreated(DomainEvent):
    """Event for testing."""

    event_type: str = "OrderCreated"
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

    # No aggregate_type defined (uses default "Unknown")

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

    # Explicitly set to "Unknown" to test rejection
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


# =============================================================================
# Test Classes
# =============================================================================


class TestAggregateTypeInference:
    """Tests for automatic aggregate_type inference."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        """Create a mock event store."""
        return MagicMock()

    def test_infers_from_factory_attribute(self, mock_store: MagicMock) -> None:
        """aggregate_type is inferred from factory.aggregate_type."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            # No aggregate_type provided - should be inferred
        )
        assert repo.aggregate_type == "Order"

    def test_explicit_overrides_inference(self, mock_store: MagicMock) -> None:
        """Explicit aggregate_type parameter overrides inference."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="CustomOrder",  # Explicit override
        )
        assert repo.aggregate_type == "CustomOrder"

    def test_raises_when_default_unknown(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate uses default 'Unknown'."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(
                event_store=mock_store,
                aggregate_factory=AggregateWithoutType,
            )

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithoutType" in error_msg
        assert "aggregate_type =" in error_msg  # Suggests adding attribute

    def test_raises_when_explicit_unknown(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate_type is explicitly 'Unknown'."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(
                event_store=mock_store,
                aggregate_factory=AggregateWithUnknownType,
            )

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithUnknownType" in error_msg

    def test_raises_when_empty_string(self, mock_store: MagicMock) -> None:
        """ValueError raised when aggregate_type is empty string."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(
                event_store=mock_store,
                aggregate_factory=AggregateWithEmptyType,
            )

        error_msg = str(exc_info.value)
        assert "Cannot infer aggregate_type" in error_msg
        assert "AggregateWithEmptyType" in error_msg

    def test_error_message_includes_class_name(self, mock_store: MagicMock) -> None:
        """Error message should include the aggregate class name."""
        with pytest.raises(ValueError) as exc_info:
            AggregateRepository(
                event_store=mock_store,
                aggregate_factory=AggregateWithoutType,
            )

        error_msg = str(exc_info.value)
        # Check helpful suggestions are included
        assert "AggregateWithoutType" in error_msg
        assert "aggregate_type =" in error_msg
        assert "AggregateRepository(..., aggregate_type=" in error_msg

    def test_different_aggregates_infer_correctly(self, mock_store: MagicMock) -> None:
        """Different aggregate types should infer their own types."""
        order_repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
        )
        invoice_repo: AggregateRepository[InvoiceAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=InvoiceAggregate,
        )

        assert order_repo.aggregate_type == "Order"
        assert invoice_repo.aggregate_type == "Invoice"


class TestBackwardCompatibility:
    """Tests to ensure backward compatibility with existing code."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        """Create a mock event store."""
        return MagicMock()

    def test_explicit_type_still_works(self, mock_store: MagicMock) -> None:
        """Existing code with explicit type continues to work."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="ExplicitOrder",
        )
        assert repo.aggregate_type == "ExplicitOrder"

    def test_explicit_type_with_all_params(self, mock_store: MagicMock) -> None:
        """Repository with all parameters still works."""
        publisher = MagicMock()
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            event_publisher=publisher,
            enable_tracing=False,
        )
        assert repo.aggregate_type == "Order"
        assert repo.event_publisher is publisher

    def test_can_use_explicit_unknown_override(self, mock_store: MagicMock) -> None:
        """Can still explicitly set 'Unknown' if really needed."""
        # If user explicitly passes "Unknown", it should work (not recommended but allowed)
        repo: AggregateRepository[AggregateWithUnknownType] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=AggregateWithUnknownType,
            aggregate_type="Unknown",  # Explicit - allowed
        )
        assert repo.aggregate_type == "Unknown"

    def test_explicit_empty_allowed_but_not_recommended(self, mock_store: MagicMock) -> None:
        """Can explicitly set empty string if needed (not recommended)."""
        repo: AggregateRepository[AggregateWithEmptyType] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=AggregateWithEmptyType,
            aggregate_type="",  # Explicit - allowed
        )
        assert repo.aggregate_type == ""


class TestPropertyAccess:
    """Tests for aggregate_type property access."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        """Create a mock event store."""
        return MagicMock()

    def test_aggregate_type_property_returns_inferred(self, mock_store: MagicMock) -> None:
        """Property returns correctly inferred type."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
        )
        assert repo.aggregate_type == "Order"

    def test_aggregate_type_property_returns_explicit(self, mock_store: MagicMock) -> None:
        """Property returns explicitly provided type."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="MyOrder",
        )
        assert repo.aggregate_type == "MyOrder"


class TestEdgeCases:
    """Tests for edge cases in type inference."""

    @pytest.fixture
    def mock_store(self) -> MagicMock:
        """Create a mock event store."""
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
            event_store=mock_store,
            aggregate_factory=SpecialAggregate,
        )
        assert repo.aggregate_type == "Order_V2-Beta"

    def test_aggregate_type_with_whitespace_only_rejected(self, mock_store: MagicMock) -> None:
        """aggregate_type with only whitespace is still valid (not empty)."""

        class WhitespaceAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "   "  # Whitespace only

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        # Whitespace-only is technically a non-empty string, so it's allowed
        # (even though it's not recommended)
        repo: AggregateRepository[WhitespaceAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=WhitespaceAggregate,
        )
        assert repo.aggregate_type == "   "

    def test_none_aggregate_type_triggers_inference(self, mock_store: MagicMock) -> None:
        """Passing None explicitly should trigger inference."""
        repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=OrderAggregate,
            aggregate_type=None,  # Explicit None triggers inference
        )
        assert repo.aggregate_type == "Order"

    def test_aggregate_type_inference_with_inheritance(self, mock_store: MagicMock) -> None:
        """Type inference should work with inherited aggregate_type."""

        class BaseOrderAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "BaseOrder"

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        class DerivedOrderAggregate(BaseOrderAggregate):
            # Inherits aggregate_type = "BaseOrder"
            pass

        repo: AggregateRepository[DerivedOrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=DerivedOrderAggregate,
        )
        # Should infer from inherited attribute
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
            aggregate_type = "DerivedOrder"  # Override

        repo: AggregateRepository[DerivedOrderAggregate] = AggregateRepository(
            event_store=mock_store,
            aggregate_factory=DerivedOrderAggregate,
        )
        assert repo.aggregate_type == "DerivedOrder"


class TestInferenceMethodDirectly:
    """Tests for the _infer_aggregate_type method directly."""

    @pytest.fixture
    def repo_with_order(self) -> AggregateRepository[OrderAggregate]:
        """Create a repository instance for testing the method."""
        return AggregateRepository(
            event_store=MagicMock(),
            aggregate_factory=OrderAggregate,
        )

    def test_infer_returns_correct_type(
        self, repo_with_order: AggregateRepository[OrderAggregate]
    ) -> None:
        """_infer_aggregate_type returns correct type for valid factory."""
        result = repo_with_order._infer_aggregate_type(OrderAggregate)
        assert result == "Order"

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
