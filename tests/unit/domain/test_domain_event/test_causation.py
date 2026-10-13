"""Tests for domain event causation tracking and metadata handling."""

from uuid import uuid4

from tests.unit.domain.test_domain_event._fixtures import OrderCreated, OrderShipped


class TestDomainEventCausation:
    """Tests for causation and correlation tracking."""

    def test_with_causation_sets_causation_id(self) -> None:
        """with_causation() sets causation_id from causing event."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        )

        caused = effect.with_causation(cause)

        assert caused.causation_id == cause.event_id
        assert caused.correlation_id == cause.correlation_id

    def test_with_causation_preserves_other_fields(self) -> None:
        """with_causation() preserves all other event fields."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        aggregate_id = uuid4()
        effect = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
            actor_id="system:shipping",
            metadata={"batch_id": "123"},
        )

        caused = effect.with_causation(cause)

        # Original fields preserved
        assert caused.aggregate_id == aggregate_id
        assert caused.tracking_number == "TRK-001"
        assert caused.carrier == "FedEx"
        assert caused.actor_id == "system:shipping"
        assert caused.metadata == {"batch_id": "123"}
        assert caused.event_id == effect.event_id  # Same event ID

    def test_with_causation_creates_new_instance(self) -> None:
        """with_causation() returns a new event instance."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        )

        caused = effect.with_causation(cause)

        assert caused is not effect
        assert effect.causation_id is None  # Original unchanged

    def test_is_caused_by_returns_true_for_cause(self) -> None:
        """is_caused_by() returns True for the causing event."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        ).with_causation(cause)

        assert effect.is_caused_by(cause) is True

    def test_is_caused_by_returns_false_for_non_cause(self) -> None:
        """is_caused_by() returns False for unrelated events."""
        event1 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        event2 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-002",
            customer_id=uuid4(),
            total_amount=50.00,
        )

        assert event1.is_caused_by(event2) is False
        assert event2.is_caused_by(event1) is False

    def test_is_correlated_with_returns_true_for_same_correlation(self) -> None:
        """is_correlated_with() returns True for same correlation_id."""
        cause = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        effect = OrderShipped(
            aggregate_id=cause.aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
        ).with_causation(cause)

        assert cause.is_correlated_with(effect) is True
        assert effect.is_correlated_with(cause) is True

    def test_is_correlated_with_returns_false_for_different_correlation(self) -> None:
        """is_correlated_with() returns False for different correlation_ids."""
        event1 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        event2 = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-002",
            customer_id=uuid4(),
            total_amount=50.00,
        )

        assert event1.is_correlated_with(event2) is False


class TestDomainEventMetadata:
    """Tests for metadata handling."""

    def test_with_metadata_adds_metadata(self) -> None:
        """with_metadata() adds new metadata fields."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        enriched = event.with_metadata(trace_id="abc123", source="api")

        assert enriched.metadata["trace_id"] == "abc123"
        assert enriched.metadata["source"] == "api"

    def test_with_metadata_preserves_existing(self) -> None:
        """with_metadata() preserves existing metadata."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"existing": "value"},
        )

        enriched = event.with_metadata(new_key="new_value")

        assert enriched.metadata["existing"] == "value"
        assert enriched.metadata["new_key"] == "new_value"

    def test_with_metadata_creates_new_instance(self) -> None:
        """with_metadata() returns a new event instance."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
        )

        enriched = event.with_metadata(key="value")

        assert enriched is not event
        assert event.metadata == {}  # Original unchanged

    def test_with_metadata_can_override_values(self) -> None:
        """with_metadata() can override existing metadata values."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
            total_amount=99.99,
            metadata={"key": "original"},
        )

        enriched = event.with_metadata(key="updated")

        assert enriched.metadata["key"] == "updated"
