"""Tests for domain event causation tracking and metadata handling."""

from uuid import uuid4

from tests.unit.domain.test_domain_event._fixtures import OrderCreated, OrderShipped


def _make_order_created(order_number: str = "ORD-001", total_amount: float = 99.99, **kwargs):
    return OrderCreated(
        aggregate_id=uuid4(),
        order_number=order_number,
        customer_id=uuid4(),
        total_amount=total_amount,
        **kwargs,
    )


class TestDomainEventCausation:
    """Tests for causation and correlation tracking."""

    def test_with_causation_sets_causation_id(self) -> None:
        cause = _make_order_created()
        effect = OrderShipped(
            aggregate_id=cause.aggregate_id, tracking_number="TRK-001", carrier="FedEx"
        )
        caused = effect.with_causation(cause)

        assert caused.causation_id == cause.event_id
        assert caused.correlation_id == cause.correlation_id

    def test_with_causation_preserves_other_fields(self) -> None:
        cause = _make_order_created()
        aggregate_id = uuid4()
        effect = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRK-001",
            carrier="FedEx",
            actor_id="system:shipping",
            metadata={"batch_id": "123"},
        )
        caused = effect.with_causation(cause)

        assert caused.aggregate_id == aggregate_id
        assert caused.tracking_number == "TRK-001"
        assert caused.carrier == "FedEx"
        assert caused.actor_id == "system:shipping"
        assert caused.metadata == {"batch_id": "123"}
        assert caused.event_id == effect.event_id

    def test_with_causation_creates_new_instance(self) -> None:
        cause = _make_order_created()
        effect = OrderShipped(
            aggregate_id=cause.aggregate_id, tracking_number="TRK-001", carrier="FedEx"
        )
        caused = effect.with_causation(cause)

        assert caused is not effect
        assert effect.causation_id is None

    def test_is_caused_by_returns_true_for_cause(self) -> None:
        cause = _make_order_created()
        effect = OrderShipped(
            aggregate_id=cause.aggregate_id, tracking_number="TRK-001", carrier="FedEx"
        ).with_causation(cause)

        assert effect.is_caused_by(cause) is True

    def test_is_caused_by_returns_false_for_non_cause(self) -> None:
        event1 = _make_order_created("ORD-001", 99.99)
        event2 = _make_order_created("ORD-002", 50.00)

        assert event1.is_caused_by(event2) is False
        assert event2.is_caused_by(event1) is False

    def test_is_correlated_with_returns_true_for_same_correlation(self) -> None:
        cause = _make_order_created()
        effect = OrderShipped(
            aggregate_id=cause.aggregate_id, tracking_number="TRK-001", carrier="FedEx"
        ).with_causation(cause)

        assert cause.is_correlated_with(effect) is True
        assert effect.is_correlated_with(cause) is True

    def test_is_correlated_with_returns_false_for_different_correlation(self) -> None:
        event1 = _make_order_created("ORD-001", 99.99)
        event2 = _make_order_created("ORD-002", 50.00)

        assert event1.is_correlated_with(event2) is False


class TestDomainEventMetadata:
    """Tests for metadata handling."""

    def test_with_metadata_adds_metadata(self) -> None:
        event = _make_order_created()
        enriched = event.with_metadata(trace_id="abc123", source="api")

        assert enriched.metadata["trace_id"] == "abc123"
        assert enriched.metadata["source"] == "api"

    def test_with_metadata_preserves_existing(self) -> None:
        event = _make_order_created(metadata={"existing": "value"})
        enriched = event.with_metadata(new_key="new_value")

        assert enriched.metadata["existing"] == "value"
        assert enriched.metadata["new_key"] == "new_value"

    def test_with_metadata_creates_new_instance(self) -> None:
        event = _make_order_created()
        enriched = event.with_metadata(key="value")

        assert enriched is not event
        assert event.metadata == {}

    def test_with_metadata_can_override_values(self) -> None:
        event = _make_order_created(metadata={"key": "original"})
        enriched = event.with_metadata(key="updated")

        assert enriched.metadata["key"] == "updated"
