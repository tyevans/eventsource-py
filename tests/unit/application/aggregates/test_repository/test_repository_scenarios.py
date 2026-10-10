"""Tests for complex scenarios, generic typing, imports, and edge cases."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    OrderAggregate,
)


class TestComplexScenarios:
    """Tests for complex usage scenarios."""

    @pytest.mark.asyncio
    async def test_full_order_lifecycle(
        self,
        order_repository: AggregateRepository[OrderAggregate],
    ) -> None:
        """Test complete order lifecycle through repository."""
        order_id = uuid4()
        customer_id = uuid4()

        # Create order
        order = order_repository.create_new(order_id)
        order.create(customer_id)
        await order_repository.save(order)

        # Add items
        order = await order_repository.load(order_id)
        order.add_item("Widget A", Decimal("10.00"))
        order.add_item("Widget B", Decimal("15.00"))
        await order_repository.save(order)

        # Ship order
        order = await order_repository.load(order_id)
        order.ship("TRACK123")
        await order_repository.save(order)

        # Verify final state
        final_order = await order_repository.load(order_id)
        assert final_order.version == 4
        assert final_order.state is not None
        assert final_order.state.status == "shipped"
        assert final_order.state.customer_id == customer_id
        assert final_order.state.items == ["Widget A", "Widget B"]
        assert final_order.state.total == 25.0

    @pytest.mark.asyncio
    async def test_multiple_aggregates_same_repository(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Repository should handle multiple aggregates correctly."""
        id1 = uuid4()
        id2 = uuid4()

        # Create first counter
        counter1 = counter_repository.create_new(id1)
        counter1.increment(10)
        await counter_repository.save(counter1)

        # Create second counter
        counter2 = counter_repository.create_new(id2)
        counter2.increment(20)
        await counter_repository.save(counter2)

        # Load and verify both
        loaded1 = await counter_repository.load(id1)
        loaded2 = await counter_repository.load(id2)

        assert loaded1.state.value == 10
        assert loaded2.state.value == 20

    @pytest.mark.asyncio
    async def test_save_multiple_times(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Multiple saves should work correctly."""
        agg_id = uuid4()
        aggregate = counter_repository.create_new(agg_id)

        for i in range(10):
            aggregate.increment(i + 1)
            await counter_repository.save(aggregate)

        loaded = await counter_repository.load(agg_id)
        assert loaded.version == 10
        assert loaded.state.value == 55  # Sum of 1 to 10


class TestGenericTyping:
    """Tests to verify generic typing works correctly."""

    @pytest.mark.asyncio
    async def test_different_aggregate_types_in_different_repos(
        self, event_store: InMemoryEventStore
    ) -> None:
        """Different repositories should handle different aggregate types."""
        counter_repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
        )

        order_repo: AggregateRepository[OrderAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
        )

        # Create and save counter
        counter = counter_repo.create_new(uuid4())
        counter.increment(5)
        await counter_repo.save(counter)

        # Create and save order
        order = order_repo.create_new(uuid4())
        order.create(uuid4())
        await order_repo.save(order)

        # Load and verify types
        loaded_counter = await counter_repo.load(counter.aggregate_id)
        loaded_order = await order_repo.load(order.aggregate_id)

        assert isinstance(loaded_counter, CounterAggregate)
        assert isinstance(loaded_order, OrderAggregate)


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_aggregates_module(self) -> None:
        """Test importing AggregateRepository from aggregates module."""
        from eventsource.application.aggregates import AggregateRepository

        assert AggregateRepository is not None

    def test_import_from_main_module(self) -> None:
        """Test importing AggregateRepository from main eventsource module."""
        from eventsource import AggregateRepository

        assert AggregateRepository is not None

    def test_import_aggregate_not_found_error(self) -> None:
        """Test importing AggregateNotFoundError."""
        from eventsource import AggregateNotFoundError

        assert AggregateNotFoundError is not None


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_large_number_of_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Repository should handle aggregates with many events."""
        aggregate = counter_repository.create_new(uuid4())

        for _ in range(100):
            aggregate.increment(1)

        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(aggregate.aggregate_id)
        assert loaded.version == 100
        assert loaded.state.value == 100

    @pytest.mark.asyncio
    async def test_save_batch_of_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should handle multiple uncommitted events correctly."""
        aggregate = counter_repository.create_new(uuid4())

        # Add many events before saving
        for _ in range(50):
            aggregate.increment(1)

        # Single save with all events
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(aggregate.aggregate_id)
        assert loaded.version == 50
        assert loaded.state.value == 50

    @pytest.mark.asyncio
    async def test_aggregate_id_type_preserved(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Aggregate ID should maintain its UUID type through save/load cycle."""
        original_id = uuid4()
        aggregate = counter_repository.create_new(original_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(original_id)

        assert isinstance(loaded.aggregate_id, UUID)
        assert loaded.aggregate_id == original_id
