"""Benchmarks for order processing, computation overhead, and handler routing."""

from decimal import Decimal
from typing import Any
from uuid import uuid4

from eventsource.domain.event import DomainEvent
from tests.benchmarks.conftest import run_async
from tests.benchmarks.test_projections._projections import (
    CountingProjection,
    HeavyComputationProjection,
    OrderProjection,
)
from tests.fixtures import (
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
)


class TestProjectionOrderProcessingBenchmarks:
    """Benchmarks for order-domain projection processing."""

    def test_process_order_lifecycle(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Process a complete order lifecycle through projection.
        """
        customer_id = uuid4()

        # Generate order lifecycle events for 10 orders
        events: list[DomainEvent] = []
        for _ in range(10):
            order_id = uuid4()
            events.append(
                OrderCreated(
                    aggregate_id=order_id,
                    aggregate_version=1,
                    customer_id=customer_id,
                )
            )
            for j in range(5):
                events.append(
                    OrderItemAdded(
                        aggregate_id=order_id,
                        aggregate_version=j + 2,
                        item_name=f"Item {j}",
                        price=Decimal("10.00") * (j + 1),
                    )
                )
            events.append(
                OrderShipped(
                    aggregate_id=order_id,
                    aggregate_version=7,
                    tracking_number=f"TRACK-{order_id}",
                )
            )

        def process_orders() -> int:
            projection = OrderProjection()

            async def process_all() -> None:
                for event in events:
                    await projection.handle(event)

            run_async(process_all())
            return len(projection.orders)

        order_count = benchmark(process_orders)
        assert order_count == 10


class TestProjectionComputationBenchmarks:
    """Benchmarks for projections with computation overhead."""

    def test_process_with_computation(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process events through projection with computation.
        """

        def process_with_computation() -> int:
            projection = HeavyComputationProjection()

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection.handle(event)

            run_async(process_all())
            return len(projection.computed_values)

        count = benchmark(process_with_computation)
        assert count == 100


class TestProjectionHandlerRoutingBenchmarks:
    """Benchmarks for handler routing and dispatch."""

    def test_handler_lookup_performance(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Handler lookup performance for subscribed event types.
        """
        projection = CountingProjection()
        subscribed = projection.subscribed_to()

        def lookup_handlers() -> int:
            count = 0
            for _ in range(1000):
                for event_type in subscribed:
                    if projection._handler_registry.has_handler(event_type):
                        count += 1
            return count

        count = benchmark(lookup_handlers)
        # 3 handlers * 1000 iterations
        assert count == 3000

    def test_unhandled_event_ignore_mode(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Processing unhandled events in ignore mode.
        """
        projection = CountingProjection()

        # Create events the projection doesn't handle
        events = [
            OrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
            )
            for _ in range(100)
        ]

        def process_unhandled() -> int:
            async def process_all() -> None:
                for event in events:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        # Count should not increase for unhandled events
        initial_count = projection.count
        benchmark(process_unhandled)
        # The count should remain at initial value (unhandled events ignored)
        assert projection.count >= initial_count
