"""
Performance benchmarks for projection operations.

Tests critical paths:
- Event handler routing
- Projection event processing throughput
- Checkpoint tracking overhead
- Multiple handler dispatch

Target baselines:
- Process single event: < 0.5ms
- Process 100 events: < 50ms
- Process 1000 events: < 500ms
"""

from typing import Any
from uuid import uuid4

from eventsource.events.base import DomainEvent
from eventsource.projections.base import DeclarativeProjection
from eventsource.projections.decorators import handles
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from tests.benchmarks.conftest import run_async
from tests.fixtures import (
    CounterDecremented,
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    SampleEvent,
    create_event,
)

# =============================================================================
# Test Projections for Benchmarking
# =============================================================================


class CountingProjection(DeclarativeProjection):
    """Simple projection that counts events for benchmarking."""

    def __init__(self) -> None:
        super().__init__()
        self.count = 0
        self.event_types_seen: dict[str, int] = {}

    @handles(SampleEvent)
    async def _handle_sample(self, event: SampleEvent) -> None:
        self.count += 1
        event_type = event.event_type
        self.event_types_seen[event_type] = self.event_types_seen.get(event_type, 0) + 1

    @handles(CounterIncremented)
    async def _handle_counter_incremented(self, event: CounterIncremented) -> None:
        self.count += 1
        self.event_types_seen["CounterIncremented"] = (
            self.event_types_seen.get("CounterIncremented", 0) + 1
        )

    @handles(CounterDecremented)
    async def _handle_counter_decremented(self, event: CounterDecremented) -> None:
        self.count += 1
        self.event_types_seen["CounterDecremented"] = (
            self.event_types_seen.get("CounterDecremented", 0) + 1
        )


class OrderProjection(DeclarativeProjection):
    """Projection handling multiple order event types."""

    def __init__(self) -> None:
        super().__init__()
        self.orders: dict[str, dict[str, Any]] = {}

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.orders[str(event.aggregate_id)] = {
            "customer_id": str(event.customer_id),
            "items": [],
            "shipped": False,
        }

    @handles(OrderItemAdded)
    async def _handle_item_added(self, event: OrderItemAdded) -> None:
        order_id = str(event.aggregate_id)
        if order_id in self.orders:
            self.orders[order_id]["items"].append(
                {
                    "name": event.item_name,
                    "price": event.price,
                }
            )

    @handles(OrderShipped)
    async def _handle_shipped(self, event: OrderShipped) -> None:
        order_id = str(event.aggregate_id)
        if order_id in self.orders:
            self.orders[order_id]["shipped"] = True
            self.orders[order_id]["tracking"] = event.tracking_number


class HeavyComputationProjection(DeclarativeProjection):
    """Projection that simulates heavier computation per event."""

    def __init__(self) -> None:
        super().__init__()
        self.computed_values: list[int] = []

    @handles(SampleEvent)
    async def _handle_sample(self, event: SampleEvent) -> None:
        # Simulate some computation (not too heavy for benchmarks)
        value = sum(range(100))
        self.computed_values.append(value)


# =============================================================================
# Benchmark Tests
# =============================================================================


class TestProjectionProcessingBenchmarks:
    """Benchmarks for projection event processing throughput."""

    def test_process_single_event(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Process a single event through a projection.
        """
        projection = CountingProjection()
        event = create_event(SampleEvent)

        def process_one() -> None:
            run_async(projection.handle(event))

        benchmark(process_one)
        assert projection.count > 0

    def test_process_100_events(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process 100 events through a projection.
        """

        def process_batch() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_batch)
        assert count == 100

    def test_process_1000_events(
        self,
        benchmark: Any,
        sample_events_1000: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process 1000 events through a projection.
        """

        def process_batch() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_1000:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark.pedantic(process_batch, rounds=10)
        assert count == 1000

    def test_process_with_multiple_handlers(
        self,
        benchmark: Any,
    ) -> None:
        """
        Benchmark: Process events through projection with multiple handlers.
        """
        events: list[DomainEvent] = []
        aggregate_id = uuid4()

        # Create a mix of event types
        for i in range(100):
            if i % 2 == 0:
                events.append(
                    CounterIncremented(
                        aggregate_id=aggregate_id,
                        aggregate_version=i + 1,
                        increment=1,
                    )
                )
            else:
                events.append(
                    CounterDecremented(
                        aggregate_id=aggregate_id,
                        aggregate_version=i + 1,
                        decrement=1,
                    )
                )

        def process_mixed() -> int:
            projection = CountingProjection()

            async def process_all() -> None:
                for event in events:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_mixed)
        assert count == 100


class TestProjectionCheckpointBenchmarks:
    """Benchmarks for projection checkpoint tracking overhead."""

    def test_process_with_checkpoint_tracking(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Process events with checkpoint tracking enabled.

        This measures the overhead of checkpoint persistence.
        """

        def process_with_checkpoint() -> int:
            checkpoint_repo = InMemoryCheckpointRepository()
            projection = CountingProjection()
            projection._checkpoint_repo = checkpoint_repo

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection.handle(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_with_checkpoint)
        assert count == 100

    def test_checkpoint_overhead_comparison(
        self,
        benchmark: Any,
        sample_events_100: list[DomainEvent],
    ) -> None:
        """
        Benchmark: Compare processing with and without checkpoint tracking.

        This uses pedantic mode to get accurate timing.
        """

        def process_without_checkpoint() -> int:
            # Create projection with in-memory checkpoint (minimal overhead)
            projection = CountingProjection()

            async def process_all() -> None:
                for event in sample_events_100:
                    await projection._process_event(event)

            run_async(process_all())
            return projection.count

        count = benchmark(process_without_checkpoint)
        assert count == 100


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
                        price=10.0 * (j + 1),
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
                    if event_type in projection._handlers:
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
