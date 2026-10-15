"""Test projections for benchmarking."""

from typing import Any

from eventsource.application.projections.base import DeclarativeProjection
from eventsource.domain.decorators import handles
from tests.fixtures import (
    CounterDecremented,
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    SampleEvent,
)


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
