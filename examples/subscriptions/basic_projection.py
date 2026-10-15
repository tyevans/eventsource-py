"""Basic Subscription Manager Example - Order Summary Projection.

Run with: python -m examples.subscriptions.basic_projection
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryCheckpointRepository,
    InMemoryEventBus,
    InMemoryEventStore,
    register_event,
)
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@register_event
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str
    carrier: str


@register_event
class OrderDelivered(DomainEvent):
    aggregate_type: str = "Order"
    delivered_at: datetime


@register_event
class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"


class OrderAggregate(AggregateRoot[OrderState]):
    """Event-sourced order aggregate."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="placed",
            )
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})
        elif isinstance(event, OrderDelivered) and self._state:
            self._state = self._state.model_copy(update={"status": "delivered"})
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(self, customer_id: UUID, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        self.apply_event(
            OrderPlaced(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                customer_name=customer_name,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str, carrier: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tracking_number=tracking_number,
                carrier=carrier,
                aggregate_version=self.get_next_version(),
            )
        )

    def deliver(self) -> None:
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")
        self.apply_event(
            OrderDelivered(
                aggregate_id=self.aggregate_id,
                delivered_at=datetime.now(),
                aggregate_version=self.get_next_version(),
            )
        )

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("delivered", "cancelled"):
            raise ValueError("Cannot cancel this order")
        self.apply_event(
            OrderCancelled(
                aggregate_id=self.aggregate_id,
                reason=reason,
                aggregate_version=self.get_next_version(),
            )
        )


class OrderSummaryProjection:
    """Projection that maintains order counts by status."""

    def __init__(self):
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0
        self.customers: set[UUID] = set()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount
            self.customers.add(event.customer_id)
            logger.info(f"Order placed: revenue=${self.total_revenue:.2f}")
        elif isinstance(event, OrderShipped):
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1
            logger.info(f"Order shipped: {self.order_counts['shipped']} shipped")
        elif isinstance(event, OrderDelivered):
            if self.order_counts["shipped"] > 0:
                self.order_counts["shipped"] -= 1
            self.order_counts["delivered"] += 1
            logger.info(f"Order delivered: {self.order_counts['delivered']} delivered")
        elif isinstance(event, OrderCancelled):
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1
            logger.info(f"Order cancelled: {self.order_counts['cancelled']} cancelled")

    def get_summary(self) -> dict:
        return {
            "total_orders": sum(self.order_counts.values()),
            "by_status": dict(self.order_counts),
            "total_revenue": self.total_revenue,
            "unique_customers": len(self.customers),
        }


def _print_summary(projection: OrderSummaryProjection) -> None:
    s = projection.get_summary()
    print(f"   Total orders: {s['total_orders']} | By status: {s['by_status']}")
    print(f"   Total revenue: ${s['total_revenue']:.2f} | Customers: {s['unique_customers']}")


async def main():
    """Demonstrate basic SubscriptionManager usage."""
    print("=" * 60 + "\nBasic Subscription Manager Example\n" + "=" * 60)
    event_store = InMemoryEventStore()
    event_bus = InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    repo = AggregateRepository(
        event_store=event_store, aggregate_factory=OrderAggregate, event_publisher=event_bus
    )
    projection = OrderSummaryProjection()

    customers = [(uuid4(), "Alice Johnson"), (uuid4(), "Bob Smith"), (uuid4(), "Carol Williams")]
    order_ids = []
    for customer_id, customer_name in customers:
        order_id = uuid4()
        order_ids.append(order_id)
        order = repo.create_new(order_id)
        order.place(customer_id, customer_name, 100.0 + len(order_ids) * 50)
        await repo.save(order)

    order = await repo.load(order_ids[0])
    order.ship("TRACK-001", "FedEx")
    await repo.save(order)
    order = await repo.load(order_ids[0])
    order.deliver()
    await repo.save(order)

    order = await repo.load(order_ids[1])
    order.cancel("Customer request")
    await repo.save(order)

    manager = SubscriptionManager(
        event_store=event_store, event_bus=event_bus, checkpoint_repo=checkpoint_repo
    )
    config = SubscriptionConfig(start_from="beginning", batch_size=100)
    await manager.subscribe(projection, config=config, name="OrderSummary")

    await manager.start()
    await asyncio.sleep(0.5)
    print("\nAfter catch-up:")
    _print_summary(projection)

    order = repo.create_new(uuid4())
    order.place(uuid4(), "David Brown", 500.0)
    await repo.save(order)
    await asyncio.sleep(0.2)

    print("\nFinal projection status:")
    _print_summary(projection)
    for name, sub_status in manager.get_all_statuses().items():
        print(f"   Subscription '{name}': {sub_status.state} ({sub_status.events_processed})")

    await manager.stop()
    checkpoint = await checkpoint_repo.get_checkpoint("OrderSummary")
    print(f"   Checkpoint: {checkpoint}" if checkpoint else "   No checkpoint saved")


if __name__ == "__main__":
    asyncio.run(main())
