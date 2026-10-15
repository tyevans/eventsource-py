"""Basic Subscription Manager Example - Order Summary Projection."""

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
        elif self._state and isinstance(event, (OrderShipped, OrderDelivered, OrderCancelled)):
            status = event.event_type.removeprefix("Order").lower()
            self._state = self._state.model_copy(update={"status": status})

    def place(self, customer_id: UUID, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        self.create_event(
            OrderPlaced,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
        )

    def ship(self, tracking_number: str, carrier: str) -> None:
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")
        self.create_event(OrderShipped, tracking_number=tracking_number, carrier=carrier)

    def deliver(self) -> None:
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")
        self.create_event(OrderDelivered, delivered_at=datetime.now())

    def cancel(self, reason: str) -> None:
        if not self.state or self.state.status in ("delivered", "cancelled"):
            raise ValueError("Cannot cancel this order")
        self.create_event(OrderCancelled, reason=reason)


class OrderSummaryProjection:
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
        elif isinstance(event, (OrderShipped, OrderDelivered)):
            prev, curr = (
                ("placed", "shipped")
                if isinstance(event, OrderShipped)
                else ("shipped", "delivered")
            )
            self.order_counts[prev] = max(0, self.order_counts[prev] - 1)
            self.order_counts[curr] += 1
            logger.info(f"Order {curr}: {self.order_counts[curr]} {curr}")
        elif isinstance(event, OrderCancelled):
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1
            logger.info(f"Order cancelled: {self.order_counts['cancelled']} cancelled")

    def get_summary(self) -> dict:
        counts = dict(self.order_counts)
        return {
            "total_orders": sum(counts.values()),
            "by_status": counts,
            "total_revenue": self.total_revenue,
            "unique_customers": len(self.customers),
        }


def _print_summary(projection: OrderSummaryProjection) -> None:
    s = projection.get_summary()
    print(f"   Total orders: {s['total_orders']} | By status: {s['by_status']}")
    print(f"   Total revenue: ${s['total_revenue']:.2f} | Customers: {s['unique_customers']}")


async def main():
    print("=" * 60 + "\nBasic Subscription Manager Example\n" + "=" * 60)
    event_store, event_bus = InMemoryEventStore(), InMemoryEventBus()
    checkpoint_repo = InMemoryCheckpointRepository()
    repo = AggregateRepository(
        event_store=event_store, aggregate_factory=OrderAggregate, event_publisher=event_bus
    )
    projection = OrderSummaryProjection()
    customers = [(uuid4(), "Alice Johnson"), (uuid4(), "Bob Smith"), (uuid4(), "Carol Williams")]
    order_ids = [uuid4() for _ in customers]
    for i, (customer_id, customer_name) in enumerate(customers):
        order = repo.create_new(order_ids[i])
        order.place(customer_id, customer_name, 100.0 + (i + 1) * 50)
        await repo.save(order)
    for action in (lambda o: o.ship("TRACK-001", "FedEx"), lambda o: o.deliver()):
        order = await repo.load(order_ids[0])
        action(order)
        await repo.save(order)
    order = await repo.load(order_ids[1])
    order.cancel("Customer request")
    await repo.save(order)

    manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
    cfg = SubscriptionConfig(start_from="beginning", batch_size=100)
    await manager.subscribe(projection, cfg, "OrderSummary")
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
