"""
Basic Subscription Manager Example - Order Summary Projection

This example demonstrates:
- Creating a simple read model projection
- Setting up the SubscriptionManager with in-memory implementations
- Subscribing a projection to receive events
- Starting catch-up and live subscriptions
- Querying the projection's read model

The projection tracks order counts by status, demonstrating how to build
a simple read model from an event stream.

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
from eventsource.subscriptions import SubscriptionConfig, SubscriptionManager

# =============================================================================
# Configure Logging
# =============================================================================
# Enable INFO level to see subscription lifecycle messages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Domain Events
# =============================================================================
# Events represent things that have happened in the domain.
# They are immutable and named in past tense.


@register_event
class OrderPlaced(DomainEvent):
    """Event emitted when an order is placed."""

    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str
    carrier: str


@register_event
class OrderDelivered(DomainEvent):
    """Event emitted when an order is delivered."""

    event_type: str = "OrderDelivered"
    aggregate_type: str = "Order"

    delivered_at: datetime


@register_event
class OrderCancelled(DomainEvent):
    """Event emitted when an order is cancelled."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"

    reason: str


# =============================================================================
# Aggregate State and Implementation
# =============================================================================


class OrderState(BaseModel):
    """Order aggregate state."""

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
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={"status": "shipped"})
        elif isinstance(event, OrderDelivered):
            if self._state:
                self._state = self._state.model_copy(update={"status": "delivered"})
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    def place(self, customer_id: UUID, customer_name: str, total_amount: float) -> None:
        """Place a new order."""
        if self.version > 0:
            raise ValueError("Order already placed")

        event = OrderPlaced(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_name=customer_name,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str, carrier: str) -> None:
        """Ship the order."""
        if not self.state or self.state.status != "placed":
            raise ValueError("Order must be placed to ship")

        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            carrier=carrier,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def deliver(self) -> None:
        """Mark order as delivered."""
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")

        event = OrderDelivered(
            aggregate_id=self.aggregate_id,
            delivered_at=datetime.now(),
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def cancel(self, reason: str) -> None:
        """Cancel the order."""
        if not self.state or self.state.status in ("delivered", "cancelled"):
            raise ValueError("Cannot cancel this order")

        event = OrderCancelled(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Order Summary Projection
# =============================================================================
# This projection counts orders by status, demonstrating a simple read model.


class OrderSummaryProjection:
    """
    Projection that maintains order counts by status.

    This is a simple in-memory read model that demonstrates how to:
    - Declare which event types the projection handles
    - Process events to update the read model
    - Query the read model for reporting

    In production, you would typically persist the read model to a database.
    """

    def __init__(self):
        # In-memory read model: status -> count
        self.order_counts: dict[str, int] = defaultdict(int)
        # Track total revenue
        self.total_revenue: float = 0.0
        # Track unique customers
        self.customers: set[UUID] = set()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which event types this projection handles."""
        return [OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event and update the read model."""
        if isinstance(event, OrderPlaced):
            self.order_counts["placed"] += 1
            self.total_revenue += event.total_amount
            self.customers.add(event.customer_id)
            logger.info(
                f"Order placed: total orders={sum(self.order_counts.values())}, "
                f"revenue=${self.total_revenue:.2f}"
            )

        elif isinstance(event, OrderShipped):
            # Decrement placed count, increment shipped
            if self.order_counts["placed"] > 0:
                self.order_counts["placed"] -= 1
            self.order_counts["shipped"] += 1
            logger.info(f"Order shipped: {self.order_counts['shipped']} shipped")

        elif isinstance(event, OrderDelivered):
            # Decrement shipped count, increment delivered
            if self.order_counts["shipped"] > 0:
                self.order_counts["shipped"] -= 1
            self.order_counts["delivered"] += 1
            logger.info(f"Order delivered: {self.order_counts['delivered']} delivered")

        elif isinstance(event, OrderCancelled):
            # Decrement the appropriate count
            for status in ["placed", "shipped"]:
                if self.order_counts[status] > 0:
                    self.order_counts[status] -= 1
                    break
            self.order_counts["cancelled"] += 1
            logger.info(f"Order cancelled: {self.order_counts['cancelled']} cancelled")

    # Query methods for the read model

    def get_summary(self) -> dict:
        """Get a summary of order statistics."""
        return {
            "total_orders": sum(self.order_counts.values()),
            "by_status": dict(self.order_counts),
            "total_revenue": self.total_revenue,
            "unique_customers": len(self.customers),
        }


# =============================================================================
# Main Demo
# =============================================================================


async def main():
    """Demonstrate basic SubscriptionManager usage."""
    print("=" * 60)
    print("Basic Subscription Manager Example")
    print("Order Summary Projection")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # Step 1: Set up infrastructure
    # -------------------------------------------------------------------------
    print("\n1. Setting up infrastructure (in-memory stores)...")

    # Create in-memory event store for storing events
    event_store = InMemoryEventStore()

    # Create in-memory event bus for live event notifications
    event_bus = InMemoryEventBus()

    # Create checkpoint repository to track projection progress
    checkpoint_repo = InMemoryCheckpointRepository()

    # Create aggregate repository for writing events
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
        event_publisher=event_bus,
    )

    print("   - Event store: InMemoryEventStore")
    print("   - Event bus: InMemoryEventBus")
    print("   - Checkpoint repo: InMemoryCheckpointRepository")

    # -------------------------------------------------------------------------
    # Step 2: Create the projection
    # -------------------------------------------------------------------------
    print("\n2. Creating OrderSummaryProjection...")

    projection = OrderSummaryProjection()
    print(f"   - Subscribed to: {[e.__name__ for e in projection.subscribed_to()]}")

    # -------------------------------------------------------------------------
    # Step 3: Create historical events (before subscription starts)
    # -------------------------------------------------------------------------
    print("\n3. Creating historical events (before subscription)...")

    customers = [
        (uuid4(), "Alice Johnson"),
        (uuid4(), "Bob Smith"),
        (uuid4(), "Carol Williams"),
    ]

    order_ids = []
    for customer_id, customer_name in customers:
        order_id = uuid4()
        order_ids.append(order_id)

        order = repo.create_new(order_id)
        order.place(customer_id, customer_name, 100.0 + len(order_ids) * 50)
        await repo.save(order)
        print(f"   Created order for {customer_name}")

    # Ship and deliver some orders
    order = await repo.load(order_ids[0])
    order.ship("TRACK-001", "FedEx")
    await repo.save(order)

    order = await repo.load(order_ids[0])
    order.deliver()
    await repo.save(order)
    print("   First order shipped and delivered")

    # Cancel one order
    order = await repo.load(order_ids[1])
    order.cancel("Customer request")
    await repo.save(order)
    print("   Second order cancelled")

    # -------------------------------------------------------------------------
    # Step 4: Set up and start the subscription manager
    # -------------------------------------------------------------------------
    print("\n4. Setting up SubscriptionManager...")

    # Create subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Configure subscription to start from the beginning
    # This will catch up on all historical events
    config = SubscriptionConfig(
        start_from="beginning",  # Start from the beginning of the event stream
        batch_size=100,  # Process 100 events per batch during catch-up
    )

    # Subscribe the projection
    subscription = await manager.subscribe(
        projection,
        config=config,
        name="OrderSummary",  # Custom name for the subscription
    )
    print(f"   Subscription created: {subscription.name}")
    print(f"   Start position: {config.start_from}")

    # Start the manager (this begins catch-up)
    print("\n5. Starting subscription manager (catching up)...")
    await manager.start()

    # Wait briefly for catch-up to complete
    await asyncio.sleep(0.5)

    # -------------------------------------------------------------------------
    # Step 5: Query the projection
    # -------------------------------------------------------------------------
    print("\n6. Querying projection after catch-up:")

    summary = projection.get_summary()
    print(f"   Total orders: {summary['total_orders']}")
    print(f"   By status: {summary['by_status']}")
    print(f"   Total revenue: ${summary['total_revenue']:.2f}")
    print(f"   Unique customers: {summary['unique_customers']}")

    # -------------------------------------------------------------------------
    # Step 6: Create new events (live subscription)
    # -------------------------------------------------------------------------
    print("\n7. Creating new events (live subscription)...")

    # Create a new order - this will be delivered via live subscription
    new_order_id = uuid4()
    order = repo.create_new(new_order_id)
    order.place(uuid4(), "David Brown", 500.0)
    await repo.save(order)
    print("   New order placed (live)")

    # Wait for live event to be processed
    await asyncio.sleep(0.2)

    # -------------------------------------------------------------------------
    # Step 7: Final status check
    # -------------------------------------------------------------------------
    print("\n8. Final projection status:")

    summary = projection.get_summary()
    print(f"   Total orders: {summary['total_orders']}")
    print(f"   By status: {summary['by_status']}")
    print(f"   Total revenue: ${summary['total_revenue']:.2f}")
    print(f"   Unique customers: {summary['unique_customers']}")

    # Check subscription status
    status = manager.get_all_statuses()
    for name, sub_status in status.items():
        print(f"\n   Subscription '{name}':")
        print(f"     State: {sub_status.state}")
        print(f"     Events processed: {sub_status.events_processed}")
        print(f"     Last position: {sub_status.position}")

    # -------------------------------------------------------------------------
    # Step 8: Graceful shutdown
    # -------------------------------------------------------------------------
    print("\n9. Stopping subscription manager...")

    await manager.stop()

    # Verify checkpoint was saved
    checkpoint = await checkpoint_repo.get_checkpoint("OrderSummary")
    if checkpoint:
        print(f"   Checkpoint saved at position: {checkpoint}")
    else:
        print("   No checkpoint saved")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
