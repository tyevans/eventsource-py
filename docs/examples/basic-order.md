# Basic Order Example

This example demonstrates the fundamental concepts of event sourcing with a simple order management system.

## Overview

We'll build an order system that:
- Creates orders with customer information
- Adds items to orders
- Submits orders for processing
- Ships orders with tracking numbers
- Delivers orders

## Complete Code

```python
"""Complete order example with event sourcing."""
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)

# =============================================================================
# Events
# =============================================================================

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_email: str

@register_event
class OrderItemAdded(DomainEvent):
    event_type: str = "OrderItemAdded"
    aggregate_type: str = "Order"
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

@register_event
class OrderSubmitted(DomainEvent):
    event_type: str = "OrderSubmitted"
    aggregate_type: str = "Order"
    total_amount: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
    carrier: str

@register_event
class OrderDelivered(DomainEvent):
    event_type: str = "OrderDelivered"
    aggregate_type: str = "Order"

# =============================================================================
# State
# =============================================================================

class OrderItem(BaseModel):
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total(self) -> float:
        return self.quantity * self.unit_price

class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    customer_email: str = ""
    items: list[OrderItem] = []
    status: str = "draft"
    total_amount: float = 0.0
    tracking_number: str | None = None

    @property
    def calculated_total(self) -> float:
        return sum(item.total for item in self.items)

# =============================================================================
# Aggregate
# =============================================================================

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                customer_email=event.customer_email,
                status="created",
            )
        elif isinstance(event, OrderItemAdded):
            if self._state:
                new_item = OrderItem(
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )
                self._state = self._state.model_copy(update={
                    "items": [*self._state.items, new_item]
                })
        elif isinstance(event, OrderSubmitted):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "submitted",
                    "total_amount": event.total_amount,
                })
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                })
        elif isinstance(event, OrderDelivered):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "delivered",
                })

    # Commands

    def create(self, customer_id: UUID, customer_email: str) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            customer_email=customer_email,
            aggregate_version=self.get_next_version(),
        ))

    def add_item(
        self,
        product_id: UUID,
        product_name: str,
        quantity: int,
        unit_price: float,
    ) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Can only add items to created orders")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        self.apply_event(OrderItemAdded(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            product_name=product_name,
            quantity=quantity,
            unit_price=unit_price,
            aggregate_version=self.get_next_version(),
        ))

    def submit(self) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be in created status")
        if not self.state.items:
            raise ValueError("Cannot submit empty order")
        self.apply_event(OrderSubmitted(
            aggregate_id=self.aggregate_id,
            total_amount=self.state.calculated_total,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str, carrier: str) -> None:
        if not self.state or self.state.status != "submitted":
            raise ValueError("Order must be submitted to ship")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            carrier=carrier,
            aggregate_version=self.get_next_version(),
        ))

    def deliver(self) -> None:
        if not self.state or self.state.status != "shipped":
            raise ValueError("Order must be shipped to deliver")
        self.apply_event(OrderDelivered(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))

# =============================================================================
# Usage
# =============================================================================

async def main():
    # Setup
    store = InMemoryEventStore()
    repo = AggregateRepository(store, OrderAggregate, "Order")

    # Create order
    order_id = uuid4()
    customer_id = uuid4()

    order = repo.create_new(order_id)
    order.create(customer_id, "customer@example.com")

    # Add items
    order.add_item(uuid4(), "Widget A", 2, 19.99)
    order.add_item(uuid4(), "Widget B", 1, 49.99)

    # Submit
    order.submit()
    await repo.save(order)

    # Ship
    order = await repo.load(order_id)
    order.ship("TRACK-12345", "FedEx")
    await repo.save(order)

    # Deliver
    order = await repo.load(order_id)
    order.deliver()
    await repo.save(order)

    # Show final state
    final = await repo.load(order_id)
    print(f"Order: {final.aggregate_id}")
    print(f"Status: {final.state.status}")
    print(f"Total: ${final.state.total_amount:.2f}")
    print(f"Tracking: {final.state.tracking_number}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Key Concepts Demonstrated

### 1. Event Design

Events capture what happened:
- `OrderCreated` - Initial order creation
- `OrderItemAdded` - Items added to order
- `OrderSubmitted` - Order finalized
- `OrderShipped` - Order dispatched
- `OrderDelivered` - Order received

### 2. State Management

The `OrderState` model holds current state, derived entirely from events.

### 3. Business Rules

Commands validate rules before emitting events:
- Can't add items to submitted orders
- Can't submit empty orders
- Can't ship unsubmitted orders

### 4. Event Application

The `_apply` method is pure - it only updates state based on events, with no side effects.

## Running the Example

```bash
python order_example.py
```

Output:
```
Order: 12345678-1234-1234-1234-123456789abc
Status: delivered
Total: $89.97
Tracking: TRACK-12345
```
