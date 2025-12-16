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

---

## Modern Aggregate Patterns

The library provides several features to reduce boilerplate and improve developer experience.

### Using `create_event()` for Less Boilerplate

The `create_event()` method automatically populates aggregate fields:

```python
# Traditional approach (more verbose)
def ship(self, tracking_number: str, carrier: str) -> None:
    if not self.state or self.state.status != "submitted":
        raise ValueError("Order must be submitted to ship")
    self.apply_event(OrderShipped(
        aggregate_id=self.aggregate_id,        # Repetitive
        aggregate_type=self.aggregate_type,    # Repetitive
        aggregate_version=self.get_next_version(),  # Repetitive
        tracking_number=tracking_number,
        carrier=carrier,
    ))

# Modern approach with create_event()
def ship(self, tracking_number: str, carrier: str) -> None:
    if not self.state or self.state.status != "submitted":
        raise ValueError("Order must be submitted to ship")
    self.create_event(OrderShipped, tracking_number=tracking_number, carrier=carrier)
```

The `create_event()` method:
- Auto-populates `aggregate_id`, `aggregate_type`, and `aggregate_version`
- Auto-populates `tenant_id` from context if using multi-tenancy
- Creates and applies the event in one call
- Returns the created event if you need it

### Automatic Type Inference

Events can infer their `event_type` from the class name:

```python
# No need to set event_type explicitly
@register_event
class OrderShipped(DomainEvent):
    # event_type will be "OrderShipped" automatically
    aggregate_type: str = "Order"
    tracking_number: str
    carrier: str
```

### Deferred State Pattern

For aggregates where the initial state depends on the creation event, use `requires_creation_event`:

```python
from eventsource import DeclarativeAggregate, handles

class ExtractionProcess(DeclarativeAggregate[ExtractionState]):
    """Aggregate that doesn't need initial state until first event."""
    aggregate_type = "Extraction"
    requires_creation_event = True  # Enable deferred state

    # No _get_initial_state() needed!

    def request(self, page_id: UUID, config: dict) -> None:
        """Start an extraction process."""
        self.create_event(ExtractionRequested, page_id=page_id, config=config)

    @handles(ExtractionRequested)
    def _on_requested(self, event: ExtractionRequested) -> None:
        # First event creates the state
        self._state = ExtractionState(
            page_id=event.page_id,
            config=event.config,
            status="requested",
        )

    def complete(self, result: dict) -> None:
        """Mark extraction as complete."""
        if self.state.status != "processing":
            raise ValueError("Can only complete processing extractions")
        self.create_event(ExtractionCompleted, result=result)

    @handles(ExtractionCompleted)
    def _on_completed(self, event: ExtractionCompleted) -> None:
        self._state = self._state.model_copy(update={
            "status": "completed",
            "result": event.result,
        })
```

Key benefits of deferred state:
- No need to implement `_get_initial_state()` with placeholder values
- Cleaner code when state depends entirely on creation event data
- `AggregateNotCreatedError` is raised if you access `state` before any events applied

### Complete Modern Example

Here's the order example rewritten using modern patterns:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    DeclarativeAggregate,
    handles,
    InMemoryEventStore,
    AggregateRepository,
)

# Events - no explicit event_type needed
@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    customer_email: str

@register_event
class OrderItemAdded(DomainEvent):
    aggregate_type: str = "Order"
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

@register_event
class OrderSubmitted(DomainEvent):
    aggregate_type: str = "Order"
    total_amount: float

@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str
    carrier: str

# State
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
    customer_id: UUID
    customer_email: str
    items: list[OrderItem] = []
    status: str = "created"
    total_amount: float = 0.0
    tracking_number: str | None = None

    @property
    def calculated_total(self) -> float:
        return sum(item.total for item in self.items)

# Aggregate using modern patterns
class Order(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"
    requires_creation_event = True  # No initial state needed

    # Commands use create_event() - much cleaner!
    def create(self, customer_id: UUID, customer_email: str) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.create_event(OrderCreated, customer_id=customer_id, customer_email=customer_email)

    def add_item(self, product_id: UUID, name: str, quantity: int, price: float) -> None:
        if self.state.status != "created":
            raise ValueError("Can only add items to created orders")
        self.create_event(OrderItemAdded, product_id=product_id, product_name=name, quantity=quantity, unit_price=price)

    def submit(self) -> None:
        if self.state.status != "created" or not self.state.items:
            raise ValueError("Cannot submit: order must be created with items")
        self.create_event(OrderSubmitted, total_amount=self.state.calculated_total)

    def ship(self, tracking_number: str, carrier: str) -> None:
        if self.state.status != "submitted":
            raise ValueError("Order must be submitted to ship")
        self.create_event(OrderShipped, tracking_number=tracking_number, carrier=carrier)

    # Event handlers using @handles decorator
    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            customer_email=event.customer_email,
        )

    @handles(OrderItemAdded)
    def _on_item_added(self, event: OrderItemAdded) -> None:
        new_item = OrderItem(
            product_id=event.product_id,
            product_name=event.product_name,
            quantity=event.quantity,
            unit_price=event.unit_price,
        )
        self._state = self._state.model_copy(update={
            "items": [*self._state.items, new_item]
        })

    @handles(OrderSubmitted)
    def _on_submitted(self, event: OrderSubmitted) -> None:
        self._state = self._state.model_copy(update={
            "status": "submitted",
            "total_amount": event.total_amount,
        })

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        self._state = self._state.model_copy(update={
            "status": "shipped",
            "tracking_number": event.tracking_number,
        })
```

This modern version:
- Uses `DeclarativeAggregate` with `@handles` decorators
- Uses `requires_creation_event = True` for deferred state
- Uses `create_event()` in all commands for less boilerplate
- Relies on automatic `event_type` inference
