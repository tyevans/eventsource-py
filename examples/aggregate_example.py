"""
Aggregate Example

This example demonstrates advanced aggregate patterns:
- DeclarativeAggregate with @handles decorators
- Complex state management
- Multi-event commands
- Aggregate-to-aggregate interaction via correlation

Run with: python -m eventsource.examples.aggregate_example
"""

import asyncio
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    DomainEvent,
    InMemoryEventStore,
    StreamId,
    handles,
    register_event,
)


@register_event
class CartCreated(DomainEvent):
    """Event emitted when a shopping cart is created."""

    aggregate_type: str = "ShoppingCart"
    customer_id: UUID


@register_event
class ItemAddedToCart(DomainEvent):
    """Event emitted when an item is added to the cart."""

    aggregate_type: str = "ShoppingCart"
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float


@register_event
class ItemRemovedFromCart(DomainEvent):
    """Event emitted when an item is removed from the cart."""

    aggregate_type: str = "ShoppingCart"
    product_id: UUID


@register_event
class ItemQuantityChanged(DomainEvent):
    """Event emitted when item quantity is changed."""

    aggregate_type: str = "ShoppingCart"
    product_id: UUID
    old_quantity: int
    new_quantity: int


@register_event
class CartCheckedOut(DomainEvent):
    """Event emitted when the cart is checked out."""

    aggregate_type: str = "ShoppingCart"
    order_id: UUID
    total_amount: float


@register_event
class CartAbandoned(DomainEvent):
    """Event emitted when the cart is abandoned."""

    aggregate_type: str = "ShoppingCart"
    reason: str


class CartItem(BaseModel):
    """A single item in the cart."""

    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total_price(self) -> float:
        return self.quantity * self.unit_price


class ShoppingCartState(BaseModel):
    """Current state of a shopping cart."""

    cart_id: UUID
    customer_id: UUID | None = None
    items: dict[str, CartItem] = Field(default_factory=dict)  # product_id -> CartItem
    status: str = "empty"  # empty, active, checked_out, abandoned
    checked_out_at: datetime | None = None
    order_id: UUID | None = None

    @property
    def total_amount(self) -> float:
        return sum(item.total_price for item in self.items.values())

    @property
    def item_count(self) -> int:
        return sum(item.quantity for item in self.items.values())


class ShoppingCartAggregate(DeclarativeAggregate[ShoppingCartState]):
    """
    Event-sourced shopping cart using declarative event handlers.

    Uses @handles decorator instead of if/elif chains in _apply().
    """

    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    @handles(CartCreated)
    def _on_cart_created(self, event: CartCreated) -> None:
        self._state = ShoppingCartState(
            cart_id=self.aggregate_id,
            customer_id=event.customer_id,
            status="empty",
        )

    @handles(ItemAddedToCart)
    def _on_item_added(self, event: ItemAddedToCart) -> None:
        if self._state:
            new_items = dict(self._state.items)
            product_key = str(event.product_id)
            qty = (
                new_items[product_key].quantity + event.quantity
                if product_key in new_items
                else event.quantity
            )
            new_items[product_key] = CartItem(
                product_id=event.product_id,
                product_name=event.product_name,
                quantity=qty,
                unit_price=event.unit_price,
            )
            self._state = self._state.model_copy(update={"items": new_items, "status": "active"})

    @handles(ItemRemovedFromCart)
    def _on_item_removed(self, event: ItemRemovedFromCart) -> None:
        if self._state:
            new_items = dict(self._state.items)
            new_items.pop(str(event.product_id), None)
            status = "active" if new_items else "empty"
            self._state = self._state.model_copy(update={"items": new_items, "status": status})

    @handles(ItemQuantityChanged)
    def _on_quantity_changed(self, event: ItemQuantityChanged) -> None:
        if self._state:
            new_items = dict(self._state.items)
            product_key = str(event.product_id)
            if product_key in new_items:
                existing = new_items[product_key]
                new_items[product_key] = CartItem(
                    product_id=existing.product_id,
                    product_name=existing.product_name,
                    quantity=event.new_quantity,
                    unit_price=existing.unit_price,
                )
            self._state = self._state.model_copy(update={"items": new_items})

    @handles(CartCheckedOut)
    def _on_cart_checked_out(self, event: CartCheckedOut) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "checked_out",
                    "checked_out_at": event.occurred_at,
                    "order_id": event.order_id,
                }
            )

    @handles(CartAbandoned)
    def _on_cart_abandoned(self, event: CartAbandoned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "abandoned"})

    def create(self, customer_id: UUID) -> None:
        """Create a new shopping cart."""
        if self.version > 0:
            raise ValueError("Cart already exists")
        self.apply_event(
            CartCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def add_item(
        self,
        product_id: UUID,
        product_name: str,
        quantity: int,
        unit_price: float,
    ) -> None:
        """Add an item to the cart."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status not in ("empty", "active"):
            raise ValueError(f"Cannot add items to {self.state.status} cart")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if unit_price < 0:
            raise ValueError("Price cannot be negative")
        self.apply_event(
            ItemAddedToCart(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                unit_price=unit_price,
                aggregate_version=self.get_next_version(),
            )
        )

    def remove_item(self, product_id: UUID) -> None:
        """Remove an item from the cart."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status not in ("empty", "active"):
            raise ValueError(f"Cannot remove items from {self.state.status} cart")
        if str(product_id) not in self.state.items:
            raise ValueError("Item not in cart")
        self.apply_event(
            ItemRemovedFromCart(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def change_quantity(self, product_id: UUID, new_quantity: int) -> None:
        """Change the quantity of an item."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status not in ("empty", "active"):
            raise ValueError(f"Cannot modify {self.state.status} cart")
        product_key = str(product_id)
        if product_key not in self.state.items:
            raise ValueError("Item not in cart")
        if new_quantity <= 0:
            raise ValueError("Quantity must be positive (use remove_item to delete)")
        self.apply_event(
            ItemQuantityChanged(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                old_quantity=self.state.items[product_key].quantity,
                new_quantity=new_quantity,
                aggregate_version=self.get_next_version(),
            )
        )

    def checkout(self) -> UUID:
        """Checkout the cart and create an order."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status != "active":
            raise ValueError(f"Cannot checkout {self.state.status} cart")
        if not self.state.items:
            raise ValueError("Cannot checkout empty cart")
        order_id = uuid4()
        self.apply_event(
            CartCheckedOut(
                aggregate_id=self.aggregate_id,
                order_id=order_id,
                total_amount=self.state.total_amount,
                aggregate_version=self.get_next_version(),
            )
        )
        return order_id

    def abandon(self, reason: str = "User abandoned") -> None:
        """Abandon the cart."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status in ("checked_out", "abandoned"):
            raise ValueError(f"Cannot abandon {self.state.status} cart")
        self.apply_event(
            CartAbandoned(
                aggregate_id=self.aggregate_id,
                reason=reason,
                aggregate_version=self.get_next_version(),
            )
        )


async def main():
    """Demonstrate advanced aggregate patterns."""
    print("=" * 60)
    print("Shopping Cart Aggregate Example")
    print("=" * 60)

    event_store = InMemoryEventStore()
    repo = AggregateRepository(event_store=event_store, aggregate_factory=ShoppingCartAggregate)
    cart_id, customer_id = uuid4(), uuid4()

    print(f"\n1. Creating cart for customer {customer_id}")
    cart = repo.create_new(cart_id)
    cart.create(customer_id)
    await repo.save(cart)
    print(f"   Cart ID: {cart_id}\n   Status: {cart.state.status}")

    print("\n2. Adding items to cart")
    products = [
        (uuid4(), "Wireless Mouse", 2, 29.99),
        (uuid4(), "USB-C Hub", 1, 49.99),
        (uuid4(), "Mechanical Keyboard", 1, 149.99),
    ]
    cart = await repo.load(cart_id)
    for product_id, name, qty, price in products:
        cart.add_item(product_id, name, qty, price)
        print(f"   Added: {qty}x {name} @ ${price:.2f}")
    await repo.save(cart)
    print(f"\n   Cart total: ${cart.state.total_amount:.2f}")
    print(f"   Item count: {cart.state.item_count}\n   Status: {cart.state.status}")

    print("\n3. Changing quantity")
    cart = await repo.load(cart_id)
    cart.change_quantity(products[0][0], 3)
    await repo.save(cart)
    print(f"   Updated mouse quantity to 3\n   New total: ${cart.state.total_amount:.2f}")

    print("\n4. Removing item")
    cart = await repo.load(cart_id)
    cart.remove_item(products[1][0])
    await repo.save(cart)
    print(f"   Removed USB-C Hub\n   New total: ${cart.state.total_amount:.2f}")
    print(f"   Remaining items: {len(cart.state.items)}")

    print("\n5. Checking out")
    cart = await repo.load(cart_id)
    order_id = cart.checkout()
    await repo.save(cart)
    print(f"   Order created: {order_id}\n   Total charged: ${cart.state.total_amount:.2f}")
    print(f"   Status: {cart.state.status}")

    print("\n6. Event history:")
    stream_id = StreamId(aggregate_id=cart_id, category="ShoppingCart")
    envelopes = [envelope async for envelope in event_store.read_stream(stream_id)]
    for i, envelope in enumerate(envelopes, 1):
        print(f"   [{i}] {envelope.event.event_type}")

    print("\n7. Business rule validation:")
    cart = await repo.load(cart_id)
    try:
        cart.add_item(uuid4(), "New Item", 1, 9.99)
    except ValueError as e:
        print(f"   Add item blocked: {e}")

    print("\n8. Cart abandonment:")
    abandoned_cart = repo.create_new(uuid4())
    abandoned_cart.create(customer_id)
    abandoned_cart.add_item(uuid4(), "Some Product", 1, 19.99)
    abandoned_cart.abandon("Session timeout")
    await repo.save(abandoned_cart)
    print(f"   Cart abandoned\n   Status: {abandoned_cart.state.status}")

    print("\n" + "=" * 60 + "\nExample completed successfully!\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
