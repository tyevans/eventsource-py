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
    handles,
    register_event,
)

# =============================================================================
# Domain Events for Shopping Cart
# =============================================================================


@register_event
class CartCreated(DomainEvent):
    """Event emitted when a shopping cart is created."""

    event_type: str = "CartCreated"
    aggregate_type: str = "ShoppingCart"

    customer_id: UUID


@register_event
class ItemAddedToCart(DomainEvent):
    """Event emitted when an item is added to the cart."""

    event_type: str = "ItemAddedToCart"
    aggregate_type: str = "ShoppingCart"

    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float


@register_event
class ItemRemovedFromCart(DomainEvent):
    """Event emitted when an item is removed from the cart."""

    event_type: str = "ItemRemovedFromCart"
    aggregate_type: str = "ShoppingCart"

    product_id: UUID


@register_event
class ItemQuantityChanged(DomainEvent):
    """Event emitted when item quantity is changed."""

    event_type: str = "ItemQuantityChanged"
    aggregate_type: str = "ShoppingCart"

    product_id: UUID
    old_quantity: int
    new_quantity: int


@register_event
class CartCheckedOut(DomainEvent):
    """Event emitted when the cart is checked out."""

    event_type: str = "CartCheckedOut"
    aggregate_type: str = "ShoppingCart"

    order_id: UUID
    total_amount: float


@register_event
class CartAbandoned(DomainEvent):
    """Event emitted when the cart is abandoned."""

    event_type: str = "CartAbandoned"
    aggregate_type: str = "ShoppingCart"

    reason: str


# =============================================================================
# Cart Item Model
# =============================================================================


class CartItem(BaseModel):
    """A single item in the cart."""

    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total_price(self) -> float:
        return self.quantity * self.unit_price


# =============================================================================
# Shopping Cart State
# =============================================================================


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


# =============================================================================
# Shopping Cart Aggregate (using DeclarativeAggregate)
# =============================================================================


class ShoppingCartAggregate(DeclarativeAggregate[ShoppingCartState]):
    """
    Event-sourced shopping cart using declarative event handlers.

    Uses @handles decorator instead of if/elif chains in _apply().
    """

    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    # Event handlers using @handles decorator

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

            if product_key in new_items:
                # Update existing item
                existing = new_items[product_key]
                new_items[product_key] = CartItem(
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=existing.quantity + event.quantity,
                    unit_price=event.unit_price,
                )
            else:
                # Add new item
                new_items[product_key] = CartItem(
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )

            self._state = self._state.model_copy(
                update={
                    "items": new_items,
                    "status": "active",
                }
            )

    @handles(ItemRemovedFromCart)
    def _on_item_removed(self, event: ItemRemovedFromCart) -> None:
        if self._state:
            new_items = dict(self._state.items)
            product_key = str(event.product_id)
            if product_key in new_items:
                del new_items[product_key]

            status = "active" if new_items else "empty"
            self._state = self._state.model_copy(
                update={
                    "items": new_items,
                    "status": status,
                }
            )

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

    # Command methods

    def create(self, customer_id: UUID) -> None:
        """Create a new shopping cart."""
        if self.version > 0:
            raise ValueError("Cart already exists")

        event = CartCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

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

        event = ItemAddedToCart(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            product_name=product_name,
            quantity=quantity,
            unit_price=unit_price,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def remove_item(self, product_id: UUID) -> None:
        """Remove an item from the cart."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status not in ("empty", "active"):
            raise ValueError(f"Cannot remove items from {self.state.status} cart")

        product_key = str(product_id)
        if product_key not in self.state.items:
            raise ValueError("Item not in cart")

        event = ItemRemovedFromCart(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

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

        old_quantity = self.state.items[product_key].quantity

        event = ItemQuantityChanged(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            old_quantity=old_quantity,
            new_quantity=new_quantity,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def checkout(self) -> UUID:
        """Checkout the cart and create an order."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status != "active":
            raise ValueError(f"Cannot checkout {self.state.status} cart")
        if not self.state.items:
            raise ValueError("Cannot checkout empty cart")

        order_id = uuid4()
        event = CartCheckedOut(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            total_amount=self.state.total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
        return order_id

    def abandon(self, reason: str = "User abandoned") -> None:
        """Abandon the cart."""
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status in ("checked_out", "abandoned"):
            raise ValueError(f"Cannot abandon {self.state.status} cart")

        event = CartAbandoned(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# =============================================================================
# Demo
# =============================================================================


async def main():
    """Demonstrate advanced aggregate patterns."""
    print("=" * 60)
    print("Shopping Cart Aggregate Example")
    print("=" * 60)

    # Setup
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=ShoppingCartAggregate,
        aggregate_type="ShoppingCart",
    )

    # Create cart
    cart_id = uuid4()
    customer_id = uuid4()

    print(f"\n1. Creating cart for customer {customer_id}")
    cart = repo.create_new(cart_id)
    cart.create(customer_id)
    await repo.save(cart)
    print(f"   Cart ID: {cart_id}")
    print(f"   Status: {cart.state.status}")

    # Add items
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
    print(f"   Item count: {cart.state.item_count}")
    print(f"   Status: {cart.state.status}")

    # Change quantity
    print("\n3. Changing quantity")
    cart = await repo.load(cart_id)
    mouse_id = products[0][0]
    cart.change_quantity(mouse_id, 3)  # Change from 2 to 3
    await repo.save(cart)
    print("   Updated mouse quantity to 3")
    print(f"   New total: ${cart.state.total_amount:.2f}")

    # Remove item
    print("\n4. Removing item")
    cart = await repo.load(cart_id)
    hub_id = products[1][0]
    cart.remove_item(hub_id)
    await repo.save(cart)
    print("   Removed USB-C Hub")
    print(f"   New total: ${cart.state.total_amount:.2f}")
    print(f"   Remaining items: {len(cart.state.items)}")

    # Checkout
    print("\n5. Checking out")
    cart = await repo.load(cart_id)
    order_id = cart.checkout()
    await repo.save(cart)
    print(f"   Order created: {order_id}")
    print(f"   Total charged: ${cart.state.total_amount:.2f}")
    print(f"   Status: {cart.state.status}")

    # Show event history
    print("\n6. Event history:")
    stream = await event_store.get_events(cart_id, "ShoppingCart")
    for i, event in enumerate(stream.events, 1):
        print(f"   [{i}] {event.event_type}")

    # Try to modify checked out cart (should fail)
    print("\n7. Business rule validation:")
    cart = await repo.load(cart_id)
    try:
        cart.add_item(uuid4(), "New Item", 1, 9.99)
    except ValueError as e:
        print(f"   Add item blocked: {e}")

    # Create another cart and abandon it
    print("\n8. Cart abandonment:")
    abandoned_cart_id = uuid4()
    abandoned_cart = repo.create_new(abandoned_cart_id)
    abandoned_cart.create(customer_id)
    abandoned_cart.add_item(uuid4(), "Some Product", 1, 19.99)
    abandoned_cart.abandon("Session timeout")
    await repo.save(abandoned_cart)
    print("   Cart abandoned")
    print(f"   Status: {abandoned_cart.state.status}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
