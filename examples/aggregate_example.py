"""Aggregate Example: DeclarativeAggregate with @handles decorators and state management."""

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
    aggregate_type: str = "ShoppingCart"
    customer_id: UUID


@register_event
class ItemAddedToCart(DomainEvent):
    aggregate_type: str = "ShoppingCart"
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float


@register_event
class ItemRemovedFromCart(DomainEvent):
    aggregate_type: str = "ShoppingCart"
    product_id: UUID


@register_event
class ItemQuantityChanged(DomainEvent):
    aggregate_type: str = "ShoppingCart"
    product_id: UUID
    old_quantity: int
    new_quantity: int


@register_event
class CartCheckedOut(DomainEvent):
    aggregate_type: str = "ShoppingCart"
    order_id: UUID
    total_amount: float


@register_event
class CartAbandoned(DomainEvent):
    aggregate_type: str = "ShoppingCart"
    reason: str


class CartItem(BaseModel):
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total_price(self) -> float:
        return self.quantity * self.unit_price


class ShoppingCartState(BaseModel):
    cart_id: UUID
    customer_id: UUID | None = None
    items: dict[str, CartItem] = Field(default_factory=dict)
    status: str = "empty"
    checked_out_at: datetime | None = None
    order_id: UUID | None = None

    @property
    def total_amount(self) -> float:
        return sum(item.total_price for item in self.items.values())

    @property
    def item_count(self) -> int:
        return sum(item.quantity for item in self.items.values())


class ShoppingCartAggregate(DeclarativeAggregate[ShoppingCartState]):
    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    @handles(CartCreated)
    def _on_cart_created(self, event: CartCreated) -> None:
        self._state = ShoppingCartState(
            cart_id=self.aggregate_id, customer_id=event.customer_id, status="empty"
        )

    @handles(ItemAddedToCart)
    def _on_item_added(self, event: ItemAddedToCart) -> None:
        if self._state:
            items, key = dict(self._state.items), str(event.product_id)
            qty = items[key].quantity + event.quantity if key in items else event.quantity
            items[key] = CartItem(
                product_id=event.product_id,
                product_name=event.product_name,
                quantity=qty,
                unit_price=event.unit_price,
            )
            self._state = self._state.model_copy(update={"items": items, "status": "active"})

    @handles(ItemRemovedFromCart)
    def _on_item_removed(self, event: ItemRemovedFromCart) -> None:
        if self._state:
            items = dict(self._state.items)
            items.pop(str(event.product_id), None)
            self._state = self._state.model_copy(
                update={"items": items, "status": "active" if items else "empty"}
            )

    @handles(ItemQuantityChanged)
    def _on_quantity_changed(self, event: ItemQuantityChanged) -> None:
        if self._state:
            items, key = dict(self._state.items), str(event.product_id)
            if key in items:
                items[key] = items[key].model_copy(update={"quantity": event.new_quantity})
            self._state = self._state.model_copy(update={"items": items})

    @handles(CartCheckedOut)
    def _on_cart_checked_out(self, event: CartCheckedOut) -> None:
        if self._state:
            updates = {
                "status": "checked_out",
                "checked_out_at": event.occurred_at,
                "order_id": event.order_id,
            }
            self._state = self._state.model_copy(update=updates)

    @handles(CartAbandoned)
    def _on_cart_abandoned(self, event: CartAbandoned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "abandoned"})

    def _require_state(self, verb: str = "modify") -> ShoppingCartState:
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status not in ("empty", "active"):
            raise ValueError(f"Cannot {verb} {self.state.status} cart")
        return self.state

    def _emit(self, event_cls: type[DomainEvent], **kwargs: object) -> None:
        self.apply_event(
            event_cls(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
                **kwargs,
            )
        )

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Cart already exists")
        self._emit(CartCreated, customer_id=customer_id)

    def add_item(
        self, product_id: UUID, product_name: str, quantity: int, unit_price: float
    ) -> None:
        self._require_state("add items to")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if unit_price < 0:
            raise ValueError("Price cannot be negative")
        self._emit(
            ItemAddedToCart,
            product_id=product_id,
            product_name=product_name,
            quantity=quantity,
            unit_price=unit_price,
        )

    def remove_item(self, product_id: UUID) -> None:
        state = self._require_state("remove items from")
        if str(product_id) not in state.items:
            raise ValueError("Item not in cart")
        self._emit(ItemRemovedFromCart, product_id=product_id)

    def change_quantity(self, product_id: UUID, new_quantity: int) -> None:
        state, key = self._require_state("modify"), str(product_id)
        if key not in state.items:
            raise ValueError("Item not in cart")
        if new_quantity <= 0:
            raise ValueError("Quantity must be positive (use remove_item to delete)")
        old_qty = state.items[key].quantity
        self._emit(
            ItemQuantityChanged,
            product_id=product_id,
            old_quantity=old_qty,
            new_quantity=new_quantity,
        )

    def checkout(self) -> UUID:
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status != "active":
            raise ValueError(f"Cannot checkout {self.state.status} cart")
        if not self.state.items:
            raise ValueError("Cannot checkout empty cart")
        order_id = uuid4()
        self._emit(CartCheckedOut, order_id=order_id, total_amount=self.state.total_amount)
        return order_id

    def abandon(self, reason: str = "User abandoned") -> None:
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status in ("checked_out", "abandoned"):
            raise ValueError(f"Cannot abandon {self.state.status} cart")
        self._emit(CartAbandoned, reason=reason)


async def main() -> None:
    event_store = InMemoryEventStore()
    repo = AggregateRepository(event_store=event_store, aggregate_factory=ShoppingCartAggregate)
    cart_id, customer_id = uuid4(), uuid4()
    cart = repo.create_new(cart_id)
    cart.create(customer_id)
    await repo.save(cart)

    products = [
        (uuid4(), "Wireless Mouse", 2, 29.99),
        (uuid4(), "USB-C Hub", 1, 49.99),
        (uuid4(), "Mechanical Keyboard", 1, 149.99),
    ]
    cart = await repo.load(cart_id)
    for product_id, name, qty, price in products:
        cart.add_item(product_id, name, qty, price)
    cart.change_quantity(products[0][0], 3)
    cart.remove_item(products[1][0])
    order_id = cart.checkout()
    await repo.save(cart)
    print(f"Order {order_id} created (${cart.state.total_amount:.2f}, {cart.state.status})")

    stream_id = StreamId(aggregate_id=cart_id, category="ShoppingCart")
    async for envelope in event_store.read_stream(stream_id):
        print(f"Event: {envelope.event.event_type}")

    abandoned = repo.create_new(uuid4())
    abandoned.create(customer_id)
    abandoned.add_item(uuid4(), "Some Product", 1, 19.99)
    abandoned.abandon("Session timeout")
    await repo.save(abandoned)


if __name__ == "__main__":
    asyncio.run(main())
