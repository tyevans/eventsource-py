"""Aggregate Example: DeclarativeAggregate with @handles decorators and state management."""

import asyncio
from datetime import datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

import eventsource as es


class ShoppingCartEvent(es.DomainEvent):
    aggregate_type: str = "ShoppingCart"


@es.register_event
class CartCreated(ShoppingCartEvent):
    customer_id: UUID


@es.register_event
class ItemAddedToCart(ShoppingCartEvent):
    product_id: UUID
    product_name: str
    quantity: int
    unit_price: float


@es.register_event
class ItemRemovedFromCart(ShoppingCartEvent):
    product_id: UUID


@es.register_event
class ItemQuantityChanged(ShoppingCartEvent):
    product_id: UUID
    old_quantity: int
    new_quantity: int


@es.register_event
class CartCheckedOut(ShoppingCartEvent):
    order_id: UUID
    total_amount: float


@es.register_event
class CartAbandoned(ShoppingCartEvent):
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


class ShoppingCartAggregate(es.DeclarativeAggregate[ShoppingCartState]):
    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    @es.handles(CartCreated)
    def _on_cart_created(self, event: CartCreated) -> None:
        self._state = ShoppingCartState(cart_id=self.aggregate_id, customer_id=event.customer_id)

    @es.handles(ItemAddedToCart)
    def _on_item_added(self, event: ItemAddedToCart) -> None:
        if self._state:
            items, key = dict(self._state.items), str(event.product_id)
            qty = items[key].quantity + event.quantity if key in items else event.quantity
            data = event.model_dump(include={"product_id", "product_name", "unit_price"})
            items[key] = CartItem(quantity=qty, **data)
            self._state = self._state.model_copy(update={"items": items, "status": "active"})

    @es.handles(ItemRemovedFromCart)
    def _on_item_removed(self, event: ItemRemovedFromCart) -> None:
        if self._state:
            items = {k: v for k, v in self._state.items.items() if k != str(event.product_id)}
            upd = {"items": items, "status": "active" if items else "empty"}
            self._state = self._state.model_copy(update=upd)

    @es.handles(ItemQuantityChanged)
    def _on_quantity_changed(self, event: ItemQuantityChanged) -> None:
        if self._state:
            items, key = dict(self._state.items), str(event.product_id)
            if key in items:
                items[key] = items[key].model_copy(update={"quantity": event.new_quantity})
            self._state = self._state.model_copy(update={"items": items})

    @es.handles(CartCheckedOut)
    def _on_cart_checked_out(self, event: CartCheckedOut) -> None:
        if self._state:
            updates = {"status": "checked_out", "checked_out_at": event.occurred_at}
            self._state = self._state.model_copy(update={**updates, "order_id": event.order_id})

    @es.handles(CartAbandoned)
    def _on_cart_abandoned(self, event: CartAbandoned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "abandoned"})

    def _require_state(self, verb: str = "modify", ok: tuple[str, ...] = ("empty", "active")):
        if not self.state or self.state.status not in ok:
            err = f"Cannot {verb} {self.state.status} cart" if self.state else "Cart does not exist"
            raise ValueError(err)
        return self.state

    def _emit(self, event_cls: type[es.DomainEvent], **kwargs: object) -> None:
        ver = self.get_next_version()
        self.apply_event(event_cls(aggregate_id=self.aggregate_id, aggregate_version=ver, **kwargs))

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Cart already exists")
        self._emit(CartCreated, customer_id=customer_id)

    def add_item(self, pid: UUID, name: str, qty: int, price: float) -> None:
        self._require_state("add items to")
        if qty <= 0 or price < 0:
            err = "Quantity must be positive" if qty <= 0 else "Price cannot be negative"
            raise ValueError(err)
        item = {"product_id": pid, "product_name": name, "quantity": qty, "unit_price": price}
        self._emit(ItemAddedToCart, **item)

    def remove_item(self, product_id: UUID) -> None:
        if str(product_id) not in self._require_state("remove items from").items:
            raise ValueError("Item not in cart")
        self._emit(ItemRemovedFromCart, product_id=product_id)

    def change_quantity(self, pid: UUID, new_qty: int) -> None:
        state, key = self._require_state("modify"), str(pid)
        if key not in state.items or new_qty <= 0:
            err = "Quantity must be positive (use remove_item to delete)"
            raise ValueError("Item not in cart" if key not in state.items else err)
        old_qty = state.items[key].quantity
        self._emit(ItemQuantityChanged, product_id=pid, old_quantity=old_qty, new_quantity=new_qty)

    def checkout(self) -> UUID:
        state = self._require_state("checkout", ("active",))
        if not state.items:
            raise ValueError("Cannot checkout empty cart")
        order_id = uuid4()
        self._emit(CartCheckedOut, order_id=order_id, total_amount=state.total_amount)
        return order_id

    def abandon(self, reason: str = "User abandoned") -> None:
        self._require_state("abandon")
        self._emit(CartAbandoned, reason=reason)


async def main() -> None:
    store = es.InMemoryEventStore()
    repo = es.AggregateRepository(event_store=store, aggregate_factory=ShoppingCartAggregate)
    cart_id, customer_id, p1, p2, p3 = uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    cart = repo.create_new(cart_id)
    cart.create(customer_id)
    for pid, name, qty, price in [
        (p1, "Mouse", 2, 29.99),
        (p2, "Hub", 1, 49.99),
        (p3, "KB", 1, 99.0),
    ]:
        cart.add_item(pid, name, qty, price)
    cart.change_quantity(p1, 3)
    cart.remove_item(p2)
    order_id = cart.checkout()
    await repo.save(cart)
    print(f"Order {order_id} created (${cart.state.total_amount:.2f}, {cart.state.status})")
    abandoned = repo.create_new(uuid4())
    abandoned.create(customer_id)
    abandoned.abandon("Session timeout")
    await repo.save(abandoned)


if __name__ == "__main__":
    asyncio.run(main())
