"""Shared test aggregate implementations and state models."""

from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from tests.fixtures.events import (
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
)


class CounterState(BaseModel):
    counter_id: UUID
    value: int = 0
    name: str = ""


class OrderState(BaseModel):
    customer_id: UUID | None = None
    status: str = "draft"
    total: Decimal = Decimal("0")
    items: list[str] = Field(default_factory=list)


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _ensure_state(self) -> CounterState:
        if self._state is None:
            self._state = self._get_initial_state()
        return self._state

    def _make_event(self, cls: type[DomainEvent], **kwargs: Any) -> DomainEvent:
        return cls(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            **kwargs,
        )

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            state = self._ensure_state()
            self._state = state.model_copy(update={"value": state.value + event.increment})
        elif isinstance(event, CounterDecremented):
            state = self._ensure_state()
            self._state = state.model_copy(update={"value": state.value - event.decrement})
        elif isinstance(event, CounterNamed):
            state = self._ensure_state()
            self._state = state.model_copy(update={"name": event.name})
        elif isinstance(event, CounterReset):
            self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        self.apply_event(self._make_event(CounterIncremented, increment=amount))

    def decrement(self, amount: int = 1) -> None:
        self.apply_event(self._make_event(CounterDecremented, decrement=amount))

    def set_name(self, name: str) -> None:
        self.apply_event(self._make_event(CounterNamed, name=name))

    def reset(self) -> None:
        self.apply_event(self._make_event(CounterReset))


class DeclarativeCounterAggregate(DeclarativeAggregate[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _ensure_state(self) -> CounterState:
        if self._state is None:
            self._state = self._get_initial_state()
        return self._state

    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        state = self._ensure_state()
        self._state = state.model_copy(update={"value": state.value + event.increment})

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        state = self._ensure_state()
        self._state = state.model_copy(update={"value": state.value - event.decrement})

    @handles(CounterNamed)
    def _on_counter_named(self, event: CounterNamed) -> None:
        state = self._ensure_state()
        self._state = state.model_copy(update={"name": event.name})

    @handles(CounterReset)
    def _on_counter_reset(self, event: CounterReset) -> None:
        self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        self._raise_event(
            CounterIncremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                increment=amount,
            )
        )

    def decrement(self, amount: int = 1) -> None:
        self._raise_event(
            CounterDecremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                decrement=amount,
            )
        )


class CreateOrder(DomainCommand):
    order_id: UUID
    customer_id: UUID


class AddOrderItem(DomainCommand):
    order_id: UUID
    item_name: str
    price: Decimal


class ShipOrder(DomainCommand):
    order_id: UUID
    tracking_number: str


class OrderAggregate(DeciderAggregate[OrderState]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def decide(command: object, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case CreateOrder(order_id=order_id, customer_id=customer_id), OrderState(
                status="draft"
            ):
                return [OrderCreated(aggregate_id=order_id, customer_id=customer_id)]
            case CreateOrder(), _:
                raise ValueError("Order already exists")
            case AddOrderItem(order_id=order_id, item_name=item_name, price=price), OrderState(
                status=status
            ) if status not in ("draft", "shipped"):
                return [OrderItemAdded(aggregate_id=order_id, item_name=item_name, price=price)]
            case AddOrderItem(), _:
                raise ValueError("Cannot add items to this order")
            case ShipOrder(order_id=order_id, tracking_number=tracking_number), OrderState(
                status="created"
            ):
                return [OrderShipped(aggregate_id=order_id, tracking_number=tracking_number)]
            case ShipOrder(), _:
                raise ValueError("Cannot ship order in current state")
            case _:
                raise ValueError(f"unknown command: {command!r}")

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderCreated(customer_id=customer_id):
                return state.model_copy(update={"customer_id": customer_id, "status": "created"})
            case OrderItemAdded(item_name=item_name, price=price):
                return state.model_copy(
                    update={"items": [*state.items, item_name], "total": state.total + price}
                )
            case OrderShipped():
                return state.model_copy(update={"status": "shipped"})
            case _:
                return state

    def create(self, customer_id: UUID) -> None:
        self.execute(CreateOrder(order_id=self.aggregate_id, customer_id=customer_id))

    def add_item(self, item_name: str, price: Decimal) -> None:
        self.execute(AddOrderItem(order_id=self.aggregate_id, item_name=item_name, price=price))

    def ship(self, tracking_number: str) -> None:
        self.execute(ShipOrder(order_id=self.aggregate_id, tracking_number=tracking_number))
