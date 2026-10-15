"""Imperative Aggregate Example (`_apply` style on `AggregateRoot`).

See `basic_usage.py` for the primary (decider) style, and
`docs/explanation/aggregate-styles.md` for a comparison of the two.
Run with: python -m eventsource.examples.imperative_example
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    StreamId,
    register_event,
)


# --8<-- [start:events]
@register_event
class AccountOpened(DomainEvent):
    """Event emitted when a bank account is opened."""

    aggregate_type: str = "BankAccount"
    owner_name: str
    initial_balance: float


@register_event
class MoneyDeposited(DomainEvent):
    """Event emitted when money is deposited."""

    aggregate_type: str = "BankAccount"
    amount: float


@register_event
class MoneyWithdrawn(DomainEvent):
    """Event emitted when money is withdrawn."""

    aggregate_type: str = "BankAccount"
    amount: float


# --8<-- [end:events]


# --8<-- [start:state]
class BankAccountState(BaseModel):
    """Current state of a BankAccount aggregate."""

    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    is_open: bool = False


# --8<-- [end:state]


class BankAccountAggregate(AggregateRoot[BankAccountState]):
    """Event-sourced bank account aggregate."""

    aggregate_type = "BankAccount"

    def _get_initial_state(self) -> BankAccountState:
        return BankAccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = BankAccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                is_open=True,
            )
        elif isinstance(event, MoneyDeposited) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance + event.amount}
            )
        elif isinstance(event, MoneyWithdrawn) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance - event.amount}
            )

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        """Open a new bank account."""
        if self.version > 0:
            raise ValueError("Account already opened")
        if initial_balance < 0:
            raise ValueError("Initial balance cannot be negative")
        self.apply_event(
            AccountOpened(
                aggregate_id=self.aggregate_id,
                owner_name=owner_name,
                initial_balance=initial_balance,
                aggregate_version=self.get_next_version(),
            )
        )

    def deposit(self, amount: float) -> None:
        """Deposit money into the account."""
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.apply_event(
            MoneyDeposited(
                aggregate_id=self.aggregate_id,
                amount=amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def withdraw(self, amount: float) -> None:
        """Withdraw money from the account."""
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if amount > self.state.balance:
            raise ValueError(f"Insufficient balance: {self.state.balance}")
        self.apply_event(
            MoneyWithdrawn(
                aggregate_id=self.aggregate_id,
                amount=amount,
                aggregate_version=self.get_next_version(),
            )
        )


async def main():
    """Demonstrate basic event sourcing usage."""
    print(f"{'=' * 60}\nEvent Sourcing Basic Usage Example\n{'=' * 60}")
    event_store = InMemoryEventStore()
    repo = AggregateRepository(event_store=event_store, aggregate_factory=BankAccountAggregate)

    account_id = uuid4()
    print(f"\n1. Opening account {account_id}")
    account = repo.create_new(account_id)
    account.open(owner_name="Alice", initial_balance=100.0)
    await repo.save(account)
    print(f"   Owner: {account.state.owner_name}, Balance: ${account.state.balance:.2f}")

    print("\n2. Loading account and making deposits")
    loaded = await repo.load(account_id)
    loaded.deposit(50.0)
    loaded.deposit(25.0)
    await repo.save(loaded)
    print(f"   Balance after deposits: ${loaded.state.balance:.2f} (v{loaded.version})")

    print("\n3. Making a withdrawal")
    account = await repo.load(account_id)
    account.withdraw(30.0)
    await repo.save(account)
    print(f"   Balance after withdrawal: ${account.state.balance:.2f} (v{account.version})")

    print("\n4. Event history:")
    stream_id = StreamId(aggregate_id=account_id, category="BankAccount")
    envelopes = [envelope async for envelope in event_store.read_stream(stream_id)]
    for i, envelope in enumerate(envelopes, 1):
        event = envelope.event
        print(f"   [{i}] {event.event_type}")
        if isinstance(event, AccountOpened):
            print(f"       Owner: {event.owner_name}, Initial: ${event.initial_balance:.2f}")
        elif isinstance(event, MoneyDeposited | MoneyWithdrawn):
            print(f"       Amount: ${event.amount:.2f}")

    print("\n5. Business rule validation:")
    account = await repo.load(account_id)
    for label, fn in [("Withdrawal", account.withdraw), ("Deposit", account.deposit)]:
        try:
            fn(1000.0 if label == "Withdrawal" else -50.0)
        except ValueError as e:
            print(f"   {label} blocked: {e}")

    print("\n6. Final account state:")
    final = await repo.load(account_id)
    print(f"   ID: {final.aggregate_id}, Owner: {final.state.owner_name}, v{final.version}")
    print(f"   Balance: ${final.state.balance:.2f}, Open: {final.state.is_open}")
    print(f"\n{'=' * 60}\nExample completed successfully!\n{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
