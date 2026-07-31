"""
Basic Usage Example

This example demonstrates the fundamental concepts of event sourcing using the
decider style, the primary way to write aggregates in this library:
- Defining domain events
- Defining commands and a pure decide/evolve aggregate
- Using the repository pattern
- Basic event store operations

See `imperative_example.py` for the hand-written `_apply` style and
`aggregate_example.py` for the `@handles`-decorated declarative style.

Run with: python -m eventsource.examples.basic_usage
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventStore,
    StreamId,
    register_event,
)

# =============================================================================
# Step 1: Define Domain Events
# =============================================================================
# Events capture things that have happened in the system.
# They are immutable and named in past tense.


# --8<-- [start:events]
@register_event
class AccountOpened(DomainEvent):
    """Event emitted when a bank account is opened."""

    event_type: str = "AccountOpened"
    aggregate_type: str = "BankAccount"

    owner_name: str
    initial_balance: float


@register_event
class MoneyDeposited(DomainEvent):
    """Event emitted when money is deposited."""

    event_type: str = "MoneyDeposited"
    aggregate_type: str = "BankAccount"

    amount: float


@register_event
class MoneyWithdrawn(DomainEvent):
    """Event emitted when money is withdrawn."""

    event_type: str = "MoneyWithdrawn"
    aggregate_type: str = "BankAccount"

    amount: float


# --8<-- [end:events]


# =============================================================================
# Step 2: Define Aggregate State
# =============================================================================
# State represents the current data of the aggregate.
# Use Pydantic BaseModel for validation and serialization.


# --8<-- [start:state]
class BankAccountState(BaseModel):
    """Current state of a BankAccount aggregate."""

    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    is_open: bool = False


# --8<-- [end:state]


# =============================================================================
# Step 3: Define Commands
# =============================================================================
# Commands are immutable intents. Unlike events, they may be rejected -- and a
# rejected command leaves no trace in the event store.


class OpenAccount(DomainCommand):
    """Request to open a new bank account."""

    owner_name: str
    initial_balance: float = 0.0


class DepositMoney(DomainCommand):
    """Request to deposit money into an account."""

    amount: float


class WithdrawMoney(DomainCommand):
    """Request to withdraw money from an account."""

    amount: float


# =============================================================================
# Step 4: The domain as pure functions
# =============================================================================
# decide: command + state -> events (or a rejection).
# evolve: state + event -> next state.
# Both are pure -- no I/O, no versions, testable with plain asserts.


class BankAccountAggregate(DeciderAggregate[BankAccountState]):
    """Bank account in the decider style."""

    aggregate_type = "BankAccount"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> BankAccountState:
        return BankAccountState(account_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: BankAccountState) -> list[DomainEvent]:
        match command, state:
            case OpenAccount(owner_name=name, initial_balance=balance), BankAccountState(
                is_open=False
            ):
                return [
                    AccountOpened(
                        aggregate_id=state.account_id,
                        owner_name=name,
                        initial_balance=balance,
                    )
                ]
            case OpenAccount(), _:
                raise CommandRejectedError("account is already open", command=command)
            case DepositMoney(amount=amount), BankAccountState(is_open=True):
                if amount <= 0:
                    raise CommandRejectedError("deposit must be positive", command=command)
                return [MoneyDeposited(aggregate_id=state.account_id, amount=amount)]
            case WithdrawMoney(amount=amount), BankAccountState(is_open=True):
                if amount <= 0:
                    raise CommandRejectedError("withdrawal must be positive", command=command)
                if amount > state.balance:
                    raise CommandRejectedError("insufficient funds", command=command)
                return [MoneyWithdrawn(aggregate_id=state.account_id, amount=amount)]
            case ((DepositMoney() | WithdrawMoney()), _):
                raise CommandRejectedError("account is not open", command=command)
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: BankAccountState, event: DomainEvent) -> BankAccountState:
        match event:
            case AccountOpened(owner_name=name, initial_balance=balance):
                return state.model_copy(
                    update={"owner_name": name, "balance": balance, "is_open": True}
                )
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case MoneyWithdrawn(amount=amount):
                return state.model_copy(update={"balance": state.balance - amount})
            case _:
                return state


# =============================================================================
# Step 5: Use the Repository
# =============================================================================


async def main():
    """Demonstrate basic event sourcing usage."""
    print("=" * 60)
    print("Event Sourcing Basic Usage Example")
    print("=" * 60)

    # Create the event store (in-memory for this example)
    event_store = InMemoryEventStore()

    # Create the repository
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=BankAccountAggregate,
        aggregate_type="BankAccount",
    )

    # Create a new account
    account_id = uuid4()
    print(f"\n1. Opening account {account_id}")

    account = repo.create_new(account_id)
    account.execute(OpenAccount(owner_name="Alice", initial_balance=100.0))
    await repo.save(account)

    print(f"   Owner: {account.state.owner_name}")
    print(f"   Balance: ${account.state.balance:.2f}")
    print(f"   Version: {account.version}")

    # Load and modify the account
    print("\n2. Loading account and making deposits")

    loaded_account = await repo.load(account_id)
    loaded_account.execute(DepositMoney(amount=50.0))
    loaded_account.execute(DepositMoney(amount=25.0))
    await repo.save(loaded_account)

    print(f"   Balance after deposits: ${loaded_account.state.balance:.2f}")
    print(f"   Version: {loaded_account.version}")

    # Make a withdrawal
    print("\n3. Making a withdrawal")

    account = await repo.load(account_id)
    account.execute(WithdrawMoney(amount=30.0))
    await repo.save(account)

    print(f"   Balance after withdrawal: ${account.state.balance:.2f}")
    print(f"   Version: {account.version}")

    # Show all events
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

    # Demonstrate business rule validation
    print("\n5. Business rule validation:")

    account = await repo.load(account_id)
    try:
        account.execute(WithdrawMoney(amount=1000.0))  # More than balance
    except CommandRejectedError as e:
        print(f"   Withdrawal blocked: {e}")

    try:
        account.execute(DepositMoney(amount=-50.0))  # Negative amount
    except CommandRejectedError as e:
        print(f"   Deposit blocked: {e}")

    # Show final state
    print("\n6. Final account state:")
    final_account = await repo.load(account_id)
    print(f"   Account ID: {final_account.aggregate_id}")
    print(f"   Owner: {final_account.state.owner_name}")
    print(f"   Balance: ${final_account.state.balance:.2f}")
    print(f"   Version: {final_account.version}")
    print(f"   Is Open: {final_account.state.is_open}")

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
