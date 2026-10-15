"""Basic Usage Example: event sourcing in the decider style."""

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

    owner_name: str = ""
    balance: float = 0.0
    is_open: bool = False


# --8<-- [end:state]


class OpenAccount(DomainCommand):
    account_id: UUID
    owner_name: str
    initial_balance: float = 0.0


class DepositMoney(DomainCommand):
    account_id: UUID
    amount: float


class WithdrawMoney(DomainCommand):
    account_id: UUID
    amount: float


class BankAccountAggregate(DeciderAggregate[BankAccountState]):
    """Bank account in the decider style."""

    aggregate_type = "BankAccount"

    @staticmethod
    def initial_state() -> BankAccountState:
        return BankAccountState()

    @staticmethod
    def decide(command: object, state: BankAccountState) -> list[DomainEvent]:
        match command, state:
            case OpenAccount(
                account_id=account_id, owner_name=name, initial_balance=balance
            ), BankAccountState(is_open=False):
                return [
                    AccountOpened(aggregate_id=account_id, owner_name=name, initial_balance=balance)
                ]
            case OpenAccount(), _:
                raise CommandRejectedError("account is already open", command=command)
            case DepositMoney(account_id=account_id, amount=amount), BankAccountState(is_open=True):
                if amount <= 0:
                    raise CommandRejectedError("deposit must be positive", command=command)
                return [MoneyDeposited(aggregate_id=account_id, amount=amount)]
            case WithdrawMoney(account_id=account_id, amount=amount), BankAccountState(
                is_open=True
            ):
                if amount <= 0:
                    raise CommandRejectedError("withdrawal must be positive", command=command)
                if amount > state.balance:
                    raise CommandRejectedError("insufficient funds", command=command)
                return [MoneyWithdrawn(aggregate_id=account_id, amount=amount)]
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


async def main():
    """Demonstrate basic event sourcing usage."""
    print("=" * 60 + "\nEvent Sourcing Basic Usage Example\n" + "=" * 60)

    event_store = InMemoryEventStore()
    repo = AggregateRepository(event_store=event_store, aggregate_factory=BankAccountAggregate)

    account_id = uuid4()
    print(f"\n1. Opening account {account_id}")
    account = repo.create_new(account_id)
    account.execute(OpenAccount(account_id=account_id, owner_name="Alice", initial_balance=100.0))
    await repo.save(account)
    print(f"   Owner: {account.state.owner_name}")
    print(f"   Balance: ${account.state.balance:.2f}")
    print(f"   Version: {account.version}")

    print("\n2. Loading account and making deposits")
    loaded_account = await repo.load(account_id)
    loaded_account.execute(DepositMoney(account_id=account_id, amount=50.0))
    loaded_account.execute(DepositMoney(account_id=account_id, amount=25.0))
    await repo.save(loaded_account)
    print(f"   Balance after deposits: ${loaded_account.state.balance:.2f}")
    print(f"   Version: {loaded_account.version}")

    print("\n3. Making a withdrawal")
    account = await repo.load(account_id)
    account.execute(WithdrawMoney(account_id=account_id, amount=30.0))
    await repo.save(account)
    print(f"   Balance after withdrawal: ${account.state.balance:.2f}")
    print(f"   Version: {account.version}")

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
    for cmd, label in [
        (WithdrawMoney(account_id=account_id, amount=1000.0), "Withdrawal"),
        (DepositMoney(account_id=account_id, amount=-50.0), "Deposit"),
    ]:
        try:
            account.execute(cmd)
        except CommandRejectedError as e:
            print(f"   {label} blocked: {e}")

    print("\n6. Final account state:")
    final = await repo.load(account_id)
    print(f"   Account ID: {final.aggregate_id}")
    print(f"   Owner: {final.state.owner_name}")
    print(f"   Balance: ${final.state.balance:.2f}")
    print(f"   Version: {final.version}")
    print(f"   Is Open: {final.state.is_open}")
    print("\n" + "=" * 60 + "\nExample completed successfully!\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
