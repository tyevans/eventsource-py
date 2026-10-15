# Execute Commands on Aggregates

`AggregateRepository.execute` and `TenantAwareRepository.execute` collapse the
standard load-decide-save cycle for command-driven aggregates (`DeciderAggregate`
or any aggregate defining `execute(command)` / `apply_command(command)`) into a
single async call.

Instead of manually calling `load_or_create`, invoking `aggregate.execute(command)`,
and awaiting `save(aggregate)`, call `await repo.execute(aggregate_id, command)`.

For wiring `AggregateRepository` and inferring `aggregate_type`, see
[Wire an AggregateRepository](repository-pattern.md). For choosing between
imperative, declarative, and decider aggregate styles, see
[Aggregate Styles](../explanation/aggregate-styles.md) and
[The Decider Pattern](../explanation/decider-pattern.md).

## Single-round-trip command execution with `AggregateRepository.execute`

Define your state, commands (`DomainCommand`), events (`DomainEvent`), and a
`DeciderAggregate` subclass:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventStore,
)


class AccountState(BaseModel):
    owner: str = ""
    balance: float = 0.0
    is_open: bool = False


class OpenAccount(DomainCommand):
    account_id: UUID
    owner: str


class DepositMoney(DomainCommand):
    account_id: UUID
    amount: float


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner: str


class MoneyDeposited(DomainEvent):
    aggregate_type: str = "Account"
    amount: float


class Account(DeciderAggregate[AccountState, OpenAccount | DepositMoney]):
    aggregate_type = "Account"

    @staticmethod
    def initial_state() -> AccountState:
        return AccountState()

    @staticmethod
    def decide(
        command: OpenAccount | DepositMoney,
        state: AccountState,
    ) -> list[DomainEvent]:
        match command:
            case OpenAccount(account_id=aid, owner=owner):
                if state.is_open:
                    raise CommandRejectedError("account already open")
                return [AccountOpened(aggregate_id=aid, owner=owner)]
            case DepositMoney(account_id=aid, amount=amount):
                if not state.is_open:
                    raise CommandRejectedError("account not open")
                return [MoneyDeposited(aggregate_id=aid, amount=amount)]

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        match event:
            case AccountOpened(owner=owner):
                return state.model_copy(update={"owner": owner, "is_open": True})
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case _:
                return state
```

Execute commands directly through the repository:

```python
async def handle_commands() -> None:
    async with InMemoryEventStore() as store:
        repo = AggregateRepository(event_store=store, aggregate_factory=Account)
        account_id = uuid4()

        # Creates the aggregate if missing, executes OpenAccount, and saves
        account = await repo.execute(
            account_id,
            OpenAccount(account_id=account_id, owner="alice"),
        )
        assert account.state.is_open is True
        assert account.version == 1

        # Loads existing aggregate at version 1, executes DepositMoney, and saves
        account = await repo.execute(
            account_id,
            DepositMoney(account_id=account_id, amount=75.0),
        )
        assert account.state.balance == 75.0
        assert account.version == 2
```

### Controlling creation with `create_if_missing`

By default, `execute()` calls `load_or_create(aggregate_id)` (`create_if_missing=True`),
so commands that initialize a new stream work without a separate creation method.

When executing a command that must target an existing aggregate, pass
`create_if_missing=False`. The repository uses `load(aggregate_id)` and raises
`AggregateNotFoundError` before running the command if the stream does not exist:

```python
from eventsource import AggregateNotFoundError

try:
    await repo.execute(
        unknown_id,
        DepositMoney(account_id=unknown_id, amount=25.0),
        create_if_missing=False,
    )
except AggregateNotFoundError:
    ...
```

### Supported aggregate types

`execute()` looks up `aggregate.execute(command)` first, falling back to
`aggregate.apply_command(command)`. `DeciderAggregate` provides `execute(command)`
out of the box. If you call `repo.execute(...)` on an `AggregateRoot` or
`DeclarativeAggregate` that defines neither method, `execute()` raises `TypeError`:

```text
TypeError: CounterAggregate does not support execute(); execute() requires an
aggregate supporting execute(command) or apply_command(command), such as DeciderAggregate.
```

### Querying aggregates with `get_or_none` and `exists`

When looking up an aggregate that may not have been created yet, use
`await repo.get_or_none(aggregate_id)` to return `TAggregate | None` instead of
catching `AggregateNotFoundError` from `load()`, or `await repo.exists(aggregate_id)`
to check whether its stream has at least one persisted event without replaying
history:

```python
async def check_and_load_account(repo: AggregateRepository[Account]) -> None:
    account_id = uuid4()

    assert await repo.exists(account_id) is False
    assert await repo.get_or_none(account_id) is None

    await repo.execute(
        account_id,
        OpenAccount(account_id=account_id, owner="alice"),
    )

    assert await repo.exists(account_id) is True
    account = await repo.get_or_none(account_id)
    assert account is not None
    assert account.state.owner == "alice"
```

## Multi-tenant command execution with `TenantAwareRepository.execute`

`TenantAwareRepository` exposes the same `execute(aggregate_id, command, *, create_if_missing=True)`,
`get_or_none(aggregate_id)`, and `exists(aggregate_id)` methods while enforcing
your configured tenant policies:

- When `require_tenant_context=True`, `execute()`, `get_or_none()`, and `exists()`
  verify that an active `tenant_scope` is set before accessing the underlying
  repository.
- When `validate_on_save=True` (the default), `execute()` validates that every
  uncommitted event produced by the command matches the active tenant context
  before delegating to `save()`, raising `TenantMismatchError` on a mismatch.

```python
from uuid import UUID, uuid4
from eventsource import (
    AggregateRepository,
    TenantAwareRepository,
    TenantDomainEvent,
    tenant_scope,
)


class TenantAccountOpened(TenantDomainEvent):
    aggregate_type: str = "TenantAccount"
    owner: str


class OpenTenantAccount(DomainCommand):
    account_id: UUID
    owner: str


class TenantAccount(DeciderAggregate[AccountState, OpenTenantAccount]):
    aggregate_type = "TenantAccount"

    @staticmethod
    def initial_state() -> AccountState:
        return AccountState()

    @staticmethod
    def decide(command: OpenTenantAccount, state: AccountState) -> list[DomainEvent]:
        return [
            TenantAccountOpened.with_tenant_context(
                aggregate_id=command.account_id,
                owner=command.owner,
            )
        ]

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        if isinstance(event, TenantAccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        return state


async def run_tenant_command() -> None:
    async with InMemoryEventStore() as store:
        base_repo = AggregateRepository(
            event_store=store,
            aggregate_factory=TenantAccount,
        )
        tenant_repo = TenantAwareRepository(
            base_repo,
            require_tenant_context=True,
            validate_on_save=True,
        )

        tenant_id = uuid4()
        account_id = uuid4()

        async with tenant_scope(tenant_id):
            assert await tenant_repo.exists(account_id) is False
            assert await tenant_repo.get_or_none(account_id) is None

            account = await tenant_repo.execute(
                account_id,
                OpenTenantAccount(account_id=account_id, owner="alice"),
            )
            assert account.version == 1
            assert await tenant_repo.exists(account_id) is True

            loaded = await tenant_repo.get_or_none(account_id)
            assert loaded is not None
            assert loaded.state.owner == "alice"
```

See [Multi-Tenancy](multi-tenant.md) for full details on `tenant_scope`,
`TenantDomainEvent`, and storage-layer read isolation.
