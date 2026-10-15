# Execute Commands on Aggregates

`AggregateRepository.execute` and `TenantAwareRepository.execute` collapse the
standard load-decide-save cycle into a single async call for both:

- **Command-driven aggregates** (`DeciderAggregate` or any aggregate defining
  `execute(command)` / `apply_command(command)`), and
- **Imperative (`AggregateRoot`) and declarative (`DeclarativeAggregate`)
  aggregates** via callable commands (`lambda order: order.ship("TRACK-123")`).

Instead of manually calling `load_or_create`, invoking `aggregate.execute(command)`
or a mutating aggregate method, and awaiting `save(aggregate)`, call
`await repo.execute(aggregate_id, command)`.

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

### Callable commands for `AggregateRoot` and `DeclarativeAggregate`

When working with imperative (`AggregateRoot`) or declarative
(`DeclarativeAggregate`) aggregates that expose domain methods rather than a
single `execute(command)` dispatcher, pass a callable `Callable[[TAggregate], Any]`
(such as `lambda order: order.ship("TRACK-123")`) to `repo.execute()`:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    DomainEvent,
    InMemoryEventStore,
    handles,
)


class OrderState(BaseModel):
    customer_id: UUID | None = None
    tracking_number: str | None = None
    status: str = "draft"


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState()

    def create(self, customer_id: UUID) -> None:
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
                customer_id=customer_id,
            )
        )

    def ship(self, tracking_number: str) -> None:
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
                tracking_number=tracking_number,
            )
        )

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = self._state.model_copy(
            update={"customer_id": event.customer_id, "status": "created"}
        )

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        self._state = self._state.model_copy(
            update={"tracking_number": event.tracking_number, "status": "shipped"}
        )


async def handle_declarative_commands() -> None:
    async with InMemoryEventStore() as store:
        repo = AggregateRepository(event_store=store, aggregate_factory=OrderAggregate)
        order_id = uuid4()
        customer_id = uuid4()

        # Creates the aggregate if missing, invokes the callable, and saves
        order = await repo.execute(
            order_id,
            lambda order: order.create(customer_id=customer_id),
        )
        assert order.state.status == "created"
        assert order.version == 1

        # Loads existing aggregate, invokes order.ship(...), and saves
        order = await repo.execute(
            order_id,
            lambda order: order.ship("TRACK-123"),
            create_if_missing=False,
        )
        assert order.state.tracking_number == "TRACK-123"
        assert order.version == 2
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

### Supported aggregate types and command dispatch

`execute(aggregate_id, command)` dispatches `command` in two ways:

1. **Callable commands (`Callable[[TAggregate], Any]`)**: If `command` is a
   callable (and not a class type), `execute()` invokes `command(aggregate)`
   directly with the loaded or created aggregate instance. This works with any
   `AggregateRoot`, `DeclarativeAggregate`, or `DeciderAggregate`.
2. **Command objects**: Otherwise, `execute()` looks up `aggregate.execute(command)`
   first, falling back to `aggregate.apply_command(command)`. `DeciderAggregate`
   provides `execute(command)` out of the box.

If you pass a non-callable command object to an `AggregateRoot` or
`DeclarativeAggregate` that defines neither `execute` nor `apply_command`,
`execute()` raises `TypeError`:

```text
TypeError: CounterAggregate does not support execute(); pass a callable (e.g.
lambda agg: agg.do_something()) or use an aggregate supporting execute(command)
or apply_command(command), such as DeciderAggregate.
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

### Batch saving and loading with `save_all` and `load_many`

When a workflow mutates or inspects multiple aggregates of the same type, use
`await repo.save_all(*aggregates)` and
`await repo.load_many(aggregate_ids, *, ignore_missing=False) -> dict[UUID, TAggregate]`:

- **`await repo.save_all(*aggregates)`**: accepts either variadic aggregate
  arguments (`await repo.save_all(order1, order2)`) or a single iterable of
  aggregates (`await repo.save_all([order1, order2])`), persisting each aggregate
  in sequence via `save()`.
- **`await repo.load_many(aggregate_ids, *, ignore_missing=False) -> dict[UUID, TAggregate]`**:
  loads multiple aggregates by ID and returns a dictionary mapping each
  `aggregate_id` (`UUID`) to its reconstructed `TAggregate` instance. By default
  (`ignore_missing=False`), raises `AggregateNotFoundError` if any requested
  aggregate stream does not exist; pass `ignore_missing=True` to omit missing
  aggregates from the returned mapping.

```python
async def batch_save_and_load_orders(
    repo: AggregateRepository[OrderAggregate],
) -> None:
    order1_id, order2_id, missing_id = uuid4(), uuid4(), uuid4()
    customer_id = uuid4()

    order1 = repo.create_new(order1_id)
    order1.create(customer_id=customer_id)

    order2 = repo.create_new(order2_id)
    order2.create(customer_id=customer_id)
    order2.ship("TRACK-200")

    # Pass variadic arguments or an iterable: await repo.save_all([order1, order2])
    await repo.save_all(order1, order2)

    # Load multiple aggregates by ID -> dict[UUID, OrderAggregate]
    loaded = await repo.load_many([order1_id, order2_id])
    assert loaded[order1_id].version == 1
    assert loaded[order2_id].state.tracking_number == "TRACK-200"

    # Skip non-existent aggregate IDs with ignore_missing=True
    partial = await repo.load_many(
        [order1_id, missing_id],
        ignore_missing=True,
    )
    assert list(partial.keys()) == [order1_id]
```

## Multi-tenant command execution with `TenantAwareRepository.execute`

`TenantAwareRepository` exposes the same `execute(aggregate_id, command, *, create_if_missing=True)`
(accepting both `DeciderAggregate` command objects and `lambda agg: ...` callables),
`get_or_none(aggregate_id)`, `exists(aggregate_id)`, `save_all(*aggregates)`, and
`load_many(aggregate_ids, *, ignore_missing=False) -> dict[UUID, TAggregate]`
methods while enforcing your configured tenant policies:

- When `require_tenant_context=True`, `execute()`, `get_or_none()`, `exists()`,
  `save_all()`, and `load_many()` verify that an active `tenant_scope` is set
  before accessing the underlying repository.
- When `validate_on_save=True` (the default), `execute()` and `save_all()`
  validate that every uncommitted event on each aggregate matches the active
  tenant context before delegating to `save()`, raising `TenantMismatchError` on
  a mismatch.

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
        second_id = uuid4()

        async with tenant_scope(tenant_id):
            assert await tenant_repo.exists(account_id) is False
            assert await tenant_repo.get_or_none(account_id) is None

            account = await tenant_repo.execute(
                account_id,
                OpenTenantAccount(account_id=account_id, owner="alice"),
            )
            assert account.version == 1
            assert await tenant_repo.exists(account_id) is True

            second = TenantAccount(second_id)
            second.execute(OpenTenantAccount(account_id=second_id, owner="bob"))
            await tenant_repo.save_all(second)

            accounts = await tenant_repo.load_many(
                [account_id, second_id],
                ignore_missing=False,
            )
            assert accounts[account_id].state.owner == "alice"
            assert accounts[second_id].state.owner == "bob"
```

See [Multi-Tenancy](multi-tenant.md) for full details on `tenant_scope`,
`TenantDomainEvent`, and storage-layer read isolation.
