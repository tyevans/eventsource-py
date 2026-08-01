# Getting Started

In this tutorial you will build a working event-sourced bank account from scratch,
using nothing but Python and the `eventsource` package. No database, no Docker, no
message broker — everything runs in memory so you can focus on the ideas.

By the end you will have written domain events, a **decider** — the aggregate style
this library recommends — that enforces business rules with a pair of pure functions,
and a repository that saves and reloads that aggregate from an event store. Along the
way you will watch the decider reject a command that would break its invariants.

Follow the steps in order and type (or copy) each snippet as you go. Every code block
shows its own imports, so the finished file at the end of Step 4 runs top to bottom
with nothing missing.

Everything you write here uses the in-memory event store. The same code runs unchanged
against PostgreSQL, SQLite, Redis, RabbitMQ, or Kafka by swapping the store object you
construct in Step 4 — see [Optional Backends](#optional-backends) below.

Expect this to take about 15 minutes.

---

## What You'll Build

You will build a `BankAccount` — the smallest domain that still has real invariants to
protect.

Three events make up the account's history:

| Event | Fields | Meaning |
| --- | --- | --- |
| `AccountOpened` | `owner_name`, `initial_balance` | the account came into existence |
| `MoneyDeposited` | `amount` | money was added |
| `MoneyWithdrawn` | `amount` | money was taken out |

Each is a frozen pydantic model registered with the library's event registry by the
`@register_event` decorator so it can be rebuilt from storage later. None of them
declares `event_type` by hand — see [Step 1](#step-1-define-your-events) for why.

Around those events you will write:

- **`AccountState`** — a frozen pydantic `BaseModel` holding `account_id`,
  `owner_name`, `balance`, and `is_open`. This is derived data, not the source of
  truth — it exists so `decide()` has something to read.
- **Three commands** — `OpenAccount`, `Deposit`, and `Withdraw`, each a
  `DomainCommand` subclass carrying the intent's payload.
- **`BankAccountAggregate`** — a
  `DeciderAggregate[AccountState, AccountCommand]` with `aggregate_type =
  "BankAccount"`, a static `initial_state()` that returns an unopened account, a
  static `decide()` that turns a command plus the current state into the events it
  produces (or raises `CommandRejectedError`), and a static `evolve()` that folds
  each event into a new `AccountState`. Business rules — insufficient funds, a
  double-open, a non-positive amount — live in `decide()` and nowhere else.
- **An `InMemoryEventStore` and an `AggregateRepository`** — the persistence seam.
  You will call `repo.create_new(...)`, run commands with `account.execute(...)`,
  `await repo.save(account)`, and `await repo.load(account_id)`, and watch the
  aggregate's version climb by one for every event applied.

Then you will try a command that should fail — withdrawing more than the balance — to
confirm the decider refuses it with `CommandRejectedError` rather than silently
recording a bad event.

Projections and subscriptions — the read side of the same events — are their own
tutorial once you are comfortable here; see
[Projections](tutorials/06-projections.md).

**What you will not need:** no database, no Docker, no message broker, no
configuration files. `InMemoryEventStore` ships in the core package, so the only
install is `eventsource-py` itself. Optional backends are mentioned at the very end,
once the concepts are in place.

---

## Prerequisites

Before you start, make sure of three things.

**Python 3.13 or newer.** The package declares `requires-python = ">=3.13"`.
Check yours:

```bash
python --version
```

**A virtual environment to work in.** Everything you install below lands in one
directory you can throw away afterwards:

```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
```

**Comfort with `async`/`await`.** The event store and repository APIs are async, and
Step 4's snippet lives inside an `async def main()` launched with `asyncio.run(main())`.
`decide()` and `evolve()` themselves are plain synchronous functions — a decider is
never async. If you have written an `asyncio` script before, you know enough.

You do *not* need prior event sourcing experience — that is what the tutorial is for.
Familiarity with pydantic helps, since events, commands, and aggregate state are all
pydantic `BaseModel` subclasses, but each model is explained as it appears.

You also do not need a database, a message broker, Docker, or any configuration files.
The in-memory event store ships in the core package, so the entire tutorial runs from
one `pip install`.

---

## Installation

With your virtual environment activated, install the package:

```bash
pip install eventsource-py
```

That is the whole install for this tutorial. The distribution is named
**`eventsource-py`** on PyPI but you import it as **`eventsource`**:

```python
import eventsource
```

The core package depends only on `pydantic` (v2) and `sqlalchemy` (v2), and it brings
along everything the next four steps need: `DomainEvent`, `DomainCommand`,
`register_event`, `DeciderAggregate`, `CommandRejectedError`, `AggregateRepository`,
and `InMemoryEventStore`.

Check that the install worked:

```bash
python -c "from eventsource import InMemoryEventStore; print('ok')"
```

```
ok
```

If that prints `ok`, you are ready for Step 1.

### Optional Backends

Storage backends and message brokers are *extras* — you opt into them by name, and each
one pulls in its own driver. You do not need any of them now; they are listed here so
you know what to reach for when you swap the in-memory store out at the end of the
tutorial.

| Extra | Install | Enables |
| --- | --- | --- |
| `postgresql` | `pip install "eventsource-py[postgresql]"` | PostgreSQL event store, snapshots, outbox, advisory locks (`asyncpg`) |
| `sqlite` | `pip install "eventsource-py[sqlite]"` | SQLite event store and snapshots (`aiosqlite`) |
| `redis` | `pip install "eventsource-py[redis]"` | Redis event bus |
| `rabbitmq` | `pip install "eventsource-py[rabbitmq]"` | RabbitMQ event bus (`aio-pika`) |
| `kafka` | `pip install "eventsource-py[kafka]"` | Kafka event bus (`aiokafka`) |
| `telemetry` | `pip install "eventsource-py[telemetry]"` | OpenTelemetry tracing integration |

There is also a `kafka-schema-registry` extra, which adds `confluent-kafka` on top of
`aiokafka` for schema-registry-backed Kafka setups, and two convenience bundles:
`eventsource-py[all-backends]` for both database backends, and `eventsource-py[all]`
for every extra in the table at once.

Quoting matters — most shells treat unquoted square brackets as glob characters, so
write `pip install "eventsource-py[postgresql]"` with the quotes.

Because these are optional, the library guards each driver import behind an
availability flag and raises a clear error — `RedisNotAvailableError` and its siblings —
if you construct a backend whose driver is missing, rather than failing at import time.
Installing an extra later never requires changing the code you write in this tutorial;
only the store object you construct in Step 4 changes.

---

## Step 1: Define Your Events

Events are the source of truth. Everything else in this tutorial — state, the
decider, the balance you print at the end — is derived by replaying them. So this is
the step that matters most: get the events right and the rest follows.

Create a file called `bank.py` and start with the imports:

```python
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    CommandRejectedError,
    DeciderAggregate,
    DomainCommand,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
```

Now define the three things that can happen to a bank account. Add these classes to
`bank.py`:

```python
@register_event
class AccountOpened(DomainEvent):
    """Event emitted when a bank account is opened."""

    aggregate_type: str = "BankAccount"

    owner_name: str
    initial_balance: Decimal


@register_event
class MoneyDeposited(DomainEvent):
    """Event emitted when money is deposited."""

    aggregate_type: str = "BankAccount"

    amount: Decimal


@register_event
class MoneyWithdrawn(DomainEvent):
    """Event emitted when money is withdrawn."""

    aggregate_type: str = "BankAccount"

    amount: Decimal
```

Three classes, three facts. Each one is a `DomainEvent` subclass sharing
`aggregate_type = "BankAccount"`, registered with the library's event registry by the
`@register_event` decorator sitting above it. The payload fields are the only thing
you had to think about: `owner_name` and `initial_balance` for the opening, `amount`
for the two money movements — and `Decimal`, never `float`, for anything that is
money. Notice how short each class is — the base class already carries `event_id`,
`occurred_at`, `aggregate_id`, and `aggregate_version`, so your definitions stay
focused on the domain.

There is no `__init__`, no `save`, no behaviour of any kind. Events are records, not
actors; the decider in Step 3 is what decides when one gets created.

Nothing runs a command yet, but the classes are not inert either: simply importing
this module executes the three `@register_event` decorators, and that is what lets the
store turn stored rows back into `AccountOpened`, `MoneyDeposited`, and
`MoneyWithdrawn` instances when you reload the account.

### What Each Piece Does

**`DomainEvent` subclass.** `DomainEvent` is a pydantic `BaseModel`, so your event is
one too: annotate the payload fields you care about (`owner_name: str`,
`amount: Decimal`) and pydantic validates them on construction. The base class already
supplies the plumbing every event needs — `event_id`, `occurred_at` (a UTC timestamp),
`aggregate_id`, `aggregate_version`, `event_version`, plus optional `tenant_id`,
`actor_id`, `correlation_id`, `causation_id`, and a `metadata` dict. You never declare
those; you just get them.

**`event_type`.** The string written into storage to identify this event on the way
back out. You never declare it: `DomainEvent.__init_subclass__` auto-derives
`event_type` from the class name, so `class AccountOpened(DomainEvent)` gets
`"AccountOpened"` for free, with no line of code and nothing that can drift out of
sync with the class. Declare it explicitly only for the one case that needs a name
different from the class — pinning a versioned wire name once a schema has shipped,
e.g. `event_type: str = "account_opened_v2"` on a class named `AccountOpenedV2`, so
old rows deserialize under a name the class itself no longer matches.

**`aggregate_type`.** Which kind of thing this event happened to. All three events use
`"BankAccount"`, which is what groups them into one stream. The decider you write in
Step 3 declares the same string, and so does the repository you build in Step 4 —
they have to match.

**`@register_event`.** Adds the class to the library's event registry, keyed by
`event_type`. When the store loads a stream it finds rows containing a type name and a
JSON payload; the registry is how it turns `"MoneyDeposited"` back into your
`MoneyDeposited` class. Forget the decorator and events save fine but come back
unrecognizable. The decorator works bare (`@register_event`) or called
(`@register_event(event_type="account.opened")`) when you want the registry key to
differ from the class.

### Two Conventions Worth Internalizing

**Name events in the past tense, commands in the imperative.** `AccountOpened`, not
`OpenAccount` — that name belongs to the command in Step 2. A command is a request
that may be refused; an event is a fact that already happened and cannot be argued
with. By the time an event exists, the decision is made.

**Events are frozen.** `DomainEvent` sets `model_config = ConfigDict(frozen=True)`, so
assigning to a field on an existing event raises a pydantic
`ValidationError`. That is deliberate: history is append-only. Correcting a mistake
means appending a new event that reverses it, never editing the old one — which is
exactly why an audit trail built from events can be trusted.

One more design note before moving on. `MoneyDeposited` carries only `amount`, not the
resulting balance. Events record *what happened*, not *what everything looks like
afterwards*. The balance is computed by folding the events together, which is what you
build in the next two steps.

---

## Step 2: Model State and Commands

Events are the truth; state is a convenience. `AccountState` is the answer to "what
does this account look like *right now*?" — a value `decide()` can read a balance off
without walking the whole history itself. Commands are the other half: requests that
may or may not be granted.

Add these to `bank.py`, below the events:

```python
class AccountState(BaseModel):
    """Current, derived view of a bank account."""

    model_config = {"frozen": True}

    account_id: UUID
    owner_name: str = ""
    balance: Decimal = Decimal("0")
    is_open: bool = False


class OpenAccount(DomainCommand):
    """Request to open a new bank account."""

    account_id: UUID
    owner_name: str
    initial_balance: Decimal = Decimal("0")


class Deposit(DomainCommand):
    """Request to deposit money into an account."""

    account_id: UUID
    amount: Decimal


class Withdraw(DomainCommand):
    """Request to withdraw money from an account."""

    account_id: UUID
    amount: Decimal


AccountCommand = OpenAccount | Deposit | Withdraw
```

### Reading `AccountState`

It is a plain, frozen pydantic `BaseModel` — not a library base class, not a special
"state" type, just a model you define. The library only asks two things of it:
pydantic validates it on construction, and `model_dump(mode="json")` /
`model_validate(...)` can round-trip it (that pair is what snapshotting uses).
Frozen means `evolve()` in Step 3 produces a *new* instance with
`model_copy(update={...})` on every event rather than mutating one in place — the same
discipline events themselves follow, applied to their fold.

- **`account_id: UUID`** — required, no default. Every state instance is tied to a
  specific account, and `initial_state()` fills it from the aggregate id.
- **`owner_name`**, **`balance`**, **`is_open`** — defaulted, and the defaults
  describe an account that does not exist yet: nameless, empty, closed. That is what
  `initial_state()` returns before any event has been applied.

Every field here is recoverable from the event stream: `balance` is
`initial_balance` plus the deposits minus the withdrawals, `owner_name` comes from
`AccountOpened`. If you deleted this class and rewrote it with different fields, the
stored events would not change at all — you would simply be folding them up a
different way.

### Reading the commands

`OpenAccount`, `Deposit`, and `Withdraw` each subclass `DomainCommand`, a frozen
pydantic model like `DomainEvent` but for requests rather than facts. The base class
manages `command_id`, `correlation_id`, and `actor_id` for you — you only declare the
payload the decider needs to evaluate the request: which account, and (for two of the
three) how much money. `AccountCommand`, the union of all three, is what makes
`DeciderAggregate[AccountState, AccountCommand]` in Step 3 a two-parameter subscript —
it is what your editor and mypy use to check that `decide()` handles every command you
defined, with no case silently missing.

With state and commands modelled, you have every input `decide()` and `evolve()` need.
Step 3 writes the two functions themselves.

---

## Step 3: Build the Decider

The decider is where events, state, and commands meet. `decide()` owns every business
rule: given a command and the current state, it returns the events that command
produces, or raises `CommandRejectedError` and produces nothing. `evolve()` owns the
fold: given the current state and an event, it returns the next state. Neither
function touches `self` — they are the whole domain, and everything else (replay,
version tracking, snapshots, provenance stamping) is inherited from
`DeciderAggregate`.

Add this class to `bank.py`, below `AccountCommand`:

```python
class BankAccountAggregate(DeciderAggregate[AccountState, AccountCommand]):
    """Event-sourced bank account, decider style."""

    aggregate_type = "BankAccount"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> AccountState:
        """Return the state of an account before any event has occurred."""
        return AccountState(account_id=aggregate_id)

    @staticmethod
    def decide(command: AccountCommand, state: AccountState) -> list[DomainEvent]:
        """Given current state, return the events a command produces, or raise."""
        match command, state:
            case OpenAccount(), AccountState(is_open=True):
                raise CommandRejectedError("account already opened", command=command)
            case OpenAccount(initial_balance=initial_balance), _ if initial_balance < 0:
                raise CommandRejectedError(
                    "initial balance cannot be negative", command=command
                )
            case OpenAccount(owner_name=owner_name, initial_balance=initial_balance), _:
                return [
                    AccountOpened(
                        aggregate_id=state.account_id,
                        owner_name=owner_name,
                        initial_balance=initial_balance,
                    )
                ]
            case Deposit(), AccountState(is_open=False):
                raise CommandRejectedError("account is not open", command=command)
            case Deposit(amount=amount), _ if amount <= 0:
                raise CommandRejectedError(
                    "deposit amount must be positive", command=command
                )
            case Deposit(amount=amount), _:
                return [MoneyDeposited(aggregate_id=state.account_id, amount=amount)]
            case Withdraw(), AccountState(is_open=False):
                raise CommandRejectedError("account is not open", command=command)
            case Withdraw(amount=amount), _ if amount <= 0:
                raise CommandRejectedError(
                    "withdrawal amount must be positive", command=command
                )
            case Withdraw(amount=amount), AccountState(balance=balance) if amount > balance:
                raise CommandRejectedError(
                    f"insufficient balance: {balance}", command=command
                )
            case Withdraw(amount=amount), _:
                return [MoneyWithdrawn(aggregate_id=state.account_id, amount=amount)]

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        """Return the next state after an event. Total: unknown events pass through."""
        match event:
            case AccountOpened(owner_name=owner_name, initial_balance=initial_balance):
                return state.model_copy(
                    update={
                        "owner_name": owner_name,
                        "balance": initial_balance,
                        "is_open": True,
                    }
                )
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case MoneyWithdrawn(amount=amount):
                return state.model_copy(update={"balance": state.balance - amount})
            case _:
                return state
```

### Reading `decide()`

Read it top to bottom as guard-then-grant, one command at a time. `OpenAccount`
against an already-open account, a negative starting balance, a closed account
receiving `Deposit` or `Withdraw`, a non-positive amount, an overdraft — each of those
is a `match` arm that raises `CommandRejectedError` with a message that says why, and
nothing else runs. The arm below it is what happens when nothing objects: construct
the event (or events — `decide()` returns a list, so one command producing several
facts is a first-class case, not a workaround) and return it. **`decide()` never calls
`evolve()` and never touches `self`.** It is a pure function of `(command, state)` — no
I/O, no side effects, easy to unit test by calling it directly with no aggregate
involved at all.

The overdraft check — `amount > balance` — is the rule you will watch fail in Step 4.
It fires *before* any event is constructed, so a rejected withdrawal never touches the
event stream. `CommandRejectedError` is "the domain said no": it is what you raise for
a business-rule refusal, as distinct from a programming error.

### Reading `evolve()`

`evolve()` is a dispatch table with one case per event type — the mirror image of
`decide()`. `AccountOpened` sets the account up from nothing; the two money events fold
into whatever state already exists, producing a new instance with
`model_copy(update={...})` rather than assigning to a field. `evolve()` must be
**total**: the `case _: return state` at the end means an event type this function
does not recognize leaves state unchanged instead of raising, which matters because
`evolve()` runs during every replay of every event ever recorded, including ones added
by a future version of this code that an older `evolve()` has never seen.

Two rules about `evolve()` that are easy to get wrong and painful to debug:

**No validation here.** `evolve()` must never raise on a business rule. It runs during
replay as well as on new commands, and history is not negotiable — an event that was
accepted in 2024 must still be applicable in 2026, even if the rules have since
tightened. Validation belongs in `decide()`.

**No side effects here.** No emails, no HTTP calls, no writes. Every time an aggregate
is loaded from the store, every event in its stream is replayed through this method.
Anything with an effect would fire again on every load.

### What `DeciderAggregate` gives you for free

`aggregate_type = "BankAccount"` is the class attribute that must match the
`aggregate_type` on your events and the string you pass to the repository in Step 4.
The base class requires it — a subclass that doesn't set it raises
`AggregateTypeNotSetError` at construction, rather than quietly ending up with events
nobody can find.

You never call `execute()` — the one public entry point `AggregateRoot` inherits from
`DeciderAggregate` — until Step 4, but it is worth knowing what it does: it runs
`decide(command, self.state)`, and for every event that comes back, stamps
`aggregate_version` and `aggregate_type` (plus `causation_id`, `correlation_id`,
`actor_id`, and `tenant_id` from the command, whenever the event didn't set them
itself) and applies it. `decide()` completes in full before any event is applied, so a
rejection leaves the aggregate exactly as it was — no partial state, no version bump,
nothing queued.

You now have a complete decider: a state to start from, a function that decides
whether a command is allowed, and a function that folds the events it produces into
new state. Nothing is persisted yet — Step 4 fixes that.

---

## Step 4: Persist It

Add the persistence seam to `bank.py` and run the whole thing. This is the finished
file — the earlier snippets, plus a `main()` that exercises it end to end.

```python
async def main() -> None:
    store = InMemoryEventStore()
    repo: AggregateRepository[BankAccountAggregate] = AggregateRepository(
        event_store=store,
        aggregate_factory=BankAccountAggregate,
        aggregate_type="BankAccount",
    )

    account_id = uuid4()
    account = repo.create_new(account_id)
    account.execute(
        OpenAccount(account_id=account_id, owner_name="Ada Lovelace", initial_balance=Decimal("100"))
    )
    account.execute(Deposit(account_id=account_id, amount=Decimal("50")))
    await repo.save(account)

    print(f"version after save: {account.version}")
    print(f"uncommitted after save: {account.uncommitted_events}")

    reloaded = await repo.load(account_id)
    print(f"reloaded balance: {reloaded.state.balance}")
    print(f"reloaded version: {reloaded.version}")

    try:
        reloaded.execute(Withdraw(account_id=account_id, amount=Decimal("1000")))
    except CommandRejectedError as e:
        print(f"rejected: {e}")

    print(f"balance after rejected withdrawal: {reloaded.state.balance}")


if __name__ == "__main__":
    asyncio.run(main())
```

`repo.create_new(account_id)` constructs a `BankAccountAggregate` at version 0 with no
events applied — `DeciderAggregate.__init__` calls `initial_state()` for you, so
`account.state` is already a real `AccountState`, never `None`. Two calls to
`execute()` run `decide()` and `evolve()` and queue two events in
`account.uncommitted_events`; `await repo.save(account)` appends them to the store and
clears that list — `account.version` is `2` after the print above, and
`uncommitted_events` is empty.

`await repo.load(account_id)` reads the stream back from the store, replays every
event through `evolve()`, and hands you a fresh `BankAccountAggregate` whose `state`
matches what you saved — `reloaded.state.balance` prints `150`. Then the rejected
`Withdraw` proves the point of Step 3: `CommandRejectedError` propagates out of
`execute()`, the account's balance is unchanged, and nothing new was appended to the
stream.

Run it:

```bash
python bank.py
```

```
version after save: 2
uncommitted after save: []
reloaded balance: 150
reloaded version: 2
rejected: insufficient balance: 150
balance after rejected withdrawal: 150
```

That is the whole loop: events as the source of truth, a decider that turns commands
into events (or refuses them), and a repository that moves those events between memory
and storage. Everything past this point — projections, subscriptions, other backends —
builds on exactly this pattern; see [Projections](tutorials/06-projections.md)
to keep going, or [aggregate styles](explanation/aggregate-styles.md) if you want to
see the same domain written the other two ways this library supports.
