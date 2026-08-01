# Getting Started

In this tutorial you will build a working event-sourced bank account from scratch,
using nothing but Python and the `eventsource` package. No database, no Docker, no
message broker — everything runs in memory so you can focus on the ideas.

By the end you will have written domain events, an aggregate that enforces business
rules, and a repository that saves and reloads that aggregate from an event store. You
will then add a projection — a read model built by replaying those same events — and
run it under a `SubscriptionManager`, the coordinated catch-up-then-live pattern the
library recommends for production use. Finally you will inspect the raw event history
the store kept for you and watch the aggregate reject a command that would break its
invariants.

Follow the steps in order and type (or copy) each snippet as you go. The snippets are
adapted from the runnable programs in the repository's `examples/` directory —
`examples/basic_usage.py` for the aggregate and repository work, and
`examples/projection_example.py` for the projection and subscription work — so at any
point you can run a finished version and compare your output against the transcript.

Everything you write here uses the in-memory backends. The same code runs unchanged
against PostgreSQL, SQLite, Redis, RabbitMQ, or Kafka by swapping the store and bus
objects you construct in Step 4 and Step 6; the last sections show how.

Expect this to take about 20 minutes.

---

## What You'll Build

You will build a `BankAccount` — the smallest domain that still has real invariants to
protect — and then a read model fed by the same events.

Three events make up the account's history:

| Event | Fields | Meaning |
| --- | --- | --- |
| `AccountOpened` | `owner_name`, `initial_balance` | the account came into existence |
| `MoneyDeposited` | `amount` | money was added |
| `MoneyWithdrawn` | `amount` | money was taken out |

Each is a frozen pydantic model carrying `event_type` and `aggregate_type="BankAccount"`,
registered with the library's event registry by the `@register_event` decorator so it
can be rebuilt from storage later.

Around those events you will write:

- **`BankAccountState`** — a plain pydantic `BaseModel` holding `account_id`,
  `owner_name`, `balance`, and `is_open`. This is derived data, not the source of truth.
- **`BankAccountAggregate`** — an `AggregateRoot[BankAccountState]` with
  `_get_initial_state()` and an `_apply()` method that folds each event into new state,
  plus `open()`, `deposit()`, and `withdraw()` command methods that check the business
  rules *before* calling `apply_event()`.
- **An `InMemoryEventStore` and an `AggregateRepository`** — the persistence seam.
  You will call `repo.create_new(...)`, `await repo.save(account)`, and
  `await repo.load(account_id)`, and watch the aggregate's version climb by one for
  every event appended.
- **An `AccountListProjection`** — a read model that keeps a dictionary of account
  summaries. It implements the subscriber contract the library expects: a
  `subscribed_to()` method returning the event types it cares about, and an
  `async handle(event)` method that updates the read model.
- **A `SubscriptionManager`** — wired to the event store, an `InMemoryEventBus`, and an
  `InMemoryCheckpointRepository`. You will register the projection with
  `await manager.subscribe(projection, config=SubscriptionConfig(start_from="beginning"),
  name="AccountList")`, call `await manager.start()` to replay the history you already
  wrote, then keep emitting events and watch the projection stay current — the
  catch-up-then-live pattern this library recommends for production.

Then you will look behind the repository at the raw stream with
`await store.read_stream(stream)` (a `StreamId(account_id, "BankAccount")`), and finish by trying two
commands that should fail — withdrawing more than the balance, and depositing a
non-positive amount — to confirm the aggregate refuses them with a `ValueError`
rather than silently recording a bad event.

**What you will not need:** no database, no Docker, no message broker, no
configuration files. `InMemoryEventStore`, `InMemoryEventBus`, and
`InMemoryCheckpointRepository` all ship in the core package, so the only install is
`eventsource-py` itself. Optional backends are mentioned at the very end, once the
concepts are in place.

---

## Prerequisites

Before you start, make sure of three things.

**Python 3.11 or newer.** The package declares `requires-python = ">=3.11"`, and
3.11, 3.12, and 3.13 are the supported versions. Check yours:

```bash
python --version
```

**A virtual environment to work in.** Everything you install below lands in one
directory you can throw away afterwards:

```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
```

**Comfort with `async`/`await`.** The event store, repository, and subscription APIs
are async throughout, and every snippet in this tutorial lives inside an
`async def main()` that you launch with `asyncio.run(main())`. If you have written an
`asyncio` script before, you know enough.

You do *not* need prior event sourcing experience — that is what the tutorial is for.
Familiarity with pydantic helps, since events and aggregate state are pydantic
`BaseModel` subclasses, but each model is explained as it appears.

You also do not need a database, a message broker, Docker, or any configuration files.
The in-memory event store, event bus, and checkpoint repository all ship in the core
package, so the entire tutorial runs from one `pip install`.

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
along everything the next eight steps need: `DomainEvent`, `register_event`,
`AggregateRoot`, `AggregateRepository`, the projection and subscription machinery, and
the in-memory implementations of the event store, event bus, and checkpoint repository.

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
you know what to reach for when you swap the in-memory objects out at the end of the
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
only the store and bus objects you construct in Step 4 and Step 6 change.

---

## Step 1: Define Your Events

Events are the source of truth. Everything else in this tutorial — state, the
aggregate, the balance you print at the end — is derived by replaying them. So this is
the step that matters most: get the events right and the rest follows.

Create a file called `bank.py` and start with the imports:

```python
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
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
```

Three classes, three facts. Each one is a `DomainEvent` subclass tagged with its own
`event_type`, sharing `aggregate_type = "BankAccount"`, and registered by the
`@register_event` decorator sitting above it. The payload fields are the only thing
you had to think about: `owner_name` and `initial_balance` for the opening,
`amount` for the two money movements. Notice how short each class is — the base class
already carries `event_id`, `occurred_at`, `aggregate_id`, and `aggregate_version`, so
your definitions stay focused on the domain.

There is no `__init__`, no `save`, no behaviour of any kind. Events are records, not
actors; the aggregate in Step 3 is what decides when one gets created.

Nothing runs a command yet, but the classes are not inert either: simply importing
this module executes the three `@register_event` decorators, and that is what lets the
store turn stored rows back into `AccountOpened`, `MoneyDeposited`, and
`MoneyWithdrawn` instances when you read the stream in Step 6.

### What Each Piece Does

**`DomainEvent` subclass.** `DomainEvent` is a pydantic `BaseModel`, so your event is
one too: annotate the payload fields you care about (`owner_name: str`,
`amount: float`) and pydantic validates them on construction. The base class already
supplies the plumbing every event needs — `event_id`, `occurred_at` (a UTC timestamp),
`aggregate_id`, `aggregate_version`, `event_version`, plus optional `tenant_id`,
`actor_id`, `correlation_id`, `causation_id`, and a `metadata` dict. You never declare
those; you just get them.

**`event_type`.** The string written into storage to identify this event on the way
back out. Setting it explicitly, as the example does, is the clearest option. You can
also omit it: `DomainEvent` auto-derives `event_type` from the class name, so
`class AccountOpened(DomainEvent)` gets `"AccountOpened"` for free. If you set it to
something that is *not* the class name — a versioned wire name like
`"account_opened_v2"`, say — the library logs a warning, which you silence with
`suppress_event_type_warning = True` on the class.

**`aggregate_type`.** Which kind of thing this event happened to. All three events use
`"BankAccount"`, which is what groups them into one stream. The aggregate you write in
Step 3 declares the same string, and so does the `StreamId(account_id, "BankAccount")`
you read back in Step 6 — they have to match.

**`@register_event`.** Adds the class to the library's event registry, keyed by
`event_type`. When the store loads a stream it finds rows containing a type name and a
JSON payload; the registry is how it turns `"MoneyDeposited"` back into your
`MoneyDeposited` class. Forget the decorator and events save fine but come back
unrecognizable — see [Troubleshooting](#troubleshooting). The decorator works bare
(`@register_event`) or called (`@register_event(event_type="account.opened")`) when
you want the registry key to differ from the class.

### Two Conventions Worth Internalizing

**Name events in the past tense.** `AccountOpened`, not `OpenAccount`. A command is a
request that may be refused; an event is a fact that already happened and cannot be
argued with. The aggregate methods in Step 3 are the commands (`open`, `deposit`,
`withdraw`) and they are the ones allowed to say no. By the time an event exists, the
decision is made.

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

## Step 2: Model Aggregate State

Events are the truth; state is a convenience. `BankAccountState` is the answer to
"what does this account look like *right now*?" — a value you can read a balance off
without walking the whole history yourself.

It is a plain pydantic `BaseModel`. Not a library base class, not a special
"state" type — just a model you define. The aggregate is generic over it
(`AggregateRoot[BankAccountState]`), and the library only asks two things of it:
pydantic validates it on construction, and `model_dump(mode="json")` /
`model_validate(...)` can round-trip it (that pair is what snapshotting uses later).

### Snippet: state model

Add this to `bank.py`, below the events:

```python
--8<-- "examples/basic_usage.py:state"
```

Four fields and a docstring — that is the entire class. It subclasses pydantic's
`BaseModel` directly, with no import from `eventsource` involved, which is the clearest
signal available that state is *yours*: the library never inspects these fields, it
only hands the object back to you through the aggregate's `state` property.

### Reading the fields

- **`account_id: UUID`** — required, no default. Every state instance is tied to a
  specific account, and Step 3 fills it from `self.aggregate_id`.
- **`owner_name: str = ""`**, **`balance: float = 0.0`**, **`is_open: bool = False`** —
  defaulted, and the defaults describe an account that does not exist yet: nameless,
  empty, closed. That is what `_get_initial_state()` returns before any event has been
  applied, and it is what lets `deposit()` and `withdraw()` refuse to touch an account
  that was never opened by testing `is_open`.

Note what is *absent*: no methods, no `deposit`, no validation of business rules. State
holds data. Decisions live on the aggregate in Step 3.

### Derived, not authoritative

Every field here is recoverable from the event stream. `balance` is
`initial_balance` plus the deposits minus the withdrawals; `owner_name` comes from
`AccountOpened`. If you deleted this class and rewrote it with different fields, the
stored events would not change at all — you would simply be folding them up a
different way. That asymmetry is the whole point of event sourcing: the log is
permanent, the shape you read it into is not.

This is also why state is safe to make mutable-ish. The events are frozen; state is
replaced wholesale on each `_apply` step, using `model_copy(update={...})` to produce a
new instance rather than mutating in place. You will see that in the next step.

### `state` starts as `None`

One detail that will matter when you write the aggregate: `AggregateRoot.state` is
typed `TState | None`, and it returns `None` for an aggregate that has had no events
applied. So `account.state.balance` is only safe *after* something has happened. The
example's `_apply` method guards with `if self._state:` before folding a deposit or
withdrawal, and `deposit()`/`withdraw()` check `not self.state or not
self.state.is_open` first — which means a
`MoneyDeposited` can never reach a `None` state in practice, because `AccountOpened`
must have come first.

With events defined and state modelled, you have the two halves of the fold. Step 3
writes the function that joins them.

## Step 3: Build the Aggregate Root

The aggregate is where the two halves meet. It owns the fold from events to state, and
it owns the decision about whether a new event is allowed to exist at all. Everything
you have written so far was data; this is the first piece with behaviour.

Add this class to `bank.py`, below `BankAccountState`:

```python
class BankAccountAggregate(AggregateRoot[BankAccountState]):
    """Event-sourced bank account aggregate."""

    aggregate_type = "BankAccount"

    def _get_initial_state(self) -> BankAccountState:
        """Return initial state for new accounts."""
        return BankAccountState(account_id=self.aggregate_id)
```

Three things are already decided by those five lines.

`AggregateRoot[BankAccountState]` binds the generic parameter, so `self.state` is typed
`BankAccountState | None` and your editor knows `self.state.balance` is a float.

`aggregate_type = "BankAccount"` is the class attribute that must match the
`aggregate_type` on your events and the string you pass to the repository in Step 4.
The base class requires it — a subclass that doesn't set it raises
`AggregateTypeNotSetError` at construction, rather than quietly ending up with
events nobody can find.

`_get_initial_state()` is one of the two abstract methods `AggregateRoot` requires. It
returns the empty account — nameless, zero balance, `is_open=False` — built from
`self.aggregate_id`, which the base class sets in `__init__`.

### Applying Events

The other abstract method is `_apply()`. It answers one question: given this event,
what is the new state? Add it to the class:

```python
    def _apply(self, event: DomainEvent) -> None:
        """Apply an event to update the state."""
        if isinstance(event, AccountOpened):
            self._state = BankAccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                is_open=True,
            )
        elif isinstance(event, MoneyDeposited):
            if self._state:
                self._state = self._state.model_copy(
                    update={"balance": self._state.balance + event.amount}
                )
        elif isinstance(event, MoneyWithdrawn) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance - event.amount}
            )
```

Read it as a dispatch table. `AccountOpened` constructs state from nothing — it is the
only event that can, because it is the only one that runs when `self._state` is still
`None`. The two money events fold into whatever state already exists, producing a new
instance with `model_copy(update={...})` rather than assigning to a field. The
`if self._state:` guards are what keep the method total: a deposit arriving before an
opening is ignored rather than raising `AttributeError`.

Two rules about `_apply()` that are easy to get wrong and painful to debug:

**No validation here.** `_apply()` must never raise on a business rule. It runs during
replay as well as on new commands, and history is not negotiable — an event that was
accepted in 2024 must still be applicable in 2026, even if the rules have since
tightened. Validation belongs in the command methods below.

**No side effects here.** No emails, no HTTP calls, no writes. Every time an aggregate
is loaded from the store, every event in its stream is replayed through this method.
Anything with an effect would fire again on every load.

Note that `_apply()` assigns to `self._state`, the private attribute, not the read-only
`state` property. You never call `_apply()` yourself either; `apply_event()` does, after
it has checked the version and before it records the event as uncommitted.

### Command Methods and Version Guards

Commands are the public surface: `open()`, `deposit()`, `withdraw()`. Each one checks
the rules, then — and only then — creates an event and hands it to `apply_event()`.

```python
    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        """Open a new bank account."""
        if self.version > 0:
            raise ValueError("Account already opened")
        if initial_balance < 0:
            raise ValueError("Initial balance cannot be negative")

        event = AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def deposit(self, amount: float) -> None:
        """Deposit money into the account."""
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")

        event = MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def withdraw(self, amount: float) -> None:
        """Withdraw money from the account."""
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Withdrawal amount must be positive")
        if amount > self.state.balance:
            raise ValueError(f"Insufficient balance: {self.state.balance}")

        event = MoneyWithdrawn(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

All three follow the same shape — **guard, construct, apply** — and it is worth naming
each part.

**The guards.** `open()` uses `self.version > 0` to mean "this account already has
history", which is the cheapest existence check available: version counts applied
events, so a brand-new aggregate is at 0. `deposit()` and `withdraw()` instead check
`not self.state or not self.state.is_open`, covering both the never-opened case
(`state` is `None`) and a hypothetical closed account in one condition. Then come the
domain rules proper: non-negative opening balance, positive amounts, and enough money
to cover a withdrawal. The failure you will exercise in Step 8 — withdrawing more than
the balance — is that last line, and it fires *before* any event is created, so the
history never records the attempt.

**The construction.** Each event gets `aggregate_id=self.aggregate_id` and
`aggregate_version=self.get_next_version()`, which is simply `self.version + 1`. That
number is not decoration. `apply_event()` compares it against the version it expects
and raises `EventVersionError` when they disagree, so an off-by-one in a command method
fails loudly at the point of the mistake rather than corrupting a stream. The
`aggregate_type` comes along from the event class's own default.

If the `aggregate_id`/`aggregate_version` boilerplate starts to grate once you have a
dozen commands, `AggregateRoot` offers `create_event()`, which fills in
`aggregate_id`, `aggregate_type`, `aggregate_version`, and the tenant id from context,
then applies the event for you:

```python
    def deposit(self, amount: float) -> None:
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.create_event(MoneyDeposited, amount=amount)
```

This tutorial sticks with the explicit form so nothing is hidden.

**The apply.** `apply_event(event)` validates the version, sets `self._version` to the
event's `aggregate_version`, calls `_apply()` to fold in the new state, and appends the
event to the aggregate's uncommitted list. That list is what the repository drains in
Step 4. Until you save, nothing has left memory — the command has happened as far as
this object is concerned, and nowhere else.

Replay takes the same path with the flag flipped: `apply_event(event, is_new=False)`
skips version validation and skips the uncommitted list, so loading an aggregate
rebuilds state without ever looking like it has pending work. You will not call that
yourself — the repository does it for you in the next step.

You now have a complete aggregate: state, a fold, and three guarded commands. Nothing
is persisted yet, which is exactly what Step 4 fixes.
