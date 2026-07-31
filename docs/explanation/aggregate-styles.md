# Aggregate Styles: AggregateRoot vs DeclarativeAggregate

`eventsource` gives you two ways to write an aggregate, and the examples in this
repository use one of each. `BankAccountAggregate` in `examples/imperative_example.py`
subclasses `AggregateRoot[BankAccountState]` and hand-writes an `_apply` method built
from `isinstance` branches. `ShoppingCartAggregate` in `examples/aggregate_example.py`
subclasses `DeclarativeAggregate[ShoppingCartState]` and decorates one method per event
type with `@handles(...)`.

This is not two competing frameworks bolted together. `DeclarativeAggregate` is a
subclass of `AggregateRoot` (`src/eventsource/domain/aggregate.py`) whose only real job
is to supply an `_apply` implementation for you: it looks the event's Python type up in
a per-subclass `_event_handlers` registry and calls the method registered for it. Every
other moving part -- `apply_event()` and its version check, `load_from_history()`,
`uncommitted_events`, `create_event()`, snapshot serialization, `schema_version` --
lives on `AggregateRoot` and behaves identically under both styles. Choosing a style
changes how events reach your state-mutation code and nothing else about how the
aggregate participates in the system.

Because the difference is confined to dispatch, the choice is mostly a readability and
strictness question rather than an architectural one. A three-event aggregate reads fine
as an `if/elif` chain; a six-event aggregate like the shopping cart does not. Against
that, `DeclarativeAggregate` adds two behaviours the base class has no equivalent for:
`requires_creation_event`, which lets an aggregate have no state at all until its first
event lands, and `unregistered_event_handling`, which decides whether an event with no
handler is silently ignored, logged, or rejected with `UnhandledEventError`. Those
capabilities, not the syntax, are usually the deciding factor.

The rest of this document walks through what the two styles share, what each one looks
like in the example code, how `@handles` dispatch is actually wired up, what only the
declarative style can do, and how to move an existing `_apply` aggregate over.

There is also a third way to organize an aggregate that builds on `AggregateRoot`
rather than being shipped as a class: pure `decide`/`evolve` functions behind a thin
adapter. See [The Decider Pattern on Top of AggregateRoot](decider-pattern.md) for the
pattern, its one integration gotcha, and benchmarks against the imperative style.

## Why two styles exist

`AggregateRoot` defines the smallest contract that event sourcing actually requires. It
is an `ABC` with exactly two abstract methods -- `_apply(event)` and
`_get_initial_state()` -- and everything else it offers is concrete machinery built on
top of them. That contract is deliberately unopinionated about *how* you decide what an
event should do to your state. A dictionary lookup, a `match` statement, a chain of
`isinstance` checks, or a single line that ignores the event entirely are all valid
implementations of `_apply`. The base class does not need to know.

That minimalism is a feature when the aggregate is small. `BankAccountAggregate` handles
three event types, and an `_apply` with three branches is shorter, has no indirection to
trace, and can be read top to bottom in the order the branches are written. There is no
registry to inspect, no decorator to look up, and no question about what happens to an
event you did not anticipate: control simply falls off the end of the `if/elif` chain.

The cost of that minimalism grows with the event count, and it grows in a way that is
purely mechanical. Every new event type means another `elif isinstance(event, X):`
guard, another indentation level of state-mutation code, and one more place where a
missing branch fails silently. By the time an aggregate reaches the six event types of
`ShoppingCartAggregate`, the `isinstance` scaffolding is a substantial fraction of the
method and the actual domain logic is buried inside it. `DeclarativeAggregate` exists to
delete that scaffolding: `@handles(EventType)` marks a method, `__init_subclass__`
collects the marked methods into a per-subclass `_event_handlers` dict at class-creation
time, and the inherited `_apply` becomes a single dict lookup by `type(event)`. One
method per event, named for the event, at a consistent indentation level.

The second reason is that dispatch-by-registry makes behaviours possible that a
hand-written `_apply` cannot express generically. Because `DeclarativeAggregate` knows
which event types it can handle, it can also decide what to do when it meets one it
cannot -- hence `unregistered_event_handling` with its `"ignore"` / `"warn"` / `"error"`
modes, and the `UnhandledEventError` that carries the list of handlers the aggregate
does know about. Because it owns `_get_initial_state()` too, it can offer
`requires_creation_event = True`, returning `None` instead of forcing every aggregate to
invent a plausible empty state, and backing that with `state` raising
`AggregateNotCreatedError`, plus `state_or_none` and `is_created` for callers that want
to check first. None of that is reachable from `AggregateRoot` alone, because
`AggregateRoot` has no idea which events your `_apply` covers.

Both styles ship because neither subsumes the other in practice. `AggregateRoot` remains
the contract that everything else in the library -- `AggregateRepository`, snapshotting,
the testing harness -- is written against, so it cannot be retired, and it stays the
better fit for genuinely small aggregates. `DeclarativeAggregate` is the ergonomic layer
for everything past that point. The examples model one of each so both patterns have a
worked reference rather than a paragraph of prose.

## What both styles share

Everything except `_apply`. That is worth stating plainly, because the two examples look
different enough on the page that it is easy to assume they are wired into the library
differently too. They are not: `DeclarativeAggregate` inherits the whole of
`AggregateRoot` and overrides exactly two things -- `_apply`, which becomes a registry
lookup, and `_get_initial_state`, which becomes conditional on
`requires_creation_event`. Every other member listed below is the same code running for
both styles.

**Identity and lifecycle.** Both take a single `aggregate_id: UUID` in `__init__`, and
both start at `_version = 0` with `_state = None` and an empty uncommitted-events list.
Both expose `aggregate_id`, `version`, and `uncommitted_events` (which returns a *copy*,
so mutating the returned list does nothing), plus `has_uncommitted_events`. Equality and
hashing are by `aggregate_id` alone for both -- two instances of the same aggregate at
different versions compare equal.

**Event application and version checking.** `apply_event(event, is_new=True)` is defined
once, on `AggregateRoot`. When `is_new` is true it checks `event.aggregate_version`
against `self._version + 1` and raises `EventVersionError` on a mismatch, unless the
class sets `validate_versions = False`, in which case the mismatch is logged as a
warning and allowed through. It then sets `self._version = event.aggregate_version`,
calls `self._apply(event)`, and appends to `_uncommitted_events`. The declarative style
does not intercept any of that -- it only supplies the `_apply` that gets called in
step three. `validate_versions` is a class attribute on `AggregateRoot`, so it works
identically on both.

**Replay.** `load_from_history(events)` iterates the list and calls
`apply_event(event, is_new=False)` for each, which skips version validation and skips
uncommitted tracking. This is the same loop for a hand-written `if/elif` chain and a
`@handles` registry; the only difference is which of your methods each iteration ends up
in. `AggregateRepository` calls exactly this method regardless of style
(`aggregates/repository.py`).

**Raising events from commands.** `get_next_version()`, `_raise_event(event)`, and
`create_event(EventClass, **kwargs)` are all on the base class. `create_event` is the
one worth knowing: it auto-populates `aggregate_id`, `aggregate_type`, and
`aggregate_version` (from `get_next_version()`), pulls `tenant_id` from the multitenancy
context if that module is installed and you did not pass one explicitly, lets your
explicit kwargs override any of those, then constructs the event and applies it as new.
A `DeclarativeAggregate` command method and an `AggregateRoot` command method are
written the same way.

**Commit bookkeeping.** `mark_events_as_committed()` clears the uncommitted list;
`clear_uncommitted_events()` clears it and returns what was there. Repositories call the
former after a successful append.

**Snapshotting.** `_serialize_state()` (`model_dump(mode="json")`, or `{}` when state is
`None`), `_restore_from_snapshot(state_dict, version)`, and `_get_state_type()` are
inherited unchanged. `_get_state_type()` walks the MRO looking for a `__orig_bases__`
entry whose origin is a subclass of `AggregateRoot`, and its `issubclass(origin,
AggregateRoot)` check is written to accept `DeclarativeAggregate[...]` as well as
`AggregateRoot[...]` -- which is why snapshot round-tripping needs no special case for
the declarative style. The `schema_version` class attribute (default `1`) that
invalidates stale snapshots is likewise shared.

**The state type parameter.** Both are `Generic[TState]` and both expect `TState` to be
a Pydantic model, because serialization and snapshot restore go through
`model_dump`/`model_validate`. Both conventionally treat state as immutable and rebuild
it with `model_copy(update={...})` inside handlers rather than mutating in place.

**`aggregate_type`.** A class attribute defaulting to `"Unknown"` on `AggregateRoot`.
Both styles should override it; both use it as the stream's type discriminator and as
the value `create_event` stamps onto every event.

The practical consequence is that anything written against `AggregateRoot` -- the
repository, the snapshot manager, the testing harness in `eventsource.testing` -- accepts
either style without knowing or caring which it got. Switching an aggregate from one
style to the other is a change to that class's file and nothing else.

## Style 1: AggregateRoot with a hand-written `_apply`

Subclassing `AggregateRoot[TState]` directly means implementing the two abstract methods
yourself and accepting that dispatch is entirely your problem. There is no registry, no
decorator, and no machinery between an event arriving and your code deciding what it
means. `apply_event()` hands the event to `_apply(event)` and whatever that method does
is the whole of the aggregate's state-mutation behaviour.

### What the code looks like (BankAccount, examples/imperative_example.py)

`BankAccountAggregate` is the minimal shape of the style. It declares
`aggregate_type = "BankAccount"`, implements `_get_initial_state()` to return a
`BankAccountState` seeded with the aggregate id, and implements `_apply` as a single
chain over its three event types:

```python
class BankAccountAggregate(AggregateRoot[BankAccountState]):
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

Three details in that block are worth naming, because they are properties of the style
rather than of this particular account.

The first branch *constructs* state; the others *evolve* it. `AccountOpened` assigns a
whole new `BankAccountState` because it is the creation event and there is nothing
sensible to copy from. `MoneyDeposited` and `MoneyWithdrawn` use
`model_copy(update={...})`, which is the convention throughout the library: state models
are treated as immutable and replaced rather than mutated, so a handler never leaves a
half-updated object behind if it raises partway through.

Every non-creation branch guards on `self._state`. `_state` starts as `None` in
`AggregateRoot.__init__` and stays `None` until some branch assigns it, so a deposit
arriving before an open would otherwise be an `AttributeError` on `None.balance`. The
example handles this two different ways in two adjacent branches -- a nested `if
self._state:` for deposits, and an `and self._state` folded into the `elif` for
withdrawals -- which is a small illustration of how easily a hand-written chain drifts
in style as it grows.

`_get_initial_state()` is implemented but never called here. Nothing in the library
calls it: `AggregateRoot` declares it abstract, and it is your handler code that decides
whether to invoke it. The bank account's `AccountOpened` branch builds its state inline
instead, so the method exists purely to satisfy the abstract contract. Other aggregates
do use it -- the counter fixtures in `tests/fixtures/aggregates.py` call
`self._state = self._get_initial_state()` at the top of each branch as a null guard --
but the base class does not care either way.

The command methods that sit alongside `_apply` are ordinary methods. `open()`,
`deposit()`, and `withdraw()` each check business rules against `self.state`, then
construct the event explicitly with `aggregate_version=self.get_next_version()` and pass
it to `self.apply_event(event)`:

```python
def deposit(self, amount: float) -> None:
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
```

This is longhand for what `create_event(MoneyDeposited, amount=amount)` would do -- the
example spells it out to keep the version bookkeeping visible. Note also that `open()`
enforces "can only open once" with `if self.version > 0`, using the version counter as
the creation flag rather than checking state; that works because `apply_event` sets
`_version` from the event before `_apply` runs.

### What you are responsible for

The base class guarantees ordering, versioning, and replay. Everything about *matching*
is yours, and the failure modes cluster in a few predictable places.

**Covering every event type in the stream.** An event with no matching branch falls off
the end of the `if/elif` chain and is silently discarded -- but `apply_event` has
already advanced `_version` before calling `_apply`, and goes on to append the event to
`_uncommitted_events` afterwards for new events. So the event is persisted and counted,
and only the state reflects nothing. That is the sharpest edge of the style: adding a new event type and
forgetting the branch produces a version number that keeps climbing over state that
quietly stops changing, with no exception anywhere. `AggregateRoot` has no equivalent of
`unregistered_event_handling` because it has no way to know which types you meant to
cover.

**Null-guarding state.** Because `_state` is `None` until a branch sets it, every branch
that reads existing state has to prove it exists first. Replay makes this concrete: if
the first event in a stream is not the creation event -- a partial stream, a
`get_events` call with a `from_version`, or an aggregate whose creation event was
renamed -- the guards are what stand between you and an `AttributeError` mid-replay. The
`state` property is typed `TState | None`, so a type checker will flag unguarded access,
but only if you are running one.

**Branch order and fall-through.** `if/elif` stops at the first match, and `isinstance`
matches subclasses. If one of your event types subclasses another, the parent's branch
placed first will swallow the child. This is a real hazard with `TenantDomainEvent`-style
hierarchies or with events refactored to share a base. Ordering is invisible to the
library; nothing warns you.

**Keeping mutations immutable and total.** Nothing enforces `model_copy` over in-place
assignment, and nothing enforces that a branch actually assigns `self._state`. A branch
that computes a new value and forgets to store it type-checks fine and does nothing.

**Testing that the chain is complete.** Since a missing branch is silent, the coverage
question has to be answered by tests rather than by the runtime. The practical
discipline is a replay test per event type: construct the event, apply it, assert the
state moved. `eventsource.testing` provides the harness and assertions for this, and it
works identically for both styles.

None of these are exotic, and at three event types they are all easy to hold in your
head at once. The argument for the declarative style is not that these problems are
unsolvable -- it is that each one scales with the number of branches, while the
registry-based alternative pays for them once.

## Style 2: DeclarativeAggregate with `@handles`

Subclassing `DeclarativeAggregate[TState]` means you stop writing `_apply` and start
writing one method per event type, each tagged with `@handles(EventType)`. The base
class supplies `_apply` as an exact-type lookup in a per-subclass registry, so the
question "what does this event do to my state?" is answered by finding the method named
for that event rather than by reading a chain top to bottom. Nothing else about the
aggregate changes: commands, versioning, replay, and snapshots work exactly as they do
under `AggregateRoot`.

### What the code looks like (ShoppingCart, examples/aggregate_example.py)

`ShoppingCartAggregate` handles six event types -- `CartCreated`, `ItemAddedToCart`,
`ItemRemovedFromCart`, `ItemQuantityChanged`, `CartCheckedOut`, `CartAbandoned` -- which
is roughly where the `if/elif` shape stops paying for itself. The class declares
`aggregate_type = "ShoppingCart"`, implements `_get_initial_state()` the same way the
bank account does, and then simply lists its handlers:

```python
class ShoppingCartAggregate(DeclarativeAggregate[ShoppingCartState]):
    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    @handles(CartCreated)
    def _on_cart_created(self, event: CartCreated) -> None:
        self._state = ShoppingCartState(
            cart_id=self.aggregate_id,
            customer_id=event.customer_id,
            status="empty",
        )

    @handles(CartAbandoned)
    def _on_cart_abandoned(self, event: CartAbandoned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "abandoned"})
```

The handler signature for aggregates is `(self, event: EventType) -> None`, synchronous,
returning nothing -- the same contract `_apply` had, narrowed to one event type. Note
that this is *not* the projection signature; `DeclarativeProjection` uses the same
`@handles` decorator but expects `async def handler(self, conn, event)`. The decorator
is shared, the calling convention is not.

The structural differences from the bank account are worth reading off the page
directly. Each handler is typed to its concrete event, so `event.customer_id` and
`event.product_id` are visible to a type checker without an `isinstance` narrowing step.
Every handler sits at the same indentation level, so a handler that grew to twenty lines
-- `_on_item_added`, which has to merge quantities when the product is already in the
cart -- does not push its neighbours around. And the method name states the intent, so
the six-way mapping is legible from a class outline rather than requiring the body to be
read.

What does *not* change is just as informative. The `if self._state:` guard that every
non-creation branch of the bank account carried is present in every non-creation handler
here too, for exactly the same reason: `_state` is `None` until `_on_cart_created`
assigns it, and this cart does not opt into `requires_creation_event`, so the declarative
style gives no protection against a partial stream. The `model_copy(update={...})`
convention is unchanged. And the command methods -- `create`, `add_item`, `remove_item`,
`change_quantity`, `checkout`, `abandon` -- are ordinary methods that validate against
`self.state`, build the event with `aggregate_version=self.get_next_version()`, and call
`self.apply_event(event)`, byte for byte the same shape as the bank account's `deposit`.
`checkout()` even reuses the same "guard on version/status, raise `ValueError`" idiom:

```python
def checkout(self) -> UUID:
    if not self.state:
        raise ValueError("Cart does not exist")
    if self.state.status != "active":
        raise ValueError(f"Cannot checkout {self.state.status} cart")
    if not self.state.items:
        raise ValueError("Cannot checkout empty cart")

    order_id = uuid4()
    event = CartCheckedOut(
        aggregate_id=self.aggregate_id,
        order_id=order_id,
        total_amount=self.state.total_amount,
        aggregate_version=self.get_next_version(),
    )
    self.apply_event(event)
    return order_id
```

The one place the cart's handlers do something the account's branches do not is
collection state. `items` is a `dict[str, CartItem]` keyed by `str(product_id)`, and the
handlers copy it with `dict(self._state.items)` before mutating the copy and passing it
to `model_copy`. That is the immutability convention applied one level down: replacing
the dict rather than mutating the one the current state object holds. It is a property
of modelling a collection, not of the declarative style -- but it is the reason the
cart's handlers are long enough that separating them into named methods is worth it.

### How dispatch is wired: `__init_subclass__` and the handler registry

`@handles` itself does almost nothing. It is a decorator factory that attaches a single
attribute to the function and returns it unmodified:

```python
def handles(event_type: type[DomainEvent]) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        func._handles_event_type = event_type
        return func
    return decorator
```

No wrapper, no registration at import time, no change to how the method behaves when
called directly. The marked function is still a plain method; it just carries a
`_handles_event_type` attribute that something else will look for later. That "something
else" is `DeclarativeAggregate.__init_subclass__`, which Python runs once per subclass at
class-creation time:

```python
def __init_subclass__(cls, **kwargs: object) -> None:
    super().__init_subclass__(**kwargs)
    cls._event_handlers = {}
    for name in dir(cls):
        try:
            method = getattr(cls, name)
            if hasattr(method, "_handles_event_type"):
                cls._event_handlers[method._handles_event_type] = name
        except AttributeError:
            continue
```

Four properties of that loop matter in practice.

**Each subclass gets its own dict, but inheritance still works.** `cls._event_handlers =
{}` deliberately shadows the empty default declared on `AggregateRoot`, so subclasses
never share or accidentally pollute a parent's registry. Inherited handlers are still
picked up, because `dir(cls)` walks the full MRO -- a subclass of `ShoppingCartAggregate`
that adds one handler ends up with all seven, not one. A subclass that redefines a
handler method under the same name simply wins, because `getattr(cls, name)` resolves to
the override.

**The registry maps event type to method *name*, not to the function.** Lookup at apply
time does `getattr(self, handler_name)`, which resolves through the instance and yields a
bound method. This is why redefinition and normal method resolution behave the way you
would expect, and why nothing keeps a stale reference to a function object.

**Registration is keyed by the decorator argument, not the annotation.** The `event:
CartCreated` type hint on the handler is documentation for you and your type checker;
dispatch uses only the class passed to `@handles`. If the two disagree, the decorator
wins silently -- and unlike `DeclarativeProjection`, the aggregate's `__init_subclass__`
performs no signature validation, so an aggregate handler with the wrong arity fails at
call time rather than at class definition.

**Two handlers for the same event type collide.** The dict is keyed by event type, so if
two methods carry `@handles(CartCheckedOut)`, whichever `dir(cls)` reaches last -- `dir`
returns names sorted alphabetically -- overwrites the other, with no warning. One event,
one handler.

Dispatch itself is then three lines:

```python
def _apply(self, event: DomainEvent) -> None:
    handler_name = self._event_handlers.get(type(event))
    if handler_name:
        getattr(self, handler_name)(event)
    else:
        self._handle_unregistered_event(event)
```

The critical word is `type(event)`: this is an **exact type match**, not `isinstance`.
An event class that subclasses a registered event type will *not* be routed to the
parent's handler -- it is treated as unregistered. That is precisely the opposite of the
`isinstance` chain's fall-through behaviour, where a parent branch placed first swallows
its children. Neither is wrong, but they are not interchangeable, and migrating an
aggregate that relied on subclass matching across the parent's branch will silently
change behaviour. If you have an event hierarchy, register each concrete leaf type.

Everything the registry knows also feeds the failure path. Because `_event_handlers` is
an explicit map of what the class can handle, `_handle_unregistered_event` can do
something more useful than fall off the end of a chain: consult
`unregistered_event_handling` and either stay silent, log a warning naming the event type
and the class alongside the list of handlers it does know about, or raise
`UnhandledEventError` carrying `event_type`, `event_id`, `handler_class`, and
`available_handlers`. That list is computed as `[et.__name__ for et in
self._event_handlers]` -- the registry, read back out. The next section covers when to
turn each mode on.
