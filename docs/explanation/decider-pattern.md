# The Decider Pattern on Top of AggregateRoot

[Aggregate Styles](aggregate-styles.md) covers the two dispatch styles the library
ships: a hand-written `_apply` on `AggregateRoot` and `@handles` methods on
`DeclarativeAggregate`. Both are object-oriented — commands are methods that mutate the
aggregate, and business rules live inside those methods next to the infrastructure they
touch. There is a third way to organize the same aggregate that the library does not
ship as a class but fully supports: the **decider pattern**, where the entire domain is
three pure functions and the aggregate class shrinks to a thin adapter.

This document shows the pattern working against the real `AggregateRepository`
machinery, names the one integration gotcha, and quantifies what it costs. The verdict
up front: the decider is behaviorally identical to the imperative style, costs nothing
measurable on the replay path that dominates aggregate loading, costs about 50% extra
on the (already microsecond-scale) command path, and buys you domain logic that can be
unit-tested with plain asserts and no async machinery.

## The pattern

A decider is three pure functions over plain values:

```python
def initial_state(order_id: UUID) -> OrderState: ...
def decide(command: OrderCommand, state: OrderState) -> list[OrderEvent]: ...
def evolve(state: OrderState, event: OrderEvent) -> OrderState: ...
```

`decide` answers "given what has happened, is this request allowed, and what new facts
does it produce?" — it either returns events or raises a rejection. `evolve` answers
"given a fact, what is the next state?" — it is the fold that replay, projections, and
`decide`'s own view of current state are all built from. Neither function touches
`self`, versions, the event store, or anything async. That purity is the entire point:
the business rules become data-in/data-out and testable in isolation.

Commands become values too — small frozen models rather than method calls:

```python
class PlaceOrder(BaseModel):
    customer_id: UUID
    total: float

class ShipOrder(BaseModel):
    tracking_number: str

OrderCommand = PlaceOrder | ShipOrder
```

With commands and state both being pydantic models, `match` can dispatch on the
*(command, state)* pair structurally, and `decide` reads like a state-transition table
— each `case` is one legal (or explicitly illegal) transition:

```python
def decide(command: OrderCommand, state: OrderState) -> list[OrderEvent]:
    match command, state:
        case PlaceOrder(customer_id=cid, total=total), OrderState(status="draft"):
            return [OrderPlaced(aggregate_id=state.order_id, customer_id=cid, total=total)]
        case PlaceOrder(), _:
            raise ValueError("Order already placed")
        case ShipOrder(tracking_number=tn), OrderState(status="placed"):
            return [OrderShipped(aggregate_id=state.order_id, tracking_number=tn)]
        case ShipOrder(), _:
            raise ValueError("Order must be placed before shipping")
```

Pydantic v2 models support class patterns with keyword capture out of the box, so this
works today with no library changes. `evolve` is the same shape the library already
uses everywhere — a `match` over event types returning `model_copy(update={...})`:

```python
def evolve(state: OrderState, event: OrderEvent) -> OrderState:
    match event:
        case OrderPlaced(customer_id=cid, total=total):
            return state.model_copy(update={
                "customer_id": cid, "total": total, "status": "placed",
            })
        case OrderShipped():
            return state.model_copy(update={"status": "shipped"})
        case _:
            return state
```

The `case _` makes `evolve` total: an event with no branch is a no-op rather than an
error, which is the same silent-skip behavior a hand-written `_apply` has (see
[Aggregate Styles](aggregate-styles.md) for why `DeclarativeAggregate`'s
`unregistered_event_handling` exists to tighten that).

## The imperative shell

Everything in the library — `AggregateRepository`, snapshotting, the testing harness —
is written against `AggregateRoot`, so the decider plugs in through a small adapter
class. This is the classic "functional core, imperative shell" split; the shell is
about ten lines:

```python
class Order(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return initial_state(self.aggregate_id)

    @property
    def decider_state(self) -> OrderState:
        # AggregateRoot._state is None until the first event; a decider
        # needs a real initial state to match against before that.
        return self._state if self._state is not None else initial_state(self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        self._state = evolve(self.decider_state, event)

    def execute(self, command: OrderCommand) -> None:
        for event in decide(command, self.decider_state):
            self.apply_event(event.with_aggregate_version(self.get_next_version()))
```

Callers issue commands as values instead of calling named methods:

```python
order = repo.create_new(uuid4())
order.execute(PlaceOrder(customer_id=uuid4(), total=100.0))
await repo.save(order)

order.execute(ShipOrder(tracking_number="TRACK-001"))
await repo.save(order)
```

Wired into the README Quick Start (same store, bus, repository, subscription manager,
and projection), this produces byte-for-byte identical output to the imperative
version. Two seams in the shell deserve explanation, because both are consequences of
keeping the core pure.

**The `decider_state` fallback.** `AggregateRoot._state` is `None` until the first
event is applied, and nothing in the library ever calls `_get_initial_state()` for you
— as [Aggregate Styles](aggregate-styles.md) notes, it exists to satisfy the abstract
contract and it is *your* code that decides whether to invoke it. The imperative style
dodges the `None` by guarding creation on `self.version > 0` and having the creation
branch construct state from scratch. A decider cannot dodge it: `decide` needs a real
`OrderState` to match `status="draft"` against *before* the first event exists. Without
the fallback, the very first `PlaceOrder` matches `(PlaceOrder(), None)`, falls through
to the rejection case, and fails with "Order already placed". This is the one genuine
gotcha of the pattern on this library — if you adopt the shell, keep the fallback.

It also shifts where the creation invariant lives: `version > 0` is an
infrastructure fact, `status == "draft"` is a domain fact. The decider forces the
domain phrasing, which is arguably more honest but means your state model must
actually encode "not yet created" (here, the `"draft"` status).

**Version stamping via `with_aggregate_version`.** Pure functions cannot know the next
`aggregate_version` — that is optimistic-concurrency bookkeeping, not domain logic. So
`decide` returns events without a meaningful version, and the shell stamps each one
with `DomainEvent.with_aggregate_version()` (a `model_copy`) before handing it to
`apply_event`, which then validates the version as usual. The alternative — passing the
next version into `decide` — keeps one copy off the hot path but leaks infrastructure
into the domain signature; measure first (below) before deciding that trade is worth
it.

## What it costs

Measured on the Quick Start `Order` aggregate: the imperative README implementation
versus the decider shell above, pure in-memory with no store or bus involved, so the
numbers isolate the domain-dispatch code itself. CPython 3.13, median of 7 runs;
absolute numbers will vary by machine, the ratios are the point.

| Path | Imperative | Decider | Ratio |
|---|---|---|---|
| Command: new order, place + ship (2 events) | ~15.0 µs/order | ~22.6 µs/order | 1.50x |
| Replay: rebuild from 1000-event history | ~1.22 µs/event | ~1.29 µs/event | 1.06x |

The asymmetry is the finding. **Replay is a wash** because per-event cost there is
`apply_event` bookkeeping plus one `match`, which both styles share; `evolve` adds only
a property access and, for the creation event, a `model_copy` where the imperative
branch constructs fresh state. Replay is the path that scales with stream length and
dominates loading an aggregate without a snapshot, so the decider does not slow down
the part that grows.

**The command path pays 1.5x**, and micro-benchmarking each piece in isolation shows
the ~7.5 µs premium per place+ship pair decomposing into five small costs, largest
first:

- *Event re-stamping* — `with_aggregate_version()` is a full pydantic `model_copy` of
  each event (~1–2 µs apiece), paid because pure `decide` cannot know the version; the
  imperative style constructs the event once with the version inline.
- *Command objects* — each command is a pydantic model (~0.9 µs to construct) where an
  imperative method takes plain arguments.
- *`match` dispatch* — the tuple class-pattern in `decide` benches at roughly 3x an
  equivalent `isinstance` chain (≈940 ns vs ≈300 ns per command pair), and
  keyword-capture patterns in `evolve` at roughly 2x. Class patterns compile to
  isinstance checks plus `__match_args__` lookups, attribute loads, and equality tests.
- *`model_copy` vs fresh construction* — `evolve`'s `model_copy(update={...})` on the
  creation event (~2.0 µs) is slower than the imperative branch's plain
  `OrderState(...)` (~1.3 µs).
- *The initial-state fallback* — constructing a throwaway draft state (~0.9 µs) before
  the first event, where the imperative style just checks `self.version > 0`.

For scale, constructing a single `DomainEvent` subclass instance costs ~5–10 µs —
pydantic validation dominates both styles — and any real event-store append is orders
of magnitude above all of this. The premium is invisible in any system that persists
events; it would only matter in a tight in-memory simulation loop, and the first two
items (the majority of it) disappear if a future `DeciderAggregate` helper lets the
shell pass the version into event construction instead of re-stamping.

## What it buys

The purchase is testability and reviewability of the domain in isolation:

```python
def test_cannot_ship_draft_order():
    state = initial_state(order_id)
    with pytest.raises(ValueError, match="must be placed"):
        decide(ShipOrder(tracking_number="X"), state)

def test_place_then_ship():
    state = initial_state(order_id)
    for cmd in (PlaceOrder(customer_id=cid, total=100.0), ShipOrder(tracking_number="X")):
        for event in decide(cmd, state):
            state = evolve(state, event)
    assert state.status == "shipped"
```

No aggregate instance, no repository, no event loop, no fixtures. Given/when/then tests
are literally "fold these events, decide this command, assert on the result". The
`decide` function doubles as an exhaustive inventory of legal transitions — the kind of
thing the `@handles` registry gives you for *apply* dispatch, extended to *command*
dispatch.

## When to use it

Reach for the decider when the transition rules are the complicated part — many
commands, state-dependent acceptance, rules you want product owners or property-based
tests to exercise without infrastructure. Stay imperative (or declarative) when the
aggregate is mostly plumbing with trivial rules; the shell is extra ceremony there, and
`DeclarativeAggregate`'s creation-event enforcement and unregistered-event modes are
features the plain shell above does not replicate.

The two styles also compose: nothing stops `execute` from living alongside ordinary
command methods during a migration, because the shell is just an `AggregateRoot`
subclass and every downstream consumer only sees that contract.

There is currently no first-class `Decider` abstraction in the library — the shell
above is the price of admission. If the pattern sees real use, a `DeciderAggregate`
helper (constructed from `decide`/`evolve`/`initial_state`, handling the
initial-state fallback and version stamping centrally) would be the natural next step.
