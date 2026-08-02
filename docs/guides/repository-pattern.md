# Wire an AggregateRepository

`AggregateRepository` sits between your domain aggregates and the event store: it
loads an aggregate by replaying its event history, and saves it by appending the
events it produced. To do that it needs to know which aggregate type its streams
belong to — a string such as `"Order"` that is stamped onto every event and
snapshot it writes.

This guide shows how to wire a repository up so that string lives in exactly one
place. Declare `aggregate_type` on the aggregate class, and the repository always
infers it from `aggregate_factory.aggregate_type` — there is no separate
constructor parameter, so it cannot diverge from what the aggregate itself
declares. You will also find here how the inference rule behaves with
inheritance, how to read the resolved value back off the repository, and how to
fix `ValueError: Cannot infer aggregate_type from <YourClass>`.

## Before you start

You need:

- A working install of `eventsource-py` and familiarity with the basics in
  [Getting Started](../getting-started.md): defining a `DomainEvent`, subclassing
  `AggregateRoot` (or `DeclarativeAggregate`), and persisting through an
  `EventStore`.
- An aggregate class you can edit. The inference rule reads a class attribute off
  the class you pass as `aggregate_factory`, so you need to be able to add one
  line to that class. There is no way to supply the type from outside the
  class — if you cannot edit it, subclass it and set the attribute there.
- An `EventStore` instance. Any implementation works; the examples here use
  `InMemoryEventStore` so they run without external services. Swap in
  `SQLiteEventStore` or `PostgreSQLEventStore` unchanged.

Everything on `AggregateRepository` is async, so the calls in this guide belong
inside an `async def` (or an `asyncio.run(...)`). If you are calling from
synchronous code, wire the repository as shown here and apply
`SyncEventStoreAdapter` at the boundary — see
[Synchronous usage](sync-usage.md).

The imports used throughout:

```python
from eventsource import (
    AggregateRepository,
    DeclarativeAggregate,
    InMemoryEventStore,
)
```

One caveat worth knowing before you start editing classes: `AggregateRoot`
declares `aggregate_type: ClassVar[str]` with **no default**. A subclass that
never assigns it fails at aggregate construction with `AggregateTypeNotSetError`,
before a repository ever enters the picture; a subclass that assigns an empty
string gets past that check but the repository still treats it as "not
configured" and raises. Either way, this is the most common reason a repository
that looks correctly wired fails at construction time.

## Step 1: Declare `aggregate_type` on your aggregate class

Add a single class attribute to the aggregate you intend to pass as
`aggregate_factory`:

```python
class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            status="created",
        )
```

That is the whole step. `aggregate_type` is declared on `AggregateRoot` as
`aggregate_type: ClassVar[str]` with no default, so you are satisfying a
required attribute rather than overriding one — no decorator, no registration
call, no metaclass hook is involved.

Pick the value deliberately. It is not a display name: the aggregate stamps it
onto every event it creates (`AggregateRoot.create_event` passes
`aggregate_type=self.aggregate_type` into the event's metadata), and the
repository uses it as the aggregate-type key for snapshots. Once events are in
the store under `"Order"`, changing the string to `"OrderAggregate"` orphans that
history. Choose the domain name you want to live with — typically the ubiquitous
-language noun (`"Order"`, `"Customer"`, `"Extraction"`), not the Python class
name.

Two things to check while you are in the class:

- **Do not skip assigning it.** A class that never assigns `aggregate_type`
  raises `AggregateTypeNotSetError` the moment you construct it — well before
  a repository enters the picture.
- **A bare assignment is enough.** `aggregate_type = "Order"` on the subclass
  satisfies the `ClassVar[str]` declaration on `AggregateRoot`; you don't need
  to repeat the `ClassVar` annotation yourself.

You can confirm the attribute is visible to the inference rule without
constructing anything:

```python
>>> OrderAggregate.aggregate_type
'Order'
```

If that prints `'Order'` — read off the *class*, not an instance — Step 2 will
work.

## Step 2: Construct the repository

With the attribute in place, build the repository from just the store and the
factory:

```python
store = InMemoryEventStore()

repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
)
```

There is no `aggregate_type=` parameter to pass. The constructor always calls
`self._infer_aggregate_type(aggregate_factory)`, which reads
`OrderAggregate.aggregate_type` and resolves to `"Order"`.

`aggregate_factory` is the class itself, not an instance and not a lambda — the
repository calls it as `self._aggregate_factory(aggregate_id)` when
reconstituting, and inference reads the attribute straight off that class object.

Confirm the resolved value before going further:

```python
>>> repo.aggregate_type
'Order'
```

`AggregateRepository.aggregate_type` is a read-only property returning whatever
was inferred at construction. If this matches the string your existing events
were written under, the repository will find that history.

Inference happens once, in `__init__`, and its result is what everything
downstream uses: the type recorded on spans (`ATTR_AGGREGATE_TYPE`), and the
`aggregate_type` passed to `take_snapshot()` / `read_valid_snapshot()` when a
`snapshot_store` is configured. Because it is resolved eagerly, a misconfigured
aggregate class fails loudly at wiring time rather than on the first `load()`.
If construction raises `ValueError: Cannot infer aggregate_type from ...`, jump
to [Troubleshooting](#troubleshooting-valueerror-cannot-infer-aggregate_type-from-yourclass).

The same call shape works with every other option — the ones covered later in
this guide layer on top without reintroducing the type string:

```python
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
    event_publisher=bus,
    snapshot_store=InMemorySnapshotStore(),
    snapshot_threshold=100,
)
```
