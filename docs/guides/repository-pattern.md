# Wire an AggregateRepository

`AggregateRepository` sits between your domain aggregates and the event store: it
loads an aggregate by replaying its event history, and saves it by appending the
events it produced. To do that it needs to know which aggregate type its streams
belong to — a string such as `"Order"` that is stamped onto every event and
snapshot it writes.

This guide shows how to wire a repository up so that string lives in exactly one
place. Declare `aggregate_type` on the aggregate class, omit it at every
construction site, and the repository infers it from
`aggregate_factory.aggregate_type`. You will also find here how the inference
rule behaves with inheritance, when passing `aggregate_type=` explicitly is still
the right call, how to read the resolved value back off the repository, and how
to fix `ValueError: Cannot infer aggregate_type from <YourClass>`.

## Before you start

You need:

- A working install of `eventsource-py` and familiarity with the basics in
  [Getting Started](../getting-started.md): defining a `DomainEvent`, subclassing
  `AggregateRoot` (or `DeclarativeAggregate`), and persisting through an
  `EventStore`.
- An aggregate class you can edit. The inference rule reads a class attribute off
  the class you pass as `aggregate_factory`, so you need to be able to add one
  line to that class — or, if it is third-party code you cannot change, be
  prepared to use the explicit `aggregate_type=` escape hatch described below.
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
defines `aggregate_type: str = "Unknown"` as its default. Inheriting that default
does **not** satisfy inference — the repository treats `"Unknown"` (and the empty
string) as "not configured" and raises. That behavior is the subject of
[Rejected values](#rejected-values-unknown-the-aggregateroot-default-and) below,
and it is the most common reason a repository that looks correctly wired fails at
construction time.

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

That is the whole step. `aggregate_type` is a plain class attribute declared on
`AggregateRoot` as `aggregate_type: str = "Unknown"`, so you are overriding a
default rather than introducing a new field — no decorator, no registration call,
no metaclass hook is involved.

Pick the value deliberately. It is not a display name: the aggregate stamps it
onto every event it creates (`AggregateRoot.create_event` passes
`aggregate_type=self.aggregate_type` into the event's metadata), and the
repository uses it as the aggregate-type key for snapshots. Once events are in
the store under `"Order"`, changing the string to `"OrderAggregate"` orphans that
history. Choose the domain name you want to live with — typically the ubiquitous
-language noun (`"Order"`, `"Customer"`, `"Extraction"`), not the Python class
name.

Two things to check while you are in the class:

- **Do not leave the default.** A class that never assigns `aggregate_type`
  inherits `"Unknown"`, which inference rejects. The failure surfaces later, at
  repository construction, not here.
- **Do not annotate it as `ClassVar`.** The surrounding attributes in
  `DeclarativeAggregate` (`requires_creation_event`, `unregistered_event_handling`)
  are `ClassVar`, but `aggregate_type` is declared as a normal `str` attribute on
  `AggregateRoot`; a bare assignment matches the base declaration and keeps mypy
  quiet.

You can confirm the attribute is visible to the inference rule without
constructing anything:

```python
>>> OrderAggregate.aggregate_type
'Order'
```

If that prints `'Order'` — read off the *class*, not an instance — Step 2 will
work.

## Step 2: Construct the repository without `aggregate_type`

With the attribute in place, build the repository from just the store and the
factory:

```python
store = InMemoryEventStore()

repo = AggregateRepository(
    event_store=store,
    aggregate_factory=OrderAggregate,
)
```

No `aggregate_type=` argument. The constructor declares it as
`aggregate_type: str | None = None`, and when the value is `None` it calls
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
was resolved at construction — inferred or explicit, it reads the same. If this
matches the string your existing events were written under, the repository will
find that history.

Inference happens once, in `__init__`, and its result is what everything
downstream uses: the type recorded on spans (`ATTR_AGGREGATE_TYPE`), and the
`aggregate_type` handed to the `AggregateSnapshotManager` when a
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
