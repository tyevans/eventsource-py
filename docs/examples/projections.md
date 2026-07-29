# Projections

In this tutorial you will build read models from an event stream and keep them
up to date with `SubscriptionManager`.

You will work through two runnable examples that ship with the repository:

- `examples/subscriptions/basic_projection.py` — one `OrderSummaryProjection`
  driven end to end: catch-up over history, live events, status inspection,
  graceful shutdown, and checkpoint verification.
- `examples/projection_example.py` — three projections (`OrderListProjection`,
  `CustomerStatsProjection`, `DailyRevenueProjection`) reading the *same* order
  event stream into three differently shaped read models.

Both examples use the same order domain — `OrderPlaced`, `OrderShipped`,
`OrderDelivered`, `OrderCancelled` and an `OrderAggregate` — and the same
in-memory infrastructure: `InMemoryEventStore`, `InMemoryEventBus`, and
`InMemoryCheckpointRepository`. Nothing here needs Docker or a database, so you
can run every step on your machine as you read.

A projection in this library is an ordinary Python object. It needs only two
things: a `subscribed_to()` method returning the event classes it cares about,
and an `async def handle(event)` method that folds each event into whatever
state you want to query later. `SubscriptionManager` supplies the hard parts —
replaying history from the event store, switching over to live events from the
event bus, and recording a checkpoint so a restart resumes where it left off.

By the end you will have run both examples, seen a read model change as new
orders arrive, and know which pieces to swap out when you move from in-memory
components to PostgreSQL or SQLite.

## What you'll build

Two working read-model pipelines, each runnable as a single command from the
repository root.

**Part 1 — one projection, full lifecycle.** You build
`OrderSummaryProjection`, an in-memory read model holding three fields: a
`status -> count` mapping, a running `total_revenue`, and a set of unique
customer ids. Its `get_summary()` query returns total orders, counts by status,
revenue, and customer count. You will write four orders' worth of history into
an `InMemoryEventStore` through an `AggregateRepository` *before* the
subscription exists, then start a `SubscriptionManager` with
`SubscriptionConfig(start_from="beginning", batch_size=100)` and watch the
projection replay that history. After catch-up you place one more order and see
the same projection pick it up live off the `InMemoryEventBus`. You finish by
reading `manager.get_all_statuses()` — subscription state, events processed,
last position — calling `await manager.stop()`, and confirming the position
survived in `checkpoint_repo.get_checkpoint("OrderSummary")`.

Run it with:

```bash
python -m examples.subscriptions.basic_projection
```

**Part 2 — three projections, one stream.** You attach three independently
shaped read models to the same order events on a single `SubscriptionManager`:

- `OrderListProjection` — a per-order record you can query with
  `get_all_orders()`, `get_orders_by_status(status)`, and
  `get_orders_by_customer(customer_id)`.
- `CustomerStatsProjection` — per-customer rollups, plus
  `get_top_customers(limit)` for a leaderboard.
- `DailyRevenueProjection` — revenue bucketed by day, queried with
  `get_daily_revenue(date_str)` or `get_revenue_report()`.

Each gets its own subscription name and therefore its own checkpoint, so one
projection can be rebuilt or fall behind without disturbing the others. You
will place new orders after startup and verify all three read models move
together.

Run it with:

```bash
python -m examples.projection_example
```

Neither example needs Docker, PostgreSQL, or any configuration — everything is
in-memory and completes in a couple of seconds. Both enable `logging` at `INFO`,
so you also see the subscription lifecycle (catch-up start, transition to live,
checkpoint writes) interleaved with the projection's own output.

Once both run, the final part of the tutorial covers the changes that matter for
production: swapping the in-memory store, bus, and checkpoint repository for
PostgreSQL or SQLite ones, choosing `start_from` when rebuilding a projection,
and handling failures with retries and a dead-letter queue.

## Prerequisites

**Python 3.11 or newer.** The package requires `>=3.11`; both examples were run
against 3.12 while writing this tutorial.

**A checkout of this repository.** The two examples live in the repository's
`examples/` package, and you run them as modules from the repository root:

```bash
git clone https://github.com/tyevans/eventsource-py.git
cd eventsource-py
```

**The library installed with its core dependencies.** Everything in this
tutorial uses `InMemoryEventStore`, `InMemoryEventBus`, and
`InMemoryCheckpointRepository`, which are part of the base package — you do not
need the `postgresql`, `sqlite`, `redis`, or any other extra. If you use `uv`,
the simplest setup is:

```bash
uv sync --all-extras
```

`--all-extras` installs more than you need here, but it is the standard
development setup for this repository and saves you from re-syncing later.
Plain `uv sync`, or `pip install -e .` into a virtual environment, is enough for
these examples. The only runtime dependencies they touch are `pydantic` and the
standard library.

**No Docker, no database, no configuration.** Both scripts create their
infrastructure in-process and exit on their own in a couple of seconds. There is
nothing to start beforehand and nothing to clean up afterwards.

Verify your setup before continuing:

```bash
uv run python -m examples.subscriptions.basic_projection
```

You should see INFO-level subscription logs followed by
`Example completed successfully!`. If you installed with `pip` into an activated
virtual environment, drop the `uv run` prefix and use `python -m ...` directly.
Run the command from the repository root — `python -m examples....` resolves the
`examples` package relative to your working directory, so it will fail with
`No module named examples` anywhere else.

**Background knowledge.** You should be comfortable with `async`/`await` and
`asyncio.run()`, since every store, bus, and projection method in this library
is a coroutine. Familiarity with this library's events and aggregates —
`DomainEvent` subclasses, `@register_event`, `AggregateRoot`, and
`AggregateRepository` — will help, but the tutorial re-introduces the order
domain it uses, so you can follow along without having written an aggregate
before.

## Part 1: A single projection end to end (basic_projection.py)

In this part you run `examples/subscriptions/basic_projection.py` and walk
through it step by step. The script builds one read model,
`OrderSummaryProjection`, and takes it through the whole subscription
lifecycle: history written before the subscription exists, catch-up, live
events, status inspection, shutdown, and a persisted checkpoint.

Run it first so you have the output in front of you:

```bash
uv run python -m examples.subscriptions.basic_projection
```

### The domain: Order events and OrderAggregate

The example defines four events, each a `DomainEvent` subclass decorated with
`@register_event`:

```python
@register_event
class OrderPlaced(DomainEvent):
    """Event emitted when an order is placed."""

    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float
```

`OrderShipped` carries `tracking_number` and `carrier`, `OrderDelivered`
carries `delivered_at`, and `OrderCancelled` carries `reason`. All four set
`aggregate_type = "Order"` and an `event_type` string matching the class name.

`OrderAggregate` is an `AggregateRoot[OrderState]` whose state is a small
pydantic model holding `order_id`, `customer_id`, `customer_name`,
`total_amount`, and a `status` string that starts at `"draft"`. Its `_apply`
method folds each event into new state — `OrderPlaced` builds the initial
`OrderState` with `status="placed"`, the other three `model_copy(update=...)`
the status to `"shipped"`, `"delivered"`, or `"cancelled"`. Command methods
(`place`, `ship`, `deliver`, `cancel`) each guard against an invalid
transition, construct the event with
`aggregate_version=self.get_next_version()`, and call
`self.apply_event(event)`. The guards are plain `ValueError` raises:

- `place()` refuses if `self.version > 0` — an order can only be placed once.
- `ship()` requires `status == "placed"`.
- `deliver()` requires `status == "shipped"`.
- `cancel()` refuses once the status is `"delivered"` or `"cancelled"`.

The aggregate matters here only as a way to produce a realistic event stream.
The projection never sees `OrderAggregate` — it sees the events.

### Defining OrderSummaryProjection: subscribed_to() and handle()

The projection is a plain class. It implements no base class and inherits
nothing:

```python
class OrderSummaryProjection:
    def __init__(self):
        self.order_counts: dict[str, int] = defaultdict(int)
        self.total_revenue: float = 0.0
        self.customers: set[UUID] = set()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped, OrderDelivered, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        ...
```

Those two methods are the whole contract that `SubscriptionManager` needs.
There is no registration decorator and no base class to extend — duck typing is
enough. (`eventsource.subscriptions` does ship a `BaseSubscriber` ABC declaring
exactly these two abstract methods, if you prefer inheritance, but the example
does not use it.)

`subscribed_to()` returns event classes. The two runners use them differently:

- The **catch-up runner** reads *every* event in the store, in global-position
  order, and filters in process — non-matching events are skipped but still
  advance the subscription's position, so progress is recorded even through
  long stretches of events you do not care about.
- The **live runner** calls `event_bus.subscribe(event_type, handler)` once per
  returned class, so the bus only ever hands you the four order events.

`handle()` is a coroutine called once per matching event, one at a time, in
order.

The body of `handle()` is an `isinstance` chain that moves counts between
buckets:

- `OrderPlaced` increments `placed`, adds `total_amount` to `total_revenue`,
  and adds `customer_id` to the `customers` set.
- `OrderShipped` decrements `placed` (guarded by `> 0`) and increments
  `shipped`.
- `OrderDelivered` decrements `shipped` and increments `delivered`.
- `OrderCancelled` walks `["placed", "shipped"]` and decrements the first
  non-zero bucket, then increments `cancelled`.

Each branch also logs a line, which is why you see projection output
interleaved with subscription logs when you run the script.

Two details worth copying into your own projections. First, `order_counts` is a
`defaultdict(int)`, so a bucket springs into existence on first touch and the
guards never have to test for a missing key — which is also why `get_summary()`
can report `'shipped': 0` for a status no order is currently in. Second,
`handle()` is entirely additive: it never reads the store, never loads an
aggregate, and never awaits anything. A projection that only mutates local
state is trivially correct under replay, because replaying the same events in
the same order rebuilds the same numbers.

Note what the read model is *not*: it is not the aggregate state. `OrderState`
tracks one order; `OrderSummaryProjection` tracks no orders individually at
all. It is a shape chosen for one query — `get_summary()` — which returns
`total_orders` (the sum of all counts), `by_status`, `total_revenue`, and
`unique_customers`. Anything that query does not need was never stored.

### Wiring the infrastructure: InMemoryEventStore, InMemoryEventBus, InMemoryCheckpointRepository

Step 1 of the script creates four objects:

```python
event_store = InMemoryEventStore()
event_bus = InMemoryEventBus()
checkpoint_repo = InMemoryCheckpointRepository()

repo = AggregateRepository(
    event_store=event_store,
    aggregate_factory=OrderAggregate,
    aggregate_type="Order",
    event_publisher=event_bus,
)
```

All three infrastructure classes are constructed with no arguments — every
parameter they accept is keyword-only and optional (tracing knobs, mostly), so
there is no configuration step here at all. `InMemoryEventStore`,
`InMemoryEventBus`, and `AggregateRepository` all come from the top-level
`eventsource` package, alongside `AggregateRoot`, `DomainEvent`, and
`register_event`. Only `SubscriptionConfig` and `SubscriptionManager` are
imported from `eventsource.subscriptions`.

Each object has a distinct job:

- **`InMemoryEventStore`** is the log. It keeps events per aggregate id *and*
  in a single global-order list with an incrementing global position, which is
  what makes the catch-up read possible: a subscription can ask for everything
  from position 0 forward and get the same order every time. It is the
  authority on history.
- **`InMemoryEventBus`** is the fan-out for events happening *now*. It maps
  event type to handlers, delivers in-process, and isolates handler errors so
  one failing subscriber does not stop the others. It knows nothing about the
  past — it only carries what is published while you are listening.
- **`InMemoryCheckpointRepository`** is the bookmark. It stores, per
  subscription name, the last processed event id, event type, and global
  position, keyed in a plain dict guarded by an `asyncio.Lock`. The manager
  writes to it with `save_position(...)`; you read it back with
  `get_checkpoint(name)` (last event id) or `get_position(name)` (integer
  position).

The split between store and bus is the reason a projection has two phases.
History can only come from the store; liveness can only come from the bus. The
checkpoint repository is what stitches the two together across restarts.

`AggregateRepository` is the write side, and the `event_publisher=event_bus`
argument is the load-bearing line in this block. Inside `save()`, the
repository appends the aggregate's uncommitted events to the store and then, if
and only if a publisher was supplied, calls `await
self._event_publisher.publish(uncommitted_events)`. Drop that argument and the
example still writes history correctly and catch-up still works — but step 7's
live order would never reach the projection, because nothing would ever be
published. `aggregate_type="Order"` is passed explicitly here; it can also be
inferred from an `aggregate_type` attribute on the aggregate class, with the
explicit argument winning when both are present.

Note that the manager is *not* created yet — that happens in step 4, after the
history in step 3 has already been written. Building the infrastructure and
building the subscription are separate acts, and the gap between them is the
whole point of the next two sections.

All three implementations are process-local and lose everything on exit; the
docstrings say so plainly. That is fine for a tutorial, and it is exactly the
set of three objects you swap for PostgreSQL or SQLite equivalents when you go
to production — the projection and the manager code above them do not change.

### Publishing history through AggregateRepository

Step 3 writes events *before* any subscription exists. Three orders are placed,
one each for Alice Johnson, Bob Smith, and Carol Williams:

```python
order_ids = []
for customer_id, customer_name in customers:
    order_id = uuid4()
    order_ids.append(order_id)

    order = repo.create_new(order_id)
    order.place(customer_id, customer_name, 100.0 + len(order_ids) * 50)
    await repo.save(order)
```

The amount is computed from the list length *after* the append, so it is 150.0,
200.0, and 250.0 — the 600.0 total you will see in the read model shortly.

The three calls in the loop are the whole write-side cycle:

- **`repo.create_new(order_id)`** just constructs an aggregate at version 0. It
  touches neither the store nor the bus; nothing is persisted yet.
- **`order.place(...)`** runs the command guard, builds an `OrderPlaced` at
  `get_next_version()`, and applies it — leaving the event in the aggregate's
  uncommitted list.
- **`await repo.save(order)`** appends the uncommitted events to the store with
  `expected_version = aggregate.version - len(uncommitted_events)`, marks them
  committed, and *then* publishes them to `event_publisher`. `save()` is a
  no-op when there is nothing uncommitted, so a redundant save is harmless.

Then the first order is shipped and delivered, and the second is cancelled:

```python
order = await repo.load(order_ids[0])
order.ship("TRACK-001", "FedEx")
await repo.save(order)

order = await repo.load(order_ids[0])
order.deliver()
await repo.save(order)

order = await repo.load(order_ids[1])
order.cancel("Customer request")
await repo.save(order)
```

Each step reloads the aggregate rather than reusing the instance from the loop.
That is not ceremony: `load()` replays the stored events to rebuild both state
and `version`, and `save()` derives `expected_version` from that version. Ship
the same in-memory object twice without reloading and you would be computing
`expected_version` from a stale count — reloading is what keeps optimistic
locking honest against whatever else has written in the meantime. Here the
reload also enforces the domain guards: `deliver()` only succeeds because the
replayed state already says `"shipped"`.

That leaves six events in the store, in this global order: three `OrderPlaced`,
then `OrderShipped`, `OrderDelivered`, and `OrderCancelled`. The
`InMemoryEventStore` assigns each an increasing global position, which is the
order catch-up will replay them in.

Every one of those six was also published to the bus — and every publish went
nowhere. `InMemoryEventBus.publish()` looks up handlers by event type, finds
none registered, and returns. The manager does not exist yet (it is built in
step 4), so there is nothing listening.

This gap is the point of the example, not an accident of ordering. It is the
normal situation for a new read model: the log already contains months of
history, and the projection has to be built from events that were published
long before anyone subscribed. The store is the only record of that history —
which is why the next two sections configure `start_from="beginning"` and let
the catch-up runner read it back.

### Configuring the subscription: SubscriptionConfig(start_from="beginning", batch_size=100)

```python
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

config = SubscriptionConfig(
    start_from="beginning",
    batch_size=100,
)

subscription = await manager.subscribe(
    projection,
    config=config,
    name="OrderSummary",
)
```

`start_from` accepts `"beginning"`, `"end"`, `"checkpoint"`, or an integer
global position. The default is `"checkpoint"`, which resumes from stored
progress; the example passes `"beginning"` explicitly so the run replays the
whole log every time regardless of what a previous run recorded. `batch_size`
is how many events each catch-up read pulls at once — 100 is the default.

`name="OrderSummary"` is optional. Without it the subscription is named after
the subscriber's class (`OrderSummaryProjection`). The name matters because it
is also the checkpoint key, so choosing it explicitly means the checkpoint
survives a class rename.

`subscribe()` returns a `Subscription` object you can hold for monitoring; the
script just prints `subscription.name`.

### Starting the manager and catching up

```python
await manager.start()
await asyncio.sleep(0.5)
```

`start()` kicks off catch-up for every registered subscription and returns —
it does not block until the replay finishes. The subscription reads the store
in batches from position 0, feeds each event to `handle()`, and when it reaches
the end of the log it switches to live delivery off the event bus. The
`asyncio.sleep(0.5)` is the example's way of waiting for that to happen before
querying; in a long-running service you would call
`await manager.run_until_shutdown()` instead and never poll at all.

The INFO logs show the transition — a catch-up runner starting, events
processed, the state change to `live`, and the live runner subscribing handlers
to the bus for each of the four event types.

### Querying the read model after catch-up

```python
summary = projection.get_summary()
```

Querying is just a method call on your own object — there is no framework in
the read path. After catch-up the script prints:

```text
6. Querying projection after catch-up:
   Total orders: 3
   By status: {'placed': 1, 'shipped': 0, 'delivered': 1, 'cancelled': 1}
   Total revenue: $600.00
   Unique customers: 3
```

Three orders, because `total_orders` sums the status buckets and each order
lives in exactly one bucket: Alice's is delivered, Bob's is cancelled, Carol's
is still placed. Revenue is 150 + 200 + 250 = 600 — note it counts the
cancelled order too, since `handle()` only adds revenue on `OrderPlaced` and
never subtracts it. That is a modelling choice, not a bug, and a good example
of how a read model encodes the question you intend to ask.

### Live events: new orders arriving through the bus

With the subscription now in the `live` state, step 7 places one more order:

```python
new_order_id = uuid4()
order = repo.create_new(new_order_id)
order.place(uuid4(), "David Brown", 500.0)
await repo.save(order)

await asyncio.sleep(0.2)
```

Nothing about this call site knows a projection exists. `repo.save()` appends
to the store and publishes to the bus; the live runner's handler picks the
event up and calls `handle()`. The short sleep gives that delivery time to
complete before the next query. Re-running `get_summary()` shows:

```text
   Total orders: 4
   By status: {'placed': 2, 'shipped': 0, 'delivered': 1, 'cancelled': 1}
   Total revenue: $1100.00
   Unique customers: 4
```

The same `handle()` code path served both history and live traffic — that is
the point of the catch-up-then-live design. Your projection never needs to know
which phase an event arrived in.

### Inspecting subscription status with get_all_statuses()

```python
status = manager.get_all_statuses()
for name, sub_status in status.items():
    print(f"     State: {sub_status.state}")
    print(f"     Events processed: {sub_status.events_processed}")
    print(f"     Last position: {sub_status.position}")
```

`get_all_statuses()` returns `dict[str, SubscriptionStatus]` keyed by
subscription name. `SubscriptionStatus` is a point-in-time snapshot with more
fields than the example prints: `state`, `position`, `lag_events`,
`events_processed`, `events_failed`, `events_dlq`, `last_processed_at`,
`started_at`, `uptime_seconds`, `error`, and `recent_errors_count`. It also has
a `to_dict()` method, which is what you would expose from a metrics or health
endpoint.

For this run it prints:

```text
   Subscription 'OrderSummary':
     State: live
     Events processed: 7
     Last position: 6
```

`live` means catch-up finished and the subscription is reading from the bus.
Seven events processed: six from history plus the one live `OrderPlaced`.

### Graceful shutdown and checkpoint verification

```python
await manager.stop()

checkpoint = await checkpoint_repo.get_checkpoint("OrderSummary")
if checkpoint:
    print(f"   Checkpoint saved at position: {checkpoint}")
```

`stop()` is a graceful shutdown: it unsubscribes the live handlers from the bus
(you can see one log line per event type), drains in-flight work, stops the
runners, and lets the final checkpoint settle. The manager was constructed with
default `shutdown_timeout=30.0` and `drain_timeout=10.0` seconds, which bound
how long that takes.

`get_checkpoint(name)` returns `UUID | None` — the **event id** of the last
processed event, not an integer position, so the printed value looks like
`16dcfbe7-b3ef-4739-a7d5-3e09befa0c1b` and differs on every run. The label in
the script's output says "position", which is a little loose; the integer
position lives on `SubscriptionStatus.position` instead.

Because the checkpoint repository here is `InMemoryCheckpointRepository`, the
value vanishes with the process. Swap in the PostgreSQL or SQLite checkpoint
repository and the same code resumes across restarts — which is exactly what
`start_from="checkpoint"` is for. This example forces `"beginning"` so each run
is reproducible.

### Full source

The complete script is `examples/subscriptions/basic_projection.py` in the
repository. Reading it top to bottom, it is laid out in the same order as this
tutorial: logging setup, the four `@register_event` classes, `OrderState` and
`OrderAggregate`, `OrderSummaryProjection`, and an `async def main()` broken
into the numbered steps you saw in the output, run under
`asyncio.run(main())`.

It is self-contained: everything it imports comes from the top-level
`eventsource` package plus `eventsource.subscriptions`, and it is safe to copy
into a scratch file and edit. Good first modifications: change
`start_from="beginning"` to `"checkpoint"` and run twice to see the second run
replay nothing; drop `OrderCancelled` from `subscribed_to()` and watch the
cancelled bucket stay empty; or make revenue exclude cancelled orders by
tracking amounts per order id.
