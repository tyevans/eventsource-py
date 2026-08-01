# Tutorial 7: Reacting to Events with the Event Bus

So far your events have been things that *happened*. In this tutorial you will make
other parts of the system *react* to them.

The event bus is the piece that connects the two sides. A producer publishes a list of
domain events and never learns who cared; a consumer subscribes to the event types it
handles and never learns who produced them. That decoupling is what lets you add an
email notifier, an inventory projection, and an audit log to the same `OrderPlaced`
event without any of them knowing about each other.

You will work entirely with `InMemoryEventBus`, the single-process implementation. It
needs no Docker, no Redis, and no database -- just `asyncio`. It is the bus you will use
in unit tests forever, and the bus you will use in production until you have more than
one process.

## What you'll build

A small ordering slice that reacts to two events, `OrderPlaced` and `OrderShipped`:

- a confirmation-email handler written as a plain async function
- an inventory projection written as an `EventSubscriber` object
- an audit logger that receives *every* event via a wildcard subscription
- a deliberately broken handler, so you can watch error isolation keep the others alive

Along the way you will publish synchronously and in the background, inspect what the bus
recorded, drain in-flight work at shutdown, and read the bus's own statistics.

## Prerequisites

- **Python 3.13 or newer.** That is the floor `eventsource` itself requires.
- **`eventsource` installed.** From a clone, `uv sync` is enough; `uv sync --all-extras`
  also works. Nothing in this tutorial needs an extra -- `InMemoryEventBus` is built on
  `asyncio` alone, and the only dependencies in play are pydantic and sqlalchemy, which
  the core install already brings in. No Redis, no RabbitMQ, no Kafka, no Docker, no
  database.
- **Everything imports from the top-level package.** `DomainEvent`, `EventSubscriber`,
  and `InMemoryEventBus` are all re-exported from `eventsource`, so a single import line
  covers the whole tutorial.
- **Familiarity with defining a `DomainEvent` subclass.** Step 2 defines two from
  scratch, so you can follow along without it, but the earlier tutorials on events and
  aggregates give useful context.
- **Comfort with `async`/`await` and `asyncio.run()`.** Publishing is async; every
  runnable snippet below drives it from an `async def main()` called through
  `asyncio.run()`.

Optional, for the testing recipe near the end: `pytest` and `pytest-asyncio`, both
already present if you installed the project's dev dependencies.

Create a file called `event_bus_tour.py`. Everything below goes into it, and every
snippet runs as written.

## Step 1: Create an in-memory event bus

```python
import asyncio
from uuid import uuid4

from eventsource import DomainEvent, EventSubscriber, InMemoryEventBus

bus = InMemoryEventBus()
```

That is the whole setup. The constructor takes no required arguments -- it builds an
empty subscriber registry, a re-entrant lock so subscription changes are thread-safe, a
set to track background tasks, and a fresh statistics dictionary.

A brand-new bus starts completely empty. You can check that right now, before writing
any handlers:

```python
print(bus.published_events)        # []
print(bus.get_subscriber_count())  # 0
print(bus.get_stats())
```

```
[]
0
{'events_published': 0, 'handlers_invoked': 0, 'handler_errors': 0,
 'background_tasks_created': 0, 'background_tasks_completed': 0}
```

Those three are the observation points you will come back to in Steps 6 and 13. Notice
that `published_events` is a property while `get_subscriber_count()` and `get_stats()`
are methods -- and that none of them require `await`. Only publishing is async.

The constructor does accept two keyword-only options, both of them about tracing:

```python
bus = InMemoryEventBus(enable_tracing=False)   # no OpenTelemetry spans
bus = InMemoryEventBus(tracer=my_tracer)       # bring your own Tracer
```

`enable_tracing` defaults to `True`, but the bus only emits real spans when the optional
OpenTelemetry dependencies are installed -- otherwise the tracer it creates is a no-op,
so the default costs you nothing. Passing an explicit `tracer` overrides
`enable_tracing` entirely. You will not need either one for this tutorial.

One instance per process is the normal arrangement: create the bus at startup, register
every handler against it, and hand the same object to whatever publishes. In tests,
invert that habit -- build a fresh bus per test, which is cheaper than resetting a
shared one and gives you zeroed statistics for free.

## Step 2: Define the domain events you'll react to

```python
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    total: float


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    carrier: str


ORDER_ID = uuid4()


def placed() -> OrderPlaced:
    return OrderPlaced(aggregate_id=ORDER_ID, order_number="ORD-001", total=42.0)


def shipped() -> OrderShipped:
    return OrderShipped(
        aggregate_id=ORDER_ID,
        order_number="ORD-001",
        carrier="UPS",
        aggregate_version=2,
    )
```

Two events, four lines of payload between them. That is all the domain you need to see
every behavior of the bus.

Notice what you did *not* write. There is no `event_type = "OrderPlaced"` line --
`DomainEvent.__init_subclass__` derives it from the class name when you subclass:

```python
print(placed().event_type)   # OrderPlaced
print(shipped().event_type)  # OrderShipped
```

That derived string is what the audit logger in Step 9 will print. You *may* set
`event_type` explicitly (for a renamed class that must keep its old wire name), but the
library logs a warning when the value disagrees with the class name unless you also set
`suppress_event_type_warning = True`.

The only fields you are required to supply are `aggregate_id` and `aggregate_type`.
Everything else has a default, which is why the factory helpers are so short:

```python
e = placed()
print(e.aggregate_version)  # 1
print(e.event_version)      # 1
print(e.tenant_id)          # None
print(e.metadata)           # {}
print(e.occurred_at.tzinfo) # UTC
```

`event_id` and `correlation_id` are each generated fresh per instance by `uuid4()`, and
`occurred_at` is a timezone-aware UTC timestamp taken at construction. `aggregate_version`
defaults to `1`, which is why `shipped()` passes `aggregate_version=2` explicitly -- it is
the second event on the same order. Nothing in the bus reads that number, but keeping it
honest costs nothing and makes the printed events in later steps easier to follow.

Two details worth internalizing before you go further:

- **Events are frozen.** `model_config = ConfigDict(frozen=True)` on `DomainEvent` means
  assigning to a field raises `pydantic.ValidationError`. A handler cannot mutate an
  event and change what the next handler sees. That is exactly the property that makes
  fanning one event out to many independent consumers safe.
- **`aggregate_type` is declared as a field with a default**, not a class variable. That
  is the idiomatic pattern here: `aggregate_type: str = "Order"` gives every instance the
  right value while keeping it a real, serialized field.

The `ORDER_ID` module constant and the two factory functions exist purely so the
remaining steps can say `placed()` instead of retyping a constructor. Each call builds a
*new* instance -- same `aggregate_id`, but a fresh `event_id` and `occurred_at` every
time -- so publishing `placed()` twice publishes two genuinely distinct events.

## Step 3: Subscribe a handler with `subscribe(EventType, handler)`

Now attach something that reacts. Add this below the event definitions:

```python
async def send_confirmation(event: OrderPlaced) -> None:
    print(f"emailed confirmation for {event.order_number}")


bus.subscribe(OrderPlaced, send_confirmation)

print(bus.get_subscriber_count())              # 1
print(bus.get_subscriber_count(OrderPlaced))   # 1
```

Two arguments: the event *class* (not its name as a string, not an instance) and the
handler. Note also what `subscribe()` is not -- it is a plain synchronous method with no
`await`, and it returns `None`. Only publishing is async.

### Handlers can be four different shapes

The bus does not require your handler to subclass or implement anything. It wraps
whatever you pass in a `HandlerAdapter`, which recognizes four shapes and normalizes all
of them to a single async call:

```python
# 1. async function
async def send_confirmation(event: OrderPlaced) -> None:
    print(f"emailed confirmation for {event.order_number}")


# 2. sync function (or a lambda)
def write_audit_row(event: OrderPlaced) -> None:
    print(f"audit: {event.order_number}")


# 3. object with an async handle()
class Mailer:
    async def handle(self, event: DomainEvent) -> None:
        print(f"object async: {event.order_number}")


# 4. object with a sync handle()
class Ledger:
    def handle(self, event: DomainEvent) -> None:
        print(f"object sync: {event.order_number}")


bus.subscribe(OrderPlaced, send_confirmation)
bus.subscribe(OrderPlaced, write_audit_row)
bus.subscribe(OrderPlaced, Mailer())
bus.subscribe(OrderPlaced, Ledger())
bus.subscribe(OrderPlaced, lambda e: print(f"lambda: {e.order_number}"))
```

Publishing one `OrderPlaced` runs all five:

```
emailed confirmation for ORD-001
audit: ORD-001
object async: ORD-001
object sync: ORD-001
lambda: ORD-001
```

Sync handlers are wrapped in an async shim, so they run inline on the event loop -- they
are not moved to a thread. A sync handler that blocks (a `requests` call, a
`time.sleep()`) stalls the loop for everyone. Keep sync handlers to fast, in-memory work
and use an async handler for anything doing I/O.

Objects are checked for `handle()` *before* being checked for callability, so a class
that defines both `handle()` and `__call__()` will have its `handle()` used.

Start with a function. Reach for an object when the handler needs state -- a database
session, an accumulated read model, a client connection. Step 8 builds one out properly
as an `EventSubscriber`.

### Bad handlers fail at subscribe time

Anything that is neither callable nor has a `handle()` method is rejected right away:

```python
bus.subscribe(OrderPlaced, "not a handler")
```

```
TypeError: Handler must have a handle() method or be callable, got <class 'str'>
```

This is a small but real convenience: a typo in your wiring surfaces at startup, when
you register handlers, instead of silently at 3am when the first event arrives.

Note that the *signature* is not checked -- only that the object is callable or has
`handle`. A handler taking the wrong number of arguments will raise at publish time, and
Step 10 shows what happens to exceptions there (they are swallowed). Type-annotate your
handlers and let mypy catch that class of mistake.

### Matching is by exact type

The bus looks up handlers with `type(event)`. That is an exact dictionary lookup, not an
`isinstance` check: a handler subscribed to a base event class will **not** receive
events of a subclass. If you want one handler to see a family of events, subscribe it to
each concrete type, or use the wildcard subscription from Step 9.

### Registration is a list, not a set

Multiple handlers can share an event type -- that is the entire point of a bus, and they
all run. But the registry keeps a plain list, so subscribing the *same* handler twice
registers it twice, and it will be invoked twice per event:

```python
bus.subscribe(OrderPlaced, write_audit_row)
bus.subscribe(OrderPlaced, write_audit_row)
print(bus.get_subscriber_count(OrderPlaced))   # 2
```

There is no deduplication. Guard against re-running your wiring code (a module-level
`subscribe()` call in a module imported twice under different names is the classic way
to get accidental double delivery).

Finally, `subscribe()` takes the internal lock, so it is safe to call from any thread and
safe to call while the bus is dispatching -- handler lists are copied before invocation,
so a subscription added mid-dispatch simply takes effect on the next event.

## Step 4: Publish events with `await bus.publish([...])`

Nothing has actually happened yet -- you have a bus and a handler, but no events have
flowed. Add a `main()` at the bottom of the file and run it:

```python
async def main() -> None:
    await bus.publish([placed()])


asyncio.run(main())
```

```
emailed confirmation for ORD-001
```

That is the whole producer side. The code publishing the event named no handler, imported
no handler module, and learned nothing about who reacted. Add three more subscribers to
`OrderPlaced` and this `publish()` call does not change.

### Always pass a list

`publish()` takes a **list of events**, even when you have exactly one. That is deliberate:
events usually arrive as the batch of uncommitted events from an aggregate, and handing
the batch over as a unit is what lets the bus preserve their order (Step 5).

Passing a bare event instead of a list is the mistake to watch for, because it fails
*quietly*. A `DomainEvent` is a pydantic model, and pydantic models are iterable over
their `(field_name, value)` pairs -- so the bus dutifully iterates the fields and
"publishes" a dozen tuples, none of which match any subscription:

```python
await bus.publish(placed())   # WRONG -- no error, no handler runs
print(len(bus.published_events))    # 12
print(bus.published_events[0])      # ('event_id', UUID('...'))
```

No exception, no output from your handler. If a handler mysteriously never fires, check
for a missing pair of brackets first.

### `publish()` waits by default

`publish()` is async, and by default it is fully synchronous in the "await until done"
sense: when the `await` returns, every handler for every event in the list has finished
running. It returns `None`.

That default is what makes the bus easy to reason about and easy to test -- no sleeps, no
polling, no races. Step 11 introduces `background=True`, which trades that guarantee for
lower latency; until then, assume every publish is complete when it returns.

Two edge cases behave the way you would hope:

- **Publishing an empty list** returns immediately and does nothing -- no statistics move.
  You never need to guard `if events:` before calling.
- **Publishing an event with no subscribers** is not an error. The bus logs a `DEBUG`
  line, records the event in `published_events`, counts it in `events_published`, and
  moves on. An unhandled event is a normal state, not a failure.

### What one `publish()` call does

For each event in the list, in order, the bus:

1. records it in `published_events` (Step 6),
2. collects the handlers subscribed to `type(event)`, plus any wildcard handlers (Step 9),
3. runs them all and waits for them to finish, catching any exception each one raises
   (Step 10),
4. increments `events_published`,

and only then moves to the next event. Everything else in this tutorial is a consequence
of those four lines, so it is worth re-reading once before continuing.

## Step 5: Confirm ordering -- events dispatch sequentially, in publication order

```python
log: list[tuple[str, str]] = []

bus = InMemoryEventBus()
bus.subscribe(OrderPlaced, lambda e: log.append(("placed", e.order_number)))
bus.subscribe(OrderShipped, lambda e: log.append(("shipped", e.carrier)))


async def main() -> None:
    await bus.publish([placed(), shipped()])
    print(log)


asyncio.run(main())
```

```
[('placed', 'ORD-001'), ('shipped', 'UPS')]
```

`OrderPlaced` first, `OrderShipped` second -- the order you passed them. Run it again and
you get the same list. That is not luck.

### Between events: guaranteed

The bus loops over your list and, for each event, `await`s the full dispatch -- every
handler for that event finished -- before it touches the next one. There is no
`gather()` across events, so nothing overlaps.

This is the guarantee you can build on. An aggregate that emits
`[OrderPlaced, OrderShipped]` will never have a shipping handler run before the placement
handlers have completed, so a read model fed from this list is always assembled in causal
order. It is also why `publish()` takes the whole batch: split the same two events across
two `publish()` calls in two different tasks and you have given up the ordering you were
relying on.

### Within one event: *not* guaranteed

Here is the nuance that catches people. Once the bus has picked an event, it collects
that event's handlers and launches them **all at once** with `asyncio.gather()`. They run
concurrently:

```python
bus = InMemoryEventBus()
order: list[str] = []


async def slow_handler(event: OrderPlaced) -> None:
    await asyncio.sleep(0.02)
    order.append("slow")


async def fast_handler(event: OrderPlaced) -> None:
    order.append("fast")


bus.subscribe(OrderPlaced, slow_handler)   # subscribed first
bus.subscribe(OrderPlaced, fast_handler)   # subscribed second


async def main() -> None:
    await bus.publish([placed()])
    print(order)


asyncio.run(main())
```

```
['fast', 'slow']
```

The handler you subscribed *first* finished *last*. Subscription order is not execution
order, and `publish()` still waited for both before returning -- concurrency here is about
interleaving, not about skipping the wait.

Purely synchronous handlers happen to complete in subscription order, because they never
yield control back to the event loop. Do not lean on that. Change one of them to an async
handler later and the ordering silently changes underneath you.

So: if handler B needs handler A's work to be done, do not encode that as "subscribe A
first." Either merge them into a single handler, or have A publish a follow-up event that
B subscribes to -- then the between-events guarantee is doing the work, and it is a
guarantee the library actually makes.

## Step 6: Inspect what was published with `bus.published_events`

The in-memory bus records every event that passes through it:

```python
print(len(bus.published_events))
print([type(e).__name__ for e in bus.published_events])
```

```
2
['OrderPlaced', 'OrderShipped']
```

`published_events` is a property, not a method, and it returns a *copy* of the internal
list under the lock -- mutating what you get back cannot corrupt the bus. Events are
recorded just before they are dispatched, so an event appears here even if every one of
its handlers blew up, and even if no handler was subscribed at all.

This is a testing affordance. It exists on `InMemoryEventBus` specifically; it is not
part of the abstract `EventBus` interface, so do not build production logic on it.

## Step 7: Reset between runs with `bus.clear_published_events()`

```python
bus.clear_published_events()
print(bus.published_events)
```

```
[]
```

This clears the recorded events only. Your subscribers stay registered, and the
statistics counters from Step 13 are *not* reset. When a single bus instance is shared
across several test cases, call this between them so one test's events do not show up in
the next test's assertions.

## Step 8: Subscribe an object handler and a whole subscriber with `subscribe_all()`

Functions are fine for one-liners, but real consumers carry state. Subclass
`EventSubscriber` and declare which event types you want:

```python
class InventoryProjection(EventSubscriber):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        self.calls.append(type(event).__name__)


bus = InMemoryEventBus()
projection = InventoryProjection()
bus.subscribe_all(projection)

print(bus.get_subscriber_count())              # 2
print(bus.get_subscriber_count(OrderPlaced))   # 1
```

`subscribe_all()` is pure convenience: it calls `subscriber.subscribed_to()` and then
does one `subscribe(event_type, subscriber)` per entry. That is why the total subscriber
count is 2 -- one registration per declared type, all pointing at the same object.

Because a single `handle()` receives every declared type, branch on the concrete type
inside it (`isinstance(event, OrderPlaced)`), or use `DeclarativeProjection` with
`@handles` to get that routing written for you.

## Step 9: Receive every event with `subscribe_to_all_events()` (wildcard)

Some consumers do not want a list of types -- they want everything. Audit logs, metrics,
and debug tracing are the classic cases:

```python
audit: list[str] = []

bus.subscribe_to_all_events(lambda e: audit.append(e.event_type))


async def main() -> None:
    await bus.publish([placed(), shipped()])
    print(projection.calls)   # ['OrderPlaced', 'OrderShipped']
    print(audit)              # ['OrderPlaced', 'OrderShipped']


asyncio.run(main())
```

Wildcard handlers live in a separate list from type-specific ones. For each event the
bus builds its handler set as *specific handlers first, then wildcard handlers* -- but
since they are all gathered concurrently, treat that as a construction detail rather
than an ordering promise.

Wildcard subscribers are counted separately too:

```python
print(bus.get_wildcard_subscriber_count())     # 1
print(bus.get_subscriber_count())              # 2 -- wildcards not included
```

## Step 10: Watch error isolation -- a failing handler doesn't stop the others

This is the property that makes a bus safe to hang side effects off. Subscribe a handler
that always raises, plus a healthy one:

```python
bus = InMemoryEventBus()
delivered: list[str] = []


def charge_card(event: OrderPlaced) -> None:
    raise RuntimeError("payment gateway down")


bus.subscribe(OrderPlaced, charge_card)
bus.subscribe(OrderPlaced, lambda e: delivered.append(e.order_number))


async def main() -> None:
    await bus.publish([placed()])
    print(delivered)
    print(bus.get_stats())


asyncio.run(main())
```

```
['ORD-001']
{'events_published': 1, 'handlers_invoked': 1, 'handler_errors': 1,
 'background_tasks_created': 0, 'background_tasks_completed': 0}
```

The healthy handler ran. `publish()` returned normally -- **it did not raise**. The
exception was caught per handler, logged at `ERROR` with a full traceback and structured
`extra` fields (handler name, event type, event id), and counted in `handler_errors`.

Take the tradeoff seriously: the bus will never tell your publishing code that a
consumer failed. If a side effect must not be silently lost, do not rely on the bus
alone -- put it behind the outbox pattern or a subscription runner with retry and a dead
letter queue.

## Step 11: Publish without blocking using `background=True`

Sometimes the publisher should not wait -- an HTTP handler wants to return 201 without
blocking on a slow email send:

```python
bus = InMemoryEventBus()
done: list[str] = []


async def slow_email(event: OrderPlaced) -> None:
    await asyncio.sleep(0.05)
    done.append(event.order_number)


bus.subscribe(OrderPlaced, slow_email)


async def main() -> None:
    await bus.publish([placed()], background=True)
    print("immediately:", done, bus.get_background_task_count())


asyncio.run(main())
```

```
immediately: [] 1
```

`publish()` returned before any handler ran. Internally it wrapped the dispatch in
`asyncio.create_task()` and kept a reference in a set so the task cannot be garbage
collected mid-flight.

The cost is eventual consistency. Right after that `publish()` returns, a read model fed
by this bus is still stale. Do not use `background=True` when the caller is about to read
back what it just wrote.

Note also that `bus.published_events` will not contain the event yet either -- recording
happens inside the background task.

## Step 12: Drain background work with `await bus.shutdown()`

An unfinished background task is lost work if the process exits. `shutdown()` waits for
them:

```python
async def main() -> None:
    await bus.publish([placed()], background=True)
    print("immediately:", done)
    await bus.shutdown()
    print("after shutdown:", done, bus.get_background_task_count())


asyncio.run(main())
```

```
immediately: []
after shutdown: ['ORD-001'] 0
```

`shutdown()` takes a `timeout` in seconds (default `30.0`). If tasks are still running
when it expires, it logs a warning and cancels them -- it does not raise. With no
background tasks pending it returns immediately, so it is always safe to call.

Wire it into your application's shutdown hook (FastAPI's lifespan, or a `finally` block
around your main loop). And call it at the end of any test that publishes with
`background=True`, or the assertion will race the handler.

`shutdown()` does not permanently close the bus -- later `background=True` publishes
still create tasks, they just will not have been waited for.

## Step 13: Check your work with `get_stats()` and `get_subscriber_count()`

`get_stats()` returns a copy of five running counters:

```python
print(bus.get_stats())
```

```
{'events_published': 1, 'handlers_invoked': 1, 'handler_errors': 0,
 'background_tasks_created': 1, 'background_tasks_completed': 1}
```

- `events_published` -- events fully dispatched (incremented *after* dispatch, so a
  background publish only counts once its task has run)
- `handlers_invoked` -- handler calls that returned without raising
- `handler_errors` -- handler calls that raised
- `background_tasks_created` / `background_tasks_completed` -- when these two are equal,
  nothing is in flight

The counters accumulate for the life of the bus. There is no reset;
`clear_published_events()` and `clear_subscribers()` leave them alone. For a fresh
baseline, construct a fresh bus.

For wiring checks, the count methods are the useful ones:

```python
bus.get_subscriber_count()               # all type-specific handlers, all types
bus.get_subscriber_count(OrderPlaced)    # handlers for this type only
bus.get_wildcard_subscriber_count()      # wildcard handlers
bus.get_background_task_count()          # currently in-flight background tasks
```

`get_subscriber_count(OrderPlaced)` counts *only* type-specific registrations. A wildcard
handler will receive `OrderPlaced` but will not be counted here -- so this returning `0`
does not mean nothing will handle the event.

## Detaching handlers: `unsubscribe`, `unsubscribe_from_all_events`, `clear_subscribers`

Each `subscribe*` call has an inverse, and each returns a `bool` rather than raising:

```python
handler = lambda e: None
bus.subscribe(OrderPlaced, handler)

print(bus.unsubscribe(OrderPlaced, handler))   # True  -- found and removed
print(bus.unsubscribe(OrderPlaced, handler))   # False -- already gone

bus.subscribe_to_all_events(handler)
print(bus.unsubscribe_from_all_events(handler))  # True
```

Matching is by **object identity**, not equality. Pass back the exact object you
subscribed with. This is the practical reason to avoid inline lambdas for anything you
might want to detach later -- you have no reference to remove.

`unsubscribe()` only searches type-specific registrations and
`unsubscribe_from_all_events()` only searches wildcards; they are not interchangeable. A
subscriber registered with `subscribe_all()` needs one `unsubscribe()` call per type it
declared.

To wipe everything at once:

```python
bus.clear_subscribers()
print(bus.get_subscriber_count())   # 0
```

That drops both type-specific and wildcard handlers. It leaves `published_events` and
the statistics counters intact.

## Using the bus in your tests

The features above combine into a compact testing recipe. Give each test its own bus,
publish synchronously, and assert:

```python
import pytest


@pytest.mark.asyncio
async def test_order_placement_notifies_customer() -> None:
    bus = InMemoryEventBus()
    notifier = ConfirmationNotifier()
    bus.subscribe(OrderPlaced, notifier)

    await bus.publish([placed()])

    assert notifier.sent == ["ORD-001"]
    assert len(bus.published_events) == 1
    assert bus.get_stats()["handler_errors"] == 0
```

Four habits worth keeping:

- **Construct a fresh `InMemoryEventBus` per test.** Cheaper and safer than remembering
  to call `clear_subscribers()` and `clear_published_events()` on a shared one, and it
  gives you zeroed statistics for free.
- **Assert `handler_errors == 0`.** Because handler exceptions are swallowed, a broken
  handler otherwise shows up only as a missing side effect -- or as a passing test.
- **Prefer the default synchronous publish.** No sleeps, no flakes.
- **If you must test `background=True`, `await bus.shutdown()` before asserting.** It is
  the only deterministic way to know the work has landed.

When you only need to check *which* events a piece of code emitted, and not what
happened downstream, subscribe nothing at all and read `published_events`. The bus works
fine as a pure spy.

## What you learned

- `InMemoryEventBus()` needs no configuration; `subscribe()` is sync and `publish()` is
  async
- Handlers can be async functions, sync functions, or objects with `handle()` -- all are
  normalized by `HandlerAdapter`
- `publish()` takes a list and dispatches events sequentially in order; handlers *of one
  event* run concurrently
- `subscribe_all()` registers an `EventSubscriber` for each type in `subscribed_to()`;
  `subscribe_to_all_events()` registers a wildcard handler that sees everything
- Handler exceptions are caught, logged, and counted -- never propagated to the publisher
- `background=True` trades read-after-write consistency for latency, and `shutdown()` is
  what makes that work durable across process exit
- `published_events`, `get_stats()`, and the count methods give you an observable bus for
  tests

## Next steps: moving to a distributed bus

`InMemoryEventBus` dispatches within one process. The moment you run two workers, a
handler registered in worker A will never see an event published by worker B.

`eventsource` ships three distributed implementations behind the same abstract
`EventBus` interface -- `RedisEventBus`, `RabbitMQEventBus`, and `KafkaEventBus` -- each
behind an optional dependency (`redis`, `aio-pika`, `aiokafka`). Because the interface is
shared, your handlers and your `subscribe()` calls carry over; what changes is
construction (each takes a config object and needs connecting) and the guarantees you
get.

Plan for the differences before you switch:

- **Delivery is at-least-once.** Make handlers idempotent.
- **`published_events` is in-memory only.** Testing helpers do not cross the network, so
  keep unit tests on `InMemoryEventBus` and use integration tests for the real backend.
- **Ordering is a backend property**, not a bus-interface guarantee.
- **Failures are no longer silent-and-local.** Redelivery, retries, and dead letter
  queues become part of your design -- see the subscription runners, which pair a durable
  event store with checkpointing, retry policy, and a DLQ.

For most applications the right sequence is: build and test everything on
`InMemoryEventBus`, then swap the construction site when you actually need a second
process.
