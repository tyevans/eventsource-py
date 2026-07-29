# 0007. Handler Registry and Adapter as Collaborators

Two pieces of machinery sit between an event and the code that reacts to it.
`HandlerRegistry` (`src/eventsource/handlers/registry.py`) finds the
`@handles`-decorated methods on an object, checks their signatures, and routes
events to them. `HandlerAdapter` (`src/eventsource/handlers/adapter.py`) takes
whatever shape a subscriber arrives in — an object with `handle()`, a bare
function, sync or async — and presents a single `await adapter.handle(event)`
interface to the event bus backends.

Neither is a base class. `DeclarativeProjection` *has* a `HandlerRegistry`; it
does not inherit one. Each bus backend *wraps* subscribers in a
`HandlerAdapter`; it does not require them to implement an interface. This ADR
explains why both were built as collaborators rather than as inheritance
points, why `require_async` is a constructor parameter instead of a fixed
policy, and why aggregates deliberately kept their own class-level handler map
in `AggregateRoot.__init_subclass__` instead of adopting the registry.

## Status

Accepted. Implemented in `src/eventsource/handlers/registry.py`
(`HandlerRegistry`, `HandlerInfo`) and `src/eventsource/handlers/adapter.py`
(`HandlerAdapter`, `get_handler_name`), with the `@handles` decorator in
`src/eventsource/handlers/decorators.py`.

`DeclarativeProjection.__init__` in `src/eventsource/projections/base.py`
constructs a per-instance `HandlerRegistry`, and every event bus backend —
`memory.py`, `redis.py`, `kafka.py`, `rabbitmq.py` under `src/eventsource/bus/`
— wraps subscribers in `HandlerAdapter` rather than type-checking them at each
call site.

The decision is partial by design: `AggregateRoot` in
`src/eventsource/aggregates/base.py` still builds its own class-level
`_event_handlers` map in `__init_subclass__` and has not been migrated. That
split, and the `require_async` parameter that makes migration possible, are the
main things this record has to justify.

## Context

### Handler discovery, validation, and routing were embedded in DeclarativeProjection

`DeclarativeProjection` is already a fairly loaded class: it extends
`CheckpointTrackingProjection`, inheriting checkpoint storage, dead-letter
handling, and optional tracing, and it adds tenant filtering of its own. On top
of that it had to scan itself for `@handles`-decorated methods, decide whether
each one had a usable signature, build an event-type routing map, and then
dispatch into it. Four further concerns — discovery, validation, routing, and
unhandled-event policy — lived inside a class whose actual job is "consume
events and update a read model." The registry module's docstring still names
this explicitly: it exists to address a Single Responsibility Principle
violation in `DeclarativeProjection`.

None of that logic is projection-specific. The `@handles` decorator does
nothing more than stamp `func._handles_event_type` onto the method
(`src/eventsource/handlers/decorators.py`); everything downstream — finding
those markers, checking that each handler takes one or two parameters after
`self`, mapping event type to handler, and deciding what to do with an event
nobody handles — is generic behavior that any `@handles` user needs. The same
decorator is documented for `DeclarativeAggregate` as well as
`DeclarativeProjection`, so the discovery logic had two potential consumers
from the start while living inside only one of them.

Leaving it there had two concrete costs. The logic could not be exercised
without constructing a projection, which drags in checkpoint and DLQ
repositories just to assert that a two-parameter async handler is accepted. And
it could not be reused by anything that was not a projection — a read-model
projection, an aggregate, or a plain class with `@handles` methods would each
have had to reimplement or inherit their way to the same behavior.

### Sync/async handler branching was duplicated across every EventBus backend

The bus interface is deliberately permissive about what a subscriber is. Both
`EventBus.subscribe()` and `EventBus.unsubscribe()` in
`src/eventsource/bus/interface.py` are typed `FlexibleEventHandler |
EventHandlerFunc`, and the docstring spells out the latitude: "Object with
handle() method or callable", with `bus.subscribe(OrderCreated, lambda e:
print(e))` given as a supported example alongside a handler object. The
`protocols.py` module carries the matching split — `AsyncEventHandler`,
`SyncEventHandler`, and `FlexibleEventHandler` for the may-be-either case.

That latitude has to be resolved somewhere, and before the adapter it was
resolved four times. `bus/memory.py`, `bus/redis.py`, `bus/kafka.py`, and
`bus/rabbitmq.py` each had to answer the same two questions at their own
invocation sites: does this object have a `handle` attribute, or do we call it
directly? Is the resulting target a coroutine function to `await`, or a plain
function to call? Each backend carried its own `hasattr` /
`asyncio.iscoroutinefunction` ladder. The questions are identical in all four,
but nothing forced the answers to stay identical — a sync `handle()` that
happens to return a coroutine, for instance, is an edge case each backend had
to remember on its own.

The duplication also leaked into unsubscription. Every backend stores
subscribers in a `dict[type[DomainEvent], list[...]]` plus a wildcard list for
`subscribe_all` / `subscribe_to_all_events`, and `unsubscribe()` has to find
the entry matching a handler the caller hands back later. That only works if
whatever the backend stored still compares equal to the caller's original
object — so any wrapping scheme could not be a transparent behavior wrapper
alone; it had to preserve handler identity through the wrap. Kafka's
`unsubscribe()` comment records the requirement directly: find and remove the
handler by identity.

So the shape of the missing abstraction was fixed by these two forces
together: one object that resolves the four handler shapes to a single
awaitable call, *and* compares equal to the handler it wrapped.

### Prior art: the tracing refactor moved from inheritance to composition

The same shape of problem had already been solved once in this codebase.
Tracing started as a `TracingMixin` that components inherited. It is now the
`Tracer` protocol in `src/eventsource/observability/tracer.py`, which
components hold as an injected collaborator instead — the module docstring
describes itself as providing "a tracer abstraction that can be injected into
components as a dependency, replacing the inheritance-based TracingMixin
pattern," and lists the payoff: components have single responsibility (no
tracing concern), the collaborator is easy to mock for testing, tracer
implementations are swappable, and there are no inheritance hierarchy issues.

The migration is visible throughout the tree: roughly two dozen constructors —
in `stores/`, `bus/`, `projections/`, `subscriptions/`, and `migration/` —
carry the same line, `self._tracer = tracer or create_tracer(__name__,
enable_tracing)`, each annotated "Composition-based tracing (replaces
TracingMixin)." The default is a real object (`NullTracer` when tracing is
off), not `None`, so call sites never branch on whether tracing exists; they
just open a span.

Handler management presented the identical trade-off: a cross-cutting
capability that several unrelated classes need, where the classes in question
already have a base class of their own. `Projection` subclasses could not
absorb a `HandlerMixin` without multiple inheritance, exactly as they could not
absorb `TracingMixin`. The tracing refactor established that this library
resolves that tension by injecting a collaborator and defaulting it to a
working object, and `HandlerRegistry` and `HandlerAdapter` follow the same
shape — with one difference worth noting up front: the registry needs a
reference back to its owner in order to discover that owner's `@handles`
methods, so it is constructed by the owner rather than passed in from outside.

## Decision

Handler management is split into two collaborator objects, neither of which is
part of any inheritance chain. `HandlerRegistry` owns the *owner-side* concern
— finding an object's `@handles` methods, validating them, and routing events
into them. `HandlerAdapter` owns the *bus-side* concern — taking a subscriber of
unknown shape and presenting one awaitable `handle(event)` call. Classes that
need either capability hold an instance; they do not subclass anything new.

### Extract discovery, signature validation, and dispatch into `eventsource.handlers.registry.HandlerRegistry`

Discovery, signature validation, routing, and unhandled-event policy now live
in one class in `src/eventsource/handlers/registry.py`, constructed as
`HandlerRegistry(owner, *, require_async=True,
unregistered_event_handling="ignore", validate_on_init=True)`. It does two
things at construction and one at runtime.

It **discovers** by walking `dir(self._owner)`, skipping names that start with
`__`, and asking `get_handled_event_type(attr)` whether each attribute carries
the marker the `@handles` decorator stamps on. Attributes that are `None`,
attributes without the marker, and attributes whose marked event type is not an
actual `type` are all skipped — the last of those is what keeps mock objects in
tests from being registered as handlers. Each survivor becomes a `HandlerInfo`
dataclass recording `event_type`, `handler_name`, the bound `handler` callable,
`is_async` (from `inspect.iscoroutinefunction`), and `param_count` (the length
of `inspect.signature(attr).parameters`, which for a bound method already
excludes `self`, falling back to `1` when the signature cannot be read). The
map is a plain `dict[type[DomainEvent], HandlerInfo]`, so it holds at most one
handler per event type; because `dir()` returns names in sorted order, two
methods decorated with the same `@handles(X)` resolve to the
alphabetically-later method name rather than raising.

It **validates**, when `validate_on_init` is left on, by running each
`HandlerInfo` through `_validate_handler`: the handler must be async if
`require_async`, its `param_count` must be 1 or 2, and — advisory only — the
last parameter's type annotation should match the type passed to `@handles`.
The first check raises `ValueError`, the second raises `HandlerSignatureError`,
and the third only logs a warning. Failing at construction means a typo'd
handler signature surfaces when the projection object is built, not on the
first event that would have hit it.

It **dispatches** through `async def dispatch(event, context=None) -> bool`.
That method looks up `type(event)` in the map, then calls `handler(event)` when
`param_count == 1` and `handler(context, event)` otherwise, awaiting the result
when `HandlerInfo.is_async`. It returns `True` when a handler ran. A missing
handler routes to `_handle_unregistered_event` and returns `False`. Around
those, the registry exposes the read-only surface owners actually need:
`get_handler()`, `has_handler()`, `get_subscribed_events()`,
`get_all_handlers()`, plus `owner` and `handler_count` properties.

Consumers hold the registry rather than inherit it. `DeclarativeProjection`
builds one as the first statement of its `__init__` in
`src/eventsource/projections/base.py`, forwarding its class-level
`unregistered_event_handling` attribute into the constructor, and then
delegates: `subscribed_to()` is nothing but `return
self._handler_registry.get_subscribed_events()`, and event application calls
`self._handler_registry.get_handler(type(event))` to decide whether this event
is its business before calling `dispatch(...)`.

The `context` argument is what lets one registry serve both projection
flavors. `DeclarativeProjection` dispatches with `context=None`, while the
database-backed projection in the same module passes the live connection
(`dispatch(event, context=conn)`) so that two-parameter handlers receive it —
same discovery, same validation, different payload in the second slot. The
read-model projection in `src/eventsource/readmodels/projection.py` reuses the
registry the same way, dispatching with `context=None` or with a repository
depending on the handler's arity. That reuse across three unrelated projection
classes is the practical payoff of extracting the registry: none of them had to
share a base class to share the behavior.

### Normalize handler shapes behind `eventsource.handlers.adapter.HandlerAdapter`

`HandlerAdapter(handler)` resolves the four accepted shapes once, in
`_normalize`, and stores the result:

- object with async `handle()` → the bound method is used directly;
- object with sync `handle()` → wrapped in an `async` function that calls it and
  awaits the result if it unexpectedly turns out to be a coroutine;
- async callable → used directly;
- sync callable → wrapped the same way as the sync `handle()` case;
- anything else → `TypeError` at construction, not at first publish.

Every bus backend then stores adapters rather than raw handlers —
`self._subscribers: dict[type[DomainEvent], list[HandlerAdapter]]` plus an
`_all_event_handlers: list[HandlerAdapter]` for wildcard subscriptions — and
invokes them with a bare `await adapter.handle(event)`. The
`hasattr`/`iscoroutinefunction` ladders that each backend used to carry are
gone; `bus/memory.py`, `bus/redis.py`, `bus/kafka.py`, and `bus/rabbitmq.py`
all call `HandlerAdapter(handler)` in `subscribe()` and nothing more.

Unsubscription works because the adapter defines `__eq__` and `__hash__`
against the *original* handler's identity (`self._original is other._original`,
and equal to the bare original object too; `__hash__` is `id(self._original)`).
So `unsubscribe(handler)` can build a throwaway `target_adapter =
HandlerAdapter(handler)` and remove the stored entry by equality, exactly as if
no wrapping had happened. The adapter also exposes `.original` and a `.name`
(via `get_handler_name`) for logging, and re-exports `AsyncEventHandler` and
`SyncEventHandler` from `protocols.py` so older imports from this module keep
working.
