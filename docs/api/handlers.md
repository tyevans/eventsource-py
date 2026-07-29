# Handlers

Reference documentation for the handler infrastructure exported from
`eventsource.handlers`: the `@handles` decorator that marks event handler
methods, the inspection helpers that read its marker, the `HandlerRegistry` that
discovers and routes decorated methods on an owner object, and the
`HandlerAdapter` that normalizes arbitrary sync/async handlers to a single
awaitable interface.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `handles` | decorator factory | Marks a method as the handler for one `DomainEvent` subclass |
| `get_handled_event_type` | function | Reads the event type off a decorated function, or `None` |
| `is_event_handler` | function | Whether a function carries the `@handles` marker |
| `HandlerRegistry` | class | Discovers, validates, and dispatches `@handles` methods on an owner |
| `HandlerInfo` | dataclass | Metadata for one registered handler |
| `HandlerSignatureError` | exception | Raised for a handler with the wrong parameter count |
| `HandlerAdapter` | class | Normalizes any handler shape to `await adapter.handle(event)` |
| `get_handler_name` | function | Descriptive name for a handler, for logs and repr |
| `UnregisteredEventHandling` | type alias | `Literal["ignore", "warn", "error"]` policy value |

```python
from eventsource.handlers import (
    handles,
    get_handled_event_type,
    is_event_handler,
    HandlerRegistry,
    HandlerInfo,
    HandlerSignatureError,
    HandlerAdapter,
    get_handler_name,
)
```

Only `handles` is re-exported from the top-level `eventsource` package
(`from eventsource import handles`); the remaining names are imported from
`eventsource.handlers`. `UnregisteredEventHandling` is not in the package's
`__all__` — import it from `eventsource.handlers.registry` if you need to
annotate the policy argument.

`HandlerRegistry.dispatch()` and `HandlerAdapter.handle()` are `async`;
everything else on this page is synchronous. The module depends only on
`eventsource.events`, `eventsource.exceptions`, and `eventsource.protocols`, so
it imports with the core dependencies alone.

The two consumers of this machinery are `DeclarativeAggregate` (sync handlers
applied to aggregate state) and `DeclarativeProjection` (async handlers that
write read models); both discover handlers through the same `@handles` marker.

Behavior described below is that of the current source in
`src/eventsource/handlers/decorators.py`, `registry.py`, and `adapter.py`.

## Overview

The package has three layers that build on one another.

1. **Marking.** `@handles(EventType)` sets a single attribute,
   `_handles_event_type`, on the decorated function and returns the function
   unchanged. It performs no validation and no registration of its own — a
   decorated method is an ordinary method that happens to carry a marker.
2. **Inspection.** `get_handled_event_type(func)` reads that attribute
   (returning `None` when absent) and `is_event_handler(func)` reports whether
   it is present. Any code can use these to discover handlers without importing
   the registry.
3. **Discovery and dispatch.** `HandlerRegistry(owner)` scans the attributes of
   an owner instance, collects the marked methods into `HandlerInfo` records,
   validates their signatures (raising `HandlerSignatureError` for a bad
   parameter count), and routes an event to the matching method via
   `await registry.dispatch(event, context)`. Events with no registered handler
   follow the registry's `unregistered_event_handling` policy.

`HandlerAdapter` sits apart from that pipeline. It is not concerned with
`@handles` at all: it wraps a handler supplied from the outside — an object with
a `handle()` method, or a plain callable, sync or async — so that callers can
uniformly `await adapter.handle(event)`. Subscription and bus code uses it so
that a single call shape works regardless of how a user wrote the handler.
`get_handler_name(handler)` provides the human-readable name used in the
adapter's `repr` and in log messages.

Nothing in this module registers itself globally. A `HandlerRegistry` belongs to
one owner object, and a `HandlerAdapter` wraps one handler; both are created
explicitly (or by `DeclarativeAggregate` / `DeclarativeProjection` on your
behalf).

### Import surface

Every public name is available from `eventsource.handlers`, which re-exports it
from the defining submodule:

| Name | Defining module | Also exported from |
| --- | --- | --- |
| `handles` | `eventsource.handlers.decorators` | `eventsource.handlers`, `eventsource` |
| `get_handled_event_type` | `eventsource.handlers.decorators` | `eventsource.handlers` |
| `is_event_handler` | `eventsource.handlers.decorators` | `eventsource.handlers` |
| `HandlerRegistry` | `eventsource.handlers.registry` | `eventsource.handlers` |
| `HandlerInfo` | `eventsource.handlers.registry` | `eventsource.handlers` |
| `HandlerSignatureError` | `eventsource.handlers.registry` | `eventsource.handlers` |
| `HandlerAdapter` | `eventsource.handlers.adapter` | `eventsource.handlers` |
| `get_handler_name` | `eventsource.handlers.adapter` | `eventsource.handlers` |
| `UnregisteredEventHandling` | `eventsource.handlers.registry` | — |

`UnregisteredEventHandling` is the one exception: it is listed in
`eventsource.handlers.registry.__all__` but is not re-exported by the
`eventsource.handlers` package, so annotate the policy argument with an explicit
submodule import:

```python
from eventsource.handlers.registry import UnregisteredEventHandling

policy: UnregisteredEventHandling = "warn"
```

`eventsource.handlers.adapter` also re-exports the `AsyncEventHandler` and
`SyncEventHandler` protocols for backward compatibility; their canonical home is
`eventsource.protocols`.

## `handles(event_type)`

Marks a method as the handler for one `DomainEvent` subclass. It is the only
decorator in the library for event routing and is shared by
`DeclarativeAggregate` and `DeclarativeProjection`.

```python
from eventsource import handles          # or: from eventsource.handlers import handles
```

### Signature and return value

```python
def handles(event_type: type[DomainEvent]) -> Callable[[F], F]
```

| Parameter | Type | Description |
| --- | --- | --- |
| `event_type` | `type[DomainEvent]` | The event class this method handles. Positional; there are no other parameters. |

`handles` is a decorator *factory*: calling it returns the actual decorator,
which sets one attribute on the function and returns **the same function
object** (`F` is a `TypeVar` bound to `Callable[..., Any]`, so the decorated
method keeps its exact type for type checkers). No wrapper is created, so
`functools.wraps` is unnecessary, `__name__`/`__doc__`/`__wrapped__` are
untouched, and the method can still be called directly in tests.

`handles` itself raises nothing and validates nothing — not the argument type,
not the method signature, not whether the same event type is already handled.
All validation happens later, at discovery time (see
[`HandlerRegistry`](#handlerregistry)).

### Valid handler shapes: `(self, event)` and `(self, context, event)`

Two shapes are accepted by the discovery machinery, distinguished only by
parameter count (`self` excluded, since discovery inspects the *bound* method):

```python
@handles(OrderCreated)
def handler(self, event: OrderCreated) -> None: ...            # 1 parameter

@handles(OrderCreated)
async def handler(self, context, event: OrderCreated) -> None: # 2 parameters
    ...
```

The event is always the **last** parameter. The first parameter of the two-arg
form is a context object supplied by the caller — for `DeclarativeProjection`
this is the database connection. `HandlerRegistry.dispatch(event, context)`
picks the call shape from the recorded `param_count`: a 1-parameter handler is
called as `handler(event)` and the `context` argument is dropped; a 2-parameter
handler is called as `handler(context, event)`, passing `None` when the caller
supplied no context.

Anything outside 1–2 parameters raises
[`HandlerSignatureError`](#handlersignatureerror) when a `HandlerRegistry` is
constructed over the owner (with `validate_on_init=True`, the default).
Parameter *names* are irrelevant; only the count matters, and the count is taken
from the bound method via `inspect.signature`, so `self` is already excluded.
When a handler's signature cannot be inspected at all — `inspect.signature`
raising `ValueError` or `TypeError`, as with some builtins and C-implemented
callables — discovery falls back to `param_count = 1`, which both passes
validation and selects the single-argument call shape.

The annotation on the final parameter is checked against the decorator argument
during validation, but a mismatch only logs a warning — it never raises. A
handler with no annotation on that parameter, or one annotated with something
that is not a class (a string, a union, a generic alias), skips the check
silently.

### Sync aggregate handlers vs. async projection handlers

`handles` does not care whether the method is sync or async; the consumer does.

| Consumer | Handler must be | Shape | Invocation |
| --- | --- | --- | --- |
| `DeclarativeAggregate` | sync | `(self, event)` | `handler(event)` inside `_apply()` |
| `DeclarativeProjection` | async | `(self, event)` or `(self, context, event)` | awaited by `HandlerRegistry.dispatch()` |

`DeclarativeProjection.__init__` builds its registry with `require_async=True`
and `validate_on_init=True`, so a non-async handler on a projection raises
`ValueError` (not `HandlerSignatureError`) at projection construction, with a
message showing the `async def` rewrite:

```
Handler '_handle_order_created' in OrderProjection must be async.

Change:
  def _handle_order_created(self, ...)

To:
  async def _handle_order_created(self, event: OrderCreated) -> None
```

Aggregates do not use `HandlerRegistry` at all.
`DeclarativeAggregate.__init_subclass__` walks `dir(cls)`, picks up every
attribute carrying `_handles_event_type`, and stores an
`event_type -> method_name` map in the subclass's own `_event_handlers` dict.
`_apply(event)` then looks up `type(event)` in that map and calls the bound
method synchronously as `handler(event)`.

Because that path is a plain `getattr`/call, it performs no validation at all:
`require_async` has no analogue, so an `async def` aggregate handler is
"called" successfully but only produces an un-awaited coroutine (state is never
mutated, and Python emits a `RuntimeWarning`), and a handler with the wrong
parameter count fails with an ordinary `TypeError` at apply time rather than at
class definition. Only projections get the up-front signature check.

Both consumers key their routing table on the exact event class, so one class
registers at most one handler per event type — a second `@handles(Same)` method
overwrites the first, with the winner determined by attribute ordering. Subclass
event types are *not* matched by a parent's handler; each concrete event class
needs its own `@handles`.

Both consumers also share the same unregistered-event policy vocabulary — the
`unregistered_event_handling` class attribute (`"ignore"` / `"warn"` /
`"error"`), defaulting to `"ignore"` on `DeclarativeAggregate` — but the
aggregate implements it inline in `_handle_unregistered_event()` while the
projection delegates to its `HandlerRegistry`. Both raise `UnhandledEventError`
under `"error"`.

### How the decorator marks a function (`_handles_event_type` attribute)

The entire implementation is an attribute assignment:

```python
def decorator(func: F) -> F:
    func._handles_event_type = event_type
    return func
```

The attribute name is `_handles_event_type` and its value is the class object
passed to `handles` — not a name or a string. `get_handled_event_type` is
`getattr(func, "_handles_event_type", None)` and `is_event_handler` is
`hasattr(func, "_handles_event_type")`; there is no other state.

Consequences worth knowing:

- **The marker is visible through both the class and an instance.** Attribute
  lookup on a bound method falls through to the underlying function, so
  `hasattr(instance.handler, "_handles_event_type")` and
  `hasattr(Cls.handler, "_handles_event_type")` are both true. The two consumers
  rely on different ends of that: `DeclarativeAggregate.__init_subclass__`
  scans `dir(cls)` before any instance exists, while `HandlerRegistry` scans
  `dir(self._owner)` on a live instance.
- **The marker is inherited.** It lives on the function object stored in the
  defining class's `__dict__`, so a subclass that does not override the method
  inherits the decorated function and is discovered identically. Overriding the
  method *without* re-applying `@handles` drops the marker for that subclass.
- **Decorators stacked below `@handles` are harmless; ones above can hide it.**
  Decorators apply bottom-up, so `@handles` on top sets the attribute on
  whatever object the decorators beneath it produced. A decorator placed *above*
  `@handles` that returns a fresh wrapper function will not carry the attribute
  unless it copies it — `functools.wraps` does (it updates the wrapper's
  `__dict__`), and a manual
  `wrapper._handles_event_type = getattr(func, "_handles_event_type", None)`
  also works.
- **Stacking `@handles` twice overwrites rather than accumulates.** The second
  assignment replaces the first, so the topmost `@handles` wins and the method
  handles exactly one event type. One method cannot handle two event types.
- **Nothing is validated here.** `handles` does not check that `event_type` is a
  `DomainEvent` subclass, and the decorator works on any callable — including a
  module-level function with no `self`. Discovery, not decoration, is where
  errors surface.
- **Treat the attribute as private.** Read it through
  [`get_handled_event_type`](#get_handled_event_typefunc) and
  [`is_event_handler`](#is_event_handlerfunc), which are the supported surface;
  the leading underscore signals that the attribute name itself is an
  implementation detail.

### Examples: DeclarativeAggregate and DeclarativeProjection

Aggregate — sync, single parameter, mutates state:

```python
from eventsource import handles
from eventsource.aggregates import DeclarativeAggregate


class OrderAggregate(DeclarativeAggregate[OrderState]):
    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(order_id=self.aggregate_id, status="created")

    @handles(OrderShipped)
    def _on_order_shipped(self, event: OrderShipped) -> None:
        self._state.status = "shipped"
```

Projection — async, two parameters, writes through the supplied connection:

```python
from eventsource import handles
from eventsource.projections import DeclarativeProjection


class OrderProjection(DeclarativeProjection):
    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        await conn.execute(insert_order, {"id": event.aggregate_id})
```

Handler method names are conventional, not meaningful: `_on_*` for aggregates
and `_handle_*` for projections are house style. Discovery looks at the marker,
not the name, and finds `_`-prefixed and public methods alike.
