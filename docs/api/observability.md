# Observability API Reference

Reference documentation for the tracing layer exported from
`eventsource.observability`: the `Tracer` protocol and its three
implementations (`NullTracer`, `OpenTelemetryTracer`, `MockTracer`), the
`create_tracer` factory, the `SpanKindEnum` enumeration, the module-level
helpers `get_tracer`, `should_trace`, and the `@traced` decorator, the
`OTEL_AVAILABLE` dependency flag, and the `ATTR_*` span-attribute constants
used consistently across every instrumented eventsource component.

OpenTelemetry is an optional dependency. Every name below is importable and
usable whether or not `opentelemetry` is installed; when it is missing, tracing
degrades to no-ops rather than raising.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `Tracer` | runtime-checkable protocol | Contract every tracer implements: `span()`, `start_span()`, `span_with_kind()`, `enabled` |
| `NullTracer` | class | No-op tracer used when tracing is disabled or OpenTelemetry is absent |
| `OpenTelemetryTracer` | class | Real tracer backed by `opentelemetry.trace` |
| `MockTracer` | class | Recording tracer for asserting spans in tests |
| `create_tracer` | function | Selects the appropriate tracer from a name and an `enable_tracing` flag |
| `SpanKindEnum` | enum | Backend-independent span kinds (INTERNAL, PRODUCER, CONSUMER, CLIENT, SERVER) |
| `get_tracer` | function | Returns a raw OpenTelemetry tracer, or `None` when unavailable |
| `should_trace` | function | Resolves whether tracing should be active for a component |
| `traced` | decorator | Wraps sync or async methods in a span, using the instance's tracer |
| `OTEL_AVAILABLE` | flag | `True` when `opentelemetry` imported successfully |
| `ATTR_*` | constants | Canonical span attribute keys (`eventsource.*` plus OTEL semantic `db.*` / `messaging.*`) |

Related material lives elsewhere: see
[guides/observability.md](../guides/observability.md) for task-oriented setup.
This page documents the surface, not the workflow.

## Overview

`eventsource.observability` is a self-contained tracing layer with three parts:

1. **A tracer abstraction** -- the `Tracer` protocol plus `NullTracer`,
   `OpenTelemetryTracer`, and `MockTracer`, selected at runtime by
   `create_tracer(name, enable_tracing=True)`. Components hold a tracer by
   composition (`self._tracer = create_tracer(__name__, enable_tracing)`),
   not by inheriting a mixin.
2. **Module-level helpers** -- `get_tracer`, `should_trace`, the `@traced`
   decorator, and the `OTEL_AVAILABLE` flag. These operate on the raw
   OpenTelemetry API rather than the `Tracer` protocol.
3. **Attribute constants** -- the `ATTR_*` names that fix the exact string keys
   attached to spans, so dashboards and queries can be written against stable
   identifiers.

The two APIs coexist: `@traced` reads `self._tracer` and expects an object with
OpenTelemetry's `start_as_current_span` method, while `Tracer.span()` /
`span_with_kind()` are the protocol-level context managers used directly inside
instrumented methods.

### Optional dependency: `OTEL_AVAILABLE` and graceful degradation

OpenTelemetry is installed through the `telemetry` extra
(`opentelemetry-api` and `opentelemetry-sdk`):

```bash
pip install "eventsource-py[telemetry]"
```

At import time, `eventsource.observability.tracing` attempts
`from opentelemetry import trace` and sets `OTEL_AVAILABLE` to `True` or
`False` accordingly. This is the single source of truth for OpenTelemetry
availability across the library -- every other module reads this flag rather
than repeating the try/except.

When OpenTelemetry is absent:

- `OTEL_AVAILABLE` is `False`.
- `get_tracer(name)` returns `None`.
- `should_trace(enable_tracing)` returns `False` regardless of the argument.
- `create_tracer(name, enable_tracing=True)` returns a `NullTracer`, whose
  `span()` and `span_with_kind()` yield `None` and whose `start_span()`
  returns `None`.
- `@traced` sees a falsy `_enable_tracing` (or a `None` `_tracer`) and calls
  the wrapped function directly, adding only an attribute lookup.

Nothing in the module raises on a missing dependency, with one exception:
constructing `OpenTelemetryTracer` directly imports `opentelemetry` in its
`__init__` and will raise `ImportError` if the package is not installed. Guard
that construction with `OTEL_AVAILABLE`, or use `create_tracer`, which does the
check for you.

One consequence to respect when wiring a component: `@traced` calls
`tracer.start_as_current_span(...)`, which is the raw OpenTelemetry API and is
*not* part of the eventsource `Tracer` protocol -- `NullTracer` does not
implement it. Follow the established pattern and derive the flag from the
tracer, so a `NullTracer` never reaches that call:

```python
self._tracer = create_tracer(__name__, enable_tracing)
self._enable_tracing = self._tracer.enabled
```

Because `NullTracer.enabled` is `False`, instrumented code can also skip
computing expensive attribute dictionaries:

```python
if self._tracer.enabled:
    attrs = build_expensive_attributes()
```

### Import surface (`eventsource.observability`) vs. submodules (`.tracer`, `.tracing`, `.attributes`)

Three submodules back the package:

| Module | Contents |
| --- | --- |
| `eventsource.observability.tracer` | `Tracer`, `NullTracer`, `OpenTelemetryTracer`, `MockTracer`, `SpanKindEnum`, `create_tracer` |
| `eventsource.observability.tracing` | `OTEL_AVAILABLE`, `get_tracer`, `should_trace`, `traced` |
| `eventsource.observability.attributes` | All `ATTR_*` constants |

`tracer` depends on `tracing` (it imports `OTEL_AVAILABLE` from it); neither
depends on `attributes`. The package `__init__` re-exports every name in
`tracer.__all__` and `tracing.__all__`, so prefer the package path for those
names:

```python
from eventsource.observability import (
    OTEL_AVAILABLE,
    MockTracer,
    SpanKindEnum,
    create_tracer,
    traced,
)
```

Attribute constants are re-exported **selectively**. The read-model and query
constants defined in `attributes.py` are exported from that module's `__all__`
but are not re-exported from the package: `ATTR_READMODEL_TYPE`,
`ATTR_READMODEL_ID`, `ATTR_QUERY_FILTER_COUNT`, and `ATTR_QUERY_LIMIT`. Import
those from the submodule directly:

```python
from eventsource.observability.attributes import ATTR_READMODEL_TYPE
```

Note also that `eventsource.observability.tracer.Tracer` (the eventsource
protocol) and the `Tracer` referenced in `tracing.py` type hints
(`opentelemetry.trace.Tracer`, imported only under `TYPE_CHECKING`) are
different types with the same name. `get_tracer` returns the OpenTelemetry one;
`create_tracer` returns the eventsource one.

## Tracer Protocol

`Tracer` is a `@runtime_checkable` `Protocol` defining the contract every
tracer in the library satisfies. Components accept it by dependency injection
rather than inheriting a mixin:

```python
from eventsource.observability import NullTracer, Tracer


class MyStore:
    def __init__(self, tracer: Tracer | None = None) -> None:
        self._tracer = tracer or NullTracer()
```

Because the protocol is runtime-checkable, `isinstance(obj, Tracer)` succeeds
for any object with the four members below. Note the standard caveat: runtime
`isinstance` checks on protocols verify member *presence* only, not signatures.

The protocol has four members: two context managers (`span`,
`span_with_kind`), one manual-lifecycle method (`start_span`), and one property
(`enabled`). All three span methods accept `attributes` as an optional
`dict[str, Any]` and may yield or return `None`.

### `span(name, attributes=None) -> ContextManager[Span | None]`

The everyday entry point. Creates a span that is entered and exited with the
`with` block, always with `INTERNAL` kind.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | `str` | required | Span name, e.g. `"eventsource.repository.save"` |
| `attributes` | `dict[str, Any] \| None` | `None` | Attributes set at span creation |

Returns a context manager yielding `Span | None`. The yielded value is an
OpenTelemetry `Span` under `OpenTelemetryTracer`, and `None` under
`NullTracer` and `MockTracer` -- so any use of the yielded object must be
guarded:

```python
with self._tracer.span(
    "eventsource.repository.save",
    {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_EVENT_COUNT: len(events)},
) as span:
    position = await self._append(events)
    if span:
        span.set_attribute(ATTR_POSITION, position)
```

Per implementation:

| Implementation | Behavior of `span()` |
| --- | --- |
| `NullTracer` | `@contextlib.contextmanager` that yields `None` and does nothing else |
| `OpenTelemetryTracer` | Returns `self._tracer.start_as_current_span(name, attributes=attributes or {})` |
| `MockTracer` | Appends the `(name, attributes)` tuple to `self.spans`, then yields `None` |

Because `OpenTelemetryTracer.span()` delegates to `start_as_current_span`, the
span becomes the *current* span for the duration of the block, so any nested
spans are parented to it automatically. Exceptions propagating out of the block
are recorded and the span status set by OpenTelemetry's own context manager;
`NullTracer` and `MockTracer` add no such handling and simply let the exception
propagate.

Note that `MockTracer` stores the `attributes` argument as given -- `None` is
recorded as `None`, not normalized to `{}` the way `OpenTelemetryTracer`
normalizes it before handing it to OpenTelemetry. Assertions should match what
the instrumented code actually passed:

```python
tracer = MockTracer()
with tracer.span("operation", {"key": "value"}):
    pass
assert tracer.spans == [("operation", {"key": "value"})]
assert tracer.span_names == ["operation"]
```

### `enabled` (property)

A read-only `bool` property -- accessed as `tracer.enabled`, never called.
`True` when the tracer will create real spans.

Each implementation returns a constant; the value is fixed by the class, not by
the tracer's arguments or by runtime state:

| Implementation | `enabled` | Why |
| --- | --- | --- |
| `NullTracer` | `False` | Every method is a no-op, so there is nothing to prepare for |
| `OpenTelemetryTracer` | `True` | Spans are handed to the real OpenTelemetry API |
| `MockTracer` | `True` | So attribute-building code under an `enabled` guard still runs and can be asserted in tests |

Selection therefore happens at construction: `create_tracer(name,
enable_tracing)` returns an `OpenTelemetryTracer` when both `enable_tracing`
and `OTEL_AVAILABLE` are true, and a `NullTracer` otherwise. `enabled` is how a
component reads back which of the two it got.

The primary use is skipping work that exists only to feed a span:

```python
attrs = None
if self._tracer.enabled:
    attrs = {ATTR_EVENT_COUNT: len(events), ATTR_TENANT_ID: get_current_tenant()}
with self._tracer.span("projection.handle_batch", attrs):
    ...
```

The second use is deriving the `_enable_tracing` flag that the `@traced`
decorator reads. This is the established constructor pattern across the
library -- the memory, Redis, RabbitMQ, and Kafka buses, `Projection`, the
projection coordinator, and the checkpoint and DLQ managers all do exactly
this:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
self._enable_tracing = self._tracer.enabled
```

Deriving the flag rather than storing the constructor argument matters:
`@traced` calls `tracer.start_as_current_span(...)`, which `NullTracer` does
not implement. Passing `enable_tracing=True` on a machine without
OpenTelemetry yields a `NullTracer` whose `enabled` is `False`, and the derived
flag keeps `@traced` on its fast path.

Finally, note what `enabled` does *not* mean. It reflects the tracer's
capability, not whether an OpenTelemetry SDK with a real exporter has been
configured. `OpenTelemetryTracer.enabled` is hard-coded `True`; if no SDK
provider is installed, OpenTelemetry's own no-op tracer absorbs the spans and
nothing is exported. `enabled` being `True` guarantees that span methods do
real work, not that a trace will reach a backend.

### `start_span(name, kind=SpanKindEnum.INTERNAL, attributes=None, context=None)`

Starts a span and returns it **without** managing its lifetime. The caller owns
ending it. This is the only span method that is not a context manager.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | `str` | required | Span name, e.g. `"eventsource.event_bus.publish"` |
| `kind` | `SpanKindEnum` | `SpanKindEnum.INTERNAL` | Role of the span in the trace |
| `attributes` | `dict[str, Any] \| None` | `None` | Attributes set at creation |
| `context` | `Any \| None` | `None` | Parent OpenTelemetry context, e.g. one extracted from message headers |

Returns `Span | None`.

| Implementation | Behavior | Returns |
| --- | --- | --- |
| `NullTracer` | Does nothing | `None` |
| `OpenTelemetryTracer` | Maps `kind` to `opentelemetry.trace.SpanKind` and calls `self._tracer.start_span(name, kind=otel_kind, attributes=attributes or {}, context=context)` | the OpenTelemetry `Span` (never `None`) |
| `MockTracer` | Appends `(name, attributes)` to `self.spans` | `None` |

Note what `MockTracer` records: only the name and attributes. `kind` and
`context` are accepted and discarded, so `tracer.spans` entries from
`start_span` are indistinguishable from those recorded by `span()` and
`span_with_kind()`. Tests that must assert on span kind mock the tracer with
`unittest.mock` and inspect `start_span.call_args` rather than using
`MockTracer`.

Two properties distinguish `start_span` from the context-manager methods:

- **The span is not made current.** `OpenTelemetryTracer.start_span` calls OpenTelemetry's `start_span`, not `start_as_current_span`, so the span does not become the active span and later spans created in the same task are *not* automatically parented to it.
- **Nothing ends it.** The docstring is explicit: the caller MUST call `span.end()`. A span that is never ended is never exported.

So the required shape is `try`/`finally` with an `if span:` guard on both
branches:

```python
span = self._tracer.start_span(
    "eventsource.event_bus.publish",
    kind=SpanKindEnum.PRODUCER,
    attributes={
        ATTR_MESSAGING_SYSTEM: "rabbitmq",
        ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
        ATTR_EVENT_TYPE: event.event_type,
        ATTR_EVENT_ID: str(event.event_id),
        ATTR_AGGREGATE_ID: str(event.aggregate_id),
    },
)
try:
    message = self._create_message_with_tracing(event, span)
    await self._exchange.publish(message, routing_key=routing_key)
    if span:
        span.set_status(Status(StatusCode.OK))
finally:
    if span:
        span.end()
```

This is the shape the RabbitMQ bus uses for `publish`, `publish_batch`, and
message consumption: the `Span` object has to be reachable *while the AMQP
message is being built*, so the current trace context can be injected into the
message headers -- something no `with` block scoped around the publish call
could provide.

On the consuming side, `context` closes the loop. The bus extracts the parent
context from the incoming headers and passes it explicitly, which links the
`CONSUMER` span to the publisher's `PRODUCER` span across the process
boundary:

```python
ctx = extract(dict(headers))
span = self._tracer.start_span(
    "eventsource.event_bus.consume",
    kind=SpanKindEnum.CONSUMER,
    attributes={ATTR_MESSAGING_SYSTEM: "rabbitmq", ATTR_EVENT_TYPE: event_type_name},
    context=ctx,
)
```

Note the guard the bus puts in front of these calls:
`if self._enable_tracing and PROPAGATION_AVAILABLE:`. `context` is only useful
when `opentelemetry.propagate` is importable, which is a separate check from
`OTEL_AVAILABLE`; when propagation is unavailable, `span` stays `None` and
every `if span:` guard short-circuits.

### `span_with_kind(name, kind=SpanKindEnum.INTERNAL, attributes=None, context=None)`

The context-manager form of `start_span`: same four parameters, same semantics
for `kind` and `context`, but the span is ended when the block exits and --
under `OpenTelemetryTracer` -- is made current for its duration via
`start_as_current_span(name, context=context, kind=otel_kind, attributes=...)`.

```python
with self._tracer.span_with_kind(
    "eventsource.event_bus.publish",
    kind=SpanKindEnum.PRODUCER,
    attributes={ATTR_MESSAGING_SYSTEM: "kafka"},
) as span:
    await producer.send_and_wait(topic, payload)
    if span:
        span.set_status(Status(StatusCode.OK))
```

The Kafka bus uses this form throughout. Prefer it over `start_span` whenever
the traced work fits inside one lexical block.

### Choosing `span` vs. `start_span`/`span_with_kind`

Three questions decide it:

| Need | Use |
| --- | --- |
| Ordinary internal operation, work fits in one block | `span()` |
| Non-`INTERNAL` kind (producer/consumer/client/server) or an explicit parent `context` | `span_with_kind()` |
| Span must outlive a single block, or the `Span` object must be handed to other code (e.g. header injection) | `start_span()` + `try`/`finally` |

**Runtime attributes.** All three methods take attributes only at creation
time. Values not known until the operation completes -- a resulting stream
position, a row count, a retry count -- are set on the yielded/returned span
afterwards, under an `if span:` guard. That guard is not optional: two of the
three implementations always produce `None`.

**Distributed context propagation.** `kind` and `context` exist for crossing a
process boundary. A publisher creates a `PRODUCER` span and injects the current
trace context into the message; a consumer extracts that context from the
message headers and passes it as `context=` so its `CONSUMER` span is linked to
the publisher's. `span()` cannot express either half of that -- it accepts
neither argument -- so messaging backends use `span_with_kind` or `start_span`.
Note that the propagation helpers (`inject`/`extract`) live in
`opentelemetry.propagate`, not in this module; the bus implementations guard
their use with their own availability flag in addition to `OTEL_AVAILABLE`.
