# Observability: Wiring OpenTelemetry Tracing

Every eventsource component that does I/O — buses, snapshot stores,
repositories, projections, subscription runners, migration coordinators, and
distributed locks — accepts an `enable_tracing` flag and emits OpenTelemetry
spans when tracing is turned on. (The event store adapters -- `InMemoryEventStore`,
`SQLiteEventStore`, `PostgreSQLEventStore` -- do not currently accept
`enable_tracing`; use a traced bus or repository for the examples below.)
OpenTelemetry itself is an *optional*
dependency: without it installed, `OTEL_AVAILABLE` is `False`, every component
falls back to a `NullTracer`, and the tracing code paths cost nothing.

This guide shows you how to:

- Install the `telemetry` extra and configure a real `TracerProvider` so spans
  are actually exported rather than dropped on the floor.
- Read the three-part gate that decides whether a span is real: `OTEL_AVAILABLE`,
  `should_trace(enable_tracing)`, and `create_tracer(name, enable_tracing)`.
- Switch tracing on per component with `enable_tracing=True`.
- Instrument your own classes with the `@traced("span.name")` decorator or the
  `Tracer` protocol (`span()`, `span_with_kind()`, `start_span()`), using the
  `ATTR_*` constants from `eventsource.observability.attributes` for consistent
  attribute names.
- Propagate trace context across the event bus with PRODUCER/CONSUMER spans.
- Assert on spans in tests with `MockTracer`, `NullTracer`, and the in-memory
  exporter fixtures.

Tracing is off-by-default and fails soft by design. A missing OpenTelemetry
install, a provider configured too late, or `enable_tracing=False` all produce
the same outcome — no spans — and never an error. That makes "no spans appeared"
the normal symptom of every misconfiguration, so the troubleshooting section at
the end walks through distinguishing the causes.

Everything below imports from the top-level `eventsource.observability` package;
`eventsource.observability.tracing` and `.tracer` are the implementing modules
but you should not need to import from them directly.

## Before you start

You need:

- **Python 3.11+** and a working install of `eventsource-py`. Nothing in this
  guide requires a database or broker — the examples use `InMemoryEventBus`
  and other in-memory components so they run anywhere.
- **The `telemetry` extra**, which pulls in `opentelemetry-api>=1.0,<2.0` and
  `opentelemetry-sdk>=1.0,<2.0`. Without it `OTEL_AVAILABLE` is `False` and
  everything below silently degrades to no-ops. Installation is covered in the
  next section.
- **Somewhere for spans to go.** The SDK ships a console exporter that is enough
  to confirm wiring; anything beyond local poking needs an OTLP endpoint (a
  collector, Jaeger, Tempo, Honeycomb, and so on).
- **A component you construct yourself.** `enable_tracing` is a constructor
  argument, so you must own the call site that builds the bus, snapshot store,
  repository, or projection you want instrumented.

Two defaults are worth internalizing before you debug anything:

- `enable_tracing` defaults to **`True`** on the components that accept it —
  all four buses, the snapshot stores, `AggregateRepository`, the
  checkpoint/DLQ/outbox repositories, projections, subscription runners, the
  migration machinery, and the PostgreSQL advisory locks. You are usually
  *not* switching tracing on; you are switching the SDK on underneath it.
- `create_tracer(name, enable_tracing)` returns an `OpenTelemetryTracer` only
  when `enable_tracing and OTEL_AVAILABLE`; otherwise it returns a `NullTracer`.
  A stock install without the extra therefore behaves as though tracing were
  globally disabled, which is why "tracing is off by default" is true in
  practice even though the flag reads `True`.

Everything is imported from the top-level package:

```python
from eventsource.observability import (
    OTEL_AVAILABLE,
    MockTracer,
    NullTracer,
    OpenTelemetryTracer,
    SpanKindEnum,
    Tracer,
    create_tracer,
    get_tracer,
    should_trace,
    traced,
)
```

The `ATTR_*` constants (`ATTR_AGGREGATE_ID`, `ATTR_EVENT_TYPE`,
`ATTR_SUBSCRIPTION_NAME`, …) are re-exported from the same place, so
`from eventsource.observability import ATTR_AGGREGATE_ID` works even though they
are defined in `eventsource.observability.attributes`.

To follow the testing section you also need the repo's test suite checked out:
the in-memory exporter fixtures live in
`tests/integration/observability/conftest.py` and are not shipped as part of the
installed package. `MockTracer` and `NullTracer`, by contrast, are public API and
available to your own tests without any OpenTelemetry install at all.

## Install the telemetry extra (`eventsource-py[telemetry]` or `[all]`)

The `telemetry` extra adds exactly two packages:

```toml
telemetry = [
    "opentelemetry-api>=1.0,<2.0",
    "opentelemetry-sdk>=1.0,<2.0",
]
```

Install it alongside whatever backends you already use:

```bash
# just tracing
pip install 'eventsource-py[telemetry]'

# tracing plus a couple of backends
pip install 'eventsource-py[postgresql,redis,telemetry]'

# everything: postgresql, sqlite, redis, rabbitmq, kafka, telemetry
pip install 'eventsource-py[all]'
```

With `uv`:

```bash
uv add 'eventsource-py[telemetry]'
```

Working inside a checkout of this repo, `uv sync --all-extras` installs the
telemetry extra along with every other optional dependency, which is what the
integration tests under `tests/integration/observability/` assume.

### Why the API *and* the SDK

`opentelemetry-api` is the part eventsource itself imports — `from
opentelemetry import trace` at the top of `observability/tracing.py` is the only
import that decides `OTEL_AVAILABLE`. On its own, the API package is a set of
no-op stubs: `trace.get_tracer(...)` hands back a proxy tracer that creates
non-recording spans, so `OTEL_AVAILABLE` would be `True`, `create_tracer()` would
return a real `OpenTelemetryTracer`, and you would still see no output anywhere.

`opentelemetry-sdk` supplies the machinery that turns those calls into recorded
spans: `TracerProvider`, span processors, and the console/in-memory exporters.
Both are in the extra precisely because "API only" is the most confusing possible
state to end up in — the counters move but nothing is emitted.

### The extra does not include an exporter for your backend

The SDK ships `ConsoleSpanExporter` and `InMemorySpanExporter`, which is enough
for the local-development and testing sections of this guide. Sending spans to a
collector, Jaeger, Tempo, Honeycomb, or any other backend needs a separate
exporter distribution that eventsource deliberately does not pin for you:

```bash
pip install opentelemetry-exporter-otlp
```

Install that only when you get to
[OTLP exporter for a collector or backend](#otlp-exporter-for-a-collector-or-backend);
if `import opentelemetry.exporter.otlp` raises `ModuleNotFoundError`, this is the
package you are missing, and it is unrelated to `OTEL_AVAILABLE`.

### Verify the install

```python
from eventsource.observability import OTEL_AVAILABLE, create_tracer

print(OTEL_AVAILABLE)                                # True
print(type(create_tracer(__name__, True)).__name__)  # OpenTelemetryTracer
print(type(create_tracer(__name__, False)).__name__) # NullTracer
```

Two `True`/`OpenTelemetryTracer` lines mean the gate in `observability/tracing.py`
is open. They do **not** mean spans are being recorded — that needs a real
`TracerProvider`, which is Step 1. If the first line prints `False`, the extra did
not land in the interpreter you are running; see
[`OTEL_AVAILABLE` is False despite install](#otel_available-is-false-despite-install).

Nothing breaks if you skip the extra entirely. `OTEL_AVAILABLE` stays `False`,
every `create_tracer()` call returns a `NullTracer` regardless of `enable_tracing`,
and `@traced`-decorated methods run their bodies with no span overhead. Tracing is
additive; leaving it uninstalled is a supported configuration, not a degraded one.

## Step 1: Configure a real TracerProvider in your application

eventsource never configures OpenTelemetry for you. `OpenTelemetryTracer.__init__`
does exactly one thing:

```python
from opentelemetry import trace

self._tracer = trace.get_tracer(tracer_name)
```

`trace.get_tracer()` asks the *global* tracer provider for a tracer. Until your
application installs an SDK `TracerProvider`, that global is a
`ProxyTracerProvider` whose spans are non-recording — they are created, they cost
almost nothing, and they go nowhere. Installing a provider is the step that turns
those calls into exported spans.

Do it once, at process startup, before you construct stores, buses, or
repositories:

```python
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

provider = TracerProvider(
    resource=Resource.create({"service.name": "orders-api"}),
)
trace.set_tracer_provider(provider)
```

A provider with no span processor still records nothing useful, so the exporter
choice below is part of the same step. Two practical constraints:

- **`set_tracer_provider()` is effectively once-per-process.** A second call logs
  a warning and is ignored; the first provider wins. Put it in your application
  entrypoint (or an ASGI lifespan startup hook), not in library code, and not in
  a factory that might run twice.
- **Set the provider before you build components, and shut it down last.** Call
  `provider.shutdown()` on exit so batched spans are flushed rather than dropped.

### Console exporter for local development

The SDK's `ConsoleSpanExporter` needs no collector and no extra install — it is
the fastest way to confirm that spans are real:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor

provider = TracerProvider()
provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
trace.set_tracer_provider(provider)
```

`SimpleSpanProcessor` exports each span synchronously as it ends, so output
appears in the order operations complete — good for a terminal, bad for
throughput. `ConsoleSpanExporter` writes to `sys.stdout` unless you pass it a
different `out=` stream. Use `BatchSpanProcessor` for anything but local poking.

With that in place, any traced component starts emitting:

```python
import asyncio
from uuid import uuid4

from eventsource.adapters.memory import InMemorySnapshotStore

async def main() -> None:
    store = InMemorySnapshotStore(enable_tracing=True)
    await store.get_snapshot(uuid4(), "Order")

asyncio.run(main())
provider.shutdown()
```

That prints one JSON span named `eventsource.snapshot.get`, carrying the
`aggregate.id` and `aggregate.type` attributes the store sets plus the trace
and span IDs. Every traced component follows the same idiom — buses,
repositories, projections, and the migration machinery each emit their own
`eventsource.*` spans the same way.

`enable_tracing=True` is written out above for clarity, but it is already the
default; the line that changed the behavior is `set_tracer_provider()`. If
nothing prints, the gate is still closed somewhere — work through
[Troubleshooting](#troubleshooting).

### OTLP exporter for a collector or backend

For anything shared — a collector, Jaeger, Tempo, Honeycomb — install the
exporter distribution, which is *not* part of the `telemetry` extra:

```bash
pip install opentelemetry-exporter-otlp
```

Then swap the console exporter for OTLP and the simple processor for a batching
one:

```python
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

provider = TracerProvider(
    resource=Resource.create(
        {
            "service.name": "orders-api",
            "deployment.environment": "production",
        }
    ),
)
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
)
trace.set_tracer_provider(provider)
```

Notes that matter in production:

- **`BatchSpanProcessor` buffers.** Spans leave the process on a background
  timer, so a hard kill loses whatever is queued. Always call
  `provider.shutdown()` (or `force_flush()`) during graceful shutdown, and be
  aware that short-lived scripts frequently exit before the first flush — that is
  the most common "my spans vanished" cause outside of provider misconfiguration.
- **`service.name` is what your backend groups by.** Without a `Resource`, spans
  arrive as `unknown_service`, and a fleet of eventsource components all look
  like one anonymous service.
- **Endpoint and headers can come from the environment instead of code**
  (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`), which keeps
  per-environment configuration out of your startup module. The HTTP variant
  lives at `opentelemetry.exporter.otlp.proto.http.trace_exporter` and defaults
  to port 4318.

None of this touches eventsource. Once a recording provider is installed, every
component you construct with `enable_tracing=True` — the default — emits into it,
which is what the next steps unpack.

## Step 2: Understand the no-op default

Nothing you did in Step 1 touched eventsource. That is deliberate: the library
decides *per component instance*, at construction time, whether it holds a real
tracer or a no-op one, and it makes that decision from two inputs — whether
OpenTelemetry could be imported, and whether the caller passed
`enable_tracing=True`. Both must be true.

The decision is made once, in the constructor, and cached on the instance. There
is no runtime re-check, no global "tracing on" switch you can flip later, and no
error path — a component built before the SDK was configured simply carries a
`NullTracer` for the rest of its life. Knowing this makes every "no spans"
symptom diagnosable, so it is worth reading the three pieces in order.

### `OTEL_AVAILABLE` — the import-time gate in `observability/tracing.py`

`OTEL_AVAILABLE` is a module-level boolean set exactly once, when
`eventsource.observability.tracing` is first imported:

```python
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]
```

This is the single source of truth for OpenTelemetry availability across the
whole library. Nothing else re-runs the import: `observability/tracer.py` does
`from eventsource.observability.tracing import OTEL_AVAILABLE`, and the backends
that need their own OpenTelemetry symbols — `bus/kafka.py`, `bus/rabbitmq.py` —
import the flag from `eventsource.observability` and derive their local
`PROPAGATION_AVAILABLE` from it rather than defining a second gate. There are
unit tests (`tests/unit/bus/test_eventbus_tracing_patterns.py`,
`tests/unit/bus/test_rabbitmq_tracing.py`) that assert no module ever writes its
own `OTEL_AVAILABLE = True/False`.

Three consequences follow:

- **It is frozen for the process, and it is bound by value in every importer.**
  Installing OpenTelemetry into a running interpreter, or monkeypatching
  `sys.modules`, will not change it after the first import. More importantly for
  tests: because every consumer used `from … import OTEL_AVAILABLE`, patching
  `eventsource.observability.tracing.OTEL_AVAILABLE` does *not* affect
  `create_tracer`. To exercise the "not installed" branch you must patch the
  name in the module that reads it — `eventsource.observability.tracer` for
  `create_tracer`, `eventsource.adapters.kafka` for the Kafka bus, and so on:

  ```python
  from unittest.mock import patch

  from eventsource.observability import tracer as tracer_module

  with patch.object(tracer_module, "OTEL_AVAILABLE", False):
      ...  # create_tracer() now returns NullTracer
  ```

- **It only reflects `opentelemetry-api`.** The `import opentelemetry` above
  succeeds with the API package alone; `opentelemetry-sdk` is never imported by
  eventsource. `OTEL_AVAILABLE is True` therefore means "spans can be created",
  not "spans will be recorded" — the recording half is the `TracerProvider` from
  Step 1.
- **It gates `get_tracer()` too.** `get_tracer(name)` returns
  `trace.get_tracer(name)` when the flag is set and `None` otherwise, so
  low-level callers get a `Tracer | None` they must check. Most code should use
  `create_tracer()` instead, which never returns `None`.

### `should_trace(enable_tracing)` — combining component config with availability

`should_trace` is a one-line helper that ANDs the per-component flag with the
import-time gate:

```python
def should_trace(enable_tracing: bool) -> bool:
    return enable_tracing and OTEL_AVAILABLE
```

That is the whole predicate, and it is the rule the rest of the library follows:
a component traces only when *its own* configuration asks for it **and**
OpenTelemetry is importable. Neither input can override the other.

Use it in your own code when you want to skip work that only exists to feed a
span — building an attribute dict, serializing an ID, counting a collection:

```python
from eventsource.observability import should_trace

if should_trace(self._enable_tracing):
    attributes = {ATTR_EVENT_COUNT: len(events)}
else:
    attributes = None
```

For that specific guard-before-you-compute pattern the `Tracer.enabled` property
is usually the better choice — `tracer.enabled` is `False` on `NullTracer` and
`True` on `OpenTelemetryTracer`, so it answers the same question without you
having to carry the boolean around. `should_trace` remains useful where you have
a raw `enable_tracing` argument in hand and no tracer object yet, such as inside
a factory or a constructor before the tracer is built.

### `create_tracer(name, enable_tracing)` — returns `OpenTelemetryTracer` or `NullTracer`

`create_tracer` is the factory every component actually calls. It applies the
same predicate and returns a concrete tracer:

```python
def create_tracer(name: str, enable_tracing: bool = True) -> Tracer:
    if enable_tracing and OTEL_AVAILABLE:
        return OpenTelemetryTracer(name)
    return NullTracer()
```

It **always returns a `Tracer`** — never `None`. That is the point of the
no-op default: call sites never branch on whether tracing exists, they just call
`self._tracer.span(...)` and let the implementation decide whether anything
happens. `NullTracer.span()` and `.span_with_kind()` are `@contextlib.contextmanager`
functions that yield `None`, and `NullTracer.start_span()` returns `None`, so
the disabled path costs one generator and nothing else.

The `name` argument becomes the OpenTelemetry *instrumentation scope* name via
`trace.get_tracer(tracer_name)`. Components pass their own module's `__name__`,
so spans from the in-memory snapshot store are scoped to
`eventsource.adapters.memory.snapshots`, spans from the in-memory event bus to
`eventsource.adapters.memory.bus`, and so on. This is the scope your backend shows
as the instrumentation library — it is not the span name.

Every instrumented component follows the identical two-line idiom in its
constructor. From `adapters/memory/snapshots.py`:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
self._enable_tracing = self._tracer.enabled
```

Read that second line carefully, because it explains most of the library's
behavior:

- The constructors also accept an explicit `tracer:` argument. When you pass
  one, `enable_tracing` is **ignored entirely** — your tracer is used as-is.
  This is the injection point for `MockTracer` in tests.
- `_enable_tracing` is *not* the argument you passed in. It is
  `tracer.enabled`, the resolved answer. Construct
  `InMemorySnapshotStore(enable_tracing=True)` without OpenTelemetry installed
  and `store._enable_tracing` is `False`, because `create_tracer` handed back a
  `NullTracer` whose `enabled` property is hard-coded `False`. The component
  normalizes your request against reality rather than storing your request.
- The resolution happens **at construction**. A component built before
  `trace.set_tracer_provider()` runs still gets an `OpenTelemetryTracer` (that
  only depends on `OTEL_AVAILABLE`), but its tracer was obtained from the proxy
  provider and may never emit. Order matters; see
  [No spans appear](#no-spans-appear-proxytracerprovider--provider-set-after-component-construction).

Two tracers exist beyond these. `MockTracer` records `(name, attributes)` tuples
into a `spans` list and reports `enabled` as `True` — it needs no OpenTelemetry
at all, which is why it is the default tool for unit tests (Step 6).
`OpenTelemetryTracer` can also be constructed directly, but it imports
`opentelemetry` in `__init__` and will raise `ImportError` if the extra is
missing; `create_tracer` exists so you never have to guard that yourself.

You can see the whole gate resolve in three lines:

```python
from eventsource.observability import OTEL_AVAILABLE, create_tracer, should_trace

print(OTEL_AVAILABLE)                                    # depends on your install
print(should_trace(True))                                # same value as above
print(type(create_tracer(__name__, True)).__name__)      # OpenTelemetryTracer | NullTracer
print(create_tracer(__name__, False).enabled)            # always False
```

With that model in place, Step 3 is just a matter of knowing which constructors
take the flag.
