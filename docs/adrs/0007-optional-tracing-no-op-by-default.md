# ADR-0007: Optional Tracing, No-Op by Default

Every durable component in this library can emit OpenTelemetry spans -- event stores, event buses, aggregate repositories, snapshot stores, projections, read models, distributed locks, and the live-migration tooling all accept a `tracer` and an `enable_tracing` flag. None of them require OpenTelemetry to be installed, and none of them pay for tracing when it is switched off.

This ADR records how that is arranged: OpenTelemetry lives behind the `telemetry` extra, a single `OTEL_AVAILABLE` probe in `eventsource/observability/tracing.py` is the only place the import is attempted, and `NullTracer` -- a null object satisfying the same `Tracer` protocol as `OpenTelemetryTracer` -- absorbs the disabled case so that call sites contain a plain `with self._tracer.span(...)` and no conditional. It also records the seams that fell out of that choice (`MockTracer`, `SpanKindEnum`, the `eventsource.*` attribute catalogue) and the asymmetries the design deliberately tolerates, chiefly that `enable_tracing` does not default the same way in every subsystem.

## Status

**Status:** Accepted

**Date:** 2026-07-27 (records a decision in force since 0.5.0)

**Deciders:** Library maintainers (architecture owner, observability owner)

The arrangement is fully in effect in the current tree. `opentelemetry-api` and
`opentelemetry-sdk` appear only under the `telemetry` extra in `pyproject.toml`
(and transitively in the `all` extra); neither is a runtime dependency of a
plain `pip install eventsource-py`. `OTEL_AVAILABLE`, `NullTracer`,
`OpenTelemetryTracer`, `MockTracer`, `SpanKindEnum`, and `create_tracer` are all
exported from `eventsource.observability`, and the components listed above take
`tracer` / `enable_tracing` through their constructors.

One part of the decision is only partly landed. Item 4 (composition through the
`Tracer` protocol) replaced the earlier inheritance-based `TracingMixin`, which
was removed in commit `0f70002`; call sites that used to inherit it now hold a
tracer and carry a `# Composition-based tracing (replaces TracingMixin)`
comment. The older `traced` decorator in `observability/tracing.py` survives
alongside the protocol and still reads `_tracer` / `_enable_tracing` attributes
off `self`, so both styles coexist. That is recorded below as an accepted
asymmetry rather than as unfinished work; superseding the decorator would be a
separate decision.

No ADR supersedes this one. It should be revisited if the `traced` decorator is
retired, if metrics (currently guarded separately in
`subscriptions/metrics.py`) are folded into the same protocol, or if
`enable_tracing` defaults are ever unified across subsystems.

## Context

### OpenTelemetry is a heavy dependency for a library with a two-package core

`pyproject.toml` declares exactly two runtime dependencies: `pydantic>=2.0,<3.0`
and `sqlalchemy>=2.0,<3.0`. Everything else -- asyncpg (`postgresql`), aiosqlite
(`sqlite`), redis (`redis`), aio-pika (`rabbitmq`), aiokafka (`kafka`), and
OpenTelemetry (`telemetry`) -- sits behind a named extra, with an `all` extra
that pulls the set together.

Tracing is not a domain-modelling concern, so making OpenTelemetry mandatory
would be an odd third entry in that two-package core. The `telemetry` extra is
also not one package but two -- `opentelemetry-api>=1.0,<2.0` and
`opentelemetry-sdk>=1.0,<2.0` -- because a user who wants spans actually
exported needs the SDK, not just the API. Those two version in lockstep, and a
library that hard-pins them is a recurring source of resolver conflicts in
applications that already carry their own observability stack, frequently a
different OTel distribution or an auto-instrumentation bundle. A library should
not get a vote in that argument.

The `pyproject.toml` mypy configuration reflects the same stance: there is an
override for `module = "opentelemetry.*"` so type checking still succeeds in an
environment where the package is absent.

At the same time, spans are the difference between a usable and an unusable
event-sourced system in production. Append/publish/project is a distributed
pipeline, and "which command produced this projection lag" is a trace question.
So the library cannot simply decline to instrument; it has to instrument
thoroughly while remaining installable without the instrumentation stack.

### Tracing call sites are hot paths (`append_events`, `publish`, `project`)

Tracing is not sprinkled lightly. The current tree constructs a tracer at 58
sites via `create_tracer(...)` and opens spans at 274 `self._tracer.span(...)` /
`.span_with_kind(...)` / `.start_span(...)` call sites spread over 46 modules.
The distribution is lopsided toward the machinery that runs per event:
`migration/` accounts for 83, `repositories/` (checkpoint, DLQ, outbox) for 69,
`readmodels/` for 43, then `bus/`, `snapshots/`, and `subscriptions/` at 16
each, `projections/` at 11, `stores/` at 8, `aggregates/` at 7, `locks/` at 2.

Those are not startup paths. `PostgreSQLEventStore.append_events` opens its span
before doing any work and delegates the body to `_do_append_events`; every
`EventBus.publish` implementation wraps the send; `Projection.handle` opens
`eventsource.projection.handle` per event and then opens a second
`eventsource.projection.handler` span per dispatched handler. A single command
that appends three events and fans out to four projections therefore crosses a
double-digit number of these call sites.

That volume creates two pressures, and they point in opposite directions.

First, whatever guard protects a call site is paid on every event, so it has to
be cheap in the *disabled* case -- which is the overwhelmingly common case, since
most installs will not have the `telemetry` extra at all. The cost that matters
is not span creation (there is none when tracing is off) but the guard itself
plus the argument evaluation that happens before the guard can run. Note the
shape of the store call site: the attribute dict, including
`",".join(type(e).__name__ for e in events)`, is built as an argument and is
therefore evaluated *before* `span()` is entered, no matter what `span()` then
decides to do. Any design where the disabled path still constructs those
dictionaries loses on a hot loop.

Second, whatever the guard *looks like* is repeated 274 times, so it has to be
short. A per-call-site `try/except ImportError`, or
`if self._tracer is not None and self._enable_tracing:` wrapping every span,
would become the most-repeated construct in the codebase; on methods whose
traced body is one delegating line, it would dominate the method. It would also
be 274 chances to get the condition subtly wrong in one backend and silently
lose instrumentation there.

These two pressures are why the design needs both a null object (so the common
call site is an unconditional `with self._tracer.span(...)`, no branch, no
import guard) *and* a cheap `enabled` predicate (so the handful of sites with
genuinely expensive attribute computation or context propagation can skip the
work). The codebase uses the second sparingly and deliberately: components cache
`self._enable_tracing = self._tracer.enabled` once in `__init__` rather than
re-querying the property, and only eleven sites branch on it -- concentrated in
`bus/kafka.py` and `bus/rabbitmq.py`, where the guard also has to cover
`PROPAGATION_AVAILABLE` before extracting or injecting W3C trace context from
message headers.

### The existing `traced` decorator requires `_tracer` / `_enable_tracing` attributes on the class

The first tracing mechanism in the library is the `traced(name, attributes=...)`
decorator in `observability/tracing.py`, alongside two helpers from the same
module: `get_tracer(name)`, which returns an OTEL tracer or `None`, and
`should_trace(enable_tracing)`, which is `enable_tracing and OTEL_AVAILABLE`.
`traced` builds a sync or an async wrapper, picks between them with
`asyncio.iscoroutinefunction(func)`, and at call time reads state off the
instance:

```python
tracer = getattr(self, "_tracer", None)
enable_tracing = getattr(self, "_enable_tracing", False)

if not enable_tracing or tracer is None:
    return func(self, *args, **kwargs)

with tracer.start_as_current_span(name, attributes=attributes or {}):
    return func(self, *args, **kwargs)
```

That contract -- "the decorated method's class must have `_tracer` and
`_enable_tracing` attributes" -- is the constraint this section is about, and it
is what kept the decorator from becoming the library's tracing mechanism.

It has three problems, in increasing order of severity.

The contract is implicit and unenforced. `getattr` with defaults of `None` and
`False` means a class that never sets `_enable_tracing` does not fail, does not
warn, and simply never traces. There is no type, no protocol, and no
construction-time check that would catch the omission; the only symptom is
missing spans in production, discovered by whoever goes looking for them. The
defaults are chosen to fail safe, which is correct behaviour and also exactly
what makes the failure invisible.

Attributes are static. `attributes` is bound at decoration time and passed
straight through to `start_as_current_span`, so the decorator can express
`attributes={"db.system": "sqlite"}` but nothing that depends on the call.
Almost every span in this library wants the opposite: the aggregate id, the
stream name, the event count, the expected version, the tenant, the topic. The
docstring concedes the point and redirects the reader to a mixin method for
"dynamic attributes that depend on method arguments" -- a method that no longer
exists (see below). A decorator that cannot see the arguments cannot carry the
attributes that make a span worth reading.

It presumed inheritance. The `_tracer` / `_enable_tracing` pair was originally
supplied by a `TracingMixin` base class, which the docstrings still reference.
That put tracing in the MRO of every store, bus, and projection -- a
cross-cutting concern occupying a base-class slot in classes that already have
real hierarchies (`EventStore` implementations, `DeclarativeProjection`), and
one that cannot be swapped per instance for a test.

The decorator's present status is worth stating plainly, because it bears on
what the Decision does *not* do: `@traced` has no production call sites. Every
occurrence in the tree is a docstring example (in `observability/__init__.py`
and `tracing.py`) or a test (`tests/unit/observability/test_tracing.py`,
which covers enabled, disabled, missing-attribute, sync, async, and
static-attribute cases). It remains exported from `eventsource.observability`
and remains supported; it is simply not how the library traces itself. The
`TracingMixin` it was designed around is gone, replaced by composition, and the
call sites that used it now hold a tracer object -- as their
`# Composition-based tracing (replaces TracingMixin)` comments record, in
`bus/memory.py`, `projections/base.py`, `projections/coordinator.py`,
`subscriptions/manager.py`, `migration/dual_write.py`, and elsewhere.

So the question the Decision has to answer is not "how do we make the decorator
cheaper" but "what replaces an implicit attribute contract that cannot carry
dynamic attributes" -- while keeping the disabled path free at the 274 span
sites counted above.

### Prior art in the codebase: the `*_AVAILABLE` optional-dependency pattern

The library already had an established way to handle an optional backend:
attempt the import at module scope inside `try/except ImportError`, set a
module-level `*_AVAILABLE` boolean, and rebind the missing names to `None` (or
to a harmless stand-in) so the module still imports. The tree currently has
eight such flags -- `KAFKA_AVAILABLE`, `RABBITMQ_AVAILABLE`, `REDIS_AVAILABLE`,
`SQLITE_AVAILABLE` (three separate copies, in `__init__.py`, `snapshots/`, and
`stores/` as `_SQLITE_AVAILABLE`), `OTEL_METRICS_AVAILABLE` (in
`subscriptions/metrics.py`, `subscriptions/shutdown.py`, and
`migration/metrics.py`), `PROPAGATION_AVAILABLE`, and `OTEL_AVAILABLE`.

`observability/tracing.py` follows exactly that shape, and labels itself as the
canonical instance:

```python
# Optional OpenTelemetry import - single source of truth
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]
```

Reusing the pattern kept tracing consistent with the rest of the optional
surface, gave the probe one home, and made it exportable -- `OTEL_AVAILABLE` is
re-exported from `eventsource.observability` and imported from there by
`observability/tracer.py`, `bus/kafka.py`, and `bus/rabbitmq.py`.

But the backend pattern stops where tracing has to keep going, and the
difference is the *failure policy* attached to a false flag.

For a backend, `*_AVAILABLE == False` is a fatal, local, construction-time
error. `SQLiteSnapshotStore.__init__` opens with `if not SQLITE_AVAILABLE: raise
SQLiteNotAvailableError()`; `KafkaEventBus.__init__` does the same with
`KafkaNotAvailableError`. Both exceptions subclass `ImportError` and carry the
`pip install` hint. There is one check, in one constructor, and the user asked
for that backend by name -- so failing loudly is the right answer, and the flag
is consulted exactly once per class.

For tracing, `OTEL_AVAILABLE == False` must be *non-fatal, global, and silent*.
Nobody asked for tracing by naming a class; they got a `PostgreSQLEventStore`
that happens to want to emit spans. Raising would make the `telemetry` extra a
de-facto core dependency, defeating the point. And the check is not needed once
in one constructor but at the 274 span sites counted above, in 46 modules across
every subsystem -- so the check cannot be spelled out at the point of use the way
`if not KAFKA_AVAILABLE: raise` can.

The two other OTel-adjacent flags show what the unextended pattern degenerates
into when a subsystem does its own guarding. `OTEL_METRICS_AVAILABLE` appears
three times, once per metrics module, because metrics have no null object; each
site pays its own `if not OTEL_AVAILABLE: return None` (see `_get_meter` in
`bus/kafka.py`). And `PROPAGATION_AVAILABLE` in `bus/kafka.py` and
`bus/rabbitmq.py` is a second import guard over `opentelemetry.propagate` and
`opentelemetry.metrics`, defined as `PROPAGATION_AVAILABLE = OTEL_AVAILABLE`
inside its own `try` -- a flag deriving from the canonical one because those
modules need symbols (`inject`, `extract`, `SpanKind`) that the tracing facade
does not expose. Those are the parts of the codebase where the raw pattern is
still visible, and they are conspicuously the least pleasant to read.

So the prior art supplies the availability *probe* and nothing more. The
mismatch it leaves open -- one flag, but a "raise immediately" policy for
backends versus a "degrade invisibly, everywhere, for free" policy for tracing
-- is the specific problem the rest of this ADR resolves, and it is why the
decision below adds a null object on top of the pattern rather than simply
adopting the pattern again.

## Decision

Tracing is provided through a **null object behind a Protocol, constructed by a
factory, gated by a per-component flag**. OpenTelemetry stays in the `telemetry`
extra; the disabled case is represented by an object rather than by a branch;
and the object is injected by composition so it can be swapped per instance.

The nine numbered items below are the whole of the decision. Items 1--3 solve
the availability/cost problem stated in the Context; items 4--7 fix the seams
the `traced` decorator could not; items 8--9 keep the OpenTelemetry vocabulary
from leaking into calling code.

### 1. OpenTelemetry ships as the `telemetry` extra, never a core dependency

`opentelemetry-api>=1.0,<2.0` and `opentelemetry-sdk>=1.0,<2.0` are declared
under `[project.optional-dependencies] telemetry` and are reachable only through
that extra or through `all`, which composes it. The runtime dependency list
stays at pydantic and sqlalchemy. `pip install eventsource-py` gets a fully
functional, fully instrumented-in-source library that emits nothing; `pip
install eventsource-py[telemetry]` turns the same code into a span emitter with
no source change.

The corollary is that the library never raises because OpenTelemetry is missing.
`OpenTelemetryTracer.__init__` does import `opentelemetry` and will raise
`ImportError` if it is absent, but nothing constructs it unless `OTEL_AVAILABLE`
is already true (item 5). This is the opposite policy from the optional
*backends*, where `KafkaEventBus.__init__` deliberately raises
`KafkaNotAvailableError`: a user who names `KafkaEventBus` asked for Kafka,
whereas nobody asks for tracing by naming `PostgreSQLEventStore`.

The mypy configuration completes the arrangement with an
`ignore_missing_imports = true` override for `module = "opentelemetry.*"`, so
`mypy src/eventsource/` passes in an environment without the extra installed.

### 2. A single `OTEL_AVAILABLE` probe in `observability/tracing.py` is the only import guard

`observability/tracing.py` performs the import once, at module scope, and
comments itself as the "single source of truth":

```python
try:
    from opentelemetry import trace

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]
```

Every other module that needs to know imports the flag rather than re-attempting
the import: `observability/tracer.py` imports it to implement `create_tracer`,
and `eventsource.observability` re-exports it as public API.

Two caveats, stated here because the rule is narrower than its heading suggests.

The probe is the only import guard *for tracing*, not for everything OTel. Two
other flags exist for capabilities the tracing facade does not cover.
`PROPAGATION_AVAILABLE`, in `bus/kafka.py` and `bus/rabbitmq.py`, guards
`opentelemetry.propagate` (`inject` / `extract`) for W3C context propagation
through message headers and is defined as `PROPAGATION_AVAILABLE = OTEL_AVAILABLE`
inside its own `try`. `OTEL_METRICS_AVAILABLE`, in `subscriptions/metrics.py`,
`subscriptions/shutdown.py`, and `migration/metrics.py`, guards
`opentelemetry.metrics`; metrics have no null object, so each of those modules
pays its own `if not ...: return None`. Both are deliberate: this ADR governs
spans, and extending the null-object treatment to metrics and propagation is a
separate decision.

Second, the flag reports *installability*, not configuration. `OTEL_AVAILABLE`
being true says the package imports, not that an SDK, exporter, or collector is
wired up. That distinction resurfaces in `OpenTelemetryTracer.enabled` (see
Consequences).

### 3. `NullTracer` as a null object instead of `try/except` or `if tracer is not None` at each call site

`NullTracer` implements the full `Tracer` surface and does nothing: `span()` and
`span_with_kind()` are `@contextlib.contextmanager` generators whose entire body
is `yield None`, `start_span()` returns `None`, and `enabled` returns `False`.
It is never absent and never `None`, so the 274 span sites counted in the
Context are written unconditionally:

```python
with self._tracer.span("eventsource.event_store.append_events", {...}):
    await self._do_append_events(...)
```

No `try`, no `if`, no `getattr`. Whether the process has OpenTelemetry installed,
has it installed but disabled for this component, or is emitting real spans, the
call site is the same text — which is what makes it tolerable to repeat 274
times across 46 modules, and what removes 274 chances to get a per-site
condition subtly wrong in one backend.

The cost in the disabled case is a generator-based context manager per span,
plus evaluation of the attribute dictionary that was passed as an argument.
Building the dict is *not* avoided by the null object, since arguments are
evaluated before the call. That residual cost is what item 6 and the `enabled`
predicate exist to address at the handful of sites where the attributes are
genuinely expensive; everywhere else, a dict literal on the event path is
accepted as the price of a branch-free call site.

`NullTracer` is also the default fallback everywhere a tracer is optional
(`tracer or create_tracer(...)`, and `tracer or NullTracer()` in the documented
component pattern), so a component can never hold `None` where a tracer belongs.

### 4. `Tracer` is a `runtime_checkable` Protocol, injected by composition

`Tracer` is a `@runtime_checkable` `Protocol` in `observability/tracer.py` with
four members: `span(name, attributes=None)`, the `enabled` property,
`start_span(name, kind, attributes=None, context=None)`, and
`span_with_kind(name, kind, attributes=None, context=None)`. `NullTracer`,
`OpenTelemetryTracer`, and `MockTracer` satisfy it structurally — none of them
inherit from it.

Components take a tracer as a constructor parameter, not a base class:

```python
def __init__(self, ..., tracer: Tracer | None = None, enable_tracing: bool = True):
    self._tracer = tracer or create_tracer(__name__, enable_tracing)
    self._enable_tracing = self._tracer.enabled
```

That signature — optional `tracer`, plus `enable_tracing` — is the standard shape
across stores, buses, snapshot stores, repositories, projections, read models,
locks, subscriptions, and the migration package. It replaces the earlier
`TracingMixin`, removed in `0f70002`; the sites that inherited it now carry a
`# Composition-based tracing (replaces TracingMixin)` comment. Tracing is out of
the MRO of every `EventStore`, `EventBus`, and `DeclarativeProjection`, and a
tracer can be swapped per instance rather than per class.

`runtime_checkable` makes `isinstance(x, Tracer)` legal for callers assembling
components dynamically. It only checks member presence, not signatures — an
accepted limit of Python protocols, and the reason the type annotation, not the
isinstance check, is the real contract.

### 5. `create_tracer(name, enable_tracing)` is the single construction path

```python
def create_tracer(name: str, enable_tracing: bool = True) -> Tracer:
    if enable_tracing and OTEL_AVAILABLE:
        return OpenTelemetryTracer(name)
    return NullTracer()
```

Both conditions in one place, evaluated once per component at construction. This
is why `OpenTelemetryTracer.__init__`'s `from opentelemetry import trace` can
never fire an `ImportError` in normal use, and why no component ever spells out
`if OTEL_AVAILABLE` itself. The return type is the `Tracer` protocol, so callers
cannot depend on which implementation they received.

`name` is conventionally `__name__`, so spans are attributed to the module that
emitted them. `enable_tracing` defaults to `True` at the factory: the factory's
default is "trace if you can", and any component that wants a different policy
sets its own parameter default (item 6).

An explicitly passed `tracer` bypasses the factory entirely — that is the seam
item 7 relies on.

### 6. Per-component `enable_tracing` flags gate tracing independently of OTEL availability

There is no global tracing switch. Each traced component exposes its own
`enable_tracing: bool` constructor parameter, and the two gates are orthogonal:
`OTEL_AVAILABLE` answers "can this process trace at all", `enable_tracing`
answers "should *this* component trace". Turning tracing off for one noisy
projection while leaving the event store instrumented requires no global state
and no environment variable, which also means two stores in the same process can
disagree — useful in tests and in multi-tenant hosts.

Components cache the resolved answer once: `self._enable_tracing =
self._tracer.enabled`, read from the tracer rather than from the constructor
argument, so the cached flag reflects availability as well as intent. That
cached boolean is the cheap predicate the hot-path sites use before computing
expensive attributes or doing context propagation — around eleven sites,
concentrated in `bus/kafka.py` and `bus/rabbitmq.py` where the guard must also
cover `PROPAGATION_AVAILABLE`.

The defaults are deliberately *not* uniform. `True` in stores, buses,
aggregates, snapshots, repositories, locks, subscriptions, and migration; `False`
in `projections/base.py`, `projections/coordinator.py`,
`projections/dlq_manager.py`, `projections/checkpoint_manager.py`, and
`readmodels/projection.py`. Projection code fans out per event per projection
and is the highest-volume span producer in the library, so it opts in. This is a
real inconsistency in the public API and is recorded as such under Consequences
rather than defended as elegant.

### 7. `MockTracer` is the supported test seam

`MockTracer` ships in `observability/tracer.py` and is exported from
`eventsource.observability` as public API — not a test fixture that happens to
be importable. It satisfies `Tracer`, appends `(name, attributes)` to a
`self.spans` list from all three span methods, exposes `span_names` for concise
assertions and `clear()` for reuse, and reports `enabled` as `True`.

Because tracers are injected (item 4), a test asserts on tracing by passing one
in:

```python
tracer = MockTracer()
store = PostgreSQLEventStore(..., tracer=tracer)
await store.append_events(...)
assert "eventsource.event_store.append_events" in tracer.span_names
```

No OpenTelemetry install, no SDK, no in-memory exporter, no monkeypatching of
`OTEL_AVAILABLE`. This is what per-backend tracing suites use
(`tests/unit/test_redis_event_bus_tracing.py`,
`tests/unit/snapshots/test_snapshot_store_tracing.py`,
`tests/unit/aggregates/test_repository_tracing.py`,
`tests/unit/repositories/test_checkpoint_tracing.py`, and
`tests/unit/observability/test_tracer.py`).

`enabled` returning `True` is the load-bearing detail: it makes tests take the
same branches production takes with tracing on, so attribute-computation code
guarded by `self._enable_tracing` is actually exercised. Its cost — that
`MockTracer` is not a faithful model of a *disabled* tracer — is why `NullTracer`
is separately available for tests that want the off path.

### 8. `SpanKindEnum` mirrors OTEL `SpanKind` so callers never import `opentelemetry`

`SpanKindEnum` is a plain `Enum` with `INTERNAL`, `PRODUCER`, `CONSUMER`,
`CLIENT`, and `SERVER` — the same five kinds OpenTelemetry defines, as
lowercase-string values. It is exported from `eventsource.observability` and is
what `start_span` and `span_with_kind` accept.

The translation happens in one place, inside `OpenTelemetryTracer`, which maps
the enum to `opentelemetry.trace.SpanKind` through a dict lookup with
`OtelSpanKind.INTERNAL` as the fallback, importing OTel *inside* the method
rather than at module scope. A caller that wants a PRODUCER span for a RabbitMQ
publish writes `kind=SpanKindEnum.PRODUCER` and imports nothing from
OpenTelemetry — which is required, not merely tidy, since in a `telemetry`-less
install that import would fail.

The mirror is intentionally lossy: it covers span *kind* and nothing else. Span
status, links, and events are still expressed by calling methods on the yielded
span object, which is `None` when tracing is off, so those call sites carry the
`if span:` guard the null object cannot remove.

### 9. Attribute naming: `eventsource.*` for domain concepts, OTEL semantic conventions for `db.*` and `messaging.*`

Every attribute key is a named constant in `observability/attributes.py`, and
the module splits on a single rule: **if OpenTelemetry has already standardized
the concept, use its key; otherwise namespace under `eventsource.`**

Standardized keys are reused verbatim — `ATTR_DB_SYSTEM = "db.system"`,
`ATTR_DB_NAME = "db.name"`, `ATTR_DB_OPERATION = "db.operation"`,
`ATTR_MESSAGING_SYSTEM = "messaging.system"`, `ATTR_MESSAGING_DESTINATION =
"messaging.destination"`, `ATTR_MESSAGING_OPERATION = "messaging.operation"` —
so that existing APM backends light up their database and messaging views for a
`PostgreSQLEventStore` or a `KafkaEventBus` with no per-user mapping config.

Everything the specification has no opinion about is prefixed:
`eventsource.aggregate.id`, `eventsource.aggregate.type`, `eventsource.event.id`,
`eventsource.event.type`, `eventsource.event.count`, `eventsource.version`,
`eventsource.expected_version`, plus the lock, migration, projection, handler,
and tenant families. The prefix guarantees no collision with a future
`db.*`/`messaging.*` convention or with an application's own attributes.

Constants rather than string literals is the second half of the rule: a renamed
key is one edit, dashboards break loudly at import time rather than silently at
query time, and the constant list in `attributes.py` doubles as the catalogue of
what this library will put on a span.
