# Code Structure

## Why this document exists

This document explains how the `eventsource` package is laid out and, more importantly, *why*
it is laid out that way. It is background reading for contributors and for anyone trying to
locate the right seam before changing behavior.

It is an explanation, not a task guide. Nothing here is a step-by-step recipe, no snippet is
meant to be copied into an application as-is, and the goal is understanding rather than a
finished result. If you want to *do* something — configure snapshots, wire up tracing, write a
projection — the how-to guides and the API reference are the right places; they tell you which
calls to make. This document tells you why those calls live where they do, which is what you
need when the guides run out because you are the one changing the code.

The layout of `src/eventsource/` is not accidental, and several of its boundaries only make
sense historically. `AggregateRepository` once contained the snapshot lifecycle, the
background-task bookkeeping, and its own tracing behavior inherited from a `TracingMixin`. All
three now live beside it as separate collaborators. Reading only the current code, those splits
look like extra indirection; reading the rationale, they are the difference between a class that
answers one question and a class that answers four. Contributors who do not know the reasoning
tend to undo it — folding a small helper back into its caller, adding a branch where a strategy
was intended, reaching for a mixin because it is fewer lines today.

So the recurring shape of each part below is: what a module owns, what it deliberately does not
own, and what forced that line. Where a test exists specifically to keep a boundary from eroding,
it is named, because a structural argument that nothing enforces is just a preference.

The running example throughout is the `aggregates` package, because it is where these principles
were applied most recently and most visibly. The same pattern — a coordinator, named
collaborators, an injected tracer — repeats in `projections/`, `subscriptions/`, `bus/`, and
`migration/`, and the closing sections come back to that.

## How the package is organized

`src/eventsource/` is a flat set of packages, each one named for the concept it owns. There is no
`core/`, `common/`, or `utils/` bucket, and no layered `infrastructure/` tree — if you can name the
thing, you can find its directory.

| Package | Owns |
| --- | --- |
| `aggregates/` | `AggregateRoot`, `DeclarativeAggregate`, `AggregateRepository`, plus the snapshot and background-task collaborators |
| `events/` | `DomainEvent` (`base.py`) and the `EventRegistry` (`registry.py`) |
| `handlers/` | The `@handles` decorator, its registry, and the sync/async handler adapter |
| `stores/` | `EventStore` interface plus PostgreSQL / SQLite / in-memory implementations |
| `bus/` | `EventBus` interface plus in-memory, Redis, RabbitMQ, and Kafka backends |
| `snapshots/` | `SnapshotStore` interface, three backends, and the snapshot *strategies* |
| `projections/` | Projection base, coordinator, checkpoint manager, DLQ manager, retry |
| `readmodels/` | Read-model projections, query surface, schema, and per-backend repositories |
| `subscriptions/` | Subscription lifecycle: manager, `runners/`, retry, health, flow control, pause/resume, shutdown |
| `repositories/` | Checkpoint, DLQ, and outbox persistence |
| `migration/` | Live event-store migration: dual write, routing, cutover, consistency, position mapping |
| `migrations/` | SQL schema files (`schemas/`, `updates/`, `templates/`) — append-only |
| `observability/` | `Tracer` protocol, tracer implementations, standard span attribute constants |
| `multitenancy/` | Tenant context vars, tenant-scoped events and repository |
| `locks/`, `serialization/`, `sync/`, `config.py` | Cross-cutting support: advisory locks, JSON encoding, the sync adapter, configuration |
| `testing/` | Assertions, BDD helpers, builders, the harness, and the backend conformance suites |
| `protocols.py`, `types.py`, `exceptions.py` | Shared vocabulary: canonical contracts, type aliases, every exception type |

Three properties of this map are worth naming, because they are choices rather than accidents.

**Backends live next to the interface they implement.** `stores/postgresql.py`,
`stores/sqlite.py`, and `stores/in_memory.py` all sit beside `stores/interface.py`; the same shape
holds in `bus/`, `snapshots/`, and `readmodels/`. Reading one directory therefore tells you both
what the contract is and how many ways it has been satisfied — and it makes an interface change
impossible to ship without seeing every implementation it breaks. An `infrastructure/` package
once held these; it was deleted precisely because it hid that coupling behind a directory boundary.

**Shared vocabulary is centralized; shared behavior is not.** `protocols.py`, `types.py`, and
`exceptions.py` are the only genuinely global modules, and they contain declarations only —
`EventHandler`, `EventSubscriber`, `AggregateId`, `OptimisticLockError`. Nothing importable from
them does work. Behavior that several packages need arrives as an injected collaborator from its
own package (a `Tracer` from `observability/`, a `SnapshotStrategy` from `snapshots/`) rather than
as a utility import, which is what keeps the dependency graph from turning into a mesh.

**Two similarly-named directories mean different things.** `migration/` (singular) is runtime
Python for moving a live system between event stores. `migrations/` (plural) is SQL DDL. They are
unrelated, and `migrations/` is append-only by design: existing schema files are never edited,
only added to, because deployed databases have already run them.

The top-level `__init__.py` re-exports the user-facing names from these packages, so application
code imports from `eventsource` directly and the internal layout stays free to move. That is also
why a rename inside a package is cheap and a change to `__init__.py` is not.

## Shared structural principles

Three rules explain most of the file boundaries you will encounter. They are not aspirations
written after the fact — each one is visible as a specific split in the tree, and the sections
that follow trace those splits in detail.

### One responsibility per module

When a class starts answering two unrelated questions — "how do I persist this aggregate?" and
"when should I snapshot it?" — the second question moves out, into a module named for the answer.
That is the literal history of `aggregates/snapshot_manager.py`: `AggregateSnapshotManager` was
carved out of `AggregateRepository`, and `aggregates/task_manager.py` exists for the same reason.

The test for whether a module has one responsibility is not line count; it is whether the
failure modes are shared. Persistence fails with `OptimisticLockError` and must propagate.
Snapshot loading fails with a schema mismatch and must *not* propagate — it degrades to a full
replay. Two different failure policies in one class means two responsibilities, and the seam
belongs where the policies diverge.

The practical payoff is reviewability. `load` and `save` read as short narratives about events
because the snapshot decisions are one call away rather than inline, and a change to snapshot
validation cannot accidentally alter the append path.

### Composition over inheritance

Shared behavior arrives as an injected collaborator held in a field, not as a base class in the
MRO. The clearest case is tracing: a `TracingMixin` used to sit in the hierarchy of every traced
class, and it was replaced by a `Tracer` object passed to `__init__`. The docstring of
`observability/tracer.py` states the reasoning outright — components keep a single
responsibility, tracers are trivially mockable, and implementations become swappable without
touching the class hierarchy.

Inheritance is still used, but only for *implementation reuse under a fixed contract*.
`BaseSnapshotStrategy` is an ABC because all three strategies genuinely share one
serialize-and-save body (`_create_snapshot`); `AggregateRoot` and `DeclarativeAggregate` are
abstract bases because "an aggregate" is what the subclass *is*. The rule of thumb: inherit to
say what something is, compose to say what something has. Tracing, snapshotting, and background
task tracking are all things a repository *has*.

A useful side effect is that the constructor becomes the whole dependency list. Reading
`AggregateRepository.__init__` tells you every collaborator the class can reach, which is not
true of a class that acquires half its behavior from three levels of `super()`.

### Protocols at the boundaries

Where a component needs a pluggable collaborator, the contract is a `typing.Protocol` — structural
typing, so an implementation satisfies it by having the right methods, with no import of the
contract and no inheritance. `SnapshotStrategy` (`snapshots/strategies.py`) and `Tracer`
(`observability/tracer.py`) are both Protocols, and both are `@runtime_checkable` so an
`isinstance` guard remains possible where one is genuinely needed.

ABCs are used where the contract also wants to *supply* behavior or enforce a base
`__init__`. `protocols.py` is deliberately mixed for this reason and says so: `EventHandler`,
`SyncEventHandler`, `FlexibleEventHandler`, and `FlexibleEventSubscriber` are Protocols, while
`EventSubscriber` and `AsyncEventHandler` are ABCs because they carry methods subclasses inherit
rather than merely satisfy. The choice is not stylistic — an ABC forces every implementation to
depend on this package, which is the wrong direction at a plug-in boundary and the right one at a
framework base class.

`protocols.py`, `types.py`, and `exceptions.py` hold declarations only. Nothing in them does
work, which is what lets any package import them without dragging behavior — or optional
dependencies — along.

### How the three interact

The snapshot path shows all three in one line of wiring: the repository *has* a manager
(composition), the manager *has* a strategy (composition), the strategy conforms to a Protocol
(boundary), and each object owns exactly one decision (SRP) — persist, cache-lifecycle, policy.
Adding a fourth snapshot behavior touches none of the existing three; it is a new class
implementing `SnapshotStrategy`. That is the shape to reach for when extending any package here.

## The aggregates bounded context

### `aggregates/base.py` — state derived from events

`base.py` holds two abstract classes and one idea: an aggregate's state is a *fold over its
events*, never a thing you set. Everything in the module either advances that fold or exists so
another package can shortcut it.

`AggregateRoot[TState]` is the base. Its instance state is deliberately four fields —
`_aggregate_id`, `_version`, `_uncommitted_events`, `_state` — exposed through read-only
properties (`aggregate_id`, `version`, `uncommitted_events`, `state`, `has_uncommitted_events`).
There is no setter for state, because there is no legitimate way to reach a state that no event
produced.

`apply_event(event, is_new=True)` is the single mutation path, and it is worth reading as four
steps in order: validate the version, adopt `event.aggregate_version` as the new `_version`, call
the abstract `_apply()` to fold the event into state, and — only when `is_new` — append to
`_uncommitted_events`. That last conditional is the whole reason one method serves both command
handling and replay. `load_from_history(events)` is a loop calling `apply_event(..., is_new=False)`,
so rehydrating an aggregate produces no new uncommitted events; `mark_events_as_committed()` and
`clear_uncommitted_events()` (which returns them first) are the two ways the repository drains the
list after a successful append.

Version validation is a class-level policy, not a per-call flag. `validate_versions` defaults to
`True`: a new event whose `aggregate_version` is not `version + 1` raises `EventVersionError`. Set
it to `False` and the same mismatch is logged as a warning and allowed through. Replayed events
skip the check entirely — history is authoritative by definition.

Three helpers keep command methods from restating the obvious. `get_next_version()` returns
`version + 1`; `_raise_event(event)` is an intent-revealing alias for `apply_event(event,
is_new=True)`; and `create_event(EventClass, **kwargs)` constructs *and* applies an event with
`aggregate_id`, `aggregate_type`, `aggregate_version`, and (when a tenant context is active)
`tenant_id` filled in automatically. They are conveniences, but they also remove the most common
source of `EventVersionError` — hand-computed version numbers.

The snapshot seam is exactly two methods, and they are the only places state crosses a
persistence boundary. `_serialize_state()` returns `self._state.model_dump(mode="json")` (or `{}`
for a never-created aggregate), and `_restore_from_snapshot(state_dict, version)` validates that
dict back into the state model via `_get_state_type()` and sets `_version` directly. Both are
underscore-private and documented as repository-internal: they let the snapshot manager skip a
replay, and they are not an alternative way to construct an aggregate. `_get_state_type()`
recovers `TState` by walking `__orig_bases__`, which is why an aggregate must be declared as
`AggregateRoot[OrderState]` rather than bare — the generic parameter is load-bearing at runtime.
`schema_version` (also on `AggregateRoot`, default `1`) is the invalidation lever: bump it when
`TState` changes incompatibly and every stored snapshot for the class stops validating.

`DeclarativeAggregate[TState]` layers `@handles`-based routing on top and implements the abstract
`_apply` once, for everyone. `__init_subclass__` walks the subclass's attributes for the
`_handles_event_type` marker the decorator leaves behind and builds a fresh per-subclass
`_event_handlers` mapping of event type to method name — fresh, so subclasses never mutate a
parent's table. `_apply` is then a dict lookup plus a call, falling through to
`_handle_unregistered_event` when nothing matches. What that fallback *means* is a per-class
decision, expressed by `unregistered_event_handling`: `"ignore"` (default, silent), `"warn"` (log
with the list of handlers the class does have), or `"error"` (raise `UnhandledEventError`). A
shared event stream where most aggregates only care about a few event types wants `"ignore"`; a
strict aggregate that should never see a foreign event wants `"error"`.

`requires_creation_event` is the other class-level switch. Left `False`, the subclass must
implement `_get_initial_state()` and the aggregate has state from construction. Set `True`,
`_get_initial_state()` returns `None`, the first event handler establishes state, and `.state`
raises `AggregateNotCreatedError` until then — with `state_or_none` and `is_created` as the
non-raising ways to ask. This is the "does an empty Order exist?" question given a type-level
answer instead of a `None` check at every call site.

### `aggregates/repository.py` — load and save, and not much else

`AggregateRepository[TAggregate]` is the persistence orchestrator, and its public surface is
deliberately small. Seven methods do the work: `load`, `load_or_create`, `save`, `exists`,
`get_version`, `get_or_raise` (an intent-revealing alias for `load`, identical in behavior), and
`create_new` (a bare `self._aggregate_factory(aggregate_id)` — an in-memory instance at version 0,
no I/O at all). Everything else on the class is either configuration read-back
(`aggregate_type`, `event_store`, `event_publisher`) or a pass-through to the snapshot manager.

The constructor takes the whole dependency list, which is the point of the composition style: an
`EventStore`, an `aggregate_factory` (the aggregate class itself), an optional `aggregate_type`
string, an optional `EventPublisher`, the three snapshot knobs, and the two tracing arguments.
When `aggregate_type` is omitted, `_infer_aggregate_type` reads the `aggregate_type` class
attribute off the factory and rejects `""` and `"Unknown"` as unset — the `ValueError` it raises
spells out both fixes rather than letting a mistyped stream name reach the store.

`load` is where the loading *sequence* lives, and reading it top to bottom is the fastest way to
understand the aggregate lifecycle: ask the snapshot manager for a valid snapshot; fetch events
from `snapshot.version` if there was one and from `0` if not; instantiate the aggregate; restore
snapshot state; replay whatever events came back; return. The method owns the two failure
policies that go with that sequence. No snapshot *and* no events means the aggregate does not
exist, so it raises `AggregateNotFoundError`. A snapshot that exists but fails
`_restore_from_snapshot` is not an error at all — it is logged with `exc_info`, all events are
re-fetched from version 0, the aggregate instance is discarded and rebuilt, and only an empty
re-fetch escalates to `AggregateNotFoundError`. A corrupt cache degrades to a slow read, never to
a failed one.

`load_or_create` is four lines wrapped around that: `try: return await self.load(...)` and, on
`AggregateNotFoundError`, hand back a fresh factory instance. It exists so callers stop writing
that try/except themselves, and it deliberately has no span of its own — it inherits `load`'s.

`save` is the write half and is equally narrow. It reads `aggregate.uncommitted_events`, returns
immediately if there are none (a no-op save, and no span — the early return happens before the
`with`), then computes `expected_version = aggregate.version - len(uncommitted_events)`. That
arithmetic is the whole optimistic-locking contract: the aggregate already advanced its version
locally as each event was applied, so the version *before* the command is what the store must
still be at. Getting it from the aggregate rather than from a caller-supplied argument is what
makes `OptimisticLockError` impossible to bypass by accident. On a successful append it marks the
events committed, publishes them if a publisher was configured, and then asks the snapshot
manager whether a snapshot is warranted — `maybe_create_snapshot(aggregate, events_since_snapshot=...)`.
It does not decide that itself, and it does not let the answer affect the outcome of the save.

The ordering here is a policy, not an accident: durable append first, then local bookkeeping,
then publication, then caching. Each step is safe to lose if the process dies after it, and none
of the later steps can invalidate the earlier ones.

`exists` and `get_version` are the two cheap questions, and both answer by reading the stream:
`exists` returns whether `get_events` came back with any events, `get_version` returns
`event_stream.version` (`0` for an aggregate that has none). Neither reconstitutes an aggregate,
which is the reason they exist as separate methods rather than as `try: await load(...)`.

What the repository conspicuously does *not* contain is as informative as what it does. There is
no snapshot validation logic, no threshold arithmetic, no `asyncio.Task` list, and no tracing
base class — those are `AggregateSnapshotManager`, `SnapshotStrategy`, `BackgroundTaskManager`,
and an injected `Tracer` respectively. What remains is a class you can read in one sitting whose
every method is a short story about events.

### `aggregates/snapshot_manager.py` — the snapshot lifecycle

`AggregateSnapshotManager[TAggregate]` owns everything snapshot-shaped. Its constructor takes a
`SnapshotStore`, the `aggregate_type` string, an optional `SnapshotStrategy`, and the two tracing
arguments — no event store, no aggregate factory, no publisher. That dependency list is the
clearest statement of scope in the module: the manager can read and write snapshots and it can
ask a strategy a question, and it can do nothing else. It cannot load events, so it can never
quietly become a second way to reconstitute an aggregate.

`strategy` is genuinely optional. Passed `None`, the manager still loads and validates snapshots
and still creates them on demand; only the automatic path goes away. "Manual snapshots only" is
therefore the absence of a collaborator rather than a mode flag.

Four operations make up the surface.

`load_valid_snapshot(aggregate_id, aggregate_factory)` is the read path, and its contract is
*never raise*. It fetches from the store inside a `try`, and three distinct outcomes all funnel to
the same `None`: a store exception (logged at `warning` with "Falling back to event replay"), a
missing snapshot (silent — not finding a cache entry is not news), and a `schema_version`
mismatch against `getattr(aggregate_factory, "schema_version", 1)` (logged at `info`, naming both
versions). Only on the way out does it set `snapshot.found` and `snapshot.version` on the span.
The uniform `None` is what lets `AggregateRepository.load` treat "no snapshot" as one branch
instead of four, and it is why bumping `schema_version` on an aggregate is a safe deployment
action rather than a breaking one — every stored snapshot silently stops matching and reads
degrade to full replay.

Note the direction of the schema check: the manager reads the *expected* version off the
aggregate class it was handed, not off itself. It holds no opinion about what version is current,
which is what keeps a single manager correct across a schema bump.

`maybe_create_snapshot(aggregate, events_since_snapshot=0)` is the automatic write path and is
mostly a pair of guards: return `None` if there is no strategy, return `None` if
`strategy.should_snapshot(aggregate, events_since_snapshot)` says no. Only past both does it open
a span and call `strategy.execute_snapshot(aggregate, store, aggregate_type)`, returning whatever
that gives back — a `Snapshot` for the synchronous strategy, `None` for the background one. Both
early returns happen *before* the `with`, so a trace contains a `maybe_create_snapshot` span only
when a snapshot actually happened; the overwhelmingly common "not at a threshold boundary" case
costs nothing and shows nothing.

`create_snapshot(aggregate)` is the manual write path and deliberately does not consult the
strategy at all. It reads `schema_version` off `type(aggregate)`, calls the aggregate's
repository-internal `_serialize_state()`, builds a `Snapshot` with `created_at=datetime.now(UTC)`,
saves it, logs at `info`, and returns it. It also returns `Snapshot`, not `Snapshot | None` —
where the automatic path is best-effort, an explicit request either produces a snapshot or raises.
Two different callers, two different failure policies, and the type signatures say so.

`pending_count` and `await_pending()` are the background-work window. Each does an `isinstance`
check for `BackgroundSnapshotStrategy` and forwards, returning `0` otherwise. That check is the
one place the manager knows a concrete strategy class, and it is a conscious concession: awaiting
in-flight tasks is meaningless for the other two strategies, so rather than widen the
`SnapshotStrategy` Protocol with methods most implementations would stub out, the capability is
detected where it is used. Tests are the main consumer — `await manager.await_pending()` before
asserting a snapshot exists.

What the manager does *not* own is equally deliberate. It does not decide *when* to snapshot
(that is the strategy), it does not own the `asyncio.Task` list (that is
`BackgroundSnapshotStrategy`), and it does not know how to replay events (that is the
repository). It is the lifecycle coordinator sitting between a store and a policy, and its
`__repr__` — `AggregateSnapshotManager(Order, strategy=ThresholdSnapshotStrategy)` — is a fair
summary of everything it holds.

### Why snapshot logic moved out of the repository

The repository's job is *persistence orchestration*: turn uncommitted events into a durable,
version-checked append, and turn stored events back into an aggregate. Snapshotting is a
different job — a cache lifecycle with its own validity rules (schema versions), its own failure
policy (degrade to replay, never fail the caller), and its own timing concerns. Fused into one
class, those two jobs shared a constructor, shared mutable state, and every change to snapshot
validation risked the save path. Split apart, `load`/`save` read as narratives about events, and
snapshot correctness can be reasoned about — and tested — on its own.

### Delegating the when/how decision to `SnapshotStrategy`

Even inside the manager, the *policy* question is factored out again. `SnapshotStrategy`
(`snapshots/strategies.py`) is a runtime-checkable Protocol with two methods: `should_snapshot()`
and `execute_snapshot()`. Three implementations descend from `BaseSnapshotStrategy`, which
supplies the shared serialize-and-save body (`_create_snapshot`).

The shared `should_snapshot` on the base returns `False` when no threshold is configured and
otherwise fires on a threshold boundary (`version > 0 and version % threshold == 0`); only the
*execution* differs between subclasses:

- `ThresholdSnapshotStrategy` — snapshot synchronously at the boundary; failures
  are logged and swallowed, returning `None`.
- `BackgroundSnapshotStrategy` — same trigger, but `execute_snapshot` spawns an `asyncio` task
  and returns `None` immediately, so the save path never waits on the store. It tracks the task,
  prunes completed ones, and exposes `pending_count` / `await_pending()`.
- `NoSnapshotStrategy` — `should_snapshot` is unconditionally `False`; snapshots only happen via
  an explicit call.

`create_snapshot_strategy(mode, threshold)` maps the legacy `"sync" | "background" | "manual"`
mode strings onto those classes and raises `ValueError` for anything else. This is the
Open/Closed payoff: a new snapshot behavior is a new class implementing the Protocol, not an
extra branch inside the repository.

### How the repository wires the manager

The wiring is a short chain in `AggregateRepository.__init__`, and it only happens when a
`snapshot_store` was supplied:

`snapshot_mode` + `snapshot_threshold` → `create_snapshot_strategy(...)` → `AggregateSnapshotManager(store, aggregate_type, strategy, enable_tracing)`

Without a store, `_snapshot_strategy` and `_snapshot_manager` are both `None` and every snapshot
path becomes a no-op or a clear error. The repository then keeps a thin pass-through surface so
callers never need to reach for the manager:

- `create_snapshot(aggregate)` — raises `RuntimeError` if no store is configured, otherwise
  delegates.
- `await_pending_snapshots()` — returns `0` with no manager, else `manager.await_pending()`.
- `pending_snapshot_count` — same shape, as a property.
- `snapshot_store`, `snapshot_threshold`, `snapshot_mode`, `has_snapshot_support` — read-only
  views of the configuration.

### `aggregates/task_manager.py` — `BackgroundTaskManager`

`BackgroundTaskManager` is a small, dependency-free collaborator for fire-and-forget asyncio
work. `submit(coro)` creates and tracks a task, attaches a done-callback that logs any exception,
and prunes finished tasks so the list cannot grow unbounded. `pending_count` / `has_pending`
report status, `await_all(timeout=None)` waits (cancelling stragglers past the timeout and
logging a warning), and `cancel_all()` cancels everything and returns the count.

It is a standalone building block rather than a wired-in dependency: today
`BackgroundSnapshotStrategy` keeps its own `_pending_tasks` list, and nothing in `aggregates/`
imports `BackgroundTaskManager`. Treat it as the intended landing place for that duplicated task
bookkeeping, and as the reusable answer for any other component that wants background work with
lifecycle tracking.

### Why task tracking is a collaborator, not repository state

A list of in-flight tasks is state with its own lifecycle rules — creation, error logging,
draining under timeout, cancellation on shutdown. Held as `self._tasks` on a repository, that
lifecycle becomes indistinguishable from persistence state, is only reachable through the
repository's API, and has to be reimplemented by the next component that needs it. As a separate
object it can be constructed in a test, driven directly, and shared.

## Tracing by composition, not `TracingMixin`

### Tracer injection

Components that emit spans take two optional constructor arguments and resolve them in one line:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
self._enable_tracing = self._tracer.enabled
```

`Tracer` is a Protocol (`span`, `start_span`, `span_with_kind`, `enabled`).
`create_tracer(name, enable_tracing)` returns an `OpenTelemetryTracer` when tracing is requested
*and* OpenTelemetry is installed, and a `NullTracer` otherwise — so `enable_tracing=False` yields
a `NullTracer` whose `span()` simply yields `None` and whose `enabled` is `False`. An explicitly
passed `tracer` wins outright; `enable_tracing` is ignored in that case. `MockTracer` exists in
the same module for tests that want to assert on recorded spans.

Because spans may be `None`, call sites guard attribute writes with `if span:` rather than
assuming a live span object.

### Span boundaries follow the split

Each collaborator names its own spans, which means the trace tree mirrors the module structure:

- `AggregateRepository`: `eventsource.repository.load`, `.save`, `.exists`,
  `.create_snapshot`. `get_version`, `load_or_create`, and `create_new` are unspanned —
  `load_or_create` inherits `load`'s span. `save` opens no span at all when there is nothing to
  commit, because the early return happens before the `with`.
- `AggregateSnapshotManager`: `eventsource.snapshot_manager.load_valid_snapshot`,
  `.maybe_create_snapshot`, `.create_snapshot`.

Attributes come from the shared constants in `observability/attributes.py`
(`ATTR_AGGREGATE_ID`, `ATTR_AGGREGATE_TYPE`, `ATTR_EVENT_COUNT`, `ATTR_VERSION`), plus
operation-specific keys set inside the block: `snapshot.used`, `snapshot.version`,
`events.replayed` on load; `save.success`, `new_version` on save; `exists` on exists.

### Why the mixin was removed

`TracingMixin` put tracing in the class hierarchy. That made observability an inheritance
decision: every traced class had to descend from it, the mixin's `__init__` had to be threaded
through cooperative `super()` calls, and a test could not hand a component a different tracer
without subclassing or monkeypatching. Composition inverts all three — tracing is a constructor
argument, any object satisfying the `Tracer` protocol is acceptable, and disabling tracing is
just a different object. The comment `# Composition-based tracing (replaces TracingMixin)`
marks the same migration across `projections/`, `subscriptions/`, `bus/`, and `migration/`.

### What the tests pin down

`tests/unit/aggregates/test_repository_tracing.py` locks the contract in place:

- `test_uses_tracer_composition` inspects `AggregateRepository.__init__` and asserts a `tracer`
  parameter exists — a structural guard against a regression to inheritance.
- `test_tracing_enabled_by_default` / `test_tracing_disabled_when_requested` assert a tracer is
  always present, and that `enable_tracing=False` produces a `NullTracer` with
  `_enable_tracing is False`.
- `test_custom_tracer_can_be_injected` asserts the passed instance is the one stored (`is`
  identity, not merely equal).
- `test_backward_compatible_constructor` asserts construction without any tracing arguments still
  works.

The remaining classes in that file exercise span behavior through a `MockTracer`: span creation
and attributes for `save` / `load` / `exists` / `create_snapshot`, correct operation with tracing
disabled, dynamic attributes such as event counts and snapshot usage, use of the standard
attribute constants, and the absence of a span when `save` has no events.

## Reading the aggregates code

A workable order:

1. `base.py` — `apply_event`, `load_from_history`, `_apply`. Everything else assumes these.
2. `repository.py` — `__init__` for the wiring, then `load` and `save`.
3. `snapshot_manager.py` — `load_valid_snapshot` and `maybe_create_snapshot`.
4. `snapshots/strategies.py` — `BaseSnapshotStrategy` then the three subclasses.
5. `observability/tracer.py` — only if you are touching spans.
6. `task_manager.py` — standalone; read whenever background tasks come up.

## The same split elsewhere

The pattern — a coordinator plus named collaborators plus an injected tracer — repeats across the
codebase. `projections/` separates the coordinator from `checkpoint_manager.py`; `subscriptions/`
splits lifecycle, pause/resume, retry, health, and flow control into distinct modules;
`migration/` separates the router, the consistency checker, and the status streamer. All of them
carry the same composition-based tracing initialization. When adding to any of these packages,
prefer a new collaborator or a new strategy implementation over a new branch or a new mixin.

## Related documents

- `docs/core-surface.md` — the Tier 0 dependency boundary.
- `src/eventsource/aggregates/README.md` — per-directory interface and invariant summary.
- `.claude/rules/architecture.md` — layer boundaries and interface patterns.
