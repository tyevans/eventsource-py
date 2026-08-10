# Subscriptions API Reference

Technical reference for the subscription configuration and multi-instance
coordination surface of the `eventsource.application.subscriptions` package.

`eventsource.application.subscriptions` is the largest package in the library: it covers the
subscription state machine (`Subscription`, `SubscriptionState`), the manager and
its collaborators (`SubscriptionManager`, `SubscriptionRegistry`,
`SubscriptionLifecycleManager`, `PauseResumeController`, `HealthCheckProvider`),
catch-up/live transition (`TransitionCoordinator`), flow control, filtering,
retry and circuit breaking, error handling, health checks, metrics, graceful
shutdown, and coordination across instances. Everything listed in
`eventsource.application.subscriptions.__all__` is public.

This page documents two of those areas in full detail:

| Area | Module | Covered here |
| --- | --- | --- |
| Configuration | `eventsource.application.subscriptions.config` | `StartPosition`, `CheckpointStrategy`, `SubscriptionConfig`, `create_catch_up_config()`, `create_live_only_config()` |
| Coordination | `eventsource.application.subscriptions.coordination` | Topic constants, coordination messages, leader election, peer tracking and redistribution, callback type aliases |

Configuration is the entry point for almost every subscription: a
`SubscriptionConfig` is a frozen dataclass that decides where a subscription
starts reading, how it batches, when it checkpoints, what it filters, and how it
retries. Coordination is the opposite end of the stack — protocols and message
types for deployments running more than one instance, where exactly one instance
should hold leadership and peers need to learn when another is shutting down so
its work can be redistributed.

Every field, default, validation rule, and method below is described as it
behaves in the current source. Where a name is a Protocol rather than a concrete
class, that is called out explicitly: Protocols define a shape you implement,
and the package ships in-memory implementations intended for tests and
single-instance deployments.

## Overview

Both areas are re-exported from the package root, so the documented names are
importable either from the package or from their defining module:

```python
from eventsource.application.subscriptions import SubscriptionConfig
from eventsource.adapters.memory import InMemoryLeaderElector
# equivalent to
from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.adapters.memory.coordination import InMemoryLeaderElector
```

`InMemoryLeaderElector` and `SharedLeaderState` are the one name-availability
break in the ADR 0032 move: the `LeaderElector` Protocol pair lives in
`eventsource.ports.coordination`, but their only concrete implementation is an
adapter, and `application/` may not import `adapters/` — so it now lives in
`eventsource.adapters.memory.coordination` (re-exported from
`eventsource.adapters.memory`) rather than in `eventsource.application.subscriptions`
alongside the rest of the coordination surface.

### Configuration at a glance

`SubscriptionConfig` is a `@dataclass(frozen=True)` with defaults for every
field, so `SubscriptionConfig()` is valid and describes a subscription that
resumes from its last checkpoint, reads 100 events per batch, checkpoints once
per batch, retries failures up to five times with exponential backoff, and
trips a circuit breaker after five consecutive failures. Overriding a field
means overriding one decision, not restating the rest.

The fields fall into five groups, each documented in its own subsection below:

| Group | Fields |
| --- | --- |
| Starting position and batching | `start_from`, `batch_size` |
| Checkpointing | `checkpoint_strategy`, `checkpoint_interval_seconds` |
| Timeouts | `processing_timeout` |
| Filtering | `event_types`, `aggregate_types`, `tenant_id` |
| Error handling and retry | `continue_on_error`, `max_retries`, `initial_retry_delay`, `max_retry_delay`, `retry_exponential_base`, `retry_jitter` |
| Circuit breaker | `circuit_breaker_enabled`, `circuit_breaker_failure_threshold`, `circuit_breaker_recovery_timeout` |

Because the dataclass is frozen, validation happens once in `__post_init__` and
a constructed instance is always valid: invalid combinations raise `ValueError`
at construction time rather than failing mid-stream. The retry and circuit
breaker fields are stored flat on the config and converted on demand by
`get_retry_config()` and `get_circuit_breaker_config()`, which build the
`RetryConfig` and `CircuitBreakerConfig` objects that `eventsource.application.subscriptions.retry`
consumes.

`create_catch_up_config()` and `create_live_only_config()` are thin factories
over the same dataclass — they set a handful of fields and leave the rest at
their defaults. They exist to name the two most common shapes, not to unlock
behavior you cannot express directly.

### Coordination at a glance

The coordination module has no dependency on the event store or the bus. It
defines message types, protocols, and an in-memory implementation; transporting
messages between processes is left to whatever bus or control plane you already
run. The four topic constants exist so that separate deployments agree on
channel names for that transport.

Coordination splits into four layers:

- **Topic constants** — the `__eventsource_coordination`-prefixed channel names
  for shutdown, heartbeat, and work-assignment traffic.
- **Messages** — `ShutdownIntent`, `ShutdownNotification`, `HeartbeatMessage`,
  and `WorkAssignment`: the payloads instances exchange.
- **Leader election** — `LeaderElector`
  (`eventsource.ports.coordination`) are Protocols describing what a
  leader-election backend must provide. `InMemoryLeaderElector`, backed by
  `SharedLeaderState` (`eventsource.adapters.memory.coordination`), is the
  only concrete implementation shipped; it coordinates within a single
  process and is intended for tests and single-instance deployments.
  Kubernetes, Redis, and Consul backends are named in the module docstring as
  future work, not as available classes.
- **Peer tracking and redistribution** — `PeerInfo` records what is known about
  a peer, and `WorkRedistributionCoordinator` ties the pieces together:
  broadcasting shutdown intent, tracking heartbeats, detecting peer timeouts,
  and dispatching registered callbacks.

The five callback type aliases (`LeaderChangeCallback`, `PeerShutdownCallback`,
`HeartbeatCallback`, `WorkAssignmentCallback`, `PeerTimeoutCallback`) are all
`Callable[[X], Awaitable[None]]` — every coordination callback is async and
returns nothing. They are aliases, not classes; any coroutine function with the
right parameter type satisfies them.

## Import surface (`eventsource.application.subscriptions`)

All names on this page are exported from `eventsource.application.subscriptions.__all__`
(123 entries in total for the package). They are **not** re-exported from the
top-level `eventsource` package — unlike most of the library, subscription
names must be imported from the subpackage:

```python
# Correct
from eventsource.application.subscriptions import SubscriptionConfig, CheckpointStrategy

# Wrong — AttributeError / ImportError
from eventsource import SubscriptionConfig
```

Each name is also importable from its defining module. The package-level import
is the supported form; the module path is stable but more verbose.

### Configuration names

Defined in `eventsource/subscriptions/config.py`. The module has no `__all__`
of its own; the package `__all__` is the contract.

| Name | Kind | Import |
| --- | --- | --- |
| `StartPosition` | Type alias — `Literal["beginning", "end", "checkpoint"] \| int` | `from eventsource.application.subscriptions import StartPosition` |
| `CheckpointStrategy` | `Enum` (`EVERY_EVENT`, `EVERY_BATCH`, `PERIODIC`) | `from eventsource.application.subscriptions import CheckpointStrategy` |
| `SubscriptionConfig` | `@dataclass(frozen=True)` | `from eventsource.application.subscriptions import SubscriptionConfig` |
| `create_catch_up_config` | Function returning `SubscriptionConfig` | `from eventsource.application.subscriptions import create_catch_up_config` |
| `create_live_only_config` | Function returning `SubscriptionConfig` | `from eventsource.application.subscriptions import create_live_only_config` |

`config.py` uses `from __future__ import annotations` and imports `DomainEvent`,
`RetryConfig`, and `CircuitBreakerConfig` only under `TYPE_CHECKING`. Those
types therefore appear in annotations without being imported at runtime — the
runtime import of `RetryConfig`/`CircuitBreakerConfig` happens inside
`get_retry_config()` and `get_circuit_breaker_config()` when you call them.
Importing `eventsource.application.subscriptions.config` directly pulls in only
`dataclasses`, `enum`, `typing`, and `uuid`.

### Coordination names

Defined in `src/eventsource/application/subscriptions/coordination.py`, which
does declare its own `__all__` — the message types, callback aliases, and
work-redistribution names below are re-exported unchanged by the package.
`LeaderElector` and `InMemoryLeaderElector`/
`SharedLeaderState` are re-exported from `eventsource.ports.coordination` and
`eventsource.adapters.memory.coordination` respectively (see the previous
section) and are **not** re-exported by `application.subscriptions`.

| Group | Names |
| --- | --- |
| Topic constants | `COORDINATION_TOPIC_PREFIX`, `SHUTDOWN_NOTIFICATIONS_TOPIC`, `HEARTBEAT_TOPIC`, `WORK_ASSIGNMENT_TOPIC` |
| Enum | `ShutdownIntent` |
| Message types | `ShutdownNotification`, `HeartbeatMessage`, `WorkAssignment` |
| Callback aliases | `LeaderChangeCallback`, `PeerShutdownCallback`, `HeartbeatCallback`, `WorkAssignmentCallback`, `PeerTimeoutCallback` |
| Leader election (`eventsource.ports.coordination`) | `LeaderElector` |
| Leader election, in-memory (`eventsource.adapters.memory`) | `InMemoryLeaderElector`, `SharedLeaderState` |
| Work redistribution | `PeerInfo`, `WorkRedistributionCoordinator` |

`LeaderElector` is a `Protocol` class: import it
for type annotations and structural conformance, not to subclass. The only
concrete elector shipped is `InMemoryLeaderElector`.

### A representative import

```python
from eventsource.adapters.memory import InMemoryLeaderElector
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    HEARTBEAT_TOPIC,
    PeerInfo,
    ShutdownIntent,
    SubscriptionConfig,
    WorkRedistributionCoordinator,
    create_catch_up_config,
)
from eventsource.ports.coordination import LeaderElector
```

Nothing in either module requires an optional dependency: both are importable
with only the core `pydantic` + `sqlalchemy` install. Other parts of the
package (metrics, observability) degrade to no-op implementations when
OpenTelemetry is absent, but configuration and coordination have no such
guards.

## Configuration

`eventsource.application.subscriptions.config` defines five public names. Together they
answer, for a single subscription: where to start, how much to read at a time,
when to persist a checkpoint, which events to keep, and what to do when a
handler fails.

```python
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    StartPosition,
    SubscriptionConfig,
    create_catch_up_config,
    create_live_only_config,
)
```

### `StartPosition`

```python
StartPosition = Literal["beginning", "end", "checkpoint"] | Position
```

A type alias, not a class — there is nothing to instantiate beyond a `Position`
itself. It is the annotation of `SubscriptionConfig.start_from` and the set of
values `StartFromResolver.resolve()` (in `eventsource.application.subscriptions.transition`)
understands:

| Value | Resolved starting position |
| --- | --- |
| `"beginning"` | `None` — read from the start of the feed |
| `"end"` | `await event_store.current_position()` — the current end, so only new events are seen |
| `"checkpoint"` | The subscription's stored checkpoint position; falls back to `None` (logging an info message) when no checkpoint exists |
| `Position` | That exact opaque position token, returned unchanged |

The `Position` branch is tested first (`isinstance(start_from, Position)`), so an
explicit position is never confused with a literal. Any other value raises
`ValueError: Unknown start_from value: ...` at resolve time — string literals
are *not* validated by `SubscriptionConfig.__post_init__`, only by the resolver.
`SubscriptionConfig` no longer accepts a bare `int` for `start_from` at all — the
member was deleted when positions became opaque; pass a `Position` obtained from
the store or a checkpoint instead.

The default for `SubscriptionConfig.start_from` is `"checkpoint"`, which makes
resume-from-where-you-left-off the behavior you get without asking for it. Note
that `"checkpoint"` degrades to reading from the start of the feed for a
subscription that has never checkpointed; use an explicit `Position` or `"end"`
if a fresh subscription must not read history.

### `CheckpointStrategy`

```python
class CheckpointStrategy(Enum):
    EVERY_EVENT = "every_event"
    EVERY_BATCH = "every_batch"
    PERIODIC = "periodic"
```

A plain `Enum` (not `StrEnum`, not `IntEnum`) with three members. Members compare
by identity — the runners test `config.checkpoint_strategy == CheckpointStrategy.X` —
so pass the member, not its string. The `.value` strings are stable and appear in
runner log records (`"checkpoint_strategy": self.config.checkpoint_strategy.value`
in the catch-up runner's start log).

| Member | Value | Checkpoint written | Trade-off |
| --- | --- | --- | --- |
| `EVERY_EVENT` | `"every_event"` | After each successfully processed event | Safest, slowest — one checkpoint write per event |
| `EVERY_BATCH` | `"every_batch"` | Once per drained batch (the default) | Balanced; on crash you reprocess at most one batch |
| `PERIODIC` | `"periodic"` | When `checkpoint_interval_seconds` has elapsed | Fewest writes, most reprocessing after a crash |

The value is the `checkpoint_strategy` field of `SubscriptionConfig`, whose
default is `CheckpointStrategy.EVERY_BATCH`.

#### How each runner interprets it

The two runners in `eventsource.application.subscriptions.runners` read the same field but
do not behave identically, and the difference is the main thing to know here.

**Catch-up runner** (`runners/catchup.py`) honors all three literally, per batch:

- `EVERY_EVENT` — checkpoints inside the per-event loop, after the event has
  been delivered and its position recorded. Events dropped by the subscription
  filter are skipped with `continue` *before* this point, so a filtered event
  never triggers a per-event checkpoint (its position is still recorded on the
  subscription).
- `PERIODIC` — calls the same per-event hook, but only writes when
  `elapsed >= config.checkpoint_interval_seconds`.
- `EVERY_BATCH` — checkpoints once after the batch loop, using the last
  `EventEnvelope` seen. The guard is
  `(events_in_batch > 0 or events_filtered > 0) and last_stored_event is not None`,
  so a batch in which *every* event was filtered out still checkpoints — that is
  what keeps a subscription with a narrow filter from re-reading the same
  filtered range after a restart. An empty batch writes nothing.

**Live runner** (`runners/live.py`) receives events one at a time from the bus,
so there is no batch boundary to attach to. Its `_maybe_checkpoint()` maps
`EVERY_BATCH` onto the `EVERY_EVENT` branch explicitly — same checkpoint write,
same frequency. In live mode the only strategy that reduces checkpoint writes is
`PERIODIC`; choosing `EVERY_BATCH` over `EVERY_EVENT` changes nothing.

#### `PERIODIC` timing

Both runners track `_last_checkpoint_time` with `time.monotonic()` and compare
`elapsed >= config.checkpoint_interval_seconds` (default `5.0`). Two consequences:

- The clock is reset by *every* checkpoint write, not just periodic ones — the
  interval means "at least this long since the last checkpoint of any kind".
- The check runs only when an event arrives. A quiet subscription writes no
  checkpoint until the next event, however long the interval has been exceeded;
  there is no background timer.

`_last_checkpoint_time` is seeded at runner start, so the first periodic
checkpoint comes no earlier than one full interval after startup.

#### Choosing one

- `EVERY_EVENT` when reprocessing a single event is unacceptable — a projection
  with non-idempotent side effects, or an integration that emits externally
  visible messages.
- `EVERY_BATCH` (the default) for catch-up work over large histories: one write
  per `batch_size` events, bounded replay on restart.
- `PERIODIC` when checkpoint writes are themselves the bottleneck and your
  handlers are idempotent. Bound the damage by lowering
  `checkpoint_interval_seconds` rather than relying on the `5.0` default.

Checkpoint writes go through `_save_checkpoint_with_retry()` under every
strategy, so a transient checkpoint-store failure is retried using the
subscription's `RetryConfig` before it surfaces.

### `SubscriptionConfig`

```python
@dataclass(frozen=True)
class SubscriptionConfig: ...
```

A standard-library `dataclasses` dataclass — not a pydantic model — with a
default for every one of its 18 fields, so `SubscriptionConfig()` is a complete,
valid configuration. Frozen means assignment after construction raises
`FrozenInstanceError`, and because `eq=True` is also in effect the class gets a
value-based `__hash__`; the collection-shaped fields are typed as `tuple`
rather than `list` so that hashing actually works. To change a setting,
construct a new instance:

```python
from dataclasses import replace

faster = replace(config, batch_size=500)
```

Every constructor argument is keyword-usable and independent — overriding one
field leaves the others at their defaults. There is no partial-config or
merge mechanism; a `SubscriptionConfig` is always the complete set of decisions
for one subscription.

```python
config = SubscriptionConfig(
    start_from="beginning",
    batch_size=500,
)
```

Validation runs once, in `__post_init__`, and raises `ValueError` for any
invalid value or inconsistent pair (see
[Validation rules](#validation-rules-__post_init__)). Combined with frozenness
this gives a useful guarantee: a `SubscriptionConfig` you hold is a config that
passed validation, and it cannot drift afterwards. Misconfiguration fails at
construction, not halfway through a replay.

The fields fall into five groups, documented in the subsections that follow:

| Group | Fields |
| --- | --- |
| [Starting position and batching](#fields-starting-position-and-batching) | `start_from`, `batch_size` |
| [Checkpointing](#fields-checkpointing) | `checkpoint_strategy`, `checkpoint_interval_seconds` |
| [Timeouts](#fields-timeouts) | `processing_timeout` |
| [Filtering](#fields-filtering-event_types-aggregate_types-tenant_id) | `event_types`, `aggregate_types`, `tenant_id` |
| [Error handling and retry](#fields-error-handling-and-retry) | `continue_on_error`, `max_retries`, `initial_retry_delay`, `max_retry_delay`, `retry_exponential_base`, `retry_jitter` |
| [Circuit breaker](#fields-circuit-breaker) | `circuit_breaker_enabled`, `circuit_breaker_failure_threshold`, `circuit_breaker_recovery_timeout` |

The retry and circuit breaker groups are stored flat on the dataclass and are
not consumed in that form: `get_retry_config()` and
`get_circuit_breaker_config()` assemble them into the `RetryConfig` and
`CircuitBreakerConfig` objects that `eventsource.application.subscriptions.retry` defines.
Those two methods are the only behavior on the class beyond validation —
`SubscriptionConfig` is data, and the runners in
`eventsource.application.subscriptions.runners` are what interpret it.

Defaults, in full, describe a subscription that resumes from its last
checkpoint, reads 100 events per batch, checkpoints once per batch, retries a
failing event up to five times with exponential backoff, dead-letters it and
continues, and trips a circuit breaker after five consecutive failures. If
that description already matches what you want, pass no arguments at all.

#### Fields: starting position and batching

| Field | Type | Default |
| --- | --- | --- |
| `start_from` | `StartPosition` | `"checkpoint"` |
| `batch_size` | `int` | `100` |

These two fields decide where a subscription begins reading and how much of
the stream it pulls in at a time. They are independent: `start_from` is read
once at startup and `batch_size` shapes each catch-up read.

##### `start_from`

Typed [`StartPosition`](#startposition) — one of `"beginning"`, `"end"`,
`"checkpoint"`, or an explicit `Position` token. The default,
`"checkpoint"`, means resume: an existing subscription picks up after its last
saved position, and a subscription that has never checkpointed falls back to
reading from the start of the feed with an info-level log from
`StartFromResolver.resolve()`. If a fresh subscription must *not* read
history, say so explicitly with `"end"` or a `Position`.

The field is consumed only by `StartFromResolver.resolve()` in
`eventsource.application.subscriptions.transition`, which is called once when the
subscription starts. Changing `start_from` on a running subscription is not
possible (the config is frozen) and would have no effect mid-run anyway.

`start_from` no longer accepts a bare `int` — with opaque positions the
integer form is unrepresentable, and `__post_init__` no longer has a numeric
value to range-check. It does **not** validate the string literals either: a
typo such as `start_from="checkpont"` constructs successfully and fails
later, at resolve time, with `ValueError: Unknown start_from value:
checkpont`. The resolver tests `isinstance(start_from, Position)` first, so an
explicit position is never confused with a literal.

`str(config.start_from)` is what `SubscriptionRegistry` records in its
registration log entry.

##### `batch_size`

The maximum number of events the **catch-up runner** requests from the event
store per read. The live runner also reads from the global feed in
`batch_size`-limited chunks (it does not read from the bus — see
[ADR 0047](../adrs/0047-live-runner-feed-driven-checkpointing.md)), but delivers
events from that chunk to the subscriber one at a time regardless of
`batch_size`; it does not (yet) dispatch through `handle_batch()`.

Each iteration of the catch-up loop computes:

```python
remaining = target_position - current_position
batch_limit = min(self.config.batch_size, remaining)
```

so the runner never over-reads past the target position captured at startup,
and a `batch_limit <= 0` ends the batch immediately (returning `0`, which
breaks the catch-up loop). The read always proceeds forward; the limit and
starting position are passed through to the store's `read_all()` as the
`from_position` argument and `FeedReadOptions(limit=batch_limit)` (`read_all`
has no direction parameter — the global feed only reads forward).

`batch_size` interacts with checkpointing: under the default
`CheckpointStrategy.EVERY_BATCH` it is also the checkpoint granularity, so it
bounds how many events are reprocessed after a crash. Larger batches mean fewer
store round-trips and fewer checkpoint writes, and more replay on restart.
`create_catch_up_config()` raises the default tenfold, to `1000`, for exactly
that trade.

Note that `batch_size` is the only per-read memory bound in this config — a
batch is read into a list before its events are dispatched, so a very large
`batch_size` holds a correspondingly large list of `EventEnvelope` objects.
There is no separate concurrency knob: neither runner delivers concurrently.
On the live runner, and on catch-up for a subscriber without `handle_batch()`,
each `handle()` call is awaited to completion before the next event starts, so
at most one event is ever in flight. On catch-up for a subscriber with
`handle_batch()`, the whole read batch is handed to it as one call — still no
concurrent delivery, but the unit of work is the batch rather than the event
(see [Handle events in batches with handle_batch()](../guides/subscriptions.md#handle-events-in-batches-with-handle_batch)
and [ordered delivery](../adrs/0059-ordered-subscription-delivery.md)).

Both the configured value (`config.batch_size`) and the actual per-batch count
(`events_in_batch`) appear in catch-up log records under the key `batch_size`;
the configured value is also set as a span attribute on the catch-up span.

##### Choosing values

| Situation | `start_from` | `batch_size` |
| --- | --- | --- |
| Rebuild a projection from scratch | `"beginning"` | `1000` (or `create_catch_up_config()`) |
| Resume a long-running projection | `"checkpoint"` (default) | default `100` |
| Tail-follow new events only | `"end"` (or `create_live_only_config()`) | irrelevant — live runner |
| Reprocess from a known incident position | that `int` | as for catch-up |

#### Fields: checkpointing

| Field | Type | Default |
| --- | --- | --- |
| `checkpoint_strategy` | `CheckpointStrategy` | `CheckpointStrategy.EVERY_BATCH` |
| `checkpoint_interval_seconds` | `float` | `5.0` |

`checkpoint_interval_seconds` is consulted only under
`CheckpointStrategy.PERIODIC`: both runners compare elapsed time against it and
save when `elapsed >= checkpoint_interval_seconds`. Under the other two
strategies the field is inert (but still validated).

#### Fields: timeouts

| Field | Type | Default |
| --- | --- | --- |
| `processing_timeout` | `float` | `30.0` |

It is validated as positive. `processing_timeout` is enforced: both runners
call the subscriber inside `asyncio.timeout(config.processing_timeout)`, at the
same chokepoint that applies the handler circuit breaker. It bounds **one
handler call** — a `handle_batch()` of 500 events gets the same budget as a
single `handle()`, because it is one call. On expiry the call raises
`TimeoutError` and is treated as an ordinary handler failure: `continue_on_error`
governs whether the subscription continues, the event follows the normal DLQ
path, and the timeout counts toward the handler breaker's consecutive-failure
run (the timeout is applied inside the breaker, not around it, so hangs open the
circuit just as raises do).

`shutdown_timeout` is **not** a `SubscriptionConfig` field. It is declared once,
on the `SubscriptionManager` constructor (default `30.0`), which passes it to the
shutdown coordinator; `stop_all()` and `run_until_shutdown()` accept a per-call
override. The config used to carry a second, inert copy that nothing read — it
was removed in [ADR 0062](../adrs/0062-single-declaration-sites-for-shutdown-timeout-and-retry-policy.md).

#### Fields: filtering (`event_types`, `aggregate_types`, `tenant_id`)

| Field | Type | Default |
| --- | --- | --- |
| `event_types` | `tuple[type[DomainEvent], ...] \| None` | `None` |
| `aggregate_types` | `tuple[str, ...] \| None` | `None` |
| `tenant_id` | `UUID \| None` | `None` |

`None` on any field means "no restriction on this dimension". These three are
consumed by `EventFilter.from_config()` and
`EventFilter.from_config_and_subscriber()` in
`eventsource.application.subscriptions.filtering`, which copy them straight across. All
configured dimensions must match for an event to pass — the filter ANDs them.

Matching semantics, as implemented by `EventFilter`:

- `event_types` matches on **exact class identity** (`type(event) in event_types`).
  A subclass of a listed event type does **not** match. If you need name-pattern
  matching (`"Order*"`), that lives on `EventFilter.event_type_patterns` /
  `EventFilter.from_patterns()`, which `SubscriptionConfig` has no field for.
- `aggregate_types` matches `event.aggregate_type` against the tuple of names.
- `tenant_id` matches `event.tenant_id == tenant_id`, which is how you scope a
  subscription to one tenant for multi-tenant streaming or a tenant-by-tenant
  migration.

`from_config_and_subscriber()` adds one fallback: when `config.event_types` is
`None`, the subscriber's `subscribed_to()` types are used instead. A config that
*does* set `event_types` always wins.

```python
from uuid import UUID

tenant_config = SubscriptionConfig(
    tenant_id=UUID("12345678-1234-5678-1234-567812345678"),
    start_from="beginning",
)
```

#### Fields: error handling and retry

| Field | Type | Default |
| --- | --- | --- |
| `continue_on_error` | `bool` | `True` |
| `max_retries` | `int` | `5` |
| `initial_retry_delay` | `float` | `1.0` |
| `max_retry_delay` | `float` | `60.0` |
| `retry_exponential_base` | `float` | `2.0` |
| `retry_jitter` | `float` | `0.1` |

`continue_on_error` governs what happens once an event has exhausted retries and
been dead-lettered: with the default `True` the runner moves on to the next
event; with `False` the runner stops. Both the catch-up and live runners check
it at that point.

The five `retry_*`/`*_retry_*` fields are stored flat here and assembled into a
`RetryConfig` by `get_retry_config()`. They are not read directly by the
runners.

#### Fields: circuit breaker

| Field | Type | Default |
| --- | --- | --- |
| `circuit_breaker_enabled` | `bool` | `True` |
| `circuit_breaker_failure_threshold` | `int` | `5` |
| `circuit_breaker_recovery_timeout` | `float` | `30.0` |

`circuit_breaker_enabled` is the only one of the three the runners read
directly: when true, they build **two independent `CircuitBreaker`
instances** from `get_circuit_breaker_config()`, one guarding the
subscriber's `handle()`/`handle_batch()` calls (`handler_circuit_breaker`)
and one guarding read-batch/checkpoint-save (`infra_circuit_breaker`). They
share the same threshold and recovery timeout — there is one config knob,
not two — but independent state: a run of handler failures cannot open the
infra breaker and block checkpointing, and a flaky store cannot mask a
broken handler. See [Handle events one at a
time](../guides/subscriptions.md#handle-events-one-at-a-time) for what
feeds the handler breaker and why a DLQ'd event alone never opens it. When
`circuit_breaker_enabled` is false, neither breaker is created and the
other two fields have no effect (they are still validated).

#### Validation rules (`__post_init__`)

Every rule below raises `ValueError` from the constructor. Messages include the
offending value, and most suggest a workable default.

| Rule | Raised when |
| --- | --- |
| `batch_size` must be positive | `batch_size < 1` |
| `processing_timeout` must be positive | `<= 0` |
| `checkpoint_interval_seconds` must be positive | `<= 0` |
| `max_retries` must be `>= 0` | negative (`0` is legal — it means no retries) |
| `initial_retry_delay` must be positive | `<= 0` |
| `max_retry_delay` must be positive | `<= 0` |
| `max_retry_delay` must be `>= initial_retry_delay` | the two are inverted |
| `retry_exponential_base` must be `> 1.0` | `<= 1.0` (exactly `1.0` is rejected — it would never back off) |
| `retry_jitter` must be between 0.0 and 1.0 | outside the inclusive range |
| `circuit_breaker_failure_threshold` must be `>= 1` | `< 1` |
| `circuit_breaker_recovery_timeout` must be positive | `<= 0` |

Note the boundaries: `retry_jitter` accepts both `0.0` and `1.0`;
`retry_exponential_base` is strictly greater than `1.0`;
`start_from` has no numeric range to validate here at all now that the `int`
member is gone — an unrecognized string value is only caught later by
`StartFromResolver.resolve()`.

#### `get_retry_config() -> RetryConfig`

Builds a `RetryConfig` (from `eventsource.application.subscriptions.retry`) from the flat
retry fields. The import happens inside the method, so `config.py` stays free of
a runtime dependency on `retry.py`.

Field mapping — note the names differ on both sides:

| `SubscriptionConfig` | `RetryConfig` |
| --- | --- |
| `max_retries` | `max_retries` |
| `initial_retry_delay` | `initial_delay` |
| `max_retry_delay` | `max_delay` |
| `retry_exponential_base` | `exponential_base` |
| `retry_jitter` | `jitter` |

The two sets of defaults are identical (`5`, `1.0`, `60.0`, `2.0`, `0.1`), so
`SubscriptionConfig().get_retry_config()` equals `RetryConfig()`. A fresh object
is returned on each call. Both runners call this once during startup to
configure their retry handling.

#### `get_circuit_breaker_config() -> CircuitBreakerConfig`

Builds a `CircuitBreakerConfig` the same way, with a locally-scoped import:

| `SubscriptionConfig` | `CircuitBreakerConfig` |
| --- | --- |
| `circuit_breaker_failure_threshold` | `failure_threshold` |
| `circuit_breaker_recovery_timeout` | `recovery_timeout` |

`CircuitBreakerConfig` has a third field, `half_open_max_calls` (default `1`),
that `SubscriptionConfig` does not expose — it always takes the default. If you
need a different half-open allowance, construct the `CircuitBreaker` yourself
rather than going through the subscription config.

The runners call this twice when `circuit_breaker_enabled` is true — once
per breaker — so `handler_circuit_breaker` and `infra_circuit_breaker` are
two separate `CircuitBreaker` objects built from equal config, not one
object shared between both.

### `create_catch_up_config()`

```python
def create_catch_up_config(
    batch_size: int = 1000,
    checkpoint_every_batch: bool = True,
) -> SubscriptionConfig
```

Returns a `SubscriptionConfig` tuned for bulk catch-up: `start_from="checkpoint"`,
`batch_size` as given (default `1000`, ten times the dataclass default), and
`checkpoint_strategy` set to `CheckpointStrategy.EVERY_BATCH` when
`checkpoint_every_batch` is true, `CheckpointStrategy.PERIODIC` when false.
Every other field keeps its dataclass default — retry and circuit breaker
settings are untouched.

The `checkpoint_every_batch=False` branch selects `PERIODIC`, which then uses
the default `checkpoint_interval_seconds=5.0`; it does not disable
checkpointing.

`batch_size` is validated by `__post_init__` as usual, so
`create_catch_up_config(batch_size=0)` raises `ValueError`.

### `create_live_only_config()`

```python
def create_live_only_config() -> SubscriptionConfig
```

Takes no arguments. Returns a `SubscriptionConfig` with `start_from="end"`,
`batch_size=100`, and `checkpoint_strategy=CheckpointStrategy.EVERY_EVENT` —
a subscription that skips history entirely and checkpoints after each event.

`start_from="end"` resolves to the event store's current position token at
startup, so any events written before the subscription starts are never
delivered. This is the intended shape for tail-following consumers that must not
replay history; it is not appropriate for projections that need a complete
stream.

Because the live runner treats `EVERY_BATCH` as `EVERY_EVENT` anyway, the
explicit `EVERY_EVENT` here documents intent more than it changes behavior — the
meaningful alternative in live mode is `PERIODIC`.
