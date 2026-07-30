# Event Bus: Contract Unification and Test Coverage

**Date:** 2026-07-29
**Status:** Approved design, pending implementation plan

## Problem

A review of `src/eventsource/bus/` found that the four `EventBus` implementations
agree on an interface but not on a contract, and that the tooling meant to catch
this is not connected to anything.

The specific findings:

1. **`EventBusConformanceSuite` is dead code.** `src/eventsource/testing/conformance.py`
   defines a six-test EventBus contract suite. No test file subclasses it. It is
   exported from `testing/__init__.py` and referenced nowhere else. Each backend
   instead hand-rolls its own tests at wildly uneven depth --
   `tests/unit/test_rabbitmq_event_bus.py` is 11,663 lines,
   `tests/unit/bus/test_memory.py` is 293 -- so the contract is asserted four
   times, differently, with nothing checking that the four agree.

2. **No property tests in `bus/`.** Hypothesis is used in `serialization/`,
   `multitenancy/`, and `repositories/`, but not for the bus.

3. **No mutation coverage in `bus/`.** `[tool.mutmut] only_mutate` covers
   `engine.py`, `repositories/_dialect.py`, and `serialization/json.py`;
   cosmic-ray covers `checkpoint.py` and `engine.py`. Nothing in `bus/`.

4. **Kafka and RabbitMQ integration tests have never run in CI.**
   `.github/workflows/ci.yml` runs `-m "integration or postgres or redis"`, and
   `docker-compose.test.yml` defines only `postgres` and `redis`. No workflow
   mentions Kafka or RabbitMQ. This leaves 5,329 lines of existing integration
   tests (`tests/integration/bus/test_kafka.py`, `test_rabbitmq.py`) running only
   on developer machines.

Four concrete divergences that the missing conformance suite is hiding:

- **`retry_jitter` means two different things.** `kafka.py:2876` applies one-sided
  positive jitter (`delay * jitter * random()`), so delay only increases and can
  exceed the advertised `max_delay`. `rabbitmq.py:1821` applies symmetric jitter
  (`uniform(-r, +r)` clamped at 0). Same config field name, same docstring
  language, different distributions.
- **`background` has four meanings.** The ABC documents fire-and-forget.
  `redis.py:381` states "Ignored for Redis." Kafka maps it to "don't wait for
  ack," RabbitMQ to "don't wait for confirms," InMemory to "spawn a task."
- **Thread safety is claimed but not honored.** The ABC states implementations
  must be thread-safe. `InMemoryEventBus` uses an `RLock`. `redis.py:507`
  explicitly relies on GIL-atomic list append and takes no lock -- but
  `unsubscribe`'s enumerate-then-pop is not GIL-atomic against a concurrent
  `subscribe`.
- **~500 lines of duplicated subscription code.** `subscribe`, `unsubscribe`,
  `subscribe_all`, `subscribe_to_all_events`, `unsubscribe_from_all_events`,
  `clear_subscribers`, `get_subscriber_count`, and `get_wildcard_subscriber_count`
  are reimplemented near-verbatim in all four backends, differing only in whether
  they lock. `_get_event_class` is byte-identical in redis, kafka, and rabbitmq.

## Scope

In scope: contract unification, shared unit extraction, conformance wiring,
property tests, mutation targets, broker CI, and the behavior fixes forced by
making the contract explicit.

Out of scope, deferred to a later cycle:

- Decomposing `RabbitMQEventBus` (4,419 lines) and `KafkaEventBus` (3,343 lines)
  along the Topology and DLQAdmin seams.
- Adding `bus/memory.py` to the mutation set.

## Decisions

These were settled during design and are not open questions.

| Decision | Resolution |
| --- | --- |
| Backward compatibility | Behavior may change; document in changelog, bump minor version. |
| `background` semantics | "Do not wait for durability" -- return once the event is handed off, without waiting for delivery to be confirmed or handled. |
| Thread safety | Genuinely thread-safe in all four backends, via a lock inside the shared registry. |
| Broker conformance layer | InMemory in unit tests; Redis, Kafka, RabbitMQ in `tests/integration/bus/` behind existing markers, against real services. |
| Composition structure | New `BaseEventBus(EventBus)` concrete base class; `EventBus` ABC stays pure. |
| Jitter | Unify on symmetric (`uniform(-r, +r)` clamped at 0), the RabbitMQ form. |
| Broker CI | Add Kafka and RabbitMQ services and a blocking CI job. |

## ADR Impact

| ADR | Disposition | Notes |
| --- | --- | --- |
| [0001 - Async-First Design](../../adrs/0001-async-first-design.md) | Stands | Nothing here changes the async-first boundary; all four backends remain async. |
| [0007 - Event Bus Delivery Semantics and Tracing Contract](../../adrs/0007-event-bus-delivery-semantics.md) | Amended | D3 (handler errors are caught, logged, and swallowed) and its "publish() will not raise" consequence no longer hold — see [0011](../../adrs/0011-handler-error-isolation-with-no-ack.md). D4 (thread-safety, `threading.RLock` owned by `InMemoryEventBus`) is superseded in detail by the shared `SubscriptionRegistry` — see [0010](../../adrs/0010-uniform-event-bus-contract.md). D1, D2, D5, D6 stand unchanged. |
| [0007 - Handler Registry and Adapter as Collaborators](../../adrs/0007-handler-registry-composition.md) | Stands | `HandlerAdapter` is explicitly unchanged by this work (see "Unchanged" above); the composition-over-inheritance shape it documents is exactly the shape `BaseEventBus` follows for bus-level concerns. |
| New: [0010 - Uniform Event Bus Contract](../../adrs/0010-uniform-event-bus-contract.md) | New | Covers the `background` semantics unification and the `BaseEventBus` concrete layer (`SubscriptionRegistry`, event-class resolution, background-task tracking), including the Kafka name-keyed dispatch bug fix this layer surfaced. |
| New: [0011 - Uniform Handler-Error Isolation with No-Ack-on-Failure](../../adrs/0011-handler-error-isolation-with-no-ack.md) | New | Covers the mid-implementation user ruling that isolation must be uniform: `HandlerDispatchError` is raised on aggregate failure, and broker consume paths withhold ack/commit so redelivery covers the failure. This directly contradicts this document's own "Error handling is unchanged" line under Behavior changes below — that line was accurate at design time and became stale once the Task 8 ruling landed; 0011 is the record of the change, not this spec. |

## Architecture

Four new units, plus changes to the four existing backends.

### `bus/registry.py` -- `SubscriptionRegistry`

The only stateful new piece. Holds
`dict[type[DomainEvent], tuple[HandlerAdapter, ...]]` plus a wildcard tuple,
guarded by an `RLock`.

Public surface: `add`, `remove`, `add_wildcard`, `remove_wildcard`,
`add_subscriber`, `clear`, `count`, `wildcard_count`, `handlers_for`.

Two implementation details carry the performance improvements:

- Handlers are stored as **immutable tuples**, and `handlers_for(event_type)`
  returns a precomputed `(specific + wildcard)` tuple. Dispatch therefore does
  zero allocation per event, replacing the two per-event list copies in
  `memory.py:215`.
- `remove` compares against the raw handler using `HandlerAdapter.__eq__`'s
  existing `self._original is other` branch, so no throwaway `HandlerAdapter` is
  constructed per `unsubscribe` call.

### `bus/retry.py` -- `RetryPolicy`

A frozen dataclass of `base_delay`, `max_delay`, `jitter`, and `max_retries`,
exposing one method: `delay_for(retry_count) -> float`.

Computes `min(base_delay * 2 ** retry_count, max_delay)`, then applies symmetric
jitter `uniform(-r, +r)` clamped at 0. Kafka and RabbitMQ each compose one built
from their existing config fields; InMemory and Redis do not need one.

### `bus/base.py` -- `BaseEventBus(EventBus)`

Concrete base class sitting between the ABC and the four backends.

Responsibilities:

- Composes a `SubscriptionRegistry` and implements the six subscription methods
  concretely, so backends stop implementing subscription entirely.
- Owns `_event_registry` and a `_resolve_event_class(name)` helper replacing the
  three identical `_get_event_class` copies.
- Owns background-task tracking: `_track_background(coro)` and
  `async _drain_background(timeout)`. `InMemoryEventBus` already has
  `_background_tasks` / `_on_background_task_done` / drain-on-shutdown, Kafka has
  its own `_track_background_publish`, and Redis is about to need one to honor
  `background`. Each backend's `shutdown()` delegates to `_drain_background`.

`EventBus` in `interface.py` keeps its shape -- only its `background` and
thread-safety docstrings change. Third parties can still implement `EventBus`
directly without inheriting this machinery, preserving the interface/
implementation split required by `.claude/rules/architecture.md`.

Resulting hierarchy:

```
interface.py:  EventBus(ABC)           # pure: publish + 6 abstract subscription methods
base.py:       BaseEventBus(EventBus)  # + SubscriptionRegistry, background tasks, event resolution
memory.py:     InMemoryEventBus(BaseEventBus)
redis.py:      RedisEventBus(BaseEventBus)
kafka.py:      KafkaEventBus(BaseEventBus)
rabbitmq.py:   RabbitMQEventBus(BaseEventBus)
```

### `testing/recording.py` -- `RecordingEventBus`

A decorator wrapping any `EventBus`, capturing `published_events` with an
optional `max_events` bound.

This removes a test affordance from production code:
`InMemoryEventBus._published_events` is an unbounded, never-trimmed list, so
long-lived processes using the in-memory bus leak memory proportional to total
events published. `InMemoryEventBus.published_events` remains for one release as
a deprecated property that emits a `DeprecationWarning` and delegates to an
internally held recorder.

### Unchanged

`HandlerAdapter`, the `EventBus` ABC's shape, and the four backends' transport
code (connection lifecycle, topology declaration, DLQ handling, consumption
loops).

## Behavior changes

Each is a user-visible change requiring a changelog entry.

**Jitter unifies on symmetric.** Kafka's one-sided positive jitter only ever
pushes delays up, which both violates the `max_delay` cap it advertises and fails
to spread a thundering herd in the direction that relieves the broker. Kafka's
effective backoff becomes slightly shorter.

**Redis honors `background`.** `publish(..., background=True)` hands pipeline
execution to `_track_background`, returns immediately, and the task is drained by
`shutdown()`. The "Ignored for Redis" docstring line is removed.

**Kafka stops publishing serially.** `kafka.py:1479` currently loops
`await self._publish_single_event(...)`, costing one broker round-trip per event,
while Redis pipelines into one round-trip and RabbitMQ gathers concurrently.
Kafka becomes: send all events to the producer, collect the futures, then
`gather` them once -- or, for `background=True`, register them with
`_track_background` without awaiting. The partition key stays `aggregate_id`, so
per-aggregate ordering is preserved.

**Redis `tenant_id` sentinel is documented, not changed.** `_serialize_event`
writes `""` for `None`, but `_deserialize_event` reads only `payload` and ignores
every flat field. The flat fields are write-only index columns. This stays as-is,
with a comment and a property-test assertion that `payload` is authoritative --
changing a serialization format here would carry migration cost for no behavioral
gain.

**Error handling is unchanged.** Handler errors stay isolated and logged in
`_safe_handle`; the DLQ and retry paths keep their current structure.

One item to verify during implementation rather than assume: `Tracer.span` is
entered twice per handler per event on the hot path, and tracing defaults to on.
If it is not a genuine no-op context manager when disabled, that is a real cost
and should be fixed; if it already is, no action.

## Testing

### Conformance suite: 6 tests to 9

The existing six are unchanged. Three are added, each covering something this
work makes explicit:

- `test_background_publish_delivers` -- `publish(events, background=True)` returns
  without raising and the event still arrives.
- `test_per_aggregate_ordering` -- events for a single `aggregate_id` arrive in
  publish order. Deliberately per-aggregate, not global: Kafka partitions by
  `aggregate_id`, so global ordering is not a contract any distributed backend
  can honor, and asserting it would make the suite untrue.
- `test_subscribe_all_registers_declared_types` -- `subscribe_all` reaches every
  type returned by `subscribed_to()` and nothing else.

The suite gains one overridable hook, `async def await_delivery(self, bus)`,
defaulting to a no-op for InMemory and to a bounded poll for brokers, so shared
assertions do not need to know about eventual delivery.

Wiring:

```
tests/unit/bus/test_memory.py           -> conformance (fast, no Docker)
tests/integration/bus/test_redis.py     -> conformance  [redis]
tests/integration/bus/test_kafka.py     -> conformance  [kafka]
tests/integration/bus/test_rabbitmq.py  -> conformance  [rabbitmq]
```

### Property tests

All unit-level and fast, in `tests/unit/bus/`:

- `test_registry_properties.py` -- for any sequence of
  add/remove/add_wildcard/remove_wildcard operations, `count()` equals the net
  specific count, `wildcard_count()` the net wildcard count, `remove` returns
  `True` iff the handler was present, and `handlers_for` returns specifics before
  wildcards with no duplicates. This is the test that would have caught the
  Redis/InMemory divergence.
- `test_retry_properties.py` -- for any `retry_count` in 0..64 and any valid
  policy, `delay_for` is finite, `>= 0`, `<= max_delay`, and its jitter-free
  expectation is non-decreasing. Seeded RNG for determinism.
- `test_serialization_properties.py` -- for any `DomainEvent` drawn from a small
  strategy (including `tenant_id=None` and unicode payloads), each backend's
  `_serialize_event` -> `_deserialize_event` roundtrip is identity. Runs against
  the pure serialization methods with no broker.
- `test_error_isolation_properties.py` -- for any subset of N handlers that raise,
  the complementary subset still receives the event and the error count matches.
  Runs against `InMemoryEventBus`.

### Mutation testing

`[tool.mutmut] only_mutate` gains `bus/registry.py`, `bus/retry.py`, and
`bus/base.py` -- three small, pure, I/O-free modules matching the shape the
existing config is scoped for, with `pytest_add_cli_args_test_selection` extended
to the corresponding unit tests.

`bus/memory.py` is a candidate but is held back to a follow-up: the runtime
rationale documented in `cosmic-ray/engine.toml` argues for adding targets one at
a time and measuring. `kafka.py` and `rabbitmq.py` stay out permanently.

### Broker CI

`docker-compose.test.yml` gains `kafka` and `rabbitmq` services. `ci.yml` gains a
job running `-m "kafka or rabbitmq"` as a required check.

This retroactively switches on 5,329 lines of integration tests that have never
run in CI. Those tests may not currently pass. Any failures they surface are in
scope for this work: the job lands as a required check, so the work is not done
until it is green.

### Coverage floor

`fail_under = 86` will move. Extracting well-tested units out of partially-tested
files shifts the denominator unpredictably, so the floor is re-measured after the
work lands and raised to just under the new number, preserving its role as a
regression ratchet rather than a stretch target.

## Implementation sequence

Seven steps, each landing green, ordered so the safety net exists before the
changes it protects.

0. **Get CI green.** As of 2026-07-29 four jobs fail on `main` and on every PR,
   for three pre-existing causes unrelated to the bus:
   - `lint`: `ruff format --check .` now reformats Python code blocks inside
     Markdown (ruff >= 0.14 behavior). `ruff>=0.14.8` is unpinned in
     `[project.optional-dependencies]`, so a ruff release enabled this and
     `.claude/agents/*.md` began failing. `ruff check` still passes.
   - `import-linter`: `lint-imports: command not found`. `import-linter` is
     declared in `[dependency-groups] dev` (PEP 735), but CI runs
     `pip install -e ".[dev,all]"`, which reads
     `[project.optional-dependencies]`. PEP 735 groups are not installed by
     extras, so the binary is never present.
   - `audit`: identical cause for `pip-audit`.

   Resolutions:
   - Exclude Markdown from `ruff format` via `[tool.ruff] extend-exclude`, and
     pin `ruff>=0.14.8,<0.15`. The Python in `.claude/agents/*.md` is
     illustrative and deliberately abbreviated; reformatting it serves no one,
     and the pin stops a future ruff release from silently widening what
     `--check` covers again.
   - Make CI install the PEP 735 group explicitly (`pip install --group dev`)
     rather than relying on the `[dev]` extra. This is preferred over
     duplicating the tools into `[project.optional-dependencies]`, which would
     leave two lists to drift apart.

   This work depends on a trustworthy CI signal -- step 4 adds a required check,
   which is meaningless while `main` is red -- so this is a prerequisite, not
   optional cleanup. It is small and independent, and should land as its own PR
   ahead of the rest.

1. **Extract `SubscriptionRegistry` and `RetryPolicy` with their property tests.**
   Pure addition; no existing code touched.
2. **Add `BaseEventBus`; migrate `InMemoryEventBus` onto it.** One backend, the
   simplest, with the most existing unit coverage to catch regressions. Includes
   background-task tracking and `_resolve_event_class`.
3. **Wire conformance into `InMemoryEventBus`;** add the three new conformance
   tests and the `await_delivery` hook.
4. **Add Kafka and RabbitMQ services and the CI job.** Done before the broker
   migration so that step 5's conformance failures are visible in CI rather than
   only locally. Fix whatever the newly-running existing suites surface.
5. **Migrate Redis, Kafka, and RabbitMQ onto `BaseEventBus`;** subclass
   conformance in integration. This is where the ~500 duplicated lines are
   deleted and where conformance starts failing on the real divergences. Fix what
   turns red -- Redis `background`, Kafka batch publish, unified jitter -- with a
   failing-test-first commit each, per the bugfix checklist in
   `.claude/rules/definition-of-done.md`.
6. **`RecordingEventBus` and deprecation shim; mutation targets; coverage floor
   re-measured; changelog and minor version bump.**

Note: steps 4 and 5 are reordered relative to the design conversation, where CI
was step 6. Standing the CI job up before the broker migration means the
conformance failures that migration is designed to surface appear in CI rather
than only on a developer machine, which is the entire point of adding the job.

## Risks

**CI is entirely red before this work starts.** Step 0 addresses three
pre-existing failures. Until it lands, no step in this plan can be verified by
CI, and the "each step lands green" sequencing above is unenforceable. If step 0
turns out to be larger than the three causes identified, it should be split out
and completed before the rest of this work begins.

**The newly-enabled broker suites are of unknown state.** 5,329 lines of Kafka and
RabbitMQ integration tests have never run in CI and may fail on their first
green-field run. The size of that backlog cannot be known until step 4 runs. If
it proves large, the decision to make the job blocking should be revisited with
the user rather than silently downgraded to non-blocking.

**`tests/unit/test_rabbitmq_event_bus.py` is 11,663 lines of mostly-mocked tests.**
Moving RabbitMQ onto `BaseEventBus` will break whichever of them reach into
`_subscribers` directly. This is mechanical but not small. Preference is to delete
tests made redundant by the shared registry's property tests rather than
mechanically patch them.

**The `published_events` deprecation touches test helpers across the repo,** so it
is sequenced last.

## Definition of done

Per `.claude/rules/definition-of-done.md`, plus:

- All CI jobs green, including the three pre-existing failures from step 0.
- `EventBusConformanceSuite` subclassed by all four backends.
- All four property test modules present and passing.
- `-m "kafka or rabbitmq"` green as a required CI check.
- Behavior changes recorded in the changelog with a minor version bump.
- `uv run mypy src/eventsource/`, `uv run ruff check`, and `uv run ruff format`
  pass.
