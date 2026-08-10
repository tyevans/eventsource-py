# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

A backpressure release, in the sense that it establishes there is no
backpressure to configure. `SubscriptionConfig.max_in_flight` and the semaphore
behind it looked like a concurrency control and were not one: both subscription
runners await each event to completion before starting the next, so the bound
never engaged and the threshold could never trip. Rather than implement the
concurrency the knob implied, the decision recorded in the new ordered-delivery
ADR is that delivery is sequential *on purpose* -- a subscription's position is
a single scalar advanced in lockstep with delivery, and concurrent completion
would let progress outrun work and silently skip events on restart. The knob is
deleted, the invariant is written down, and the sanctioned way to go faster --
handing a subscriber a whole batch, which still settles before the position
moves -- is now wired on the catch-up path.

Pulling that thread found the same shape elsewhere. Several bounds were
missing where it mattered: the live drain, and `replay()`, each read an entire
feed into memory in one call, because every store adapter materializes a result
set before yielding its first envelope. Several knobs were the reverse -- declared,
documented, and read by nothing: a projection coordinator's polling interval
with no polling loop, a reconnect delay with no backoff, a dual-write timeout
nothing checked. And `batch_size` meant three unrelated things across three bus
adapters, one of them in bytes.

Two fixes have operational consequence beyond tidiness. Kafka DLQ clients were
built through a second, independently-maintained security config that had
drifted: it omitted `ssl_check_hostname`, so setting it was silently ignored on
those connections. And a tenant-scoped subscription read every tenant's rows
from the feed once it went live -- filtered correctly before delivery, so not a
data leak, but every such subscription was over-fetching instead of using the
indexed predicate.

### Breaking

- **`SubscriptionConfig.max_in_flight` and `backpressure_threshold` are deleted, and `FlowController` no longer limits concurrency.** Both subscription runners await each event's `handle()` to completion before starting the next, so at most one event was ever in flight per subscription and the semaphore these fields configured never blocked -- a real-runner reproduction measured `peak_in_flight == 1` with `max_in_flight=1000`. `FlowController` now only tracks in-flight count and gives graceful shutdown a drain latch (`wait_for_drain`, used by `SubscriptionManager` shutdown); its constructor takes no arguments. Also deleted: `FlowController.is_paused`, `is_backpressured`, `utilization`, `available_capacity`, `wait_for_capacity` (zero callers anywhere in the tree), and the `FlowControlStats` fields `peak_in_flight`, `pause_count`, `total_pause_time_seconds`. `HealthCheckConfig.backpressure_warning_duration_seconds` and `backpressure_critical_duration_seconds` are deleted along with `SubscriptionHealthChecker`'s `flow_controller` constructor parameter and its `_check_backpressure` indicator -- the duration thresholds were never measured against anything, so the warning/critical distinction could never fire. No shim, per the pre-1.0 no-shim policy.
- **`batch_size` is renamed on all three broker bus configs, because it named three different quantities.** Kafka's was a producer send-buffer threshold in *bytes*; Redis's was a consumer read *count* in events; RabbitMQ's was a publish *chunk size* in events -- with Redis and RabbitMQ on opposite sides of the wire from each other. A user configuring one adapter from another's docs was wrong by roughly 160x with no error. Each field is renamed to say what it measures and which side of the wire it is on: `KafkaEventBusConfig.batch_size` -> `producer_max_batch_bytes`, `RedisEventBusConfig.batch_size` -> `stream_read_count` (and `RedisEventBus.recover_pending_messages()`'s `batch_size` parameter with it), `RabbitMQEventBusConfig.batch_size` -> `publish_chunk_size`. Wire behavior is unchanged; only the names are. `SubscriptionConfig.batch_size` is untouched -- it is a different, correctly-named field.
- **`ProjectionCoordinator` loses `batch_size`, `poll_interval_seconds`, and a `start()` method that never existed.** The class docstring claimed it "sets up the event bus connection" and "polls event store", with a usage example calling `await coordinator.start()` -- verified against full history as never having existed. `batch_size` and `poll_interval_seconds` were stored and read only by `health_check()`, which echoed them back into a stats dict; there was no polling loop to configure. All four fields are deleted and the docstring now describes what the class is: a dispatch coordinator over `dispatch_events`, `rebuild_all`, `rebuild_projection`, `catchup`, and `health_check`, driven by a caller that already has the events.
- **`RabbitMQEventBusConfig.max_reconnect_delay` is deleted.** It was documented as capping exponential reconnect backoff, and nothing read it -- `aio-pika`'s `connect_robust` owns reconnection, and this backend has never implemented a backoff of its own.
- **`MigrationConfig.dual_write_timeout_minutes` is deleted.** It was validated (`must be >= 1`), serialized through `to_dict`/`from_dict`, and documented as "Max time in dual-write phase (default 30)" -- but nothing in the dual-write phase read it, checked it, or enforced any bound. Every sibling `MigrationConfig` field traces to a real read site; this was the only one that didn't. No shim, per the pre-1.0 no-shim policy.
- **`SQLITE_AVAILABLE` is gone; use `AIOSQLITE_AVAILABLE`.** Two names guarded the identical `import aiosqlite` and were therefore always equal, both re-exported from `eventsource` with nothing distinguishing them. The redundant per-module `KAFKA_AVAILABLE` and `RABBITMQ_AVAILABLE` declarations are likewise collapsed to one apiece; the publicly re-exported names are unchanged for those two backends.
- **`RoutingError` is deleted.** It was defined in the migration exception hierarchy and never raised, never caught, and never documented as catchable. `StoreNotFoundError` covers the one routing failure that is actually detected.
- **`SubscriptionConfig.processing_timeout` is now enforced, where before it was read by nothing.** The field was validated at construction (`must be positive`), documented as "Max seconds to wait for event processing", and never read outside `config.py` -- both runners awaited the subscriber as a bare `await`, so a handler that hung blocked its subscription **forever**, with no timeout, no error, no metric and no log line. The subscription simply stopped making progress, and the only symptom was lag. Both runners now call the subscriber inside `asyncio.timeout(config.processing_timeout)`, at the same chokepoint that applies the handler circuit breaker. This is listed as breaking because the default is `30.0` and it has never fired before: a handler that legitimately takes longer than 30s now raises `TimeoutError` where it previously ran to completion. It bounds **one handler call**, so a `handle_batch()` of 500 events shares one 30s budget rather than getting 500 of them -- if you deliver large batches and do slow per-event work, raise `processing_timeout` to match the batch. A timed-out call is an ordinary handler failure from that point on: `continue_on_error` governs whether the subscription proceeds, the event takes the normal DLQ path, and because the timeout is applied *inside* the breaker rather than around it, a run of hangs opens the handler circuit exactly as a run of raises does.
- **Both runners' `circuit_breaker` property is split into `handler_circuit_breaker` and `infra_circuit_breaker`, each backed by its own `CircuitBreaker` instance.** Previously one breaker was shared between checkpoint-save/read-batch retries and (now) handler-call gating; sharing meant a run of handler failures could open the circuit and then immediately block the following checkpoint-save too, since it uses the same breaker -- a real reproduction crashed a subscription entirely on 5 consecutive handler failures with default settings. Both breakers are built from the same `CircuitBreakerConfig` (there is one threshold/recovery-timeout knob, not two), with independent state. `SubscriptionHealthChecker`'s health output follows: the single `"circuit_breaker"` indicator is now two, `"handler_circuit_breaker"` and `"infra_circuit_breaker"`, reported independently.

### Added

- **`eventsource.snapshot.miss`, a counter for snapshot reads that fell back to full event replay.** ADR 0017 recorded, as a known negative, that "silent failure means snapshot loss is only visible in logs/metrics" -- a snapshot is a cache, so every failure degrades to a correct-but-slower load and nothing surfaces. The counter closes that: it is keyed by `reason` (`missing`, `schema_mismatch`, `store_error`, `deserialization_error`, `state_restore_failed`) and `aggregate_type`, and the reasons separate on the axis an operator acts on -- **permanence**. A store outage is transient and hits every aggregate at once; a corrupt row is permanent for one aggregate and costs a full replay on every load of it, forever, until the row is rewritten. Both previously logged at `WARNING` and were indistinguishable. OpenTelemetry is optional as ever (ADR 0016); `snapshot_miss_counts()` exposes the same tally in-process for callers without an exporter.
- **Migration metrics and the migration audit log are wired to real call sites.** `MigrationMetrics`' recorders and the `ATTR_MIGRATION_*` span attributes were defined and never emitted; `MigrationAuditLogRepository` was fully implemented, with an adapter, and had no application-ring consumer since the day it was written -- so an operator who injected an audit repository expecting phase transitions to be recorded got an empty table.
- **`max_background_tasks` on all three buses that publish in the background, bounding `publish(background=True)`** (`InMemoryEventBus`, `RedisEventBus`, `KafkaEventBus`; default `DEFAULT_MAX_BACKGROUND_TASKS` = 1000, `None` to disable). Each background publish spawned a tracked `asyncio.Task` with no ceiling, so a producer faster than its handlers grew the in-flight set without limit -- the same negative ADR 0021 and ADR 0017 record for background snapshot scheduling, in a second place -- and `shutdown()` then had to wait for or cancel all of it. **At the ceiling, publishing runs inline rather than blocking on a slot**, which is the load-bearing part: a handler running inside a background publish task may itself publish, and if that inner call waited for a slot it would wait for one held by the task it is running inside. Running inline completes instead, so no `contextvars` re-entrancy guard is needed. Dropping at capacity was rejected -- it would contradict ADR 0007's at-least-once promise, invisibly. The cost is that `publish(background=True)` is only non-blocking while there is headroom; once saturated it takes as long as delivery takes, which is the backpressure the bound exists for. Recorded in `docs/adrs/0060-bounded-background-publishing.md`, which amends ADR 0010: `BaseEventBus._track_background` is now a coroutine, so an adapter implementing `background=True` must `await` it.
- **`ProjectionRegistry` and `SubscriberRegistry` accept an optional `max_concurrency`.** Fan-out within a single event was unbounded; the cap is enforced by one semaphore owned by the registry instance rather than one per call.
- **`replay()` accepts `batch_size`** (`REPLAY_BATCH_SIZE`, default 1000), the size of each feed read.

### Fixed

- **The catch-up runner now dispatches through `handle_batch()` for subscribers that implement it.** `BatchSubscriber`/`BatchAwareSubscriber`/`FilteringSubscriber` and `supports_batch_handling()` were declared, implemented, and documented, but no runner ever called `handle_batch()` -- both runners delivered one event at a time regardless. `CatchUpRunner` now detects batch capability once at construction and, when present, delivers each read batch as one `handle_batch()` call, with the position advancing only after the whole batch settles (preserving the invariant recorded in `docs/adrs/0059-ordered-subscription-delivery.md`). A `handle_batch()` that raises falls back to per-event delivery through `handle()`, per `BatchSubscriber`'s documented contract, reusing the existing retry/DLQ/`continue_on_error` machinery rather than inventing partial-batch semantics. The live runner does not dispatch through `handle_batch()` yet -- tracked separately, since its per-envelope stop/pause responsiveness was built on per-event delivery.
- **A subscription with a high error rate reported healthy.** `ErrorStats.error_rate_per_minute` was declared, serialized by `to_dict()`, and read at four sites that gate health -- including `SubscriptionHealthChecker._check_errors`, which degrades a subscription when the rate exceeds `HealthCheckConfig.max_error_rate_per_minute` (default 10.0). It had **no write site**: `record_error()` updated every other counter and left this one at its `0.0` default, so the comparison was `0.0 > 10.0` on every call and the rate axis of that indicator could never fire. The count-based half of the same indicator did work, which is what kept it invisible. It is now a computed property over a rolling 60-second window -- a *rate*, not a lifetime average, because the gate asks whether a subscription is erroring **now**: a lifetime average would hold a recovered subscription DEGRADED for hours after an incident and would barely move for a long-lived subscription that just started failing everything. The window is 60 one-second buckets rather than a list of timestamps, so it costs the same whether the subscription sees one error a minute or a storm -- the error path is exactly where an unbounded structure would grow fastest.
- **A `SyncSubscriber` failed every event forever, and blamed the user's handler.** `SyncSubscriber` is a published Protocol whose `handle()` is declared *not* async, but both runners awaited every handler call's return value unconditionally. A sync handler returns `None`, and `await None` raises `TypeError: object NoneType can't be used in 'await' expression` -- **inside** the runners' generic `except Exception` around handler dispatch. So it was recorded as a handler failure and attributed to user code, while the handler body had in fact already run successfully: the side effect happened, then the runner failed on the return value. Under `continue_on_error=True` every event "failed" forever while the subscriber worked correctly; under `continue_on_error=False` a perfectly good handler stopped the subscription on its first event. Both runners now settle the handler's result conditionally (`settle_handler_result`, new in `application/subscriptions/subscriber.py`), which covers the async and sync Protocols with no capability detection. Note that `processing_timeout` cannot interrupt a *sync* handler -- asyncio timeouts only fire at an await point, and a sync handler has none. Also routed the three sites that called `subscriber.subscribed_to()` directly through `get_subscribed_event_types()`, whose validation had consequently never run.
- **A `BatchSubscriber`-only subscriber raised `AttributeError` per event on the live path.** `BatchSubscriber` requires only `subscribed_to()` and `handle_batch()` -- not `handle()` -- and catch-up honors that, but `LiveRunner` called `subscriber.handle(event)` unconditionally. A subscriber implementing exactly the published Protocol therefore failed on every event the moment its subscription transitioned to live: each one recorded as a handler failure, counted toward the handler circuit breaker, and under `continue_on_error` able to fill the DLQ with events whose handler was never reached. The docs compounded it by saying such a subscriber "will not receive live events", which reads like a silent no-op rather than a per-event exception. The live runner now detects the subscriber's handlers once at construction and delivers `handle_batch([event])` -- a one-event batch, the subscriber's only handler -- when there is no `handle()`. `handle()` still wins whenever it exists, so nothing changes for subscribers that already worked, and per-envelope stop/pause granularity is untouched; batching the live path proper remains tracked separately. A subscriber implementing *neither* method is now rejected with a `TypeError` at runner construction, naming the subscription, instead of failing per event at runtime.
- **`stop()` could not interrupt a runner parked on a pause, in either runner.** `Subscription.wait_if_paused()` awaited an event that only `resume()` sets, and every stop check sat *after* that await -- so a paused subscription parked its drain loop inside the wait and never reached the check. `stop()` set its flag and returned having stopped nothing; the runner stayed parked until somebody resumed it, and a `SubscriptionManager` shutdown waiting on that runner waited with it. `stop()` was therefore unreliable in exactly the state an operator chooses deliberately, and the state a shutdown is most likely to find. `wait_if_paused()` now takes an optional `stop_signal` event and returns on whichever of resume-or-stop arrives first; both runners pass their own. Waking on stop is **not** an implicit resume -- the pause event is untouched, so a subscription stopped while paused still reports as paused and an operator's deliberate pause survives a runner restart. Each runner's `_stop_requested` bool became an `asyncio.Event` (a bool cannot be awaited) with the old name kept as a derived read-only property, so "stop was requested" still has one writable site rather than two copies that can disagree. The public `CatchUpRunner.stop_requested` property is unchanged.
- **Kafka DLQ clients silently ignored `ssl_check_hostname`.** `KafkaDLQAdmin` built its own consumer kwargs from a second, independently-maintained security dict rather than the main config builder, and that dict had drifted: it omitted `ssl_check_hostname` entirely, so a user who set it got it applied to the main consumer and dropped on every DLQ connection. `get_consumer_config()` now accepts keyword overrides and is the single construction path for every `AIOKafkaConsumer` this backend builds, so a field added to it cannot skip DLQ clients again.
- **A tenant-scoped subscription read every tenant's events from the feed once it went live.** `_drain_feed` built `FeedReadOptions` with only `limit`, dropping the `tenant_id` that catch-up passes. `EventFilter` still discarded other tenants' events before delivery, so this was not a delivery-level leak -- but every SQL/Kafka/RabbitMQ-backed tenant-scoped live subscription over-fetched every tenant's rows instead of using the indexed predicate, and the two runners disagreed on call shape.
- **The live drain read the entire global feed in one call, and `stop()`/`pause()` could not interrupt it.** Every store adapter materializes a feed read fully before yielding its first envelope, so a busy feed made each drain a memory hazard proportional to the backlog. It now reads in `batch_size` batches, matching catch-up. Separately, `_running` was never consulted inside the drain and `wait_if_paused()` only at the wake gate, so a stop or pause requested mid-drain took effect only after the whole feed had been processed.
- **`replay()` read the entire feed into memory in one call.** Same adapter behavior, on the operation most likely to be run against the whole log. Its `max_events` guard could not prevent this -- it counts envelopes already in hand, so it fires only after the allocation it would have prevented. `replay()` now reads in bounded batches and folds each before reading the next; `from_position` is exclusive, so no event is seen twice or skipped.
- **`RabbitMQPublisher` built a fresh semaphore on every publish call**, so `max_concurrent_publishes` bounded concurrency only *within* one call. Two overlapping `publish_many()` callers each got their own ceiling, making the real bound the configured value times the number of concurrent callers. The semaphore is now constructed once per publisher and shared by every publish path.
- **Ten "not connected" sites across the bus adapters raised bare `RuntimeError`** instead of `EventBusConnectionError`, so the library's own exception type could not be used to catch the condition it names.
- **`StoreNotFoundError` and `SubscriptionMigrationError` are now raised.** Both were defined and exported; `migrate_subscriptions()` even documented `Raises: SubscriptionMigrationError` in its docstring, and no code path raised it.
- **The SQL snapshot stores now raise `SnapshotDeserializationError` for an unreadable payload**, honoring the contract ADR 0017 publishes for `SnapshotStore` implementors. No shipped store implemented it, so corruption surfaced one step later as a generic restore failure, and the counter's `deserialization_error` reason was reachable only by a hand-written test double.
- **Circuit breakers now observe handler failures.** `RetryableOperation`/`CircuitBreaker` in both runners previously wrapped only checkpoint-save and read-batch -- never the subscriber's `handle()`/`handle_batch()` call -- so `circuit_breaker_failure_threshold` could never trip on the failure mode it is documented for. Every handler outcome now feeds a dedicated `handler_circuit_breaker` (see the Breaking entry above for why it is a separate instance from the infra breaker), gated but not retried: `CircuitBreakerOpenError` is raised instead of calling `handle()` at all while open, and "nothing retries your handler" stays true either way. A DLQ'd or `continue_on_error`-swallowed failure needs no special case -- `CircuitBreaker.record_success` resets the consecutive-failure count on the very next success, so an isolated bad event never opens the circuit; only a *run* of consecutive handler failures does.
- **`SubscriptionHealthChecker` now reports an OPEN circuit breaker.** Its `circuit_breaker`/`retry_operation` constructor parameters were accepted but the only production construction site (`HealthCheckProvider.register_subscription`) never passed them, so `manager.health_check()` could never report a tripped breaker even though the runners built real ones. Replaced with `handler_circuit_breaker_provider`/`infra_circuit_breaker_provider` -- zero-argument callables looked up fresh on every check, since the underlying `CircuitBreaker` instances change identity when a subscription transitions from catch-up to live. The never-read `retry_operation` parameter is deleted outright; no health indicator ever existed to wire it to.

## [0.13.0] - 2026-08-09

A conformance release. Every item here is one fact that was stored in more than
one place, with nothing that failed when the copies disagreed: three verbatim
copies of the read-model operator dispatch that had drifted apart, an in-memory
store that did not enforce what the SQL stores enforce, eight exception families
that were not reachable from the base class the docs told you to catch, and two
front-door pages describing a version three minors old. The common repair is the
same in each case -- state the fact once, and pin it in a shared conformance
suite that runs against every backend rather than in one adapter's own tests.

Four breaking changes, none of which require code edits for a correct consumer:
a protocol attribute nobody read is deleted, and the other three tighten
in-memory behavior to match what SQLite and PostgreSQL always did. If your tests
pass against a SQL backend today, they pass against 0.13.0's memory backend too
-- and that is precisely the guarantee that did not hold before.

### Breaking

- **In-memory stores reject unregistered event types at append.** `InMemoryEventStore` accepted an `event_registry` and never read it, while the SQL backends require one. A `DomainEvent` subclass missing `@register_event` was therefore invisible in memory-backed tests and raised `EventTypeNotFoundError` on the first read against SQLite or PostgreSQL -- falsifying the library's headline promise that the code you test against is the code that runs. `InMemoryEventStore` and the partitioned in-memory store now validate at append time through a shared `check_registered`, and `InMemoryEventStore` falls back to `default_registry` exactly as the SQL adapters do. The check is pinned in the shared appender conformance suite and runs against memory, partitioned, SQLite, and PostgreSQL. Breaking only for a test suite that had unregistered events and had never run against a SQL backend -- in which case it was already failing there. The fix is `@register_event` on the class, or a module-local `EventRegistry` threaded into the store; a blanket sweep does not work, because `EventRegistry.register` rejects one name mapping to two classes and colliding class names across modules then break at import.
- **`EventAppender.max_append_batch` is deleted.** It was declared on the protocol, set to `None` by all six implementers, and read by nothing. Because it is a data attribute rather than a method, every hand-rolled test double and `Mock(spec=...)` had to declare it to satisfy mypy -- a tax on custom-backend authors for a fact no code consumed. **No shim**, per the pre-1.0 no-shim policy. Custom backends should delete their declaration; nothing reads it either way.
- **An unknown filter operator now raises on the memory backend.** It returned `False` per row, silently dropping every result, where the SQL backends raised. The three backends now share one operator table, so an operator outside it fails the same way everywhere.
- **`ne` and `not_in` now match a field whose value is `None` on the SQL backends.** They previously did on memory (Python semantics) and did not on SQLite or PostgreSQL (SQL three-valued logic). `find()` is defined over read models -- objects whose optional fields are `None` -- not over table rows, so the port now states that a field with no value is distinct from every value, and the SQL renderers implement it. SQL's old answer was the invisible one: a smaller result set, never an error.

### Fixed

- **Every library exception is now reachable from `EventSourceError`** (ADR 0058). Eight families inherited bare `Exception` (`ShutdownError`, `BatchPublishError`, `StoreNotFoundError`, `WritePausedError`, `RetryError`, `CircuitBreakerOpenError`, `SnapshotError`, `ReadModelError`) and a ninth, `RabbitMQNotAvailableError`, inherited `ImportError`, so the documented catch-all `except EventSourceError` silently missed them -- including all four snapshot errors, which are exported from the top-level `__all__`. A package-walking guard test asserts the property from both directions and keeps an exact list of sanctioned holdouts, so a new orphan family fails CI rather than accruing quietly. Note that `except EventSourceError` now catches strictly more than it did; code that relied on a snapshot or migration error escaping it will now see it caught. `tests/unit/domain/test_exceptions_home.py` had asserted `not issubclass(SnapshotError, EventSourceError)` -- a test encoding the bug as the spec -- and is inverted.
- **Read-model filter semantics are unified across all three backends.** The operator dispatch was three verbatim copies that disagreed, and is hoisted into `adapters/_common/readmodel_filters.py` behind a single operator table read by both a Python predicate and a dialect-parameterized SQL renderer. Beyond the two divergences listed under Breaking, an unknown field name was a silent no-match in memory and an error in SQL.
- **`Query.offset` without a `limit` no longer fails on SQLite.** The adapter emitted a bare `OFFSET`, which is a syntax error in SQLite; the combination had never worked. Found by the expanded conformance suite, not by anyone using it.
- **`test_event_registry.py` no longer empties the process-global registry.** It cleared `default_registry` in setup and teardown and never restored it, permanently emptying it for every test that ran afterward -- the reason three modules failed only in a full run, and the source of the full-suite "hang".
- **The benchmark harness builds its in-memory store with a registry**, as its PostgreSQL and SQLite siblings two lines below always did.

### Docs

- **The README's decider quickstart runs.** It reused an `OrderState` whose `order_id` was required while `initial_state()` returned `OrderState()`, so `create_new()` raised `ValidationError` before any command executed -- fallout from ADR 0056 moving identity onto the command. The same block taught three practices this codebase forbids: hand-declared `event_type` (a 246-site sweep deleted these), `float` for money, and manual `aggregate_id`/`aggregate_version` instead of `create_event()`. `getting-started` was already correct on all three; this was drift between two front doors. Both now open with the smallest program that works rather than 14 concepts and 175 lines, the README's projection and `SubscriptionManager` material moves below the fold, and its `await asyncio.sleep(0.1)` is labelled a script-only shortcut. Every code block was extracted after editing and executed; the documented output is real output.
- **`installation.md`, `faq.md`, and `tutorials/index.md` describe the current release rather than 0.5.0.** The pin advice (`>=0.5,<0.6`) was actively wrong, and the pages named pre-rings paths that no longer exist (`events/base.py`, `stores/postgresql.py`, `snapshots/postgresql.py`, `locks/postgresql.py`, `migration/repositories/`, `readmodels/`). Both also claimed the library depends only on pydantic and sqlalchemy: `orjson` has been a third core dependency and went unmentioned on every front-door page. `installation.md` and `tutorials/index.md` claimed `from eventsource import SQLiteEventStore` raises `ImportError`, contradicting `faq.md` -- executed in a core-only venv, the import always succeeds and only construction fails without `aiosqlite`; the FAQ was right. The tutorial index promised a 21-part series whose entry point was not among the eight pages that exist, and now describes what is written.
- **ADR 0058 is in the nav**, and two anchors in `docs/api/exceptions.md` that still pointed at the pre-ADR-0050 name `OptimisticLockError` are repointed. A page missing from the nav is invisible to readers, and the strict build does not catch it.
- **`eventsource.testing` is surfaced in both front doors** and re-exports the port conformance suites.
- **Two findings from a downstream 0.12 upgrade are recorded in `BACKLOG.md`**: an unclosed connection-owning adapter raises `RuntimeError: Event loop is closed` naming no store, path, or construction site, and `_stamp` passes an explicitly-set `aggregate_id` through without checking it belongs to this aggregate -- the same check it already performs one field over on `aggregate_type`.

### Tests

- **`SupportsClose` and `LeaderElector` conformance suites.** `SupportsClose` mandates idempotence and non-destruction of caller-owned resources but was covered only per-adapter, for PostgreSQL, via mocks -- the shape that cannot catch the next backend. `LeaderElector` had no coverage at all. No adapter failed, so no adapter changed; the suites were instead proven to bite by temporarily mutating each contract property (dropping SQLite's `close()` guard, making PostgreSQL dispose an engine it does not own, making `renew()` always return `True`), all three caught, then reverted. Worth recording: the caller-owned-resource check first asked whether the caller's engine could still run a query, and the ownership mutant survived it -- a disposed `AsyncEngine` happily opens a new connection. The real observable is pool identity, which `dispose()` swaps.
- **The read-model conformance suite grew from `Filter.eq` alone** to the full operator matrix, NULL handling, empty lists, unknown field, unknown operator, UUIDs, offset, and `include_deleted`. That is what surfaced the SQLite `OFFSET` bug.
- **41 unregistered `DomainEvent` subclasses across 30 modules are registered.** Every one was already latently broken against a SQL backend; append-time validation is what made them visible. Nothing was skipped, xfailed, or weakened.
- The reviewed claim that `InMemoryLeaderElector.renew()` is a no-op bug is **refuted**: in-memory leadership has no lease and cannot expire, so `return self._is_leader` faithfully implements the documented contract. Pinned rather than "fixed".

### Chore

- **`gitpython` is floored to clear five pip-audit advisories.** It reaches the tree only through `cosmic-ray`, a dev dependency, so nothing shipped to consumers was affected -- but `make audit` was red on `main` before this work started.

## [0.12.0] - 2026-08-06

An upstreaming release. Most of it comes from one downstream consumer's list of
things it had built for itself because this library did not offer them: a
rebuild driver for projections, a projection base that stops a subclass having
to restate its parent's constructor, and an explicit public surface on the
`ports` modules. Alongside those, two names that promised more than they
delivered are corrected, and the declared dependency floors are tested for the
first time — which found eight of eleven wrong.

Four breaking changes, three of them renames or signature changes with a
mechanical upgrade path, and one that only affects an install that pinned a
dependency below a bound no supported interpreter could satisfy anyway. The
`SQLiteSnapshotStore` change is the one to read before upgrading: it now holds
a connection open and must be closed.

### Breaking

- **`SQLiteSnapshotStore` opens one connection and requires closing** (ADR 0053). It opened a fresh `aiosqlite` connection per operation, so it owned nothing and had no `close()`. It now opens one connection lazily, applies the bundled sqlite `snapshots` schema to it, reuses it for its lifetime, and implements `SupportsClose`. **Callers must call `await store.close()`**: `aiosqlite` backs each connection with a *non-daemon* thread, which keeps the interpreter alive at shutdown until something closes it. Nothing in the library closes a snapshot store for you. The upside is that `":memory:"` now works — it never did, because every operation saw a different, empty database — and a store pointed at a file whose `snapshots` table was created elsewhere still works, the DDL being idempotent.
- **`DeciderAggregate.initial_state()` takes no arguments** (ADR 0056). It was declared `initial_state(aggregate_id: UUID) -> TState`, and that parameter was the route by which `decide` learned the aggregate id: state carried an id field, and `decide` read it back out when constructing events. The state of a decider is the fold of one aggregate's events, and the value before any event is one value for the aggregate *type* — the id belongs on the command, which names the aggregate it targets. **No shim**, per the pre-1.0 no-shim policy: there is no two-argument overload, no `*args`, and no signature sniffing. A subclass that still declares the parameter defines fine — `abstractmethod` does not check signatures — and raises `TypeError: initial_state() missing 1 required positional argument` the first time it is instantiated. To upgrade a `DeciderAggregate` subclass: delete the parameter from `initial_state` and drop the id field from its state model; add the target id to each command class; and in `decide`, capture that id in the `match` arm (`case ShipOrder(order_id=order_id, ...)`) and pass it as the event's `aggregate_id` instead of reading `state.order_id`. `_get_initial_state()`, `_apply()`, and `__init__` are unchanged — they are instance methods and still have `self.aggregate_id`.
- **`DeciderScenario` no longer accepts `aggregate_id=`**, and its `initial_state=` argument is now a zero-argument callable. The scenario's only use for an aggregate id was feeding `initial_state`; the aggregate a command targets is named by the command. Drop the argument — the events passed to `given()` already carry their own `aggregate_id`.
- **`TenantAwareRepository`'s `enforce_on_load` keyword is renamed `require_tenant_context`** (ADR 0057). The flag enforces nothing about what is loaded: it calls `get_required_tenant()` and delegates, never comparing the resolved tenant against the aggregate and never filtering events. A `load()` inside tenant A's scope for an id belonging to tenant B returns B's aggregate, fully replayed, at either setting. ADR 0018 §6 recorded that as deliberate, but the name promised enforcement the code does not perform, and consumers have twice reported it as a bug. The new name states the precondition it does impose -- a tenant scope must be active. **No deprecation alias**, per the pre-1.0 no-shim policy: `enforce_on_load=` now raises `TypeError`, which is the point. Runtime behavior is otherwise unchanged; nothing previously prevented is now allowed.
- **Most declared dependency floors are raised, and `sqlalchemy` is now declared as `sqlalchemy[asyncio]`.** `pydantic>=2.0` to `>=2.8.0`, `sqlalchemy>=2.0` to `sqlalchemy[asyncio]>=2.0.43`, `orjson>=3.9` to `>=3.10.7`, `asyncpg>=0.27.0` to `>=0.30.0`, `aio-pika>=9.0.0` to `>=9.0.5`, `aiokafka>=0.9.0` to `>=0.12.0`, `confluent-kafka>=2.0.0` to `>=2.6.0`, and both OpenTelemetry packages `>=1.0` to `>=1.16.0`. Breaking only for an install that pinned a dependency below one of the new bounds — and no such install ever worked, which is the point. The old floors predated `requires-python = ">=3.13"` (ADR 0043): five of them have no wheel for any interpreter this package supports and fail to build from source, and `aio-pika` below 9.0.5 imports `pkg_resources`, removed in 3.13. SQLAlchemy is the subtlest of the set, and failed twice. First, it gated `greenlet` behind a marker that excluded Python 3.13 until 2.0.37, so a bare install resolved and imported cleanly and then failed every async call with `the greenlet library is required`; naming the `[asyncio]` extra states the dependency this library actually has instead of inheriting it from someone else's marker. Second, with that fixed, `adapters/_sql/engine.py` calls `Dialect.detect_autocommit_setting()`, which does not exist before 2.0.43 — hence that floor rather than 2.0. Resolved versions in `uv.lock` are unchanged, so nothing moves for anyone installing from the lock.

### Changed

- **Every module under `eventsource.ports` declares an explicit `__all__`.** Their public surface was implicit, and under a strict type checker (`--no-implicit-reexport`) an implicit surface is not a surface: whether a name is importable depended on whether it happened to be defined in that module or merely imported for an annotation, and nothing failed when that diverged from what the docs told consumers to import — the error lands in consumer code, which `mypy src/eventsource/` never sees. The criterion is that a module exports what it *defines*. So `ports/store.py` exports its seven protocols and `collect`, and deliberately does **not** re-export `FeedReadOptions`, `EventEnvelope`, `Position` and the rest: each of those already has one blessed home, and a second would be the same fact in two places. `from eventsource.ports import FeedReadOptions` and `from eventsource.ports.envelopes import FeedReadOptions` both type-check under `--strict`; `from eventsource.ports.store import FeedReadOptions` still does not, by design. `docs/reference/event-store-protocol.md` now states that its "Defining module" column is also the deepest import path that works.
- **The absence of read isolation is documented where it is read, not only in an ADR.** The `TenantAwareRepository` class docstring and its `load()`, `exists()`, and `load_or_create()` docstrings, `docs/tutorials/16-multi-tenancy.md`, and `docs/api/multitenancy.md` now state that reads are never filtered by tenant and that isolation must come from the storage layer. ADR 0057 also replaces ADR 0018 §6's justification, which argued from `EventStore.get_events` and `ReadOptions.tenant_id` -- neither of which survived the rings campaign -- with one that holds on the current `read_stream`/`StreamReadOptions` path. `docs/api/multitenancy.md` additionally corrected a table row claiming `create_new()` raises when a context is required; it never has, and it touches no storage.

### Added

- **`replay()`, a foreground rebuild driver for projections** (ADR 0054). `ProjectionCoordinator` polls on a timer for live catch-up; nothing in the library drove a rebuild, and the difference is not cosmetic. `CheckpointTrackingProjection.handle` retries, writes to the DLQ, and re-raises — correct for a live subscription, where the re-raise is what stops it checkpointing past a failure, and wrong for a rebuild, where it means one poison event denies the projection every event after it. `replay(feed, projections, ...)` records the failure and carries on, returning a `ReplayReport` with `applied`, `last_position`, `failures`, and `failures_truncated`. A `ReplayFailure` names the position, event id, event type, rejecting projection, and the exception itself — a caller can turn detail into a raise, but no caller can turn a count back into detail. `strict=True` raises `ReplayFailedError` on the first rejection instead. Reads are scoped with `tenant_id=`/`aggregate_type=`, pushed into the adapter's query rather than filtered after delivery. The retained-failure list is capped by `max_failures` because each entry pins a live traceback; `failures_truncated` reports what the cap dropped rather than truncating silently, and `on_failure=` fires for every failure regardless of the cap.
- **`StoreProjection[TStore]` and `ProjectionOptions`** (ADR 0055). A `DeclarativeProjection` that holds one store and forwards its parent's full constructor via PEP 692 `**options: Unpack[ProjectionOptions]`. A subclass restates nothing: with no constructor of its own it writes none, and with parameters of its own it writes only those, while `retry_policy`, `tracer`, `tenant_filter`, `checkpoint_repo`, `dlq_repo`, and `enable_tracing` still reach the base. Explicit parameters would have solved this only for a subclass that adds nothing, and `**kwargs: Any` would have cost the typing. This also writes down as a promise what 0.10.0 performed without stating: **every projection base's constructor accepts at least what its parent's accepts**, in every release, enforced by the constructor-superset test — scoped to acceptance, not to defaults or semantics.
- **`FeedReadOptions.aggregate_type`** (ADR 0052). The global feed could be scoped to a tenant but not to an aggregate type, so a consumer interested in one type read the whole feed and discarded the rest in Python — forced, not stylistic, since the filter had nowhere to go. The adapters push it into the same query that already handles `from_position`, `tenant_id`, and `limit`. `limit` still bounds events returned *after* filtering, so a position taken from one page resumes the filtered sequence. PostgreSQL gains a composite `(aggregate_type, global_position)` index; SQLite needs none, `global_position` being its rowid. Additive with a default — existing callers and stored data are unaffected. Note the difference from `read_category`, which selects the same events but orders by storage time.
- **`SQLiteSnapshotStore.close()`**, and the `":memory:"` support that came with owning a connection — see the Breaking entry above.
- **`make floors` and a `Dependency floors` CI workflow.** Every other gate installs with `uv sync --locked`, which resolves near the top of every declared range, so the `>=` bounds were the one point in the supported range nothing exercised — a floor that was too low could not fail anything. The new gate installs with `uv --resolution lowest-direct` into a throwaway environment, putting every declared dependency exactly on its bound, and runs the existing unit suite there. It is deliberately not part of `make check`: it is the one gate that must not use the lockfile, and it needs network access. `lowest-direct` rather than `lowest` because transitive versions are not something this project declares or promises. Documented in `docs/development/dependency-floors.md`; it is what found the floor breakage above.

## [0.11.0] - 2026-08-05

A backlog sweep. One read-model defect fixed, one public exception renamed, and
the Tier 0 boundary given the runtime check it had been specified for. The
rename is the only thing requiring action on upgrade, and only for code that
catches the read-model conflict by name.

### Breaking

- **`eventsource.ports.readmodels.OptimisticLockError` is renamed `ReadModelVersionConflictError`** (ADR 0050). It shared the name with `eventsource.domain.exceptions.OptimisticLockError` — an unrelated `EventSourceError` subclass raised by `append`, taking `aggregate_id` where this one takes `model_id`, neither catching the other and neither deriving from the other. Only the import path distinguished them, so `except OptimisticLockError` read as though it covered both and covered exactly one. **No deprecation alias**, per the pre-1.0 no-shim policy: an alias would preserve the ambiguity the rename exists to remove. The domain name and import path are unchanged, so write-side code needs no action; code catching the read-model error by name must update its import, and code catching it via `ReadModelError` is unaffected.

### Fixed

- **`InMemoryReadModelRepository` no longer aliases the caller's objects.** `get`, `get_many`, `get_deleted`, `find`, and `find_deleted` returned the live dict entry, and `save`, `save_many`, and `save_with_version_check` adopted the caller's object and mutated it — bumping `version`, stamping `updated_at`. The SQL adapters do neither: they hydrate a fresh instance per read and bump `version` in the row. So a model a caller had saved or fetched could change underneath it from a later unrelated write, on the memory backend only, and a projection that held a reference across two saves saw a value it never assigned. Reads now return a copy and writes take one. The contract is pinned in `ReadModelRepositoryConformance` for all three backends rather than in the memory adapter's own tests, which is what let the divergence stand: the suite had *documented* it as a known adapter difference and routed around it.

### Changed

- **`check_expected` and `describe_expected` move to `adapters/_common/`** (ADR 0051), a new adapters-internal package beside `_sql/` (dialect-specific) and `_bus/` (transport-specific), for port semantics that belong to no backend. The expected-version dispatch existed verbatim in three store adapters and the partitioned-memory testing double. Internal only — no public API changes, and no behavior changes.
- **The Tier 0 import-linter contract forbids six drivers, not just `sqlalchemy`**: `redis`, `asyncpg`, `aiosqlite`, `aiokafka`, and `aio_pika` join it. `redis` had been an optional extra since 0.5.0 with nothing enforcing its absence from the core surface. `tests/unit/test_core_surface_purity.py` asserts the same property at runtime, which the static contract cannot do for a dynamic import or a driver a third-party package registers on import.
- **`store_id` uniqueness is documented** on `Position` and at each adapter's default. It is the whole of the foreign-position guard, and the defaults (`pg:{database}`, `sqlite::memory:`, `"memory"`) collide under conditions users hit — two stores sharing one make `PositionForeignError` silently not fire. The defaults are deliberately unchanged: `store_id` is embedded in every persisted position and checkpoint, so redefining one invalidates the checkpoints of every deployment that took it.
- **`MigrationCoordinator.run_resync_pass` logs its two zero-return cases distinctly.** Converged and "coordinator restarted, no interceptor registered" both return 0 and both leave the operator retrying cutover; only the second means the state that would have explained more is gone.

## [0.10.0] - 2026-08-03

A single fix, released as a minor because the fix is additive public API: three
projection constructors now accept arguments they previously rejected with
`TypeError`. Nothing existing changes meaning, and no action is required on
upgrade.

### Added

- **`DeclarativeProjection`, `DatabaseProjection`, and `ReadModelProjection` accept `retry_policy` and `tracer`** (keyword-only), forwarding them to `CheckpointTrackingProjection.__init__`. `ReadModelProjection` additionally accepts `tenant_filter`, which its own parent already took.

### Fixed

- **Projection subclasses no longer silently narrow their parent's constructor.** `CheckpointTrackingProjection` accepts `retry_policy` and `tracer`, but every subclass re-declared the constructor and dropped both. Since `DeclarativeProjection` is its only subclass in the tree and every other projection base descends from it, the two parameters were reachable only by subclassing the abstract base directly — the one path the docs steer users away from, `@handles` routing being the advertised way to write a projection. There was no other way in: `_retry_policy` has no property setter, so the only workaround was assigning the private attribute after `super().__init__()` returned. The practical cost was the default policy — three attempts spanning roughly six seconds, the right shape for a projection writing over a network and the wrong one for a projection writing to a local SQLite file, where the realistic transient is a briefly-locked database that clears in milliseconds: the backoff bought nothing, delayed the DLQ entry saying something was genuinely wrong, and held up the subscription meanwhile. A test now asserts each subclass constructor is a superset of its parent's, since four signatures restating one parameter list is exactly the shape that drifts.

## [0.9.1] - 2026-08-03

A single fix: the default snapshot policy could not fire for a large class of
aggregates. Nothing about the API changes, and no action is required on upgrade
— but a deployment whose aggregates emit several events per command will see
snapshot writes begin where there were none, and loads shorten accordingly.

### Fixed

- **`EveryNEvents(n)` snapshots when a save crosses a multiple of `n`, not when the version lands exactly on one** (ADR 0049). An aggregate that emits several events per command advances its version in strides, and whether a stride ever lands on a multiple is arithmetic: for a constant stride `s` from a starting version `v0`, some version is a multiple of `n` only if `gcd(s, n)` divides `-v0 mod n`. A stream starting at version 1 and advancing by 6 against the default threshold of 50 satisfies no such version — every version it reaches is odd and every multiple of 50 is even — so it snapshotted *never*, not merely late. Nothing failed: the snapshot store stayed empty and every load replayed the full history, a little slower each time. ADR 0017 documented the straddle and accepted it, and ADR 0021 carried the caveat forward; both assumed a later save would land on a boundary. **No action required** — affected aggregates begin snapshotting on the next save that crosses a multiple. The version a snapshot lands on now depends on how events were batched, rather than always being an exact multiple; nothing reads a snapshot by version, and each aggregate keeps one upserted row.

## [0.9.0] - 2026-08-01

The first release on the completed ring architecture, and the largest breaking release to date. It spans ADRs 0038-0048: the multitenancy dissolution and out-of-ring settlement wave that finished the ring-migration campaign, the domain-hardening wave, the PEP 695 migration and Python 3.13 floor, and two audit waves that closed silent-failure paths across the store, bus, subscription, and migration surfaces.

Two things to know before upgrading:

- **Python 3.13 is now the minimum.** See `### Changed`.
- **Read `### Breaking` in full.** Several changes turn previously-silent misbehavior into a loud error — a mismatched `aggregate_type`, an unknown event kwarg, a sync store call from a running event loop. Code that appeared to work may now raise, which is the point: each one was concealing data loss or miscategorization.

This release also finally lands the ADR 0038-0040 wave that was written for 0.8.0 but never reached it — its PR was stacked and merged into its parent branch seconds *after* the parent merged to main, so GitHub marked it merged while the commits were orphaned. The published 0.8.0/0.8.1 wheels still contain `eventsource.multitenancy` and `eventsource.migrations`; this release is where they are actually removed.

### Breaking

- **`SyncEventStoreAdapter` raises `RuntimeError` when called from a thread with a running event loop** (ADR 0048), where it previously scheduled the coroutine onto that same loop with `run_coroutine_threadsafe` and blocked the loop's only thread waiting for it — a guaranteed self-deadlock that hung until the timeout expired and then reported a `TimeoutError` as if the operation were merely slow. There is no correct fallback, so the call is refused with a message naming the two ways out. **Fix:** `await` the async store directly, or move the sync call to a worker thread (`await asyncio.to_thread(sync_store.get_stream_version, stream_id)`), where no loop is running. `SyncEventStoreAdapter.shutdown_executor()`, `_get_executor()`, and the class-level `_executor`/`_executor_lock` are removed — no code path ever dispatched to that pool.
- **`EventStoreConnectionError` now subclasses `EventStoreError`, not `SubscriptionError`** (ADR 0048). It is a store-connection failure, and the old parentage made `except SubscriptionError` the only way to catch it — including for callers that never ran a subscription. **Fix:** catch `EventStoreConnectionError` or `EventStoreError`. It is still importable from `eventsource.ports`.
- **An event class declaring an `aggregate_type` different from its aggregate's now raises `AggregateTypeMismatchError`** at emit time (ADR 0048, extending 0046), where the declared value was previously overwritten in silence. Since `aggregate_type` is the stream category, the disagreement was invisible in a save/load round-trip and showed up only as events missing from a category read. **Fix:** delete the declaration from the event class (the aggregate stamps it) or emit the event from the matching aggregate. A declaration that *matches* remains legal, if redundant.
- **`OptimisticLockError.expected_version` is `int | str`.** The non-numeric `ExpectedVersion` kinds now render by name (`no_stream`, `stream_exists`, `any`) instead of as the integer sentinels `0`/`-2`/`-1`. `expected_version=0` told the caller the store expected a version they never wrote. **Fix:** code branching on the numeric sentinels should compare against the kind names.
- **`AggregateRepository.__init__` no longer accepts `aggregate_type=`.** The type is always inferred from `aggregate_factory.aggregate_type` — the same required class attribute `AggregateTypeNotSetError` already enforces. Fixes a reproduced silent-miscategorization bug: with the class attribute, the (now-removed) repository parameter, and an event's own field default set to three different strings, the repository parameter silently won for both stream category and event stamping while the event's own declaration was discarded — invisible on a save/load round-trip through the same misconfigured repository. **Fix:** delete the `aggregate_type=` argument from `AggregateRepository(...)` calls; if the aggregate class can't be edited to add or fix the attribute, subclass it and set `aggregate_type` there. See ADR 0046.
- **`LiveRunner.__init__` requires a new `event_feed: GlobalEventFeed` argument** (ADR 0047). Any code constructing `LiveRunner` directly (rather than through `TransitionCoordinator`, which now supplies it automatically from its own `event_store`) must pass one. `LiveRunnerStats.events_skipped_duplicate` and `TransitionResult.buffer_events_skipped` are removed — the catch-up→live duplicate check they measured is now unreachable by construction (see Fixed) and the metrics they backed were permanently pinned at zero.
- **`TState` is no longer importable.** `from eventsource import TState` (and `from eventsource.domain.types import TState`) now raises `ImportError`. The library's generics moved to native PEP 695 type-parameter syntax (`class AggregateRoot[TState: BaseModel](ABC)`), and a PEP 695 type parameter is scoped to its declaration — there is no module-level `TypeVar` object left to export. **Fix:** declare your own inline parameter — `def f[T: BaseModel](a: AggregateRoot[T]) -> None:` instead of importing `TState`. `AggregateRoot`, `DeclarativeAggregate`, and `DeciderAggregate` are otherwise unchanged, including both `DeciderAggregate[State]` and `DeciderAggregate[State, Command]` subscript forms.
- **`TAggregate` is no longer exported** from `eventsource.application.aggregates` or `eventsource.application.aggregates.repository`, for the same reason. **Fix:** `def f[A: AggregateRoot[Any]](repo: AggregateRepository[A]) -> None:`.
- **`TEvent` is no longer exported** from `eventsource.testing.builder`'s `__all__`. It was never re-exported at the `eventsource.testing` package level, so `from eventsource.testing import ...` is unaffected. **Fix:** `def f[E: DomainEvent](b: EventBuilder[E]) -> None:`.

### Added

- **Two new `import-linter` forbidden contracts formalize `eventsource.observability` and `eventsource.testing` as settled out-of-ring packages** (ADR 0040): "Domain and ports must not import observability" and "Rings must not import the testing toolkit." Both assert properties that already held; neither required a code change. With ADRs 0038 and 0039 dissolving `eventsource.multitenancy` and `eventsource.migrations`, every top-level package under `src/eventsource/` is now one of the four rings or one of these two settled exceptions -- the ring-migration campaign's completion criterion.

- `SyncEventStoreAdapter.close()` and context-manager support, so a sync caller can release a store that owns a connection (`SQLiteEventStore`, or `PostgreSQLEventStore` with `owns_engine=True`). Previously there was no way to, and the process could hang at exit. A no-op for stores that do not implement `SupportsClose`.
- `RedisEventBus.start_consuming_in_background()`, matching Kafka and RabbitMQ. `start_consuming()` blocks for the consumer's lifetime; the new method schedules it as a task and retains it on the bus, so `disconnect()` cancels it. The `_consumer_task` attribute was previously never populated by anything.
- `AggregateTypeMismatchError`, exported from `eventsource`.
- `ReadModel` and `AggregateTypeNotSetError` are now exported from `eventsource`. `ReadModelProjection` was exported while `ReadModel` was not, and `AggregateTypeNotSetError` was named in the getting-started prose without being importable from the front door.
- A conformance test pinning that `read_category(from_timestamp=...)` compares instants, not rendered offsets, across every store backend.
- `DeciderAggregate[TState, TCommand]`: optional second type parameter (PEP 696 default `object`) so `decide` can be typed against a userland command union and mypy flags unhandled commands. Single-parameter subscripts keep working unchanged.
- `AggregateTypeNotSetError` exception.

- `DomainEvent.event_type_name()` classmethod — the canonical wire name for an event class.
- `eventsource.domain.decorators.discover_handlers()` — shared @handles discovery used by aggregates and projections.
- `eventsource.ports` facade now re-exports the 13 infrastructure exceptions moved there (ADR 0041), parallel to the domain facade's exception re-exports.
- `DuplicateHandlerError` exception; `HandlerSignatureError` gains an optional `reason` parameter.
- `domain/__init__` now exports `HandlerSignatureError`, `DuplicateHandlerError`, and the three tenant-context exceptions (surface sync).

### Changed

- **BREAKING: aggregates must declare `aggregate_type`.** The silent `"Unknown"` default on `AggregateRoot` is removed; constructing a concrete aggregate class that does not set the attribute raises the new `AggregateTypeNotSetError`. Aggregate identity is not optional — the old default silently created `"Unknown"`-typed streams.
- **BREAKING: `DomainEvent.aggregate_type` is validated as a stream category** (must match `CATEGORY_PATTERN`); values that would corrupt a `StreamId` (e.g. containing `:`) now fail at event construction instead of detonating at stream-render time.
- **BREAKING: `DeclarativeAggregate.unregistered_event_handling` now defaults to `"error"`** (was `"ignore"`). An aggregate replaying an event it has no `@handles` method for raises `UnhandledEventError` instead of silently skipping it — silent skips let command handlers reason over divergent state. Opt down explicitly with `unregistered_event_handling = "ignore"`. Projections are unaffected.
- **BREAKING: `eventsource.domain.types` reshaped.** `Version`, `StreamPosition`, and `GlobalPosition` are deleted (global positions are opaque adapter-owned tokens — use `eventsource.ports.positions.Position`); `TenantId` and `CausationId` are now plain `UUID` aliases (optionality belongs to the referencing field, not the identity type). The identity aliases are now threaded through `DomainEvent`/`DomainCommand` annotations, so the published vocabulary matches the real signatures.
- **BREAKING: Python 3.13+ is now required** (`requires-python = ">=3.13"`; was `>=3.11`). The typed decider uses native PEP 696 TypeVar defaults, and the project now targets one modern floor instead of carrying compatibility imports. CI tests 3.13.
- **`eventsource.application.migration.exceptions` is decomposed into four single-responsibility modules** (ADR 0044): the classification vocabulary (`ErrorSeverity`, `ErrorRecoverability`, `ErrorClassification`, `RetryConfig` and the three retry-config constants) moves to `error_classification.py`; the circuit breaker (`CircuitBreaker`, `CircuitBreakerConfig`, `CircuitState`, `CircuitBreakerContext`) to `circuit_breaker.py`; `ErrorHandler` and `classify_exception` to `error_handling.py`. `exceptions.py` is now the `MigrationError` taxonomy alone, down from 1533 lines. **This is not a breaking change** — every name is still exported from `eventsource.application.migration` and the package's `__all__` is unchanged; only direct submodule imports (`from eventsource.application.migration.exceptions import CircuitBreaker`) need updating to the new module.

- **BREAKING: `DomainEvent` now uses `extra="forbid"`.** Unknown constructor kwargs (typically typos) raise `pydantic.ValidationError` instead of being silently dropped and persisting an event with missing data. Arbitrary payload data belongs in the `metadata` field. This also applies on the read path: `from_dict()` is `model_validate()`, so a stored event carrying a field no longer declared on its event class now fails replay with `ValidationError` instead of silently dropping the extra field.
- **BREAKING: `EventTypeNotFoundError`, `DuplicateEventTypeError`, and `HandlerSignatureError` no longer subclass `KeyError`/`ValueError`.** `except KeyError`/`except ValueError` will no longer catch them; catch the specific type or `EventSourceError`. Their `str()` output is no longer re-quoted by `KeyError.__str__`.
- **BREAKING: 13 infrastructure exceptions moved from `eventsource.domain.exceptions` to `eventsource.ports.exceptions`** (ADR 0041, no shims): `CheckpointError`, `CheckpointNotFoundError`, `EventBusConnectionError`, `EventStoreConnectionError`, `LockAcquisitionError`, `LockNotHeldError`, `PositionDecodeError`, `PositionForeignError`, `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `TransitionError`. Top-level `from eventsource import ...` re-exports are unchanged.
- **BREAKING (behavioral): `DeciderAggregate` stamping now applies the ambient tenant-context fallback for every command type, not only `DomainCommand`** (unified with `create_event()` semantics via the shared `_provenance_updates()` helper). Events emitted from non-`DomainCommand` commands may now carry a `tenant_id` they previously lacked.
- **BREAKING: `DeclarativeAggregate` validates handler signatures at class-definition time**: async handlers and wrong parameter counts raise `HandlerSignatureError` (previously only projections validated). A subclass that imported cleanly before this change may now raise at class-definition time.
- `DeciderAggregate.state` raises `RuntimeError` on a `None` state instead of using a bare `assert` (which `python -O` strips).

### Removed

- **BREAKING: `eventsource.multitenancy` no longer exists** (ADR 0038, dissolving the last transitional package alongside `eventsource.migrations` below). `import eventsource.multitenancy` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029-0034 already applied without qualification. Replacements: `eventsource.multitenancy.context` (`tenant_context`, `TenantContextToken`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `reset_tenant_context`, `clear_tenant_context`, `tenant_scope`, `tenant_scope_sync`) -> `eventsource.domain.tenant_context`; `eventsource.multitenancy.events.TenantDomainEvent` -> `eventsource.domain.tenant_events`; `eventsource.multitenancy.exceptions` (`TenantContextNotSetError`, `TenantContextResetError`, `TenantMismatchError`) -> merged into `eventsource.domain.exceptions` (already `EventSourceError`-rooted, no rebase); `eventsource.multitenancy.repository.TenantAwareRepository` -> `eventsource.application.aggregates.tenant_repository`. Top-level `from eventsource import ...` imports are unaffected -- `__all__` is byte-identical, the barrel re-exports from the new homes directly. `TenantAwareRepository` was never re-exported from the top-level package and still is not; import it from its new module path. As part of this move, the `importlib`-based soft dependency `AggregateRoot._get_tenant_from_context()` used while reaching for an out-of-ring package is replaced by a direct import of `eventsource.domain.tenant_context.get_current_tenant`, now that the target is a same-ring sibling shipped unconditionally.
- **BREAKING: `eventsource.migrations` (plural, the schema-DDL package) no longer exists** (ADR 0039). `import eventsource.migrations` now raises `ModuleNotFoundError`. No shim, no deprecation warning, same standing rule. The whole package relocates as one unit -- `__init__.py`, `SCHEMA_DESIGN.md`, `additive/`, `schemas/`, `templates/`, `updates/` -- to `eventsource.adapters.sql.schemas`, unchanged in every other respect: `from eventsource.migrations import get_schema` becomes `from eventsource.adapters.sql.schemas import get_schema`, and every other name (`get_all_schemas`, `get_template_path`, `list_schemas`, `list_backends`, `get_alembic_template`, `list_alembic_templates`, the seven `*_SCHEMA` constants, `SchemaName`, `BackendName`) moves the same way. None of these were ever re-exported from top-level `eventsource`. Packaging is unaffected: the `.sql`/`.md`/template files are wheel-verified to ship at the new path (`uv build` + `unzip -l` on the built wheel), with no `pyproject.toml` change needed.

### Fixed

- **The Kafka consumer honors the retry backoff it was already computing** (ADR 0048). It wrote a `retry_after` header from the shared `RetryPolicy` and never read it back, so the same configuration that made the RabbitMQ consumer back off made the Kafka consumer retry immediately, forever. Republished messages now wait out their scheduled time before processing. Both backends block while waiting — a genuinely non-blocking delay needs a dedicated retry topic and is tracked in `BACKLOG.md` — but they no longer diverge from identical config.
- **The Kafka consumer no longer commits offsets for events it did not retain** (ADR 0048). `_republish_for_retry` returned early when the producer was disconnected, and the caller committed the offset regardless: the event was neither retried nor retained. `_send_to_dlq` and `_republish_for_retry` now report whether the event was retained, the offset is committed only then, and an unretained message is logged `CRITICAL` and left uncommitted for redelivery. An explicitly disabled DLQ still drops poison messages, since that is the configured choice.
- **A projection checkpoint failure no longer re-runs the handler or DLQs a successfully-projected event** (ADR 0048). `record_checkpoint()` sat inside the `try` the retry loop wraps around `_process_event()`, so a checkpoint-store outage was indistinguishable from a poison event: the loop retried, re-applying the read-model mutation once per attempt, then wrote the event to the DLQ where an operator replaying it would apply it again. Checkpointing moved to the success path; failure re-raises (a stalled projection is a liveness problem) but never retries and never reaches the DLQ.
- **SQLite's `read_category(from_timestamp=...)` no longer silently returns nothing for a non-UTC bound.** `created_at` is TEXT and the comparison is lexical, so a `+05:00` timestamp sorted after every stored `+00:00` row regardless of the instant it denoted. The bound is normalized to UTC first; a naive datetime is read as UTC.
- **SQLite and PostgreSQL store adapters raise `EventStoreConnectionError` when they cannot reach the database** (ADR 0048), instead of letting `sqlite3.OperationalError: unable to open database file` reach the user with nothing naming the library, the adapter, or the path. The driver exception is attached as `__cause__`. The exception type existed and was exported but was raised nowhere in the library.
- **`RedisEventBus.recover_pending_messages()` honors an explicit `0`.** `min_idle_time_ms`, `max_retries`, and `batch_size` used falsy checks, so passing `0` (claim every pending message; DLQ without retrying) was silently replaced by the config default.
- **`import eventsource` no longer executes `aiosqlite`** — 149 modules instead of 177. Computing `__all__` imported the driver to test whether it was installed, defeating the lazy front door ADR 0035 established; it now asks `importlib.util.find_spec`.
- **`StreamId` reports which argument is wrong.** The signature is `StreamId(aggregate_id, category)`, which reads opposite to the library's category-first read APIs and to the rendered `"{aggregate_id}:{category}"` form, so transposing is easy — and produced a bare `TypeError: expected string or bytes-like object` from `re`, naming neither argument.
- **The live runner's transition and pause buffers no longer grow without bound.** They were `asyncio.Queue`s holding one interchangeable `None` sentinel per bus notification, discarded wholesale at drain time; they are now counters. A subscription paused through a long incident accumulated one entry per event while retaining no information. (The pause/resume reordering concern recorded in `BACKLOG.md` was re-verified against post-#114 code and is fixed by construction: `process_pause_buffer()` discards the sentinels and performs a single ordered feed drain from the checkpoint.)
- **A failed cutover no longer leaves traffic on the target store.** `_rollback` restored the migration state to `DUAL_WRITE` but never reverted `set_routing`, so a tenant reported as rolled back had every request still going to the store the cutover had failed to complete. Rollback now restores both halves, and a failure between the two writes of the switch itself reverts the route before propagating. The pair is still not atomic — the routing port has no transaction boundary — and that limitation is recorded in `BACKLOG.md`.
- **Live-phase lag now has a signal.** `Subscription.lag` was structurally zero during live processing, because only the catch-up runner incremented `_events_seen`; an operator's dashboard could not distinguish a healthy live subscription from a stalled one. The feed-driven live runner (ADR 0047) records each envelope as seen and releases the receipt symmetrically when an event is filtered, unpositioned, or terminally disposed.
- **`eventsource.application` and `eventsource.adapters` declare `__all__`** (empty, with the reason). They were the only subpackages without one.
- **`LiveRunner` now checkpoints correctly during the live phase** (ADR 0047). It previously read a `_position` attribute off the bus-delivered `DomainEvent` that nothing in the codebase ever set, so every live event was recorded at the subscription's unchanged catch-up position and `_maybe_checkpoint` was never called — a subscription live for any length of time would replay its entire live period from the catch-up watermark on restart. The store now owns ordering: on each bus notification, `LiveRunner` drains `GlobalEventFeed.read_all(from_position=...)` forward from its checkpoint and delivers what the feed returns, so the checkpointed position is always the event's real feed position. The catch-up→live duplicate-suppression check, which the same bug had made permanently inert, is removed as unreachable by construction rather than left as dead code.
- **`DomainEvent.__init_subclass__` no longer mutates the parent class's shared `event_type` FieldInfo.** Subclassing a concrete event corrupted the parent's registry key: `register_event(Parent)` after `class Child(Parent)` filed Parent under "Child", making stored `"Parent"` events undeserializable (or raising a spurious `DuplicateEventTypeError`). Event-type derivation is now unified on the new `DomainEvent.event_type_name()` classmethod, used by both instance construction and `EventRegistry`.
- **BREAKING (behavioral): `clear_tenant_context()` now actually clears.** Previously it left the token stack intact, so any enclosing `tenant_scope()` exit silently resurrected the "cleared" tenant — a cross-tenant leakage vector. It now invalidates all outstanding tokens in the current context; a subsequent `reset_tenant_context()` (including a scope exit) raises `TenantContextResetError` instead of restoring a stale tenant. Code that relied on the old restore-after-clear behavior (there should be none, since it was the leakage vector this fixes) must stop calling `clear_tenant_context()` inside an active scope.
- Two `@handles` methods for the same event type in one class now raise the new `DuplicateHandlerError` — at class-definition time for aggregates, at instance construction time for projections — instead of silently dropping one handler (discovery order used to decide the winner alphabetically).

### Docs

- The event-bus guide records that the transactional outbox is a PostgreSQL capability: `SQLiteEventStore` has no `outbox_enabled` flag, so a service developed against SQLite and deployed against PostgreSQL gets different delivery guarantees from identical code.
- `docs/api/sync.md` and `docs/guides/sync-usage.md` rewritten for the sync adapter's single supported calling context, and `close()` documented.
- `docs/guides/subscriptions.md` corrected: `StartFromResolver` resolves a missing checkpoint to `None`, not `0` (positions are opaque tokens with no zero), and `CheckpointNotFoundError` lives in `eventsource.ports.exceptions`, not `eventsource.domain.exceptions`.
- `docs/getting-started.md` no longer understates the core dependency set — `orjson` is a core dependency alongside pydantic and sqlalchemy.
- The teaching layer is now decider-first per ADR-0022 §5: getting-started and index lead with `DeciderAggregate` + `DomainCommand` + `CommandRejectedError`; `explanation/aggregate-styles.md` is a three-style comparison with the decider first; the testing tutorial leads with `DeciderScenario`. The quickstart no longer recommends hand-declaring `event_type` (auto-derived; explicit only for versioned wire names) and uses `Decimal` money. Stale docstrings in `aggregate.py`/`event_registry.py` updated to current practice.

## [0.8.1] - 2026-08-01

### Fixed

- **The 0.8.0 wheel published to PyPI was missing the entire `eventsource.adapters.memory` package** -- `pip install eventsource-py==0.8.0` failed on `from eventsource import InMemoryEventStore` (and every other memory-adapter name) with `ModuleNotFoundError: No module named 'eventsource.adapters.memory'`. Root cause: `.gitignore` carried an unanchored `memory/` pattern (intended for machine-local agent-team memory at the repo root), and hatchling applies `.gitignore` patterns when selecting wheel contents even for files git tracks -- so the adapter package was silently dropped from the build. The pattern is now anchored to `/memory/`. 0.8.0 is yanked on PyPI; this release is identical except for the packaging fix.
- The release workflow now smoke-tests the built wheel (install + import `DomainEvent`, `InMemoryEventStore`, `InMemoryEventBus`) before anything is published, so an incomplete wheel fails the build instead of reaching PyPI.

## [0.8.0] - 2026-08-01

### Added

- **New ports/value-object surface (`eventsource.ports`)** -- `StreamId`, `Position`, `ExpectedVersion`, `EventEnvelope`, `AppendResult`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions`, `ReadDirection`, and the five composable store ports (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`, composed as `FullEventStore`) plus the `collect` helper. `StreamId`, `Position`, `EventEnvelope`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions`, `ExpectedVersion`, `ReadDirection`, `AppendResult`, and the five ports are re-exported from top-level `eventsource`.
- **Three new backend adapters** implementing the ports above: `eventsource.adapters.memory.InMemoryEventStore` (re-exported as `eventsource.InMemoryEventStore`), `eventsource.adapters.sqlite.SQLiteEventStore`, `eventsource.adapters.postgresql.PostgreSQLEventStore`. All three expose a public `store_id` property; `PostgreSQLEventStore` additionally takes `outbox_enabled=False` to write to the transactional outbox in the same transaction as the append. The outbox reader the drain side of the pattern needs -- the `OutboxRepository` contract and its `memory`/`postgresql`/`sqlite` adapters -- lives in `eventsource.ports.outbox` and `eventsource.adapters.{memory,postgresql,sqlite}` (see the outbox ring migration entry below).
- **PostgreSQL global feed no-skip guarantee**: the PostgreSQL adapter's feed reader no longer risks skipping events committed out of insertion order under concurrent writers.
- **Conformance suites for the new ports** (`eventsource.testing.conformance_ports`) -- `AppenderConformance`, `StreamReaderConformance`, `EventLookupConformance`, `GlobalFeedConformance`, `CategoryQueryConformance`, and `SnapshotConformance`, run against the memory, sqlite, and (integration) postgresql adapters.
- New exceptions `DuplicateEventError`, `PositionDecodeError`, `PositionForeignError`, and the `IntPositionCodec` position codec are re-exported from top-level `eventsource`.
- **`eventsource.application.aggregates`** -- `SnapshotPolicy` (`EveryNEvents`, `Never`), `SnapshotScheduler` (`ImmediateScheduler`, `BackgroundScheduler`), and the `take_snapshot` / `read_valid_snapshot` helpers, composed by `AggregateRepository` to decide and schedule snapshotting. `AggregateRepository` gained `snapshot_policy=` / `snapshot_scheduler=` constructor parameters for injecting custom policy/scheduler implementations.
- ADR 0021, documenting the snapshot composition design (policy + scheduler replacing the monolithic snapshot manager/strategy) and superseding ADR 0017.
- **Command objects and the decider aggregate style** -- `eventsource.commands.DomainCommand` (frozen pydantic base for commands), `eventsource.domain.decider.DeciderAggregate` (decide/evolve aggregate style), `CommandRejectedError`, and `create_event(command=...)` provenance stamping on `AggregateRoot`. All re-exported from top-level `eventsource`; documented in ADR 0022. (Landed on main via PR #82; recorded here on merge since it shipped without a changelog entry.)
- **`eventsource.ports.outbox`** -- `OutboxRepository` (the transactional outbox Protocol), `OutboxEntry`, `OutboxStats`, and `outbox_event_data()` (the single authority for the JSON-safe payload dict stored in `event_outbox.event_data`, replacing four independent constructions of the same shape). Plus the three adapter modules (`eventsource.adapters.memory.outbox`, `eventsource.adapters.postgresql.outbox`, `eventsource.adapters.sqlite.outbox`) and `OutboxRepositoryConformance` in `eventsource.testing.conformance_ports`, exercised against all three. See ADR 0026.
- **`AggregateStore`, `ASYNCPG_AVAILABLE`, `AIOSQLITE_AVAILABLE`** are now re-exported from top-level `eventsource`.
- **`MigrationCoordinator.run_resync_pass(migration_id) -> int`** -- runs one bounded catch-up copy pass while a migration is in `DUAL_WRITE`, returning the number of unabsorbed dual-write mirror failures remaining (0 means the sync-lag anchor is unclamped and cutover can proceed). Previously a mirror failure after the bulk copy finished clamped the lag anchor permanently and the only remedy was to abort and restart the migration. The migration's phase is never touched. See ADR 0028.
- **`eventsource.ports.locks`** -- `DistributedLock` and `LockRegistry` (small Protocols, ISP-split along the two real consumer groups: acquire/release individual locks vs. bulk lifecycle over everything one manager holds), `LockInfo`, and `migration_lock_key`. See ADR 0029.
- **`eventsource.adapters.memory.locks.InMemoryLockManager`** -- a second `DistributedLock`/`LockRegistry` implementation, test-scoped only: single-process, no crash release, no fairness. Its docstring leads with what it does not guarantee. See ADR 0029.
- **`eventsource.ports.readmodels`** -- a subpackage (not a flat module) holding `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, and the read-model exception family (`ReadModelError`, `OptimisticLockError`, `ReadModelNotFoundError`). See ADR 0029.
- **`DistributedLockConformance` and `ReadModelRepositoryConformance`** in `eventsource.testing.conformance_ports`, exercised against the memory and postgresql (locks) and memory/postgresql/sqlite (read models) adapters.
- **`eventsource.ports.migration`** -- a new subpackage holding `models.py` (`Migration`, `MigrationConfig`, `MigrationPhase`, `MigrationStatus`, `MigrationResult`, `TenantRouting`, `TenantMigrationState`, `PositionMapping`, `SyncLag`, `CutoverResult`, `MigrationAuditEntry`, `AuditEventType`) and `repositories.py` (`MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`, `MigrationAuditLogRepository` Protocols), extracted from the migration ring migration below. See ADR 0034.
- **`eventsource.ports.snapshots.SnapshotTypeInvalidation`** -- optional capability Protocol for bulk snapshot invalidation by aggregate type (`delete_snapshots_by_type`), split out of `SnapshotStore`. See ADR 0036.
- **`eventsource.ports.lifecycle.SupportsClose`** -- optional capability Protocol for releasing resources an adapter owns (a single `close()` method), with a documented ownership contract: never tears down a resource injected by and still owned by the caller. `SyncStoreFacade.close` uses it via `isinstance` instead of duck-typing `getattr`. See ADR 0037.
- **PEP 562 lazy front door for `eventsource/__init__.py`.** `import eventsource` no longer imports sqlalchemy, asyncpg, aiosqlite, redis, aiokafka, or aio-pika -- every public name resolves on first `__getattr__` access and is cached. `__all__` is unchanged (same names, same order). Payoff: `import eventsource, sys; assert "sqlalchemy" not in sys.modules` now holds, and runtime Tier-0 purity checks (previously only possible via static `ast` analysis, see `tests/unit/ports/test_readmodels_port_surface.py`) are now directly verifiable at import time. See ADR 0035.

### Changed

- **`MigrationConfig.cutover_max_lag_events` now defaults to `0` (strict), was `100`.** Cutover no longer proceeds while any source event is provably absent from the target. Writes are paused for the entire cutover and nothing in the sequence copies the residue, so lag remaining at the routing switch was events the target never received while it became authoritative — caught only by a non-fatal post-cutover consistency check. **Behavior change:** a cutover that previously succeeded with residual lag now raises `CutoverLagError` and rolls back to `DUAL_WRITE`. To restore the old behavior, pass `MigrationConfig(cutover_max_lag_events=100)` explicitly — and understand it as accepting up to 100 lost events at the switch. When lag will not drain, the remedy is the new `MigrationCoordinator.run_resync_pass` rather than a higher threshold. See ADR 0028.
- **PostgreSQL deployments MUST apply `migrations/updates/004_add_events_txid.sql` before upgrading.** The global feed read path (`read_all`, `current_position`) now filters on a new `events.txid xid8` column instead of the `xmin` system column, and fails loudly with an undefined-column error against a database that has not applied it. The old predicate compared a 32-bit `xmin` against an epoch-extended 64-bit `pg_snapshot_xmin(...)`, so it became universally true — silently dropping the no-skip guarantee — once a cluster crossed its first xid epoch. Fresh provisioning via `get_schema`/`get_all_schemas` needs nothing: the column arrives as an additive fragment. Rows left with a NULL `txid` (those predating the migration) are always read; no backfill is needed. Requires PostgreSQL 13+, the same floor as before. See ADR 0027.
- **`PostgreSQLEventStore`'s constructor now takes `engine: AsyncEngine` as its primary argument instead of `session_factory: async_sessionmaker`.** The adapter builds its own internal session factory from the engine; callers that previously constructed and passed a `session_factory` now pass the `AsyncEngine` directly.
- **`tracer=` / `enable_tracing=` constructor kwargs are gone from all three store adapters.** `InMemoryEventStore`, `SQLiteEventStore`, and `PostgreSQLEventStore` (`eventsource.adapters.memory` / `.sqlite` / `.postgresql`) all accepted these on `eventsource.stores`; none of the ports adapters do.
- **Snapshot store implementations re-homed** into their adapters: `InMemorySnapshotStore` -> `eventsource.adapters.memory.snapshots`, `SQLiteSnapshotStore` -> `eventsource.adapters.sqlite.snapshots`, `PostgreSQLSnapshotStore` -> `eventsource.adapters.postgresql.snapshots`. `Snapshot` and `SnapshotStore` now live in `eventsource.ports.snapshots`; snapshot exceptions live in `eventsource.exceptions`. The `eventsource.snapshots` package itself has been **deleted** -- see Removed. Top-level `eventsource` re-exports of `Snapshot`, `SnapshotStore`, and the snapshot store adapters are unchanged.
- **`ExpectedVersion`, `ReadDirection`, and `AppendResult` are the ports-layer classes, full stop.** With the legacy `EventStore` ABC and `eventsource.stores` deleted (see Removed), the naming collision these three names used to have with `eventsource.stores.interface.ExpectedVersion` / `.ReadDirection` / `.AppendResult` no longer exists. They are re-exported from top-level `eventsource` as well as `eventsource.ports`. Likewise `eventsource.adapters.sqlite.SQLiteEventStore` and `eventsource.adapters.postgresql.PostgreSQLEventStore` are the only `SQLiteEventStore` / `PostgreSQLEventStore` in the library and are re-exported from top-level `eventsource`.
- **`AggregateRoot` and `DeclarativeAggregate` re-homed** to `eventsource.domain.aggregate`; the `eventsource.aggregates` import path is gone (see Removed). Top-level `eventsource` imports are unaffected.
- **`AggregateRepository` re-homed** to `eventsource.application.aggregates`. Top-level `eventsource` imports are unaffected.
- **`EventPublisher` re-homed** to `eventsource.ports.bus`; the legacy re-export path from `eventsource.stores.interface` was removed with the stores package.
- **Outbox ring migration** (ADR 0026, completing the split ADR 0024 made for checkpoints and DLQ): `OutboxRepository`, `OutboxEntry`, `OutboxStats`, and `outbox_event_data` moved from `eventsource.repositories` to `eventsource.ports.outbox`; `InMemoryOutboxRepository` moved to `eventsource.adapters.memory`; `PostgreSQLOutboxRepository` moved to `eventsource.adapters.postgresql`; `SQLiteOutboxRepository` moved to `eventsource.adapters.sqlite`. Unlike checkpoints and DLQ, the outbox backends are per-technology modules rather than one dialect-parameterized module -- `SQLiteOutboxRepository` is written against a raw `aiosqlite.Connection`, and unifying it onto sqlalchemy would have meant rewriting a working adapter's driver layer with no caller requesting it. Top-level `eventsource` re-exports are unaffected in name, only in the module they resolve to.
- **`sql_connection(conn, *, write=...)`** (`eventsource.adapters._sql.connection`, introduced by ADR 0024) is now the single SQL connection-normalization helper in the codebase. `PostgreSQLOutboxRepository`'s seven call sites, plus five callers outside `adapters/` (`eventsource.readmodels.postgresql` and four `eventsource.migration.repositories` modules), moved onto it from the retired `execute_with_connection`.
- **Behavior change: the in-memory outbox adapter's `event_data` JSON formatting changed from orjson's compact separators to stdlib `json.dumps`'s default spaced separators** (`", "` / `": "` instead of `","` / `":"`). This is cosmetic for any consumer that parses the field back into a dict -- round-trip equality is unaffected -- and breaking for a consumer that compares the stored string byte-for-byte. Only `InMemoryOutboxRepository` is affected; PostgreSQL and SQLite serialize through their own drivers' JSON handling, unchanged. This swap also drops one non-stdlib import from a Tier 0 adapter: `eventsource.adapters.memory.outbox` no longer imports `eventsource.serialization` for orjson.
- **Projection persistence re-homed** (ADR 0024): `eventsource.projections` -> `eventsource.application.projections`; the checkpoint and DLQ Protocols -> `eventsource.ports.checkpoints` (`ProjectionCheckpoints`, `SubscriptionPositions`, `CheckpointRepository`) and `eventsource.ports.dlq` (`DLQRepository`); the checkpoint and DLQ implementations -> `eventsource.adapters.sql` (`SQLCheckpointRepository`, `SQLDLQRepository`, dialect-parameterized for PostgreSQL and SQLite) and `eventsource.adapters.memory` (`InMemoryCheckpointRepository`, `InMemoryDLQRepository`); `DatabaseProjection` -> `eventsource.adapters.sql.projection`. Top-level `eventsource` imports are unaffected.
- **Behavior change: `checkpoint_repo=None` / `dlq_repo=None` now disable the concern** instead of constructing a per-instance in-memory repository. A `CheckpointTrackingProjection` (and subclasses `DeclarativeProjection`, `DatabaseProjection`) built with no `checkpoint_repo` no longer checkpoints at all -- `get_checkpoint()` / `get_lag_metrics()` return `None` -- and one built with no `dlq_repo` no longer captures failed events to a DLQ; a permanently failed event is logged at `critical` and re-raised either way. To keep the old vanish-on-restart behavior, pass `InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` (from `eventsource`) explicitly.
- **Breaking: subscription and checkpoint positions are now opaque `Position` value objects, not integers** (legacy store retirement, slice (b), amending ADR 0024 -- see ADR 0025). `SubscriptionPositions.get_position` / `.save_position` and `CheckpointData` carry `Position` rather than an integer global position. `SubscriptionConfig.start_from` (`eventsource.subscriptions`) no longer accepts a bare `int`; its type is now `Literal["beginning", "end", "checkpoint"] | Position`. Checkpoint rows are stored under a new additive `position_token` column -- a row that carries only the legacy `global_position` column (written before this change, or by a store that has not been migrated) reads back as **no position** and causes catch-up to restart from the beginning rather than resuming; this is a deliberate fail-safe, not a bug. `Subscription.lag` is redefined: it is now a **count of events not yet delivered within the current run** (undelivered envelopes still pending in the current catch-up window), not a store-wide distance between two global positions -- the old integer subtraction (`max_position - last_processed_position`) is gone because opaque positions cannot be subtracted.
- **Breaking: `eventsource.migration` runs on the ports store surface, not the legacy `EventStore` ABC** (legacy store retirement, slice (c)). `MigrationCoordinator`, `TenantStoreRouter`, `BulkCopier`, `DualWriteInterceptor`, `ConsistencyVerifier`, and `SyncLagTracker` all take/return `FullEventStore` (source and target) instead of the old ABC, and their positions are opaque `Position` tokens end to end -- the migration's position mappings and its `tenant_migrations.last_source_position` / `last_target_position` progress are now persisted as tokens (`*_position_token` columns), not integers. `MigrationCoordinator` and `SubscriptionMigrator` no longer take a `position_store_id`: the token-keyed position mapping table needs nothing to convert.
  - The dropped cross-type capability: `get_events(aggregate_type=None)` (querying across all aggregate types in one call) is gone with the legacy store; the bulk copier reads per-stream through the ports surface instead.
  - A duplicate append during bulk copy (e.g. on resume after a crash) is now counted as **already copied** rather than silently skipped -- resuming a bulk copy is idempotent in outcome, not just in effect.
  - `SyncLagTracker.calculate_lag()` reports a **bounded count** of source events not yet copied to the target, not a position delta -- opaque positions cannot be subtracted. The count is exact up to `cutover_max_lag_events + 1`; beyond that, `SyncLag.count_is_bounded` is `True` and the number is a floor, not an exact count. The anchor it counts from is clamped by any unabsorbed dual-write mirror failure (fail-closed), so a mirror error after `BULK_COPY` completes freezes the reported lag rather than letting it read as more caught-up than reality; frozen lag is recovered with `MigrationCoordinator.run_resync_pass` (see above).
  - `find_nearest_source_position` (checkpoint translation) is now a binary search over the position-mapping table's surrogate row order rather than a `source_position DESC` index scan, since opaque tokens have no SQL-orderable representation. See `docs/api/migration-schema.md` for the constraint this rests on.
- **Locks ring migration** (ADR 0029, completing the split ADR 0024/0026 applied to checkpoints/DLQ and outbox): `PostgreSQLLockManager` moved from `eventsource.locks` to `eventsource.adapters.postgresql.locks`; `LockInfo` and `migration_lock_key` moved to `eventsource.ports.locks`. Top-level `eventsource` imports are unaffected -- none of these names were ever re-exported from `eventsource`.

  | Old import | New import |
  | --- | --- |
  | `eventsource.locks.PostgreSQLLockManager` | `eventsource.adapters.postgresql.locks.PostgreSQLLockManager` |
  | `eventsource.locks.LockInfo` | `eventsource.ports.locks.LockInfo` |
  | `eventsource.locks.migration_lock_key` | `eventsource.ports.locks.migration_lock_key` |
  | `eventsource.locks.LockAcquisitionError` | `eventsource.exceptions.LockAcquisitionError` |
  | `eventsource.locks.LockNotHeldError` | `eventsource.exceptions.LockNotHeldError` |

- **Read-model ring migration** (ADR 0029): the contract half moved from `eventsource.readmodels` to `eventsource.ports.readmodels`; the backend half split across three adapter modules plus `eventsource.adapters.sql`. Top-level `eventsource` imports are unaffected -- `ReadModelProjection` remains the only name re-exported from `eventsource`, unchanged.

  | Old import | New import |
  | --- | --- |
  | `eventsource.readmodels.ReadModel` | `eventsource.ports.readmodels.ReadModel` |
  | `eventsource.readmodels.Query` | `eventsource.ports.readmodels.Query` |
  | `eventsource.readmodels.Filter` | `eventsource.ports.readmodels.Filter` |
  | `eventsource.readmodels.ReadModelRepository` | `eventsource.ports.readmodels.ReadModelRepository` |
  | `eventsource.readmodels.ReadModelError` | `eventsource.ports.readmodels.ReadModelError` |
  | `eventsource.readmodels.OptimisticLockError` | `eventsource.ports.readmodels.OptimisticLockError` |
  | `eventsource.readmodels.ReadModelNotFoundError` | `eventsource.ports.readmodels.ReadModelNotFoundError` |
  | `eventsource.readmodels.InMemoryReadModelRepository` | `eventsource.adapters.memory.readmodels.InMemoryReadModelRepository` |
  | `eventsource.readmodels.PostgreSQLReadModelRepository` | `eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository` |

  (`SQLiteReadModelRepository` moved to `eventsource.adapters.sqlite.readmodels`; `ReadModelProjection` to `eventsource.adapters.sql.readmodel_projection`; `generate_schema`/`generate_indexes`/`generate_full_schema`/`POSTGRESQL_TYPE_MAP`/`SQLITE_TYPE_MAP` to `eventsource.adapters.sql.readmodel_schema` -- nine rows moved in total.)

- **`eventsource/engine.py` moved to `eventsource/adapters/_sql/engine.py`.** `eventsource.create_async_engine` (the canonical public name) is unchanged in signature and behavior. Anyone importing `eventsource.engine` directly -- which the docs never told them to do -- should import from `eventsource` instead. See ADR 0029.
- **`LockAcquisitionError` and `LockNotHeldError` now subclass `EventSourceError` and live in `eventsource.exceptions`** (ADR 0029). This is the one semantic change in the locks/readmodels/engine ring-migration slice, and it is widening only: every existing `except LockAcquisitionError` and `except Exception` still catches exactly as before; the newly-catching clause is `except EventSourceError`, which caught nothing lock-related before this change. Previously both derived directly from `Exception`, defined in `eventsource/locks/postgresql.py`.
- **BREAKING: `eventsource.migration` no longer exists** (ADR 0034, the last top-level package to join the ring map). `import eventsource.migration` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030/0031/0032/0033 already applied without qualification.

  | Old import | New import |
  | --- | --- |
  | `eventsource.migration.{coordinator,cutover,router,bulk_copier,dual_write,exceptions,metrics,position_mapper,status_streamer,subscription_migrator,sync_lag_tracker,write_pause}` | `eventsource.application.migration.*` (same names) |
  | `eventsource.migration.models` | `eventsource.ports.migration.models` |
  | `eventsource.migration.repositories.{migration,routing,position_mapping,audit_log}` (the four `Protocol` classes) | `eventsource.ports.migration.repositories` |
  | `eventsource.migration.repositories.{migration,routing,position_mapping,audit_log}` (the four `PostgreSQL*` implementations, plus `VALID_TRANSITIONS`) | `eventsource.adapters.sql.migration` |

  Moving the package onto a ring the `import-linter` layers contract actually covers surfaced a latent violation: five application-ring orchestration modules (`coordinator.py`, `cutover.py`, `router.py`, `position_mapper.py`, `bulk_copier.py`) imported the repository *implementations* directly, with no Protocol indirection. The fix was completing the Protocol/implementation split those modules should always have had, not an exception to the contract. **`MigrationError` now subclasses `EventSourceError`** (previously a bare `Exception`), widening only -- every existing `except MigrationError` still catches, and `except EventSourceError` newly catches migration failures too. The two targeted `import-linter` forbidden contracts guarding this boundary ("Application ring must not import adapters" and "Ports must not import adapters, application, or migration") are replaced by one full `type = "layers"` contract (`adapters > application > ports > domain`), adding domain-ring coverage neither predecessor contract had. See ADR 0034.
- **BREAKING: `eventsource.ports.snapshots.SnapshotStore` is a `Protocol`, not an `ABC`.** Subclassing it no longer enforces abstractness (no `TypeError` on missing methods), and instantiating it directly now raises `TypeError: Protocols cannot be instantiated` rather than an abstract-class error. `snapshot_exists` is now one of the four core (bodyless) Protocol methods rather than a concrete default implemented via `get_snapshot`; every shipped adapter already implemented it natively. `delete_snapshots_by_type` moves to a new, separate `SnapshotTypeInvalidation` Protocol (see Added) and no longer raises `NotImplementedError` by default -- there is no default at all. `InMemorySnapshotStore`, `SQLiteSnapshotStore`, and `PostgreSQLSnapshotStore` no longer inherit from `SnapshotStore`; they satisfy it structurally. **`SnapshotConformance` (`eventsource.testing.conformance_ports`) is now core-only** (7 tests); the combined suite exercising both capabilities is renamed `SnapshotStoreConformance`, with the 2 bulk-invalidation tests split into a new `SnapshotTypeInvalidationConformance`. See ADR 0036.
- **BREAKING: `PostgreSQLEventStore.close()` no longer disposes the underlying engine by default.** The engine is always caller-supplied to the constructor; `close()` previously disposed it unconditionally, which could silently tear down a connection pool the caller still held or shared with other consumers (e.g. `SyncStoreFacade(PostgreSQLEventStore(shared_engine)).close()`). A new keyword-only `owns_engine: bool = False` constructor parameter controls this: `close()` disposes only when `owns_engine=True`. To restore the old behavior, pass `owns_engine=True` explicitly at construction. `SyncStoreFacade.close` now checks `isinstance(store, SupportsClose)` (see Added) instead of `getattr(store, "close", None)` duck-typing. See ADR 0037.

### Removed

- **`eventsource.snapshots` package deleted**, including the `eventsource.aggregates` and `eventsource.snapshots` import paths themselves (the `# TRANSITION` re-export shims planned for these modules were never shipped -- the package was dissolved directly). Import `Snapshot` / `SnapshotStore` from `eventsource.ports.snapshots` or continue using the top-level `eventsource` re-exports.
- **`AggregateSnapshotManager`** and the strategy classes it composed -- `SnapshotStrategy`, `ThresholdSnapshotStrategy`, `BackgroundSnapshotStrategy`, `NoSnapshotStrategy`, `create_snapshot_strategy` (formerly `eventsource.snapshots.strategies`) -- replaced by the `SnapshotPolicy` / `SnapshotScheduler` composition on `AggregateRepository` (see Added).
- **`KafkaEventBus.record_reconnection()` / `record_rebalance()`**, deprecated in 0.7.0 with removal planned for 0.8.0. Use their replacements directly.
- **`InMemoryEventBus.published_events` / `clear_published_events()`**, deprecated in 0.6.0 with removal planned for 0.8.0. Use `eventsource.testing.RecordingEventBus` instead.
- **`eventsource.repositories._json`** internal module.
- **`CheckpointRepositoryProtocol`, `DLQRepositoryProtocol`, `DLQRepository.list_failed_events`, `DLQRepository.get_failed_event`, `ProjectionCheckpointManager`, `ProjectionDLQManager`, `eventsource.repositories._dialect`** -- removed as part of the projection persistence ports split (ADR 0024). `list_failed_events` / `get_failed_event` were pure aliases for `get_failed_events` / `get_failed_event_by_id`, which remain. `ProjectionCheckpointManager` and `ProjectionDLQManager` are replaced by module-level functions in `eventsource.application.projections.checkpoints` and `.dlq` (see Changed).
- **`eventsource.repositories` -- the whole package -- is gone** (ADR 0026, completing the outbox ring migration). `import eventsource.repositories` now raises `ModuleNotFoundError`. Its `EventSourceJSONEncoder`/`json_dumps`/`json_loads` re-exports go with it -- import them from `eventsource.serialization` instead, which is where they are actually defined. No shim, no deprecation warning: the library is unreleased, so the standing rule applies without qualification.
- **`OutboxRepositoryProtocol`** -- a bare alias for `OutboxRepository`, kept "for compatibility." One name per thing; use `OutboxRepository`.
- **`OutboxRepository.list_pending_events`** -- a second name that delegated to `get_pending_events`. Use `get_pending_events` directly.
- **`eventsource.repositories._connection.execute_with_connection`** -- the SQL connection-normalization helper. Every former caller uses `sql_connection` from `eventsource.adapters._sql.connection` instead (see Changed).
- **The legacy `EventStore` ABC surface is retired: `eventsource.stores` no longer exists** (legacy store retirement, slice (d); see ADR 0025). `import eventsource.stores` now raises `ModuleNotFoundError`. No shim, no deprecation warning, no back-compat alias -- the library is unreleased, so the standing rule applies without qualification. Every name that used to live under `eventsource.stores` or the legacy ABC surface is gone: `EventStore` (the ABC), `EventStream`, `StoredEvent`, `ReadOptions`, `LegacyStoreAdapter`, `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`, the int-sentinel `ExpectedVersion` class, the legacy `AppendResult` / `ReadDirection` classes, the legacy `InMemoryEventStore` (spelled `MemoryEventStore`) / `PostgreSQLEventStore` / `SQLiteEventStore` classes, and `EventStoreConformanceSuite` (`eventsource.testing.conformance`). The ports-layer `ExpectedVersion`, `ReadDirection`, and `AppendResult` (`eventsource.ports`) are the only classes with those names now, and are re-exported from top-level `eventsource` (see Changed above). Third-party backend authors validate against `eventsource.testing.conformance_ports` instead of subclassing `EventStoreConformanceSuite`.
- **`eventsource.adapters.memory.MemoryEventStore` renamed `InMemoryEventStore`**, for sibling-naming consistency with `PostgreSQLEventStore` / `SQLiteEventStore`. `MemoryEventStore` is not kept as an alias.
- **Behavior change: `AppendResult.position` is the position of the first appended event, not the last.** The legacy stores' `global_position` was the position of the last event in the batch; every ports adapter (memory, postgresql, sqlite) returns the first event's position. A caller doing its own arithmetic on the returned position across a multi-event append must account for this.
- **Behavior change: duplicate `event_id` appends now raise `DuplicateEventError`.** The legacy in-memory and PostgreSQL stores silently skipped a duplicate append and returned as if it had succeeded; every ports adapter raises instead.
- **Behavior change: category reads (`CategoryQuery.read_category`) filter and order on storage time, inclusive, not event time, exclusive.** The legacy `get_events_by_type(..., from_timestamp=...)` filtered and ordered on the event's own `occurred_at`, exclusive (`>`). The ports equivalent filters and orders on `EventEnvelope.stored_at`, inclusive (`>=`), with position as a deterministic tie-break, and rejects naive datetimes with `ValueError` rather than silently comparing them against timezone-aware ones.
- **Behavior change: the in-memory adapter's `stored_at` is a real timestamp, not a fabrication.** The legacy in-memory store fabricated `stored_at=event.occurred_at`; `InMemoryEventStore` stamps `datetime.now(UTC)` at append time. Tests asserting `stored_at == occurred_at` against the in-memory store no longer hold.
- **Behavior change: appending an empty event list now raises `ValueError`.** The legacy stores returned a no-op successful `AppendResult`; every ports adapter raises instead.
- **Behavior change: `current_position()` on an empty store returns `None`, not `0`.** The legacy `get_global_position()` returned `0` for an empty store; the ports equivalent's `None` must be treated as "empty feed," not as a comparable floor.
- **Removed capability: BACKWARD feed reads and feed-level timestamp filters have no ports equivalent.** No in-tree consumer used either (catch-up, bulk copy, and consistency checking all read FORWARD without timestamp filters); both died with `ReadOptions`. Per-stream BACKWARD reads (`StreamReader.read_stream(..., direction=ReadDirection.BACKWARD)`) are unaffected and remain native in all three adapters.
- **Removed capability: cross-type `get_events(aggregate_type=None)` has no ports equivalent.** No production caller exercised it. A narrow `StreamDiscovery.find_streams(aggregate_id) -> list[StreamId]` port was considered and rejected as unbuilt speculation; see ADR 0025.
- **`TypeConverter` and its field-name-guessing behavior are gone, not replaced.** Structured payload fields on a `DomainEvent` should be declared as typed pydantic sub-models; pydantic's own coercion handles datetimes, UUIDs, and decimals at the JSON boundary. See `docs/explanation/sql-backend-type-handling.md`.
- **`OptimisticLockError` keeps its int-typed `expected_version` field, deliberately.** It is not retyped to carry the ports `ExpectedVersion` VO -- see ADR 0025.
- **The legacy BIGINT position columns (`projection_checkpoints.global_position`, `migration_position_mappings.source_position` / `.target_position`, `tenant_migrations.last_source_position` / `.last_target_position`) are frozen, not dropped.** They are neither written nor read by the library after this release; they remain in the schema and die with their own schema revision, not this one (dropping a column is destructive, and `schemas/checkpoints.sql` is under the Do Not Modify rule).
- Nearest-position lookup in migration checkpoint translation (`find_nearest_source_position`) is now a binary search over the position-mapping table's surrogate row order, resting on a documented monotonicity precondition, since opaque `Position` tokens have no SQL-orderable representation.
- **BREAKING: top-level module ring consolidation -- `eventsource.types`, `eventsource.exceptions`, `eventsource.protocols`, `eventsource.commands`, `eventsource.sync`, and `eventsource.serialization` no longer exist** (ADR 0030, completing the ring migration). `import eventsource.types`, `.exceptions`, `.protocols`, `.commands`, `.sync`, and `.serialization` now all raise `ModuleNotFoundError`. No shim, no deprecation warning: the library is unreleased, so the standing rule already applied to `eventsource.stores` (ADR 0025) and `eventsource.repositories` (ADR 0026) applies here without qualification. Replacements: `eventsource.types` -> `eventsource.domain.types`; `eventsource.exceptions` -> `eventsource.domain.exceptions`; `eventsource.protocols` -> `eventsource.ports.handlers`; `eventsource.commands` (`DomainCommand`, from ADR 0022) -> `eventsource.domain.command` -- this move also fixes a dependency-rule violation, since `domain/aggregate.py` and `domain/decider.py` had been importing `DomainCommand` from a top-level package the ring map placed nowhere; `eventsource.sync` -> `eventsource.adapters.sync`; `eventsource.serialization` -> `eventsource.adapters.serialization`. Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly and always did.
- **BREAKING: `eventsource.locks` and `eventsource.readmodels` no longer exist, ahead of the 0.8.0 removal ADR 0029 originally scheduled** (ADR 0030). `import eventsource.locks` and `import eventsource.readmodels` now raise `ModuleNotFoundError`. The two deprecation shims ADR 0029 introduced are deleted as part of the same pre-1.0 "no shims" decision applied to the six modules above. Replacements unchanged from ADR 0029: `eventsource.ports.locks` / `eventsource.adapters.{memory,postgresql}.locks` and `eventsource.ports.readmodels` / `eventsource.adapters.{memory,postgresql,sqlite}.readmodels`.
- **`eventsource.config` deleted.** ADR 0030: a seven-line placeholder module (docstring + one trailing comment, zero imports, zero classes, zero functions, no `__all__`) with no importer anywhere in `src/` or `tests/`. There was no old import path to keep working, because nothing imported it.
- **BREAKING: `eventsource.bus` -- the whole package, including its facade `__init__.py` -- no longer exists** (ADR 0031, completing the ring migration's last multi-backend top-level package). `import eventsource.bus` and every `eventsource.bus.*` submodule import now raise `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030 already applied without qualification. Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly.

  | Old import | New import |
  | --- | --- |
  | `eventsource.bus.interface.EventBus` | `eventsource.ports.bus.EventBus` |
  | `eventsource.bus.base.BaseEventBus` | `eventsource.adapters._bus.BaseEventBus` |
  | `eventsource.bus.registry.SubscriptionRegistry` | `eventsource.adapters._bus.SubscriptionRegistry` |
  | `eventsource.bus.memory.InMemoryEventBus` | `eventsource.adapters.memory.bus.InMemoryEventBus` |
  | `eventsource.bus.redis.RedisEventBus` / `.RedisEventBusConfig` | `eventsource.adapters.redis.bus.RedisEventBus` / `.RedisEventBusConfig` |
  | `eventsource.bus.kafka.*` (`KafkaEventBus` + consumer/publisher/connection/config/dlq/metrics/models collaborators) | `eventsource.adapters.kafka.*` |
  | `eventsource.bus.rabbitmq.*` (`RabbitMQEventBus` + consumer/publisher/connection/config/dlq/topology/serialization/models/death_headers collaborators) | `eventsource.adapters.rabbitmq.*` |

  `REDIS_AVAILABLE`, `KAFKA_AVAILABLE`, and `RABBITMQ_AVAILABLE` move with their respective adapter packages, unchanged in behavior. This deletion also completes the "Remove bus facade compat shims" backlog entry -- there is no facade left to shim, so the ~90 white-box test call sites that reached through `bus._connection_manager.*`-style properties are retargeted onto `eventsource.adapters._bus` and the per-backend collaborator modules in the same pass. See ADR 0031, which amends ADR 0007, ADR 0010, ADR 0011, and ADR 0020 for module locations only -- none of those four ADRs' Decisions change.
- **BREAKING: `eventsource.subscriptions` no longer exists** (ADR 0032, completing the ring migration). `import eventsource.subscriptions` now raises `ModuleNotFoundError`. No shim, no deprecation warning: the same "the library is unreleased" standing rule ADR 0025, ADR 0026, and ADR 0030 already applied. Replacements: seventeen orchestration modules (`manager.py`, `lifecycle.py`, `registry.py`, `pause_resume.py`, `health_provider.py`, `health.py`, `shutdown.py`, `metrics.py`, `transition.py`, `subscription.py`, `config.py`, `filtering.py`, `flow_control.py`, `retry.py`, `error_handling.py`, `runners/{catchup,live}.py`, plus the concrete subscriber base classes in `subscriber.py`) -> `eventsource.application.subscriptions` (same names); the `Subscriber`/`SyncSubscriber`/`BatchSubscriber` Protocols and their two helper functions -> `eventsource.ports.subscribers`; the `LeaderElector`/`LeaderElectorWithLease` Protocols and `LeaderChangeCallback` -> `eventsource.ports.coordination`; the coordination message types, topic constants, and `WorkRedistributionCoordinator` -> `eventsource.application.subscriptions` (still, via `coordination.py`); **`InMemoryLeaderElector` and `SharedLeaderState` move to `eventsource.adapters.memory`** (`adapters.memory.coordination`), not `application.subscriptions` -- the one name-availability break beyond the package path itself, since the application ring may not import adapters; `EventHandlerFunc` -> `eventsource.ports.bus` (relocated together with `EventBus` by the bus ring split, ADR 0031). A new `SubscribableEventBus` port (`eventsource.ports.bus`), a two-method Protocol `EventBus` satisfies structurally, replaces the `TYPE_CHECKING`-only `EventBus` imports the runners previously depended on. **The subscription exception hierarchy (`SubscriptionError` and its eight subclasses) moves from its own module into `eventsource.domain.exceptions`, and `SubscriptionError` is rebased onto `EventSourceError`** (previously a bare `Exception` subclass) -- widening only: every existing `except SubscriptionError` still catches, and `except EventSourceError` newly catches subscription failures too. Top-level `from eventsource import ...` imports are unaffected -- the top-level package never exported subscription names, so nothing there changes.

- **BREAKING: `eventsource.events` no longer exists** (ADR 0033, dissolving the last transitional entities-ring package). `import eventsource.events` and every `eventsource.events.*` submodule import now raise `ModuleNotFoundError`. No shim, no deprecation warning: the same pre-1.0, no-external-consumers standing rule ADR 0025/0026/0029/0030/0031/0032 already applied without qualification. Replacements: `eventsource.events.base.DomainEvent` -> `eventsource.domain.event.DomainEvent`; `eventsource.events.registry.EventRegistry` / `.register_event` / `.default_registry` / `.get_event_class` / `.get_event_class_or_none` / `.is_event_registered` / `.list_registered_events` -> `eventsource.domain.event_registry` (same names); `eventsource.events.registry.EventTypeNotFoundError` / `.DuplicateEventTypeError` -> `eventsource.domain.exceptions` (both rebased onto `EventSourceError`, widening only -- their `KeyError` / `ValueError` mixins are retained). Top-level `from eventsource import ...` imports are unaffected -- the barrel re-exports from the new homes directly.
- **BREAKING: `eventsource.handlers` no longer exists** (ADR 0033). `import eventsource.handlers` now raises `ModuleNotFoundError`. No shim, no deprecation warning, same standing rule. Replacements: `eventsource.handlers.decorators.handles` / `.get_handled_event_type` / `.is_event_handler` -> `eventsource.domain.decorators` (domain-ring, since `DeclarativeAggregate` is their only consumer); `eventsource.handlers.registry.HandlerRegistry` / `.HandlerInfo` / `.UnregisteredEventHandling` -> `eventsource.application.projections.handlers` (the ADR-0013 collaborator extracted out of `DeclarativeProjection`); `eventsource.handlers.registry.HandlerSignatureError` -> `eventsource.domain.exceptions` (rebased onto `EventSourceError`, `ValueError` mixin retained, widening only); `eventsource.handlers.adapter.HandlerAdapter` / `.get_handler_name` -> `eventsource.adapters._bus.handler_adapter` (every importer is a bus adapter). **The `AsyncEventHandler` / `SyncEventHandler` compatibility re-export that `eventsource.handlers.adapter` used to carry is dropped, not repointed** -- import them from their canonical home, `eventsource.ports.handlers`, which already worked. Top-level `from eventsource import ...` imports are unaffected; `handles` is still re-exported from the barrel.
- **BREAKING: `eventsource._internal` no longer exists** (ADR 0033). `import eventsource._internal` now raises `ModuleNotFoundError`. Replacement: `eventsource._internal.background_tasks.BackgroundTaskManager` -> `eventsource.application.background_tasks.BackgroundTaskManager`. `BackgroundTaskManager` is shared by `application/aggregates/`'s background snapshot scheduling and `adapters/_bus/`'s shutdown drain; it lands in `application/` because the Dependency Rule lets an outer ring (`adapters/`) depend inward on an inner ring (`application/`) but never the reverse, so the innermost of its two consumers is the only dependency-rule-compatible owner. No code inside the class changed -- this is a pure relocation.
- **`MigrationRepositoryProtocol`, `TenantRoutingRepositoryProtocol`, `PositionMappingRepositoryProtocol`, `MigrationAuditLogRepositoryProtocol`** -- bare aliases for the corresponding Protocol classes, kept "for compatibility." One name per thing; no consumers were found anywhere in the codebase. Use `MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`, `MigrationAuditLogRepository` directly from `eventsource.ports.migration.repositories`. See ADR 0034.

### Fixed

- **`InMemoryEventStore`/SQLite test fixtures no longer hang the test process on shutdown.** `tests/unit/test_sqlite_read_isolation.py` left `:memory:` stores open across tests, and aiosqlite's background writer thread is non-daemon, so an unclosed connection kept the interpreter alive after the test run finished. Stores are now closed in a fixture teardown.
- **SQLite reads can no longer observe a partially committed append.** All read paths shared the writer's aiosqlite connection but ran outside the write lock, and `append` is multi-statement — so a read scheduled between two of its INSERTs ran inside the open transaction and could yield a torn batch from `read_all`, or mint a `Position` from `current_position` for a row that was then rolled back. Reads now take the same lock; the connection stays shared, which `":memory:"` databases require.
- **SQLite outbox schema corrected: `event_outbox.id` is now `TEXT PRIMARY KEY`, was `INTEGER PRIMARY KEY AUTOINCREMENT`.** `SQLiteOutboxRepository.add_event` inserts a `str(uuid4())` into that column, which SQLite's strictly-typed rowid alias rejects with `sqlite3.IntegrityError: datatype mismatch` -- so every insert against the shipped schema failed, and the table has never held a row written by this library. **Migration note:** an existing SQLite database provisioned from `migrations/templates/sqlite/outbox.sql` or `migrations/schemas/sqlite_all.sql` carries an empty, unusable `event_outbox` table. `CREATE TABLE IF NOT EXISTS` will not replace it -- run `DROP TABLE event_outbox;` and re-provision from the corrected schema. No data can be lost: none can have existed. See ADR 0027.
- **`SQLiteOutboxRepository.cleanup_published` no longer silently deletes nothing at `days=0`.** It compared `published_at` (written as `datetime.now(UTC).isoformat()`, `'T'`-separated with microseconds and a UTC offset) against SQLite's `datetime('now', '-N days')` (space-separated, no microseconds) as raw TEXT; `'T'` (0x54) sorts after `' '` (0x20), so the comparison never matched for a cutoff computed within the same wall-clock second, and an entry published moments ago was never eligible for cleanup regardless of `days`. The cutoff is now computed in Python (`datetime.now(UTC) - timedelta(days=days)`) and bound as a parameter in the same `isoformat()` shape `published_at` is written in, so both sides of the comparison share one format. Only surfaced once the `id` column fix above made `add_event` succeed against the real schema -- the outbox conformance suite could not previously reach this code path.
- **Default-path migrations now record position mappings.** `MigrationCoordinator` accepted a `position_mapper` and `MigrationConfig.position_mapping_enabled` documented a default of `True`, but the flag was read nowhere and the coordinator never passed the mapper to its `BulkCopier` -- so an ordinary migration recorded nothing and subscription checkpoint translation (`migrate_subscriptions=True`, also a documented default) silently skipped. Mappings are now recorded whenever the coordinator was given a mapper and the flag is True. Note the cost: with a mapper attached the bulk copier appends one event at a time so each target position can be recorded, where it otherwise batches; set `position_mapping_enabled=False` to keep the batched path.
- **Live-phase subscription lag now reports events received but not yet delivered; it was previously always 0.** `Subscription.lag` is `events_seen - events_delivered`, and the live runner counted deliveries without ever counting receipts -- so a stalled subscriber with events arriving was indistinguishable from a healthy idle one, and the accumulated delivered-surplus made a later return to catch-up under-report real backlog. Live lag now includes the catch-up->live transition buffer and the pause buffer, so a paused or stalled subscription shows growing lag.
- **Catch-up no longer terminates early with `completed=False` when a read batch is entirely filtered out.** The loop broke on a zero *delivered* count, which conflated "the feed is exhausted" with "nothing in this batch matched the event-type filter" — so a heavily-filtered subscription reported failure despite having advanced its position with more feed behind it. Termination is now exactly reaching the target position (or a stop request). `CatchUpResult.events_processed` is unchanged and still counts events delivered to the subscriber.
- **`InMemoryDLQRepository.delete_resolved_events` now uses the same rolling cutoff as the SQL adapter.** It truncated `now` to midnight UTC before subtracting `older_than_days`, so `older_than_days=0` kept entries resolved earlier the same day while PostgreSQL and SQLite deleted them. The port now specifies the cutoff — exactly `datetime.now(UTC) - timedelta(days=older_than_days)`, with an entry deleted iff it is resolved and `resolved_at` is strictly before it — and the conformance suite pins it for every backend. This is a behavior change on a public class, though in practice the in-memory DLQ is a test and development backend.

## [0.7.0] - 2026-07-30

### Changed

- **RabbitMQ and Kafka backends decomposed into internal collaborator packages.** `eventsource.bus.rabbitmq` and `eventsource.bus.kafka` are now packages of internal, state-owning collaborators (connection, topology/config, publisher, consumer, DLQ admin, serialization) composed by a facade; imports are unchanged. See ADR 0020.
- **Kafka `background=True` publishes are now scheduled as tracked background tasks** per ADR 0010 -- send/serialization errors are logged and recorded in stats rather than raised to the caller, stats settle asynchronously, and `shutdown()` drains outstanding background publishes.

### Removed

- `KafkaEventBus.get_handlers_for_event` (deprecated in 0.6.0).

### Deprecated

- `KafkaEventBus.record_reconnection` / `record_rebalance` are deprecated; they warn and delegate to their replacements. Removal planned for 0.8.0.

## [0.6.0] - 2026-07-29

### Added

- **`eventsource.bus.base.BaseEventBus`** - Shared concrete base class for all four `EventBus` backends. Centralizes subscription management, event-class resolution, and fire-and-forget background-task tracking/draining so `interface.py` can stay a pure ABC.
- **`eventsource.bus.registry.SubscriptionRegistry`** - Thread-safe registry of event handlers keyed by event class, with a cached specific-then-wildcard handler tuple per event type so dispatch allocates nothing per event. Used internally by all four bus backends, replacing four independent (and inconsistent) implementations.
- **`eventsource.bus.retry.RetryPolicy`** - Shared retry/backoff policy with symmetric jitter, used by the broker-backed buses for consume-side redelivery and publish retries.
- **`eventsource.testing.RecordingEventBus`** - Purpose-built in-memory bus for tests that need to assert on published events, replacing the ad hoc `InMemoryEventBus.published_events` / `clear_published_events` attributes.
- **`eventsource.HandlerDispatchError`** - New public exception. Broker consume paths (Kafka, RabbitMQ, Redis) now run every registered handler for a delivered event, aggregate any failures into a `HandlerDispatchError`, and withhold the ack so the broker redelivers -- instead of aborting on the first handler failure and silently dropping the rest.
- CI now runs the Kafka and RabbitMQ integration suites, run against real brokers via testcontainers in a blocking CI job.
- ADR 0010 and ADR 0011, documenting the event bus contract decisions behind this release (shared base/registry/retry, uniform handler-error isolation, broker CI).

### Changed

- **`EventBusConformanceSuite` gained a new abstract method `create_subscriber`** (and an overridable `await_delivery` hook). Existing third-party subclasses of the conformance suite must implement `create_subscriber` to keep passing on upgrade.

- **`RedisEventBus.publish` now honors `background=True`.** Previously the parameter was accepted but silently ignored for Redis (documented as "Ignored for Redis"); background publishes are now genuinely fire-and-forget, matching the other backends.
- **Uniform handler-error isolation across all backends.** Every registered handler now runs for each delivered event, regardless of whether an earlier handler raised. Previously Redis and RabbitMQ aborted dispatch on the first handler failure, silently skipping any handlers registered after it.
- **Kafka retry jitter is now symmetric.** Previously jitter was one-sided positive, which meant effective backoff could exceed `retry_max_delay`. Jitter is now applied symmetrically, so backoff never exceeds the configured maximum.
- **Kafka publishes are now batched** rather than awaiting one broker round-trip per event, improving publish throughput for multi-event batches.
- **Kafka handler dispatch is now keyed by event class** rather than by class name. Previously, an event class whose `event_type` field differed from its class name would silently fail to reach its handlers; dispatch now resolves handlers the same way as the other backends.
- **Kafka background publishes no longer crash.** A misuse of aiokafka's `Future` API in the background-publish path is fixed.
- Subscription management (subscribe/unsubscribe/wildcard/clear/count) is now genuinely thread-safe in all four backends, via the shared `SubscriptionRegistry`.

### Deprecated

- `KafkaEventBus.get_handlers_for_event` is deprecated. It remains available as a shim for existing callers but new code should not depend on it.
- `InMemoryEventBus.published_events` and `InMemoryEventBus.clear_published_events` are deprecated in favor of `eventsource.testing.RecordingEventBus`.

## [0.5.0] - 2025-12-15

### Added

- **Multi-Tenancy Module** (`eventsource.multitenancy`) - First-class multi-tenant support
  - `tenant_context` ContextVar for managing tenant context across async boundaries
  - `tenant_scope` async context manager for scoped tenant operations
  - `tenant_scope_sync` sync context manager for synchronous code paths
  - Helper functions: `get_current_tenant()`, `get_required_tenant()`, `set_current_tenant()`, `clear_tenant_context()`
  - `TenantDomainEvent` base class with required `tenant_id` field and `with_tenant_context()` class method
  - `TenantAwareRepository` wrapper that enforces tenant isolation on load/save operations
  - `TenantContextNotSetError` and `TenantMismatchError` exceptions for clear error handling
  - Tenant-aware projections with automatic filtering via `tenant_filter` parameter
  - Public exports from `eventsource`: `tenant_context`, `tenant_scope`, `tenant_scope_sync`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `clear_tenant_context`, `TenantDomainEvent`, `TenantContextNotSetError`, `TenantMismatchError`

- **Sync Adapter** (`eventsource.sync`) - Synchronous wrappers for async components
  - `SyncEventStoreAdapter` for using async event stores in synchronous contexts
  - Ideal for Celery tasks, Django management commands, RQ workers, and other sync environments
  - Configurable timeout for operations (default: 30 seconds)
  - Public export from `eventsource`: `SyncEventStoreAdapter`

- **Testing Module** (`eventsource.testing`) - Comprehensive testing utilities
  - `EventBuilder` - Fluent builder for creating test events with minimal boilerplate
    - `with_aggregate_id()`, `with_version()`, `with_tenant_id()`, `with_timestamp()` chainable methods
    - `build()` for single events, `build_sequence()` for event chains
  - `InMemoryTestHarness` - Pre-configured in-memory infrastructure for fast tests
    - Includes event store, event bus, checkpoint repository, and DLQ
    - `setup()` and `teardown()` lifecycle methods
    - `clear()` to reset state between tests
  - `EventAssertions` - Domain-specific test assertions with clear error messages
    - `assert_event_published()`, `assert_no_events_published()`, `assert_event_count()`
    - `assert_event_sequence()` for verifying event ordering
    - `assert_aggregate_version()`, `assert_aggregate_state()`
  - BDD-style helpers for readable tests:
    - `given_events()` - Set up initial event history
    - `when_command()` - Execute a command/action
    - `then_event_published()` - Assert expected event was published
    - `then_no_events_published()` - Assert no events were published
    - `then_event_sequence()` - Assert specific sequence of events
    - `then_event_count()` - Assert number of events
  - Public exports from `eventsource.testing`: `EventBuilder`, `InMemoryTestHarness`, `EventAssertions`, `given_events`, `when_command`, `then_event_published`, `then_no_events_published`, `then_event_sequence`, `then_event_count`

- **Aggregate `create_event()` Method** - Reduced boilerplate for event creation
  - Auto-populates `aggregate_id`, `aggregate_type`, and `aggregate_version`
  - Auto-populates `tenant_id` from context when available
  - Explicit kwargs always override auto-populated values
  - Example: `self.create_event(OrderShipped, tracking_number="TRACK-001")` instead of manually setting all aggregate fields

- **Deferred State Pattern** - Aggregates without upfront initial state
  - `requires_creation_event` class attribute on `DeclarativeAggregate`
  - When `True`, `_get_initial_state()` returns `None` and state is set by first event handler
  - `AggregateNotCreatedError` raised when accessing `state` before creation event applied
  - Useful for aggregates where initial state depends entirely on creation event data

- **Automatic Type Inference** - Less boilerplate for events and aggregates
  - `DomainEvent.event_type` now auto-infers from class name if not explicitly set
  - `DomainEvent.aggregate_type` auto-infers from aggregate's `aggregate_type` when created via `create_event()`
  - Aggregate state type (`TState`) auto-detected from Generic parameter

### Changed

- **InMemoryEventBus** - Now thread-safe with proper locking for concurrent access
- **AggregateRoot._get_initial_state()** - Return type changed from `TState` to `TState | None` to support deferred state pattern

### Tests

- Added multi-tenancy module tests (`tests/unit/multitenancy/`)
  - Context management tests (`test_context.py`)
  - TenantDomainEvent tests (`test_events.py`)
  - TenantAwareRepository tests (`test_repository.py`)
  - Projection tenant filtering tests (`tests/unit/projections/test_tenant_filter.py`)
- Added sync adapter tests (`tests/unit/sync/`)
  - Adapter functionality tests (`test_adapter.py`)
  - Concurrency tests (`test_concurrency.py`)
- Added testing module tests (`tests/unit/testing/`)
  - EventBuilder tests (`test_builder.py`)
  - InMemoryTestHarness tests (`test_harness.py`)
  - EventAssertions tests (`test_assertions.py`)
  - BDD helpers tests (`test_bdd.py`)
  - Module structure tests (`test_module_structure.py`)
- Added aggregate improvement tests
  - `create_event()` tests (`tests/unit/aggregates/test_create_event.py`)
  - Deferred state tests (`tests/unit/aggregates/test_deferred_state.py`)
  - Type inference tests (`tests/unit/aggregates/test_aggregate_type_inference.py`)
- Added automatic event type inference tests (`tests/unit/test_event_type_auto.py`)
- Added InMemoryEventBus threading tests (`tests/unit/bus/test_memory.py`)

## [0.4.0] - 2025-12-13

### Added

- **Tracer Protocol & Implementations** (`eventsource.observability.tracer`) - Composition-based tracing
  - `Tracer` protocol defining the contract for tracing implementations
  - `NullTracer` - No-op implementation for when tracing is disabled
  - `OpenTelemetryTracer` - Full OpenTelemetry integration when OTEL is available
  - `MockTracer` - Testing implementation for verifying trace calls
  - `create_tracer()` factory function for automatic tracer selection based on configuration
- **Serialization Module** (`eventsource.serialization`) - Centralized JSON utilities
  - `EventSourceJSONEncoder` for consistent JSON serialization across the library
  - `json_dumps()` and `json_loads()` helper functions
  - Proper handling of UUID, datetime, Enum, dataclass, and Pydantic model serialization
- **Handler Decorators** (`eventsource.handlers.decorators`) - Relocated and enhanced decorator
  - `@handles` decorator now in canonical location with full backward compatibility
  - `HandlerSignatureError` exception with detailed validation messages for invalid handler signatures
- **Repository Method Aliases** - Consistent naming conventions
  - `list_pending()` alias for `get_pending_events()` in OutboxRepository
  - `list_failed()` alias for `get_failed_events()` in DLQRepository
  - `get_by_id()` alias for `get_failed_event_by_id()` in DLQRepository
- **AsyncEventHandler ABC** - Consolidated to single definition in `eventsource.protocols`

### Changed

- **Tracing Architecture** - Migrated from inheritance to composition pattern
  - All 47+ traced classes now use `Tracer` composition instead of `TracingMixin` inheritance
  - Components accept optional `tracer` parameter for dependency injection
  - Enables easier testing with `MockTracer` and better separation of concerns
- **Handler Registry** - Improved validation and error messages
  - Better detection of invalid handler signatures
  - More descriptive error messages for common mistakes

### Tests

- Added comprehensive Tracer protocol tests (`tests/unit/observability/test_tracer.py`)
- Added handler decorator tests (`tests/unit/handlers/test_decorators.py`)
- Added handler registry tests (`tests/unit/handlers/test_registry.py`)
- Added serialization module tests (`tests/unit/serialization/test_json.py`)
- Added protocol consolidation tests (`tests/unit/test_protocols.py`)
- Added import compatibility tests (`tests/integration/test_imports.py`)
- Updated all existing tracing tests to use new composition pattern

## [0.3.1] - 2025-12-13

### Changed

- **Schema: `global_position` replaces `id` as primary key** - Events table now uses `global_position` as the primary key for strict sequential ordering, while `event_id` (UUID) remains as a unique constraint for deduplication and idempotency
  - PostgreSQL: `global_position BIGSERIAL PRIMARY KEY` with `event_id UUID NOT NULL UNIQUE`
  - SQLite: `global_position INTEGER PRIMARY KEY AUTOINCREMENT` with `event_id TEXT NOT NULL UNIQUE`
  - Updated all SQL templates, Alembic migration templates, and store implementations
  - Consistent naming across PostgreSQL and SQLite backends

### Fixed

- **SQLite store consistency** - SQLite event store now uses `global_position` column naming consistent with PostgreSQL, fixing column name mismatch between backends

## [0.3.0] - 2025-12-12

### Added

- **ReadModel Persistence Tooling** - Standardized read model persistence infrastructure (`eventsource.readmodels`)
  - **Phase 1 - Core Components**:
    - `ReadModel` base class with standard fields (id, timestamps, version, deleted_at)
    - `ReadModelRepository` protocol with 13 methods for CRUD, querying, and lifecycle management
    - `Query` and `Filter` classes for flexible, type-safe querying with operators (eq, ne, lt, gt, le, ge, in_, contains, startswith)
    - `InMemoryReadModelRepository` implementation for testing and development
  - **Phase 2 - SQL Backends**:
    - `PostgreSQLReadModelRepository` with full async support via asyncpg
    - `SQLiteReadModelRepository` with async support via aiosqlite
    - Schema generation utilities (`generate_postgresql_schema()`, `generate_sqlite_schema()`) for automatic table creation from ReadModel classes
  - **Phase 3 - Projection Integration**:
    - `ReadModelProjection` base class integrating with `DatabaseProjection`
    - `HandlerRegistry` integration with `@handles` decorator for event-driven updates
    - Automatic repository injection into event handlers
  - **Phase 4 - Enhanced Features**:
    - Soft delete support with `get_deleted()` and `find_deleted()` methods
    - Optimistic locking via `save_with_version_check()` for concurrent update safety
    - `OptimisticLockError` and `ReadModelNotFoundError` exceptions
  - Public exports from `eventsource.readmodels`: `ReadModel`, `ReadModelRepository`, `ReadModelProjection`, `Query`, `Filter`, `InMemoryReadModelRepository`, `PostgreSQLReadModelRepository`, `SQLiteReadModelRepository`
  - New observability attributes: `ATTR_READ_MODEL_TYPE`, `ATTR_READ_MODEL_ID`
- **Multi-Tenant Live Migration** - Zero-downtime tenant migration between event stores (`eventsource.migration`)
  - `MigrationCoordinator` orchestrating full migration lifecycle with pause/resume/abort controls
  - `BulkCopier` for streaming historical event migration with checkpointing and configurable batch sizes
  - `DualWriteInterceptor` for simultaneous writes to source and target stores during migration
  - `CutoverManager` for sub-100ms atomic tenant routing switch with rollback capability
  - `ConsistencyVerifier` for data integrity validation with COUNT, HASH, and FULL verification modes
  - `SubscriptionMigrator` for checkpoint position translation between stores
  - `TenantStoreRouter` for tenant-aware read/write routing during and after migration
  - `WritePauseManager` for coordinated write pausing during cutover
  - `SyncLagTracker` for monitoring replication lag between stores
  - Real-time status streaming via `StatusStreamer` for migration monitoring
  - Position mapping for checkpoint translation between source and target stores
  - Comprehensive error classification with retry policies and circuit breaker pattern
  - Audit logging for all migration operations
  - OpenTelemetry metrics integration (`eventsource.migration.metrics`)
- **PostgreSQL Advisory Locks** - Distributed locking for migration coordination (`eventsource.locks`)
  - `PostgreSQLAdvisoryLock` for session-level and transaction-level advisory locks
  - Lock context managers for safe acquisition and release
  - Lock timeout and retry configuration
- **Migration Exceptions** - Comprehensive exception hierarchy (`eventsource.migration.exceptions`)
  - `MigrationError`, `MigrationStateError`, `MigrationNotFoundError`
  - `BulkCopyError`, `DualWriteError`, `CutoverError`
  - `ConsistencyError`, `RoutingError`, `LockError`
  - Error classification with `ErrorCategory` and `ErrorSeverity` enums
- **Migration Documentation** - Comprehensive guides in `docs/migration/`:
  - Architecture overview and component documentation
  - Step-by-step migration guide
  - API reference for all migration components
  - Operational runbooks and troubleshooting guides
  - Monitoring and alerting setup

- **Subscription Tracing** - OpenTelemetry tracing for all subscription components
  - `SubscriptionManager` tracing for subscription lifecycle operations:
    - `subscribe`, `unsubscribe`, `start_subscription`, `stop`, `stop_subscription`
    - `pause_subscription`, `resume_subscription`
  - `TransitionCoordinator` tracing for catch-up to live transitions:
    - `execute` span with phase tracking (initial_catchup → live_subscribed → final_catchup → processing_buffer → live)
    - Watermark and buffer size attributes
  - `CatchUpRunner` tracing for historical event processing:
    - `run_until_position` span with batch progress
    - `deliver_event` span for individual event delivery
  - `LiveRunner` tracing for real-time event processing:
    - `start`, `stop`, `process_event` spans
    - `process_buffer`, `process_pause_buffer` for transition buffers
  - New subscription trace attributes in `eventsource.observability.attributes`:
    - `ATTR_SUBSCRIPTION_NAME`, `ATTR_SUBSCRIPTION_STATE`, `ATTR_SUBSCRIPTION_PHASE`
    - `ATTR_FROM_POSITION`, `ATTR_TO_POSITION`, `ATTR_BATCH_SIZE`
    - `ATTR_BUFFER_SIZE`, `ATTR_EVENTS_PROCESSED`, `ATTR_EVENTS_SKIPPED`, `ATTR_WATERMARK`
  - All components support `enable_tracing` parameter (default: `True`)
  - Graceful degradation when OpenTelemetry is not installed

- **Subscription Manager** - New `eventsource.subscriptions` module for building event-driven projections with catch-up subscriptions and live event streaming
  - `SubscriptionManager` class for coordinating subscriptions with unified lifecycle management
    - Automatic catch-up from event store historical data
    - Seamless transition to live event streaming via event bus
    - Multiple subscriber support with concurrent processing
    - Graceful shutdown with SIGTERM/SIGINT signal handling (`run_until_shutdown()`)
    - Pause/resume functionality for individual subscriptions
  - `Subscription` class with state machine for subscription lifecycle (idle → starting → catching_up → live → pausing → paused → resuming → stopping → stopped → failed)
  - `SubscriptionConfig` for configurable subscription behavior:
    - `batch_size`: Events per batch during catch-up (default: 100)
    - `checkpoint_interval`: Events between checkpoints (default: 100)
    - `checkpoint_strategy`: "batch" or "interval" checkpointing
    - `start_from`: Start position ("beginning", "end", or specific position)
    - `filter_event_types`: Optional event type filtering
  - Subscriber protocols and base classes:
    - `Subscriber` and `SyncSubscriber` protocols for event handlers
    - `BatchSubscriber` protocol for batch event processing
    - `BaseSubscriber`, `BatchAwareSubscriber`, and `FilteringSubscriber` base classes
  - Catch-up and live runner implementations:
    - `CatchupRunner` for reading historical events from event store with batching
    - `LiveRunner` for streaming real-time events from event bus
    - `TransitionCoordinator` for seamless handoff between modes
  - Comprehensive error handling (`eventsource.subscriptions.error_handling`):
    - `SubscriptionErrorHandler` with configurable retry policies
    - `ErrorSeverity` levels: low, medium, high, critical
    - `ErrorCategory` classification: event_processing, checkpoint, transition, infrastructure
    - Error callbacks: `on_error()` and `on_critical_error()` hooks
    - Circuit breaker pattern for failing subscriptions
  - Retry system (`eventsource.subscriptions.retry`):
    - Configurable retry with exponential backoff
    - Jitter support for distributed systems
    - Max retries and timeout limits
  - Health monitoring (`eventsource.subscriptions.health`):
    - `ManagerHealthChecker` for overall system health
    - `SubscriptionHealthChecker` for per-subscription health
    - Kubernetes-compatible liveness/readiness probes
    - `HealthStatus`, `LivenessStatus`, `ReadinessStatus` enums
    - Configurable health check thresholds
  - Metrics collection (`eventsource.subscriptions.metrics`):
    - Events processed, errors, lag, and processing duration metrics
    - Per-subscription and aggregate statistics
  - Flow control (`eventsource.subscriptions.flow_control`):
    - Backpressure handling for slow consumers
    - Rate limiting support
  - Graceful shutdown (`eventsource.subscriptions.shutdown`):
    - `ShutdownCoordinator` with phased shutdown sequence
    - Configurable shutdown timeout
    - In-flight event completion before shutdown
    - `FlowController.wait_for_drain()` for tracking in-flight events during shutdown
    - `ShutdownReason` enum for tracking shutdown triggers (SIGNAL_SIGTERM, SIGNAL_SIGINT, PROGRAMMATIC, HEALTH_CHECK, TIMEOUT, DOUBLE_SIGNAL)
    - Pre-shutdown hooks (`on_pre_shutdown()`) for cleanup before shutdown (e.g., load balancer deregistration)
    - Post-shutdown hooks (`on_post_shutdown()`) for actions after shutdown completes
    - Shutdown deadline support (`set_shutdown_deadline()`) for Kubernetes `terminationGracePeriodSeconds` compliance
    - Periodic checkpoint saves during drain phase (`checkpoint_interval` parameter)
    - Shutdown metrics with OpenTelemetry integration (`ShutdownMetricsSnapshot`)
  - Multi-instance coordination (`eventsource.subscriptions.coordination`):
    - `LeaderElector` protocol for distributed leadership election
    - `LeaderElectorWithLease` extended protocol for lease-based leadership
    - `InMemoryLeaderElector` implementation for single-instance and testing scenarios
    - `WorkRedistributionCoordinator` for coordinating work handoff during shutdown
    - `ShutdownNotification` and `HeartbeatMessage` for peer-to-peer coordination
    - Support for graceful work redistribution when instances shut down
  - Event filtering (`eventsource.subscriptions.filtering`):
    - Filter events by type, aggregate, or custom predicates
  - Global position support in event stores:
    - `PostgreSQLEventStore.subscribe_all_from_position()` for ordered event streaming
    - `SQLiteEventStore.subscribe_all_from_position()` for ordered event streaming
    - `global_position` field in stored events for total ordering
  - Database migrations for `checkpoints` table with position tracking
  - Comprehensive documentation: API reference, user guide, migration guide, and examples
  - Exception hierarchy: `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `CheckpointNotFoundError`
- **TypeConverter Extraction** - Refactored serialization logic from event stores into a dedicated component
  - New `TypeConverter` protocol defining the contract for type conversion during event deserialization
  - `DefaultTypeConverter` implementation with configurable UUID and datetime field detection
  - `DEFAULT_UUID_FIELDS` and `DEFAULT_STRING_ID_FIELDS` constants for common field patterns
  - `DefaultTypeConverter.strict()` factory method for explicit-only UUID field configuration
  - SQLiteEventStore now has full configuration parity with PostgreSQLEventStore:
    - Added `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` constructor parameters
    - Added `with_strict_uuid_detection()` factory method
  - Public exports from `eventsource.stores`: `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`
  - 37 unit tests for comprehensive TypeConverter coverage

### Changed

- Removed ~120 lines of duplicate serialization code from PostgreSQL and SQLite event stores by extracting to shared `TypeConverter`

### Fixed

- **SQLiteOutboxRepository**: `get_pending_events()` now returns `list[OutboxEntry]` instead of `list[dict]`, matching the protocol specification
- **SQLiteOutboxRepository**: `add_event()` now properly stores and returns a UUID as the outbox ID, matching PostgreSQL behavior
- **SQLiteDLQRepository**: `get_failed_events()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`) from ISO 8601 strings to `datetime` objects
- **SQLiteDLQRepository**: `get_failed_event_by_id()` now properly parses timestamp fields (`first_failed_at`, `last_failed_at`, `resolved_at`) to `datetime` objects
- **SQLite schema**: Event outbox table now uses `TEXT PRIMARY KEY` for the `id` column (UUID as string) instead of `INTEGER PRIMARY KEY AUTOINCREMENT` to match PostgreSQL schema

### Breaking Changes (Internal)

- Internal methods `_is_uuid_field()` and `_convert_types()` on event stores have been removed
  - Users who were calling these internal methods directly should migrate to `store._type_converter.is_uuid_field()` and `store._type_converter.convert_types()`

### Documentation

- Added subscription manager user guide (`docs/guides/subscriptions.md`) covering:
  - Getting started with catch-up and live subscriptions
  - Basic usage patterns and configuration
  - Resilience patterns and error handling
  - Advanced patterns for production deployments
  - Troubleshooting guide
- Added subscription API reference (`docs/api/subscriptions.md`) with complete class and method documentation
- Added subscription migration guide (`docs/guides/subscription-migration.md`) for migrating from manual projection processing
- Added subscription examples (`examples/subscriptions/`) with:
  - Basic projection example
  - Multi-subscriber example
  - Resilient projection with error handling
- **Updated all projection examples to use SubscriptionManager pattern**:
  - `examples/projection_example.py` now demonstrates SubscriptionManager with catch-up, live subscriptions, and checkpoint tracking
  - `docs/getting-started.md` updated with SubscriptionManager as the recommended approach
  - `docs/examples/projections.md` now recommends SubscriptionManager over direct `event_bus.subscribe_all()`
  - `docs/examples/sqlite-usage.md` integration tests updated to use SubscriptionManager
- Added comparison table showing benefits of SubscriptionManager vs direct EventBus subscription
- Added Kubernetes deployment guide (`docs/guides/kubernetes-deployment.md`) covering:
  - Pod lifecycle integration and graceful shutdown
  - Health probe configuration (liveness, readiness, startup)
  - `terminationGracePeriodSeconds` configuration with shutdown deadline
  - Example Deployment, Service, and PodDisruptionBudget manifests
  - Spot instance and preemptible VM considerations (AWS, GCP, Azure)
  - Shutdown metrics and observability
  - Troubleshooting guide for common Kubernetes issues

### Tests

- Added comprehensive ReadModel persistence test suite:
  - Unit tests for all ReadModel components (`tests/unit/readmodels/`)
    - Base class and field validation tests
    - Query and Filter class tests
    - Repository protocol compliance tests
    - In-memory repository tests
    - PostgreSQL and SQLite repository tests
    - Schema generation tests
    - Projection integration tests
    - Handler registry integration tests
  - Integration tests (`tests/integration/readmodels/`)
    - Repository CRUD operations across all backends
    - Projection event handling flows
    - Enhanced features (soft delete, optimistic locking)
- Added comprehensive migration test suite (~950 new tests):
  - Unit tests for all migration components (`tests/unit/migration/`)
  - Integration tests for PostgreSQL locks and migration schema
  - Chaos tests for failure scenarios and recovery
  - Load testing benchmarks for performance validation
  - Phase integration tests for bulk copy, dual write, and cutover
- Added comprehensive subscription manager test suite:
  - Unit tests for all subscription components (`tests/unit/subscriptions/`)
  - Integration tests for catch-up, live, and transition flows (`tests/integration/subscriptions/`)
  - Resilience tests for error handling, retries, and recovery
  - Health check and metrics tests
  - Pause/resume functionality tests
  - Backpressure and flow control tests
  - Drain functionality tests (`test_drain.py`) for shutdown coordination
  - Coordination protocol tests (`test_coordination.py`) for leader election and work redistribution
  - Shutdown tests for pre/post hooks, deadline, metrics, and reason tracking

## [0.2.0] - 2025-12-08

### Added

#### Observability & Telemetry

- **Observability Module** (`eventsource.observability`) - Reusable OpenTelemetry utilities
  - `OTEL_AVAILABLE` constant for checking OpenTelemetry availability
  - `get_tracer()` and `should_trace()` helper functions
  - `@traced` decorator for method-level tracing
  - `TracingMixin` class for consistent tracing across components
- **Kafka Event Bus Metrics** - Comprehensive OpenTelemetry metrics
  - Counters: `messages.published`, `messages.consumed`, `handler.invocations`, `handler.errors`, `messages.dlq`, `connection.errors`, `reconnections`, `rebalances`, `publish.errors`
  - Histograms: `publish.duration`, `consume.duration`, `handler.duration`, `batch.size`
  - Gauges: `connections.active`, `consumer.lag` (per partition)
  - New `KafkaEventBusMetrics` class with `enable_metrics` config option
  - Less than 5% performance overhead
- **SQLiteEventStore Tracing** - `enable_tracing` parameter for `append_events` and `get_events` operations
- **InMemoryEventBus Tracing** - `enable_tracing` parameter for event dispatch and handler execution

#### Aggregate Snapshotting

- `Snapshot` dataclass for point-in-time aggregate state capture
- `SnapshotStore` interface with `InMemorySnapshotStore`, `PostgreSQLSnapshotStore`, and `SQLiteSnapshotStore` implementations
- `AggregateRepository` snapshot support: `snapshot_store`, `snapshot_threshold`, and `snapshot_mode` parameters
- `AggregateRoot.schema_version` for snapshot schema evolution with automatic invalidation
- `create_snapshot()` and `await_pending_snapshots()` methods
- Snapshot exceptions: `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`
- Database migrations for `snapshots` table (PostgreSQL and SQLite)

#### Event Handling & Validation

- `EventVersionError` exception and configurable version validation via `AggregateRoot.validate_versions`
- `UnhandledEventError` exception with configurable handling via `unregistered_event_handling` attribute ("ignore", "warn", "error")
- `FlexibleEventHandler` and `FlexibleEventSubscriber` protocols for sync/async handler signatures
- Consolidated `eventsource.protocols` module as canonical location for protocol definitions

#### Database & Repository

- `DatabaseProjection` class for projections requiring raw database connection access
- `execute_with_connection` helper for consistent connection handling
- Configurable UUID field detection in `PostgreSQLEventStore` via `uuid_fields`, `string_id_fields`, and `auto_detect_uuid` parameters
- `PostgreSQLEventStore.with_strict_uuid_detection()` class method

#### Developer Experience

- Pre-commit hooks with ruff, mypy, and bandit
- GitHub Actions workflow for performance benchmarks with baseline tracking and PR comparison
- Shared test fixtures module (`tests/fixtures/`) with reusable components

### Changed

- Improved type annotations for better mypy compatibility
- Consolidated `@handles` decorator to `eventsource.projections.decorators` (old location deprecated)
- Consolidated protocol definitions to `eventsource.protocols` (old locations deprecated)
- Repository methods `get_pending_events`, `get_failed_events`, `get_failed_event_by_id` now return typed dataclasses (`OutboxEntry`, `DLQEntry`) instead of dicts
- Unified `get_events_by_type()` timestamp parameters to use `datetime` instead of `float`
- Refactored repositories to use `execute_with_connection` helper
- In-memory repositories now use `asyncio.Lock` for proper async concurrency

### Removed

- `SyncEventStore` abstract class (use `asyncio.run()` for sync access; see ADR-0007)

### Fixed

- Broken documentation links in ADRs and guides
- Mypy type errors in projections, repositories, and event bus modules
- `DeclarativeProjection` connection handling for proper transaction sharing

## [0.1.3] - 2025-12-07

### Documentation

- Added documentation badge linking to GitHub Pages
- Updated all documentation URLs to point to https://tyevans.github.io/eventsource-py
- Fixed mkdocs.yml site configuration with correct repository URLs
- Simplified README documentation section with links to hosted docs

## [0.1.2] - 2025-12-07

### Fixed

- Fixed release workflow version validation

## [0.1.1] - 2025-12-07

### Fixed

- Release infrastructure corrections

## [0.1.0] - 2025-12-07

### Added

- Initial release of eventsource-py library
- Event Store with PostgreSQL, SQLite, and In-Memory backends
- Domain Events with Pydantic validation
- Aggregate base class with optimistic concurrency control
- Projection system for building read models with checkpoint tracking
- Dead Letter Queue (DLQ) for failed event handling
- Snapshot support for aggregate state caching
- Multi-tenant support with tenant isolation
- Async-first API design throughout
- Comprehensive type hints and mypy compatibility
- Event registry for type-safe event deserialization
- Event Bus with In-Memory and Redis Streams backends
- Transactional Outbox pattern implementation
- `DatabaseProjection` class for projections requiring raw database connection access
- Pre-commit hooks configuration with ruff, mypy, and bandit

### Infrastructure

- PostgreSQL backend with connection pooling (asyncpg)
- SQLite backend for lightweight deployments, development, and testing
- Redis Streams backend for distributed event bus
- In-Memory backends for testing and development
- Automatic schema creation and migrations
- GitHub Actions CI/CD pipeline

[Unreleased]: https://github.com/tyevans/eventsource-py/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/tyevans/eventsource-py/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/tyevans/eventsource-py/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/tyevans/eventsource-py/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/tyevans/eventsource-py/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/tyevans/eventsource-py/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/tyevans/eventsource-py/compare/v0.8.1...v0.9.0
[0.8.1]: https://github.com/tyevans/eventsource-py/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/tyevans/eventsource-py/compare/v0.5.0...v0.8.0
[0.5.0]: https://github.com/tyevans/eventsource-py/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/tyevans/eventsource-py/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/tyevans/eventsource-py/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/tyevans/eventsource-py/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/tyevans/eventsource-py/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/tyevans/eventsource-py/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/tyevans/eventsource-py/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/tyevans/eventsource-py/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/tyevans/eventsource-py/releases/tag/v0.1.0
