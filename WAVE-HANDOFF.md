# Backpressure wave — handoff

**Branch:** `feat/backpressure-controls` (not pushed, no PR, not merged)
**Date:** 2026-08-09
**Started as:** "research what backpressure controls we should be offering"

This file records what landed, what was decided, and what is deliberately left.
Delete it before merge — it is a working document, not project documentation.

---

## What the research actually found

`SubscriptionConfig.max_in_flight` (default 1000) was **inert**. Both subscription
runners awaited `handle()` inside the flow-control slot in a sequential loop, so the
semaphore never blocked. Reproduced empirically with a real `LiveRunner`: 5 events,
200ms handler, `max_in_flight=1000` → `peak_in_flight == 1`.

Ten parallel audits then found the same shape across the library. The defect class:

> **A capability the library declares, documents, and is supposed to dispatch to —
> and never does.**

The criterion matters, because without it a library's whole public API looks like a
false positive. `EventStore.append` has no internal caller *because users call it* —
not a defect. The defect is when the **library itself** is supposed to detect,
dispatch to, or honor something and doesn't.

Why it survives CI: every instance was tested in isolation. ~60 `SubscriptionMetrics`
tests, zero asserting `runner.metrics` after running a runner — delete every
`record_*` call from both runners and the suite still passes. `test_health.py:391`
literally assigned `stats.peak_in_flight = 50`.

---

## Landed (see `git log main..HEAD`)

**Correctness bugs, real user impact**

| Fix | Was |
|---|---|
| Live drain bounded (`FeedReadOptions.limit`) | Unbounded read; all 3 adapters materialize the full result set before yielding |
| `stop()`/`pause()` interrupt a live drain | `_stop_requested` didn't exist in `live.py`; `stop()` returned having stopped nothing |
| Live feed read scoped to tenant | Dropped `tenant_id`, so tenant-scoped live subscriptions over-fetched every tenant's rows |
| Kafka `ssl_check_hostname` reaches DLQ clients | Security dict built twice, copies diverged by one key; DLQ inspection failed TLS against a cert setup the main bus accepted |
| `EventBusConnectionError` raised at 10 sites | Bare `RuntimeError`; `error-handling.md` told users to catch a type nothing raised |
| RabbitMQ publish concurrency bounded per instance | Semaphore built per chunk, so concurrent callers multiplied the ceiling |
| `release_migration_metrics()` on the live path | Wired to `_complete_migration`, dead since `798ae2e`; gauge never released on success |
| `replay()` batched | Unbounded, and its `max_events` guard sat *downstream* of the materialization |

**Inert machinery removed** — `max_in_flight`/`backpressure_threshold` and the
unreachable `FlowController` internals (909 deletions), `max_reconnect_delay`,
`dual_write_timeout_minutes`, `ProjectionCoordinator`'s fictional polling API and
`start()` that never existed, `RoutingError`, `_complete_migration`.

**Capabilities wired** — batch delivery on catch-up (`handle_batch` now actually
dispatched), migration metrics (all 6 recorders), migration audit log (optional,
8 lifecycle points), the 8 `ATTR_MIGRATION_*` attributes, snapshot-miss counter
keyed by reason.

**Bounds added** — projection fan-out cap, Kafka consumer fetch bounds
(`fetch_max_bytes`, `max_partition_fetch_bytes`).

**Naming/consistency** — `batch_size` split into `producer_max_batch_bytes` /
`stream_read_count` / `publish_chunk_size` (it meant three incompatible things
across sibling adapters, ~160x apart, failing silently); `SQLITE_AVAILABLE`
collapsed into `AIOSQLITE_AVAILABLE`; redundant `*_AVAILABLE` copies removed.

**Docs** — 22 false/misleading backpressure claims corrected; conformance rule in
`definition-of-done.md` now names suites that exist (it required
`EventStoreConformanceSuite`, which never existed — store conformance was
*decomposed* into 14 per-port suites and the rule's pointer was never updated);
exception docstrings now name their audience.

**ADR** — `docs/adrs/DRAFT-ordered-subscription-delivery.md`. **Provisional filename,
no number allocated** (highest on main is 0058; number-at-drafting has collided
three times here). Records ordered-per-subscription delivery as deliberate, and the
sanctioned ways to scale past it.

---

## Decisions made (with reasoning, so they aren't relitigated)

**Delivery stays ordered per subscription.** The checkpoint is a scalar
`last_processed_position` advanced inside the delivery step. Concurrent per-event
delivery would let a later event checkpoint past an earlier one still in flight →
silent data loss in a projection on restart. ADR 0047 rests on this. Lifting it
needs low-water-mark completion tracking, amplifies redelivery, **resurrects the
duplicate suppression 0047 deleted**, and reintroduces head-of-line blocking anyway.

**Batch delivery is the sanctioned way to scale** — a batch completes as a unit
*before* the position advances, preserving exactly the invariant per-event
concurrency would break. Partitioned lanes (hash `aggregate_id`, each lane
sequential, lanes concurrent) are the future option; per-aggregate order is the only
order a projection needs.

**Two circuit breakers, not one.** One breaker guarding both handler calls and
infra calls means handler failures block checkpointing (reproduced, ~15% flake).
One breaker per dependency. No new config knob — both from the existing
`CircuitBreakerConfig`.

**DLQ'd events feed the handler breaker uniformly.** `reset-on-success` already means
an isolated bad event never opens it while a consecutive run does — no special case
needed. (This corrected the orchestrator's initial position.)

**Snapshot exceptions stay.** ADR 0017 has a section titled *"Why
`SnapshotSchemaVersionError` exists but is not raised on the load path"* — they are
published vocabulary for third-party `SnapshotStore` implementors. Not dead code.
Strict mode rejected: it forks the hottest path and duplicates an extension point
that already exists.

**Telemetry attribute collision resolved additively.** `cutover.py` had its own
`ATTR_MIGRATION_ID` with a *different* wire string, so dashboards filtering the
canonical ID went blind exactly at cutover. Emit both rather than break the schema —
`architecture.md` designates these strings stable public surface.

**Breaking changes taken freely** — pre-1.0, single user, no deprecation cycles.

---

## Deferred, with reasons

| # | Item | Why deferred |
|---|---|---|
| 34 | Batch delivery on the **live** path | Batching live would defer the `stop`/`pause` checks added this wave to per-batch granularity, partially reverting that fix. Needs a lifecycle decision, not implementation. |
| 25 | `stop()` can't interrupt a drain blocked on `pause` | Pre-existing in **both** runners. `stop()` is unreliable exactly when a subscription is paused — a state operators choose deliberately. |
| 13 | `processing_timeout` never applied | Zero reads; both handler calls are bare awaits. A hanging handler blocks a subscription **forever**. Enforce or delete. |
| 19 | `error_rate_per_minute` has no write site | Read at 4 sites gating health DEGRADED → a high error rate reports healthy. |
| 28 | `SyncSubscriber` never dispatched | Awaiting a sync handler's `None` raises `TypeError` *inside the generic except* → every event fails forever, blamed on user code. Decided: wire it via `handler_adapter.py`'s existing mechanism. |
| 27 | `LeaderElectorWithLease` | Decided: keep and make real (renewal loop + probe + conformance). Nothing calls `renew()`, so a correct implementation silently loses leadership. |
| 3 | Bound background publish tasks | Needs a `contextvars` re-entrancy guard or handlers that publish deadlock at cap. Shares `BackgroundTaskManager` with snapshot scheduling. |
| 17 | `shutdown_timeout` duplicate; `RetryPolicy` name clash | Two declaration sites (manager wins); `from eventsource import RetryPolicy` gives the bus dataclass, not the projections Protocol that `adapters/sql/projection.py:92` consumes. |
| 24 | 12 remaining never-set telemetry attributes | 4 `ATTR_LOCK_*` need tracing introduced to lock adapters (real work); `ATTR_EVENTS_SKIPPED` is probably a delete. Deleting is a public-surface decision. |
| 20 | Migration repo conformance (33 methods) | All 4 Protocols have exactly **one** implementation; conformance exists to stop 2+ diverging. Low priority — **reprioritize the moment a second migration backend is proposed.** |
| 38 | `BatchSubscriber`-only subscriber breaks on the live path | **Real bug, found in final review.** The Protocol requires only `subscribed_to()` + `handle_batch()`, not `handle()`. Catch-up now dispatches `handle_batch()`; live still calls `handle()` unconditionally → `AttributeError` **per event** once it transitions, recorded as a handler failure and potentially filling the DLQ. Docs say "will not receive live events", which reads like a silent no-op. Resolves naturally with #34. |

---

## Watch items (not defects today)

- `release_migration_metrics()` is pinned by a test asserting the call *happens*,
  not *where in the call graph*. If `_complete_cutover` is refactored, the same
  "cleanup wired to a method with no real caller" shape could reappear silently —
  which is exactly how that bug arose. Speculative, not acted on.
- `PostgreSQL` binds the snapshot-deserialization contract via a hand-written
  regression test rather than the shared conformance mixin (JSONB structurally
  prevents the suite's raw-write injection). Documented in that test's docstring.
  A third backend with a TEXT-like column should bind the mixin, not copy the
  hand-written test.
- Four `rabbitmq/dlq.py` `require_channel()` sites look like the ones #21 fixed but
  are guarded behind graceful early-returns — dead paths, not untyped raises.
  Correctly untouched; noted so the next audit doesn't re-flag them.

## What a reviewer should look at hardest

In order, from the final cross-lane review:

1. **Catch-up batch dispatch and its per-event fallback** — largest behavior change,
   touches the checkpoint invariant, and the one place a subtle bug would silently
   double-apply events.
2. `_drain_feed` — two independent changes to one loop (bounding, and stop/pause).
3. The three-adapter `batch_size` rename — one field forwards to a
   differently-named driver argument.
4. The snapshot counter's fifth reason living in a different file *on purpose*
   (`repository.py`, not `snapshotting.py`) — without it the reason is unreachable
   with any shipped store.
5. `docs/guides/subscriptions.md:294` — correct today, and the first thing to go
   stale when #34 batches the live path.

## Before this merges

1. **Full unit suite** (#31) — ruff, mypy (193 files), and `import eventsource`
   (141 exports) are **clean at final HEAD**. The full unit run was still in
   progress when this was written; integration subscriptions pass (34, no Docker
   needed — in-memory fixtures). An earlier full run hit 6226 passed / 1 failed,
   but that failure was **stale** (it exercised `_complete_migration`, deleted
   mid-run); migration units are 837 passing now. Do not record that run as a pass —
   it predates six commits.
2. **Allocate the ADR number at merge**, after re-checking `docs/adrs/` on current
   `main`. Rename the `DRAFT-` file, fix its four referrers (one in shipped source),
   add reciprocal "Amended by" pointers to 0047 and 0054, and add it to `index.md`
   **and the mkdocs nav** — a strict build won't catch the nav omission.
3. **CHANGELOG** — was 2 entries for 27 commits; rewritten this wave. Verify a reader
   learns about the TLS drop and the tenant over-fetch.
4. Delete this file.

---

## Process lessons

**Every PR blocker found by the end-of-wave review was cross-lane.** Each lane
verified its own subset and was *right* about it; nothing checked the union. CI went
red because a lane deleted a public symbol, swept its own directory thoroughly, and
never looked at `tests/integration/`. **After deleting any public symbol, grep the
whole tree.** A lane isn't done until its CHANGELOG line exists.

**`git add <paths>` does not protect a commit** — plain `git commit` commits the whole
index, including another agent's staged files. Use `git commit -- <paths>`, then
**always** `git show --stat HEAD`. A pre-commit hook that *modifies* files aborts the
commit, after which `git log` shows another lane's HEAD and reads exactly like
success. A test run racing another lane's commit can report stale counts.

**Audit claims invert on verification, often.** `LeaderElector` "has no conformance
suite" (it does); "31 uncovered methods" (33); "cross-tenant leak" (over-fetching,
not disclosure); "delete the audit repo" (wire it — it shipped with the original
feature and simply lost nothing, it never had a caller); ADR 0007 "amended" (stands).
Every one was caught by an agent checking a claim instead of acting on it.
