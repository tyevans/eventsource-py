# 0048. Failure Paths Report Honestly and Retain What They Cannot Handle

A sweep across the adapters and application rings found the same shape in a
dozen places: a failure path that *looks* handled. A fallback that is the only
path. A header written and never read. A config value computed and discarded. An
exception type defined, exported, and never raised. Each one passes every test,
because the test asserts the code did what the code does.

This ADR records the two rules the fixes share, so future adapters inherit them
rather than re-deriving them.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0001](0001-async-first-design.md) | Amended — the `SyncEventStoreAdapter` running-loop path and its shared `ThreadPoolExecutor` are removed. ADR 0001's async-first decision, the `asyncio.run(asyncio.wait_for(...))` no-loop path, and the per-call timeout are unchanged; only the "detect a running loop and schedule onto it" branch is superseded, because it could not work. |
| [0007](0007-event-bus-delivery-semantics.md) | Stands — at-least-once with idempotent handlers is exactly what "leave the offset uncommitted rather than lose the event" preserves. This ADR resolves *how* the Kafka adapter honors it on its failure paths, not what it promises. |
| [0035](0035-lazy-front-door.md) | Reaffirmed — the front door was executing `import aiosqlite` to compute `__all__`, defeating its own laziness. It now asks `importlib.util.find_spec` instead. |
| [0041](0041-infrastructure-exceptions-to-ports.md) | Amended — `EventStoreConnectionError` moves from `SubscriptionError` to `EventStoreError`. Filing a store-connection failure under the subscription taxonomy meant `except SubscriptionError` was the only way to catch it, including for callers with no subscription. |
| [0046](0046-aggregate-type-single-source.md) | Extended — the third declaration site ADR 0046 left out of scope (a divergent `aggregate_type` default on an event class) now raises `AggregateTypeMismatchError` instead of being silently restamped. |

## Context

Three parallel read-only reviews of `adapters/`, `application/`, and the public
API produced 28 findings. Three Criticals were fixed on the day (ADRs 0046,
0047, plus a memory-adapter checkpoint fix). The remainder, recorded in
`BACKLOG.md`, turned out not to be twenty-odd unrelated bugs but two recurring
mistakes.

### Silent degradation

The defining property is that nothing observable changes when the code is
wrong.

- `SyncEventStoreAdapter` detected a running event loop, logged a warning about
  "additional overhead", handed the coroutine to that same loop, and then
  blocked the loop's only thread waiting for it. The loop could never run what
  it had just been given. The call hung until the timeout, and the timeout
  message named the situation as if it were a slow operation.
- The Kafka consumer computed a retry delay from the shared `RetryPolicy`,
  wrote it into a `retry_after` header, and never read that header back. The
  same config that made the RabbitMQ consumer back off made the Kafka consumer
  retry immediately, forever.
- `EventStoreConnectionError` existed, was exported from two modules, and was
  raised by nothing. A misconfigured SQLite store surfaced
  `sqlite3.OperationalError: unable to open database file` — no library name,
  no adapter name, no path.
- SQLite's `read_category(from_timestamp=...)` compared ISO strings lexically
  against `+00:00` rows. A bound at any other offset sorted after everything
  and returned an empty result — not an error, just nothing.
- An event class declaring `aggregate_type = "Shipment"`, emitted from an
  `Order` aggregate, was restamped to `"Order"`. Since `aggregate_type` is the
  stream category, the disagreement is invisible in a save/load round-trip.

The tell, where there is one, is a metric pinned at a constant. But most of
these had no metric at all, which is the point: **a fallback branch that is the
only reachable branch is indistinguishable from correct code until you ask what
sets the value it falls back from.**

### Losing what you cannot handle

The Kafka consumer committed offsets on paths that had not retained the event:

- `_republish_for_retry` returned early when the producer was absent, logging an
  error; the caller committed regardless. Neither retried nor retained.
- The max-retries path sent to the DLQ and committed unconditionally, including
  when the DLQ send itself did nothing.

Committing an offset is a claim that the event is handled. Making that claim
falsely is worse than redelivering.

The projection retry loop had the mirror-image problem. `record_checkpoint()`
sat inside the `try` the retry loop wraps around `_process_event()`, so a
checkpoint-store outage was indistinguishable from a poison event: the loop
retried, re-applying the read-model mutation once per attempt, then wrote a
successfully-projected event to the DLQ, where an operator replaying it would
apply it again.

## Decision

**1. A failure path that cannot do its job says so, loudly, at the point of
failure.**

Where no correct behavior exists, refuse rather than degrade. `_run_sync`
raises `RuntimeError` naming the deadlock and the two ways out
(`await` the async store, or `asyncio.to_thread`) rather than scheduling work
that cannot run. Where a driver error would otherwise escape untranslated, the
adapter wraps it in a library type naming the adapter, with the original as
`__cause__`.

Corollary: an exception type the library defines and exports must be raised by
the library. An unraised error type is documentation that lies.

**2. Never claim work is done that is not.**

`_send_to_dlq` and `_republish_for_retry` return whether the event was
retained. The consumer commits only on `True`. When neither the retry topic nor
the DLQ accepted a message, the offset is left uncommitted and a `CRITICAL` log
records why — noisy redelivery, never silent loss. The one `True` that does not
mean "stored" is an explicitly disabled DLQ, which is the operator choosing to
drop poison messages.

Symmetrically, work that has already succeeded is never retried. Checkpointing
moved out of the projection retry loop onto the success path: a checkpoint
failure re-raises so a runner sees a stalled projection, but it never re-runs
the handler and never reaches the DLQ.

**3. Two backends configured the same way behave the same way.**

The Kafka consumer now waits out `retry_after` before processing a republished
message, applying the same `RetryPolicy` the RabbitMQ consumer applies. Both
block their partition/consumer while waiting — that is a known limitation,
recorded in `BACKLOG.md`, and a genuinely non-blocking delay needs a dedicated
retry topic. What is not acceptable is one backend silently ignoring the
policy the other honors.

Cross-backend divergence that remains gets documented at the point of choice
rather than left for a production surprise: the transactional outbox is a
PostgreSQL capability and the event-bus guide now says so, because a service
developed against SQLite and deployed against PostgreSQL otherwise gets
different delivery guarantees from identical code.

## Consequences

- `SyncEventStoreAdapter` raises `RuntimeError` where it previously hung. Code
  that called it from async and lived with the timeout now fails immediately,
  which is the intended outcome. `shutdown_executor()`, `_get_executor()`, and
  the class-level `_executor` are gone; nothing used them. `close()` is new,
  and the adapter is a context manager, so sync callers can release a store
  that owns a connection.
- `OptimisticLockError.expected_version` is `int | str`. The non-numeric
  `ExpectedVersion` kinds render by name; `no_stream` no longer reports as the
  integer `0`, a version the caller never wrote.
- `EventStoreConnectionError` no longer subclasses `SubscriptionError`. Handlers
  catching it that way must catch `EventStoreError` (or the type itself).
- An event class declaring an `aggregate_type` different from its aggregate's
  now raises `AggregateTypeMismatchError` at emit time. A declaration that
  *matches* stays legal, though it remains redundant.
- `StreamId` validates argument types, so the common transposition
  (`StreamId("Order", uuid)`) reports which argument is wrong instead of
  surfacing a `re` `TypeError` naming neither.
- The live runner's two pause/transition buffers are counters, not
  `asyncio.Queue`s of interchangeable `None` sentinels. They held no
  information and grew without bound during a long pause.
- `import eventsource` no longer executes `aiosqlite`: 149 modules instead of
  177.

## Alternatives Rejected

**Make the sync adapter work from a running loop by spinning a second loop.**
The wrapped store binds connection-pool state to a loop; driving it from a
second one is a different bug with a longer fuse. There is no correct behavior
here, so there should be no behavior.

**Give Kafka a truly non-blocking retry now.** It needs a separate retry topic
and a scheduler, which is a feature, not a bug fix. Converging the two backends
on the same (blocking) semantics removes the divergence today and leaves the
improvement clearly scoped.

**Implement a transactional outbox for SQLite.** Worth doing, but it is a
schema and transaction-boundary change, not part of a defect sweep. Documented
as a gap instead, at the point where the choice is made.

**Leave `EventStoreConnectionError` where it was for compatibility.** Pre-1.0,
the project takes clean breaks over shims. The parentage was simply wrong, and
a wrong hierarchy costs more the longer it is inherited.
