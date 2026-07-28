# Projection Delivery Guarantees -- Milestones

**Date:** 2026-07-27
**Supplements:** [2026-07-27-projection-delivery-guarantees-design.md](2026-07-27-projection-delivery-guarantees-design.md)

Five milestones, sequenced so each one leaves the library in a coherent state
and strictly safer than the one before. The design document is the authority on
*what* is built; this document covers *ordering*, *exit criteria*, and what
remains broken at each boundary.

The honest framing of the sequence: **M2 is where duplicate application stops.**
M1 builds the parts, M3 closes the remaining window and makes the guarantee
structural, M4 stops the amplification loop, M5 proves all of it. If work has to
pause, M2 is the first safe stopping point and M4 is the second.

---

## M1 -- Primitives and honesty

**Goal:** Build the pieces the guarantee needs, change no behavior, and stop the
documentation from lying in the meantime.

**Changes**

- `migrations/schemas/processed_events.sql`; wire into `all.sql` and
  `sqlite_all.sql`; add an `updates/` script for existing schemas.
- `ProcessedEventLedger` with PostgreSQL, SQLite, and in-memory
  implementations: `was_processed`, `mark_processed`, `forget` (for DLQ replay),
  and `prune_below`.
- Optional `conn: AsyncConnection | None = None` kwarg on checkpoint and DLQ
  repository write methods, implemented as
  `execute_with_connection(conn or self.conn, ...)`.
- `DeliveryGuarantee` enum, defined and exported but not yet consulted.
- **Correct the false docstring at `projections/base.py:688-690`** and replace it
  with an accurate statement of current at-least-once behavior.

**Exit criteria**

- Ledger unit tests pass against all three backends.
- Passing `conn=` writes on the supplied connection and participates in that
  connection's transaction; omitting it preserves existing behavior exactly.
- Full existing test suite green -- this milestone must be behaviorally inert.

**Still broken after M1:** everything. Duplicate application, silent loss under
`continue_on_error`, and the amplification loop are all untouched.

**Risk:** low. Additive, with an inert-behavior exit criterion that is easy to
verify.

---

## M2 -- Exactly-once application for `DatabaseProjection`

**Goal:** Duplicate application stops. This is the milestone that discharges
requirement 2 and unblocks the downstream consumer.

**Changes**

- Move the ledger read, ledger insert, and projection checkpoint update inside
  `_execute_in_transaction`, around the handler dispatch.
- Ledger hit short-circuits: handler never runs, transaction commits, event is
  acknowledged.
- `DeliveryGuarantee` becomes live on `DatabaseProjection`, defaulting to
  `EXACTLY_ONCE`.
- Construction-time validation: `EXACTLY_ONCE` with a non-enlistable checkpoint
  repository raises.
- Ledger read moves inside the retry attempt, so a post-crash retry of an
  already-applied event short-circuits.

**Exit criteria**

- Redelivering one event N times produces one set of side effects.
- Two events with identical payloads and distinct `event_id`s both apply. This
  is the downstream invariant; it gets a test before the mechanism it protects
  is considered done.
- Injected failure between handler write and commit leaves no handler writes
  behind.
- `EXACTLY_ONCE` + `InMemoryCheckpointRepository` raises at construction with a
  message naming the actual fix.

**Still broken after M2:** the subscription cursor (`save_position`) still
advances outside the transaction at `EVERY_BATCH` granularity, so mid-batch
crashes still *redeliver* -- but the ledger now absorbs those redeliveries, so
they no longer *duplicate*. The amplification loop persists: terminal failures
still re-raise without advancing, and `continue_on_error` still drops events
silently.

**Risk:** medium. Touches the core delivery path. The `EXACTLY_ONCE` default
means every existing `DatabaseProjection` test exercises the new path, which is
good for coverage and will surface fixture wiring that assumed an in-memory
checkpoint repo.

---

## M3 -- Inverted transaction ownership

**Goal:** The resume cursor commits with the handler's writes. Discharges
requirement 1.

**Changes**

- `TransactionalDelivery` protocol and `DeliveryOutcome` enum.
- `DatabaseProjection` implements `deliver_atomically`, accepting participant
  closures and running them inside the handler transaction.
- `CatchUpRunner` checks for the protocol at `start()`; under `EXACTLY_ONCE`
  with a conforming subscriber it stops calling `_save_checkpoint_with_retry`
  and passes a position-write participant instead.
- `start()` fails for a non-conforming subscriber under `EXACTLY_ONCE`, before
  any delivery.
- Config validation rejects `EXACTLY_ONCE` with `EVERY_BATCH` or `PERIODIC`.
- Runner interprets outcomes: `SKIPPED_DUPLICATE` excluded from throughput
  metrics, surfaced as its own counter.

**Exit criteria**

- Injected crash between handler commit and cursor advance is demonstrably
  impossible -- the seam no longer exists under `EXACTLY_ONCE`.
- Restart after a mid-batch crash resumes at the correct position with no
  reapplication.
- Non-conforming subscriber under `EXACTLY_ONCE` fails at `start()`, not at
  first delivery.
- Throughput measured against M2 on a representative catch-up, so the per-event
  commit cost is a known number rather than an assertion. If the regression is
  worse than expected, that informs the rebuild guidance before it ships rather
  than after.

**Still broken after M3:** the amplification loop and `continue_on_error` loss.

**Risk:** medium-high. Genuine control-flow inversion in the runner, and the
throughput cost lands here. Highest chance of needing design revision mid-flight
of any milestone.

---

## M4 -- Failure handling

**Goal:** No event is lost, and no event is retried forever.

**Changes**

- Poison-pill path: on terminal failure, a fresh transaction writes DLQ record +
  ledger row + cursor advance together, returning `POISONED`.
- `CatchUpRunner` gains a DLQ repository dependency; `continue_on_error=True`
  routes through the poison-pill path instead of dropping.
- Config validation rejects `continue_on_error=True` without a DLQ repository.
- Hard retry-attempt ceiling independent of the backoff policy.
- DLQ replay as a first-class operation: deletes the ledger row and re-delivers
  in one transaction.

**Exit criteria**

- A permanently-failing event produces exactly one DLQ row and one cursor
  advance, and is never redelivered. Run it against the original symptom -- a
  handler that always raises -- and confirm the row count stays at one rather
  than climbing.
- Every delivered event ends in the read model or the DLQ under every
  `continue_on_error` setting. Never neither.
- DLQ replay after a handler fix reapplies the event successfully and leaves no
  stale ledger row.

**Still broken after M4:** nothing in scope. Remaining gaps are the documented
exclusions -- non-transactional subscribers and out-of-transaction side effects.

**Risk:** medium. New dependency on `CatchUpRunner`, and the poison-pill
transaction is a second write path that must stay consistent with the primary
one.

---

## M5 -- Verification and documentation

**Goal:** The guarantees are pinned by tests a future refactor cannot quietly
break, and stated where users will find them.

**Changes**

- `DeliveryGuaranteeConformanceSuite` in `testing/conformance.py`, subclassed
  per backend, covering the six properties in the design document.
- The crash-injection hook, promoted from whatever ad-hoc form M2-M4 used into
  the documented, named production seam.
- ADR `0010-projection-delivery-guarantees.md`.
- Delivery semantics sections in `docs/guides/subscriptions.md` and the
  projection guide, including the exactly-once-application-not-delivery framing
  and the outbox pointer for side effects.
- Ledger retention guidance: what `prune_below` does, what safety margin to use,
  and why it is not automatic.

**Exit criteria**

- Conformance suite passes for PostgreSQL and SQLite.
- The suite fails when the M2 or M3 changes are reverted -- verified by actually
  reverting them, not by inspection. A conformance suite that cannot detect the
  bug it was written for is decoration.
- ADR states what is *not* guaranteed as prominently as what is.

**Risk:** low, with one caveat -- crash-injection tests are the likeliest source
of flakiness. If they prove unstable, the fix is a more deterministic seam, not
a retry decorator on the test.

---

## Sequencing notes

- **M1 -> M2 -> M3 is a hard chain.** M4 depends on `DeliveryOutcome` from M3.
- **M5 is nominally last but should not be deferred wholesale.** Each milestone
  writes its own tests as it goes; M5 consolidates them into the reusable
  cross-backend suite and writes the prose. Treating M5 as "the testing
  milestone" would mean M2-M4 land unverified, which is how the original bug
  survived.
- **The docstring correction in M1 is deliberately out of proportion to its
  size.** It is currently the only place a user is told a guarantee exists, and
  it is wrong. It should not survive the first commit of this work.
