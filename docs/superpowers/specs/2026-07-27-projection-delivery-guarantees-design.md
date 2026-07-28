# Projection Delivery Guarantees

**Date:** 2026-07-27
**Status:** Approved, pending implementation plan

## Problem

The library delivers events to projections at-least-once, persists the resume
cursor outside the handler's transaction, and provides no idempotency
mechanism. Consumers are therefore forced to deduplicate inside handlers --
the one place deduplication cannot be done correctly, because a handler cannot
distinguish a redelivery of one event from two distinct events that happen to
carry identical payloads.

`DatabaseProjection` documents a guarantee it does not provide.
`projections/base.py:688-690` claims "Checkpoint updates share the same
transaction (when using compatible repos)." No such path exists. The actual
sequence in `_handle_with_retry` (`base.py:311-315`) is:

```python
await self._process_event(event)               # opens a session, commits, returns
await self._checkpoint_manager.update(event)   # separate connection, after commit
```

### Two checkpoints, only one of which matters

Investigation surfaced a distinction that is easy to miss and that invalidates
any fix targeting only the obvious checkpoint:

| API | Key | Stores | Read by |
| --- | --- | --- | --- |
| `update_checkpoint` / `get_checkpoint` | `projection_name` | `last_event_id` | `base.py:410` (reporting), lag metrics |
| `save_position` / `get_position` | `subscription_id` | integer global position | `transition.py:543` -- **resume cursor** |

Nothing resumes from the projection checkpoint. Replay position is driven
entirely by `get_position`, written by `CatchUpRunner._save_checkpoint_with_retry`
outside any handler transaction, at `EVERY_BATCH` granularity by default
(`config.py:98`). A crash mid-batch replays the entire delivered prefix of that
batch.

### Failure modes in the current implementation

1. **Duplicate application.** Handler commits, process dies before the cursor
   advances, event is redelivered and applied again.
2. **Silent event loss.** `catchup.py:_deliver_event` with
   `continue_on_error=True` logs a warning and advances past the failed event.
   No DLQ record, no retry. Delivery is at-most-once for precisely the events
   that failed.
3. **Amplification loop.** `CheckpointTrackingProjection` sends terminal
   failures to the DLQ and then re-raises (`base.py:360-372`), so the cursor
   never advances. The event is redelivered indefinitely, accumulating DLQ rows
   and re-applying any partial writes committed before the failure.

## Requirements

From the downstream consumer, adopted as binding:

1. The resume cursor advances in the same transaction as the handler's writes.
2. Idempotency comes from the framework, never from the handler. Handlers
   remain faithful folds -- no dedupe, no `WHERE NOT EXISTS`, no filtering.
   Two identical events are two facts.

## Design

### Delivery modes

```python
class DeliveryGuarantee(Enum):
    EXACTLY_ONCE = "exactly_once"    # default for DatabaseProjection
    AT_LEAST_ONCE = "at_least_once"
```

`EXACTLY_ONCE` means **exactly-once application, not exactly-once delivery**.
The bus may deliver an event any number of times; the framework absorbs the
duplicates. Effects a handler produces outside its connection -- HTTP calls,
cache writes, published messages -- remain at-least-once and always will be.

There are no existing users, so `EXACTLY_ONCE` is the default and misconfiguration
is a hard error rather than a silent downgrade.

### The processed-events ledger

A framework-owned table, written inside the handler's transaction, keyed on
event identity:

```sql
CREATE TABLE projection_processed_events (
    projection_name VARCHAR(255) NOT NULL,
    event_id        UUID NOT NULL,
    global_position BIGINT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (projection_name, event_id)
);

-- Serves the prune query; the composite PK cannot.
CREATE INDEX idx_processed_events_prune
    ON projection_processed_events (projection_name, global_position);
```

The primary key serves as both the lookup index and the unique constraint that
makes concurrent double-processing impossible rather than merely unlikely. The
`global_position` column exists solely so `DELETE ... WHERE global_position < X`
has an index to use -- the composite PK is useless for that query.

Keying on `event_id` rather than content is what satisfies requirement 2. Two
`causal_edge.declared` events with byte-identical payloads carry distinct
`event_id`s, so both pass the ledger check and both apply. The framework
collapses only redeliveries of a single fact.

**Claim-before-dispatch, via `INSERT ... ON CONFLICT DO NOTHING RETURNING`.**
The ledger row is written *first*, at the top of the transaction; the handler is
dispatched only if the insert returned a row. On rollback the ledger row is
discarded along with the handler's writes, so correctness is preserved.

This is preferred over `SELECT`-then-dispatch for three reasons: one round trip
instead of two; no dead tuples on the duplicate path (a failed unique insert
burns an XID and bloats table and indexes, whereas `ON CONFLICT` does not); and
it is atomic against a concurrent second processor, which `SELECT`-first is not.
That race is reachable in this project, which supports multi-instance
subscription coordination (ADR 0009).

Note the resulting blocking behavior: a concurrent duplicate insert waits on the
first transaction's index lock rather than fast-skipping. That is correct
serialization, not a defect, but it means duplicate delivery under
multi-instance dispatch costs a wait rather than an immediate return.

The ledger applies in both modes. Under `AT_LEAST_ONCE` it cannot be written
atomically, so a crash between handler commit and ledger insert still permits
one duplicate application; it narrows the window from a batch to a single
event without closing it. This must be labeled as such in the docs.

**Retention.** The ledger grows one row per event per projection. Rows below
`checkpoint_position - safety_margin` cannot be redelivered by a catch-up
starting at the checkpoint and are safe to delete. Pruning is exposed as an
explicit method, not a background task -- libraries that spawn their own timers
are difficult to reason about in tests and inside someone else's event loop.

### Transaction ownership: inverted

The runner is generic over subscribers and owns no database session. Rather
than the runner writing the cursor after delivery, it hands the cursor write to
the subscriber as a participant in the subscriber's transaction.

```python
@runtime_checkable
class TransactionalDelivery(Protocol):
    """A subscriber that can apply an event and arbitrary co-writes atomically."""

    async def deliver_atomically(
        self,
        event: DomainEvent,
        *,
        participants: Sequence[Callable[[AsyncConnection], Awaitable[None]]],
    ) -> DeliveryOutcome: ...


class DeliveryOutcome(Enum):
    APPLIED = "applied"            # handler ran, everything committed
    SKIPPED_DUPLICATE = "skipped"  # ledger hit, handler never ran
    POISONED = "poisoned"          # terminal failure, DLQ'd and cursor advanced
```

`CatchUpRunner` checks for the protocol at `start()`. Under `EXACTLY_ONCE` with
a conforming subscriber it stops calling `_save_checkpoint_with_retry` entirely
and passes a participant closure that writes the subscription position. One
transaction then contains, in order:

1. Ledger claim (`INSERT ... ON CONFLICT DO NOTHING RETURNING`) -- if no row
   returned, the event is already applied: skip the handler, commit, return
   `SKIPPED_DUPLICATE`
2. Handler execution
3. Projection checkpoint (`update_checkpoint`)
4. Subscription position (`save_position`) -- supplied by the runner

Commit is the single atomic moment. Crash before it and everything rolls back
together. Crash after it and redelivery short-circuits at step 1.

`DeliveryOutcome` is load-bearing for observability: without it the runner
cannot distinguish applied work from skipped redeliveries, and would report
replays as fresh throughput. `SKIPPED_DUPLICATE` also provides the signal for
tuning batch size.

### Repository enlistment

Because the framework always passes `conn=self._current_connection` explicitly,
writes land in the projection's database **by construction**. Co-location is
structural, not assumed or validated.

Checkpoint and DLQ repositories gain an optional override:

```python
async def save_position(
    self, subscription_id: str, position: int,
    event_id: UUID, event_type: str,
    *, conn: AsyncConnection | None = None,
) -> None:
```

Implemented as `execute_with_connection(conn or self.conn, ...)`. The existing
PostgreSQL and SQLite repositories work unchanged -- `_connection.py:60` already
yields a passed-in connection directly and leaves transaction management to the
caller. `InMemoryCheckpointRepository` does not accept the kwarg, which is what
makes it non-enlistable and what the `EXACTLY_ONCE` construction check keys on.

The remaining requirement is a deployment fact, documented: the
`projection_checkpoints`, `subscription_checkpoints`, and
`projection_processed_events` tables must live in the same database as the read
models.

### Validation and failure to start

- A `DatabaseProjection` constructed in `EXACTLY_ONCE` mode with a
  non-enlistable checkpoint repository raises at construction.
- A subscription in `EXACTLY_ONCE` mode whose subscriber does not implement
  `TransactionalDelivery` fails at `start()`, before any event is delivered.
- `EXACTLY_ONCE` combined with `EVERY_BATCH` or `PERIODIC` is rejected by config
  validation. Batch checkpointing is incoherent once the cursor is bound to a
  per-event transaction.
- `continue_on_error=True` without a DLQ repository is rejected. "Continue past
  failures with nowhere to put them" is not a valid configuration.

Warn-and-degrade occurs only when the user has explicitly selected
`AT_LEAST_ONCE`.

### Throughput cost

`EXACTLY_ONCE` forces one commit per event instead of one per batch. This is a
material reduction for catch-up over a large backlog. `AT_LEAST_ONCE` retains
batch checkpointing and the ledger, remaining the fast path for rebuilds.

The recommended escape hatch for large rebuilds is to rebuild under
`AT_LEAST_ONCE` into a fresh table and swap -- safe because a rebuild from
position zero has no duplicates to absorb.

### Failure handling

**Terminal failures become poison-pill records.** When the retry policy gives
up, a fresh transaction (the handler's is rolled back and unusable) writes
three things together and commits:

1. The DLQ record
2. The ledger row
3. The cursor advance

Returns `POISONED`. Writing the ledger row for a *failed* event is what stops
the amplification loop -- the event is marked terminally accounted-for, so even
a rewound cursor short-circuits rather than re-failing. The event is quarantined
exactly once and forward progress is guaranteed regardless of handler behavior.

**`continue_on_error` no longer means "drop."** It routes through the identical
poison-pill path and thereafter controls only whether a terminal failure stops
the subscription. No configuration causes an event to disappear without a
durable record. This requires adding a DLQ repository as a `CatchUpRunner`
dependency, which it currently lacks entirely.

**Retry changes.** Each attempt continues to get a fresh transaction
(`base.py:790`, correct as-is). The ledger read moves inside the attempt, so an
event that succeeded in a prior process short-circuits on retry after a crash.
A hard attempt ceiling independent of the backoff policy prevents a
misconfigured unbounded policy from reproducing the original symptom.

**DLQ replay is a first-class operation.** Replay deletes the ledger row and
re-delivers in one transaction, so "fix the handler, replay the DLQ" works
without hand-written DELETEs against `projection_processed_events`. This is the
supported recovery path; rewinding the cursor is not, because poisoned events
carry ledger rows that suppress redelivery.

**Out-of-transaction effects.** Handler side effects outside the connection are
not rolled back by the transaction and remain at-least-once. Guidance is the
outbox pattern, using the existing `repositories/outbox.py`.

## Verification

A `DeliveryGuaranteeConformanceSuite` in `testing/conformance.py`, alongside the
existing store and bus suites, subclassed per backend:

- No duplicate application under `EXACTLY_ONCE`: deliver one event N times,
  assert one set of side effects and one ledger row.
- Distinct events with identical payloads both apply: same content, different
  `event_id`, assert two rows. Pins the downstream invariant in the library's
  own suite.
- Crash between handler commit and cursor advance is impossible: inject failure
  at the seam, assert handler writes absent after rollback. This is the case
  that would have caught the original bug.
- Crash after commit replays cleanly: assert short-circuit, singular effects.
- Terminal failure advances exactly once: one DLQ row, one ledger row, cursor
  moved, no loop.
- No event lost under any `continue_on_error` setting: every delivered event
  lands in the read model or the DLQ, never neither.

Crash-injection requires a seam. An internal hook on the delivery path -- a
no-op in production, a named injection point for tests -- ships in the library
rather than being monkeypatched, so the tests exercise the real code path. This
is the one piece of test infrastructure that lives in production code.

## Documentation

- **ADR `0010-projection-delivery-guarantees.md`** (`docs/adrs/` numbering has
  drifted -- eight files share `0007` and two share `0009`; `0010` is the next
  free number, and renumbering is out of scope here). States guarantees per
  configuration and, explicitly, what is not guaranteed: exactly-once
  application within the transaction, at-least-once outside it, outbox for side
  effects.
- Correct the false docstring at `base.py:688-690`.
- `docs/guides/subscriptions.md` and the projection guide currently say nothing
  about delivery semantics.

## Migration

New `migrations/schemas/processed_events.sql`, added to `all.sql` and
`sqlite_all.sql`, plus an `updates/` script for existing schemas. Append-only,
per project convention.

## Revisions after research (2026-07-28)

Two research passes -- one on inbox/idempotent-consumer prior art, one on
SQLAlchemy async transaction enlistment -- produced the following changes. Items
1-3 are amended inline above; 4-7 are additions.

1. **Claim-before-dispatch replaces read-before-dispatch.** See the ledger
   section. The original rationale ("relying on constraint violation requires
   distinguishing our constraint from a handler's") was answered by reordering
   rather than by choosing between the two.
2. **`global_position` column and prune index added.** The composite PK does not
   serve `DELETE ... WHERE global_position < X`.
3. **SQLite ledger table declared `WITHOUT ROWID`** -- roughly half the space and
   near-2x on composite-key point lookups. Requires all PK columns `NOT NULL`.

4. **Connection threading must be fixed before M2.** `DatabaseProjection` stores
   the live connection as instance state (`base.py:761`, set at `892`, cleared in
   `finally` at `922`, read at `961`). Under concurrent `handle()` calls on one
   instance this silently leaks connections across transactions: event A awaits,
   event B overwrites the attribute, A resumes and writes into B's transaction,
   committing or rolling back with it. No error is raised. Building the ledger
   and checkpoint writes on that reference would make the exactly-once guarantee
   silently false under concurrency. Fix: thread `conn` explicitly through
   `_process_event(event, conn)` and delete the attribute.

5. **SQLAlchemy SQLite engines need explicit transaction control.** sqlite3's
   legacy default does not emit `BEGIN` for `SELECT`, so reads are not repeatable
   within a transaction. No `connect_args`, `isolation_level`, or `do_begin` hook
   exists anywhere in `src/` or `tests/` today. Required: a shared engine-factory
   helper applying `connect_args={"autocommit": False}` (Python 3.12+) or the
   portable `isolation_level = None` + `exec_driver_sql("BEGIN")` event-hook pair,
   used by both library and test fixtures. Without it, SQLite conformance tests
   would pass against transaction boundaries that do not exist.

   Related: SAVEPOINT is documented as silently incorrect on SQLite without this
   configuration, which independently confirms the design's choice of a fresh
   transaction (not `begin_nested()`) for the poison-pill write.

6. **When `conn` is supplied, bypass `execute_with_connection`.** Its
   `transactional` flag is documented as ignored for caller-supplied connections
   (`_connection.py:62-65`); an explicit branch makes the ownership contract
   legible. An `in_transaction()` guard was considered and rejected -- autobegin
   means it can essentially never fail, so it would be a check that cannot catch
   the mistake it targets.

7. **Why the ledger exists, per mode.** Research found that mature event-sourcing
   systems (Marten, Axon, Kafka Connect sinks) generally do *not* keep per-event
   ledgers: a monotonic cursor committed in the same transaction as the read-model
   write already yields exactly-once application for strictly ordered dispatch.
   That is correct, and after M3 the ledger is redundant for single-instance
   ordered catch-up. It remains load-bearing for:

   - the live (unordered) delivery path, where no single resumable position exists
   - multi-instance dispatch, where two processors can race the same event
   - `AT_LEAST_ONCE`, where the cursor advance is not transactional
   - M2 shipping ahead of M3, which is the milestone plan's first safe stop

   The docs must state this rather than implying a per-event ledger is
   universally required. Retention guidance takes the ClickHouse
   `replicated_deduplication_window` case as its cautionary example: a dedupe
   window that ages out mid-retry silently stops deduplicating.

## Explicitly out of scope

- Exactly-once for subscribers without a joinable transaction (Elasticsearch,
  Redis, external HTTP). These run `AT_LEAST_ONCE` with the ledger's narrowed
  window and are documented as such.
- Distributed transactions across heterogeneous stores.
- Changes to `EventBus` delivery semantics, covered by the existing bus ADR.
