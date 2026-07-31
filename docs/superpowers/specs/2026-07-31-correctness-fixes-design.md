# Correctness Fixes Slice — Design

**Status:** Design complete, ready for planning
**Source of scope:** `BACKLOG.md` wrongness-class entries accumulated during the
clean-architecture rings campaign (PR #88). Exactly ten items; every other
backlog entry — including all ring-residue refactors and the "Re-benchmark pg
catch-up horizon predicate at scale" entry — is out of scope. §7 states
explicitly how this slice changes that re-bench entry's premise.

Each section below is self-contained: the defect with citations, the ruling
with rationale and rejected alternatives, the fix precisely enough specified
for red/green TDD tasks, the failing test that must exist first, acceptance
criteria, and docs/CHANGELOG obligations. §11 is the ADR impact ledger; §12
the suggested task grouping.

House rule applied throughout: where an item was genuinely two-sided, the
fail-closed / consistent-with-documented-invariants side wins.

---

## 1. SQLite outbox adapter incompatible with its shipped migration schema (P1)

### Defect

`migrations/templates/sqlite/outbox.sql:27` declares
`id INTEGER PRIMARY KEY AUTOINCREMENT`. `INTEGER PRIMARY KEY` is SQLite's
strictly-typed rowid alias — the only column type SQLite actually enforces —
and `SQLiteOutboxRepository.add_event` inserts `str(uuid4())` into it
(`adapters/sqlite/outbox.py:107-131`), so **every** insert against the shipped
schema raises `sqlite3.IntegrityError: datatype mismatch`. The same broken
table body is embedded in the combined `migrations/schemas/sqlite_all.sql:61`,
which `SQLiteEventStore._conn()` applies on every first connection
(`adapters/sqlite/store.py:141`) — so every store-provisioned SQLite database
carries the unusable table.

The defect was masked because `tests/conftest.py::sqlite_outbox_repo`
(`tests/conftest.py:636-677`) hand-rolls an `id TEXT PRIMARY KEY` table
instead of using `get_schema()`. The real-schema conformance binding exists
but is `xfail(strict=False)`
(`tests/unit/adapters/test_sqlite_conformance.py:178-201`).

### Ruling

**Replace the schema in place**: `id TEXT PRIMARY KEY` in both
`migrations/templates/sqlite/outbox.sql` and the `event_outbox` block of
`migrations/schemas/sqlite_all.sql`. This is the sanctioned exception to the
append-only-by-file rule, and the justification must be understood precisely:

- Append-only exists to protect deployed data. This table **cannot hold data
  written by its only shipped writer** — every `add_event` since the file
  first shipped has raised before commit. There is no row anywhere that the
  old column definition describes.
- Compat analysis: a deployment that provisioned this schema has an empty
  `event_outbox` table. A deployment writing integer ids into it with its own
  code is not using `SQLiteOutboxRepository` (which cannot write to it) and is
  not affected by what fresh provisioning creates.
- The additive-fragment mechanism (`migrations/__init__.py`
  `_ADDITIVE_FRAGMENTS`) cannot express this: SQLite cannot ALTER a primary
  key's type, so no additive fragment can repair an already-created table.

`TEXT` (36-char hyphenated UUID) matches the PostgreSQL outbox
(`migrations/templates/outbox.sql:28`: `id UUID PRIMARY KEY DEFAULT
gen_random_uuid()`), the port contract (`OutboxEntry.id: UUID`), and the
file's own stated convention ("UUID stored as TEXT"). No `DEFAULT` on the
SQLite side — the adapter always supplies the id, and SQLite has no native
UUID generator to mirror `gen_random_uuid()`.

### Rejected alternatives

- **Adapter surfaces the rowid instead** (stop generating UUIDs): makes
  `OutboxEntry.id` an `int` for one backend, breaking the port type and
  cross-backend parity with PostgreSQL. The port is right; the schema is wrong.
- **Ship a second schema file** (`outbox_v2.sql` or a "replacement" name):
  leaves the broken file as what `get_schema("outbox", backend="sqlite")`
  returns, or forces naming gymnastics in the loader. Two shipped schemas for
  one table is worse than one corrected file with a recorded exception.
- **WONTFIX with the fixture as the de-facto schema**: the shipped migration
  is the product; tests hand-rolling a different table is the bug this item
  exists to kill.

### Fix specification

1. Edit `migrations/templates/sqlite/outbox.sql`: `id TEXT PRIMARY KEY`,
   comment updated to "UUID as TEXT (36 characters, hyphenated)"; delete the
   "Auto-incrementing ID for ordering" comment (ordering comes from
   `created_at`, which `get_pending_events` already orders by).
2. Edit `migrations/schemas/sqlite_all.sql` `event_outbox` block identically.
   No other table in either file changes.
3. Retarget `tests/conftest.py::sqlite_outbox_repo` onto
   `get_schema("outbox", backend="sqlite")` via `executescript`, deleting the
   hand-rolled DDL.
4. Remove the `@pytest.mark.xfail` decorator from
   `TestSQLiteOutboxRepository` in
   `tests/unit/adapters/test_sqlite_conformance.py`.
5. No adapter change.

### Failing-test-first requirement

The failing test already exists: the xfail'd conformance binding. Red step =
delete the xfail decorator and run
`uv run pytest tests/unit/adapters/test_sqlite_conformance.py -k Outbox` —
every insert-path test must fail with `IntegrityError: datatype mismatch`.
Green step = the two schema edits. The fixture retarget (step 3) then puts the
migration under the rest of the outbox unit suite as well.

### Acceptance criteria

- `TestSQLiteOutboxRepository` passes with no xfail marker.
- `tests/conftest.py` contains no hand-rolled `event_outbox` DDL;
  `grep -rn "AUTOINCREMENT" src/eventsource/migrations/templates/sqlite/outbox.sql
  src/eventsource/migrations/schemas/sqlite_all.sql` is empty.
- `SQLiteEventStore` + `SQLiteOutboxRepository` on the same freshly-provisioned
  database can `add_event` and round-trip (covered by the conformance suite
  once the fixture provisions from `get_schema`).
- Full unit suite green.

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed**, with an explicit migration note: existing SQLite
  databases carry an empty, unusable `event_outbox` table; operators should
  `DROP TABLE event_outbox` and re-provision from the corrected schema
  (`CREATE TABLE IF NOT EXISTS` will not replace it). State plainly that no
  data can have existed in it.
- ADR 0027 (§11) records the in-place replacement and the exception criteria:
  a shipped schema that provably never worked with its only writer may be
  corrected in place; everything else remains append-only by file.

---

## 2. Cutover can switch routing with up to `cutover_max_lag_events` missing (P2)

### Defect

`MigrationConfig.cutover_max_lag_events` defaults to 100
(`migration/models.py:391`, and again in the `from_dict` fallback at `:451`).
`cutover.py:321` allows cutover when
`lag.is_within_threshold(config.cutover_max_lag_events)` — i.e. with up to
100 source events provably absent from the target. Writes are paused for the
entire cutover (`cutover.py:306`), and nothing in the cutover sequence copies
the residue — Step 5 (`:350-354`) only sleeps ≤10 ms to drain in-flight
mirror appends. So any lag remaining at the routing switch (`:389-395`) is
events the target never receives while it becomes authoritative: a real loss
window, caught only by the **non-fatal** post-cutover consistency check.

This directly contradicts the invariant `dual_write.py:356-360` documents and
the campaign final review verified: "the accepted failure mode here is
stuck-until-recopied, **never a cutover over missing data**." The lag being
counted is not optimistic slack — `safe_lag_anchor` guarantees every counted
event is genuinely not in the target.

### Ruling

**Default `cutover_max_lag_events` to 0 (strict).** The knob survives: a
nonzero value becomes an explicit, documented operator acceptance of bounded
event loss at the switch, never a silent default.

Rationale: zero is *achievable* on the healthy path — writes pause before the
lag check, so a working mirror has already drained to exactly zero by Step 2;
the interceptor's whole anchor machinery exists so that lag counts only
provably-missing events. And zero is *recoverable* on the unhealthy path once
§5's in-phase resync exists: a clamped anchor is cleared by
`run_resync_pass`, not by tolerating the hole. Items 2 and 5 are designed
together for exactly this reason — strict-0 without resync means
"stuck-until-abort"; with resync it means "run one pass, then cut over."

### Rejected alternatives

- **Keep 100 and document the loss window prominently**: a default that
  silently discards events contradicts the module's own documented stance;
  defaults should be safe and overrides loud, not the reverse.
- **Remove the knob**: `__post_init__` already validates `>= 0`
  (`models.py:410`), and an operator consciously trading a bounded window for
  cutover availability is a legitimate, now-explicit choice.

### Fix specification

1. `migration/models.py:391` → `cutover_max_lag_events: int = 0`; `:451`
   `from_dict` fallback → `0`; attribute docstring (`:373`) rewritten:
   "Max lag allowed before cutover (default 0 — strict). Any nonzero value
   permits cutover while that many source events are absent from the target."
2. Update in-tree examples that model the old default:
   `sync_lag_tracker.py:22` and `:140` docstring examples, and any
   `cutover_max_lag_events=100` occurrences found by grep across `src/` and
   `docs/`.
3. No change to `cutover.py`, `is_within_threshold`, or `calculate_lag` —
   with threshold 0 the tracker reads `limit=1` and the existing bounded-count
   logic behaves correctly.

### Failing-test-first requirement

Two red tests:

- `MigrationConfig().cutover_max_lag_events == 0` and
  `MigrationConfig.from_dict({}).cutover_max_lag_events == 0` (fails: both 100).
- Behavioral: a cutover attempt with exactly one un-mirrored source event and
  a default config fails with `CutoverLagError` and rolls back to
  `DUAL_WRITE` (fails today: 1 ≤ 100 passes and routing switches).

Existing tests asserting the old default (`tests/unit/migration/test_models.py`
default-values test, any cutover tests relying on slack) flip in the green step.

### Acceptance criteria

- Default-config cutover refuses on any nonzero lag and rolls back; explicit
  `cutover_max_lag_events=N` preserves the old tolerance.
- `grep -rn "cutover_max_lag_events" src/ docs/` shows no remaining claim that
  the default is 100.
- Full unit suite green.

### Docs / CHANGELOG obligations

- CHANGELOG **Changed** (behavior change, prominent): new default refuses
  cutover until lag is exactly 0; cutovers that previously succeeded with
  residual lag now return `CutoverLagError` and roll back. Migration note: to
  restore the old behavior pass `MigrationConfig(cutover_max_lag_events=100)`
  explicitly — and understand it as accepting up to 100 lost events at the
  switch.
- `docs/guides/live-migration.md`: the `DUAL_WRITE -> CUTOVER` transition text
  and any config walkthrough updated; add a warning box on the meaning of a
  nonzero threshold; cross-reference `run_resync_pass` (§5) as the remedy for
  a clamped anchor.
- ADR 0028 (§11) records the decision together with §5.

---

## 3. Live-phase lag has no signal (P2)

### Defect

`Subscription.lag` is `max(0, _events_seen - _events_delivered)`
(`subscriptions/subscription.py:493`), with a documented invariant that
callers keep the two counters symmetric across any boundary (`:484-488`).
Only the catch-up runner honors it: `catchup.py:309` records seen per read
batch and `:374-376` reconciles abandoned tails. The live runner never
records seen — `_handle_live_event` (`runners/live.py:220-254`) counts
receipts only into its private stats, while `_process_live_event` increments
`_events_delivered` via `record_event_processed` (`:311`, `:339`, `:349`).

Consequences: lag is structurally 0 during LIVE (a stalled subscriber with
events arriving is indistinguishable from a healthy idle one), and the
accumulated delivered-surplus makes `seen - delivered` deeply negative, so a
later transition back to catch-up under-reports real backlog until the
surplus is burned off.

### Ruling

**Make the accounting phase-symmetric; the bus delivery receipt is the live
seen-point.** `_handle_live_event` records `record_events_seen(1)` for every
received event before branching (transition buffer, pause buffer, or direct
processing). Every terminal path in `_process_live_event` that does *not*
call `record_event_processed` compensates with `record_events_unseen(1)`:

| Path | Today | After |
|---|---|---|
| delivered (with or without position, `:339`/`:349`) | delivered+1 | seen+1 at receipt, delivered+1 — balanced |
| filtered, position known (`:311`) | delivered+1 | balanced as above |
| duplicate skip (`:285-296`) | nothing | seen+1 then unseen+1 — net zero |
| filtered, no position (`:299-316`) | nothing | net zero via unseen |
| failure swallowed under `continue_on_error` (`:355-379`) | failed+1 | unseen+1 — the event is terminally disposed (possibly DLQ'd); failure counters carry that signal, lag must not report it as outstanding work forever |
| failure re-raised (`continue_on_error=False`) | raises | seen stays +1 — honest: the subscription enters ERROR with one genuinely unprocessed event |

Buffered events (catch-up→live transition buffer, pause buffer) are seen at
receipt and drain through `_process_live_event` on `process_buffer` /
`process_pause_buffer` — so live lag now equals queue depth plus in-flight
count. That is precisely the operator signal the backlog item asks for: a
paused or stalled subscription shows growing lag; a healthy one shows ~0.

### Rejected alternatives

- **"Lag unavailable in live" marker on SubscriptionStatus**: gives operators
  nothing actionable, adds a tri-state to a public surface, and ignores that
  half the machinery (`record_events_seen`/`record_events_unseen` with a
  written invariant) already exists — the live runner simply violates it.
- **Position-distance lag in live**: live events carry no position from any
  in-tree bus (`live.py:381-401` returns None in every current
  configuration), and positions are opaque tokens that cannot be subtracted
  (ADR 0019).

### Fix specification

1. `runners/live.py::_handle_live_event`: first statement
   `await self.subscription.record_events_seen(1)`.
2. `_process_live_event`: add `await self.subscription.record_events_unseen(1)`
   on the duplicate-skip return, the filtered-without-position return, and in
   the swallowed-failure branch (after `record_event_failed`, only when not
   re-raising).
3. `Subscription.lag` and `record_events_seen` docstrings updated to describe
   the phase-symmetric semantics (seen = read from feed *or* received from
   bus; live lag ≈ received-not-yet-delivered including buffers).
4. No catch-up runner change; no public API change.

### Failing-test-first requirement

- **Stalled-subscriber test** (the headline red test): live runner, subscriber
  whose `handle()` blocks on an `asyncio.Event`; dispatch 3 events as tasks;
  assert `subscription.lag >= 1` while blocked (today: 0), and `lag == 0`
  after release and drain.
- **Symmetry test**: deliver N events live, assert
  `subscription._events_seen == subscription._events_delivered` and `lag == 0`
  (today: seen == 0, delivered == N).
- **Net-zero disposal tests**: duplicate-skip and filtered-no-position paths
  leave lag at 0; swallowed failure leaves lag at 0 with `events_failed == 1`.
- **Buffer test**: `start(buffer_events=True)`, push K events, assert
  `lag == K` before `process_buffer()` and 0 after.

### Acceptance criteria

- All four red tests green; existing catch-up lag/unseen tests pass
  unmodified (the catch-up side is untouched).
- The `lag` invariant (`subscription.py:484-488`) holds across
  CATCHING_UP → LIVE → pause → resume in an end-to-end manager test if one
  exists; if none exercises the transition, add the assertion to the closest
  existing transition test rather than building new scaffolding.

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed**: live-phase lag now reports events received but not yet
  delivered; previously always 0.
- Grep `docs/` for prose describing lag as catch-up-only and update (the
  subscriptions guide/health docs if they mention it). No ADR — bugfix
  restoring a documented invariant.

---

## 4. Wire `position_mapper` into BulkCopier's coordinator call site (P2)

### Defect

`MigrationCoordinator` accepts and stores a `position_mapper`
(`migration/coordinator.py:203`, `:261`) and `MigrationConfig` documents
`position_mapping_enabled: bool = True` ("Whether to record position
mappings", `models.py:375`, `:393`) — but the flag is read **nowhere** in
`src/`, and the coordinator's `BulkCopier` construction
(`coordinator.py:815-820`) omits `position_mapper=`. So a default-path
migration records no mappings, and subscription checkpoint translation —
`migrate_subscriptions: bool = True` is also the default — silently skips
(`coordinator.py:1141-1153`) or raises when called directly (`:1733-1751`).
Two documented-True defaults are dead on arrival; the only code exercising
the mapper path is tests that construct `BulkCopier` with a mapper by hand.

### Ruling

**Wire it, gated by the config flag**: the coordinator builds its copier with
`position_mapper=self._position_mapper if migration.config.position_mapping_enabled
else None`. This makes both documented defaults true when the coordinator was
given a mapper, and makes `position_mapping_enabled=False` a real switch.

Extract the construction into a private helper — `_build_copier(migration)` —
used by `_run_bulk_copy` **and** by §5's `run_resync_pass`, so the two copy
entry points can never diverge on wiring. This is the deliberate joint seam
with item 5.

Performance consequence, stated honestly: with a mapper attached the copier
appends one event at a time (`bulk_copier.py:540`, `:586`) so each target
position can be recorded — the unmeasured cost named as risk 2 in the
legacy-store-retirement spec §11 now runs on the default path *when a mapper
was provided*. A coordinator constructed without a mapper (the parameter
defaults to None) keeps batched appends, as does
`position_mapping_enabled=False`. The guide documents the trade.

### Rejected alternatives

- **Document the opt-in instead of wiring**: leaves two documented-True
  config defaults permanently inert and `migrate_subscriptions` broken by
  default — a documented lie is still a lie.
- **Always wire, ignore the flag**: makes `position_mapping_enabled` dead in
  the opposite direction and removes the only escape hatch from per-event
  append cost short of rebuilding the coordinator.

### Fix specification

1. Add `MigrationCoordinator._build_copier(self, migration: Migration) ->
   BulkCopier`: constructs with source, target-for-that-migration,
   `self._migration_repo`, `enable_tracing=self._enable_tracing`, and the
   gated `position_mapper` above. (`_run_bulk_copy` already has
   `target_store` in scope; pass it as a parameter or resolve inside —
   implementer's choice, but one helper, two callers.)
2. `_run_bulk_copy` (`coordinator.py:815-820`) uses `_build_copier`.
3. No `BulkCopier` change; no config change.

### Failing-test-first requirement

- Coordinator constructed **with** a position mapper, default config, run a
  small migration to completion → assert mappings were recorded (via the
  mapper/repository test double). Fails today: zero recordings.
- Same setup with `position_mapping_enabled=False` → zero recordings, and the
  copier used the batched path (assert via the mapper double never being
  touched; batched-vs-individual can be asserted with a spy on the target
  store's append batch sizes if cheap, otherwise the mapper assertion
  suffices).

### Acceptance criteria

- Default-config migration through a mapper-equipped coordinator produces
  translatable positions: `SubscriptionMigrator`/`translate_position` works
  end-to-end in at least one test without hand-built copiers.
- `position_mapping_enabled` is read in exactly one place
  (`grep -rn position_mapping_enabled src/eventsource/migration/` shows
  models + the new gate).
- Full unit suite green.

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed** (default-path migrations now record position mappings
  when a mapper is configured) with the per-event append cost note.
- `docs/guides/live-migration.md` checkpoint-translation section: state the
  gate and the cost trade.
- Covered by ADR 0028's scope note (§11); no standalone decision.

---

## 5. In-phase resync for dual-write mirror failures after bulk copy (P2)

### Defect

`safe_lag_anchor`'s failure clamp (`dual_write.py:311-324`, `:393-398`) is
released only by `mark_copy_pass_complete`, and the sole caller is the
bulk-copy loop (`coordinator.py:835`). Once the migration is in `DUAL_WRITE`,
no code path can run an absorbing copy pass: a transient mirror failure late
in a long migration clamps the anchor permanently, cutover refuses forever
(correctly — the data really is missing), and today's only remedy is
abort-and-restart, as `docs/guides/live-migration.md:143-145` documents. The
same dead end applies when `_MAX_CATCHUP_ROUNDS` is exhausted
(`coordinator.py:838-848`).

### Ruling

**Add an operator-triggered resync entry point**:

```python
async def run_resync_pass(self, migration_id: UUID) -> int:
    """Run one bounded catch-up copy pass while in DUAL_WRITE.

    Returns the number of unabsorbed mirror failures remaining
    (0 means the lag anchor is unclamped and cutover can proceed
    once lag drains).
    """
```

Semantics, each load-bearing:

- **Preconditions**: migration exists (`MigrationNotFoundError`), phase is
  `DUAL_WRITE` (`MigrationStateError` otherwise), no copier already active
  for this migration id (`MigrationError`).
- **Body**: build the copier via `_build_copier(migration)` (§4's shared
  seam — the resync pass inherits identical position-mapper wiring), register
  it in `_active_copiers`, run one pass via the existing
  `_run_copy_pass(copier, migration)` (which streams progress to status
  observers), pop the copier in `finally`.
- **On completion**: re-fetch the migration, and if an interceptor is
  installed for this migration id, call
  `interceptor.mark_copy_pass_complete(current.last_source_position)` and
  return its result. This preserves the attestation contract to the letter
  (`dual_write.py:407-435`): the pass began after interceptor installation
  (the interceptor has been installed since bulk-copy start and `DUAL_WRITE`
  postdates it) and ran to completion — both facts the coordinator, and only
  the coordinator, can attest. If no interceptor is installed (coordinator
  restart — interceptor state is in-memory), skip the call and return 0: the
  pass still advanced the persisted checkpoint, which is what
  `_lag_anchor` (`coordinator.py:1289-1292`) falls back to; there is no
  in-memory failure list left to absorb.
- **On a pass that does not complete** (cancelled): do **not** call
  `mark_copy_pass_complete` (an incomplete pass proves nothing); raise
  `MigrationError` naming the incomplete pass. Copy failures propagate as
  `BulkCopyError` unchanged.
- **Phase is never touched.** Verified: `BulkCopier.run`
  (`bulk_copier.py:259-436`) writes progress and errors only, never phase, so
  a resync pass cannot corrupt the `VALID_TRANSITIONS` state machine. The
  migration reads as `DUAL_WRITE` throughout.
- One pass per call; the return value tells the operator whether to call
  again. No internal round cap — bounding is the caller's policy here, unlike
  the automated `_MAX_CATCHUP_ROUNDS` loop.

None of the dual-write invariants weaken: the clamps, the
never-advance-over-a-hole rule, and the saturation fail-closed path are
untouched — this adds the *sanctioned* release path to a phase that lacked
it. Re-copying is already safe by design (the copier treats
already-present events as copied, `_run_bulk_copy` docstring
`coordinator.py:786-789`).

Interaction with §2: strict-0 cutover makes this API the standard remedy —
the pairing turns "stuck-until-recopied" from a dead end into a runbook step.

### Rejected alternatives

- **Automatic background resync during DUAL_WRITE**: a self-triggering copy
  pass hides mirror instability from the operator and contends with live
  writes on an unbounded schedule; the failure mode being remedied is rare
  and operator-visible by design (lag refuses to drain).
- **Allow `mark_copy_pass_complete` to be driven externally** (public
  "absorb" API without a copy pass): violates the method's contract — the
  attestation is only sound following a completed covered pass.
- **Fold into `trigger_cutover` as an automatic pre-pass**: couples a
  read-heavy bulk operation into the sub-second cutover window and blurs the
  phase model.

### Fix specification

As specified in the ruling. Public API: one new coordinator method — additive
only, exported implicitly via the class; no `__init__.py` change needed
(`MigrationCoordinator` is already exported).

### Failing-test-first requirement

Red test (unit, in-memory stores): drive a migration into `DUAL_WRITE` with
one mirror failure (target store double that fails one append during
dual-write, then recovers); assert `is_cutover_ready` is False with the
anchor clamped; call `run_resync_pass` → returns 0; assert the anchor
advanced (`_lag_anchor` past the failure position) and `is_cutover_ready`
becomes True once lag is 0. Fails today: the method does not exist. Guard
tests: wrong phase raises `MigrationStateError`; concurrent active copier
raises `MigrationError`; unknown id raises `MigrationNotFoundError`;
restart-shaped case (no interceptor registered) completes and returns 0.

### Acceptance criteria

- The red scenario above passes end to end, including a subsequent successful
  strict-0 cutover (ties §2 and §5 together in one test).
- No dual-write invariant test changes: the existing `safe_lag_anchor` /
  `mark_copy_pass_complete` suites pass unmodified.
- mypy strict passes on the new method.

### Docs / CHANGELOG obligations

- CHANGELOG **Added**: `MigrationCoordinator.run_resync_pass`.
- `docs/guides/live-migration.md:143-145`: replace "There is currently no
  in-phase resync … abort the migration and restart it" with the API, a short
  runbook example, and the §2 cross-reference.
- ADR 0028 (§11) records §2+§5 as one decision: strict-by-default cutover
  with an explicit in-phase recovery path.

---

## 6. Reconcile `events.tenant_id` schema drift (P2)

### Defect

The canonical schema declares `tenant_id UUID`
(`migrations/schemas/events.sql:35`; identically
`migrations/templates/events.sql:35`), matching the type system end to end:
`DomainEvent.tenant_id: UUID | None` (`events/base.py:118`),
`TenantId = UUID | None` (`types.py:14`), `FeedReadOptions.tenant_id: UUID |
None` (`ports/envelopes.py:96`), and the ports PostgreSQL adapter binds
`uuid.UUID` values directly (`adapters/postgresql/store.py:312`, `:501-503`).

`tests/integration/conftest.py:280` hand-rolls `tenant_id VARCHAR(255)` into
the shared session `postgres_engine` fixture (`:407-433`), alongside three
more hand-rolled blocks (`EVENTS_SCHEMA_STATEMENTS`,
`CHECKPOINTS_SCHEMA_STATEMENTS`, `DLQ_SCHEMA_STATEMENTS`,
`OUTBOX_SCHEMA_STATEMENTS`, `:272-376`) that drift from the canonical files
in columns, constraints, and indexes. The drift is why the ports conformance
tests had to retreat to a private `ports_conformance` database
(`tests/integration/adapters/conftest.py:1-19`, whose docstring still
attributes the VARCHAR side to the now-deleted legacy store). Consequence:
the main integration suite exercises a schema no production deployment has,
and the canonical schema's only full-suite consumer is the private database.

### Ruling

**The canonical migrations schema is authoritative; the hand-rolled test DDL
dies.** The `postgres_engine` fixture provisions from
`eventsource.migrations.get_schema(...)` (events, checkpoints, dlq, outbox —
or `get_all_schemas()`), executed through a **raw asyncpg connection**, not
sqlalchemy `text()` — the canonical files are multi-statement scripts with
`COMMENT ON`, which is exactly why the snapshots schema already goes through
the raw connection in this fixture (commit ce6d15a). Any integration test
that then fails by binding `str(tenant_id)` into `events` gets fixed to bind
`UUID` — that *is* the reconciliation the backlog item asks for.

The private `ports_conformance` database **stays** (its DROP+recreate
isolation is independently valuable), but the stale docstring in
`tests/integration/adapters/conftest.py` is rewritten: the drift it documents
as out-of-scope is now fixed, and the remaining justification is isolation
from table-recreating tests. The stale comment at
`tests/integration/adapters/test_postgresql_conformance.py:159` is updated in
the same pass.

### Rejected alternatives

- **Make the canonical schema VARCHAR**: fights the entire type system above,
  would break every deployed database provisioned from the canonical UUID
  schema, and `migrations/` is append-only for schemas that work — this one
  does, as the ports conformance suite proves.
- **Keep both, document the difference**: two schemas for one table where one
  is test-only fiction is the defect, not a state to preserve.

### Fix specification

1. Delete the four `*_SCHEMA_STATEMENTS` blocks from
   `tests/integration/conftest.py`; provision via `get_schema()` through the
   raw asyncpg driver connection in `postgres_engine` (pattern already in the
   file for snapshots).
2. Run the full `-m postgres` integration suite; fix any test that assumed
   VARCHAR semantics (string tenant ids, missing `uq_events_aggregate_version`
   constraint name, index differences) to match the canonical schema.
3. Update the two stale comment/docstring sites named above.

### Failing-test-first requirement

This is test-infrastructure reconciliation, so the red step is the fixture
flip itself: swap the provisioning **first**, run the `-m postgres` suite, and
treat every failure as the reconciliation worklist. No failure may be
resolved by editing schema — only by fixing the test (or, if one surfaces, an
adapter bug, which then gets its own red test). Record in the PR description
which tests failed on the flip; "none" is a legitimate and reportable outcome.

### Acceptance criteria

- `tests/integration/conftest.py` contains no hand-rolled DDL for events,
  checkpoints, dlq, or outbox (`grep -n "CREATE TABLE" tests/integration/conftest.py`
  is empty).
- Full `-m postgres` integration suite green against canonical-schema
  provisioning.
- No remaining reference to the legacy store or the VARCHAR drift in
  `tests/integration/adapters/conftest.py`'s docstring.

### Docs / CHANGELOG obligations

- CHANGELOG: only if step 2 surfaces an adapter fix (then **Fixed** with
  specifics); the fixture change itself is not user-facing.
- ADR 0027 (§11) notes the reconciliation direction (canonical UUID wins) for
  the record. **Ordering constraint**: this item must land after §7, whose
  additive fragment changes what `get_schema("events")` returns — flipping
  the fixture first would provision a schema the updated adapter can't use.

---

## 7. Make the postgres safe-horizon predicate wraparound-safe (P2)

### Defect

`adapters/postgresql/store.py:80`:

```
_HORIZON_PREDICATE = "xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint"
```

used in `read_all`'s feed queries at `:493` and `:524`. `xmin` is a 32-bit
xid whose textual value wraps at 2^32; `pg_snapshot_xmin` returns an
epoch-extended 64-bit `xid8`. Once a cluster crosses its first xid epoch
(~4 billion transactions), the right side exceeds 2^32 permanently while the
left side stays below it: the predicate becomes universally true and the
no-skip protection — deferring rows whose inserting transaction may still be
uncommitted, so a resuming reader can't leap over them — silently vanishes.
High-write-volume deployments are exactly the ones that both cross epochs and
need the guard. The code comment (`:77-79`) already names the fix it defers:
"revisit if a `xid8` column is ever added."

### Ruling

**Add the xid8 column and restructure the predicate.**

Schema (within append-only rules, via the existing mechanisms):

- New additive fragment `migrations/additive/events_txid.sql`: adds
  `txid xid8` (nullable, `DEFAULT pg_current_xact_id()`) to `events`, plus
  registration in `_ADDITIVE_FRAGMENTS` for `("events", "postgresql")`,
  `("events_partitioned", "postgresql")`, and `("all", "postgresql")`
  (`migrations/__init__.py:68-74`). SQLite needs nothing (single-writer, no
  horizon — `adapters/sqlite/store.py:10-15`).
- New operator script `migrations/updates/004_add_events_txid.sql` for
  existing databases, as **two statements**:
  `ALTER TABLE events ADD COLUMN txid xid8;` then
  `ALTER TABLE events ALTER COLUMN txid SET DEFAULT pg_current_xact_id();`
  Split deliberately: `ADD COLUMN` with a volatile default forces a full
  table rewrite; adding nullable-no-default is metadata-only, and
  `SET DEFAULT` afterwards applies to future inserts only.

NULL semantics: a NULL `txid` row predates the ALTER. `ALTER TABLE` takes
ACCESS EXCLUSIVE, so any transaction that inserted a NULL row finished before
every post-migration snapshot — NULL rows are always definitely-committed and
safe to read.

Predicate: compute the horizon **once per `read_all` batch** with a separate
scalar query — `SELECT pg_snapshot_xmin(pg_current_snapshot())` — and bind it:

```
(txid IS NULL OR txid < :txid_horizon)
```

Native `xid8` comparison (64-bit, does not wrap on any human timescale), no
casts, and a plain bound-parameter filter in place of the volatile inline
expression. The adapter's INSERT does not name the column; the DEFAULT
covers it. Version floor: `pg_current_xact_id`/`xid8` require PostgreSQL 13 —
the same floor `pg_current_snapshot()` already imposes, so no requirement
change.

Failure mode for un-migrated databases: the feed query errors naming the
missing column — **loud by design** (see rejected alternatives).

**Effect on the deferred re-bench backlog entry**: its premise changes and the
entry must be updated, not closed. It benchmarked the inline xmin-cast
predicate that EXPLAIN showed defeating the `global_position` index; that
predicate is deleted here, and the once-per-batch-horizon restructure is the
very mitigation the entry proposed. The re-bench remains warranted — at 1M+
rows with concurrent writers, now against `(txid IS NULL OR txid < $n)` — and
its methodology section must be rewritten against the new shape.

### Rejected alternatives

- **Modular 32-bit comparison / `age()`-style arithmetic** (no DDL):
  PostgreSQL defines no ordering operators on `xid`, so this means hand-rolled
  modulo arithmetic with an irreducible ambiguity at the 2^31 boundary — it
  narrows the failure window rather than closing it, while keeping an
  unindexable expression.
- **WONTFIX + document**: silently losing a correctness guard in precisely
  the deployments that need it is fail-open; this slice's standing rule is
  fail-closed.
- **`NOT NULL` with backfill**: forces a full rewrite of production events
  tables during the operator migration for zero correctness gain over the
  NULL-is-safe argument above.
- **Runtime column-existence probe with fallback to the old predicate**:
  keeps the wraparound-unsafe path alive forever and hides the operational
  requirement; a loud error with a clear migration note is safer than a
  silent downgrade.

### Fix specification

1. The two migration artifacts and registry entries above.
2. `adapters/postgresql/store.py`: delete `_HORIZON_PREDICATE`'s xmin form;
   add the per-batch horizon fetch in the `read_all` path (one scalar query
   per batch, same connection) and the bound-parameter predicate at both call
   sites (`:493`, `:524`); rewrite the explanatory comment (`:72-79`)
   including the NULL-rows rationale.
3. `tests/integration/adapters/` no-skip and conformance tests run unchanged
   against the now-fragment-carrying canonical schema.

### Failing-test-first requirement

- Unit (red): assert the assembled feed SQL contains `txid IS NULL OR txid <`
  and does **not** contain `xmin::text` (query-assembly test in the existing
  adapter unit-test style); assert `get_schema("events")` and
  `get_schema("all")` contain the `txid` fragment while
  `get_schema("events", additive=False)` does not.
- Integration (green must hold): existing pg no-skip test passes; new test
  that a row with explicit `txid = NULL` (simulating a pre-migration row) is
  returned by `read_all`.
- True epoch wraparound is not integration-testable (an epoch cannot be
  advanced in a testcontainer); the unit-level predicate-shape assertions are
  the regression guard, and the ADR records the reasoning.

### Acceptance criteria

- No `xmin` reference remains in `src/eventsource/adapters/postgresql/`.
- `-m postgres` suite green, including the no-skip conformance tests.
- `migrations/updates/004_add_events_txid.sql` exists and its two-statement
  structure is commented with the rewrite-avoidance rationale.

### Docs / CHANGELOG obligations

- CHANGELOG **Changed**, prominent migration note: PostgreSQL deployments
  MUST apply `migrations/updates/004_add_events_txid.sql` before upgrading —
  the feed read path now references `events.txid` and fails loudly without
  it. (Fresh provisioning via `get_schema` needs nothing.)
- BACKLOG: rewrite the "Re-benchmark pg catch-up horizon predicate at scale"
  entry's premise per the ruling above (part of this slice's PR, since this
  slice invalidates its text).
- ADR 0027 (§11) records the column, the NULL-is-safe argument, and the
  per-batch horizon design.

---

## 8. Reconcile DLQ `delete_resolved_events` cutoff semantics (P3)

### Defect

`adapters/memory/dlq.py:330-332` truncates now-UTC to midnight before
subtracting `older_than_days`; `adapters/sql/dlq.py:386` subtracts from
`now()` directly. At `older_than_days=0` a moments-ago resolution is deleted
by both SQL dialects but kept by the memory adapter until midnight rolls
over. The port docstring (`ports/dlq.py:186-197`) specifies nothing, which is
why two conforming-looking adapters diverged; the divergence is currently
*institutionalized* — excluded from the shared conformance suite by its
module docstring (`testing/conformance_ports/dlq.py:4-6`) and pinned by
per-backend tests
(`tests/unit/adapters/test_sqlite_conformance.py:155-176`,
`tests/unit/adapters/test_memory_dlq_conformance.py:17-34`).

### Ruling

**The SQL semantic wins**: cutoff is exactly `datetime.now(UTC) -
timedelta(days=older_than_days)`; an entry is deleted iff `status ==
"resolved"` and `resolved_at < cutoff`; `older_than_days=0` therefore deletes
every already-resolved entry. Rationale: it is the plain-language reading of
"older than N days"; it is what two of the three backends (the SQL adapter
serves both dialects) already do; the memory adapter's truncation is
self-described as "simplified" (`memory/dlq.py:331`); and it matches the
rolling-cutoff shape of the outbox's SQL cleanup. The memory adapter conforms
to the port, not the port to an implementation shortcut.

### Rejected alternatives

- **Midnight-truncation as the contract**: makes `older_than_days=0` mean
  "since midnight UTC" — calendar-flavored semantics nobody documented, would
  change the behavior PostgreSQL/SQLite deployments already observe, and
  requires touching two adapters instead of one.
- **Leave divergent, document per-backend**: cleanup retention is exactly the
  kind of behavior a conformance suite exists to pin; "pick one semantic" is
  the backlog item's own framing.

### Fix specification

1. `adapters/memory/dlq.py::delete_resolved_events`: delete the
   `.replace(hour=0, minute=0, second=0, microsecond=0)` truncation; cutoff
   becomes `datetime.now(UTC) - timedelta(days=older_than_days)`.
2. `ports/dlq.py::delete_resolved_events` docstring: state the cutoff formula,
   the strict `<` comparison against `resolved_at`, and the day-zero
   consequence explicitly.
3. `testing/conformance_ports/dlq.py`: add the shared day-zero test (add →
   resolve → `delete_resolved_events(older_than_days=0)` deletes it; the
   existing failed-entries-intact test at `:230` stays); rewrite the module
   docstring removing the "intentionally excluded" carve-out.
4. Delete the two per-backend divergence tests
   (`test_sqlite_delete_resolved_events_removes_past_cutoff_entries`,
   `test_memory_delete_resolved_events_cutoff_is_truncated_to_midnight`) —
   their content now lives in the shared suite.

### Failing-test-first requirement

The new shared conformance day-zero test, run via the existing memory binding
(`tests/unit/adapters/test_memory_dlq_conformance.py`) — red today (memory
keeps the entry), already green on the SQL bindings. That asymmetry is the
proof the test pins the right thing.

### Acceptance criteria

- Shared suite carries the day-zero test; zero per-backend
  `delete_resolved_events` tests remain; all three bindings (memory unit,
  sqlite unit, postgres integration) green.
- Port docstring specifies the cutoff; conformance module docstring no longer
  claims cleanup semantics differ per backend.

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed**: in-memory DLQ `delete_resolved_events` now uses the
  exact rolling cutoff (was midnight-truncated); affects tests/dev only in
  practice, but it is a behavior change on a public class — say so.
- No ADR — port-contract tightening plus one-adapter conformance fix.

---

## 9. Catch-up can end early with `completed=False` on an all-filtered batch (P3)

### Defect

`subscriptions/runners/catchup.py::_process_batch` returns `events_in_batch`
— **delivered** events only. Envelopes excluded by the event-type filter
advance position and the reconciliation counter (`:335-345`) but not
`events_in_batch`. The outer loop breaks on a zero return (`:239-241`), so a
batch whose envelopes are all filtered ends catch-up with
`self._reached_target` still False → `completed=False`
(`:243`), even though the store position advanced and the feed has more. The
loop conflates "empty read" (genuinely caught up; sets `_reached_target` at
`:306-308`) with "all filtered" (nothing matched *this batch*). The live
runner happens to cover the gap end-to-end, but catch-up's own completion
signal is unreliable for heavily-filtered subscriptions.

### Ruling

**Split the return contract and delete the delivered-count break.**
`_process_batch` returns a small module-private frozen dataclass:

```python
@dataclass(frozen=True)
class _BatchOutcome:
    envelopes_read: int
    events_delivered: int
```

The outer loop accumulates `total_processed += outcome.events_delivered` and
has **no** zero-break at all: termination is exactly `_reached_target`
(already set on empty read `:306-308` and on target overshoot `:325-327`),
`_stop_requested`, and `not self._running` — all already in the `while`
condition (`:224`). Progress is guaranteed without the break: every counted
envelope (delivered *or* filtered) calls `record_event_processed`, advancing
`last_processed_position`, and the next read starts strictly after it; the
position-`None` guard (`:325`) already converts the only no-progress
pathology into `_reached_target`.

`CatchUpResult.events_processed` keeps its current meaning — events
*delivered* to the subscriber — which is why filtered envelopes must not be
folded into a single return count (see rejected alternatives).

### Rejected alternatives

- **Count filtered envelopes into the existing single return value**: fixes
  the loop but silently redefines `CatchUpResult.events_processed` (public
  surface) from "delivered" to "delivered + filtered". Two numbers exist;
  return two numbers.
- **Break on "position did not advance"**: positions are opaque tokens;
  per-iteration comparison adds machinery the empty-read check already
  provides for the only genuine terminal case.

### Fix specification

As in the ruling: `_BatchOutcome` in `catchup.py`, `_process_batch` returns
it (`envelopes_read = len(envelopes)`, `events_delivered = events_in_batch`),
the `if batch_result == 0: break` block (`:239-241`) is deleted, and the
docstrings of `run_until_position`/`_process_batch` updated. No public API
change; `CatchUpResult` untouched.

### Failing-test-first requirement

Red test: subscription filtered to event type B over a feed containing only
type-A events spanning at least two read batches, target at the feed head →
`run_until_position` must return `completed=True`, `events_processed == 0`,
and `final_position` at the last A envelope. Fails today: returns after the
first batch with `completed=False`. Companion test: alternating A/B batches
where an interior batch is all-A still reaches the target with the correct
delivered count.

### Acceptance criteria

- Both tests green; existing catch-up suite (including the unseen-reconcile
  tests around `:368-376`) passes unmodified.
- `completed` is True iff `_reached_target` — no other exit reports success.

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed**: catch-up no longer terminates early with
  `completed=False` when a batch is entirely filtered.
- No ADR; no doc pages describe the old behavior.

---

## 10. SQLite adapter: reads share the writer's connection (P3)

### Defect

All read paths — `_do_read_stream` (`adapters/sqlite/store.py:306`),
`_do_read_all` (`:370`), `_do_read_category` (`:418`),
`get_stream_version` (`:341`), `event_exists` (`:354`),
`current_position` (`:402`) — execute on the same aiosqlite connection as
`append` but **outside** `self._lock`, which only `append` holds (`:206`).
`append` is multi-statement: per-event INSERTs then `commit()`
(`:233-260`). aiosqlite serializes individual statements on one background
thread, but each INSERT is separately awaited, so the event loop can schedule
a reader's `execute` between two of them — and a same-connection statement
runs *inside* the append's open implicit transaction. A read interleaved
mid-append observes uncommitted rows of the in-flight batch: `read_all` can
yield a torn batch, and `current_position` can mint a `Position` for a row
that is then rolled back on `IntegrityError` (`:263-296`) — a position for an
event that never existed.

### Ruling

**Take reads under the existing write lock — not a second connection, and not
WONTFIX.** The shared single connection *is* load-bearing, exactly as the
task anticipated: the class docstring (`:71-74`) pins it as required for
`":memory:"` databases, whose contents live only as long as the creating
connection (a second connection sees a different, empty database; shared-cache
URIs change semantics and WAL does not apply to in-memory). That rules out
the separate-read-connection design — but it does not justify a documented
no-op, because the cheap fix exists: every read helper already fetches
eagerly (`fetchall()` before any yield), so wrapping `execute` + fetch in
`async with self._lock:` gives bounded critical sections and never holds the
lock across consumer iteration. Concurrency cost is ~nil — aiosqlite already
serializes per-statement on one thread; the lock only adds ordering at
transaction granularity, which is the correctness requirement.

Deadlock safety: `asyncio.Lock` is non-reentrant; `append`'s internal SELECTs
(`:208-217`, `:273-282`) use the connection directly and must stay that way —
the fix must not route append internals through the public read helpers. No
current code path acquires the lock twice.

### Rejected alternatives

- **Separate read connection**: breaks `":memory:"` (see above) and forks the
  adapter into two connection-lifecycle regimes for a P3.
- **WONTFIX with documentation**: the defect violates append atomicity — an
  observable torn batch and phantom positions — which the store ports imply
  and every other adapter provides; the fix is ~six small edits.
- **Snapshot isolation via explicit `BEGIN` on reads**: same connection means
  the reader would still be inside the writer's transaction; SQLite offers no
  same-connection isolation.

### Fix specification

Wrap the `conn.execute(...)` + `fetchall()`/`fetchone()` pair of each of the
six read paths listed above in `async with self._lock:`. Yielding of
envelopes stays outside the lock. `append` unchanged. Update the module/class
docstring to state the discipline: *all* statements on the shared connection
run under `self._lock`; readers therefore never observe an open append
transaction.

### Failing-test-first requirement

Deterministic interleave test (unit, `tests/unit/adapters/`): obtain the live
connection via `await store._conn()`, wrap its `execute` so that after the
**first** INSERT into `events` it sets an `asyncio.Event` and waits on a
second one; start `append(stream, [e1, e2], ...)` as a task; when the first
INSERT has landed, run `read_all` (and `current_position`) from the test
task, then release the append. Red today: the read observes 1 of 2 events /
a position for an uncommitted row. Green after: the read blocks until the
append commits and observes 0-or-2, never 1. (White-box interception of the
store's connection is acceptable here; the wrapping must be transparent to
the lock itself — intercept below the adapter, not around `_lock`.)

### Acceptance criteria

- The interleave test passes; a rollback variant (second INSERT forced to
  `IntegrityError`) shows readers never observed the first row.
- Existing sqlite conformance + state-machine suites pass unmodified.
- No read path touches the connection outside `self._lock`
  (reviewable by inspection of the six sites).

### Docs / CHANGELOG obligations

- CHANGELOG **Fixed**: SQLite reads can no longer observe a partially
  committed append.
- No ADR — adapter-internal correctness fix.

---

## 11. ADR Impact

| ADR | Verdict |
|---|---|
| 0019 (clean architecture store ports) | **Stands.** Positions stay opaque; §3 and §9 explicitly refuse position arithmetic; §7 changes an adapter's internal predicate, not the port. |
| 0024 (projection persistence ports) | **Stands.** §8 tightens the DLQ port's docstring within 0024's structure; no interface change. |
| 0025 (legacy store retirement) | **Stands.** §7 replaces the safe-horizon *mechanism* the ports adapter shipped with, but the decision 0025 records (retire legacy stores onto the ports adapters, with no-skip protection) is unchanged; ADR 0027 cross-references it. |
| 0026 (outbox ring migration) | **Stands.** §1 corrects the SQLite schema 0026's split exposed; the per-backend adapter shape and payload contract are untouched. |
| **0027 (new)** | Schema correctness: (a) in-place replacement of the SQLite outbox schema and the exception criteria to append-only-by-file (a shipped schema that provably never worked with its only writer); (b) the `events.txid xid8` additive column, the NULL-is-safe argument, the per-batch horizon predicate, and the PostgreSQL 13 floor; (c) canonical-schema authority for `events.tenant_id` (UUID) and the retirement of hand-rolled test DDL. |
| **0028 (new)** | Cutover strictness and recovery: `cutover_max_lag_events` defaults to 0 (cutover never proceeds over provably-missing data by default; nonzero is explicit operator-accepted loss), paired with the `run_resync_pass` in-phase recovery API and the shared `_build_copier` seam that keeps position-mapper wiring uniform across copy entry points. |

Items 3, 8, 9, 10 are invariant-restoring bugfixes: CHANGELOG entries, no ADR.

## 12. Suggested task grouping for the implementation plan

| Task | Items | Rationale |
|---|---|---|
| **T1** | §1 (sqlite outbox schema) | P1, fully standalone; ships the xfail-drop + fixture retarget + two SQL edits + ADR 0027 draft (part a). First. |
| **T2** | §7 (xid8 horizon) | Isolated to the pg adapter + migrations machinery; needs `-m postgres`; ADR 0027 part (b); rewrites the re-bench backlog entry. |
| **T3** | §6 (tenant_id reconciliation) | **Blocked by T2** — the fixture must provision the fragment-carrying schema. ADR 0027 part (c). |
| **T4** | §4 + §5 (mapper wiring + resync API) | One task: they share the `_build_copier` seam and the dual-write test scaffolding; designed together so neither forecloses the other. ADR 0028 draft. |
| **T5** | §2 (cutover strict-0) | **Blocked by T4** — the strict default's story depends on `run_resync_pass` existing; the joint end-to-end test (resync → strict cutover) lives here. Finalizes ADR 0028 and the guide updates. |
| **T6** | §3 (live lag) | Standalone; `subscriptions/runners/live.py` + `subscription.py` docstrings. |
| **T7** | §9 (catch-up batch outcome) | Standalone; `subscriptions/runners/catchup.py` only. Parallel-safe with T6 (different runner files; both add tests under the same test dir — coordinate file names only). |
| **T8** | §8 (DLQ cutoff) | Standalone; memory adapter + port docstring + conformance suite. |
| **T9** | §10 (sqlite read lock) | Standalone; `adapters/sqlite/store.py` + one interleave test module. |

Dependency edges: T2 → T3; T4 → T5. All else parallelizable. CHANGELOG
entries accumulate per task; a final assembly pass (or T5, as the last
serialized task) verifies the CHANGELOG carries all nine user-facing changes
(§1, §2, §3, §4, §5, §7, §8, §9, §10 — §6 only if it surfaced an adapter
fix) and that ADRs 0027/0028 are complete with index.md updated.
