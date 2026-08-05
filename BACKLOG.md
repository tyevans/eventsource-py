# Backlog

Open work items, carried over from the retired `bd` (beads) tracker.

Everything below was verified open on 2026-08-04. Seventeen entries were
removed in that sweep -- some closed by the work in the same PR, most closed
by earlier work that never came back to update this file. What they were and
where they went is recorded at the bottom, because "why is this not here
anymore" is the question a backlog most often has to answer about itself.

## Delta-compress stored event payloads (P2)

Event payloads are stored verbatim. An aggregate that keeps a document in its
events — a file's contents, a rendered artifact, any state carried whole rather
than as a diff — rewrites the entire payload on every change, so storage grows
with (size x revisions) rather than with the size of the change.

Measured on a 500 KB payload over 200 revisions of small edits: 100.7 MB stored.
zstd against the previous payload of the same stream, with a self-contained
payload written whenever the delta stops being worth it, brought that to 130 KB
at a chain cap of 32 (774x), worst-case reconstruction 2.1 ms, every revision
byte-exact. Compressing each payload independently — no chain, no base — is only
13x, so the win is the dictionary, not the compressor.

The shape to copy is git's: snapshot semantics above, delta storage below, with
the delta layer invisible to the domain model and rebuildable from the
reconstructed payloads. Points worth carrying in from the research:

- Re-baseline on a size ratio (delta vs. fulltext, and cumulative chain vs.
  fulltext), not on a fixed interval — fixed-interval is the one strategy no
  production system uses. Mercurial's revlog rejects a delta once the chain
  would cost more than a few times the fulltext.
- Cap the chain independently of the ratio rule. The ratio alone never
  re-baselined in 200 revisions of small edits, which is a 200-long chain and a
  200-revision blast radius for one damaged row.
- Two checksums: the content hash of the reconstructed payload (proves a
  reconstruction is right) and a per-payload checksum (finds a bad row without
  replaying the chain — the job git's per-object CRC32 does).
- zstd frames already carry an XXH64 of the decompressed content and the
  decompressed size. VCDIFF specifies no integrity check at all.
- Base selection can be "the previous payload in this stream". Git's window
  heuristic exists only because git has no stream identity to key on; 2.51's
  path-walk mode moves toward what a stream-addressed store has natively.

**The blocker is the column type, and it is a public-contract decision.**
`payload` is `JSONB NOT NULL`, commented "stored as JSONB for flexible
querying", and the difference is observable: `docs/guides/repository-operations.md`
tells users PostgreSQL's JSONB column deserializes to a `dict` where SQLite and
InMemory do not. A compressed payload cannot be JSONB. So this cannot be a
quiet default — it needs a decision about whether compression is opt-in per
store, what happens to rows already written, and whether a store that opts in
gives up JSONB queryability as a documented consequence. (No shipped adapter
code queries into `payload` today, and the schema carries no GIN index on it,
so the capability is advertised rather than used internally — but users' own
SQL against the events table is outside our reach.)

Other open questions: what it does to the outbox and to any reader that touches
payload bytes directly, and whether the codec/version tag lives on the row or in
the payload header.

## Investigate making sqlalchemy an optional dependency (P3)

Investigate whether sqlalchemy can be moved from core deps to optional extras.
Its importers, per `grep -rlE '^(from|import) sqlalchemy' src/eventsource/`, are
now confined to two packages -- `adapters/postgresql/` (`locks.py`, `outbox.py`,
`readmodels.py`, `snapshots.py`, `store.py`) and the SQL adapter packages
(`adapters/_sql/{connection,dialect,engine}.py`, `adapters/sql/{checkpoints,dlq}.py`,
`adapters/sql/migration/*.py`), plus the Alembic templates under
`adapters/sql/schemas/`.

The entry's key question -- do any core interfaces import sqlalchemy at module
level? -- is now answered, and the answer is no: the Tier 0 import-linter
contract and `tests/unit/test_core_surface_purity.py` both enforce that they do
not. What remains is the packaging half: moving sqlalchemy behind an extra,
deciding what a plain install of `eventsource` can still do, and what the import
error reads like when it cannot. That is a release-notes-and-extras decision
rather than an investigation.

Prerequisite (done): drop redis from core dependencies.

## Deterministic or scheduled coverage for bus performance assertions (P3)

The kafka/rabbitmq metrics-overhead and duration tests assert wall-clock thresholds
and were excluded from the blocking broker-tests CI job (marked `@pytest.mark.benchmark`)
because shared runners make elapsed-time assertions nondeterministic (observed 55.79%
vs a 20% threshold on a green code path). Nothing in CI watches metrics overhead now.
Either rewrite the assertions as deterministic proxies (count instrumentation calls
rather than elapsed time) or add a scheduled, non-blocking benchmark workflow that
runs `-m benchmark` and reports results.

## Re-benchmark pg catch-up horizon predicate at scale (P2)

The slice-(b) completion bench (50k events, batch 500, no concurrent writers) measured
the ports pg adapter's read_all at +27% wall time vs the legacy no-horizon path
(0.8 ms/batch median) against the *inline* `xmin::text::bigint <
pg_snapshot_xmin(...)::text::bigint` predicate — acceptable at that scale, but
EXPLAIN ANALYZE showed that predicate defeating the `global_position` index
(Seq Scan + top-N heapsort instead of Index Scan), so per-batch cost was
O(table size). ADR 0027 part (b) has since replaced that predicate: it was also
wraparound-unsafe (32-bit `xmin` cast compared against a 64-bit epoch-extended
`xid8`, universally true past the first xid epoch), and the fix is exactly the
mitigation this entry proposed — `(txid IS NULL OR txid <
CAST(:txid_horizon AS text)::xid8)` against a new `events.txid` column, with the
horizon computed once per read in a separate query rather than inlined per row.

The re-bench is still warranted at 1M+ rows under concurrent writers — the exact
conditions spec §11 risk 1 (legacy-store-retirement design) names — but must be
re-run against the new predicate shape, not re-read from the old numbers: a bound
parameter against an indexed column is a materially different query plan than a
volatile inline expression, and whether the planner now keeps the `global_position`
index path is exactly what needs re-measuring. Prior methodology + numbers (now
describing a predicate that no longer exists): session artifact
pg-catchup-bench.md (2026-07-31).

## Small ring-consistency cleanups (P3)

Batch of low-risk campaign leftovers (2026-07-31): (a) consider renaming
`testing/conformance_ports/` now that it is the only conformance package —
folding it into `testing/conformance.py`'s namespace or documenting the split;
(b) move the tracing decorator used by adapters up to a ports-level helper so
adapters stop importing `observability/` internals directly; (c) consolidate
`protocols.py`'s remaining ABCs/Protocols with their ring homes (several now
duplicate `ports/` definitions in spirit) with deprecation re-exports; (d) the
readmodels port-purity `ast`-based test (verifying `ports/readmodels/` imports
no sqlalchemy) is blind to relative imports — none exist in that package
today, so this is not a live gap, but tighten the test if `ports/` ever
adopts relative imports (structure slice A, 2026-07-31). Found by the A6
engine-move task (structure slice A, 2026-07-31), out of scope there by the
task's own boundary.

Sub-item (e) -- `scripts/_mutmut_configure.py`'s selectors pointing at the
deleted `repositories/` package -- is closed (2026-08-04). The selectors are
retargeted and `tests/unit/test_mutmut_configure.py` now fails when one goes
stale or when `mutation.sh`'s selector list drifts from the Python table.

## given_events supports one aggregate per scenario (P3)

The testing harness's `given_events` seeds a single aggregate stream per
scenario; multi-aggregate scenarios need manual store setup. Behavior is
documented in docs/tutorials/08-testing.md (~line 323); this entry exists so
the limitation is tracked as improvable, closing a ledger note from the
aggregates slice (2026-07-31).

## Buffered live events dropped on stop/transition-failure leave lag inflated (P3)

`subscriptions/transition.py` `_cleanup` (~392-402) and the live runner's
`stop()` drop the transition/pause buffers without calling
`record_events_unseen` for the buffered events' seen-receipts (recorded at bus
receipt since the live-lag fix), violating the reconciliation contract stated
in `Subscription.lag`'s docstring. Bounded consequence: no in-tree path
auto-restarts the same Subscription object, and nonzero lag on a stopped/ERROR
subscription is directionally truthful — but re-calling manager.start() on the
same registry objects inherits permanently inflated lag from the re-read. Fix:
unsee `qsize()` on buffer drop, or reset both counters at transition start.
Found by the correctness-fixes slice final review (2026-07-31).

---

# Audit findings, 2026-08-02

Surfaced by three parallel read-only audits (`adapters/`, `application/`, public
API + DX); 28 findings, three Criticals fixed on the day (PRs #112, #113, #114)
and most of the rest closed by the ADR-0048 sweep (see
`docs/adrs/0048-failure-paths-report-and-retain.md`). What remains below is what
that sweep deliberately did not take on: work that is a feature or a refactor
rather than a defect fix.

## Cutover's routing switch needs a real transaction, not compensation (P2)

`set_routing()` and `set_migration_state(MIGRATED)` are still two writes. The
ADR-0048 sweep made them safe rather than atomic: a failure between them now
reverts the route before propagating, and `_rollback` restores the route as
well as the state (it previously declared the tenant `DUAL_WRITE` while leaving
every request on the target store). Compensation is what the port allows —
`TenantRoutingRepository` exposes no multi-statement transaction — so a process
crash between the two writes still leaves the pair inconsistent until an
operator intervenes. A genuinely atomic switch needs the port to grow a
transaction boundary.

## SubscriptionManager, shutdown.py, and migration/coordinator.py have outgrown their modules (P3)

CONFIRMED by line count. Structural, not a defect.

## Kafka and RabbitMQ retries block while they back off (P2)

Both consumers now honor the same `RetryPolicy` (ADR 0048) by sleeping before
processing a republished message: RabbitMQ inline in the consumer, Kafka before
`_process_message` proceeds. That ends the divergence where Kafka ignored the
backoff entirely, but it means a retrying message holds up its partition (Kafka)
or the consumer (RabbitMQ) for the duration. A genuinely non-blocking delay
needs a dedicated retry topic plus a scheduler on Kafka, and a TTL + dead-letter
delay queue on RabbitMQ. Deliberately deferred by the ADR-0048 sweep as a
feature, not a bug fix.

## SQLite has no transactional outbox; PostgreSQL does (P2)

`adapters/sqlite/store.py` has no `outbox_enabled`; `adapters/postgresql/store.py`
does. A user developing on SQLite and deploying on PostgreSQL gets different
delivery guarantees from identical code. CONFIRMED. The ADR-0048 sweep
documented the gap in the event-bus guide rather than closing it: implementing
it is a schema and transaction-boundary change. Still worth implementing.

---

# Closed, 2026-08-04

Verified against the tree rather than taken on the entry's word. Two shapes:
work this sweep did, and work already done whose entry outlived it -- the
second being the larger group, and the reason a stale backlog costs more than
an empty one.

**Closed by this sweep** (see ADRs 0050 and 0051):

- *InMemoryReadModelRepository aliases live objects* — reads now hand back a
  copy and writes take one; pinned for all three backends in
  `ReadModelRepositoryConformance` rather than the memory adapter's own tests.
- *Resolve the duplicate `OptimisticLockError` name* — the read-model one is
  `ReadModelVersionConflictError` (ADR 0050). No alias, per the no-shim policy.
- *Share ExpectedVersion dispatch across store adapters* — `adapters/_common/`
  (ADR 0051); four verbatim copies down to one definition.
- *Add CI boundary check for core surface purity* — the runtime `sys.modules`
  check exists as `tests/unit/test_core_surface_purity.py`, and the static
  contract's driver list widened from `sqlalchemy` alone to six. The entry
  asked whether the runtime check added anything over the static one; it does,
  and the answer is written up in `docs/core-surface.md`.
- *Document store_id uniqueness expectations* — documented on `Position` and
  at each adapter default. The suggested "derive the pg default from
  host+port+database" is deliberately **not** done: `store_id` is embedded in
  every persisted position, so changing a default invalidates the checkpoints
  of every deployment that took it.
- *Differentiate run_resync_pass's two zero-return shapes in logs* — done.

**Already closed; the entry was stale.** Each verified against the tree:

- *SQLite adapter: reads share the writer's connection* — fixed; reads run
  under the append lock, pinned by `tests/unit/adapters/test_sqlite_read_isolation.py`.
- *Reconcile DLQ delete_resolved_events cutoff semantics* — both backends use
  the rolling instant, and `ports/dlq.py` specifies it.
- *Catch-up can end early with completed=False* — `_BatchOutcome` carries
  `envelopes_read` alongside `events_delivered`, which is the fix the entry
  proposed.
- *Wire position_mapper into BulkCopier's coordinator call site* — one
  `_build_copier` now serves both call sites.
- *No in-phase resync for dual-write mirror failures* — `run_resync_pass`
  exists.
- *Cutover can switch routing with up to cutover_max_lag_events missing* —
  `cutover_max_lag_events` defaults to 0 (strict).
- *Relocate subscriptions/ into the application ring* — done (ADR 0032); no
  top-level `subscriptions/` remains. Note the entry called for deprecation
  shims, which the no-shim policy had already overtaken.
- *Remove bus facade compat shims* / *Migrate bus/ to ports/adapters* — ADR
  0031. Both entries already said so in their own bodies.
- *Remove the locks/readmodels deprecation shims* — ADR 0030, likewise.
- *Decide engine.py's ring placement* — ADR 0029, likewise.
