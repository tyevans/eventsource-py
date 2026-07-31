# Correctness Fixes Slice

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the ten ruled correctness fixes from the design spec — a SQLite outbox schema that has never been writable, a cutover that can switch routing over provably-missing events, a live-phase lag signal that is structurally zero, two documented-True config defaults that are dead on arrival, a dual-write dead end with no in-phase recovery, test DDL that has drifted from the canonical schema, a safe-horizon predicate that fails open after xid epoch wraparound, a DLQ cleanup cutoff that two adapters disagree about, a catch-up loop that reports failure on an all-filtered batch, and SQLite reads that can observe a half-committed append. After this slice, `grep -rn "AUTOINCREMENT" src/eventsource/migrations/templates/sqlite/outbox.sql` is empty, no `xmin` reference remains in `src/eventsource/adapters/postgresql/`, `MigrationConfig().cutover_max_lag_events == 0`, and `MigrationCoordinator.run_resync_pass` exists.

**Architecture:** No ring boundaries move. Seven of the ten fixes are adapter-internal or use-case-internal edits; two are schema artifacts under `migrations/`; one adds a single public coordinator method. The one deliberate structural seam is `MigrationCoordinator._build_copier(migration, target_store=None)`, introduced in T4 so that the automated bulk-copy path and the new operator-triggered `run_resync_pass` can never diverge on position-mapper wiring. Two new ADRs record the decisions: 0027 (schema correctness — the sanctioned in-place SQLite replacement, the `events.txid xid8` column, and canonical-schema authority for `events.tenant_id`) and 0028 (cutover strictness paired with in-phase recovery).

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), asyncpg, aiosqlite, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter.

**Spec:** `docs/superpowers/specs/2026-07-31-correctness-fixes-design.md` (in full). Read the section named in each task before starting it.

**Baseline:** authored against the tree at commit `dc16938`. All line numbers below were read from that tree, not inferred. If a line number does not match what you find, re-read the file and proceed on the code, not the number — but if the *code* differs, stop and report.

## Global Constraints

- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>`, no trailing period — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Path-scoped `git add` only.** Other agents work concurrently in this worktree. Never `git add -A`, never `git add .`; stage exactly the files the task names, and prefer `git commit --only <paths>`. On `index.lock` contention, wait 5 seconds and retry.
- **Implementers do not push.** Commit only. Branch pushes and PRs are the orchestrator's.
- **Every task is independently green.** The full unit suite (`make test`) must pass at every task boundary; the orchestrator gates each one. The spec sanctions **no** red handoffs in this slice — if you find yourself wanting to leave a test failing "for the next task," you have misread the plan; stop and report.
- **`migrations/` SQL files are append-only BY FILE**, with exactly two sanctioned exceptions in this slice, both spelled out in the spec:
  - T1 replaces `id INTEGER PRIMARY KEY AUTOINCREMENT` with `id TEXT PRIMARY KEY` **in place** in `migrations/templates/sqlite/outbox.sql` and `migrations/schemas/sqlite_all.sql` (spec §1: the table provably cannot hold a row written by its only shipped writer, and SQLite cannot ALTER a primary key's type, so no additive fragment can express the repair).
  - T2 adds a **new** additive fragment `migrations/additive/events_txid.sql` plus a **new** operator script `migrations/updates/004_add_events_txid.sql`. Neither is an edit to a base file.

  Any other apparent need to edit a base schema file means the design is wrong: stop and report.
- **Positions are opaque.** Compare and persist (`to_str`/`from_str`); never subtract, never compare to an int. T6 and T9 both touch lag/feed code that could tempt position arithmetic — the spec explicitly refuses it (ADR 0019 stands).
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Red/green TDD.** Every task writes or unmasks its failing test first, observes the failure with the stated symptom, then implements. Steps are ordered so the red step precedes the green one; do not reorder them. Where the spec says a red test already exists (T1's xfail, T3's fixture flip), the red step is unmasking it.
- **Property tests only where the spec requires them.** This slice requires **none**. Do not add speculative hypothesis suites, and do not extend the existing ones (`tests/unit/adapters/test_memory_stateful.py`, `test_memory_dlq_properties.py`, `test_memory_checkpoints_properties.py`, `tests/unit/migration/test_bulk_copy_resume_property.py`) — they must stay green untouched.
- **Conformance-suite additions where the spec moves cases there.** Two moves, both mandatory: T1 un-xfails `TestSQLiteOutboxRepository` so the outbox suite runs against the real schema, and T8 adds the day-zero DLQ cleanup case to `testing/conformance_ports/dlq.py` while deleting the two per-backend divergence tests. Do not add conformance cases the spec does not name.
- **CHANGELOG obligations accumulate per task.** Nine of the ten items are user-facing (§6 only if it surfaces an adapter fix). Each task writes its own entry under `## [Unreleased]` in the section the spec names (Added / Changed / Fixed). T5, as the last serialized task, verifies the full set is present.
- **ADRs land in the task that completes their subject matter.** ADR 0027 spans T1 (part a), T2 (part b), and is **completed and accepted in T3** (part c) — T3 also adds its `docs/adrs/index.md` row. ADR 0028 is drafted in T4 and **completed and accepted in T5**, which adds its index row and the "Amended by ADR 0028" pointer on `docs/adrs/0014-live-migration-cutover-semantics.md` (whose §"Why `cutover_timeout_ms` is a hard bound" text at `:352` states the old default of 100). ADR bodies are immutable records — while 0027 and 0028 are new in this slice they may be built up across their own tasks, but no existing ADR's Decision is rewritten.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- **import-linter green per task, not only at the end.** `uv run lint-imports` is in every task's verify step.
- **No live-src mutation probes.** Do not "try an edit and see what mypy says" on shared source while other agents are active. Reason from the code you read; experiment on a copy under `$CLAUDE_JOB_DIR/tmp`.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds, which means **every task must leave the tree importable and type-clean**.

### Dependency order

```
T1 (standalone, first)
T2 ──▶ T3          (T3's fixture must provision the fragment-carrying events schema)
T4 ──▶ T5          (strict-0's story and its joint end-to-end test need run_resync_pass)
T6, T7, T8, T9     (parallel-safe, any order)
```

T6 and T7 touch different runner modules (`runners/live.py` vs `runners/catchup.py`) and add test modules with distinct names (`test_live_runner_lag.py`, `test_catchup_batch_outcome.py`) — coordinate on nothing else.

### Integration-test tasks

T2 and T3 need a live PostgreSQL. Bring it up once:

```bash
docker compose -f docker-compose.test.yml up -d
```

and run the marked suite with `uv run pytest tests/integration/ -m postgres -v`. If Docker is unavailable in your environment, complete the unit-level steps, run the integration steps as far as they go, and **report the gap explicitly** rather than claiming the acceptance criteria met.

---

### Task 1: SQLite outbox schema — replace the unusable primary key

Spec §1. P1, fully standalone. The shipped SQLite outbox table declares `id INTEGER PRIMARY KEY AUTOINCREMENT` — SQLite's strictly-typed rowid alias — while `SQLiteOutboxRepository.add_event` inserts `str(uuid4())`, so every insert against the shipped schema raises `sqlite3.IntegrityError: datatype mismatch`. The defect is masked twice: by a hand-rolled fixture and by a non-strict xfail.

**Files:**
- Modify: `src/eventsource/migrations/templates/sqlite/outbox.sql`
- Modify: `src/eventsource/migrations/schemas/sqlite_all.sql` (the `event_outbox` block only)
- Modify: `tests/conftest.py` (`sqlite_outbox_repo` fixture)
- Modify: `tests/unit/adapters/test_sqlite_conformance.py` (drop the xfail)
- Create: `docs/adrs/0027-schema-correctness-fixes.md` (part (a) only; Status: Proposed)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `eventsource.migrations.get_schema("outbox", backend="sqlite")` — unchanged signature.
- Produces: nothing consumed by another task. `get_schema("outbox", backend="sqlite")` now returns a schema whose `id` column accepts the adapter's `str(uuid4())`.

- [ ] **Step 1 (red): remove the xfail that institutionalizes the defect**

In `tests/unit/adapters/test_sqlite_conformance.py`, delete the entire `@pytest.mark.xfail(...)` decorator block immediately above `class TestSQLiteOutboxRepository(OutboxRepositoryConformance):` (the block beginning `@pytest.mark.xfail(` and ending `    strict=False,\n)`). Leave the class body untouched.

Run: `uv run pytest tests/unit/adapters/test_sqlite_conformance.py -k Outbox -q`

Expected: FAIL. Every test whose path calls `add_event` errors with `sqlite3.IntegrityError: datatype mismatch`. (`test_unknown_outbox_id_is_a_no_op_for_mutating_methods` never calls `add_event` and passes — that asymmetry is why the xfail was non-strict.)

- [ ] **Step 2 (green): correct the template**

In `src/eventsource/migrations/templates/sqlite/outbox.sql`, replace these two lines inside `CREATE TABLE IF NOT EXISTS event_outbox (`:

```sql
    -- Auto-incrementing ID for ordering
    id INTEGER PRIMARY KEY AUTOINCREMENT,
```

with:

```sql
    -- Outbox entry identity (UUID as TEXT, 36 characters, hyphenated).
    -- No DEFAULT: SQLite has no native UUID generator to mirror
    -- PostgreSQL's `gen_random_uuid()`, and the adapter always supplies
    -- the id. Ordering comes from `created_at`, which
    -- `get_pending_events` already orders by -- not from a rowid.
    id TEXT PRIMARY KEY,
```

Change nothing else in the file: no other column, no index, no CHECK constraint.

- [ ] **Step 3 (green): correct the combined schema identically**

In `src/eventsource/migrations/schemas/sqlite_all.sql`, inside the `-- 2. Event Outbox Table` block, replace:

```sql
    id INTEGER PRIMARY KEY AUTOINCREMENT,
```

with:

```sql
    id TEXT PRIMARY KEY,
```

No other table in either file changes.

Run: `uv run pytest tests/unit/adapters/test_sqlite_conformance.py -k Outbox -q` — Expected: PASS.

- [ ] **Step 4 (green): retarget the fixture onto the real schema**

In `tests/conftest.py`, in the `sqlite_outbox_repo` fixture, delete the hand-rolled DDL (the `await sqlite_connection.execute("""CREATE TABLE IF NOT EXISTS event_outbox (...)""")` call and its `await sqlite_connection.commit()`) and provision from the migration instead:

```python
    from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
    from eventsource.migrations import get_schema

    await sqlite_connection.executescript(get_schema("outbox", backend="sqlite"))
    await sqlite_connection.commit()

    repo = SQLiteOutboxRepository(sqlite_connection)
    yield repo
```

Also update the fixture docstring's "Creates the event_outbox table in the in-memory database" line to say it provisions from `get_schema("outbox", backend="sqlite")` — the point of the change is that the fixture and production now share one schema.

Run: `uv run pytest tests/unit/adapters/test_sqlite_outbox.py tests/unit/adapters/test_sqlite_conformance.py -q` — Expected: PASS.

- [ ] **Step 5: verify**

```bash
grep -rn "AUTOINCREMENT" src/eventsource/migrations/templates/sqlite/outbox.sql \
                         src/eventsource/migrations/schemas/sqlite_all.sql
```
Expected: no matches.

```bash
grep -n "CREATE TABLE" tests/conftest.py | grep -i outbox
```
Expected: no matches.

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/ tests/conftest.py tests/unit/adapters/test_sqlite_conformance.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 6: ADR 0027, part (a)**

Create `docs/adrs/0027-schema-correctness-fixes.md`. Read `docs/adrs/0026-outbox-ring-migration.md` first for house structure (Status / Context / Decision / Consequences, with a Rejected Alternatives section). Write only part (a) now; T2 appends part (b) and T3 appends part (c) and flips the Status.

Content to land in this task:

- **Status:** `Proposed` — with an explicit note: "Built across the correctness-fixes slice; parts (b) and (c) land in the same slice's later tasks. Accepted when part (c) lands."
- **Context (part a):** `migrations/templates/sqlite/outbox.sql` declared `id INTEGER PRIMARY KEY AUTOINCREMENT`; `SQLiteOutboxRepository.add_event` inserts `str(uuid4())`; SQLite's `INTEGER PRIMARY KEY` is the strictly-typed rowid alias, so every insert since the file first shipped raised before commit. The same table body was embedded in `migrations/schemas/sqlite_all.sql`, which `SQLiteEventStore._conn()` applies on first connection.
- **Decision (part a):** replace `id INTEGER PRIMARY KEY AUTOINCREMENT` with `id TEXT PRIMARY KEY` **in place** in both files. Record the exception criterion precisely: *a shipped schema that provably never worked with its only shipped writer may be corrected in place; everything else under `migrations/` remains append-only by file.* State the three supporting facts — the table cannot hold a row the library wrote; a deployment writing integer ids there is not using `SQLiteOutboxRepository`; SQLite cannot ALTER a primary key's type, so the additive-fragment mechanism cannot express the repair.
- **Rejected alternatives (part a):** adapter surfaces the rowid (breaks the `OutboxEntry.id: UUID` port type and PostgreSQL parity); ship a second schema file (`get_schema` would still return the broken one); WONTFIX with the fixture as de-facto schema (the shipped migration is the product).
- **Consequences (part a):** existing SQLite databases carry an empty, unusable `event_outbox`; `CREATE TABLE IF NOT EXISTS` will not replace it, so operators must `DROP TABLE event_outbox` and re-provision. No data can have existed in it.

Do **not** add the `docs/adrs/index.md` row yet — that lands with the Accepted status in T3.

- [ ] **Step 7: CHANGELOG**

Add under `## [Unreleased]` → `### Fixed`:

```markdown
- **SQLite outbox schema corrected: `event_outbox.id` is now `TEXT PRIMARY KEY`, was `INTEGER PRIMARY KEY AUTOINCREMENT`.** `SQLiteOutboxRepository.add_event` inserts a `str(uuid4())` into that column, which SQLite's strictly-typed rowid alias rejects with `sqlite3.IntegrityError: datatype mismatch` — so every insert against the shipped schema failed, and the table has never held a row written by this library. **Migration note:** an existing SQLite database provisioned from `migrations/templates/sqlite/outbox.sql` or `migrations/schemas/sqlite_all.sql` carries an empty, unusable `event_outbox` table. `CREATE TABLE IF NOT EXISTS` will not replace it — run `DROP TABLE event_outbox;` and re-provision from the corrected schema. No data can be lost: none can have existed. See ADR 0027.
```

- [ ] **Step 8: commit**

```bash
git add src/eventsource/migrations/templates/sqlite/outbox.sql \
        src/eventsource/migrations/schemas/sqlite_all.sql \
        tests/conftest.py tests/unit/adapters/test_sqlite_conformance.py \
        docs/adrs/0027-schema-correctness-fixes.md CHANGELOG.md
git commit --only src/eventsource/migrations/templates/sqlite/outbox.sql \
        src/eventsource/migrations/schemas/sqlite_all.sql \
        tests/conftest.py tests/unit/adapters/test_sqlite_conformance.py \
        docs/adrs/0027-schema-correctness-fixes.md CHANGELOG.md \
        -m "fix: sqlite outbox id column must be text, not an autoincrement rowid"
```

Record in the commit body: that this is the append-only exception ADR 0027 part (a) sanctions, and that the fixture now provisions from `get_schema` so the migration is under test.

---

### Task 2: Wraparound-safe PostgreSQL feed horizon (`events.txid xid8`)

Spec §7. The current predicate compares a 32-bit `xmin` cast to bigint against an epoch-extended 64-bit `xid8`; once a cluster crosses its first xid epoch the right side permanently exceeds 2^32, the predicate becomes universally true, and the no-skip protection silently vanishes — in exactly the high-write-volume deployments that need it.

**Files:**
- Create: `src/eventsource/migrations/additive/events_txid.sql`
- Create: `src/eventsource/migrations/updates/004_add_events_txid.sql`
- Modify: `src/eventsource/migrations/__init__.py` (`_ADDITIVE_FRAGMENTS` only)
- Modify: `src/eventsource/adapters/postgresql/store.py`
- Create tests: `tests/unit/adapters/test_postgresql_feed_horizon.py`
- Modify tests: `tests/unit/migrations/test_additive_schema.py`
- Modify: `tests/integration/adapters/test_postgresql_conformance.py` (one new NULL-txid case)
- Modify: `docs/adrs/0027-schema-correctness-fixes.md` (append part (b))
- Modify: `BACKLOG.md` (rewrite the re-bench entry's premise)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `_ADDITIVE_FRAGMENTS` in `migrations/__init__.py`, and the existing `get_schema(name, backend, *, additive=True)` composition path.
- Produces (**consumed by T3**): `get_schema("events")` and `get_schema("all")` now return text that carries the `txid` fragment; `get_schema("events", additive=False)` does not. T3's `postgres_engine` fixture provisions from the former, so T3 must not run before this task.

- [ ] **Step 1 (red): pin the predicate shape and the fragment composition**

Create `tests/unit/adapters/test_postgresql_feed_horizon.py`. Read `tests/unit/adapters/test_postgresql_outbox.py` first for house style (module docstring, `from __future__ import annotations`, mocked-session construction).

```python
"""The PostgreSQL feed's safe-horizon predicate is wraparound-safe.

`xmin` is a 32-bit xid whose textual value wraps at 2^32, while
`pg_snapshot_xmin(pg_current_snapshot())` returns an epoch-extended
64-bit `xid8`. Comparing them makes the predicate universally true once a
cluster crosses its first xid epoch -- fail-open, in exactly the
high-write-volume deployments that need the no-skip guard. The predicate
now filters on the `events.txid xid8` column against a horizon bound once
per batch. True epoch wraparound cannot be reproduced in a
testcontainer, so these query-shape assertions are the regression guard;
ADR 0027 records the reasoning.
"""

from __future__ import annotations

from pathlib import Path

from eventsource.adapters.postgresql import store as pg_store
from eventsource.migrations import get_all_schemas, get_schema


class TestHorizonPredicateShape:
    def test_predicate_filters_on_the_txid_column(self) -> None:
        assert "txid IS NULL OR txid <" in pg_store._HORIZON_PREDICATE

    def test_predicate_binds_the_horizon_as_a_parameter(self) -> None:
        assert ":txid_horizon" in pg_store._HORIZON_PREDICATE

    def test_no_xmin_cast_remains_in_the_adapter_module(self) -> None:
        source = Path(pg_store.__file__).read_text()
        assert "xmin" not in source


class TestTxidReachesComposedSchemas:
    def test_events_schema_carries_the_column(self) -> None:
        assert "txid" in get_schema("events")

    def test_partitioned_events_schema_carries_the_column(self) -> None:
        assert "txid" in get_schema("events_partitioned")

    def test_all_schema_carries_the_column(self) -> None:
        assert "txid" in get_all_schemas()

    def test_base_schema_alone_does_not_carry_the_column(self) -> None:
        assert "txid" not in get_schema("events", additive=False)

    def test_sqlite_schemas_do_not_carry_the_column(self) -> None:
        # SQLite is a single serialized writer with no horizon predicate.
        assert "txid" not in get_all_schemas(backend="sqlite")
```

Run: `uv run pytest tests/unit/adapters/test_postgresql_feed_horizon.py -q`

Expected: FAIL — `AssertionError` on every case in both classes (`_HORIZON_PREDICATE` still holds the `xmin::text::bigint` form; no fragment is registered).

- [ ] **Step 2 (green): the additive fragment**

Create `src/eventsource/migrations/additive/events_txid.sql`:

```sql
-- Additive fragment: wraparound-safe transaction id for the global feed
-- horizon. Appended to the events schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
--
-- The feed's no-skip guard defers rows whose inserting transaction is not
-- yet definitely-committed. It used to read the `xmin` system column, a
-- 32-bit xid, and compare it against `pg_snapshot_xmin(...)`, an
-- epoch-extended 64-bit `xid8` -- a comparison that becomes universally
-- true once the cluster crosses its first xid epoch. `xid8` does not wrap
-- on any human timescale, so the column makes the guard permanent.
--
-- Two statements, deliberately: `ADD COLUMN` carrying a volatile DEFAULT
-- forces a full table rewrite. Adding the column nullable and without a
-- default is metadata-only; `SET DEFAULT` afterwards applies to future
-- inserts only.
--
-- NULL semantics: a NULL `txid` row predates this ALTER. `ALTER TABLE`
-- takes ACCESS EXCLUSIVE, so any transaction that inserted such a row
-- finished before every post-migration snapshot -- NULL rows are always
-- definitely-committed and always safe to read.
--
-- Requires PostgreSQL 13 (`xid8`, `pg_current_xact_id`) -- the same floor
-- `pg_current_snapshot()` already imposed.
ALTER TABLE events ADD COLUMN IF NOT EXISTS txid xid8;

ALTER TABLE events ALTER COLUMN txid SET DEFAULT pg_current_xact_id();
```

`events_partitioned.sql` also creates a table named `events` (verified: `CREATE TABLE IF NOT EXISTS events (...) PARTITION BY RANGE`), so the same fragment text serves it — no second file.

- [ ] **Step 3 (green): register the fragment**

In `src/eventsource/migrations/__init__.py`, extend `_ADDITIVE_FRAGMENTS` (currently lines 68-74) to:

```python
_ADDITIVE_FRAGMENTS: dict[tuple[str, str], tuple[str, ...]] = {
    ("checkpoints", "postgresql"): ("checkpoints_position_token",),
    ("checkpoints", "sqlite"): ("checkpoints_position_token",),
    ("all", "postgresql"): ("checkpoints_position_token", "events_txid"),
    ("all", "sqlite"): ("checkpoints_position_token",),
    ("migration", "postgresql"): ("migration_position_tokens",),
    ("events", "postgresql"): ("events_txid",),
    ("events_partitioned", "postgresql"): ("events_txid",),
}
```

Note the ordering inside `("all", "postgresql")`: fragments are appended in tuple order after the base text, and the base `schemas/all.sql` creates `events` before `projection_checkpoints`, so either order works — keep `events_txid` last to match the tuple's read order against the file.

- [ ] **Step 4 (green): the operator script**

Create `src/eventsource/migrations/updates/004_add_events_txid.sql`. Read `updates/002_add_position_token.sql` first for the header style.

```sql
-- =============================================================================
-- Migration: Add txid to events
-- For: PostgreSQL 13+
-- Version: 004
-- =============================================================================
-- REQUIRED before upgrading: the global feed read path
-- (`read_all` / `current_position` in the PostgreSQL adapter) now
-- references `events.txid`, and fails loudly without it.
--
-- Replaces the old `xmin::text::bigint < pg_snapshot_xmin(...)::text::bigint`
-- safe-horizon predicate, which silently became universally true -- losing
-- the no-skip guarantee -- once a cluster crossed its first xid epoch,
-- because `xmin` is a 32-bit xid and `pg_snapshot_xmin` returns a
-- 64-bit epoch-extended `xid8`.
--
-- Two statements, deliberately. `ADD COLUMN` with a volatile DEFAULT in a
-- single statement forces a full rewrite of the events table -- unacceptable
-- on a production event store. Adding the column nullable and without a
-- default is a metadata-only catalog change; the `SET DEFAULT` that follows
-- applies to future inserts only and rewrites nothing.
--
-- Rows left with a NULL txid predate this migration and are always safe to
-- read: `ALTER TABLE` takes ACCESS EXCLUSIVE, so every transaction that
-- inserted one had already finished before any post-migration snapshot.
-- No backfill, and deliberately no NOT NULL.
--
-- This is an idempotent migration - safe to run multiple times.
-- =============================================================================

ALTER TABLE events ADD COLUMN IF NOT EXISTS txid xid8;

ALTER TABLE events ALTER COLUMN txid SET DEFAULT pg_current_xact_id();

COMMENT ON COLUMN events.txid IS 'Inserting transaction id (xid8) for the wraparound-safe global feed horizon; NULL for rows predating this migration';
```

Run: `uv run pytest tests/unit/adapters/test_postgresql_feed_horizon.py::TestTxidReachesComposedSchemas -q` — Expected: PASS.

- [ ] **Step 5 (green): restructure the adapter predicate**

In `src/eventsource/adapters/postgresql/store.py`, replace the comment block and constant at lines 72-80 with:

```python
# Rows whose inserting transaction is not yet definitely-committed are
# deferred to a later poll -- the global_position sequence commits out of
# order under concurrent writers, and reading past a still-uncommitted
# lower position would skip it forever once the reader resumes from
# higher up.
#
# The horizon is fetched once per read (a scalar query on the same
# session) and bound as a parameter, rather than inlined as a volatile
# expression. Two reasons: `xid8` comparison is native 64-bit and does not
# wrap on any human timescale (the old `xmin::text::bigint` form compared a
# 32-bit xid against an epoch-extended 64-bit value, and went universally
# true -- fail-open -- once a cluster crossed its first xid epoch); and a
# bound parameter is a plain filter the planner can reason about, where
# the inline volatile expression was not.
#
# `txid IS NULL` rows predate `migrations/updates/004_add_events_txid.sql`.
# `ALTER TABLE` takes ACCESS EXCLUSIVE, so every transaction that inserted
# one finished before any post-migration snapshot: NULL is always
# definitely-committed and always safe to read.
#
# Databases that have not applied 004 fail loudly here with an undefined
# column, by design -- a silent fallback to the old predicate would keep
# the wraparound-unsafe path alive forever.
_HORIZON_PREDICATE = "(txid IS NULL OR txid < CAST(:txid_horizon AS xid8))"

# Rendered to text so the value crosses the driver as a plain string and
# is cast back server-side; asyncpg has no native xid8 codec.
_HORIZON_QUERY = "SELECT pg_snapshot_xmin(pg_current_snapshot())::text"
```

In `_do_read_all`, the query is assembled before the session opens and executed inside it. Fetch the horizon on the same session, immediately before the feed query, and bind it:

```python
        async with self._session_factory() as session:
            horizon = (await session.execute(text(_HORIZON_QUERY))).scalar_one()
            params["txid_horizon"] = horizon
            result = await session.execute(text("\n".join(query_parts)), params)
            rows = result.mappings().all()
```

In `current_position`, do the same:

```python
        async with self._session_factory() as session:
            horizon = (await session.execute(text(_HORIZON_QUERY))).scalar_one()
            result = await session.execute(
                text(
                    "SELECT MAX(global_position) FROM events"  # nosec B608 -- constant predicate
                    f" WHERE {_HORIZON_PREDICATE}"
                ),
                {"txid_horizon": horizon},
            )
            value = result.scalar()
```

Leave `read_category` alone — category reads are storage-time inclusive and carry no horizon today.

Finally, update the module docstring's safe-horizon paragraph (lines 10-15) to say the predicate filters on the `events.txid` column against a per-read horizon, and to name `migrations/updates/004_add_events_txid.sql` as the operator prerequisite.

Run: `uv run pytest tests/unit/adapters/test_postgresql_feed_horizon.py -q` — Expected: PASS.

- [ ] **Step 6 (green): extend the additive-schema base-file guard**

`tests/unit/migrations/test_additive_schema.py::TestPositionTokenReachesComposedSchemas::test_base_schema_files_are_unmodified` pins that a column arrives by fragment, never by an edited base file. Add the sibling class for this fragment at the end of the module:

```python
class TestEventsTxidReachesComposedSchemas:
    """The feed-horizon column arrives by fragment, PostgreSQL only."""

    def test_base_events_files_are_unmodified(self) -> None:
        from eventsource.migrations import _SCHEMAS_DIR, _TEMPLATES_DIR

        for path in (
            _SCHEMAS_DIR / "all.sql",
            _SCHEMAS_DIR / "events.sql",
            _TEMPLATES_DIR / "events.sql",
            _TEMPLATES_DIR / "events_partitioned.sql",
        ):
            assert "txid" not in path.read_text(), path

    def test_operator_script_exists_with_the_split_alter(self) -> None:
        from eventsource.migrations import _PACKAGE_DIR

        script = (_PACKAGE_DIR / "updates" / "004_add_events_txid.sql").read_text()
        assert "ADD COLUMN IF NOT EXISTS txid xid8" in script
        assert "ALTER COLUMN txid SET DEFAULT pg_current_xact_id()" in script
        assert "rewrite" in script  # the rationale for splitting the two statements
```

Run: `uv run pytest tests/unit/migrations/test_additive_schema.py -q` — Expected: PASS.

- [ ] **Step 7 (integration): pre-migration rows stay readable**

In `tests/integration/adapters/test_postgresql_conformance.py`, add one case to the `TestPostgreSQLGlobalFeed` class body (after its `store` fixture):

```python
    @pytest.mark.postgres
    async def test_rows_with_null_txid_are_returned_by_read_all(
        self, store: PostgreSQLEventStore
    ) -> None:
        """A row predating updates/004 has a NULL txid and is always safe.

        `ALTER TABLE` takes ACCESS EXCLUSIVE, so any transaction that
        inserted such a row finished before every post-migration snapshot.
        """
        from eventsource.ports import collect

        stream = StreamId(aggregate_id=uuid4(), category="Order")
        await store.append(stream, [_make_event(stream)], ExpectedVersion.no_stream())

        async with store._engine.begin() as conn:  # type: ignore[attr-defined]
            await conn.execute(text("UPDATE events SET txid = NULL"))

        envelopes = await collect(store.read_all())

        assert len(envelopes) == 1
```

Adapt the event construction and the engine handle to whatever the module's existing cases use — read `_fresh_store` and the neighbouring `TestPostgreSQLGlobalFeed` cases and reuse their helpers rather than inventing new ones. If the module has no event factory in scope, import the one the conformance suites use (`eventsource.testing.conformance_ports._fixtures.make_event`).

```bash
docker compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/adapters/ -m postgres -q
```
Expected: PASS, including the existing no-skip conformance cases.

- [ ] **Step 8: verify**

```bash
grep -rn "xmin" src/eventsource/adapters/postgresql/
```
Expected: no matches.

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/ tests/unit/adapters/test_postgresql_feed_horizon.py tests/unit/migrations/test_additive_schema.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 9: ADR 0027 part (b), BACKLOG, CHANGELOG**

Append part (b) to `docs/adrs/0027-schema-correctness-fixes.md` (Status stays `Proposed`): the `events.txid xid8` additive column and its two-statement operator script, the NULL-is-safe argument spelled out via ACCESS EXCLUSIVE, the once-per-read bound horizon replacing the inline volatile expression, and the PostgreSQL 13 floor being unchanged (`pg_current_snapshot()` already imposed it). Include the rejected alternatives from spec §7: modular 32-bit arithmetic (PostgreSQL defines no ordering operators on `xid`; irreducible ambiguity at the 2^31 boundary; still unindexable), WONTFIX (fail-open in exactly the deployments that need the guard), `NOT NULL` with backfill (full rewrite for zero correctness gain), and a runtime column-existence probe with fallback (keeps the unsafe path alive and hides the operational requirement). Cross-reference ADR 0025, whose decision is unchanged — this replaces the mechanism, not the guarantee.

In `BACKLOG.md`, rewrite the body of the `## Re-benchmark pg catch-up horizon predicate at scale (P2)` entry (lines 177-189). Do **not** close it. The new premise: the inline `xmin`-cast predicate that EXPLAIN showed defeating the `global_position` index is gone, replaced by `(txid IS NULL OR txid < CAST($n AS xid8))` with the horizon bound once per read — which is the very mitigation this entry proposed. The re-bench remains warranted at 1M+ rows with concurrent writers, now against the new shape, and the methodology section must be re-run rather than re-read. Keep the pointer to the session artifact but mark its numbers as measuring a predicate that no longer exists.

Add under `## [Unreleased]` → `### Changed`:

```markdown
- **PostgreSQL deployments MUST apply `migrations/updates/004_add_events_txid.sql` before upgrading.** The global feed read path (`read_all`, `current_position`) now filters on a new `events.txid xid8` column instead of the `xmin` system column, and fails loudly with an undefined-column error against a database that has not applied it. The old predicate compared a 32-bit `xmin` against an epoch-extended 64-bit `pg_snapshot_xmin(...)`, so it became universally true — silently dropping the no-skip guarantee — once a cluster crossed its first xid epoch. Fresh provisioning via `get_schema`/`get_all_schemas` needs nothing: the column arrives as an additive fragment. Rows left with a NULL `txid` (those predating the migration) are always read; no backfill is needed. Requires PostgreSQL 13+, the same floor as before. See ADR 0027.
```

- [ ] **Step 10: commit**

```bash
git add src/eventsource/migrations/additive/events_txid.sql \
        src/eventsource/migrations/updates/004_add_events_txid.sql \
        src/eventsource/migrations/__init__.py \
        src/eventsource/adapters/postgresql/store.py \
        tests/unit/adapters/test_postgresql_feed_horizon.py \
        tests/unit/migrations/test_additive_schema.py \
        tests/integration/adapters/test_postgresql_conformance.py \
        docs/adrs/0027-schema-correctness-fixes.md BACKLOG.md CHANGELOG.md
git commit --only src/eventsource/migrations/additive/events_txid.sql \
        src/eventsource/migrations/updates/004_add_events_txid.sql \
        src/eventsource/migrations/__init__.py \
        src/eventsource/adapters/postgresql/store.py \
        tests/unit/adapters/test_postgresql_feed_horizon.py \
        tests/unit/migrations/test_additive_schema.py \
        tests/integration/adapters/test_postgresql_conformance.py \
        docs/adrs/0027-schema-correctness-fixes.md BACKLOG.md CHANGELOG.md \
        -m "fix: make the postgres feed horizon wraparound-safe via an xid8 column"
```

Record in the commit body: that true epoch wraparound is not integration-testable and the query-shape assertions are the regression guard, and that the two-statement ALTER split avoids a full table rewrite.

---

### Task 3: Reconcile `events.tenant_id` schema drift (**blocked by T2**)

Spec §6. The canonical schema declares `tenant_id UUID`, matching the type system end to end; `tests/integration/conftest.py` hand-rolls `tenant_id VARCHAR(255)` into the shared session `postgres_engine` fixture. The drift is why the ports conformance tests retreated to a private `ports_conformance` database.

**Blocked by T2:** the fixture must provision the fragment-carrying `get_schema("events")`, which now includes the `txid` column the adapter's feed query requires. Flipping the fixture first would provision a schema the updated adapter cannot read.

**Files:**
- Modify: `tests/integration/conftest.py`
- Modify: `tests/integration/adapters/conftest.py` (docstring only)
- Modify: `tests/integration/adapters/test_postgresql_conformance.py` (the stale comment above `_CHECKPOINTS_DDL`)
- Modify: whichever `-m postgres` tests fail on the flip (the reconciliation worklist — unknown until Step 2 runs)
- Modify: `docs/adrs/0027-schema-correctness-fixes.md` (append part (c); Status → Accepted)
- Modify: `docs/adrs/index.md`
- Modify: `CHANGELOG.md` (only if Step 3 surfaces an adapter fix)

**Interfaces:**
- Consumes (from T2): `get_schema("events")` returning the base template plus the `events_txid` fragment.
- Produces: nothing consumed by another task.

**Scope note — read before starting.** Spec §6 step 1 says to provision events, checkpoints, dlq, and outbox from `get_schema()` through a raw asyncpg connection. That is achievable for **`events` only**. `get_schema("checkpoints")`, `get_schema("dlq")`, and `get_schema("outbox")` each ship PL/pgSQL helper functions with dollar-quoted bodies containing `GET DIAGNOSTICS`, which asyncpg's simple-query path mis-splits — the failure is already documented in `tests/integration/adapters/test_postgresql_conformance.py` above `_CHECKPOINTS_DDL`, and is why that module provisions those two tables as bare DDL statements. The backlog item's actual subject is `events.tenant_id`, so this task reconciles `events` onto the canonical schema and leaves the other three blocks as explicit DDL, each gaining a comment naming the PL/pgSQL constraint. Do not attempt to make the other three go through `get_schema` and do not "fix" it by stripping functions out of the canonical files.

- [ ] **Step 1: read the two schemas side by side**

Read `src/eventsource/migrations/schemas/events.sql` and the `EVENTS_SCHEMA_STATEMENTS` block in `tests/integration/conftest.py` (lines 272-303). Write down every difference before changing anything — at minimum `tenant_id UUID` vs `VARCHAR(255)`, the `CONSTRAINT uq_events_aggregate_version UNIQUE (...)` table constraint vs a separately-created `CREATE UNIQUE INDEX uq_events_aggregate_version`, and the index name set (`idx_events_aggregate`/`idx_events_type` vs the canonical names). Every test that breaks in Step 3 will break on one of these.

- [ ] **Step 2 (red): flip the fixture**

The red step here *is* the flip — this is test-infrastructure reconciliation, so the failures it produces are the worklist.

In `tests/integration/conftest.py`, delete the `EVENTS_SCHEMA_STATEMENTS` list entirely. Add a comment above the three surviving lists explaining why they remain:

```python
# `get_schema("checkpoints"/"dlq"/"outbox")` each ship PL/pgSQL helper
# functions whose dollar-quoted bodies asyncpg's simple-query path
# mis-splits (`PostgresSyntaxError: unrecognized GET DIAGNOSTICS item`),
# so these three are provisioned as bare DDL, one statement at a time.
# `events` carries no functions and is provisioned from the canonical
# schema below -- it is the table whose type (`tenant_id UUID`) the
# adapters and the whole type system depend on.
```

In the `postgres_engine` fixture, replace the `for statement in EVENTS_SCHEMA_STATEMENTS:` loop with a raw-driver script execution, mirroring the pattern in `tests/integration/adapters/test_postgresql_conformance.py::TestPostgreSQLSnapshotStore`:

```python
    from eventsource.migrations import get_schema

    async with engine.begin() as conn:
        # get_schema returns a multi-statement script; asyncpg's
        # prepared-statement path rejects those, so run it via the raw
        # driver connection (simple query protocol), same as
        # PostgreSQLEventStore._ensure_schema.
        raw = await conn.get_raw_connection()
        driver_connection = raw.driver_connection
        assert driver_connection is not None
        await driver_connection.execute(get_schema("events"))

        for statement in CHECKPOINTS_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
        for statement in DLQ_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
        for statement in OUTBOX_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
```

```bash
docker compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/ -m postgres -q
```

Expected: some number of failures, plausibly zero. **Record the exact list** — it goes in the commit body and the PR description. "None" is a legitimate and reportable outcome.

- [ ] **Step 3 (green): work the list**

Fix each failure **in the test**, never in the schema. Typical shapes:

- A test binding `str(tenant_id)` into `events.tenant_id` now hits `DatatypeMismatchError` — bind the `UUID` object.
- A test asserting on an index or constraint name that only the hand-rolled DDL had — assert on the canonical name from `migrations/schemas/events.sql`.
- A test that inserted rows without `version` or relied on the absence of `uq_events_aggregate_version` — supply the column / use distinct versions.

**One hard rule:** no failure may be resolved by editing a `migrations/` file. If a failure traces to an actual adapter bug rather than a test assumption, stop, write a red test for the adapter bug, fix the adapter, and note it — that path also earns a CHANGELOG **Fixed** entry.

```bash
uv run pytest tests/integration/ -m postgres -q
```
Expected: PASS.

- [ ] **Step 4 (green): retire the stale drift documentation**

Rewrite the module docstring of `tests/integration/adapters/conftest.py`. Its current text attributes the VARCHAR side to `tests/integration/stores/test_postgresql.py` — a suite that no longer exists (the legacy stores were retired in ADR 0025) — and describes the drift as out-of-scope. Both facts are now false. The private `ports_conformance` database **stays**; the surviving justification is isolation from table-recreating tests. New docstring:

```python
"""Fixtures private to the port-adapter integration tests.

These suites DROP and recreate the `events` table between fixtures. The
shared session-scoped `postgres_engine` (tests/integration/conftest.py)
provisions the same canonical `migrations/schemas/events.sql` schema, so
there is no longer any schema drift between them -- but a suite that
recreates tables mid-session would still pull the rug out from under
every other suite sharing the database. This module gives the
port-adapter tests their own private database on the same
testcontainer, so their table churn is invisible to everything else.
"""
```

In `tests/integration/adapters/test_postgresql_conformance.py`, update the comment above `_CHECKPOINTS_DDL` (around line 159): its closing sentence points at `tests/integration/conftest.py`'s `CHECKPOINTS_SCHEMA_STATEMENTS`/`DLQ_SCHEMA_STATEMENTS` as the thing it mirrors — still true, and now the *reason* both exist is the PL/pgSQL constraint alone, not schema drift. Say that explicitly.

- [ ] **Step 5: verify**

```bash
grep -n "CREATE TABLE IF NOT EXISTS events" tests/integration/conftest.py
grep -rn "VARCHAR(255)" tests/integration/conftest.py | grep -i tenant
grep -rn "legacy\|eventsource.stores" tests/integration/adapters/conftest.py
```
Expected: all three empty.

Run: `uv run ruff check tests/integration/` — Expected: clean.
Run: `uv run pytest tests/integration/ -m postgres -q` — Expected: PASS.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 6: complete ADR 0027**

Append part (c) to `docs/adrs/0027-schema-correctness-fixes.md`: the canonical `migrations/` schema is authoritative for `events.tenant_id` (UUID), matching `DomainEvent.tenant_id: UUID | None`, `TenantId`, `FeedReadOptions.tenant_id`, and the ports PostgreSQL adapter's `uuid.UUID` binds; the hand-rolled test DDL for `events` is retired. Record the rejected alternatives: make the canonical schema VARCHAR (fights the whole type system, breaks every deployed database provisioned from the canonical schema) and keep both with documentation (two schemas for one table where one is test-only fiction is the defect). Record the scope note above as a Consequence: checkpoints, dlq, and outbox keep explicit test DDL because their canonical scripts carry PL/pgSQL that asyncpg's simple-query path mis-splits, and reconciling those three is a separate piece of work.

Change the Status to `Accepted` and delete the "Accepted when part (c) lands" note.

Add a row to `docs/adrs/index.md` under the **Store contract and layering** group (after the `0026` row), matching the surrounding one-paragraph style: name the three parts (in-place SQLite outbox replacement with its exception criterion, the `events.txid xid8` column with the NULL-is-safe argument and per-read bound horizon, canonical UUID authority for `events.tenant_id`), state that it extends ADR 0025's mechanism without amending its decision, and mark it Complete.

- [ ] **Step 7: commit**

```bash
git add tests/integration/conftest.py tests/integration/adapters/conftest.py \
        tests/integration/adapters/test_postgresql_conformance.py \
        docs/adrs/0027-schema-correctness-fixes.md docs/adrs/index.md
# plus every test file Step 3 changed, and CHANGELOG.md only if Step 3
# surfaced an adapter fix -- name them explicitly, no globs
git commit --only <the same explicit list> \
        -m "test: provision integration events table from the canonical schema"
```

Record in the commit body: the exact list of tests that failed on the flip (or "none"), and that no failure was resolved by editing a migration file.

---

### Task 4: Wire `position_mapper` and add the in-phase resync pass

Spec §4 + §5, deliberately one task: they share the `_build_copier` seam, and the spec designs them together so neither forecloses the other.

`MigrationCoordinator` accepts and stores a `position_mapper`, and `MigrationConfig.position_mapping_enabled` documents `True` — but the flag is read nowhere in `src/` and the coordinator's `BulkCopier` construction omits `position_mapper=`, so a default-path migration records no mappings and `migrate_subscriptions=True` silently skips. Separately, `safe_lag_anchor`'s failure clamp is released only by `mark_copy_pass_complete`, whose sole caller is the bulk-copy loop — so a transient mirror failure during `DUAL_WRITE` clamps the anchor permanently with no remedy short of abort-and-restart.

**Files:**
- Modify: `src/eventsource/migration/coordinator.py`
- Create tests: `tests/unit/migration/test_coordinator_resync.py`
- Modify: `docs/guides/live-migration.md`
- Create: `docs/adrs/0028-strict-cutover-and-in-phase-resync.md` (Status: Proposed; T5 completes it)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `BulkCopier.__init__(source_store, target_store, migration_repo, *, position_mapper=None, tracer=None, enable_tracing=True)` — unchanged; `MigrationCoordinator._run_copy_pass(copier, migration) -> bool`; `DualWriteInterceptor.mark_copy_pass_complete(checkpoint: Position | None) -> int`.
- Produces (**both consumed by T5**):

```python
    def _build_copier(
        self,
        migration: Migration,
        target_store: FullEventStore | None = None,
    ) -> BulkCopier: ...

    async def run_resync_pass(self, migration_id: UUID) -> int: ...
```

`_build_copier` resolves `target_store` from `self._target_stores[migration.id]` when the argument is omitted, and raises `MigrationError` when neither is available. `run_resync_pass` returns the number of unabsorbed mirror failures remaining — 0 means the lag anchor is unclamped.

- [ ] **Step 1: read before writing**

Read, in this order: `coordinator.py:772-876` (`_run_bulk_copy`, including the `BulkCopier(...)` construction at 815-820 and the catch-up round loop), `coordinator.py:878-902` (`_run_copy_pass`), `coordinator.py:904-935` (`_install_interceptor`), `coordinator.py:1278-1292` (`_lag_anchor`), and `dual_write.py:290-440` (`safe_lag_anchor` and `mark_copy_pass_complete`, including the full attestation contract docstring). Then read `tests/unit/migration/test_coordinator_dual_write.py:1-90` for the `coordinator_deps` fixture shape (four `AsyncMock`/`MagicMock` collaborators) that the new test module reuses.

Confirm by reading, not assumption: `BulkCopier.run` writes progress and errors only and never touches `migration.phase` — this is what makes a resync pass safe to run inside `DUAL_WRITE` without corrupting the `VALID_TRANSITIONS` state machine. If it does touch phase, stop and report.

- [ ] **Step 2 (red): the wiring gate and the resync entry point**

Create `tests/unit/migration/test_coordinator_resync.py`:

```python
"""The shared copier seam: position-mapper wiring and in-phase resync.

`_build_copier` is the single construction site for `BulkCopier`, used by
the automated bulk-copy path and by the operator-triggered
`run_resync_pass`, so the two can never diverge on wiring.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.migration.models import Migration, MigrationConfig, MigrationPhase


@pytest.fixture
def coordinator_deps() -> dict:
    return {
        "source_store": AsyncMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }


def _migration(phase: MigrationPhase, config: MigrationConfig | None = None) -> Migration:
    return Migration(
        id=uuid4(),
        tenant_id=uuid4(),
        source_store_id="default",
        target_store_id="dedicated",
        phase=phase,
        config=config or MigrationConfig(),
    )


class TestBuildCopierWiring:
    def test_mapper_is_wired_when_the_config_enables_mapping(
        self, coordinator_deps: dict
    ) -> None:
        mapper = AsyncMock()
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=mapper, enable_tracing=False
        )
        migration = _migration(MigrationPhase.BULK_COPY)

        copier = coordinator._build_copier(migration, AsyncMock())

        assert copier._position_mapper is mapper

    def test_mapper_is_withheld_when_mapping_is_disabled(
        self, coordinator_deps: dict
    ) -> None:
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=AsyncMock(), enable_tracing=False
        )
        migration = _migration(
            MigrationPhase.BULK_COPY,
            MigrationConfig(position_mapping_enabled=False),
        )

        copier = coordinator._build_copier(migration, AsyncMock())

        assert copier._position_mapper is None

    def test_no_mapper_on_the_coordinator_means_no_mapper_on_the_copier(
        self, coordinator_deps: dict
    ) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        copier = coordinator._build_copier(_migration(MigrationPhase.BULK_COPY), AsyncMock())

        assert copier._position_mapper is None

    def test_target_store_is_resolved_from_the_registry_when_omitted(
        self, coordinator_deps: dict
    ) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        migration = _migration(MigrationPhase.DUAL_WRITE)
        target = AsyncMock()
        coordinator._target_stores[migration.id] = target

        copier = coordinator._build_copier(migration)

        assert copier._target_store is target

    def test_missing_target_store_raises(self, coordinator_deps: dict) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationError):
            coordinator._build_copier(_migration(MigrationPhase.DUAL_WRITE))


class TestRunResyncPassGuards:
    async def test_unknown_migration_raises(self, coordinator_deps: dict) -> None:
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationNotFoundError):
            await coordinator.run_resync_pass(uuid4())

    async def test_wrong_phase_raises(self, coordinator_deps: dict) -> None:
        migration = _migration(MigrationPhase.BULK_COPY)
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationStateError):
            await coordinator.run_resync_pass(migration.id)

    async def test_active_copier_raises(self, coordinator_deps: dict) -> None:
        migration = _migration(MigrationPhase.DUAL_WRITE)
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration.id] = AsyncMock()
        coordinator._active_copiers[migration.id] = MagicMock()

        with pytest.raises(MigrationError):
            await coordinator.run_resync_pass(migration.id)
```

Mark the module for asyncio the way the neighbouring migration test modules do (`pytestmark = pytest.mark.asyncio` or per-test `@pytest.mark.asyncio` — match `tests/unit/migration/test_coordinator_dual_write.py`).

Run: `uv run pytest tests/unit/migration/test_coordinator_resync.py -q`

Expected: FAIL — `AttributeError: 'MigrationCoordinator' object has no attribute '_build_copier'` and `... 'run_resync_pass'`.

- [ ] **Step 3 (green): extract `_build_copier`**

In `src/eventsource/migration/coordinator.py`, add the helper immediately above `_run_copy_pass` (i.e. after `_run_bulk_copy` ends around line 876):

```python
    def _build_copier(
        self,
        migration: Migration,
        target_store: FullEventStore | None = None,
    ) -> BulkCopier:
        """Construct the migration's BulkCopier -- the only place that does.

        Both copy entry points go through here: the automated bulk-copy
        phase and the operator-triggered `run_resync_pass`. One
        construction site means the two can never diverge on wiring, which
        matters most for the position mapper: a resync pass that recorded
        no mappings would leave subscription checkpoint translation with
        holes the bulk pass had filled.

        The mapper is attached only when the coordinator was given one AND
        `config.position_mapping_enabled` is True (the documented default).
        With a mapper attached the copier appends one event at a time so
        each target position can be recorded; without one it batches. That
        is the cost `position_mapping_enabled=False` buys back.

        Args:
            migration: Migration to build a copier for.
            target_store: Target store to copy into. Resolved from the
                coordinator's registry when omitted -- which is how
                `run_resync_pass` calls it, since it has only an id.

        Returns:
            A BulkCopier that resumes from the migration's persisted
            checkpoint.

        Raises:
            MigrationError: If no target store was passed and none is
                registered for this migration (the shape a coordinator
                restart leaves behind -- the registry is in-memory).
        """
        store = target_store or self._target_stores.get(migration.id)
        if store is None:
            raise MigrationError(
                "No target store registered for migration; the coordinator "
                "that started it holds that registry in memory, so a "
                "restarted coordinator must re-register before copying.",
                migration_id=migration.id,
            )

        mapper = self._position_mapper if migration.config.position_mapping_enabled else None

        return BulkCopier(
            self._source_store,
            store,
            self._migration_repo,
            position_mapper=mapper,
            enable_tracing=self._enable_tracing,
        )
```

Then replace the inline construction in `_run_bulk_copy` (lines 815-820):

```python
            copier = BulkCopier(
                self._source_store,
                target_store,
                self._migration_repo,
                enable_tracing=self._enable_tracing,
            )
```

with:

```python
            copier = self._build_copier(migration, target_store)
```

Run: `uv run pytest tests/unit/migration/test_coordinator_resync.py::TestBuildCopierWiring -q` — Expected: PASS.

- [ ] **Step 4 (green): `run_resync_pass`**

Add the public method after `_build_copier`. Place it in the "Phase 2 (P2-005): Dual-Write and Cutover Methods" section if that reads better against the file's own grouping — but keep it adjacent to `_build_copier`, which it exists to share.

```python
    async def run_resync_pass(self, migration_id: UUID) -> int:
        """Run one bounded catch-up copy pass while in DUAL_WRITE.

        The remedy for a clamped lag anchor. `safe_lag_anchor` refuses to
        advance past a mirror failure, and only a completed copy pass
        attested by `mark_copy_pass_complete` releases that clamp -- which
        until now only the bulk-copy loop could do, so a transient mirror
        failure after the copy finished was a dead end whose only exit was
        abort-and-restart.

        The migration's phase is never touched: `BulkCopier.run` writes
        progress and errors only, so the migration reads as DUAL_WRITE
        throughout and the VALID_TRANSITIONS state machine is untouched.
        Re-copying is safe by construction -- the copier treats
        already-present events as copied.

        One pass per call. The return value is the caller's bounding
        policy: call again while it is nonzero, and stop when the
        underlying mirror problem is not transient.

        Args:
            migration_id: UUID of the migration to resync.

        Returns:
            The number of unabsorbed mirror failures remaining. 0 means
            the lag anchor is unclamped and cutover can proceed once lag
            drains to the configured threshold.

        Raises:
            MigrationNotFoundError: If migration not found.
            MigrationStateError: If migration is not in DUAL_WRITE phase.
            MigrationError: If a copier is already active for this
                migration, if no target store is registered, or if the
                pass did not run to completion.
            BulkCopyError: Propagated unchanged from the copy itself.
        """
        with self._tracer.span(
            "eventsource.coordinator.run_resync_pass",
            {"migration.id": str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(
                    message=(
                        f"Cannot run a resync pass: migration is in "
                        f"{migration.phase.value} phase"
                    ),
                    migration_id=migration_id,
                    current_phase=migration.phase,
                    expected_phases=[MigrationPhase.DUAL_WRITE],
                    operation="run_resync_pass",
                )

            if migration_id in self._active_copiers:
                raise MigrationError(
                    "A copy pass is already running for this migration",
                    migration_id=migration_id,
                )

            copier = self._build_copier(migration)
            self._active_copiers[migration_id] = copier
            try:
                completed = await self._run_copy_pass(copier, migration)
            finally:
                self._active_copiers.pop(migration_id, None)

            if not completed:
                raise MigrationError(
                    "Resync pass did not run to completion; the lag anchor "
                    "stays clamped (an incomplete pass attests nothing)",
                    migration_id=migration_id,
                )

            current = await self._migration_repo.get(migration_id)
            if current is None:
                raise MigrationNotFoundError(migration_id)

            interceptor = self._interceptors.get(migration_id)
            if interceptor is None:
                # Coordinator restart: interceptor state is in-memory, so
                # there is no failure list left to absorb. The pass still
                # advanced the persisted checkpoint, which is what
                # `_lag_anchor` falls back to without an interceptor.
                return 0

            # The pass began after installation (the interceptor has been
            # installed since bulk-copy start, and DUAL_WRITE postdates
            # that) and ran to completion -- the two ordering facts only
            # the coordinator can attest. See
            # `DualWriteInterceptor.mark_copy_pass_complete`.
            return interceptor.mark_copy_pass_complete(current.last_source_position)
```

Run: `uv run pytest tests/unit/migration/test_coordinator_resync.py -q` — Expected: PASS.

- [ ] **Step 5 (red then green): the mapper reaches the default path end-to-end**

Add to `tests/unit/migration/test_coordinator_resync.py` a behavioral case proving the *default* path records mappings — the gate test above pins construction, this pins the consequence. It uses real in-memory stores and the `AsyncMock` repo shape from `tests/unit/migration/test_bulk_copier.py` (see its `position_mapper` cases around lines 797 and 992).

```python
class TestDefaultPathRecordsMappings:
    async def _run(
        self, coordinator_deps: dict, config: MigrationConfig
    ) -> AsyncMock:
        from eventsource.adapters.memory.store import InMemoryEventStore
        from eventsource.domain import StreamId
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import make_event

        source = InMemoryEventStore()
        target = InMemoryEventStore()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Conformance")
        await source.append(
            stream,
            [make_event(aggregate_id) for _ in range(3)],
            ExpectedVersion.no_stream(),
        )

        migration = _migration(MigrationPhase.BULK_COPY, config)
        coordinator_deps["source_store"] = source
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].set_events_total = AsyncMock()
        coordinator_deps["migration_repo"].update_progress = AsyncMock()
        coordinator_deps["migration_repo"].record_error = AsyncMock()

        mapper = AsyncMock()
        mapper.record_mapping = AsyncMock()
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=mapper, enable_tracing=False
        )

        await coordinator._run_bulk_copy(migration, target)
        return mapper

    async def test_default_config_records_a_mapping_per_copied_event(
        self, coordinator_deps: dict
    ) -> None:
        mapper = await self._run(coordinator_deps, MigrationConfig())

        assert mapper.record_mapping.await_count == 3

    async def test_disabling_the_flag_records_nothing(
        self, coordinator_deps: dict
    ) -> None:
        mapper = await self._run(
            coordinator_deps, MigrationConfig(position_mapping_enabled=False)
        )

        assert mapper.record_mapping.await_count == 0
```

`_run_bulk_copy` also installs an interceptor and transitions to dual-write against the mocked router and repo — that is fine, and any assertion on those belongs in `test_coordinator_dual_write.py`, not here. If the mocked `_transition_to_dual_write` path raises, stub the specific repo method it needs rather than catching broadly.

Since Step 3 has already landed, you cannot observe the red for these two here. Verify it on a scratch copy under `$CLAUDE_JOB_DIR/tmp` with `_build_copier`'s `position_mapper=mapper` reverted to `position_mapper=None`, confirming `await_count == 0` in both. Do not mutate live source to produce a red.

Run this case **before** it can pass only if you wrote it before Step 3 — since Step 3 has already landed, instead verify the red by temporarily reverting `_build_copier`'s `position_mapper=mapper` to `position_mapper=None` **in a scratch copy under `$CLAUDE_JOB_DIR/tmp`**, confirming the assertion fails there. Do not mutate live source to produce a red.

Run: `uv run pytest tests/unit/migration/test_coordinator_resync.py tests/unit/migration/test_bulk_copier.py tests/unit/migration/test_coordinator_dual_write.py tests/unit/migration/test_coordinator_subscriptions.py -q` — Expected: PASS.

- [ ] **Step 6: verify**

```bash
grep -rn "position_mapping_enabled" src/eventsource/migration/
```
Expected: exactly the `models.py` declarations (docstring, field, `to_dict`, `from_dict`) plus the single gate in `_build_copier` — one read site, as spec §4's acceptance criteria require.

```bash
grep -n "BulkCopier(" src/eventsource/migration/coordinator.py
```
Expected: one occurrence, inside `_build_copier`.

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migration/ tests/unit/migration/test_coordinator_resync.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 7: docs and ADR 0028 draft**

In `docs/guides/live-migration.md`, replace the sentence at lines 143-145 ("There is currently no in-phase resync for that case -- if mirroring falls behind after the copy has finished, the current remedy is to abort the migration and restart it.") with the API and a short runbook:

```markdown
When that happens, run an in-phase resync rather than aborting:

    remaining = await coordinator.run_resync_pass(migration.id)
    while remaining:
        remaining = await coordinator.run_resync_pass(migration.id)

Each call runs one bounded catch-up copy pass while the migration stays in
`DUAL_WRITE`, and returns the number of unabsorbed mirror failures left. A
return of 0 means the lag anchor is unclamped and cutover can proceed once
lag drains. Bounding the retries is your policy, not the library's: a count
that stops falling is a mirror problem to investigate, not a pass to repeat.
```

In the checkpoint-translation section of the same guide, state the position-mapper gate and its cost: mappings are recorded when the coordinator was constructed with a `PositionMapper` **and** `config.position_mapping_enabled` is True (the default); with a mapper attached the bulk copier appends one event at a time so each target position can be recorded, where it otherwise batches. `position_mapping_enabled=False` buys the batched path back at the cost of subscription checkpoint translation.

Create `docs/adrs/0028-strict-cutover-and-in-phase-resync.md` with **Status: Proposed** and a note that T5 completes it. Write the resync/wiring half now: the `_build_copier` seam and why one construction site is load-bearing; `run_resync_pass`'s preconditions, the never-touch-phase guarantee, the attestation contract it preserves, the restart-shaped case returning 0, and one-pass-per-call with caller-owned bounding. Record the rejected alternatives from spec §5: automatic background resync (hides mirror instability, contends with live writes on an unbounded schedule), a public "absorb" API without a copy pass (violates `mark_copy_pass_complete`'s contract), and folding it into `trigger_cutover` as a pre-pass (couples a read-heavy bulk operation into the sub-second cutover window). Leave the cutover-strictness half for T5.

- [ ] **Step 8: CHANGELOG**

Add under `## [Unreleased]` → `### Added`:

```markdown
- **`MigrationCoordinator.run_resync_pass(migration_id) -> int`** — runs one bounded catch-up copy pass while a migration is in `DUAL_WRITE`, returning the number of unabsorbed dual-write mirror failures remaining (0 means the sync-lag anchor is unclamped and cutover can proceed). Previously a mirror failure after the bulk copy finished clamped the lag anchor permanently and the only remedy was to abort and restart the migration. The migration's phase is never touched. See ADR 0028.
```

and under `### Fixed`:

```markdown
- **Default-path migrations now record position mappings.** `MigrationCoordinator` accepted a `position_mapper` and `MigrationConfig.position_mapping_enabled` documented a default of `True`, but the flag was read nowhere and the coordinator never passed the mapper to its `BulkCopier` — so an ordinary migration recorded nothing and subscription checkpoint translation (`migrate_subscriptions=True`, also a documented default) silently skipped. Mappings are now recorded whenever the coordinator was given a mapper and the flag is True. Note the cost: with a mapper attached the bulk copier appends one event at a time so each target position can be recorded, where it otherwise batches; set `position_mapping_enabled=False` to keep the batched path.
```

- [ ] **Step 9: commit**

```bash
git add src/eventsource/migration/coordinator.py \
        tests/unit/migration/test_coordinator_resync.py \
        docs/guides/live-migration.md \
        docs/adrs/0028-strict-cutover-and-in-phase-resync.md CHANGELOG.md
git commit --only src/eventsource/migration/coordinator.py \
        tests/unit/migration/test_coordinator_resync.py \
        docs/guides/live-migration.md \
        docs/adrs/0028-strict-cutover-and-in-phase-resync.md CHANGELOG.md \
        -m "feat: add in-phase resync and wire the position mapper through one copier seam"
```

Record in the commit body: that `_build_copier` exists so the two copy entry points cannot diverge, and that you verified `BulkCopier.run` never writes phase.

---

### Task 5: Cutover defaults to strict zero lag (**blocked by T4**)

Spec §2. `cutover_max_lag_events` defaults to 100, and `cutover.py:321` allows the routing switch with up to 100 source events provably absent from the target — events the target never receives while it becomes authoritative, caught only by a non-fatal post-cutover consistency check. This contradicts the invariant `dual_write.py:356-360` documents: "the accepted failure mode here is stuck-until-recopied, never a cutover over missing data."

**Blocked by T4:** strict-0 without `run_resync_pass` means "stuck-until-abort." The joint end-to-end test (resync, then a strict cutover that succeeds) lives here.

**Files:**
- Modify: `src/eventsource/migration/models.py`
- Modify: `src/eventsource/migration/sync_lag_tracker.py` (docstring examples)
- Modify: `src/eventsource/migration/README.md`
- Modify: `docs/guides/live-migration.md`
- Modify: `docs/api/migration.md`
- Modify tests: `tests/unit/migration/test_models.py`, `tests/unit/migration/test_cutover_manager.py`, and any other cutover test relying on the old slack
- Modify tests: `tests/unit/migration/test_coordinator_resync.py` (the joint end-to-end case)
- Modify: `docs/adrs/0028-strict-cutover-and-in-phase-resync.md` (complete; Status → Accepted)
- Modify: `docs/adrs/0014-live-migration-cutover-semantics.md` (Status section only — "Amended by" pointer)
- Modify: `docs/adrs/index.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes (from T4): `MigrationCoordinator.run_resync_pass(migration_id) -> int` and `_build_copier(migration, target_store=None) -> BulkCopier`.
- Produces: nothing consumed by another task.

- [ ] **Step 1 (red): the default itself**

In `tests/unit/migration/test_models.py`, find the `MigrationConfig` default-values test and change its `cutover_max_lag_events` assertion to `== 0`. Add a companion assertion on the `from_dict` fallback if the module does not already have one:

```python
    def test_from_dict_defaults_to_strict_zero_lag(self) -> None:
        assert MigrationConfig.from_dict({}).cutover_max_lag_events == 0
```

Run: `uv run pytest tests/unit/migration/test_models.py -q` — Expected: FAIL (both read 100).

- [ ] **Step 2 (red): the behavior**

The lag threshold is enforced in `CutoverManager.execute_cutover` (`cutover.py:321`), not in the coordinator, so the behavioral case belongs in `tests/unit/migration/test_cutover_manager.py` alongside `TestSuccessfulCutover`. Its fixtures (`cutover_manager`, `tenant_id`, `migration_id`, `target_store_id`, `mock_lag_tracker`) already exist; `execute_cutover` called **without** a `config=` kwarg is the default-config path.

Add this class to that module:

```python
class TestStrictZeroLagDefault:
    """`cutover_max_lag_events` defaults to 0: no cutover over missing data."""

    @pytest.mark.asyncio
    async def test_default_config_refuses_cutover_on_a_single_missing_event(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        mock_lag_tracker.current_lag = SyncLag(
            events=1,
            source_position=Position(store_id="source", key=(101,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert result.rolled_back is True
        # Routing never switched: the source stays authoritative.
        mock_routing_repo.set_routing.assert_not_called()
        mock_router.resume_writes.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_an_explicit_threshold_still_tolerates_lag(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """The knob survives -- it just stops being the default."""
        mock_lag_tracker.current_lag = SyncLag(
            events=1,
            source_position=Position(store_id="source", key=(101,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,  # cutover_max_lag_events=100
        )

        assert result.success is True
```

Check how `execute_cutover` surfaces a `CutoverLagError` before asserting: if it propagates rather than returning a failed `CutoverResult`, use `pytest.raises(CutoverLagError)` in the first case and import it from `eventsource.migration.exceptions`. Read the module's existing failure cases (the rollback ones) and match whichever shape they use — do not assume.

Run: `uv run pytest tests/unit/migration/test_cutover_manager.py -q` — Expected: FAIL on the first case (today 1 ≤ 100, so the cutover succeeds and routing switches over the missing event).

- [ ] **Step 3 (green): flip the default**

In `src/eventsource/migration/models.py`:

- line 373 (attribute docstring): replace `cutover_max_lag_events: Max lag allowed before cutover (default 100).` with:

  ```
        cutover_max_lag_events: Max lag allowed before cutover (default 0 --
            strict). Any nonzero value permits cutover while that many
            source events are absent from the target; they are never
            copied, because writes are paused for the whole cutover and
            nothing in the sequence copies the residue. Set it only as an
            explicit acceptance of that bounded loss.
  ```

- line 391: `cutover_max_lag_events: int = 0`
- line 451 (`from_dict`): `cutover_max_lag_events=data.get("cutover_max_lag_events", 0),`

Leave `__post_init__`'s `>= 0` validation, `to_dict`, `cutover.py`, `is_within_threshold`, and `calculate_lag` untouched — with threshold 0 the tracker reads `limit=1` and the existing bounded-count logic behaves correctly.

Run: `uv run pytest tests/unit/migration/test_models.py tests/unit/migration/test_cutover_manager.py -q` — Expected: PASS.

- [ ] **Step 4 (green): the joint end-to-end case**

Add the test that ties §2 and §5 together, the one spec §5 names as an acceptance criterion, to `tests/unit/migration/test_coordinator_resync.py`:

```python
class TestResyncThenStrictCutover:
    async def test_a_clamped_anchor_is_recovered_by_a_resync_pass(
        self, coordinator_deps: dict
    ) -> None:
        from eventsource.adapters.memory.store import InMemoryEventStore
        from eventsource.domain import StreamId
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import make_event

        source = InMemoryEventStore()
        target = InMemoryEventStore()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Conformance")
        await source.append(
            stream, [make_event(aggregate_id)], ExpectedVersion.no_stream()
        )

        migration = _migration(MigrationPhase.DUAL_WRITE)
        coordinator_deps["source_store"] = source
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].set_events_total = AsyncMock()
        coordinator_deps["migration_repo"].update_progress = AsyncMock()
        coordinator_deps["migration_repo"].record_error = AsyncMock()

        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration.id] = target
        interceptor = coordinator._install_interceptor(migration, target)

        # Force one mirror failure, then let the mirror recover.
        await _record_one_mirror_failure(interceptor, stream, aggregate_id)

        # The anchor is clamped: cutover would refuse (correctly -- the
        # data really is missing), and before run_resync_pass existed the
        # only exit was abort-and-restart.
        assert interceptor.safe_lag_anchor(migration.last_source_position) != (
            await source.current_position()
        )

        remaining = await coordinator.run_resync_pass(migration.id)

        assert remaining == 0
        # With the clamp released the anchor tracks the checkpoint again.
        assert coordinator._lag_anchor(migration) == migration.last_source_position
```

`_record_one_mirror_failure` is a small module-level helper you write against `DualWriteInterceptor.append`'s **actual** signature — read `src/eventsource/migration/dual_write.py` for it, and for how a failed target append is recorded. The shape is: temporarily replace the interceptor's target-store append with one that raises, drive a single append through the interceptor so the failure is recorded, then restore it. Reuse whatever `tests/unit/migration/test_dual_write.py` already does to force a mirror failure rather than inventing a second mechanism — if that module has such a helper, import it instead of writing one.

The strict-cutover half of the pairing is covered by `TestStrictZeroLagDefault` in Step 2; this case proves the recovery path that makes strict-0 livable rather than a dead end.

Run: `uv run pytest tests/unit/migration/ -q` — Expected: PASS. Any pre-existing cutover test that relied on the old slack flips here; change the test's config to an explicit `MigrationConfig(cutover_max_lag_events=100)` where the test's *subject* is tolerance, and to a zero-lag setup where the tolerance was incidental.

- [ ] **Step 5 (green): retire the "default is 100" claims**

- `src/eventsource/migration/sync_lag_tracker.py:22`: change `config=MigrationConfig(cutover_max_lag_events=100),` to `config=MigrationConfig(),  # cutover_max_lag_events defaults to 0 (strict)`.
- `src/eventsource/migration/sync_lag_tracker.py:140`: keep the explicit `cutover_max_lag_events=50` but add a trailing comment marking it as a deliberate nonzero example: `# nonzero: accepts up to 50 events lost at the switch`.
- `src/eventsource/migration/README.md:130`: the config walkthrough's `cutover_max_lag_events=100,   # Max lag before cutover allowed` becomes `cutover_max_lag_events=0,     # strict (the default): no cutover over missing events`. Check `:405` in the same file and adjust its comment if it implies a default.
- `docs/guides/live-migration.md:134`: `- \`DUAL_WRITE -> CUTOVER\` once sync lag is under \`cutover_max_lag_events\`` becomes `... once sync lag is within \`cutover_max_lag_events\` (0 by default -- exactly zero)`. Add a warning box in the config walkthrough:

  ```markdown
  > **Warning — a nonzero `cutover_max_lag_events` accepts event loss.** The lag
  > it tolerates is not optimistic slack: `safe_lag_anchor` guarantees every
  > counted event is provably absent from the target. Writes are paused for the
  > whole cutover and nothing in the sequence copies the residue, so any lag
  > remaining at the routing switch is events the target never receives while it
  > becomes authoritative. The default is 0. If a cutover refuses because lag
  > will not drain, the remedy is `run_resync_pass` (above), not a higher
  > threshold.
  ```

- `docs/api/migration.md:102`: update the sentence so it does not imply a nonzero default.

```bash
grep -rn "cutover_max_lag_events" src/ docs/ | grep -v "docs/superpowers/" | grep "100"
```
Expected: only the CHANGELOG migration note and the ADR text that deliberately quote the old value.

- [ ] **Step 6: verify**

Run: `uv run pytest tests/unit/migration/ -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migration/ tests/unit/migration/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 7: complete ADR 0028**

Append the cutover-strictness half to `docs/adrs/0028-strict-cutover-and-in-phase-resync.md`: the default flips to 0; zero is *achievable* on the healthy path (writes pause before the lag check, so a working mirror has already drained to exactly zero) and *recoverable* on the unhealthy path via `run_resync_pass`; a nonzero value survives as an explicit, documented operator acceptance of bounded loss at the switch. Record the rejected alternatives: keep 100 and document the loss window (a default that silently discards events contradicts the module's own documented stance — defaults should be safe and overrides loud), and remove the knob (an operator consciously trading a bounded window for cutover availability is legitimate, now explicit). State plainly why §2 and §5 are one record: strict-0 without resync is stuck-until-abort; with resync it is "run one pass, then cut over."

Set Status to `Accepted` and delete the "T5 completes it" note.

Add the "Amended by ADR 0028" pointer to the Status section of `docs/adrs/0014-live-migration-cutover-semantics.md` — its `:352` text states cutover raises `CutoverLagError` unless lag is within `cutover_max_lag_events` "(default 100)". Do **not** rewrite that sentence; ADR bodies are immutable records. The Status pointer is the amendment.

Add a row for 0028 to `docs/adrs/index.md` under **Coordination and migration** (after the `0014` row), in the surrounding one-paragraph style, noting that it amends `0014`.

- [ ] **Step 8: CHANGELOG, and verify the slice's full set**

Add under `## [Unreleased]` → `### Changed`, prominently:

```markdown
- **`MigrationConfig.cutover_max_lag_events` now defaults to `0` (strict), was `100`.** Cutover no longer proceeds while any source event is provably absent from the target. Writes are paused for the entire cutover and nothing in the sequence copies the residue, so lag remaining at the routing switch was events the target never received while it became authoritative — caught only by a non-fatal post-cutover consistency check. **Behavior change:** a cutover that previously succeeded with residual lag now raises `CutoverLagError` and rolls back to `DUAL_WRITE`. To restore the old behavior, pass `MigrationConfig(cutover_max_lag_events=100)` explicitly — and understand it as accepting up to 100 lost events at the switch. When lag will not drain, the remedy is the new `MigrationCoordinator.run_resync_pass` rather than a higher threshold. See ADR 0028.
```

Then, as the last serialized task, verify the CHANGELOG carries all the slice's user-facing changes. Nine are expected — spec §1, §2, §3, §4, §5, §7, §8, §9, §10; §6 appears only if T3 surfaced an adapter fix. Confirm each is present under the right heading and that ADRs 0027 and 0028 are both `Accepted` with rows in `docs/adrs/index.md`. Report any gap to the orchestrator rather than writing another task's entry yourself.

- [ ] **Step 9: commit**

```bash
git add src/eventsource/migration/models.py src/eventsource/migration/sync_lag_tracker.py \
        src/eventsource/migration/README.md \
        tests/unit/migration/test_models.py tests/unit/migration/test_cutover_manager.py \
        tests/unit/migration/test_coordinator_resync.py \
        docs/guides/live-migration.md docs/api/migration.md \
        docs/adrs/0028-strict-cutover-and-in-phase-resync.md \
        docs/adrs/0014-live-migration-cutover-semantics.md docs/adrs/index.md CHANGELOG.md
# plus any cutover test file Step 4 changed -- name it explicitly
git commit --only <the same explicit list> \
        -m "fix: default cutover to strict zero lag"
```

Record in the commit body: which existing tests flipped and why, and the CHANGELOG completeness check's result.

---

### Task 6: Live-phase lag has a signal

Spec §3. `Subscription.lag` is `max(0, _events_seen - _events_delivered)` with a documented invariant that callers keep the two counters symmetric across any boundary. Only the catch-up runner honors it: the live runner never records seen, so lag is structurally 0 during LIVE — a stalled subscriber with events arriving is indistinguishable from a healthy idle one — and the accumulated delivered-surplus makes a later return to catch-up under-report real backlog.

Standalone. Parallel-safe with T7, T8, T9.

**Files:**
- Modify: `src/eventsource/subscriptions/runners/live.py`
- Modify: `src/eventsource/subscriptions/subscription.py` (docstrings only)
- Create tests: `tests/unit/test_live_runner_lag.py`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `Subscription.record_events_seen(count: int) -> None`, `Subscription.record_events_unseen(count: int) -> None`, `Subscription.lag -> int`. No signature changes.
- Produces: nothing consumed by another task. No public API change.

- [ ] **Step 1: read the accounting table before touching code**

Read spec §3's ruling table in full — it enumerates every terminal path in `_process_live_event` and what each must do. Then read `runners/live.py:219-379` and confirm the six paths exist as described: duplicate skip (`:285-296`), filtered with position (`:311`), filtered without position (`:299-316`), delivered with position (`:339`), delivered without position (`:349`), and the failure branch (`:355-379`) with its `continue_on_error` fork.

- [ ] **Step 2 (red): the four tests**

The live-runner tests live at `tests/unit/test_live_runner.py`, **not** under `tests/unit/subscriptions/`. Create the sibling `tests/unit/test_live_runner_lag.py` and reuse that module's fixture idiom (`event_bus`, `checkpoint_repo`, `subscriber`, `config`, `subscription`, `runner` — see its lines 60-130); do not build a second way to stand a runner up.

```python
"""Live-phase lag reports received-but-not-yet-delivered events.

`Subscription.lag` is `events_seen - events_delivered`, and its invariant
requires callers to keep the two symmetric across any boundary. The
catch-up runner did; the live runner counted deliveries without ever
counting receipts, so lag read 0 throughout LIVE no matter how far behind
the subscriber was. The bus delivery receipt is the live seen-point.
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.bus import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.subscriptions import CheckpointStrategy, Subscription, SubscriptionConfig
from eventsource.subscriptions.runners import LiveRunner

pytestmark = pytest.mark.asyncio


class LagTestEvent(DomainEvent):
    aggregate_type: str = "LagTestAggregate"


class BlockingSubscriber:
    """Blocks in `handle()` until released -- a stalled live subscriber."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.entered = asyncio.Event()
        self.handled: list[DomainEvent] = []
        self.fail = False

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LagTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.entered.set()
        await self.release.wait()
        if self.fail:
            raise ValueError("intentional failure")
        self.handled.append(event)


def _runner(subscriber: BlockingSubscriber, **config_kwargs) -> LiveRunner:  # type: ignore[no-untyped-def]
    config = SubscriptionConfig(
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        **config_kwargs,
    )
    subscription = Subscription(name="LagSubscription", config=config, subscriber=subscriber)
    return LiveRunner(
        event_bus=InMemoryEventBus(enable_tracing=False),
        checkpoint_repo=InMemoryCheckpointRepository(enable_tracing=False),
        subscription=subscription,
    )


def _event() -> LagTestEvent:
    return LagTestEvent(aggregate_id=uuid4())


class TestLiveLagSignal:
    async def test_a_stalled_subscriber_shows_nonzero_lag(self) -> None:
        subscriber = BlockingSubscriber()
        runner = _runner(subscriber)

        tasks = [asyncio.create_task(runner._handle_live_event(_event())) for _ in range(3)]
        await asyncio.wait_for(subscriber.entered.wait(), 1.0)

        assert runner.subscription.lag >= 1

        subscriber.release.set()
        await asyncio.gather(*tasks)

        assert runner.subscription.lag == 0

    async def test_seen_and_delivered_stay_symmetric(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber)

        for _ in range(4):
            await runner._handle_live_event(_event())

        assert runner.subscription._events_seen == runner.subscription._events_delivered
        assert runner.subscription.lag == 0


class TestNetZeroDisposal:
    async def test_a_filtered_event_without_a_position_leaves_no_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        # Subscribed to LagTestEvent only, so an unrelated type is filtered.
        runner = _runner(subscriber, event_types=(LagTestEvent,))

        class OtherEvent(DomainEvent):
            aggregate_type: str = "LagTestAggregate"

        await runner._handle_live_event(OtherEvent(aggregate_id=uuid4()))

        assert runner.subscription.lag == 0

    async def test_a_swallowed_failure_leaves_no_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        subscriber.fail = True
        runner = _runner(subscriber, continue_on_error=True)

        await runner._handle_live_event(_event())

        assert runner.subscription.lag == 0
        assert runner.subscription.events_failed == 1


class TestBufferedEventsCountAsLag:
    async def test_transition_buffer_depth_is_lag(self) -> None:
        subscriber = BlockingSubscriber()
        subscriber.release.set()
        runner = _runner(subscriber)
        await runner.start(buffer_events=True)

        for _ in range(3):
            await runner._handle_live_event(_event())

        assert runner.subscription.lag == 3

        await runner.process_buffer()

        assert runner.subscription.lag == 0
```

Three things to verify against the code as you write, adjusting rather than forcing: whether `continue_on_error` and `event_types` are `SubscriptionConfig` fields with those names, whether `events_failed` is a `Subscription` property or lives on the runner stats, and the exact `start(buffer_events=True)` / `process_buffer()` spelling. Add the duplicate-skip sub-case (which needs a subscription with a `last_processed_position` and an event carrying a position at or below it) only if `tests/unit/test_live_runner.py` already has a fixture that mints positioned live events — spec §3's acceptance criteria do not license new scaffolding for it.

Run: `uv run pytest tests/unit/test_live_runner_lag.py -q`

Expected: FAIL. `test_a_stalled_subscriber_shows_nonzero_lag` fails on `lag == 0`; `test_seen_and_delivered_stay_symmetric` fails with seen 0 against delivered 4; the buffer test fails on `lag == 0` where 3 is expected. The two `TestNetZeroDisposal` cases pass vacuously today — they are the regression guard for the compensations you are about to add, and they must still pass after Step 4.

- [ ] **Step 3 (green): record the receipt**

In `runners/live.py::_handle_live_event`, make the seen-record the first statement — before the buffer branch, so buffered events are seen at receipt and drain through `_process_live_event` later:

```python
    async def _handle_live_event(self, event: DomainEvent) -> None:
        """
        Handle a live event from the event bus.

        The bus delivery receipt is the live phase's seen-point: every
        received event is counted here, before branching, so live lag
        equals queue depth plus in-flight count. Every terminal path in
        `_process_live_event` that does not deliver compensates with
        `record_events_unseen(1)` -- see `Subscription.lag`'s invariant.

        Args:
            event: The event received from the bus
        """
        self._stats.events_received += 1
        await self.subscription.record_events_seen(1)
```

- [ ] **Step 4 (green): compensate the non-delivering paths**

In `_process_live_event`, add `await self.subscription.record_events_unseen(1)` at exactly three sites:

- **Duplicate skip** — immediately before the bare `return` after `self._stats.events_skipped_duplicate += 1` and its debug log.
- **Filtered without position** — inside the filter branch, in the `else` of `if position is not None:`; that is, the branch currently falls through to `return` having recorded nothing. Restructure minimally:

  ```python
                # Still update position to track progress
                if position is not None:
                    await self.subscription.record_event_processed(
                        position=position,
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )
                else:
                    # No position to record against, so nothing increments
                    # `_events_delivered` -- release the receipt instead of
                    # leaving it outstanding forever.
                    await self.subscription.record_events_unseen(1)
                return
  ```

- **Swallowed failure** — in the `except Exception` branch, after `await self.subscription.record_event_failed(e)` and after the `if not self.config.continue_on_error: raise`, so it runs only when the failure is swallowed:

  ```python
                    # Terminally disposed (possibly DLQ'd): the failure
                    # counters carry that signal, and lag must not report
                    # it as outstanding work forever.
                    await self.subscription.record_events_unseen(1)

                    logger.warning(
                        "Live event processing failed, continuing",
                        ...
                    )
  ```

  The re-raising fork deliberately leaves seen at +1: the subscription enters ERROR with one genuinely unprocessed event, which is honest.

Both delivered paths already call `record_event_processed`, which increments `_events_delivered` — they balance without change.

Run: `uv run pytest tests/unit/test_live_runner_lag.py -q` — Expected: PASS.

- [ ] **Step 5 (green): the docstrings that carry the contract**

In `subscriptions/subscription.py`:

- `record_events_seen` (line 446): extend the docstring to say seen means "read from the feed (catch-up) **or** received from the bus (live)" — one counter, two phases.
- `lag` (line 474): extend the description to state the phase-symmetric semantics — during catch-up lag rises by a read batch and falls as it drains; during live it is receipts not yet delivered, which includes both the catch-up→live transition buffer and the pause buffer, so a paused or stalled subscription shows growing lag and a healthy one shows ~0. Keep the existing invariant paragraph verbatim.

- [ ] **Step 6: verify**

Run: `uv run pytest tests/unit/subscriptions/ -q` — Expected: PASS, with the existing catch-up lag/unseen tests unmodified (the catch-up side is untouched).

If an end-to-end manager test exercises `CATCHING_UP → LIVE → pause → resume`, add the lag-invariant assertion to it rather than building new scaffolding — spec §3's acceptance criteria say so explicitly. Find it with:

```bash
grep -rln "CATCHING_UP" tests/unit/subscriptions/ | xargs grep -ln "pause\|resume"
```

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/subscriptions/ tests/unit/test_live_runner_lag.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 7: CHANGELOG and docs sweep**

```bash
grep -rn "lag" docs/guides/ docs/api/ | grep -i "catch\|subscription"
```

Update any prose describing lag as catch-up-only (the subscriptions guide and health docs are the likely sites). No ADR — this restores a documented invariant.

Add under `## [Unreleased]` → `### Fixed`:

```markdown
- **Live-phase subscription lag now reports events received but not yet delivered; it was previously always 0.** `Subscription.lag` is `events_seen - events_delivered`, and the live runner counted deliveries without ever counting receipts — so a stalled subscriber with events arriving was indistinguishable from a healthy idle one, and the accumulated delivered-surplus made a later return to catch-up under-report real backlog. Live lag now includes the catch-up→live transition buffer and the pause buffer, so a paused or stalled subscription shows growing lag.
```

- [ ] **Step 8: commit**

```bash
git add src/eventsource/subscriptions/runners/live.py \
        src/eventsource/subscriptions/subscription.py \
        tests/unit/test_live_runner_lag.py CHANGELOG.md
git commit --only src/eventsource/subscriptions/runners/live.py \
        src/eventsource/subscriptions/subscription.py \
        tests/unit/test_live_runner_lag.py CHANGELOG.md \
        -m "fix: give live-phase subscription lag a real signal"
```

Record in the commit body: that the re-raising failure fork deliberately leaves the receipt outstanding, and that the catch-up runner was not touched.

---

### Task 7: Catch-up must not end early on an all-filtered batch

Spec §9. `_process_batch` returns delivered events only; envelopes excluded by the event-type filter advance position and the reconciliation counter but not that count. The outer loop breaks on a zero return, so a batch whose envelopes are all filtered ends catch-up with `_reached_target` still False and `completed=False`, even though the store position advanced and the feed has more.

Standalone. Parallel-safe with T6, T8, T9.

**Files:**
- Modify: `src/eventsource/subscriptions/runners/catchup.py`
- Create tests: `tests/unit/subscriptions/test_catchup_batch_outcome.py`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing consumed by another task. `CatchUpResult` is untouched and `events_processed` keeps its current meaning — events **delivered** to the subscriber. That is exactly why filtered envelopes must not be folded into a single return count.

- [ ] **Step 1 (red): the two tests**

Create `tests/unit/subscriptions/test_catchup_batch_outcome.py`. The construction below mirrors `tests/unit/subscriptions/test_catchup_resumption_property.py` (same store, subscriber, and runner wiring) — read that module first and keep to its idiom.

```python
"""Catch-up terminates on reaching the target, not on an empty delivery.

An all-filtered batch delivers nothing but still advances position, so
breaking on a zero delivered-count conflated "genuinely caught up" with
"nothing matched this batch" -- and reported `completed=False` for a
heavily-filtered subscription that had in fact made progress.
"""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from eventsource.subscriptions import CheckpointStrategy, Subscription, SubscriptionConfig
from eventsource.subscriptions.runners import CatchUpRunner

pytestmark = pytest.mark.asyncio


class FilteredOutEvent(DomainEvent):
    """Type A -- never matches the subscription's filter."""

    aggregate_type: str = "BatchOutcomeAggregate"


class DeliveredEvent(DomainEvent):
    """Type B -- the only type the subscription subscribes to."""

    aggregate_type: str = "BatchOutcomeAggregate"


class RecordingSubscriber:
    def __init__(self) -> None:
        self.delivered: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [DeliveredEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.delivered.append(event)


async def _append(store: InMemoryEventStore, types: list[type[DomainEvent]]) -> None:
    """Append one event of each given type, each on its own stream."""
    for event_type in types:
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="BatchOutcomeAggregate")
        await store.append(
            stream, [event_type(aggregate_id=aggregate_id)], ExpectedVersion.no_stream()
        )


def _make_runner(store: InMemoryEventStore, subscriber: RecordingSubscriber) -> CatchUpRunner:
    config = SubscriptionConfig(
        batch_size=2,
        event_types=(DeliveredEvent,),
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )
    subscription = Subscription(
        name="BatchOutcomeSubscription",
        config=config,
        subscriber=subscriber,
    )
    return CatchUpRunner(
        store,
        InMemoryCheckpointRepository(),
        subscription,
        enable_metrics=False,
        enable_tracing=False,
    )


class TestAllFilteredBatches:
    async def test_a_wholly_filtered_feed_still_reaches_the_target(self) -> None:
        store = InMemoryEventStore()
        # Five type-A events at batch_size=2 -- three batches, every one
        # of them delivering nothing.
        await _append(store, [FilteredOutEvent] * 5)
        target = await store.current_position()
        assert target is not None

        subscriber = RecordingSubscriber()
        runner = _make_runner(store, subscriber)

        result = await runner.run_until_position(target)

        assert result.completed is True
        assert result.events_processed == 0
        assert result.final_position == target
        assert subscriber.delivered == []

    async def test_an_interior_all_filtered_batch_does_not_end_catch_up(self) -> None:
        store = InMemoryEventStore()
        # batch_size=2: [B, A], [A, A], [A, B] -- the middle batch
        # delivers nothing, and the final B is only reached if the loop
        # keeps going.
        await _append(
            store,
            [
                DeliveredEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                DeliveredEvent,
            ],
        )
        target = await store.current_position()
        assert target is not None

        subscriber = RecordingSubscriber()
        runner = _make_runner(store, subscriber)

        result = await runner.run_until_position(target)

        assert result.completed is True
        assert result.events_processed == 2
        assert len(subscriber.delivered) == 2
```

If `SubscriptionConfig` does not accept `event_types` as a keyword in the form above, read `src/eventsource/subscriptions/config.py:108` and use the field as declared — do not switch to a hand-built `EventFilter`.

Run: `uv run pytest tests/unit/subscriptions/test_catchup_batch_outcome.py -q`

Expected: FAIL. The first case returns `completed=False` after its first (all-filtered) batch; the second returns `completed=False` with `events_processed == 1`, never reaching the trailing `B`.

- [ ] **Step 2 (green): the batch outcome type**

In `src/eventsource/subscriptions/runners/catchup.py`, add a module-private frozen dataclass near the top of the module (after the imports, before the runner class). Add `from dataclasses import dataclass` to the imports if it is not already there.

```python
@dataclass(frozen=True)
class _BatchOutcome:
    """What one `_process_batch` call did.

    Two numbers, because they answer different questions and the public
    `CatchUpResult.events_processed` is defined by the second: how many
    envelopes the batch read (whether or not the filter passed them), and
    how many events reached the subscriber.
    """

    envelopes_read: int
    events_delivered: int
```

- [ ] **Step 3 (green): return it, and delete the zero-break**

Change `_process_batch`'s signature to `async def _process_batch(self, target_position: Position) -> _BatchOutcome:` and its Returns docstring to describe both fields. The empty-read early exit becomes:

```python
        envelopes = await self._read_batch_with_retry(current_position, self.config.batch_size)
        if not envelopes:
            self._reached_target = True
            return _BatchOutcome(envelopes_read=0, events_delivered=0)
```

and the final `return events_in_batch` becomes:

```python
        return _BatchOutcome(envelopes_read=len(envelopes), events_delivered=events_in_batch)
```

In `run_until_position`'s loop, replace:

```python
                    batch_result = await self._process_batch(target_position)
                    total_processed += batch_result

                    if batch_result == 0:
                        # No more events to process
                        break
```

with:

```python
                    outcome = await self._process_batch(target_position)
                    total_processed += outcome.events_delivered
```

The break is deleted outright, not conditionalized. Termination is exactly `_reached_target`, `_stop_requested`, and `not self._running` — all already in the `while` condition. Add a short comment above the loop recording why that is safe:

```python
                # Termination is `_reached_target` and the stop flags, not a
                # zero delivery: a batch can deliver nothing (every envelope
                # filtered) and still have advanced position with more feed
                # behind it. Progress is guaranteed regardless -- every
                # counted envelope, delivered or filtered, calls
                # `record_event_processed`, so the next read starts strictly
                # after it, and the position-None guard below converts the
                # only no-progress pathology into `_reached_target`.
```

Update the `run_until_position` docstring's description of when it stops, and `_process_batch`'s Returns section.

Run: `uv run pytest tests/unit/subscriptions/test_catchup_batch_outcome.py -q` — Expected: PASS.

- [ ] **Step 4: verify**

Run: `uv run pytest tests/unit/subscriptions/ -q` — Expected: PASS, including the existing unseen-reconcile tests around the `finally` block; they must pass **unmodified**. If one needs changing, you have altered behavior the spec did not sanction — stop and report.

```bash
grep -n "batch_result" src/eventsource/subscriptions/runners/catchup.py
```
Expected: no matches.

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/subscriptions/ tests/unit/subscriptions/test_catchup_batch_outcome.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 5: CHANGELOG and commit**

Add under `## [Unreleased]` → `### Fixed`:

```markdown
- **Catch-up no longer terminates early with `completed=False` when a read batch is entirely filtered out.** The loop broke on a zero *delivered* count, which conflated "the feed is exhausted" with "nothing in this batch matched the event-type filter" — so a heavily-filtered subscription reported failure despite having advanced its position with more feed behind it. Termination is now exactly reaching the target position (or a stop request). `CatchUpResult.events_processed` is unchanged and still counts events delivered to the subscriber.
```

```bash
git add src/eventsource/subscriptions/runners/catchup.py \
        tests/unit/subscriptions/test_catchup_batch_outcome.py CHANGELOG.md
git commit --only src/eventsource/subscriptions/runners/catchup.py \
        tests/unit/subscriptions/test_catchup_batch_outcome.py CHANGELOG.md \
        -m "fix: catch-up must not end early on an all-filtered batch"
```

Record in the commit body: why two numbers are returned rather than one (folding filtered envelopes into the existing count would silently redefine the public `CatchUpResult.events_processed`), and why deleting the break cannot loop forever.

---

### Task 8: One DLQ `delete_resolved_events` cutoff semantic

Spec §8. The memory adapter truncates now-UTC to midnight before subtracting `older_than_days`; the SQL adapter subtracts from `now()` directly. At `older_than_days=0` a moments-ago resolution is deleted by both SQL dialects but kept by the memory adapter until midnight. The port docstring specifies nothing, which is why two conforming-looking adapters diverged — and the divergence is currently institutionalized by a carve-out in the conformance suite's module docstring plus two per-backend tests.

Standalone. Parallel-safe with T6, T7, T9.

**Files:**
- Modify: `src/eventsource/adapters/memory/dlq.py`
- Modify: `src/eventsource/ports/dlq.py` (docstring only)
- Modify: `src/eventsource/testing/conformance_ports/dlq.py`
- Modify: `tests/unit/adapters/test_memory_dlq_conformance.py` (delete one test)
- Modify: `tests/unit/adapters/test_sqlite_conformance.py` (delete one test)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `DLQRepository.delete_resolved_events(older_than_days: int = 30) -> int` — signature unchanged.
- Produces: `DLQRepositoryConformance` gains one case, run by all three bindings (memory unit, sqlite unit, postgres integration).

- [ ] **Step 1 (red): move the case into the shared suite**

In `src/eventsource/testing/conformance_ports/dlq.py`, add the day-zero case to `DLQRepositoryConformance`, next to the existing `test_delete_resolved_events_leaves_failed_entries_intact` (which stays):

```python
    async def test_delete_resolved_events_day_zero_deletes_resolved_entries(
        self, store: DLQRepository
    ) -> None:
        """`older_than_days=0` means "resolved before now", not "before midnight".

        The cutoff is exactly `datetime.now(UTC) - timedelta(days=N)`, so a
        moments-ago resolution is already past a day-zero cutoff by the time
        the delete runs.
        """
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")

        deleted = await store.delete_resolved_events(older_than_days=0)

        assert deleted == 1
        assert await store.get_failed_event_by_id(entry.id) is None
```

Rewrite the module docstring, deleting the carve-out:

```python
"""Conformance suite for the `DLQRepository` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
Cleanup retention is pinned here rather than per backend: the cutoff is
exactly `datetime.now(UTC) - timedelta(days=older_than_days)`, so
`older_than_days=0` deletes every already-resolved entry on every
backend.
"""
```

Run: `uv run pytest tests/unit/adapters/test_memory_dlq_conformance.py tests/unit/adapters/test_sqlite_conformance.py -k delete_resolved -q`

Expected: the memory binding FAILS on the new case (`assert 0 == 1` — the entry survives until midnight); the sqlite binding PASSES it. That asymmetry is the proof the test pins the right thing.

- [ ] **Step 2 (green): the memory adapter conforms to the port**

In `src/eventsource/adapters/memory/dlq.py::delete_resolved_events`, replace:

```python
            cutoff = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            # Subtract days (simplified for in-memory implementation)
            cutoff = cutoff - timedelta(days=older_than_days)
```

with:

```python
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
```

Run: `uv run pytest tests/unit/adapters/test_memory_dlq_conformance.py -q` — Expected: PASS.

- [ ] **Step 3 (green): specify the contract on the port**

In `src/eventsource/ports/dlq.py::delete_resolved_events`, replace the docstring with one that leaves nothing for an implementation to decide:

```python
    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        The cutoff is exactly `datetime.now(UTC) -
        timedelta(days=older_than_days)`, evaluated when the call runs. An
        entry is deleted if and only if its `status` is `"resolved"` and
        its `resolved_at` is strictly before that cutoff. It is a rolling
        instant, not a calendar boundary: `older_than_days=0` therefore
        deletes every already-resolved entry, including one resolved a
        moment ago. Failed and retrying entries are never touched.

        Useful for periodic cleanup to prevent DLQ table growth.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        ...
```

- [ ] **Step 4 (green): delete the two divergence tests**

Their content now lives in the shared suite.

- `tests/unit/adapters/test_memory_dlq_conformance.py`: delete `test_memory_delete_resolved_events_cutoff_is_truncated_to_midnight` in full, including its explanatory comment. If the class body becomes empty apart from the `store` fixture, that is correct — the suite it inherits is the point.
- `tests/unit/adapters/test_sqlite_conformance.py`: delete `test_sqlite_delete_resolved_events_removes_past_cutoff_entries` from `TestSQLiteDLQRepository`, including its comment.

- [ ] **Step 5: verify**

```bash
grep -rn "delete_resolved_events" tests/unit/ src/eventsource/testing/
```
Expected: the two conformance-suite cases and nothing per-backend.

```bash
grep -rn "truncat\|midnight" src/eventsource/adapters/memory/dlq.py src/eventsource/testing/conformance_ports/dlq.py
```
Expected: no matches.

Run: `uv run pytest tests/unit/adapters/ -k "dlq or DLQ" -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/ tests/unit/adapters/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

If Docker is up, also run `uv run pytest tests/integration/ -m postgres -k dlq -q` — Expected: PASS (the SQL adapter already had this semantic).

- [ ] **Step 6: CHANGELOG and commit**

Add under `## [Unreleased]` → `### Fixed`:

```markdown
- **`InMemoryDLQRepository.delete_resolved_events` now uses the same rolling cutoff as the SQL adapter.** It truncated `now` to midnight UTC before subtracting `older_than_days`, so `older_than_days=0` kept entries resolved earlier the same day while PostgreSQL and SQLite deleted them. The port now specifies the cutoff — exactly `datetime.now(UTC) - timedelta(days=older_than_days)`, with an entry deleted iff it is resolved and `resolved_at` is strictly before it — and the conformance suite pins it for every backend. This is a behavior change on a public class, though in practice the in-memory DLQ is a test and development backend.
```

```bash
git add src/eventsource/adapters/memory/dlq.py src/eventsource/ports/dlq.py \
        src/eventsource/testing/conformance_ports/dlq.py \
        tests/unit/adapters/test_memory_dlq_conformance.py \
        tests/unit/adapters/test_sqlite_conformance.py CHANGELOG.md
git commit --only src/eventsource/adapters/memory/dlq.py src/eventsource/ports/dlq.py \
        src/eventsource/testing/conformance_ports/dlq.py \
        tests/unit/adapters/test_memory_dlq_conformance.py \
        tests/unit/adapters/test_sqlite_conformance.py CHANGELOG.md \
        -m "fix: one dlq cleanup cutoff semantic across all backends"
```

Record in the commit body: that the SQL semantic won because it is the plain-language reading, two of three backends already had it, and the memory adapter's truncation was self-described as "simplified" — the adapter conforms to the port, not the port to a shortcut.

---

### Task 9: SQLite reads must not observe a half-committed append

Spec §10. All six read paths execute on the same aiosqlite connection as `append` but outside `self._lock`, which only `append` holds. `append` is multi-statement — per-event INSERTs, then `commit()` — and each INSERT is separately awaited, so the event loop can schedule a reader's `execute` between two of them, inside the append's open implicit transaction. A read interleaved mid-append observes uncommitted rows: `read_all` can yield a torn batch, and `current_position` can mint a `Position` for a row that is then rolled back.

Standalone. Parallel-safe with T6, T7, T8.

**Files:**
- Modify: `src/eventsource/adapters/sqlite/store.py`
- Create tests: `tests/unit/adapters/test_sqlite_read_isolation.py`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `SQLiteEventStore._conn()`, `SQLiteEventStore._lock` — both existing internals.
- Produces: nothing consumed by another task. No public API change.

- [ ] **Step 1: confirm the deadlock analysis before editing**

`asyncio.Lock` is non-reentrant. Read `adapters/sqlite/store.py:196-296` (`append`) and confirm that its internal SELECTs — the pre-check at `:208-217` and the post-`IntegrityError` version re-read at `:273-282` — use `conn.execute` **directly** and not the public read helpers. They must stay that way: routing an append internal through `get_stream_version` after this change would deadlock on the lock it already holds. If any append internal calls a public read helper, stop and report.

Also confirm every read helper fetches eagerly (`fetchall()`/`fetchone()`) **before** any `yield`. That is what makes the critical sections bounded — the lock is never held across consumer iteration.

- [ ] **Step 2 (red): the deterministic interleave test**

Create `tests/unit/adapters/test_sqlite_read_isolation.py`:

```python
"""SQLite reads run under the same lock as appends.

All statements share one aiosqlite connection (required for `":memory:"`
databases, whose contents live only as long as the creating connection).
`append` is multi-statement and each INSERT is separately awaited, so
before this fix a read scheduled between two INSERTs ran inside the
append's open transaction and observed a torn batch -- or minted a
`Position` for a row that was then rolled back.

The interception below wraps the connection's `execute` beneath the
adapter, not around `_lock`, so the lock discipline under test stays
exactly what production runs.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.sqlite.store import SQLiteEventStore
from eventsource.ports import ExpectedVersion, collect
from eventsource.testing.conformance_ports._fixtures import make_event, make_stream

pytestmark = pytest.mark.asyncio


class _PausingConnection:
    """Wraps the store's connection, pausing after the first event INSERT.

    Interception sits *beneath* the adapter -- the store still calls
    `execute` on what it believes is its connection, and `self._lock` is
    untouched -- so the lock discipline under test is exactly what
    production runs.
    """

    def __init__(self, conn) -> None:  # type: ignore[no-untyped-def]
        self._conn = conn
        self.first_insert_landed = asyncio.Event()
        self.release = asyncio.Event()
        self._paused = False

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        return getattr(self._conn, name)

    async def execute(self, sql, parameters=None):  # type: ignore[no-untyped-def]
        cursor = await (
            self._conn.execute(sql, parameters)
            if parameters is not None
            else self._conn.execute(sql)
        )
        if not self._paused and "INSERT INTO events" in str(sql):
            self._paused = True
            self.first_insert_landed.set()
            await self.release.wait()
        return cursor


async def _paused_store() -> tuple[SQLiteEventStore, _PausingConnection]:
    store = SQLiteEventStore(":memory:")
    conn = await store._conn()
    pausing = _PausingConnection(conn)
    store._connection = pausing  # type: ignore[assignment]
    return store, pausing


class TestReadsDoNotObserveAnOpenAppend:
    async def test_read_all_sees_zero_or_two_events_never_one(self) -> None:
        store, pausing = await _paused_store()
        stream = make_stream()
        events = [make_event(stream.aggregate_id), make_event(stream.aggregate_id)]

        append_task = asyncio.create_task(
            store.append(stream, events, ExpectedVersion.no_stream())
        )
        await asyncio.wait_for(pausing.first_insert_landed.wait(), 1.0)

        read_task = asyncio.create_task(collect(store.read_all()))
        position_task = asyncio.create_task(store.current_position())

        # Both must block on the write lock while the append's
        # transaction is open.
        done, _ = await asyncio.wait({read_task, position_task}, timeout=0.1)
        assert done == set()

        pausing.release.set()
        await append_task
        envelopes = await read_task
        position = await position_task

        assert len(envelopes) in (0, 2)
        if envelopes:
            assert position is not None

    async def test_a_rolled_back_append_is_never_observed(self) -> None:
        store, pausing = await _paused_store()
        stream = make_stream()
        duplicate = make_event(stream.aggregate_id)

        # Land the duplicate first so the second INSERT of the next batch
        # violates the event_id unique constraint and rolls the whole
        # batch back.
        pausing.release.set()
        await store.append(stream, [duplicate], ExpectedVersion.no_stream())

        pausing.release.clear()
        pausing.first_insert_landed.clear()
        pausing._paused = False

        append_task = asyncio.create_task(
            store.append(
                stream,
                [make_event(stream.aggregate_id), duplicate],
                ExpectedVersion.exact(1),
            )
        )
        await asyncio.wait_for(pausing.first_insert_landed.wait(), 1.0)

        read_task = asyncio.create_task(collect(store.read_all()))
        done, _ = await asyncio.wait({read_task}, timeout=0.1)
        assert done == set()

        pausing.release.set()
        with pytest.raises(Exception):
            await append_task

        # Only the original event survives: the reader never saw the
        # first row of the rolled-back batch.
        assert len(await read_task) == 1
```

Two details to verify against the code as you write this, and adjust rather than force: `ExpectedVersion.exact(1)` must be the constructor the ports layer actually exposes for "stream is at version 1" (read `eventsource/ports/positions.py`), and `_PausingConnection` must forward whatever `append` calls besides `execute` (`commit`, `rollback`, `row_factory`) — `__getattr__` covers attribute access but not the `row_factory` *assignment* the store does in `_conn`, which has already happened by the time you wrap. If the pause never fires, log the SQL strings the wrapper sees before changing the match.

Red today: without the lock, the `asyncio.wait(..., timeout=0.1)` assertions fail because the reads complete immediately, observing exactly one event.

Run: `uv run pytest tests/unit/adapters/test_sqlite_read_isolation.py -q` — Expected: FAIL (the read completes mid-append and sees one event).

- [ ] **Step 3 (green): take the lock on all six read paths**

In `src/eventsource/adapters/sqlite/store.py`, wrap the `conn.execute(...)` plus its `fetchall()`/`fetchone()` in `async with self._lock:` at each of the six sites. Yielding stays outside. The two feed/stream generators become:

```python
        async with self._lock:
            cursor = await conn.execute("\n".join(query_parts), params)
            rows = await cursor.fetchall()

        for row in rows:
            yield self._row_to_envelope(row)
```

(identically in `_do_read_stream`, `_do_read_all`, and `_do_read_category`), and the three scalar paths become:

```python
    async def get_stream_version(self, stream: StreamId) -> int:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(
                """
                SELECT COALESCE(MAX(version), 0)
                FROM events
                WHERE aggregate_id = ? AND aggregate_type = ?
                """,
                (str(stream.aggregate_id), stream.category),
            )
            row = await cursor.fetchone()
        return row[0] if row else 0

    async def event_exists(self, event_id: UUID) -> bool:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute(
                "SELECT 1 FROM events WHERE event_id = ? LIMIT 1",
                (str(event_id),),
            )
            return await cursor.fetchone() is not None

    async def current_position(self) -> Position | None:
        conn = await self._conn()
        async with self._lock:
            cursor = await conn.execute("SELECT MAX(global_position) FROM events")
            row = await cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return self._codec.encode(row[0])
```

`await self._conn()` stays **outside** the lock — it has its own `_init_lock`, and nesting them is unnecessary.

Do not touch `append`.

Run: `uv run pytest tests/unit/adapters/test_sqlite_read_isolation.py -q` — Expected: PASS.

- [ ] **Step 4 (green): state the discipline in the class docstring**

In `SQLiteEventStore`'s class docstring (lines 69-80), extend the shared-connection paragraph:

```
    Uses aiosqlite for async database operations. A single connection is
    opened lazily on first use and reused for the store's lifetime --
    required for `":memory:"` databases, whose contents live only as
    long as the connection that created them stays open. A second
    connection would see a different, empty database, which is why reads
    are not given one.

    Connection discipline: *every* statement on that shared connection
    runs under `self._lock` -- reads as well as `append`. `append` is
    multi-statement (per-event INSERTs, then `commit()`), and a
    same-connection read scheduled between two of them would run inside
    its open transaction and observe a torn batch. Readers therefore
    never see an open append transaction. `asyncio.Lock` is
    non-reentrant, so `append`'s internal SELECTs use the connection
    directly and must never be routed through the public read helpers.
```

- [ ] **Step 5: verify**

```bash
grep -n "conn.execute" src/eventsource/adapters/sqlite/store.py
```
Review by inspection: every occurrence is either inside `append` (which holds the lock at `:206`), inside `_conn`/`_apply_additive_updates` (setup, before the connection is published), or inside an `async with self._lock:` block. There must be no read path touching the connection outside the lock.

Run: `uv run pytest tests/unit/adapters/test_sqlite_conformance.py tests/unit/adapters/test_sqlite_outbox.py tests/unit/adapters/test_sqlite_snapshots.py tests/unit/adapters/test_sqlite_read_isolation.py -q` — Expected: PASS. The existing sqlite conformance and state-machine suites must pass **unmodified**.

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/adapters/sqlite/ tests/unit/adapters/test_sqlite_read_isolation.py` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 6: CHANGELOG and commit**

Add under `## [Unreleased]` → `### Fixed`:

```markdown
- **SQLite reads can no longer observe a partially committed append.** All read paths shared the writer's aiosqlite connection but ran outside the write lock, and `append` is multi-statement — so a read scheduled between two of its INSERTs ran inside the open transaction and could yield a torn batch from `read_all`, or mint a `Position` from `current_position` for a row that was then rolled back. Reads now take the same lock; the connection stays shared, which `":memory:"` databases require.
```

```bash
git add src/eventsource/adapters/sqlite/store.py \
        tests/unit/adapters/test_sqlite_read_isolation.py CHANGELOG.md
git commit --only src/eventsource/adapters/sqlite/store.py \
        tests/unit/adapters/test_sqlite_read_isolation.py CHANGELOG.md \
        -m "fix: take the write lock on sqlite read paths"
```

Record in the commit body: why a separate read connection was rejected (`":memory:"` contents live only as long as the creating connection), and that the concurrency cost is ~nil because aiosqlite already serializes per-statement on one background thread — the lock only adds ordering at transaction granularity, which is the correctness requirement.
