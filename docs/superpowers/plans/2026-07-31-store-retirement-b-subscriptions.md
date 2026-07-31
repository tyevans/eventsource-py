# Legacy Store Retirement — Slice (b): Subscriptions

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `src/eventsource/subscriptions/` off the legacy `EventStore` ABC and onto the `GlobalEventFeed` port, and make the global position opaque everywhere it is held, compared, or persisted by a subscription — including the `SubscriptionPositions` port and its two checkpoint adapters. Nothing is deleted: `src/eventsource/stores/` stays in place and green, and every other legacy consumer (`migration/`, top-level `__init__.py`, the legacy stores' own tests) is untouched.

**Architecture:** Subscriptions stop asking the store for an integer and start holding an opaque `Position`. `TransitionCoordinator` and `StartFromResolver` take their watermark from `GlobalEventFeed.current_position()` (`None` = empty feed = nothing to catch up to). `CatchUpRunner` reads `read_all(from_position=current, FeedReadOptions(tenant_id, limit=batch_size))` and stops on an empty batch or the first envelope past the watermark — the `target - current` subtraction that sized every batch is replaced by a comparison. `Subscription.lag` stops subtracting positions and becomes a counter maintained by the runner. `SubscriptionPositions` persists `Position`; the SQL adapter stores `Position.to_str()` in a new nullable `position_token TEXT` column, reached through an **additive composition path** in `eventsource.migrations` that requires no edit to any existing schema file (Task 1).

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), aiosqlite, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md` (slice (b): §1.5, §1.6, §6 slice (b), §7 slice (b), §11 risks 1 and 3)

**Sibling slice:** `docs/superpowers/plans/2026-07-31-store-retirement-a-leaf-consumers.md` has LANDED. `AggregateStore` exists in `ports/store.py`; `harness`/`sync`/`bench` are on the ports; the import-linter independence contract already names the adapter packages. Do not redo any of it.

## Global Constraints

- **Unreleased software — no shims, no back-compat aliases.** Positions become `Position` outright. Do not keep an int-typed `get_position`, do not add a `position_int` property, do not accept `int` in `SubscriptionConfig.start_from`, do not reconstruct a `Position` from a legacy `global_position` row. A row carrying only the legacy int reads as `None` and the subscription restarts catch-up.
- **Positions are opaque.** Compare (`<`, `<=`, `>`, `>=`, `==`) and persist (`to_str`/`from_str`). Never subtract, never `max()`-against-a-number, never default to `0`, never compare to an int. Any "how far behind" number is a **count of events**, produced by counting, not by arithmetic on positions.
- **Nothing is deleted in this slice.** `src/eventsource/stores/` stays and must remain green. `LegacyStoreAdapter` (`src/eventsource/stores/legacy.py`) is the authoritative executable translation reference and survives until slice (d). If a translation rule here disagrees with `legacy.py`, `legacy.py` wins for the mechanics and the spec wins for the deliberate deltas — report the disagreement rather than silently choosing.
- **`migrations/` SQL files are append-only BY FILE.** Adding a new file under `src/eventsource/migrations/` is legal. Modifying an existing `.sql` file under `schemas/` or `templates/` requires Ty's sign-off and is to be avoided. Task 1's design needs no such edit; if an implementer concludes otherwise, stop and report rather than editing.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Grep sweeps include `bench/`.** Every verification grep covers `src/`, `tests/`, and `bench/`.
- **Path-scoped `git add` only.** Other agents are working concurrently in this worktree. Never `git add -A` and never `git add .`; stage exactly the files the task names. If git reports `index.lock` contention, wait 5 seconds and retry.
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Red/green TDD.** Every task that changes behavior writes or edits its failing test first, observes the failure, then implements. Steps are ordered so the red step precedes the green one; do not reorder them.
- **Hypothesis property tests** where this plan names them (Task 5's no-skip catch-up resumption property, which the spec mandates). Do not add speculative property tests elsewhere.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds — which means **every task must leave the tree type-clean**, not only the last one. This constraint is what forces Task 3's size; see its preamble.

### Behavior deltas this slice makes visible

Spec decisions (§1.5, §1.6, §6 slice (b)), not implementation choices. Repeated in the task that first encounters each.

| Delta | Legacy | Ports | Who feels it |
|---|---|---|---|
| Empty feed | `get_global_position()` returns `0` | `current_position()` returns `None` | Task 3: `TransitionCoordinator` (nothing to catch up to → complete immediately), `StartFromResolver` with `start_from="end"` |
| Resume semantics | `ReadOptions.from_position=0` means "from the start"; non-zero is exclusive | `from_position=None` means "from the start"; a `Position` is strictly-after | Task 3 catch-up loop |
| Batch sizing | `remaining = target - current; limit = min(batch_size, remaining)` | `limit=batch_size` always; stop on empty batch or first envelope `> watermark` | Task 3 `_process_batch` |
| Lag | `_max_position - last_processed_position` | count of events seen-but-not-yet-delivered in the current run | Task 2 (lands first, while positions are still ints) |
| Explicit start position | `SubscriptionConfig.start_from` accepts an `int` | it accepts a `Position` — the int form dies | Task 3 Step 8 |
| DLQ error context | `ErrorInfo.position: int`, `-1` when unknown | `ErrorInfo.position` is `Position` or `None` | Task 3 Step 7 |
| Feed event type | `StoredEvent` (`.global_position`, `.event_id`, `.event_type`) | `EventEnvelope` (`.position`, `.event.event_id`, `.event.event_type`) | Task 3 Steps 5-7 |
| PostgreSQL feed horizon | legacy feed had none | adapter feed is bounded to the xmin safe horizon: `current_position()` can lag a just-appended position under concurrent writers | Task 3 Step 4 (the watermark is a floor to catch up to, never an assertion about what exists) |
| Store spans | legacy stores emit `inmemory_event_store.*` etc. | adapters emit none (ADR 0016 amendment) | Any subscription test asserting a store span must be deleted, not retargeted |

---

### Task 1: An additive `position_token` schema path that touches no existing schema file

**Files:**
- Create: `src/eventsource/migrations/updates/002_add_position_token.sql`, `src/eventsource/migrations/updates/002_add_position_token_sqlite.sql`
- Create: `src/eventsource/migrations/additive/checkpoints_position_token.sql`, `src/eventsource/migrations/additive/sqlite/checkpoints_position_token.sql`
- Modify: `src/eventsource/migrations/__init__.py` (Python composition — not a schema file)
- Modify: `src/eventsource/adapters/sqlite/store.py` (guarded application of the additive fragment)
- Modify tests: `tests/unit/migrations/` (add a module for the composition), `tests/integration/conftest.py` (inline DDL at `:308`)

**Interfaces:**
- Consumes: nothing.
- Produces (used by Task 3): a `projection_checkpoints.position_token TEXT` column present in every environment the library creates or that the test suites create, reached without editing `schemas/all.sql`, `schemas/sqlite_all.sql`, `templates/checkpoints.sql`, or `templates/sqlite/checkpoints.sql`.

**This task resolves spec §11 open risk 3.** The reconnaissance below was already performed against this tree by the plan author; Step 1 re-verifies it rather than rediscovering it, because the whole design rests on it.

- [ ] **Step 1 (experiment): confirm the four reach points**

Run each and confirm the stated result. If any differs, STOP and report — the design below depends on all four.

```bash
grep -rn "updates/" --include='*.py' src/ tests/ bench/
```
Expected: **no matches.** `migrations/updates/001_add_global_position*.sql` are applied by nobody in-tree; they are operator documentation. An additive file placed there alone therefore reaches nothing.

```bash
grep -rn 'get_schema(' --include='*.py' src/ tests/ | grep -v 'migrations/__init__.py'
```
Expected, among others: `src/eventsource/adapters/sqlite/store.py:144` → `get_schema("all", backend="sqlite")`; `tests/conftest.py:660` and `tests/unit/adapters/test_memory_checkpoints.py:359,930` → `get_schema("checkpoints", backend="sqlite")`.

```bash
sed -n '300,325p' tests/integration/conftest.py
```
Expected: an **inline** `CREATE TABLE IF NOT EXISTS projection_checkpoints (...)`. PostgreSQL integration tests do not go through `migrations/` at all — this is a test file and may be edited freely.

Read `src/eventsource/migrations/__init__.py::get_schema`: `name == "all"` reads a single pre-combined file (`schemas/all.sql` / `schemas/sqlite_all.sql`); every other name reads one template. Nothing is composed today.

**Conclusion to record in the commit body:** an additive file under `updates/` does not reach sqlite `:memory:` stores. The reach must come from `get_schema()` composing base + additive fragments in Python. No existing `.sql` file needs editing, so **no task in this plan is blocked pending user sign-off.**

- [ ] **Step 2 (red): pin the composition contract**

Create `tests/unit/migrations/test_additive_schema.py`:

```python
"""The additive-fragment composition path (spec §11 risk 3).

`position_token` is added by a new fragment file appended at composition
time, never by editing a base schema file (migrations/ is append-only by
file). These cases pin that the column reaches every composed schema.
"""

from eventsource.migrations import get_all_schemas, get_schema


class TestPositionTokenReachesComposedSchemas:
    def test_postgres_checkpoints_schema_carries_the_column(self) -> None:
        assert "position_token" in get_schema("checkpoints")

    def test_sqlite_checkpoints_schema_carries_the_column(self) -> None:
        assert "position_token" in get_schema("checkpoints", backend="sqlite")

    def test_postgres_all_schema_carries_the_column(self) -> None:
        assert "position_token" in get_all_schemas()

    def test_sqlite_all_schema_carries_the_column(self) -> None:
        assert "position_token" in get_all_schemas(backend="sqlite")

    def test_base_schema_files_are_unmodified(self) -> None:
        """The column must come from a fragment, never from an edited base file."""
        from eventsource.migrations import _SCHEMAS_DIR, _TEMPLATES_DIR

        for path in (
            _SCHEMAS_DIR / "all.sql",
            _SCHEMAS_DIR / "sqlite_all.sql",
            _TEMPLATES_DIR / "checkpoints.sql",
            _TEMPLATES_DIR / "sqlite" / "checkpoints.sql",
        ):
            assert "position_token" not in path.read_text(), path
```

Run: `uv run pytest tests/unit/migrations/test_additive_schema.py -q` — Expected: the four reach cases FAIL, the base-file case PASSES.

- [ ] **Step 3 (green): add the fragment files**

`src/eventsource/migrations/additive/checkpoints_position_token.sql`:

```sql
-- Additive fragment: opaque position token for subscription checkpoints.
-- Appended to the checkpoints schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS position_token TEXT;

CREATE INDEX IF NOT EXISTS idx_checkpoints_position_token
ON projection_checkpoints (position_token)
WHERE position_token IS NOT NULL;
```

`src/eventsource/migrations/additive/sqlite/checkpoints_position_token.sql`:

```sql
-- Additive fragment: opaque position token for subscription checkpoints.
-- SQLite has no ADD COLUMN IF NOT EXISTS: this fragment is safe only
-- against a table that does not already have the column. Callers that
-- may re-apply a schema to an existing database must guard with
-- PRAGMA table_info (see adapters/sqlite/store.py).
ALTER TABLE projection_checkpoints ADD COLUMN position_token TEXT;
```

Also create the two operator-facing scripts, matching the `001_add_global_position*` precedent in shape and comment style (they are not applied by library code; they exist so operators can migrate an existing database): `updates/002_add_position_token.sql` (same body as the postgres fragment, plus a `COMMENT ON COLUMN` line) and `updates/002_add_position_token_sqlite.sql` (same body as the sqlite fragment).

- [ ] **Step 4 (green): compose in `migrations/__init__.py`**

Add an `_ADDITIVE_DIR = _PACKAGE_DIR / "additive"` alongside the existing `_TEMPLATES_DIR` / `_SCHEMAS_DIR`, a backend-directory resolver mirroring `_get_backend_templates_dir` (postgresql → `_ADDITIVE_DIR`, others → `_ADDITIVE_DIR / backend`), and a module-level registry keyed by `(schema name, backend)`:

```python
# Schema fragments appended at composition time. Base schema files under
# schemas/ and templates/ are append-only BY FILE: a new column arrives as
# a new fragment here plus an operator script under updates/, never as an
# edit to a base file.
_ADDITIVE_FRAGMENTS: dict[tuple[str, str], tuple[str, ...]] = {
    ("checkpoints", "postgresql"): ("checkpoints_position_token",),
    ("checkpoints", "sqlite"): ("checkpoints_position_token",),
    ("all", "postgresql"): ("checkpoints_position_token",),
    ("all", "sqlite"): ("checkpoints_position_token",),
}
```

Give `get_schema` one new keyword-only parameter, `additive: bool = True`. When true (the default, and what every existing caller gets), append each registered fragment's text for that `(name, backend)` pair after `path.read_text()`, separated by a blank line. When false, return the base file's text alone — this exists for the one caller that must apply the fragment under its own guard (Step 5). Keep the function's `FileNotFoundError`/`ValueError` behavior and its docstring's existing examples; extend the docstring with a short paragraph naming the additive path, the new parameter, and the append-only-by-file rule.

A fragment with no file for the requested backend raises `FileNotFoundError` — do **not** fall back to serving PostgreSQL DDL to SQLite. Both fragments exist here, so this should never fire.

`get_all_schemas` needs no change: it delegates to `get_schema("all", ...)` and picks up the default.

- [ ] **Step 5 (green): guard the SQLite store's re-application path**

`src/eventsource/adapters/sqlite/store.py::_conn` (`:143-146`) runs `executescript(get_schema("all", backend="sqlite"))` on every first connection to a database — including a **file-backed** database that already has the column from a previous process, where the unguarded `ALTER TABLE` raises `sqlite3.OperationalError: duplicate column name`.

This is the one caller that must apply the fragment under its own guard, which is what `additive=False` (Step 4) is for. Apply the **base** script, then the fragment only when the column is absent:

```python
            await conn.executescript(get_schema("all", backend="sqlite", additive=False))
            await self._apply_additive_updates(conn)
            await conn.commit()
```

```python
    async def _apply_additive_updates(self, conn: aiosqlite.Connection) -> None:
        """Apply additive schema fragments SQLite cannot express idempotently.

        SQLite has no `ADD COLUMN IF NOT EXISTS`, and this schema is applied
        on every first connection -- including to a file that already carries
        the column from an earlier process.
        """
        async with conn.execute("PRAGMA table_info(projection_checkpoints)") as cursor:
            columns = {row[1] for row in await cursor.fetchall()}
        if "position_token" not in columns:
            await conn.execute("ALTER TABLE projection_checkpoints ADD COLUMN position_token TEXT")
```

The base script's `CREATE TABLE IF NOT EXISTS` statements stay idempotent, and the `PRAGMA` decides the fragment. Do not use `additive=False` anywhere else: every other caller (the test conftests, operators reading `get_schema("checkpoints")`) wants the composed schema.

The acceptance is mechanical and is tested in Step 6: connecting twice to the *same file-backed* database succeeds, and the column exists after each.

- [ ] **Step 6 (green): cover the SQLite re-application and the integration DDL**

Add to `tests/unit/adapters/` (extend the existing sqlite store test module rather than creating a new one if one covers `_conn`): a case that opens a `SQLiteEventStore` on a `tmp_path` file, closes it, opens a second store on the same path, and asserts both succeed and `PRAGMA table_info(projection_checkpoints)` lists `position_token`. This is the regression that the naive design fails.

In `tests/integration/conftest.py`, add `position_token TEXT` to the inline `projection_checkpoints` DDL at `:308`. This is a test file, not a shipped schema.

- [ ] **Step 7: verify**

```bash
git status --porcelain src/eventsource/migrations/
```
Expected: only **added** files under `migrations/additive/` and `migrations/updates/`, plus a modified `migrations/__init__.py`. If any `schemas/*.sql` or `templates/*.sql` shows as modified, revert it and report.

Run: `uv run pytest tests/unit/migrations/ tests/unit/adapters/ -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/migrations/ src/eventsource/adapters/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migrations/ src/eventsource/adapters/sqlite/ tests/unit/migrations/` — Expected: clean.

- [ ] **Step 8: commit**

```bash
git add src/eventsource/migrations/ src/eventsource/adapters/sqlite/store.py \
        tests/unit/migrations/test_additive_schema.py tests/unit/adapters/ tests/integration/conftest.py
git commit -m "feat: additive position_token schema fragment and composition path"
```

Record in the commit body: the four reach points from Step 1, and the fact that no existing schema file was edited.

---

### Task 2: Lag becomes a count, not a position delta

**Files:**
- Modify: `src/eventsource/subscriptions/subscription.py`, `src/eventsource/subscriptions/runners/catchup.py`
- Modify tests: `tests/unit/subscriptions/test_subscription_state.py`, `tests/unit/test_catchup_runner.py`, `tests/unit/subscriptions/test_health.py` and `test_metrics.py` if they construct lag through `update_max_position`

**Interfaces:**
- Consumes: nothing.
- Produces (used by Task 3): `Subscription.lag: int` maintained by counting, with no reference to any position. `update_max_position` is **replaced** by `record_events_seen(count: int)`; `_max_position` is replaced by `_events_seen: int`.

This lands **before** the Position flip, while positions are still ints, precisely so the flip does not have to redesign lag at the same time. `Subscription.lag` at `subscription.py:445-452` is `max(0, self._max_position - self.last_processed_position)` — position arithmetic that no opaque token can support. Everything downstream of `lag` (`health.py:332-340`, `health_provider.py:271,303,310,493,518`, `metrics.record_lag`) consumes an `int` and needs **no change**: the number keeps its meaning ("events behind"), only its derivation changes.

- [ ] **Step 1 (red): pin lag's new derivation**

In `tests/unit/subscriptions/test_subscription_state.py`, replace the cases that drive `update_max_position` with cases driving the new counter:

- A fresh subscription has `lag == 0`.
- After `record_events_seen(10)` with nothing processed, `lag == 10`.
- After `record_events_seen(10)` and 4 `record_event_processed(...)` calls, `lag == 6`.
- Processing more events than were seen clamps at `0`, never negative.
- `lag` does not change when `last_processed_position` changes without a `record_event_processed` call (it is no longer derived from the position at all).

Run: `uv run pytest tests/unit/subscriptions/test_subscription_state.py -q` — Expected: FAIL (`AttributeError: record_events_seen`).

- [ ] **Step 2 (green): rewrite the counter in `subscription.py`**

Replace the `_max_position: int = field(default=0, repr=False)` field (`:303`) with `_events_seen: int = field(default=0, repr=False)` and `_events_delivered: int = field(default=0, repr=False)`. Replace `update_max_position` (`:432-442`) with:

```python
    async def record_events_seen(self, count: int) -> None:
        """Record events observed in the feed but not yet delivered.

        Lag is a count, not a distance: global positions are opaque tokens
        that cannot be subtracted (ADR 0019).
        """
        async with self._lock:
            self._events_seen += count
```

Increment `_events_delivered` inside `record_event_processed` (`:359-372`) under the same lock that already updates `last_processed_position`, and rewrite the `lag` property (`:445-452`) as `max(0, self._events_seen - self._events_delivered)`.

`SubscriptionStats.lag_events` (`:213`, populated at `:533`) keeps its type and its meaning.

- [ ] **Step 3 (green): feed the counter from the catch-up runner**

In `runners/catchup.py`, `run_until_position` currently calls `await self.subscription.update_max_position(target_position)` (`:211`) — a position, passed as a lag input. Delete that call. Instead, in `_process_batch`, after the batch is read and before the delivery loop, call `await self.subscription.record_events_seen(len(events))`.

This changes lag's shape: it is now "events read from the feed in this run that have not yet been delivered", which rises by a batch and falls as the batch drains, rather than a store-wide distance. That is the intended new meaning — the spec's "counting delivered-vs-seen within the current run". Say so in the `lag` docstring; do not try to reproduce the old number.

`runners/live.py` does not touch `update_max_position` (verified) and needs no change.

- [ ] **Step 4: verify**

```bash
grep -rn "update_max_position\|_max_position" src/ tests/ bench/
```
Expected: no matches.

Run: `uv run pytest tests/unit/subscriptions/ tests/unit/test_catchup_runner.py -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/subscriptions/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/subscriptions/ tests/unit/subscriptions/ tests/unit/test_catchup_runner.py` — Expected: clean.

- [ ] **Step 5: commit**

```bash
git add src/eventsource/subscriptions/subscription.py src/eventsource/subscriptions/runners/catchup.py \
        tests/unit/subscriptions/ tests/unit/test_catchup_runner.py
git commit -m "refactor: derive subscription lag by counting instead of position arithmetic"
```

---

### Task 3: The flip — positions go opaque across the subscription runtime

**Why this task is large.** Every commit must be type-clean (pre-commit runs mypy) and no shims are permitted. The moment `SubscriptionPositions.get_position` returns `Position | None`, its adapters and all five in-tree call sites must agree; the moment `Subscription.last_processed_position` holds a `Position`, the catch-up runner, the live runner, the transition coordinator, the error handler and the config's explicit-start value must agree too. There is no intermediate state that is both green and shim-free, so the flip is one commit. The steps below are ordered so an implementer can work outside-in without long stretches of red.

**Files:**
- Modify: `src/eventsource/ports/checkpoints.py`
- Modify: `src/eventsource/adapters/memory/checkpoints.py`, `src/eventsource/adapters/sql/checkpoints.py`
- Modify: `src/eventsource/subscriptions/{subscription,config,transition,lifecycle,manager,error_handling}.py`, `src/eventsource/subscriptions/runners/{catchup,live}.py`
- Modify: `src/eventsource/migration/subscription_migrator.py` (boundary only — see Step 9)
- Modify: `src/eventsource/testing/harness.py` (one docstring example at `:131-132`)
- Modify: `src/eventsource/testing/conformance_ports/checkpoints.py`
- Modify tests: `tests/unit/test_catchup_runner.py` (831), `tests/unit/test_transition.py` (971), `tests/unit/test_subscription_manager.py` (1490), `tests/unit/subscriptions/test_manager_pause_resume.py` (678), `tests/unit/subscriptions/test_error_handling.py`, `tests/unit/adapters/test_checkpoint_position.py`, `tests/unit/adapters/test_memory_checkpoints.py`, `tests/unit/adapters/test_memory_checkpoints_properties.py`, `tests/unit/test_subscription_config.py`, `tests/integration/subscriptions/conftest.py` (371) and the suites that follow from it

**Interfaces:**
- Consumes: Task 1's `position_token` column; Task 2's counted lag.
- Produces:

```python
# ports/checkpoints.py
@dataclass(frozen=True)
class CheckpointData:
    ...
    position: Position | None = None          # replaces global_position: int | None

class SubscriptionPositions(Protocol):
    async def get_position(self, subscription_id: str) -> Position | None: ...
    async def save_position(self, subscription_id: str, position: Position,
                            event_id: UUID, event_type: str) -> None: ...
```

```python
# subscriptions
Subscription.last_processed_position: Position | None          # was int, default 0
CatchUpRunner.run_until_position(target_position: Position) -> CatchUpResult
CatchUpResult.final_position: Position | None
TransitionCoordinator.watermark -> Position | None
StartFromResolver.resolve(subscription) -> Position | None
SubscriptionConfig.start_from: Literal["beginning", "end", "checkpoint"] | Position
ErrorInfo.position: Position | None
```

- [ ] **Step 1 (red): retype the port and its conformance suite**

In `src/eventsource/ports/checkpoints.py`: import `Position` from `eventsource.ports.positions` (a leaf-to-leaf import inside `ports/`, no ring crossing), rename `CheckpointData.global_position: int | None` to `position: Position | None`, and retype both `SubscriptionPositions` methods per the Interfaces block. Update the docstrings: the position is an **opaque token**, and `get_position` returns `None` both when no checkpoint exists and when the stored row predates tokens.

In `src/eventsource/testing/conformance_ports/checkpoints.py`, `SubscriptionPositionsConformance` currently saves bare ints (`save_position("S", 42, ...)`). Give the suite an overridable position factory so both adapters can supply tokens of their own shape:

```python
    def make_position(self, n: int) -> Position:
        """A comparable, persistable position for this adapter's store.

        Overridable: adapters that only accept their own store's tokens
        supply them here. The default is a synthetic single-key token,
        which any checkpoint adapter must round-trip verbatim -- the
        checkpoint table stores an opaque string and never interprets it.
        """
        return Position(store_id="conformance", key=(n,))
```

Rewrite the four existing position cases against it, and add the three the spec names:

- **Token round-trip**: a multi-element key (e.g. `Position(store_id="s", key=(7, "abc"))`) survives save/get exactly.
- **Legacy-int-only rows read as `None`**: a row written with a `global_position` but no `position_token` reads `None`. For the SQL adapter this needs a raw `INSERT`/`UPDATE` fixture hook; give the suite an overridable `async def write_legacy_int_row(self, store, subscription_id, value) -> None` that defaults to `pytest.skip("adapter has no legacy int column")` so the memory adapter opts out honestly.
- **Cross-store token isolation**: saving a token from store `A` and comparing it with a token from store `B` raises `PositionForeignError` — the checkpoint repository stores and returns tokens verbatim and does **not** validate their `store_id`; the error surfaces at the comparison, which is the honest place for it.

Run: `uv run pytest tests/unit/adapters/ -q` — Expected: FAIL (adapters still int-typed).

- [ ] **Step 2 (green): retype the memory checkpoint adapter**

`src/eventsource/adapters/memory/checkpoints.py:152-209`: `get_position` returns the stored `Position | None`; `save_position` takes a `Position` and stores the VO itself (no serialization — this adapter holds objects). The tracing attribute at `:195` currently passes the int to the span; pass `position.to_str()` (span attributes must be primitives).

- [ ] **Step 3 (green): retype the SQL checkpoint adapter onto `position_token`**

`src/eventsource/adapters/sql/checkpoints.py`:

- `get_position` (`:259-284`): `SELECT position_token FROM projection_checkpoints WHERE projection_name = :subscription_id`; return `Position.from_str(row[0])` when the token is present, `None` otherwise. A row with only the legacy `global_position` therefore reads `None` and the subscription restarts catch-up — deliberate (unreleased software; reconstructing a token would require inventing a `store_id`). Do **not** call `IntPositionCodec.decode` here: its bare-int branch exists for store-side legacy checkpoint strings, not for this table.
- `save_position` (`:286-340`): bind `position.to_str()` into a `position_token` column in both the INSERT column list and the `ON CONFLICT ... DO UPDATE SET` clause. Leave the legacy `global_position` column alone — it is not written and not read; it dies with its own slice, not this one. The span attribute `"global_position"` (`:310`) becomes `"position_token"` carrying the string.
- `get_all_checkpoints` (`:342-374`): select `position_token` instead of `global_position` and build `CheckpointData(position=Position.from_str(row[5]) if row[5] else None, ...)`.

The repository never inspects a token's `store_id` — it is an opaque string here.

- [ ] **Step 4 (green): watermark and start resolution**

`src/eventsource/subscriptions/transition.py`:

- The TYPE_CHECKING import at `:37` becomes `from eventsource.ports.store import GlobalEventFeed`; the `event_store` parameters at `:136` and `:497` retype to `"GlobalEventFeed"`.
- `TransitionCoordinator._watermark` becomes `Position | None`, sourced from `await self.event_store.current_position()` (`:202`). `None` means the feed is empty: skip catch-up and complete the transition immediately, exactly as `last_processed_position >= watermark` used to short-circuit at `:218`.
- The `:218` comparison becomes: complete immediately when the watermark is `None`, or when `last_processed_position is not None and last_processed_position >= self._watermark`. Order matters — a `None` current position means "nothing processed yet", which is *behind* any watermark, not ahead of it.
- The `watermark` property (`:430-436`) returns `Position | None`; its docstring's "0 if not yet captured" becomes "None if not yet captured or the feed is empty".
- `StartFromResolver.resolve` (`:515-545`) returns `Position | None`: `"beginning"` → `None` (read from the start), `"end"` → `await self.event_store.current_position()`, `"checkpoint"` → `await self.checkpoint_repo.get_position(...)` (already a `Position | None` after Step 1), and an explicit `Position` in the config → itself.
- Span/log attributes that carried the int position must render `position.to_str()` or `None`, never the VO's repr.

**Delta to respect (spec §1.5):** on PostgreSQL the adapter's feed is bounded to the xmin safe horizon, so `current_position()` can lag a position that was just appended. The watermark is a floor to catch up to, never an assertion about what the store contains. Do not add an assertion that the last delivered position equals the watermark.

- [ ] **Step 5 (green): the catch-up batch loop**

`src/eventsource/subscriptions/runners/catchup.py`:

- Replace the module import at `:29` (`ReadDirection, ReadOptions, StoredEvent`) with `from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions` and retype the TYPE_CHECKING `EventStore` at `:43` to `GlobalEventFeed`.
- `CatchUpResult.final_position` (`:63`) becomes `Position | None`.
- `run_until_position(target_position: Position)`. The loop condition at `:215-219` drops the `<` against an int and becomes: keep going while running, not stopped, and the last batch was non-empty. `completed` (`:238`) becomes "the last batch was empty, or the first envelope past the watermark was seen" — track it with a flag set inside `_process_batch` rather than re-deriving it from a comparison.
- `_read_batch_with_retry(from_position: Position | None, limit: int)` builds `FeedReadOptions(tenant_id=self.config.tenant_id, limit=limit)` and calls `self.event_store.read_all(from_position, options)`. `ReadDirection.FORWARD` disappears: the feed port is forward-only.
- `_process_batch` (`:281-370`) loses its arithmetic entirely:

```python
        current_position = self.subscription.last_processed_position
        envelopes = await self._read_batch_with_retry(current_position, self.config.batch_size)
        if not envelopes:
            self._reached_target = True
            return 0
        await self.subscription.record_events_seen(len(envelopes))

        for envelope in envelopes:
            if envelope.position is None or envelope.position > target_position:
                self._reached_target = True
                break
            ...
```

  `limit=self.config.batch_size` unconditionally — no `min(batch_size, remaining)`, no `batch_limit <= 0` early return. Overshoot is prevented by the per-envelope comparison, not by sizing the read. An envelope with `position is None` cannot be ordered against the watermark and must stop the loop rather than be delivered blind; in practice the three adapters always stamp a position on feed reads, so this branch is a guard, not a path.
- Every `stored_event.global_position` becomes `envelope.position`; `stored_event.event_id` / `.event_type` become `envelope.event.event_id` / `envelope.event.event_type` (spec §1.6). `record_event_processed(position=envelope.position, ...)`.
- Span attributes `ATTR_POSITION` / `ATTR_FROM_POSITION` / `ATTR_TO_POSITION` take `position.to_str()` or `None`.
- `_save_checkpoint_with_retry` (`:470`, `:500`) passes `envelope.position` straight to `save_position` — but `save_position` requires a non-`None` `Position`. Skip the checkpoint when the envelope has no position rather than inventing one.

- [ ] **Step 6 (green): the live runner**

`src/eventsource/subscriptions/runners/live.py`: `_get_event_position` (`:373-393`) returns `Position | None` — the `_global_position` attribute probe becomes a `_position` probe returning the VO if present, otherwise `None` (this path returns `None` in every in-tree configuration today; keep it that shape, do not build position lookup here). The dedup comparison at `:277` (`position <= self.subscription.last_processed_position`) must handle a `None` on either side: a subscription with no processed position has nothing to dedup against, so the guard only fires when both are present. The `ATTR_POSITION` fallback of `-1` at `:273` becomes `None`. `_maybe_checkpoint(position: Position, ...)` and its `save_position` call retype; the `position is not None` guards at `:302`, `:330` already exist and stay.

- [ ] **Step 7 (green): error handling and DLQ context**

`src/eventsource/subscriptions/error_handling.py`: the TYPE_CHECKING `StoredEvent` import (`:30`) becomes `EventEnvelope`; every `stored_event: "StoredEvent | None"` parameter (`:725`, `:789`, `:803`, `:881`) becomes `envelope: "EventEnvelope | None"`. `ErrorInfo.position` (`:308`) becomes `Position | None` and `_get_position` (`:813-816`) returns `envelope.position` or `None` — **the `-1` sentinel dies**. `ErrorInfo.to_dict` (`:324`) and the log `extra` at `:826` render `position.to_str()` when present, `None` otherwise, so structured logs and DLQ payloads stay JSON-serializable.

- [ ] **Step 8 (green): config, lifecycle, manager**

- `src/eventsource/subscriptions/config.py:24`: `StartPosition = Literal["beginning", "end", "checkpoint"] | Position`. The `int` member dies (no shims). Delete the `isinstance(self.start_from, int)` validation at `:141-145` — a `Position` validates itself on construction; there is no negative case left to reject. Update the `start_from` docstring (`:51`) and any factory in the module that passes an int.
- `src/eventsource/subscriptions/lifecycle.py`: TYPE_CHECKING import at `:37` → `GlobalEventFeed`; the parameter at `:62` retypes; `start_position` (`:112-122`) is `Position | None`; the span attribute at `:116` and the log `extra` at `:122` render the token string.
- `src/eventsource/subscriptions/manager.py`: TYPE_CHECKING import at `:78` → `GlobalEventFeed`; the `event_store` parameter at `:115` retypes; the `save_position` call at `:750-761` passes `subscription.last_processed_position` — which is now `Position | None`, and `save_position` requires a `Position`. Guard: skip the save when the subscription has no position yet, and log at debug that there was nothing to checkpoint.
- `src/eventsource/subscriptions/subscription.py`: `last_processed_position` (`:280`) becomes `Position | None = None`; `record_event_processed(position: Position | None, ...)` (`:359`); `record_event_failed(position: Position | None, ...)` (`:394`) and `FailedEvent.position` (`:168`) retype; `SubscriptionStats.position` (`:212`) becomes `Position | None`; the `to_dict` renderings at `:179`, `:233` emit `to_str()` or `None`; `__repr__` (`:740`) prints the token string or `-`.

- [ ] **Step 9 (green): the migration boundary**

`src/eventsource/migration/subscription_migrator.py` calls `checkpoint_repo.get_position` (`:427`, `:643`, `:838`) and `save_position` (`:698-700`) with ints, and feeds them to the int-keyed `PositionMapper`. Retyping the port breaks its type-check, and slice (c) is where the migrator is properly redesigned — but this slice cannot leave the tree red.

Do the **minimum** that keeps it honest and green: convert at the boundary using the store's own codec, which is exactly what a translation tool is entitled to do.

- Where the migrator reads a subscription's current position, it now receives `Position | None`. To feed the int-keyed mapper it needs the int: use `IntPositionCodec(store_id=<source store's store_id>).value_of(position)`, taking the `store_id` from the source store the migrator already holds. When `get_position` returns `None`, the subscription has no translatable checkpoint — record it in `skipped_subscriptions` with a reason, exactly as an absent checkpoint is already handled.
- Where it writes a translated position, wrap the mapper's int with `IntPositionCodec(store_id=<target store's store_id>).encode(...)`.
- Add a module-level comment (three lines, no more) stating that this int↔token conversion is a slice-(c) seam: the `position_mapping` table is int-keyed and is retyped there.

If the migrator does not currently hold enough context to name the source and target `store_id`s, STOP and report — do not fabricate one, and do not widen slice (c)'s redesign into this task.

- [ ] **Step 10: retarget and rewrite the test suites**

- REWRITE (int positions → `Position`): `tests/unit/test_catchup_runner.py`, `tests/unit/test_transition.py`, `tests/unit/test_subscription_manager.py`. Their store doubles return ints from `get_global_position()` and `StoredEvent`s from `read_all` — rebuild them on `MemoryEventStore` where a real store will do, and on `GlobalEventFeed`-shaped fakes yielding `EventEnvelope`s where a fake is genuinely needed. Cases asserting `final_position == <int>` assert against a token obtained from the store. Cases asserting an empty store's watermark is `0` assert `is None`.
- REWRITE: `tests/unit/subscriptions/test_error_handling.py` for `ErrorInfo.position` (the `-1` sentinel cases become `None` cases), and `tests/unit/test_subscription_config.py` for `start_from` (int cases die; add a `Position` case and one asserting an int is rejected by mypy — as a comment, not a runtime assertion).
- RETARGET: `tests/unit/subscriptions/test_manager_pause_resume.py`, `tests/integration/subscriptions/conftest.py` — after the conftest moves to the memory adapter, `test_advanced_features.py` and `test_resilience.py` follow nearly free; check them and fix only what breaks.
- RETARGET: `tests/unit/adapters/test_checkpoint_position.py`, `test_memory_checkpoints.py`, `test_memory_checkpoints_properties.py` — `save_position(..., 42, ...)` becomes a `Position`; `CheckpointData.global_position` becomes `.position`.
- Any case asserting a store-level span (`inmemory_event_store.*`) must be **deleted**, not retargeted — the adapters emit none (ADR 0016 amendment).

- [ ] **Step 11: verify**

```bash
grep -rn "stores\.interface\|StoredEvent\|ReadOptions\|get_global_position\|global_position" src/eventsource/subscriptions/ src/eventsource/ports/checkpoints.py src/eventsource/adapters/memory/checkpoints.py src/eventsource/adapters/sql/checkpoints.py
```
Expected: no matches (the SQL adapter keeps no reference to the legacy column at all).

```bash
grep -rn "last_processed_position\|target_position\|watermark" src/eventsource/subscriptions/ | grep -E "[-+*/]|min\(|max\(|>= 0|== 0"
```
Expected: no arithmetic on positions. `max(0, ...)` on the **lag counter** in `subscription.py` is fine and expected — confirm each hit is a count, not a position.

```bash
grep -rln "stores\.interface\|append_events" src/eventsource/migration/ src/eventsource/__init__.py
```
Expected: several matches — slices (c) and (d) still own those. An empty result means this slice overreached.

- [ ] **Step 12: run targeted tests**

Run: `uv run pytest tests/unit/test_catchup_runner.py tests/unit/test_transition.py tests/unit/test_subscription_manager.py tests/unit/test_subscription_config.py tests/unit/subscriptions/ -q` — Expected: PASS.
Run: `uv run pytest tests/unit/adapters/ tests/unit/ports/ -q` — Expected: PASS.
Run: `uv run pytest tests/integration/subscriptions/ -q` — Expected: PASS (Docker services required; if unavailable, report rather than skipping silently).
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean (whole package: this task touches ports, adapters, subscriptions and migration).
Run: `uv run ruff check src/eventsource/ tests/unit/ tests/integration/subscriptions/` — Expected: clean.

- [ ] **Step 13: commit**

```bash
git add src/eventsource/ports/checkpoints.py src/eventsource/adapters/memory/checkpoints.py \
        src/eventsource/adapters/sql/checkpoints.py src/eventsource/subscriptions/ \
        src/eventsource/migration/subscription_migrator.py src/eventsource/testing/harness.py \
        src/eventsource/testing/conformance_ports/checkpoints.py \
        tests/unit/test_catchup_runner.py tests/unit/test_transition.py \
        tests/unit/test_subscription_manager.py tests/unit/test_subscription_config.py \
        tests/unit/subscriptions/ tests/unit/adapters/ tests/integration/subscriptions/
git commit -m "refactor: retype subscriptions and checkpoint positions onto the position value object"
```

Record in the commit body: the migrator boundary conversion (Step 9) as a slice-(c) seam, and the count of deleted store-span cases if any.

---

### Task 4: Position round-trip coverage for the checkpoint adapters

**Files:**
- Modify tests: `tests/unit/adapters/test_memory_checkpoint_conformance.py`, the SQL checkpoint conformance module alongside it (locate with `ls tests/unit/adapters/ | grep -i checkpoint`), `tests/unit/adapters/test_checkpoint_position.py`

**Interfaces:**
- Consumes: Task 3's retyped port, adapters, and conformance suite.
- Produces: no source change — the acceptance evidence for the retype.

Task 3 generalized `SubscriptionPositionsConformance`; this task makes both adapters actually exercise the three new cases, which requires each conformance subclass to supply its hooks.

- [ ] **Step 1: wire the memory adapter's conformance subclass**

`make_position` can stay at the suite default (the memory adapter stores the VO verbatim). Confirm `write_legacy_int_row` is left unimplemented so the legacy-row case skips with its explicit reason — a skip here is correct and must not be silenced.

- [ ] **Step 2: wire the SQL adapter's conformance subclass**

Override `write_legacy_int_row` to `INSERT` a checkpoint row with `global_position` set and `position_token` NULL, then assert through the suite that `get_position` reads `None`. This is the case that proves the "restart catch-up rather than guess a store_id" decision is actually implemented, and it is the single most valuable new test in this slice.

- [ ] **Step 3: extend the codec round-trip coverage**

Spec §7 slice (b): position codec round-trips are already covered by `tests/unit/adapters/` and `tests/unit/ports/`; extend with a `to_str`/`from_str` ↔ checkpoint-table round-trip. Add to `tests/unit/adapters/test_checkpoint_position.py` a case that takes a position produced by a real `SQLiteEventStore` append, saves it through `SQLCheckpointRepository.save_position`, reads it back, and asserts it equals the original and still compares correctly against a later position from the same store.

- [ ] **Step 4: run targeted tests**

Run: `uv run pytest tests/unit/adapters/ -q` — Expected: PASS, with exactly one skip (the memory adapter's legacy-row case).
Run: `uv run ruff check tests/unit/adapters/` — Expected: clean.

- [ ] **Step 5: commit**

```bash
git add tests/unit/adapters/
git commit -m "test: cover position token round-trip and legacy int rows in checkpoint adapters"
```

---

### Task 5: The no-skip catch-up resumption property

**Files:**
- Create: `tests/unit/subscriptions/test_catchup_resumption_property.py`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: the executable form of ADR 0019 decision 5 at the subscriptions layer. Spec-mandated (§7 slice (b)); it is not optional and not to be replaced by example-based cases.

- [ ] **Step 1: write the property**

Against a real `MemoryEventStore` and a real `InMemoryCheckpointRepository`:

```python
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    batches=st.lists(st.integers(min_value=1, max_value=6), min_size=1, max_size=8),
    batch_size=st.integers(min_value=1, max_value=5),
    restart_after=st.integers(min_value=0, max_value=20),
)
@pytest.mark.asyncio
async def test_catchup_delivers_every_event_exactly_once_across_a_restart(
    batches: list[int], batch_size: int, restart_after: int
) -> None:
    """Random stream shapes, batch sizes and restart points: every event is
    delivered exactly once, in position order.

    A resumption that reads `from_position=checkpoint` inclusively would
    redeliver; one that advances past an undelivered event would skip. Both
    are single-comparison errors in the batch loop, and both fail here.
    """
```

Shape: append `sum(batches)` events across the given batch shapes into distinct streams; run a `CatchUpRunner` with `CheckpointStrategy.EVERY_EVENT` and `batch_size` until it has delivered `restart_after` events, then stop it; construct a **second** subscription and runner over the same store and the same checkpoint repository, resolving its start position from the checkpoint; run to completion. Assert the concatenation of both subscribers' delivered event ids equals the store's feed order (`[e.event.event_id async for e in store.read_all()]`) with no duplicates and no gaps.

Use `hypothesis.assume` sparingly; prefer clamping `restart_after` to the total rather than discarding examples.

- [ ] **Step 2: run it**

Run: `uv run pytest tests/unit/subscriptions/test_catchup_resumption_property.py -q` — Expected: PASS.
Run: `uv run pytest tests/unit/subscriptions/test_catchup_resumption_property.py -q -p no:randomly --hypothesis-seed=0` — Expected: PASS (a deterministic second opinion; if the two disagree, the property has a state leak between examples — fix the fixture, do not loosen the property).
Run: `uv run ruff check tests/unit/subscriptions/test_catchup_resumption_property.py` — Expected: clean.

- [ ] **Step 3: commit**

```bash
git add tests/unit/subscriptions/test_catchup_resumption_property.py
git commit -m "test: property test for no-skip catch-up resumption across restarts"
```

---

### Task 6: ADR 0024 amendment, mutation config, docs, final sweep

**Files:**
- Modify: `docs/adrs/0024-projection-persistence-ports.md` (Status section only), `pyproject.toml` (`[tool.mutmut]` only), `CHANGELOG.md`
- Modify: `docs/` pages that document subscription start positions or checkpoint columns (locate in Step 3)

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: no code surface.

**Concurrency caveat:** other agents are editing `pyproject.toml`. Locate the `[tool.mutmut]` block by its TOML header, not by line number, and re-read the file immediately before editing. If an edit is already present, verify rather than duplicate and say so in the commit body.

- [ ] **Step 1: amend ADR 0024's Status**

Per `.claude/rules/definition-of-done.md`, ADR bodies are immutable records: **do not** rewrite 0024's Decision about `position: int`. Add to its **Status** section only:

```
Amended by ADR 0025 (legacy store retirement): `SubscriptionPositions` and
`CheckpointData` carry the opaque `Position` value object rather than an
integer global position. The integer was the legacy store's global position
leaking through a port.
```

ADR 0025 itself is written in slice (d) (spec §9) — do not create it here. If `docs/adrs/` has gained an ADR numbered 0025 or higher since the spec was written, use the next free number in this note and report the renumber.

- [ ] **Step 2: extend the mutation-testing selection**

Spec §7: slice (b) adds `src/eventsource/subscriptions` to `only_mutate`, and `tests/unit/test_catchup_runner.py`, `tests/unit/test_transition.py`, `tests/unit/subscriptions/` to `pytest_add_cli_args_test_selection`. The position-comparison logic in the batch loop is exactly the off-by-one surface mutmut exists for. Make both edits.

Run: `uv run python -c "import tomllib,pathlib; c=tomllib.loads(pathlib.Path('pyproject.toml').read_text()); print(c['tool']['mutmut'])"` and confirm both keys read back as intended. Do not run mutmut itself — that is the orchestrator's call.

- [ ] **Step 3: docs and changelog**

```bash
grep -rn "start_from\|global_position\|get_global_position" docs/ examples/ README.md
```

For each hit in prose or example code: `start_from` integers become `Position` (or `"beginning"`, which is what most examples actually mean); `global_position` on a checkpoint becomes the position token. Do not touch `docs/adrs/` entries other than 0024's Status, and do not touch documentation of the legacy stores themselves — those pages are slice (d)'s.

Add a `CHANGELOG.md` entry for this slice naming the deltas loudly: opaque positions in subscriptions and checkpoints; `SubscriptionConfig.start_from` no longer accepts an int; checkpoint rows carrying only the legacy `global_position` read as no-position and restart catch-up; lag is a count of undelivered events within the current run rather than a store-wide distance.

- [ ] **Step 4: final sweep**

```bash
grep -rn "get_global_position\|ReadOptions\|StoredEvent" src/eventsource/subscriptions/ src/eventsource/ports/ src/eventsource/adapters/
```
Expected: no matches.

```bash
grep -rn "position" src/eventsource/subscriptions/ | grep -E "== 0|>= 0|position - |- position|min\(|sum\("
```
Expected: no matches, or only hits on counters. Inspect each.

```bash
git diff main --stat -- src/eventsource/stores/
```
Expected: empty. The legacy stores are frozen in this slice.

- [ ] **Step 5: commit**

```bash
git add docs/adrs/0024-projection-persistence-ports.md pyproject.toml CHANGELOG.md docs/
git commit -m "docs: amend adr 0024 for opaque positions and extend mutation selection"
```

---

## Slice Completion Criteria

The orchestrator runs these; implementers do not.

- [ ] `make check` passes (lint, mypy, import-linter, bandit/pip-audit, full unit suite).
- [ ] Integration suites pass with Docker services up: `uv run pytest tests/integration/ -v`.
- [ ] The full suite runs at least once in default random order (do not pass `-p no:randomly`).
- [ ] **Spec §11 risk 1 — catch-up throughput on PostgreSQL under the xmin horizon.** Before this slice merges, run the `store.read_all`-family bench scenarios against postgres with concurrent writers and bound the regression. The adapter runs `pg_current_snapshot()` per batch where the legacy catchup path had no horizon; plan-time xmin cast cost varies by version and load, so this cannot be settled by reading code. If a catch-up-loop scenario does not exist in `bench/scenarios/`, the read_all scenarios are an acceptable proxy — record which was used.
- [ ] `src/eventsource/stores/` is byte-identical to its pre-slice state.
- [ ] No existing file under `src/eventsource/migrations/schemas/` or `templates/` was modified (`git diff main --stat -- src/eventsource/migrations/` shows additions and `__init__.py` only).
- [ ] `src/eventsource/migration/` beyond the `subscription_migrator.py` boundary conversion is untouched; `src/eventsource/__init__.py`'s store section is untouched.
- [ ] `eventsource.__all__` is unchanged (this slice adds no public name).

## Plan Self-Review

- **Spec coverage, slice (b) only.** Every row of the spec's §6 slice (b) migration table has a task: `lifecycle.py:37,62` → Task 3 Step 8; `transition.py:37,136,202,497,540` → Task 3 Step 4; `manager.py:78,115` → Task 3 Step 8; `runners/catchup.py:29,43,95,308-407` → Task 3 Step 5; `error_handling.py:30,725-816` → Task 3 Step 7; `ports/checkpoints.py` → Task 3 Step 1; `adapters/memory/checkpoints.py` and `adapters/sql/checkpoints.py` → Task 3 Steps 2-3. The five numbered changes in §6 slice (b) map to Task 3 Steps 4 (watermark), 5 (batch loop), 1-3 (`SubscriptionPositions` retype), 7 (`error_handling`), and Task 2 (state/metrics lag). The §7 slice (b) test inventory is fully allocated: RETARGET and REWRITE items to Task 3 Step 10, the checkpoint conformance extensions to Tasks 3 Step 1 and 4, the hypothesis no-skip property to Task 5, the codec round-trip extension to Task 4 Step 3. §7's mutation-testing additions and the ADR 0024 amendment → Task 6. §11 risk 3 → Task 1; §11 risk 1 → Slice Completion Criteria. Nothing from slices (a), (c), or (d) appears here except the §9 Step-9 boundary conversion, which exists only to keep the tree green and is labelled as a (c) seam.
- **Placeholder scan.** No TODO, TBD, or `???` remains. Every code block is complete or is an explicit, bounded instruction over code the implementer is reading anyway.
- **Signature consistency.** `Position` (never `position_token` as a type) is the VO throughout; `position_token` names only the SQL column and its span attribute. `get_position -> Position | None` and `save_position(..., position: Position, ...)` are spelled identically in Task 3's Interfaces block, Step 1, Step 2 and Step 3. `current_position()` (port) is never called `get_global_position` after Task 3. `record_events_seen` is spelled identically in Task 2 Steps 1-3 and Task 3 Step 5. `MemoryEventStore` (not `InMemoryEventStore`) is used throughout — the rename is slice (d)'s.

## Spec Gaps Found (report, do not silently deviate)

1. **§6 slice (b) item 6 understates the lag problem.** It says the events-behind gauge changes are "confined to `subscription.py`/`metrics.py` field types". In fact `Subscription.lag` (`subscription.py:445-452`) is `max(0, self._max_position - self.last_processed_position)` — genuine position subtraction, fed by `update_max_position(target_position)` from `catchup.py:211`. It is a behavior redesign, not a field retype, and it is large enough to land on its own: Task 2 does it first, while positions are still ints, so Task 3 does not have to solve two problems at once. `metrics.py` and `health*.py` need no change at all — they consume an `int` whose meaning is preserved.
2. **`SubscriptionConfig.start_from` accepts an explicit `int` position (`config.py:24`, validated at `:141-145`) and the spec never addresses it.** With opaque positions the int form is unrepresentable. Resolved in favor of `Literal[...] | Position` with the int member deleted outright (Task 3 Step 8) — consistent with the no-shims rule. Flagging because it is a user-facing config type change the spec's §6 table does not list.
3. **`migration/subscription_migrator.py` consumes `SubscriptionPositions` (`:427`, `:643`, `:698-700`, `:838`) but the spec assigns it to slice (c).** Retyping the port in (b) breaks the migrator's type-check immediately, and every commit must be type-clean. Resolved with a minimal `IntPositionCodec` boundary conversion in Task 3 Step 9, explicitly labelled as a slice-(c) seam. The alternative — pulling the migrator's redesign forward — would widen this slice into (c)'s scope.
4. **§6 slice (b) item 4 says the `position_token` column is added by "append-only update scripts (`migrations/updates/002_add_position_token.sql` + `_sqlite` variant)".** Verified: nothing in `src/`, `tests/` or `bench/` reads `migrations/updates/` — those scripts are operator documentation and reach no test or runtime environment. The column must arrive through a **composition** change in `migrations/__init__.py` plus new fragment files. Task 1 does both (the update scripts are still written, for operators). This is the resolution of open risk 3, and it requires **no** edit to an existing schema file, so no task in this plan is blocked pending user sign-off.
5. **The spec's §6 slice (b) file list omits `runners/live.py`, `config.py` and `subscription.py`**, all of which hold or compare positions (`live.py:254-342,373-393`; `config.py:24`; `subscription.py:168,212,280,359,394,532,740`). They are in scope by necessity and are covered by Task 3 Steps 6, 8 and Task 2.
6. **`SubscriptionPositionsConformance` (`testing/conformance_ports/checkpoints.py:98-125`) hard-codes bare-int positions** (`save_position("S", 42, ...)`). The spec asks for new conformance cases but does not note that the existing four must be generalized first. Task 3 Step 1 adds an overridable `make_position` factory and an opt-out `write_legacy_int_row` hook so the memory adapter can skip the legacy-row case honestly rather than faking a column it does not have.
