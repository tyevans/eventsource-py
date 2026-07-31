# Legacy Store Retirement — Slice (c): Migration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `src/eventsource/migration/` off the legacy `EventStore` ABC and onto the ports surface, and make every position it holds, compares, persists, or maps an opaque `Position`. This includes retiring the slice-(c) seam that slice (b) deliberately left behind: `migration/subscription_migrator.py` imports `adapters/_sql/positions.IntPositionCodec` and both it and `MigrationCoordinator` take required keyword-only `source_store_id` / `target_store_id` / `source_position_store_id` / `target_position_store_id` parameters whose docstrings name this slice as the place they die. Nothing is deleted: `src/eventsource/stores/` stays in place and green, and slice (d) still owns the deletion, the `MemoryEventStore` rename, and the public-API swap.

**Architecture:** Migration stops being a legacy-ABC citizen. `TenantStoreRouter` and `DualWriteInterceptor` stop *subclassing* `EventStore` and become ports-shaped structural `FullEventStore` wrappers (a routing wrapper and a source-then-best-effort-target wrapper); `BulkCopier` reads the source `GlobalEventFeed` and appends through `EventAppender`; `ConsistencyVerifier` compares `EventEnvelope`s; `SyncLagTracker` stops subtracting two stores' positions — which is not merely banned but *meaningless*, since positions from different stores raise `PositionForeignError` on comparison — and reports a bounded count of source events not yet copied. The `migration_position_mappings` table becomes token-keyed, and once it is, the int↔token codecs at the subscription-migrator boundary have nothing left to convert: positions flow from the checkpoint table through the mapper to the checkpoint table as tokens, end to end. That is what retires the seam, and it is why neither the migrator nor the coordinator needs a declared `store_id` afterwards.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters and migration repositories only), asyncpg, aiosqlite, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md` (slice (c): §1.1, §1.2, §1.3, §1.5, §1.6, §2, §3 `migration/`, §6 slice (c), §7 slice (c), §11 risks 2, 4 and 5)

**Sibling slices:** `docs/superpowers/plans/2026-07-31-store-retirement-a-leaf-consumers.md` and `docs/superpowers/plans/2026-07-31-store-retirement-b-subscriptions.md` have both LANDED. `AggregateStore` exists in `ports/store.py`; `harness`/`sync`/`bench` are on the ports; subscriptions and the checkpoint adapters hold opaque `Position`s; `SubscriptionPositions.get_position` already returns `Position | None`; the additive-fragment composition path exists in `src/eventsource/migrations/__init__.py` (`_ADDITIVE_FRAGMENTS`, `_ADDITIVE_DIR`, `get_schema(..., additive=...)`) and Task 1 below reuses it exactly as it was designed to be reused. Do not redo any of it.

## Global Constraints

- **Unreleased software — no shims, no back-compat aliases.** No int-accepting overload survives anywhere in `migration/`. Do not keep `PositionMapper.translate_position(source_position: int)` alongside a `Position` form, do not leave `Migration.last_source_position` as an int with a token twin, do not keep `TenantStoreRouter.get_events`. When a capability is dropped (spec §2: `aggregate_type=None` cross-type lookup), it is dropped, not stubbed.
- **Positions are opaque.** Compare (`<`, `<=`, `>`, `>=`, `==`) and persist (`to_str`/`from_str`). Never subtract, never `max()` two positions into a number, never default to `0`, never compare to an int. Cross-store comparison raises `PositionForeignError` by design — any code that would compare a source position to a target position is wrong, not merely unlucky. Every "how far behind" number is a **count of events**, produced by counting.
- **Nothing is deleted in this slice.** `src/eventsource/stores/` stays and must remain green; `LegacyStoreAdapter` (`src/eventsource/stores/legacy.py`) is the authoritative executable translation reference and survives until slice (d). If a translation rule here disagrees with `legacy.py`, `legacy.py` wins for the mechanics and the spec wins for the deliberate deltas — report the disagreement rather than silently choosing.
- **`migrations/` SQL files are append-only BY FILE.** Adding a new file under `src/eventsource/migrations/` is legal. Modifying an existing `.sql` file under `schemas/` or `templates/` requires Ty's sign-off and is to be avoided. Task 1's design needs no such edit; if an implementer concludes otherwise, stop and report rather than editing.
- **The catch-up early-exit backlog item is NOT in scope.** It is a pre-existing item recorded in the backlog and belongs to neither this slice nor its review. Do not touch `subscriptions/runners/catchup.py`.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Grep sweeps include `bench/`.** Every verification grep covers `src/`, `tests/`, and `bench/`.
- **Path-scoped `git add` only.** Other agents are working concurrently in this worktree. Never `git add -A` and never `git add .`; stage exactly the files the task names. If git reports `index.lock` contention, wait 5 seconds and retry.
- **No live-src mutation probes.** Do not "try an edit and see what mypy says" on shared source while other agents are active. Reason from the code you read; if you must experiment, do it on a copy under `$CLAUDE_JOB_DIR/tmp`.
- **Implementers do not push.** Commit only. Branch pushes and PRs are the orchestrator's.
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Red/green TDD.** Every task that changes behavior writes or edits its failing test first, observes the failure, then implements. Steps are ordered so the red step precedes the green one; do not reorder them.
- **Hypothesis property tests** where this plan names them (Task 5's bulk-copy resume idempotency property, which the spec mandates). Do not add speculative property tests elsewhere.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- **import-linter must be green per task, not only at the end.** `uv run lint-imports` is listed in every task's verify step. Task 3 cleans up the recorded ring violation (`migration/` importing `adapters/_sql/`); note that this is documentation-level debt (ADR 0024 amendment + module docstring), not lint-enforced (no import-linter contract covers `eventsource.migration`), and Task 3 removes it regardless.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds — which means **every task must leave the tree type-clean**, not only the last one. This constraint is what forces Task 4's size; see its preamble.

### Behavior deltas this slice makes visible

Spec decisions (§1.1, §1.3, §1.5, §1.6, §2, §6 slice (c)), not implementation choices. Repeated in the task that first encounters each.

| Delta | Legacy | Ports | Who feels it |
|---|---|---|---|
| `AppendResult.position` | position of the **last** appended event | position of the **first** appended event | Task 4: `BulkCopier._write_batch`'s `result.global_position - len(agg_events) + i + 1` arithmetic is invalid in both directions and is replaced, not adjusted |
| Duplicate `event_id` | legacy in-memory/postgresql silently **skip** an already-stored id | adapters raise `DuplicateEventError` | Task 4 `BulkCopier`: catch and count as already-copied; this is what makes a resumed copy converge, and Task 5 is its property test |
| Empty append batch | returns `AppendResult.successful(expected_version)` | raises `ValueError` | Task 4: `_write_batch` groups by aggregate and can only produce non-empty groups — the guard is re-verified, not assumed |
| Append conflict | `AppendResult.success=False` / `.conflict` | `OptimisticLockError` raised | Task 4: the `if not source_result.success` branch in `dual_write.py:369-374` and every `result.conflict` read is deleted, not translated; test doubles returning `AppendResult.conflicted(...)` become raising doubles (spec §11 risk 5) |
| Empty store | `get_global_position()` returns `0` | `current_position()` returns `None` | Task 4: `SyncLagTracker`, `cutover.py:367` |
| Cross-store position math | `source_position - target_position` | `PositionForeignError` on the comparison | Task 4: lag becomes a bounded count-behind on the **source** feed |
| Cross-type lookup | `get_events(aggregate_id, aggregate_type=None)` | no port; capability dropped (spec §2) | Task 4: `TenantStoreRouter.get_events` and `DualWriteInterceptor.get_events` cease to exist; `tests/unit/migration/test_dual_write.py:731` is rewritten against the ports shape |
| Category read | filters/orders on `event.occurred_at`, exclusive `>` | filters/orders on `stored_at`, inclusive `>=`, position tie-break | Task 4: `read_category` routing in `TenantStoreRouter` |
| Feed event type | `StoredEvent` (`.global_position`, `.stream_position`, `.aggregate_id`) | `EventEnvelope` (`.position`, `.stream_version`, `.stream_id.aggregate_id`) | Tasks 4 (`BulkCopier`, `ConsistencyVerifier`) |
| PostgreSQL feed horizon | legacy feed had none | adapter feed is bounded to the xmin safe horizon: `current_position()` can lag a just-appended position under concurrent writers | Task 4: lag and convergence are advisory signals, never assertions about what the store contains |
| Store spans | legacy stores emit `postgresql_event_store.*` etc. | adapters emit none (ADR 0016 amendment) | Any migration test asserting a store span must be deleted, not retargeted |

---

### Task 1: Token columns for the migration schema

**Files:**
- Create: `src/eventsource/migrations/additive/migration_position_tokens.sql`
- Create: `src/eventsource/migrations/updates/003_add_migration_position_tokens.sql`
- Modify: `src/eventsource/migrations/__init__.py` (registry entry only — Python composition, not a schema file)
- Modify tests: `tests/unit/migrations/test_additive_schema.py`, `tests/unit/migrations/test_migration_schema.py`

**Interfaces:**
- Consumes: the `_ADDITIVE_FRAGMENTS` registry and `get_schema(..., additive=...)` built by slice (b) Task 1.
- Produces (used by Tasks 2-4): `migration_position_mappings.source_position_token TEXT` / `.target_position_token TEXT` and `tenant_migrations.last_source_position_token TEXT` / `.last_target_position_token TEXT`, present in `get_schema("migration")`, reached without editing `templates/migration.sql`.

**This task resolves spec §11 open risk 4.** The reconnaissance was already performed against this tree by the plan author; Step 1 re-verifies it rather than rediscovering it, because the rest of the slice rests on it.

- [ ] **Step 1 (experiment): confirm where the migration DDL lives and who applies it**

Run each and confirm the stated result. If any differs, STOP and report.

```bash
grep -rln "migration_position_mappings" --include='*.sql' src/
```
Expected: exactly `src/eventsource/migrations/templates/migration.sql`. The migration tables live in a **template**, not in `schemas/all.sql`.

```bash
grep -c "migration_position_mappings" src/eventsource/migrations/schemas/all.sql src/eventsource/migrations/schemas/sqlite_all.sql
```
Expected: `0` for both. The migration tables are deliberately not part of the combined schema — they are opt-in operator DDL.

```bash
ls src/eventsource/migrations/templates/sqlite/
```
Expected: no `migration.sql`. The migration schema is **PostgreSQL-only**; `get_schema("migration", backend="sqlite")` raises `ValueError` today and must keep doing so. Register no sqlite fragment.

```bash
grep -rn "^class \|PositionMappingRepository\|MigrationRepository" src/eventsource/migration/repositories/*.py | grep "^.*:class "
```
Expected: each repository has exactly a `Protocol` and a `PostgreSQL*` implementation — no in-memory or SQLite variant. Only PostgreSQL DDL is in play.

**Conclusion to record in the commit body:** the migration DDL is `templates/migration.sql`, PostgreSQL-only, applied by operators and by `tests/integration/migrations/test_migration_schema_postgresql.py`; unit tests exercise the repositories against mocks. The token columns therefore arrive through the same additive-fragment mechanism slice (b) built, registered for `("migration", "postgresql")` only.

- [ ] **Step 2 (red): pin the composition contract**

Extend `tests/unit/migrations/test_additive_schema.py` with a class covering the migration fragment:

- `get_schema("migration")` contains `source_position_token`, `target_position_token`, `last_source_position_token`, `last_target_position_token`.
- `templates/migration.sql` contains **none** of those four names (the column must come from a fragment, never from an edited base file) — this mirrors the existing `test_base_schema_files_are_unmodified` case and must be written in the same shape.
- `get_schema("migration", additive=False)` contains none of them.
- `get_schema("migration", backend="sqlite")` still raises `ValueError` (the migration schema has no SQLite variant and gains none here).

Run: `uv run pytest tests/unit/migrations/test_additive_schema.py -q` — Expected: the reach cases FAIL, the base-file and sqlite cases PASS.

- [ ] **Step 3 (green): write the fragment**

`src/eventsource/migrations/additive/migration_position_tokens.sql`, in the comment style of `additive/checkpoints_position_token.sql`. The legacy `global_position` checkpoint column is now frozen (adapters write only position_token) — this fragment work is the natural place to mark or drop it.

```sql
-- Additive fragment: opaque position tokens for migration bookkeeping.
-- Appended to the migration schema at composition time
-- (eventsource.migrations.get_schema). Idempotent on PostgreSQL.
--
-- The legacy BIGINT position columns are left in place and are neither
-- written nor read by the library after slice (c); they die with their
-- own schema revision, not this one.
ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS source_position_token TEXT;

ALTER TABLE migration_position_mappings
ADD COLUMN IF NOT EXISTS target_position_token TEXT;

-- Each source position maps to exactly one target position, as the legacy
-- UNIQUE (migration_id, source_position) constraint expressed for ints.
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token)
WHERE source_position_token IS NOT NULL;

-- Ordering for checkpoint translation is by surrogate id (mappings are
-- recorded in ascending source-position order); these indexes serve the
-- exact-token lookups.
CREATE INDEX IF NOT EXISTS idx_position_mappings_source_token
ON migration_position_mappings (migration_id, source_position_token);

CREATE INDEX IF NOT EXISTS idx_position_mappings_target_token
ON migration_position_mappings (migration_id, target_position_token);

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_source_position_token TEXT;

ALTER TABLE tenant_migrations
ADD COLUMN IF NOT EXISTS last_target_position_token TEXT;
```

The legacy `NOT NULL` on `source_position` / `target_position` is a problem for new inserts that carry only tokens: **drop the NOT NULL in the same fragment** (`ALTER TABLE migration_position_mappings ALTER COLUMN source_position DROP NOT NULL;` and the same for `target_position`) rather than writing a fabricated int. Re-read `templates/migration.sql:149-170` before writing this to confirm the constraint names and nullability you are relaxing; if the real DDL differs from what this plan quotes, follow the real DDL and report the difference.

Also create the operator-facing `updates/003_add_migration_position_tokens.sql`, matching the shape and comment style of `updates/002_add_position_token.sql`: the same body plus `COMMENT ON COLUMN` lines for the four new columns. It is not applied by library code; it exists so operators can migrate an existing database.

- [ ] **Step 4 (green): register the fragment**

In `src/eventsource/migrations/__init__.py`, add one entry to `_ADDITIVE_FRAGMENTS`:

```python
    ("migration", "postgresql"): ("migration_position_tokens",),
```

No other change is needed: `get_schema` already resolves the backend additive directory, appends each registered fragment, and raises `FileNotFoundError` for a missing one. Do **not** add a `("migration", "sqlite")` entry — there is no SQLite migration schema to compose against, and a registry entry would turn today's clean `ValueError` into a confusing `FileNotFoundError`.

- [ ] **Step 5 (green): update the migration-schema test module**

`tests/unit/migrations/test_migration_schema.py` asserts on the text of `get_schema("migration")` (column presence, index presence, comment presence). Its existing assertions stay true — the fragment only appends. Add a case asserting the four token columns are present via the composed schema, and confirm no existing case asserts the *absence* of anything the fragment adds or counts occurrences of `ALTER TABLE`.

- [ ] **Step 6: verify**

```bash
git status --porcelain src/eventsource/migrations/
```
Expected: only **added** files under `additive/` and `updates/`, plus a modified `__init__.py`. If any `schemas/*.sql` or `templates/*.sql` shows as modified, revert it and report.

Run: `uv run pytest tests/unit/migrations/ -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/migrations/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migrations/ tests/unit/migrations/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 7: commit**

```bash
git add src/eventsource/migrations/ tests/unit/migrations/
git commit -m "feat: additive position token columns for the migration schema"
```

Record in the commit body: the four reach points from Step 1, that the migration schema is PostgreSQL-only, and that no existing schema file was edited.

---

### Task 2: The position-mapping table goes token-keyed

**Files:**
- Modify: `src/eventsource/migration/repositories/position_mapping.py`, `src/eventsource/migration/position_mapper.py`, `src/eventsource/migration/models.py` (`PositionMapping`), `src/eventsource/migration/exceptions.py` (`PositionMappingError.source_position`)
- Modify tests: `tests/unit/migration/test_position_mapping_repository.py` (1102), `tests/unit/migration/test_position_mapper.py` (904), `tests/unit/migration/test_models.py` (the `PositionMapping` cases only)

**Interfaces:**
- Consumes: Task 1's token columns.
- Produces (used by Task 3 and Task 4):

```python
# models.py
@dataclass(frozen=True)
class PositionMapping:
    ...
    source_position: Position
    target_position: Position

# position_mapper.py
@dataclass(frozen=True)
class TranslationResult:
    source_position: Position
    target_position: Position
    is_exact: bool
    nearest_source_position: Position | None = None

class PositionMapper:
    async def record_mapping(self, migration_id: UUID, source_position: Position,
                             target_position: Position, event_id: UUID) -> None: ...
    async def record_mappings_batch(
        self, migration_id: UUID,
        mappings: list[tuple[Position, Position, UUID]]) -> int: ...
    async def translate_position(self, migration_id: UUID, source_position: Position,
                                 *, use_nearest: bool = True) -> TranslationResult: ...
```

`ReverseTranslationResult` and the reverse-translation path retype symmetrically.

**The one genuinely hard problem in this task is `find_nearest_source_position`.** Today it is `WHERE source_position <= :p ORDER BY source_position DESC LIMIT 1` — SQL ordering over a BIGINT. A `Position` is an opaque token whose `to_str()` is JSON; lexicographic ordering of that text is *not* position ordering, so no `ORDER BY source_position_token` can be correct, and inventing an int sort key would be exactly the arithmetic the ports forbid.

**Decision: order by the surrogate `id`, compare tokens in Python, and find the nearest by binary search over the row ordinal.** Mappings for one migration are recorded in ascending source-position order by a single writer (`BulkCopier` streams the source feed in position order; `DualWriteInterceptor` records in append order), so `id` order *is* source-position order. Under that precondition the nearest-match query is a binary search: `O(log n)` single-row `LIMIT 1 OFFSET k` reads against `ORDER BY id`, each comparing one `Position` against the target in Python. Exact lookups stay a single indexed query on the canonical token string (`Position.to_str()` is deterministic — fixed separators, fixed key order — so string equality is position equality within a store). The rejected alternative is loading every mapping and scanning in Python: correct, but unbounded memory on a large migration.

The monotonicity precondition must be **stated in the repository docstring** as a contract, not left implicit, and the batch writer must preserve it.

- [ ] **Step 1 (red): retype the model and pin the repository contract**

In `src/eventsource/migration/models.py`, retype `PositionMapping.source_position` and `.target_position` to `Position` (import from `eventsource.ports.positions`). In `src/eventsource/migration/exceptions.py`, `PositionMappingError.source_position` becomes `Position | None` and its message renders `to_str()`.

In `tests/unit/migration/test_position_mapping_repository.py`, rewrite the cases: every `source_position=1000` becomes a `Position`. Build them with a small module-level helper (`def pos(n: int, store: str = "src") -> Position: return Position(store_id=store, key=(n,))`) so ordering intent stays readable. Add the cases the new design needs:

- exact source lookup matches on the canonical token and misses on a token from a different store;
- nearest lookup returns the greatest mapping whose source position is `<=` the query;
- nearest lookup returns `None` when every mapping is greater than the query;
- nearest lookup over a single-row table returns that row or `None` correctly (the binary search's degenerate case);
- `get_position_bounds` returns the first and last mapping by `id`.

Run: `uv run pytest tests/unit/migration/test_position_mapping_repository.py -q` — Expected: FAIL.

- [ ] **Step 2 (green): rewrite `PostgreSQLPositionMappingRepository`**

- `create` / `create_batch`: bind `source_position_token` / `target_position_token` from `to_str()`. Do **not** write the legacy BIGINT columns — Task 1 dropped their `NOT NULL`, and writing a fabricated int is precisely the arithmetic this slice removes. Say so in a comment at the INSERT.
- `_row_to_mapping` (or whatever the module's row mapper is called — locate it, do not assume the name): build positions with `Position.from_str(row[...])`. A row carrying only the legacy int is **not** decodable into a token: raise `PositionDecodeError` rather than guessing a `store_id`. Unreleased software; a pre-token mapping row cannot exist in any environment this library created.
- `find_by_source_position` / `find_by_target_position`: `WHERE migration_id = :m AND source_position_token = :token`.
- `find_nearest_source_position`: the binary search described above. Implement it as a private helper returning the row ordinal, so `find_nearest_target_position` (if the module has one) can share it. Sketch, to be fitted to the module's existing session/tracing idiom:

```python
        # Mappings are recorded in ascending source-position order by a single
        # writer, so `id` order is source-position order. Positions are opaque
        # tokens: they cannot be ordered in SQL, so the nearest match is a
        # binary search over the row ordinal with the comparison in Python.
        total = await self.count_by_migration(migration_id)
        if total == 0:
            return None
        lo, hi = 0, total - 1
        best: PositionMapping | None = None
        while lo <= hi:
            mid = (lo + hi) // 2
            candidate = await self._get_by_ordinal(migration_id, mid)
            if candidate is None:
                break
            if candidate.source_position <= source_position:
                best = candidate
                lo = mid + 1
            else:
                hi = mid - 1
        return best
```

  `_get_by_ordinal` is `SELECT ... WHERE migration_id = :m ORDER BY id LIMIT 1 OFFSET :k`. The `<=` comparison raises `PositionForeignError` if a caller passes a position from the wrong store — that is the correct failure and must not be caught here.
- `list_by_migration`, `list_in_source_range`, `get_position_bounds`: order by `id`. For `list_in_source_range`, resolve both endpoints to ordinals with the same binary search, then select the ordinal range — do not attempt a SQL `BETWEEN` on tokens.
- Add the monotonicity precondition to the class docstring: "Mappings for a migration must be recorded in ascending source-position order; ordering and nearest-match lookups rely on it, because opaque position tokens cannot be ordered in SQL."

- [ ] **Step 3 (green): retype `PositionMapper`**

`translate_position` / `translate_position_reverse` take and return `Position`. `record_mapping` / `record_mappings_batch` take `Position`. Every log line and span attribute that carried `%d` positions renders `to_str()` — span attributes must be primitives, so pass the string, never the VO. The `use_nearest` behavior, the `is_exact` flag, and the caching (if the module caches — check before changing it) are unchanged in shape.

- [ ] **Step 4: verify**

```bash
grep -rn "source_position\|target_position" src/eventsource/migration/position_mapper.py src/eventsource/migration/repositories/position_mapping.py | grep -E "int\b|[-+*/]|min\(|max\("
```
Expected: no matches (no int annotations, no arithmetic).

Run: `uv run pytest tests/unit/migration/test_position_mapping_repository.py tests/unit/migration/test_position_mapper.py tests/unit/migration/test_models.py -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migration/ tests/unit/migration/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 5: commit**

```bash
git add src/eventsource/migration/repositories/position_mapping.py \
        src/eventsource/migration/position_mapper.py \
        src/eventsource/migration/models.py src/eventsource/migration/exceptions.py \
        tests/unit/migration/test_position_mapping_repository.py \
        tests/unit/migration/test_position_mapper.py tests/unit/migration/test_models.py
git commit -m "refactor: key position mappings by opaque position tokens"
```

Record in the commit body: the monotonicity precondition and why nearest-match became a binary search.

---

### Task 3: Retire the slice-(c) seam

**Files:**
- Modify: `src/eventsource/migration/subscription_migrator.py`, `src/eventsource/migration/coordinator.py`
- Modify tests: `tests/unit/migration/test_subscription_migrator.py` (1136), `tests/unit/migration/test_coordinator_subscriptions.py`, and any other module constructing `SubscriptionMigrator` or `MigrationCoordinator` (find them in Step 1)

**Interfaces:**
- Consumes: Task 2's token-keyed mapper.
- Produces:

```python
class SubscriptionMigrator:
    def __init__(self, position_mapper: PositionMapper,
                 checkpoint_repo: CheckpointRepository, *,
                 tracer: Tracer | None = None,
                 enable_tracing: bool = True) -> None: ...
```

  — no `source_store_id`, no `target_store_id`, no `IntPositionCodec` import. `MigrationCoordinator.__init__` loses `source_position_store_id` and `target_position_store_id`; `source_store_id` survives as what it always was, a routing label.

**Why the seam dies here without any new port.** Slice (b)'s docstrings anticipated deriving the ids "from the ports stores' own `store_id` attributes". Read `src/eventsource/ports/store.py` before implementing: `FullEventStore` declares no `store_id` — it is an attribute of the concrete adapters, not of the port. Deriving the ids from it would mean widening a port to carry an adapter detail. That turns out to be unnecessary: once Task 2 makes the mapping table token-keyed, the migrator never constructs a position from an int and never reduces one to an int. It reads a `Position` from the checkpoint repository, hands it to the mapper, receives a `Position` back, and writes it to the checkpoint repository. There is nothing left to name a `store_id` for. **Do not add `store_id` to any port.** If an implementer finds a call that genuinely still needs one, STOP and report — that is a design change, not an implementation detail.

- [ ] **Step 1 (red): find every construction site and delete the parameters from the tests first**

```bash
grep -rn "SubscriptionMigrator(\|source_position_store_id\|target_position_store_id\|IntPositionCodec" src/ tests/ bench/
```

Record the full list. In each test construction site, delete the `source_store_id=` / `target_store_id=` / `*_position_store_id=` arguments. In `tests/unit/migration/test_subscription_migrator.py`, every checkpoint value and every mapper stub currently traffics in ints at this boundary — retype them to `Position`, and add a case asserting that a checkpoint position and its translated target position round-trip through `save_position` as tokens, with no int appearing anywhere in the path.

Run: `uv run pytest tests/unit/migration/test_subscription_migrator.py -q` — Expected: FAIL (`TypeError: unexpected keyword argument` is not the failure you want to stop at; the meaningful red is the int-vs-`Position` mismatch once the constructor accepts the call).

- [ ] **Step 2 (green): strip the codecs from the migrator**

In `src/eventsource/migration/subscription_migrator.py`:

- Delete the `from eventsource.adapters._sql.positions import IntPositionCodec` import, the two `store_id` parameters, their docstring paragraphs, the `_source_codec` / `_target_codec` attributes, and the module-header paragraph at `:21-23` that declares the seam.
- `_plan_single_migration` (`:427` area) and `_migrate_single_subscription` (`:663` area): `current_token = await self._checkpoint_repo.get_position(...)` is already a `Position | None`. Delete the `self._source_codec.value_of(current_token)` conversion and pass the `Position` straight to `translate_position`. The `None` branch (skip with a reason) is unchanged and stays.
- The `save_position` call (`:698` area) receives `translation.target_position`, now a `Position`. Delete the `_target_codec.encode(...)` wrapping.
- Every log `extra` and span attribute carrying a position renders `to_str()`.
- Rename local variables that say `current_position` for an int to something that reads as a token (`current_position` holding a `Position` is fine; `current_token` next to a `Position` is not — pick one spelling and use it consistently).

- [ ] **Step 3 (green): collapse the coordinator's duplicated ids**

In `src/eventsource/migration/coordinator.py`:

- Delete the `source_position_store_id` and `target_position_store_id` keyword-only parameters (`:200-201`), their assignments (`:244-245`), their docstring paragraphs (`:219-226`), and the two arguments passed to `SubscriptionMigrator` at `:1636-1637`.
- Keep `source_store_id: str = "default"` exactly as it is — it is the routing label consumed by `_routing_repo.get_or_default` and `register_store`, a different namespace that now has no twin to be confused with. Simplify its docstring: the "see source_position_store_id" cross-reference (`:216-218`) must go, since the thing it points at no longer exists.

- [ ] **Step 4: verify**

```bash
grep -rn "IntPositionCodec\|position_store_id\|slice-(c) seam\|slice (c)" src/eventsource/migration/
```
Expected: no matches. This is the acceptance for the task.

```bash
grep -rn "from eventsource.adapters" src/eventsource/migration/
```
Expected: no matches. `migration/` is a use-case-ring package and must not import an adapter.

Run: `uv run pytest tests/unit/migration/test_subscription_migrator.py tests/unit/migration/test_coordinator_subscriptions.py -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migration/ tests/unit/migration/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean. If a contract was previously *allowing* `migration → adapters` as recorded debt, remove that allowance now and re-run; if no such allowance exists, say so in the commit body.

- [ ] **Step 5: commit**

```bash
git add src/eventsource/migration/subscription_migrator.py src/eventsource/migration/coordinator.py \
        tests/unit/migration/test_subscription_migrator.py \
        tests/unit/migration/test_coordinator_subscriptions.py
git commit -m "refactor: retire the int position codec seam in subscription migration"
```

Record in the commit body: that the seam died by making the mapping table token-keyed rather than by widening a port with `store_id`, and that `MigrationCoordinator.source_store_id` survives as a routing label only.

---

### Task 4: The flip — migration moves onto the ports store surface

**Why this task is large.** Every commit must be type-clean (pre-commit runs mypy) and no shims are permitted. `MigrationCoordinator` holds the stores and hands the same objects to `TenantStoreRouter`, `DualWriteInterceptor`, `BulkCopier`, `ConsistencyVerifier`, `SyncLagTracker` and `CutoverManager`. A legacy `EventStore` ABC instance does not structurally satisfy `FullEventStore` (the method names differ: `append_events` vs `append`), so the moment any one of those collaborators retypes, the coordinator's call site is red — and the moment the coordinator retypes, all of them are. There is no intermediate state that is both green and shim-free, so the flip is one commit. The steps below are ordered so an implementer can work outside-in.

The orchestrator may dispatch Steps 1-7 and Steps 8-11 as separate work units against the same uncommitted tree, but there is exactly one commit at the end.

**Files:**
- Modify: `src/eventsource/migration/router.py`, `dual_write.py`, `bulk_copier.py`, `consistency.py`, `sync_lag_tracker.py`, `coordinator.py`, `cutover.py`, `models.py` (`Migration`, `SyncLag`), `repositories/migration.py` (progress columns)
- Modify tests: `tests/unit/migration/test_router.py` (1350), `test_dual_write.py` (888), `test_bulk_copier.py` (945), `test_consistency_verifier.py` (1077), `test_sync_lag_tracker.py` (1027), `test_cutover_manager.py` (1340), `test_coordinator.py`, `test_coordinator_dual_write.py`, `test_migration_repository.py` (1044), `test_load_benchmarks.py` (1487), `test_phase2_integration.py` (1791), `test_chaos.py` (1812), `test_phase3_integration.py` (2238), `test_final_integration.py` (2322)

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces:

```python
class TenantStoreRouter:                     # structural FullEventStore, no ABC base
    def __init__(self, default_store: FullEventStore, routing_repo, *,
                 stores: dict[str, FullEventStore] | None = None,
                 default_store_id: str = "default", ...) -> None: ...
    async def append(self, stream: StreamId, events: Sequence[DomainEvent],
                     expected: ExpectedVersion) -> AppendResult: ...
    def read_stream(self, stream, options=None) -> AsyncIterator[EventEnvelope]: ...
    async def get_stream_version(self, stream: StreamId) -> int: ...
    async def event_exists(self, event_id: UUID) -> bool: ...
    def read_all(self, from_position=None, options=None) -> AsyncIterator[EventEnvelope]: ...
    async def current_position(self) -> Position | None: ...
    def read_category(self, category, options=None) -> AsyncIterator[EventEnvelope]: ...

class DualWriteInterceptor:                  # structural FullEventStore, no ABC base
    ...same eight members; reads delegate to the source verbatim

@dataclass(frozen=True)
class FailedWrite:
    ...
    source_position: Position | None

@dataclass
class SyncLag:
    events: int                              # exact up to the bound, then ">bound"
    source_position: Position | None
    target_position: Position | None
    timestamp: datetime

@dataclass
class Migration:
    ...
    last_source_position: Position | None = None
    last_target_position: Position | None = None
```

- [ ] **Step 1 (red): rewrite the two wrapper suites against the ports shape**

`tests/unit/migration/test_router.py` and `test_dual_write.py` are the specification of the two wrappers. Rewrite them first:

- Every `await router.append_events(agg_id, "Order", events, 0)` becomes `await router.append(StreamId(aggregate_id=agg_id, category="Order"), events, ExpectedVersion.no_stream())`. Consult `stores/legacy.py::_expected_from_int` for the by-name sentinel mapping (spec §1.1) when a test's intent was `ANY` or `STREAM_EXISTS`, and map by name, never by the numeric coincidence.
- Add one structural-conformance case per wrapper: `_: FullEventStore = router` at runtime (a plain annotated assignment in the test body, which mypy checks and which documents the intent), plus a call through each of the eight members. This is what replaces the ABC base class as the guarantee that the wrappers are complete.
- `TestRouterIntegration::test_interceptor_works_as_eventstore_replacement` (`test_dual_write.py:731`) is the one call in the whole test tree that omits `aggregate_type`. Per spec §2 the capability is dropped: rewrite the case as a ports-shaped replacement check (the interceptor stands in wherever a `FullEventStore` is expected) with a concrete category. Do not add a cross-type port.
- Any double returning `AppendResult.conflicted(...)` or asserting `result.success` / `result.conflict` becomes a double that **raises** `OptimisticLockError` (spec §11 risk 5). Sweep for these deliberately — a double that silently returns success where the real store raises produces a passing test that proves nothing.

Run: `uv run pytest tests/unit/migration/test_router.py tests/unit/migration/test_dual_write.py -q` — Expected: FAIL.

- [ ] **Step 2 (green): `TenantStoreRouter` becomes a ports-shaped wrapper**

`src/eventsource/migration/router.py`: drop the `EventStore` base class and the `stores.interface` import; type `_stores`, `_default_store`, `_dual_write_interceptors` and every accessor as `FullEventStore`. Translate method by method per spec §1:

- `append_events` → `append(stream, events, expected)`. Tenant extraction (`_extract_tenant_id`) reads the events, unchanged; the routing decision (`_get_write_store`) is unchanged; the write-pause wait is unchanged.
- `get_events` → **deleted**. It was the ABC's cross-type-capable read; consumers use `read_stream` (spec §2).
- `get_events_by_type` → `read_category(category, options)`, returning an `AsyncIterator[EventEnvelope]` rather than a materialized `list[DomainEvent]`. Note the semantic change this exposes (storage-time, inclusive, position tie-break) and let it through — the port's semantics win.
- `read_stream(stream_id: str, ReadOptions)` → `read_stream(stream: StreamId, StreamReadOptions | None)`.
- `read_all(ReadOptions)` → `read_all(from_position: Position | None, FeedReadOptions | None)`.
- `get_global_position` → `current_position() -> Position | None`, delegating to the default store. Its docstring must say that the returned position belongs to the default store and is not comparable with any other store's positions.
- `event_exists` and `get_stream_version` keep their shapes; `get_stream_version` takes a `StreamId`.

- [ ] **Step 3 (green): `DualWriteInterceptor` becomes a ports-shaped wrapper**

`src/eventsource/migration/dual_write.py`: drop the ABC base and the `stores.interface` import.

- `append`: write to the source; on success, best-effort write to the target with the existing failure recording. **Delete the `if not source_result.success: return` branch entirely** — a failed source append now raises `OptimisticLockError` out of `append`, which is the honest propagation and is what the caller already expects.
- `FailedWrite.source_position` becomes `Position | None`, taken from `source_result.position`. Its docstring says "position of the first event of the source append" (spec §1.1 first-vs-last), not "position after the write".
- The four read ports delegate to the source verbatim; `get_events` ceases to exist.
- The existing `ValueError` on an empty event list stays — the interceptor raised it before the adapters did, so this behavior is unchanged.

- [ ] **Step 4 (green): `SyncLagTracker` becomes a bounded count-behind**

`src/eventsource/migration/sync_lag_tracker.py`. Today `calculate_lag` (`:266-270`) reads both stores' `get_global_position()` and subtracts. With opaque tokens that subtraction is not merely banned, it is impossible: the two positions come from different stores, and comparing them raises `PositionForeignError`.

New shape:

```python
    async def calculate_lag(self, *, since: Position | None = None) -> SyncLag:
        """Count source events not yet copied, bounded by the sync threshold.

        `since` is the last source position the target has copied (the
        migration's `last_source_position`); None means nothing has been
        copied and the count starts at the head of the source feed. The
        count is exact up to `cutover_max_lag_events + 1`; beyond that it
        reports the bound and stops reading, which is all a convergence
        decision needs.
        """
```

- Read `self._source.read_all(since, FeedReadOptions(tenant_id=self._tenant_id, limit=threshold + 1))` and count the envelopes. `threshold` is `self._config.cutover_max_lag_events`.
- `SyncLag.source_position` is the source's `current_position()` and `target_position` the target's, both **reporting only** — they are never compared with each other, and their docstrings must say so.
- `SyncLag.events` keeps its `int` type and its "events behind" meaning; the spec notes it is now *more* correct than the old delta, which over-counted across tenants (positions are global; a tenant's events are a subset).
- Add a `count_is_bounded: bool` field (or equivalent) to `SyncLag` so `events == threshold + 1` is distinguishable from "exactly threshold+1 behind" — without it, `is_converged` is still correct (both are over the threshold) but the reported number lies. If adding a field turns out to churn `models.py` consumers unacceptably, report and keep the number with a docstring caveat instead; do not silently ship an ambiguous count.
- `is_converged`, `is_sync_ready`, `is_fully_converged`, `get_lag_stats`, `_is_converging`, `record_lag` and the whole sample-history machinery are **unchanged** — they consume `events: int`, whose meaning is preserved.
- `_source` / `_target` retype to `FullEventStore` (the tracker needs the feed and nothing else, but it is constructed from the coordinator's full stores; type it as `GlobalEventFeed` if nothing else is used — check before widening, and prefer the narrowest port that compiles).

Callers must supply `since`. `MigrationCoordinator` has it on the `Migration` record (`last_source_position`); wire it through in Step 7.

- [ ] **Step 5 (green): `BulkCopier` moves to the feed and per-event appends**

`src/eventsource/migration/bulk_copier.py`:

- Imports: `stores.interface` goes; `EventEnvelope` / `FeedReadOptions` / `Position` / `StreamId` / `ExpectedVersion` come from `eventsource.ports`.
- `BulkCopyProgress.last_source_position` / `.last_target_position` and `BulkCopyResult`'s twins become `Position | None`. `progress_percent` is a ratio of **counts** and is unchanged.
- `_count_tenant_events`: `read_all(None, FeedReadOptions(tenant_id=tenant_id))`, counting — shape unchanged.
- `_stream_tenant_events(tenant_id, from_position: Position | None)`: `read_all(from_position, FeedReadOptions(tenant_id=tenant_id))`. The legacy `from_position=0` "start" sentinel becomes `None` (spec §1.5); a `Position` is strictly-after, which matches the legacy exclusive `>` predicate, so resume semantics are preserved exactly.
- `run`: `from_position = migration.last_source_position` is now `Position | None`; `last_source_position = batch[-1].position`; the `%d` format specifiers in the log lines become `%s` over `to_str()` or `None`.
- `_write_batch` — the real redesign. Grouping by aggregate stays, but the key comes from the envelope: `(envelope.stream_id.aggregate_id, envelope.stream_id.category)`. Per group:

```python
                stream = StreamId(aggregate_id=aggregate_id, category=aggregate_type)
                try:
                    current_version = await self._target.get_stream_version(stream)
                except Exception:
                    current_version = 0
```

  Then, per spec §6 slice (c) item 3:

  - **With a `position_mapper` configured**: append one event at a time. Each append's `result.position` is that event's exact target position — strictly more correct than today's `result.global_position - len(agg_events) + i + 1` estimate, which was already wrong and becomes nonsense under first-position semantics. Record the mapping with `(envelope.position, result.position, envelope.event.event_id)`, skipping the mapping when either position is `None` (a feedless store has nothing to map). Advance `current_version` by one per successful append.
  - **Without a mapper**: keep the batched append per aggregate (`ExpectedVersion.exact(current_version)`).
  - **Either way**: catch `DuplicateEventError` around the append and count the event as already-copied, continuing rather than failing. This replaces the legacy silent skip and is what makes a resumed copy converge (Task 5 is its property test). Log it at debug with the event id; do not log per event at info.
  - A group is never empty (it exists because an event was appended to it), so the adapters' empty-batch `ValueError` cannot fire — state this in a comment rather than adding a defensive guard.
  - `last_target_position` is the last successful append's `result.position`; there is no `max()` over positions, because appends within a run are already in ascending target order.

- `BulkCopyError`'s `last_position` argument (`exceptions.py:836-845`) becomes `Position | None` and its message renders `to_str()` or `"start"`.
- The `record_mappings_batch` path, if `BulkCopier` uses it, passes `(Position, Position, UUID)` tuples in ascending source order — Task 2's repository depends on that ordering.

**Spec §11 risk 2 (per-event append cost with a mapper) is a Slice Completion Criterion, not an implementer's call.** Implement the per-event path as specified and let the orchestrator measure.

- [ ] **Step 6 (green): `ConsistencyVerifier` compares envelopes**

`src/eventsource/migration/consistency.py`: `StoredEvent` → `EventEnvelope` throughout (`:526`, `:537`, `:547-568`, `:575-576`, `:721-724`, `:760`); `ReadOptions` → `FeedReadOptions` at the collection site; `.stream_position` → `.stream_version` (`:568`, `:608-609`, `:674-693`, `:780`); the grouping key comes from `envelope.stream_id.render()` where the code used the legacy `stream_id` string, keeping the same wire format (`domain/stream_id.py`). Event hashes are payload-based and their inputs are unchanged apart from `stream_version` replacing `stream_position` — the same number under a new name, so hash values do not move.

- [ ] **Step 7 (green): coordinator, cutover, and the migration record**

- `src/eventsource/migration/models.py`: `Migration.last_source_position` / `.last_target_position` become `Position | None = None`. Check every `to_dict` / serialization on the dataclass and render `to_str()` or `None`.
- `src/eventsource/migration/repositories/migration.py`: `update_progress(migration_id, events_copied, last_source_position: Position | None, last_target_position: Position | None = None)` binds `last_source_position_token` / `last_target_position_token` (Task 1's columns) and stops writing the legacy BIGINT columns; the row mapper (`:760-761`) reads the token columns and returns `Position.from_str(...)` or `None`. `create` (`:310-329`) does the same.
- `src/eventsource/migration/coordinator.py`: `source_store` / `_target_stores` / every `EventStore` annotation → `FullEventStore`. Wire `since=migration.last_source_position` into the `SyncLagTracker.calculate_lag` calls.
- `src/eventsource/migration/cutover.py:367`: `await target_store.get_global_position()` → `await target_store.current_position()`; whatever it compared or reported becomes a `Position | None` rendered as a token string. Read the surrounding block before changing it — if it was comparing against a source position, that comparison must become a lag check through the tracker, not a cross-store compare.

- [ ] **Step 8: rewrite the remaining unit suites**

Per spec §7 slice (c). Work in this order, cheapest first:

- `test_bulk_copier.py`, `test_consistency_verifier.py`, `test_sync_lag_tracker.py`, `test_cutover_manager.py`, `test_coordinator.py`, `test_coordinator_dual_write.py`, `test_migration_repository.py`: fixture and assertion retype (`StoredEvent` → `EventEnvelope`, ints → `Position`, `get_global_position` → `current_position`). `test_sync_lag_tracker.py` needs the most thought: its cases assert a *delta*; they become assertions about a **count of uncopied source events**, with a case at the bound (`threshold + 1` events behind reports the bound and stops reading) and a case where the target has copied everything (`events == 0`).
- `test_phase2_integration.py`, `test_phase3_integration.py`, `test_final_integration.py`: large but shallow — two store fixtures each. Retarget the fixtures to `MemoryEventStore` (**not** `InMemoryEventStore`; the rename is slice (d)'s) and fix what follows.
- `test_load_benchmarks.py` and `test_chaos.py` are the expensive ones because they subclass the legacy classes. `FailureInjectableStore` in `test_chaos.py` must become a ports-shaped failure-injecting **wrapper** around a `MemoryEventStore` (delegate all eight members, inject on the ones the test targets) rather than an ABC subclass. Write that wrapper once, at module scope, and reuse it.
- Any case asserting a store-level span (`postgresql_event_store.*`, `inmemory_event_store.*`) is **deleted**, not retargeted — the adapters emit none (ADR 0016 amendment). Count them for the commit body.

- [ ] **Step 9: verify the surface is gone**

```bash
grep -rn "stores\.interface\|StoredEvent\|ReadOptions\|append_events\|get_events_by_type\|get_global_position\|global_position" src/eventsource/migration/
```
Expected: no matches.

```bash
grep -rn "position" src/eventsource/migration/ | grep -E "[-+*/] *(source|target|last)_position|min\(.*position|max\(.*position"
```
Expected: no matches. Inspect every hit; a `max()` over a *count* is fine, a `max()` over positions is not.

```bash
grep -rn "\.success\|\.conflict" src/eventsource/migration/ tests/unit/migration/
```
Expected: no hits on an `AppendResult`. Other `success` fields (`SubscriptionMigrationResult.success`, `BulkCopyResult.success`) are unrelated and stay — check each hit rather than assuming.

```bash
grep -rln "stores\.interface\|append_events" src/eventsource/__init__.py src/eventsource/stores/
```
Expected: several matches — slice (d) still owns those. An empty result means this slice overreached.

```bash
git diff main --stat -- src/eventsource/stores/ src/eventsource/subscriptions/
```
Expected: empty. Both are frozen in this slice.

- [ ] **Step 10: run targeted tests**

Run: `uv run pytest tests/unit/migration/ -q` — Expected: PASS.
Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: PASS (this slice adds and removes no public name).
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/migration/ tests/unit/migration/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 11: commit**

```bash
git add src/eventsource/migration/ tests/unit/migration/
git commit -m "refactor: retype migration onto the ports store surface"
```

Record in the commit body: the dropped `aggregate_type=None` capability (spec §2), the per-event append decision and why the old target-position estimate could not be preserved, the `DuplicateEventError`-as-already-copied rule, the count-behind lag, and the number of deleted store-span cases.

---

### Task 5: The bulk-copy resume idempotency property

**Files:**
- Create: `tests/unit/migration/test_bulk_copy_resume_property.py`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: the executable form of the spec's duplicate-skip ruling. Spec-mandated (§7 slice (c)); it is not optional and not to be replaced by example-based cases.

- [ ] **Step 1: write the property**

Against two real `MemoryEventStore`s and a `PositionMapper` over an in-memory double of `PositionMappingRepository` (there is no in-memory implementation in the tree — write a minimal one in the test module, holding a list and honouring the ascending-source-order precondition, or drive `BulkCopier` without a mapper if wiring the double proves to dominate the test; prefer the double, since the mapper path is the one this slice redesigned):

```python
@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    streams=st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=6),
    batch_size=st.integers(min_value=1, max_value=4),
    crash_after=st.integers(min_value=0, max_value=20),
)
@pytest.mark.asyncio
async def test_bulk_copy_resumes_to_a_source_equal_target(
    streams: list[int], batch_size: int, crash_after: int
) -> None:
    """Random stream shapes, batch sizes and crash points: a resumed copy
    converges on a target whose streams equal the source's.

    A resume that re-reads from an inclusive position re-appends events the
    target already has; the copy is only correct because that re-append
    raises DuplicateEventError and is counted as already-copied. Remove that
    handler and this property fails immediately.
    """
```

Shape: append `sum(streams)` events across that many distinct streams into the source; run a `BulkCopier` and cancel it after `crash_after` events have been copied; construct a **second** copier over the same stores, resuming from the persisted `last_source_position`; run it to completion. Assert with `ConsistencyVerifier` that the target is source-equal, and separately that no event id appears twice in the target's feed.

Clamp `crash_after` to the total rather than discarding examples with `hypothesis.assume`.

- [ ] **Step 2: run it**

Run: `uv run pytest tests/unit/migration/test_bulk_copy_resume_property.py -q` — Expected: PASS.
Run: `uv run pytest tests/unit/migration/test_bulk_copy_resume_property.py -q -p no:randomly --hypothesis-seed=0` — Expected: PASS (a deterministic second opinion; if the two disagree, the property has a state leak between examples — fix the fixture, do not loosen the property).
Run: `uv run ruff check tests/unit/migration/test_bulk_copy_resume_property.py` — Expected: clean.

- [ ] **Step 3: commit**

```bash
git add tests/unit/migration/test_bulk_copy_resume_property.py
git commit -m "test: property test for bulk copy resume idempotency"
```

---

### Task 6: Mutation config, docs, changelog, final sweep

**Files:**
- Modify: `pyproject.toml` (`[tool.mutmut]` only), `CHANGELOG.md`
- Modify: `docs/` pages documenting migration positions or the migration schema (locate in Step 2)

**Interfaces:**
- Consumes: Tasks 1-5.
- Produces: no code surface.

**Concurrency caveat:** other agents are editing `pyproject.toml`. Locate the `[tool.mutmut]` block by its TOML header, not by line number, and re-read the file immediately before editing. If an edit is already present, verify rather than duplicate and say so in the commit body.

- [ ] **Step 1: extend the mutation-testing selection**

Spec §7: slice (c) adds `src/eventsource/migration/bulk_copier.py` to `only_mutate` and `tests/unit/migration/test_bulk_copier.py` to `pytest_add_cli_args_test_selection` — the resume and duplicate-skip logic is exactly the off-by-one surface mutmut exists for. The spec marks this "optionally"; take it, and add `src/eventsource/migration/position_mapper.py` with `tests/unit/migration/test_position_mapper.py` alongside it, since Task 2's binary search is a second such surface.

Run: `uv run python -c "import tomllib,pathlib; c=tomllib.loads(pathlib.Path('pyproject.toml').read_text()); print(c['tool']['mutmut'])"` and confirm both keys read back as intended. Do not run mutmut itself — that is the orchestrator's call.

- [ ] **Step 2: docs and changelog**

```bash
grep -rn "get_global_position\|append_events\|source_position\|target_position\|position_store_id" docs/guides/live-migration.md docs/api/migration-schema.md docs/guides/database-schema.md docs/explanation/schema-design.md docs/ examples/ README.md
```

For each hit in prose or example code about **migration**: int positions become tokens; `get_global_position()` becomes `current_position()`; `append_events(...)` becomes `append(StreamId(...), events, ExpectedVersion...)`; the migration schema pages gain the four token columns and the note that the legacy BIGINT columns are no longer written. Do **not** touch documentation of the legacy stores themselves, of `subscriptions/`, or `docs/adrs/` — those are slice (d)'s and slice (b)'s respectively. ADR 0025 is written in slice (d) (spec §9); do not create it here.

Add a `CHANGELOG.md` entry for this slice naming the deltas loudly: migration runs on the ports store surface; cross-type `get_events(aggregate_type=None)` is gone; a duplicate append during bulk copy is counted as already-copied instead of silently skipped; sync lag is a bounded count of uncopied source events rather than a position delta; position mappings and migration progress are persisted as opaque tokens; `MigrationCoordinator` and `SubscriptionMigrator` no longer take position-store ids.

- [ ] **Step 3: final sweep**

```bash
grep -rn "EventStore\b" src/eventsource/migration/
```
Expected: no matches other than prose in docstrings that means "event store" generically — inspect each and rewrite the prose where it names the dead ABC.

```bash
grep -rn "IntPositionCodec\|position_store_id" src/ tests/ bench/
```
Expected: no matches outside `src/eventsource/adapters/_sql/positions.py` and its own tests (the codec itself stays; it is the SQL adapters' internal encoding).

```bash
git diff main --stat -- src/eventsource/stores/ src/eventsource/subscriptions/ src/eventsource/__init__.py
```
Expected: empty.

```bash
git diff main --stat -- src/eventsource/migrations/
```
Expected: additions under `additive/` and `updates/` plus a modified `__init__.py` — no `schemas/` or `templates/` file modified.

- [ ] **Step 4: commit**

```bash
git add pyproject.toml CHANGELOG.md docs/
git commit -m "docs: document opaque positions in migration and extend mutation selection"
```

---

## Slice Completion Criteria

The orchestrator runs these; implementers do not.

- [ ] `make check` passes (lint, mypy, import-linter, bandit/pip-audit, full unit suite).
- [ ] Integration suites pass with Docker services up: `uv run pytest tests/integration/ -v`, including `tests/integration/migrations/test_migration_schema_postgresql.py` against a database created from the composed `get_schema("migration")`.
- [ ] The full suite runs at least once in default random order (do not pass `-p no:randomly`).
- [ ] **Spec §11 risk 2 — per-event append cost in `BulkCopier` with a position mapper.** Run `tests/unit/migration/test_load_benchmarks.py`'s successor against memory, and the `bench/` store scenarios against postgres, to bound the regression against the batched path. If it is unacceptable, the fallback design is a batched append followed by a feed read to harvest the actual positions (more code, same correctness) — decide on numbers, and record them either way.
- [ ] `src/eventsource/stores/` and `src/eventsource/subscriptions/` are byte-identical to their pre-slice state.
- [ ] No existing file under `src/eventsource/migrations/schemas/` or `templates/` was modified.
- [ ] `src/eventsource/__init__.py`'s store section is untouched and `eventsource.__all__` is unchanged (this slice adds and removes no public name).
- [ ] `grep -rn "IntPositionCodec" src/eventsource/migration/` is empty — the slice-(b) seam is retired, which is this slice's headline acceptance.

## Plan Self-Review

- **Spec coverage, slice (c) only.** Every row of the spec's §6 slice (c) migration table has a task: `router.py:61-91` → Task 4 Step 2; `dual_write.py:65-138` → Task 4 Step 3; `bulk_copier.py:47,486-590` → Task 4 Step 5; `consistency.py:59-62` → Task 4 Step 6; `sync_lag_tracker.py:59,266` → Task 4 Step 4; `coordinator.py:100` → Task 4 Step 7; `subscription_migrator.py` → Tasks 2 and 3. The seven numbered changes in §6 slice (c) map to Task 4 Steps 2, 3, 5, 6, 4, 7 and Tasks 1-3 respectively. §2's dropped cross-type capability → Task 4 Steps 2, 3 and the rewritten `test_dual_write.py:731` case. The §7 slice (c) test inventory is fully allocated: the nine REWRITE modules to Task 4 Steps 1 and 8, the mandated resume-idempotency property to Task 5, the mutation additions to Task 6. §11 risk 4 → Task 1; risk 5 → Task 4 Step 1; risk 2 → Slice Completion Criteria. Nothing from slices (a), (b) or (d) appears here: `stores/`, `subscriptions/`, `__init__.py`, the `MemoryEventStore` rename, ADR 0025 and the conformance-suite retirement are all explicitly excluded, and the catch-up early-exit backlog item is named as out of scope in the Global Constraints.
- **Placeholder scan.** No TODO, TBD, or `???` remains. Every code block is complete or is an explicit, bounded instruction over code the implementer is reading anyway. The three places where the implementer must read before writing (the `migration.sql` nullability, the `position_mapping.py` row-mapper name, the `cutover.py:367` surrounding block) say so and say what to do if reality differs.
- **Signature consistency.** `Position` (never `position_token` as a type) is the VO throughout; `*_position_token` names only SQL columns. `translate_position(migration_id, source_position: Position, *, use_nearest)` is spelled identically in Task 2's Interfaces block and Steps 2-3. `calculate_lag(*, since: Position | None)` is spelled identically in Task 4 Step 4 and Step 7. `read_all(from_position, options)` and `current_position()` match `ports/store.py` exactly. `MemoryEventStore` (not `InMemoryEventStore`) is used throughout — the rename is slice (d)'s. `FullEventStore` is the annotation for a held store; `GlobalEventFeed` where only the feed is used.
- **Every task leaves mypy and import-linter green.** Tasks 1 and 2 touch no store-typed code, so nothing downstream moves. Task 3 is the one that *fixes* a standing violation: it deletes `migration/`'s import of `adapters/_sql/positions`, and its verify step runs `uv run lint-imports` explicitly and instructs the implementer to remove any contract allowance that existed for it. Task 4 is one commit precisely because a partial store retype cannot type-check: the preamble states why, and Step 10 runs whole-package mypy rather than a per-directory subset. Tasks 5 and 6 add no source. Every task's verify step lists `uv run lint-imports`.

## Spec Gaps Found (report, do not silently deviate)

1. **The spec's plan for retiring the seam does not survive contact with `ports/store.py`.** Slice (b)'s docstrings (and the dispatch brief) say the store ids will "derive from the ports stores' own `store_id` attributes once migration holds ports stores". `FullEventStore` declares no `store_id` — it is a concrete-adapter attribute. Deriving from it would require widening a port to carry an adapter detail, which ADR 0019's segregation forbids. Resolved instead by making the position-mapping table token-keyed (Task 2), after which the migrator has nothing to convert and needs no `store_id` at all (Task 3). This is a better outcome than the anticipated one and it removes rather than adds port surface, but it is not the mechanism the earlier slice predicted.
2. **§6 slice (c) item 7 says the mapping rows "gain token TEXT columns" and stops there.** It does not address that `find_nearest_source_position` — the whole reason the table exists — is an `ORDER BY source_position DESC LIMIT 1` over an orderable integer, and that no `ORDER BY` over an opaque token can replace it (`Position.to_str()` is JSON; its lexicographic order is not position order). Resolved in Task 2 with a binary search over the surrogate `id` and Python-side `Position` comparison, resting on a documented monotonicity precondition (mappings are recorded in ascending source order by a single writer). The rejected alternative — load all mappings and scan — is correct but unbounded. This is a real algorithmic change the spec does not anticipate, and it deserves a line in ADR 0025 when slice (d) writes it.
3. **The `migration_position_mappings` legacy columns are `NOT NULL` and the spec's additive-only rule does not cover relaxing them.** Token-only inserts violate `source_position NOT NULL`. Task 1 drops the NOT NULL in the additive fragment rather than fabricating an int, which is still a fragment (no base-file edit) but is a constraint change, not purely an addition. Flagging because "append-only" could be read as forbidding it; the alternative — writing a derived int — would reintroduce exactly the arithmetic this slice removes.
4. **`SyncLagTracker` has no source anchor to count from.** §6 slice (c) item 5 says to "read the source feed strictly after the target's last-copied source position", but the tracker holds only two stores; the last-copied source position lives on the `Migration` record, which the coordinator holds. Resolved by making it a `calculate_lag(*, since: Position | None)` parameter supplied by the caller (Task 4 Steps 4 and 7), with `None` meaning "nothing copied yet". The alternative — giving the tracker a `MigrationRepository` — would widen its dependencies for one field.
5. **`SyncLag.events` becomes ambiguous at the bound.** A bounded count that stops at `threshold + 1` cannot distinguish "exactly threshold+1 behind" from "far behind". Convergence decisions are unaffected (both are over threshold), but the *reported* number is no longer exact at the top of its range and the spec presents it as exact. Task 4 Step 4 adds a flag to `SyncLag` so the reported number stays honest, with an explicit fallback and a report-instead-of-guess instruction if that field churns `models.py` consumers.
6. **Neither the `Migration` record's progress columns nor `BulkCopyError.last_position` appear in the spec's §6 slice (c) file list**, but `tenant_migrations.last_source_position` / `.last_target_position` are the bulk copier's resume state and `BulkCopyError` carries a position in its message. They are in scope by necessity (Task 1's fragment, Task 4 Steps 5 and 7). Likewise `cutover.py:367` calls `get_global_position()` and `migration/exceptions.py` types `PositionMappingError.source_position` as `int | None`; both are covered here and neither is listed in the spec.
