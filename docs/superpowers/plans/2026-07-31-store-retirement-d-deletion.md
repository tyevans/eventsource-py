# Legacy Store Retirement — Slice (d): Deletion, Public-API Swap, Docs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** End the legacy store surface. Port the one legacy feature the adapters lack (`outbox_enabled` + `_write_to_outbox`), retire `EventStoreConformanceSuite` after a case-by-case gap check, rename `MemoryEventStore` → `InMemoryEventStore`, delete `src/eventsource/stores/` entirely, rewrite the top-level `__init__.py` store section so `eventsource` exports exactly one blessed set of store names, and sweep the contracts, ADRs, and docs. After this slice the string `eventsource.stores` does not appear in any executable file in the repository.

**Architecture:** The five segregated ports in `ports/store.py` and the three adapters under `adapters/` become the only store surface. The names the legacy ABC owned at top level (`InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`, `ExpectedVersion`, `ReadDirection`, `AppendResult`) rebind to their ports/adapter counterparts in a single edit, and the deliberate dual-export comment block at `__init__.py:24-38` — which existed only to keep two referents apart — is deleted along with the second referent. `EventPublisher` keeps its home in `ports/bus.py`; the `stores/interface.py` re-export dies with the module. ADR 0025 stops being a placeholder and records every behavior delta the four slices made visible.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), asyncpg, aiosqlite, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md` (slice (d): §3 `testing/conformance.py`, §4 in full, §5.1, §5.2, §5.3, §6 slice (d), §7 slice (d), §8, §9)

**Sibling slices:** `2026-07-31-store-retirement-a-leaf-consumers.md`, `-b-subscriptions.md` and `-c-migration.md` have ALL LANDED. Verified against the tree at HEAD: the only remaining importers of `eventsource.stores` in `src/` are `__init__.py` (5 import statements), `testing/conformance.py` (1), and `stores/`'s own internals; `bench/` and `examples/` have none; two adapter module docstrings mention the legacy module by name in prose. Every runtime consumer — aggregates, sync, harness, subscriptions, migration, bench — is already on the ports. Do not redo any of it.

## Global Constraints

- **Unreleased software — no shims, no back-compat aliases.** No `MemoryEventStore = InMemoryEventStore` alias, no `eventsource.stores` stub package, no deprecation warning module. A deleted name is deleted.
- **Positions are opaque.** Compare and persist (`to_str`/`from_str`); never subtract, never compare to an int. This slice writes little new position code, but the outbox port and the docs sweep both touch position-adjacent prose — do not reintroduce int-position language.
- **`migrations/` SQL files are append-only BY FILE.** Adding a file under `src/eventsource/migrations/` is legal; modifying an existing `.sql` under `schemas/` or `templates/` requires Ty's sign-off. This slice needs **no** schema change at all. The one SQL edit it makes is a comment added to the already-additive `additive/checkpoints_position_token.sql` (Task 5) — a fragment this project created, not a base file.
- **Public-API changes keep `tests/unit/test_public_api.py` as the single pin.** That file is the acceptance test for spec §4; it is updated in the *same task* as the swap it pins, never before and never after. Do not create a second file that also asserts export identities.
- **Every deletion task ends with a full grep sweep** over `src/ tests/ bench/ examples/ docs/`. Executable references (`.py`, `.toml`, `.yml`) must be **zero**; docs *prose* references are updated in Task 5 and may still be non-zero until then — the sweep reports them, and Task 5's sweep is the one that must come back clean everywhere.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Path-scoped `git add` only.** Other agents work concurrently in this worktree. Never `git add -A`, never `git add .`; stage exactly the files the task names. On `index.lock` contention, wait 5 seconds and retry.
- **No live-src mutation probes.** Do not "try an edit and see what mypy says" on shared source while other agents are active. Reason from the code you read; experiment on a copy under `$CLAUDE_JOB_DIR/tmp`.
- **Implementers do not push.** Commit only. Branch pushes and PRs are the orchestrator's.
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Red/green TDD.** Every task that changes behavior writes or edits its failing test first, observes the failure, then implements. Steps are ordered so the red step precedes the green one; do not reorder them.
- **No speculative property tests.** This slice mandates none. The hypothesis suites that exist (`tests/unit/adapters/test_memory_stateful.py`, `tests/unit/subscriptions/test_catchup_resumption_property.py`, `tests/unit/migration/test_bulk_copy_resume_property.py`) must stay green; do not extend them.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- **import-linter green per task, not only at the end.** `uv run lint-imports` is in every task's verify step.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds, which means **every task must leave the tree importable and type-clean**. That constraint is what forces Task 4's size; see its preamble and the deletion-ordering argument below.

### Deletion ordering: why the rename, the `__init__` swap, and the `stores/` deletion are one task

Three constraints interlock, and no ordering of them produces an intermediate state that is both green and shim-free.

1. **The rename cannot precede the deletion.** `InMemoryEventStore` is a live top-level export today, bound to `stores.in_memory.InMemoryEventStore`. Renaming `adapters.memory.MemoryEventStore` to `InMemoryEventStore` before the legacy class is gone gives one name two referents in one namespace — exactly the collision the dual-export comment block was written to avoid. Spec §4.1 calls this out and requires simultaneity.
2. **The `__init__` swap cannot precede the deletion.** Rebinding `InMemoryEventStore`/`PostgreSQLEventStore`/`SQLiteEventStore`/`ExpectedVersion`/`ReadDirection`/`AppendResult` to the ports and adapters while `stores/` still exists would leave ~10 legacy test modules — which import those names *from `eventsource`* and assert legacy behavior — failing on the next collection. And `tests/unit/test_public_api.py::TestCollisionDecisions` asserts, positively, that each of those six names **is** the legacy class; it goes red the instant the binding moves.
3. **The deletion cannot precede the swap.** `__init__.py` is a runtime importer of five `stores` modules. Deleting `stores/` first makes `import eventsource` an `ImportError` — nothing collects, no test runs, the commit cannot pass pre-commit.

So the swap, the rename, the `stores/` deletion, the legacy-test deletion, and the `test_public_api.py` rewrite are one commit (Task 4). Everything that can be *lifted out* of that commit has been: the outbox port (Task 1) and the conformance retirement (Task 2) are additive and land first; the surviving test modules that merely *touch* the legacy surface are pre-migrated onto the adapters in Task 3, where they are green both before and after; and the ADR and docs, which reference nothing at import time, land after in Task 5.

Two smaller orderings inside that:

- **Task 2 before Task 4** because retiring `EventStoreConformanceSuite` removes `testing/conformance.py`'s import of `stores.interface` — the only `src/` importer other than `__init__.py`. Doing it first means Task 4's deletion faces exactly one src-side importer.
- **Task 1 before Task 4** because the outbox fixture in `tests/integration/conftest.py:611` constructs the *legacy* `PostgreSQLEventStore` from `eventsource` with a `session_factory`. When Task 4 rebinds that name to the adapter (which takes an `AsyncEngine`), the fixture breaks with a `TypeError` that no unit run would catch. Task 1 retargets it explicitly and unskips the e2e test that has been waiting on it.

### Behavior deltas this slice makes visible

Spec decisions, not implementation choices. Repeated in the task that first encounters each.

| Delta | Legacy | Ports | Who feels it |
|---|---|---|---|
| `TypeConverter` field-name guessing | string values inside untyped `dict[str, Any]` event fields rewritten to `UUID`/`datetime` by `_id`/`_at` suffix heuristics | untyped dict fields round-trip as plain JSON types | Task 4: `stores/_type_converter.py` and its 333-line suite die; CHANGELOG calls it out loudly (Task 5) |
| `stored_at` on the in-memory store | fabricated as `event.occurred_at` | real `datetime.now(UTC)` | Task 3: any surviving assertion of `stored_at == occurred_at` is wrong and is rewritten, not relaxed |
| Store spans | `inmemory_event_store.*` / `postgresql_event_store.*` / `sqlite_event_store.*` | adapters emit none (ADR 0016 amendment) | Task 4: the three `tests/unit/stores/test_*_tracing.py` suites are deleted, not retargeted |
| Category read | filters/orders on `event.occurred_at`, exclusive `>` | filters/orders on `stored_at`, inclusive `>=`, position tie-break | Task 3: timestamp-suite cases that assert the exclusive boundary die; §5.2 |
| BACKWARD feed reads, feed timestamp filters | expressible via `ReadOptions` | no ports equivalent; capability dropped | Task 4: the BACKWARD cases in `tests/stores/test_read_all_tenant_filter.py` die with the file |
| Cross-type `get_events(aggregate_type=None)` | expressible | no port; dropped (spec §2) | already gone in slice (c); Task 5 records it in ADR 0025 |
| Duplicate `event_id` | in-memory/postgresql silently skip | `DuplicateEventError` | Task 5: ADR 0025 + CHANGELOG |
| `AppendResult.position` | position of the **last** appended event | position of the **first** | Task 5: ADR 0025 + CHANGELOG |
| Empty append batch | `AppendResult.successful(expected_version)` | `ValueError` | Task 5: CHANGELOG |
| Empty store | `get_global_position()` returns `0` | `current_position()` returns `None` | Task 5: CHANGELOG |
| `OptimisticLockError.expected_version` | int, carrying the -1/0/-2 sentinels | **unchanged** — deliberately still int (spec §1.7) | Task 5: ADR 0025 records the deliberate non-change |

---

### Task 1: Port the outbox write onto the PostgreSQL adapter

**Files:**
- Modify: `src/eventsource/adapters/postgresql/store.py`
- Create tests: `tests/unit/adapters/test_postgresql_outbox.py`
- Modify tests: `tests/integration/conftest.py` (the two postgres store fixtures only), `tests/integration/e2e/test_full_flow.py` (unskip), `tests/integration/repositories/test_outbox.py` (whatever it constructs)

**Interfaces:**
- Consumes: nothing from this slice.
- Produces (used by Task 4, which rebinds `eventsource.PostgreSQLEventStore` to this class):

```python
class PostgreSQLEventStore:
    def __init__(self, engine: AsyncEngine, event_registry: EventRegistry | None = None, *,
                 store_id: str | None = None, create_schema: bool = False,
                 outbox_enabled: bool = False) -> None: ...
    @property
    def outbox_enabled(self) -> bool: ...
```

**Why this is a real feature and not legacy cruft (spec §4.4).** Same-transaction outbox writes are the entire point of the transactional outbox pattern: the event row and the outbox row commit or roll back together. The legacy `PostgreSQLEventStore` is the **only** outbox writer in the tree. Deleting it without a replacement would silently downgrade the pattern to a two-phase write with no atomicity, which is a correctness regression disguised as a refactor. The wider outbox ring-migration (protocol split, move to `ports/`+`adapters/`) is explicitly out of scope (spec §10) — only the write path moves.

- [ ] **Step 1: read the legacy implementation before writing anything**

Read `src/eventsource/stores/postgresql.py:417-459` (`_write_to_outbox`) in full, and the `append_events` block that calls it — specifically **where** in the transaction the call sits relative to the event `INSERT` and the `session.commit()`. Read `src/eventsource/adapters/postgresql/store.py:241-330` (`append`) alongside it.

Two things must be true of the port and must be confirmed by reading, not assumed:

- The outbox `INSERT` runs on the **same `AsyncSession`** as the event `INSERT`, before the single `await session.commit()`. If the adapter's structure would put it on a different session or after the commit, stop and report — the atomicity is the feature.
- The legacy version writes **one outbox row per event** in the batch, with `event_data` a `json.dumps` of a dict containing `event_id`, `aggregate_id`, `aggregate_type`, `tenant_id`, `occurred_at` (ISO), and `payload` (`model_dump(mode="json")`). Reproduce that payload shape exactly; the outbox *reader* (`repositories/outbox.py`) parses it and is not changing.

Note the two call-site differences you must bridge: the legacy method takes `aggregate_type: str` explicitly, where the adapter has `stream.category`; and the legacy store uses `json.dumps` from the stdlib where the adapter has `json_dumps` from `eventsource.serialization`. Use the adapter's `json_dumps` for consistency with the rest of the module **only if** it produces byte-compatible output for this payload — check `serialization/` before switching, and if there is any doubt, use stdlib `json.dumps` exactly as the legacy store does and say so in a comment. The reader's expectations win over module-local consistency.

- [ ] **Step 2 (red): unit-test the outbox write against the adapter**

Create `tests/unit/adapters/test_postgresql_outbox.py`. The existing unit coverage for this adapter mocks sqlalchemy sessions — read `tests/unit/test_postgresql_event_store.py:100-140` (the legacy fixtures, which still exist at this point and are the closest working model) and `tests/unit/adapters/` for the house style, and follow whichever the adapter's own tests already use.

Cases:

- `outbox_enabled=False` (the default): `append` executes no `INSERT INTO event_outbox`.
- `outbox_enabled=True`: `append` of N events executes N outbox inserts, on the same session object as the event inserts, before commit.
- `outbox_enabled=True`: the bound `event_data` parses back to a dict carrying the six documented keys, with `payload` equal to `event.model_dump(mode="json")`.
- The `outbox_enabled` property reflects the constructor argument.

Run: `uv run pytest tests/unit/adapters/test_postgresql_outbox.py -q` — Expected: FAIL (`TypeError: unexpected keyword argument 'outbox_enabled'`).

- [ ] **Step 3 (green): implement**

Add `outbox_enabled: bool = False` as a keyword-only constructor parameter, store it, expose the read-only property, and add `_write_to_outbox` ported per Step 1. Call it inside `append`'s existing `async with self._session_factory() as session:` block, per event, after that event's `INSERT ... RETURNING global_position` and before `await session.commit()`.

Docstring the parameter with the guarantee it provides ("the outbox row and the event row commit in the same transaction") and the pointer that the outbox *reader* lives in `eventsource.repositories.outbox`. Do not add tracing (ADR 0016 amendment: the adapters carry no store spans).

- [ ] **Step 4 (green): retarget the integration fixtures and unskip the e2e test**

- `tests/integration/conftest.py:~587` (`postgres_event_store`) and `:~611` (`postgres_event_store_with_outbox`) both do `from eventsource import EventRegistry, PostgreSQLEventStore` and construct with `postgres_session_factory`. Change both to `from eventsource.adapters.postgresql import PostgreSQLEventStore` — **path-only, explicitly**, not through the top-level name, because Task 4 has not rebound it yet — and construct with the `postgres_engine` fixture (`tests/integration/conftest.py:407`) instead of the session factory. Add the `postgres_engine` fixture to each fixture's parameters and drop `postgres_session_factory` if nothing else in the fixture body needs it. Leave `outbox_enabled=False` / `=True` as they are. Keep `clean_postgres_tables` in the signature.
  - Check whether either fixture needs `await store.close()` on teardown: the adapter's `close()` disposes the engine, and the `postgres_engine` fixture already owns that lifecycle. Disposing a session-scoped engine from a function-scoped fixture would break every later test — read `postgres_engine`'s scope before deciding, and if it is not function-scoped, do **not** call `close()`.
- `tests/integration/e2e/test_full_flow.py:~522`: delete the `@pytest.mark.skip` decorator and its reason block. The test body already calls `append(StreamId(...), [event], ExpectedVersion.no_stream())` — ports-shaped — so it should pass unmodified once the fixture yields the adapter. Verify the imports at the top of that module resolve `PostgreSQLEventStore` to something that type-checks against the fixture; retarget the type hint if it points at the legacy class.
- `tests/integration/repositories/test_outbox.py`: locate what it constructs and retarget identically.

- [ ] **Step 5: verify**

```bash
grep -rn "outbox" src/eventsource/adapters/postgresql/store.py
```
Expected: the constructor parameter, the property, `_write_to_outbox`, its call site, and docstring prose — nothing else.

```bash
grep -rn "pytest.mark.skip" tests/integration/e2e/test_full_flow.py
```
Expected: no matches referencing the outbox.

Run: `uv run pytest tests/unit/adapters/ -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/adapters/ tests/unit/adapters/ tests/integration/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

The integration suites require Docker and are the orchestrator's to run; do not start services.

- [ ] **Step 6: commit**

```bash
git add src/eventsource/adapters/postgresql/store.py tests/unit/adapters/test_postgresql_outbox.py \
        tests/integration/conftest.py tests/integration/e2e/test_full_flow.py \
        tests/integration/repositories/test_outbox.py
git commit -m "feat: same-transaction outbox writes on the postgresql adapter"
```

Record in the commit body: that the outbox row and event row share one session and one commit, the payload shape reproduced verbatim for the existing reader, and that the e2e test skipped since slice (a) is now live.

---

### Task 2: Retire `EventStoreConformanceSuite` after a case-by-case gap check

**Files:**
- Modify: `src/eventsource/testing/conformance.py`, `src/eventsource/testing/__init__.py`
- Possibly modify: `src/eventsource/testing/conformance_ports/{stream_reader,feed}.py` (only if Step 1 finds a real gap)
- Modify tests: `tests/unit/test_conformance.py` (trim to the bus half)

**Interfaces:**
- Consumes: nothing.
- Produces: `testing/conformance.py` containing `EventBusConformanceSuite` and nothing else, with no import of `eventsource.stores`.

**Spec §5.3 requires the gap check before the deletion, case by case.** `conformance_ports/` is the richer per-port successor (78 cases across six suites plus a hypothesis `StoreStateMachine`), but "richer overall" is not "covers each of these eight". Do the comparison honestly; a lost assertion is a lost guarantee for every third-party adapter author.

- [ ] **Step 1 (analysis): the eight-case gap check**

The legacy suite has exactly eight store cases (`src/eventsource/testing/conformance.py`, lines 91-351). Read each one's body, then read the candidate successor's body, and record a verdict for each in the commit body. The plan author's reading is below — **verify it, do not trust it**; where you disagree, your reading of the code wins and you say so in the commit body.

| Legacy case | Candidate successor | Author's reading |
|---|---|---|
| `test_append_and_get_roundtrip` (`:91`) | `StreamReaderConformance::test_read_returns_exactly_appended_events_in_order` (`stream_reader.py:31`) | covered |
| `test_stream_isolation` (`:130`) | — | **gap candidate.** No ports case appends to two distinct streams and asserts each read returns only its own. `CategoryQueryConformance::test_only_named_category_returned` is category isolation, not stream isolation. |
| `test_optimistic_locking` (`:171`) | `AppenderConformance::test_exact_append_mismatched_version_conflicts` (`appender.py:104`) | covered |
| `test_empty_stream` (`:207`) | `StreamReaderConformance::test_get_stream_version_of_absent_stream_is_zero` (`stream_reader.py:98`) | **partial.** The version half is covered; whether *reading* an absent stream yields zero envelopes rather than raising may not be. Check. |
| `test_event_metadata_preserved` (`:230`) | — | **gap candidate**, and the one spec §5.3 names by name. Assert at envelope level: `event_id`, `event_type`, `aggregate_id`, `aggregate_version`, `occurred_at`, `tenant_id` survive a round-trip unchanged, plus `envelope.stream_version` and `envelope.stream_id`. |
| `test_event_exists_idempotency` (`:260`) | `EventLookupConformance` (`event_lookup.py:32,39,44`) | covered |
| `test_expected_version_any` (`:288`) | `AppenderConformance::test_any_append_to_{fresh,existing}_stream_succeeds` (`appender.py:62,69`) | covered |
| `test_global_position_tracking` (`:320`) | `GlobalFeedConformance::test_full_read_returns_all_events_in_position_order` + `test_current_position_equals_last_envelope_position` (`feed.py:32,94`) | covered |

- [ ] **Step 2 (red then green): port the real gaps into the ports suites**

For each case Step 1 confirms as a gap, add it to the appropriate `conformance_ports/` suite — `test_stream_isolation` and the absent-stream read to `StreamReaderConformance`, metadata preservation to `StreamReaderConformance` (it is a stream-read assertion about envelope contents). Match the surrounding style exactly: the suites take a `store: _AppenderReader` fixture and use the `_fixtures.py` helpers; read two neighbouring cases before writing.

Do **not** port a case Step 1 found covered "for completeness" — a duplicated assertion in a suite every adapter runs is pure cost.

Run: `uv run pytest tests/unit/adapters/ -q` before adding (baseline PASS) and after (still PASS, with more cases collected). The memory/sqlite adapter conformance runners under `tests/unit/adapters/` pick up new suite cases automatically; confirm the collected count rose by exactly the number you added.

If a newly added case **fails** on an adapter, stop and report. That is a genuine adapter bug the legacy suite was catching and the ports suites were not — which is precisely what this gap check exists to find, and it is not an implementer's call to fix silently.

- [ ] **Step 3 (green): delete the suite**

- `src/eventsource/testing/conformance.py`: delete `class EventStoreConformanceSuite` (lines ~34-351), the `from eventsource.stores.interface import EventStore, ExpectedVersion` import, the `OptimisticLockError` import if it becomes unused, the `"EventStoreConformanceSuite"` entry in `__all__` (`:695`), and the module docstring's store example (`:9-16`). Rewrite the module docstring to describe a bus-only conformance surface and to point store implementers at `eventsource.testing.conformance_ports`.
- `src/eventsource/testing/__init__.py`: delete the import (`:46`) and the `__all__` entry (`:66`).
- `tests/unit/test_conformance.py` (123 lines): delete the `InMemoryEventStoreConformance` class and its imports; keep the `EventBusConformanceSuite` half. If nothing remains of the store half, the file shrinks; if the file becomes bus-only in a way that duplicates an existing bus conformance test module, say so in the commit body rather than deleting the file on your own judgement.

- [ ] **Step 4: verify**

```bash
grep -rn "EventStoreConformanceSuite" src/ tests/ bench/ examples/
```
Expected: no matches. (`docs/` still has matches in `guides/validate-custom-backend.md`, `api/index.md`, `api/testing.md`, `tutorials/08-testing.md`, `core-surface.md` — those are Task 5's.)

```bash
grep -rn "eventsource.stores" src/eventsource/testing/
```
Expected: no matches.

Run: `uv run pytest tests/unit/test_conformance.py tests/unit/adapters/ -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/testing/ tests/unit/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean. (`eventsource.testing.conformance` stays listed in the Tier-0 forbidden contract and stays Tier 0 — it now imports strictly fewer modules.)

- [ ] **Step 5: commit**

```bash
git add src/eventsource/testing/conformance.py src/eventsource/testing/__init__.py \
        src/eventsource/testing/conformance_ports/ tests/unit/test_conformance.py
git commit -m "refactor: retire the abc event store conformance suite"
```

Record in the commit body: the eight-case verdict table from Step 1, which gaps were real, and where each ported assertion now lives.

---

### Task 3: Pre-migrate the surviving legacy-touching test modules

**Files:**
- Modify tests: `tests/unit/test_edge_cases.py` (621), `tests/unit/test_additional_coverage.py` (495), `tests/unit/test_timestamp_types.py` (228), `tests/conftest.py` (the `:622` legacy sqlite import only)

**Interfaces:**
- Consumes: nothing.
- Produces: zero references to `eventsource.stores` or to legacy store classes in any test module that **survives** slice (d). Everything left pointing at the legacy surface after this task is a file Task 4 deletes outright.

**Why this task exists.** These four modules are green against the legacy surface today and must be green against the adapters tomorrow; nothing about them requires the deletion to have happened. Lifting them out of Task 4 removes roughly a thousand lines of unrelated churn from the one commit that cannot be split, and it lets their behavior deltas (`stored_at`, category-read boundaries) be reviewed on their own.

- [ ] **Step 1: inventory before editing**

```bash
grep -n "stores\|InMemoryEventStore\|PostgreSQLEventStore\|SQLiteEventStore\|StoredEvent\|ReadOptions\|EventStream\|append_events\|get_events\|get_global_position\|ExpectedVersion\|validate_timestamp" \
  tests/unit/test_edge_cases.py tests/unit/test_additional_coverage.py tests/unit/test_timestamp_types.py tests/conftest.py
```

Record the hit list. Classify each hit as RETARGET (constructor/fixture swap), REWRITE (asserts on legacy types or legacy boundary semantics), or DELETE (tests a construct that dies — the legacy dataclasses `StoredEvent`/`EventStream`/`ReadOptions`, the `validate_timestamp` helper, `TypeConverter`). Spec §7 slice (d) marks all four modules REWRITE with a DELETE subset; the classification per case is yours to make from the code.

- [ ] **Step 2 (red/green): `tests/unit/test_timestamp_types.py`**

Per spec §5.2: `stores/_compat.py::validate_timestamp` dies with its importers, so cases exercising the helper directly are **deleted**. Cases asserting *store-level* timestamp behavior are checked against `CategoryQueryConformance` (which has `test_from_timestamp_honored`, `test_from_timestamp_is_inclusive`, `test_ordered_by_stored_at`) — if the assertion is already covered there, delete it; if it is a genuinely uncovered store behavior, retarget it onto `MemoryEventStore` and note in the commit body why it is not conformance material.

The category-read boundary flips here: legacy filtered on `event.occurred_at` **exclusive** (`>`), the port filters on `stored_at` **inclusive** (`>=`) with position tie-break. A case asserting the exclusive boundary is asserting behavior that no longer exists — delete it or invert it deliberately; do not "fix" it by nudging a timestamp until it passes.

- [ ] **Step 3 (red/green): `tests/unit/test_edge_cases.py` and `tests/unit/test_additional_coverage.py`**

Drop the legacy-dataclass cases (`StoredEvent`, `EventStream`, `ReadOptions`, legacy `AppendResult`/`ExpectedVersion` construction). Retarget the rest onto `MemoryEventStore` — **`MemoryEventStore`, not `InMemoryEventStore`**; the rename is Task 4's, and using the future name here would not import.

Translate call sites per spec §1: `append_events(id, type, events, n)` → `append(StreamId(aggregate_id=id, category=type), events, ExpectedVersion...)` mapping the sentinel **by name** (`stores/legacy.py::_expected_from_int` is the executable reference and still exists at this point); `get_events(...)` → `[e async for e in store.read_stream(...)]` or `collect(...)`; `EventStream.version` → `get_stream_version(stream)`; `StoredEvent.global_position`/`.stream_position` → `EventEnvelope.position`/`.stream_version`.

Watch for the `stored_at` delta: the legacy in-memory store fabricated `stored_at=event.occurred_at`; the memory adapter stamps a real `datetime.now(UTC)`. An assertion of equality between the two is now false and must be rewritten to assert what it actually meant (usually ordering or presence), not deleted silently.

- [ ] **Step 4 (green): `tests/conftest.py:622`**

That line is `from eventsource.stores.sqlite import SQLiteEventStore` inside a fixture. Retarget it to `from eventsource.adapters.sqlite import SQLiteEventStore` and adjust the construction: the adapter takes `(database, event_registry=None, *, store_id=None, wal_mode=True, busy_timeout=5000)` and **lazily connects and applies schema on first use** — delete any `initialize()` / `async with` ceremony the fixture performs, and keep `close()` on teardown (the adapter has one). Read the fixture body and its consumers before changing it; if a consumer depends on `is_connected` / `database` / `wal_mode` / `busy_timeout` properties the adapter does not have, report rather than inventing them.

The root `in_memory_store` fixture (`:305`) already yields `MemoryEventStore` — slice (a) did that. Do not touch it.

- [ ] **Step 5: verify**

```bash
grep -rn "eventsource.stores" tests/ --include='*.py' | grep -v "tests/unit/test_in_memory_event_store.py\|tests/unit/test_event_store_interface.py\|tests/unit/test_postgresql_event_store.py\|tests/stores/\|tests/unit/stores/\|tests/unit/test_eventstore_global_position.py\|tests/unit/test_public_api.py"
```
Expected: no matches. Everything still referencing the legacy surface is a file Task 4 deletes or rewrites.

Run: `uv run pytest tests/unit/test_edge_cases.py tests/unit/test_additional_coverage.py tests/unit/test_timestamp_types.py -q` — Expected: PASS.
Run: `uv run pytest tests/unit/ -q -x --co` — Expected: collection succeeds (nothing else broke).
Run: `uv run ruff check tests/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 6: commit**

```bash
git add tests/unit/test_edge_cases.py tests/unit/test_additional_coverage.py \
        tests/unit/test_timestamp_types.py tests/conftest.py
git commit -m "test: move the surviving legacy-surface test modules onto the adapters"
```

Record in the commit body: the DELETE/RETARGET/REWRITE counts, which timestamp cases were dropped as conformance-covered, and every `stored_at` assertion that changed meaning.

---

### Task 4: The deletion — rename, public-API swap, `stores/` removed

**Why this task is one commit.** See "Deletion ordering" in the Global Constraints. In short: the rename needs the legacy class gone, the swap needs the legacy tests gone, and the deletion needs the swap done. Tasks 1-3 removed everything that could be removed from this commit; what remains is irreducible. The steps are ordered so an implementer can work outside-in, and the orchestrator may dispatch Steps 1-4 and Steps 5-8 as separate work units against the same uncommitted tree — but there is exactly one commit at the end.

**Files:**
- Delete: `src/eventsource/stores/` (entire package: `interface.py` 624, `in_memory.py` 695, `postgresql.py` 981, `sqlite.py` 1102, `legacy.py` 261, `_type_converter.py` 317, `_compat.py` 31, `__init__.py` 54, `README.md` 471)
- Delete tests: `tests/unit/test_event_store_interface.py` (706), `tests/unit/test_in_memory_event_store.py` (1157), `tests/unit/test_postgresql_event_store.py` (1473), `tests/stores/` (`__init__.py`, `test_read_all_tenant_filter.py` 437, `test_sqlite_event_store.py` 1214), `tests/unit/stores/` (`__init__.py`, `test_legacy_adapter.py` 269, `test_type_converter.py` 333, `test_memory_tracing.py` 593, `test_postgresql_tracing.py` 538, `test_sqlite_tracing.py` 445), `tests/unit/test_eventstore_global_position.py` (299)
- Modify: `src/eventsource/__init__.py`, `src/eventsource/adapters/memory/store.py`, `src/eventsource/adapters/memory/__init__.py`, `pyproject.toml` (import-linter Tier-0 rows only), plus every `MemoryEventStore` importer (Step 1 enumerates them)
- Rewrite tests: `tests/unit/test_public_api.py` (126)

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces, per spec §4.1 — the blessed store surface of `eventsource`:

| Category | Names |
|---|---|
| Ports | `EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`, `FullEventStore`, `AggregateStore`, `collect` |
| Value objects | `EventEnvelope`, `AppendResult`, `Position`, `ExpectedVersion`, `ReadDirection`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions` — all the `ports` definitions, now bound at top level |
| Store adapters | `InMemoryEventStore` (renamed), `SQLiteEventStore`, `PostgreSQLEventStore` (the adapter classes), `IntPositionCodec`, `ASYNCPG_AVAILABLE`, `AIOSQLITE_AVAILABLE`, `SQLITE_AVAILABLE` |
| Sync | `SyncEventStoreAdapter` (already retyped in slice (a)) |
| Exceptions | unchanged |

Names that die from `eventsource` and everywhere (spec §4.2): `EventStore`, `EventStream`, `StoredEvent`, `ReadOptions`, `LegacyStoreAdapter`, `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS`, the legacy `ExpectedVersion` / `ReadDirection` / `AppendResult` classes, the legacy `InMemoryEventStore` / `PostgreSQLEventStore` / `SQLiteEventStore` classes, `MemoryEventStore` (renamed, not removed), and the `eventsource.stores` import path itself. `EventPublisher` **survives** — it keeps its home in `ports/bus.py`; only the `stores/interface.py` re-export dies.

- [ ] **Step 1: enumerate the rename's blast radius**

```bash
grep -rln "MemoryEventStore" src/ tests/ bench/ examples/ | sort
```

Record the list. It includes files where the match is the *legacy* `InMemoryEventStore` (a substring of the same string) — separate the two before editing. A blind `sed s/MemoryEventStore/InMemoryEventStore/` would turn `InMemoryEventStore` into `InInMemoryEventStore`; the safe form is a word-boundary-anchored replacement of `MemoryEventStore` that does not match when preceded by `In`, applied only to files whose legacy references are being deleted in this same commit anyway.

Note the four `src/` sites that are **not** simple imports: `adapters/memory/store.py` (the class definition and its docstring), `adapters/memory/__init__.py` (import + `__all__`), `testing/harness.py` (6 references incl. two `-> MemoryEventStore` return annotations and prose at `:46`), `application/aggregates/repository.py` (2, in docstrings), `testing/partitioned_memory.py` (1), `__init__.py` (4).

- [ ] **Step 2 (red): rewrite `tests/unit/test_public_api.py`**

This file is the acceptance test for spec §4 and it flips wholesale. It currently asserts, positively, that six top-level names **are** the legacy classes (`TestCollisionDecisions`, lines ~64-126). After the swap:

- Delete `TestCollisionDecisions` entirely. Every assertion in it is about a collision that no longer exists.
- In `CORE_RINGS_EXPORTS`: `"MemoryEventStore"` → `"InMemoryEventStore"`; delete `"LegacyStoreAdapter"`; add `"SQLiteEventStore"`, `"PostgreSQLEventStore"`, `"ASYNCPG_AVAILABLE"`, `"AIOSQLITE_AVAILABLE"`, `"SQLITE_AVAILABLE"`, `"ExpectedVersion"`, `"ReadDirection"`, and any other §4.1 name the list is missing. Guard the sqlite-dependent entries the way the module's other optional names are guarded, if any are — if `aiosqlite` is always installed in the dev environment, still write the test so it does not lie when it is not (read how `SQLITE_AVAILABLE` is used elsewhere in the suite).
- Replace the deleted collision class with a positive identity class asserting the **new** bindings, which is the real acceptance for §4.1:

```python
class TestBlessedStoreSurface:
    """Spec §4.1: one referent per name. The ports and adapters own them all."""

    def test_top_level_expected_version_is_the_port_vo(self) -> None:
        from eventsource.ports import ExpectedVersion
        assert eventsource.ExpectedVersion is ExpectedVersion

    def test_top_level_in_memory_event_store_is_the_memory_adapter(self) -> None:
        from eventsource.adapters.memory.store import InMemoryEventStore
        assert eventsource.InMemoryEventStore is InMemoryEventStore
```

  — and the same shape for `ReadDirection`, `AppendResult`, `SQLiteEventStore`, `PostgreSQLEventStore`.
- Add a class asserting the **absence** of every §4.2 name, which is the acceptance for the deletion:

```python
DEAD_NAMES = [
    "EventStore", "EventStream", "StoredEvent", "ReadOptions", "LegacyStoreAdapter",
    "TypeConverter", "DefaultTypeConverter", "DEFAULT_UUID_FIELDS",
    "DEFAULT_STRING_ID_FIELDS", "MemoryEventStore",
]

def test_dead_names_are_gone_from_the_public_api() -> None:
    for name in DEAD_NAMES:
        assert not hasattr(eventsource, name), f"eventsource.{name} should not exist"
        assert name not in eventsource.__all__
```

- Add a case asserting `import eventsource.stores` raises `ModuleNotFoundError`. That is the single strongest pin on the deletion and it belongs in this file, not a new one.

Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: FAIL.

- [ ] **Step 3 (green): rename `MemoryEventStore` → `InMemoryEventStore`**

Rename the class in `src/eventsource/adapters/memory/store.py` and update its docstring; update `adapters/memory/__init__.py` (import and `__all__`); then apply the rename across every site from Step 1 **except** the files being deleted in Step 5 (do not spend edits on doomed files). Rationale for the name, for the commit body: every sibling memory adapter is `InMemory*` (`InMemorySnapshotStore`, `InMemoryCheckpointRepository`, `InMemoryDLQRepository`, `InMemoryEventBus`, `InMemoryTestHarness`); a lone `Memory*` would be permanent public-API inconsistency.

Do not add an alias. Do not keep `MemoryEventStore` importable from anywhere.

- [ ] **Step 4 (green): rewrite the `__init__.py` store section**

In `src/eventsource/__init__.py`:

- Delete the five `from eventsource.stores...` imports (`:206`, `:209-218`, `:219`, `:220`, `:228`).
- Delete the dual-export comment block (`:24-38`) in full. It exists solely to explain a collision that no longer exists; leaving it would document a state the code has left.
- Bind `ExpectedVersion`, `ReadDirection`, `AppendResult` from `eventsource.ports` (the module already imports many names from there — extend that import rather than adding a second one).
- Bind `PostgreSQLEventStore` from `eventsource.adapters.postgresql` and `SQLiteEventStore` from `eventsource.adapters.sqlite`; bind `InMemoryEventStore` from `eventsource.adapters.memory` (which the module already imports from).
- **The `SQLITE_AVAILABLE` guard (spec §4.3) needs care.** Today the flag is a side effect of a `try/except ImportError` that imports *both* `repositories.outbox.SQLiteOutboxRepository` and the legacy sqlite store (`:224-231`). Two facts change it: `adapters/sqlite/store.py` imports cleanly **without** aiosqlite (guarded `import aiosqlite`, constructor raises `ImportError` with the install hint), and `adapters/sqlite/__init__.py` already computes and exports both `AIOSQLITE_AVAILABLE` (store) and `SQLITE_AVAILABLE` (snapshots), which are equivalent — both mean "aiosqlite importable".
  - Import `SQLITE_AVAILABLE` and `AIOSQLITE_AVAILABLE` from `eventsource.adapters.sqlite` rather than deriving a flag from a `try/except`.
  - Import `SQLiteEventStore` unconditionally (it imports cleanly without the driver).
  - Keep the `try/except ImportError` for `repositories.outbox.SQLiteOutboxRepository` — spec §4.3 says that guard is untouched (it is the outbox slice's problem) — but stop having it define `SQLITE_AVAILABLE`. If the resulting shape has the outbox import in a bare `try/except ImportError: pass`, ruff may object; follow the module's existing idiom for optional imports and report if the two rules conflict.
  - Keep the `if SQLITE_AVAILABLE: __all__ += [...]` conditional for the sqlite class names, exactly as spec §4.3 describes.
- Update `__all__`: remove every §4.2 name (`:272-282` region plus `"LegacyStoreAdapter"` at `:402` and `"MemoryEventStore"` at `:399`), add `"InMemoryEventStore"`, `"ASYNCPG_AVAILABLE"`, `"AIOSQLITE_AVAILABLE"`, `"PostgreSQLEventStore"`, and confirm `"ExpectedVersion"`, `"ReadDirection"`, `"AppendResult"` are present exactly once each. Run the duplicate check the test file already has.
- Update the module docstring's opening bullet list if it names the legacy backends.

- [ ] **Step 5 (green): delete**

```bash
git rm -r src/eventsource/stores/
git rm tests/unit/test_event_store_interface.py tests/unit/test_in_memory_event_store.py \
       tests/unit/test_postgresql_event_store.py tests/unit/test_eventstore_global_position.py
git rm -r tests/stores/ tests/unit/stores/
```

Before running these, confirm with `git status --porcelain` that no other agent has uncommitted work under those paths. If any file there is modified, stop and report rather than deleting someone's work.

Spec §7 slice (d) justifies each deletion: `test_read_all_tenant_filter.py`'s tenant-filter cases are already in `GlobalFeedConformance::test_tenant_filter_honored`, and its BACKWARD-feed cases die with a capability that has no ports equivalent and no runtime consumer; the three `test_*_tracing.py` suites assert store spans the adapters deliberately do not emit (ADR 0016 amendment); `test_type_converter.py` dies with the field-name-guessing it tests (§5.1).

- [ ] **Step 6 (green): contracts**

In `pyproject.toml`, in the `Tier 0 modules must not import sqlalchemy` contract's `source_modules`, delete the two rows whose modules no longer exist: `"eventsource.stores.interface"` and `"eventsource.stores.in_memory"`. Change nothing else — the independence contract was already swapped to `eventsource.adapters.*` in an earlier slice (verified at HEAD), and `[tool.mutmut]` has no `stores/` entry (verified), so neither needs an edit here.

**Concurrency caveat:** other agents edit `pyproject.toml`. Locate the contract by its `name = "Tier 0 modules must not import sqlalchemy"` line, not by line number, and re-read the file immediately before editing.

- [ ] **Step 7: sweep**

```bash
grep -rn "eventsource\.stores\|from eventsource import.*LegacyStoreAdapter\|LegacyStoreAdapter\|StoredEvent\|EventStream\|ReadOptions\|TypeConverter\|DEFAULT_UUID_FIELDS\|DEFAULT_STRING_ID_FIELDS\|validate_timestamp" \
  src/ tests/ bench/ examples/ pyproject.toml
```
Expected: **zero** matches. Any hit is either a file that should have been deleted or a consumer nobody retargeted.

```bash
grep -rn "\bMemoryEventStore\b" src/ tests/ bench/ examples/ | grep -v "InMemoryEventStore"
```
Expected: zero matches.

```bash
grep -rn "append_events\|get_events_by_type\|get_global_position\|\.stream_position\|\.global_position" src/ tests/ bench/ examples/
```
Expected: zero matches in `src/`. In `tests/`, the only acceptable hits are SQL column names (`global_position` is still the PostgreSQL/SQLite column the `IntPositionCodec` mints from, and the frozen legacy checkpoint column) — inspect every hit and confirm it is a column reference, not a method call.

```bash
grep -rn "eventsource.stores\|EventStore\b" src/eventsource/adapters/postgresql/store.py src/eventsource/adapters/sqlite/store.py
```
Expected: only the class names `PostgreSQLEventStore` / `SQLiteEventStore` themselves. The two module docstrings currently say "Ported from `eventsource.stores.postgresql.PostgreSQLEventStore` (untouched -- that module remains the legacy `EventStore` ABC implementation)" (`postgresql/store.py:3-4`) and the same for sqlite (`sqlite/store.py:3`). That statement is now false. Rewrite both to describe what the module *is*, without referencing a module that does not exist.

Two more one-liner docstring sweeps folded in here (accumulated carry-forwards, both stale prose left by slice (c)):

- `src/eventsource/migration/router.py:~79` — `StoreNotFoundError`'s docstring says "cannot be resolved to an `EventStore`". Say "to a registered store".
- `src/eventsource/migration/dual_write.py:~15` — the module docstring bullet "Implement EventStore protocol for transparent integration". Say "Satisfy the `FullEventStore` port for transparent integration".

- [ ] **Step 8: verify**

Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: PASS.
Run: `uv run python -c "import eventsource; print(len(eventsource.__all__))"` — Expected: imports cleanly.
Run: `uv run python -c "import eventsource.stores"` — Expected: `ModuleNotFoundError`.
Run: `uv run pytest tests/unit/ -q` — Expected: PASS. (This task is the one exception to "targeted tests only": the unit suite is the only thing that proves the deletion did not orphan a consumer, and no narrower selection covers it. Do **not** run integration or `make check` — those stay the orchestrator's.)
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/ tests/ bench/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 9: commit**

```bash
git add -u src/eventsource/stores/ tests/stores/ tests/unit/stores/
git add src/eventsource/__init__.py src/eventsource/adapters/ src/eventsource/testing/ \
        src/eventsource/application/ src/eventsource/migration/router.py \
        src/eventsource/migration/dual_write.py pyproject.toml tests/
git status --porcelain
```

Review that `git status` output line by line before committing — this is the largest commit of the retirement and a stray file from another agent must not ride along.

```bash
git commit -m "refactor: delete the legacy store surface and swap the public api"
```

Record in the commit body: the deleted line count (~4,536 src + ~5,700 test), the rename, the six rebound names, the `EventPublisher` survival, the two removed Tier-0 contract rows, and the confirmation that `import eventsource.stores` now raises.

---

### Task 5: ADR 0025, contracts documentation, docs sweep, changelog

**Files:**
- Modify: `docs/adrs/0025-legacy-store-retirement.md`, `docs/adrs/0016-optional-tracing-no-op-by-default.md`, `docs/adrs/0019-clean-architecture-store-ports.md`, `docs/adrs/0024-projection-persistence-ports.md`, `docs/adrs/index.md`
- Modify: `docs/core-surface.md`, `docs/architecture.md`, `docs/reference/event-store-protocol.md`, `docs/api/{index,testing,sync,exceptions}.md`, `docs/guides/validate-custom-backend.md`, `docs/tutorials/08-testing.md`, `docs/how-to/choose-an-event-store-backend.md`, `docs/explanation/sql-backend-type-handling.md`, plus whatever Step 3's grep finds
- Modify: `CLAUDE.md` (Project Structure block), `CHANGELOG.md`
- Modify: `src/eventsource/migrations/additive/checkpoints_position_token.sql` (one comment)
- Create: `src/eventsource/adapters/README.md`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: no code surface.

- [ ] **Step 1: fill in ADR 0025**

`docs/adrs/0025-legacy-store-retirement.md` exists with `Status: Proposed` and a Context section. Change the Status to `Accepted` and write the Decision and Consequences. Follow the house shape — read `0019-clean-architecture-store-ports.md` and `0024-projection-persistence-ports.md` first; both are recent, complete, and by the same hand this record should sound like. **Do not rewrite the existing Context**; ADR bodies are immutable records and that paragraph was written by an earlier slice.

Per spec §9, the Decision must record all of:

1. Retirement of the `EventStore` ABC surface with **no shims and no back-compat aliases** (unreleased software; the standing rule).
2. The **by-name sentinel mapping** (`ANY`/`NO_STREAM`/`STREAM_EXISTS` → `.any_()`/`.no_stream()`/`.stream_exists()`), never by numeric coincidence.
3. Drop of cross-type `get_events(aggregate_type=None)`. Rejected alternative, named and not built: a narrow `StreamDiscovery.find_streams(aggregate_id) -> list[StreamId]` port.
4. `AppendResult.position` is the **first** appended event's position, where legacy `global_position` was the **last**.
5. Duplicate-append **raises** `DuplicateEventError` where legacy in-memory/postgresql silently skipped.
6. Category reads filter and order on **storage time**, **inclusive**, with position tie-break — where legacy used the event's own `occurred_at`, exclusive. Naive datetimes are rejected rather than silently compared.
7. `TypeConverter` removal: typed pydantic sub-models over field-name guessing inside untyped `dict[str, Any]` fields.
8. Store-span removal (**amends ADR 0016**); a ports-level tracing decorator is backlogged, not promised.
9. `SubscriptionPositions` Position retype (**amends ADR 0024**).
10. Count-behind lag (**completes 0019's amendment of 0014**).
11. `OptimisticLockError` keeps its **int** `expected_version` field, deliberately — retyping a widely-caught exception to carry the VO is churn with no consumer demand, and the sentinel constants are an adapter-internal message-formatting detail.
12. `MemoryEventStore` → `InMemoryEventStore`, for sibling-naming consistency.
13. Outbox write support ported onto the PostgreSQL adapter (Task 1), so the same-transaction guarantee survives the deletion of its only previous writer.

Two more that the slice (c) plan's self-review surfaced and that spec §9 does not list — include them, and note in the commit body that they are additions to the spec's enumeration:

14. **Nearest-position lookup became a binary search.** `find_nearest_source_position` was `ORDER BY source_position DESC LIMIT 1` over a BIGINT. Opaque tokens cannot be ordered in SQL (`Position.to_str()` is JSON; its lexicographic order is not position order), so the lookup became a binary search over the surrogate `id` with the comparison in Python, resting on a documented monotonicity precondition. Rejected alternative: load all mappings and scan (correct, unbounded memory).
15. **The legacy BIGINT position columns are frozen, not dropped.** `projection_checkpoints.global_position`, `migration_position_mappings.source_position`/`.target_position`, and `tenant_migrations.last_source_position`/`.last_target_position` are neither written nor read by the library after slice (c). They stay in the schema: dropping a column is destructive, `schemas/checkpoints.sql` is under the Do Not Modify rule, and the additive-fragment mechanism exists to *add*. They die with their own schema revision, not this one.

The ADR needs an **ADR Impact** table per `.claude/rules/definition-of-done.md`. Reproduce the spec's §"ADR Impact" table (0001 stands, 0014 stands as amended by 0019, 0015 stands, 0016 amended, 0018 stands, 0019 amended, 0021 stands, 0024 amended), rendered as this record's own impact statement rather than copied verbatim as prose.

- [ ] **Step 2: amend the three ADRs this one touches**

Per the definition-of-done rule, the Status pointers are part of the work, not follow-up. Add to each **Status** section — do not touch any Decision:

- `0016-optional-tracing-no-op-by-default.md`: "Amended by ADR 0025" + one line that per-operation store spans were removed with the legacy stores and the adapters carry none.
- `0019-clean-architecture-store-ports.md`: "Amended by ADR 0025". Its Status currently says the legacy ABC "remains the default shipped surface, behind that compatibility wrapper, until the application layer is retyped" — that condition has now ended, and the Status must say so. Add a Consequences line recording that the compat wrapper (`LegacyStoreAdapter`) and the legacy ABC are deleted, and that its Decisions all stand and are now the only surface.
- `0024-projection-persistence-ports.md`: "Amended by ADR 0025" for the `SubscriptionPositions` int→`Position` retype — **unless slice (b) already added it.** Read before writing; if the pointer is there, say so in the commit body and change nothing.

Update `docs/adrs/index.md`: the 0025 bullet currently ends "**Proposed** (full decision and consequences filled in by slice (d))". Rewrite it as a complete entry in the style of its neighbours — one dense sentence naming what it decides, its amendment relationships, and "Complete." Also update the 0016, 0019, and 0024 bullets to carry their new "Amended by ADR 0025" notes, matching how the file already marks amendments (e.g. the 0015 bullet's "**Amended by ADR 0024**").

- [ ] **Step 3: the docs sweep**

```bash
grep -rn "eventsource\.stores\|EventStore\b\|StoredEvent\|EventStream\|ReadOptions\|append_events\|get_events_by_type\|get_global_position\|LegacyStoreAdapter\|TypeConverter\|MemoryEventStore\|EventStoreConformanceSuite" docs/ examples/ README.md CLAUDE.md
```

Work the hit list. Skip `docs/superpowers/` entirely — specs and plans are dated records of what was decided when, and rewriting them destroys the audit trail. Everything else:

- **`docs/reference/event-store-protocol.md`** is the largest single job: it is a reference page *for the deleted ABC*, opening "Reference for the event store contract defined in `eventsource.stores.interface`". It becomes the reference for the five ports and their value objects — `EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`, `FullEventStore`, `AggregateStore`, plus `EventEnvelope`, `AppendResult`, `Position`, `ExpectedVersion`, `ReadDirection`, and the three read-options records. Rewrite it rather than patching names through it; a page describing "five abstract methods and three concrete methods with overridable defaults" cannot be edited into a description of segregated protocols. If a rewrite of this size looks like it will dominate the task, do it and report the size — do not leave it half-converted.
- **`docs/core-surface.md`** (spec §8): delete the `stores/*` rows (`:53`, `:71`, `:72`, `:73`, `:131`, `:184`, `:185`), update `:81` and finding 6 at `:244` (`testing/conformance.py` is now bus-only and imports `bus/interface`, `events/base`, `exceptions` — three modules, not four), update `:85`/`:86`/`:205` (the repository, sync adapter, and harness no longer name `stores/`), update `:100`/`:102`/`:108`/`:226`/`:228`/`:272` where they list `stores/interface` or `stores/in_memory` as a module's Tier-0 floor, and update the import-chain example at `:379-384` plus the file list at `:347-348`. Note that the deletion **shortens** the front-door sqlalchemy chain documented there: the chain no longer routes through `stores/__init__` → `stores/postgresql`. Re-derive it rather than editing it — read `__init__.py` and follow the first sqlalchemy-bearing import. The lazy-`__init__` work itself stays out of scope (spec §10); the document should still name the remaining front-door cost honestly.
- **`docs/architecture.md`**: the store narrative runs through `:19`, `:28`, `:218`, `:229`, `:233-234`, `:246`, `:272-273`, `:284`, `:299-300`, `:315-317`, `:329-332`, `:405`, `:418`, `:726`, `:853`, `:1058`, `:1075`. Two structural claims are now false and must be rewritten, not renamed: "`EventStore` (`stores/interface.py`) is an ABC, not a Protocol, and its abstract methods..." (`:418`) — the surface is five Protocols now — and the type-conversion paragraph at `:329-332` describing `_type_converter.py`/`_compat.py`, which describes deleted modules. `:315-317`'s account of the `SQLITE_AVAILABLE` guard changes shape per Task 4 Step 4.
- **`docs/guides/validate-custom-backend.md`** and **`docs/tutorials/08-testing.md`**: both teach subclassing `EventStoreConformanceSuite`. Retarget onto `eventsource.testing.conformance_ports` — the per-port suites are what a third-party adapter implements against now, and there are six of them plus a hypothesis state machine. Read `tests/unit/adapters/test_memory_conformance.py` for the working example of how a backend wires them up, and base the guide on it rather than inventing an idiom.
- **`docs/api/{index,testing,sync,exceptions}.md`**, **`docs/how-to/choose-an-event-store-backend.md`**, **`docs/installation.md`**, **`docs/getting-started.md`**, **`docs/index.md`**, **`docs/guides/*`**, **`docs/tutorials/*`**, **`docs/examples/projections.md`**, **`docs/development/testing.md`**: constructor signatures per spec §4.4 (postgres takes an `AsyncEngine`, not a `session_factory`; sqlite self-initializes and needs no `initialize()`/`async with`; memory takes `store_id` and `event_registry`), blessed names, ports-shaped call sites. `docs/explanation/sql-backend-type-handling.md` documents `TypeConverter` behavior that no longer exists — it either dies or becomes an explanation of *why* the library now relies on pydantic coercion and typed sub-models; prefer the rewrite, since the question it answers is still a real one.
- **`examples/*.py`** (`basic_usage`, `aggregate_example`, `imperative_example`, `projection_example`, `subscriptions/*`): these are executable. Their `InMemoryEventStore` references now resolve to the renamed adapter, so imports may already work — but any `append_events` / `get_events` call site does not. Run each one you touch: `uv run python examples/<file>.py`. If an example is not runnable without a database, say so in the commit body rather than guessing.
- **`CLAUDE.md`** Project Structure block: delete the `stores/` row (`:86`); update the `sync/` row (`:88`) to say the adapter wraps a `FullEventStore`; update the `testing/` row (`:89`) to name `conformance_ports/` as the backend conformance surface; update the `adapters/` row (`:71-73`) to say it holds the event store implementations for all three backends, since that is now where they solely live.
- **`src/eventsource/adapters/README.md`**: create it. `src/eventsource/stores/README.md` (471 lines) was deleted in Task 4; spec §9 says its content folds in here. Fold — do not copy. Roughly half of it documents `TypeConverter`, the legacy `ExpectedVersion` sentinels, `_SQLITE_AVAILABLE` privacy, and the ABC's method set, none of which exist. What survives is the backend-choice guidance, the outbox note (now pointing at the adapter's `outbox_enabled`), and the availability-flag explanation (now `AIOSQLITE_AVAILABLE`/`SQLITE_AVAILABLE`/`ASYNCPG_AVAILABLE` from the adapter packages). Match the style of the other per-directory READMEs in the repo.

- [ ] **Step 4: mark the frozen legacy column**

Add a comment to `src/eventsource/migrations/additive/checkpoints_position_token.sql` recording that `projection_checkpoints.global_position` is frozen — neither written nor read by the library after slice (b) — and that it is deliberately **not** dropped (destructive; `schemas/checkpoints.sql` is under the Do Not Modify rule; it dies with its own schema revision). Match the comment style already in that file and in `additive/migration_position_tokens.sql`, which carries the equivalent note for the migration tables.

This is a comment only. Do not add a `DROP COLUMN`. Do not touch anything under `schemas/` or `templates/`.

- [ ] **Step 5: changelog**

`CHANGELOG.md` `[Unreleased]` already carries per-slice entries from (a)-(c). Add slice (d)'s, and — because this is the slice where the surface actually changes for a user — a **Removed** section that names every §4.2 name explicitly. Three existing entries must also be corrected, because they describe a state that no longer holds:

- the `LegacyStoreAdapter` "Added" bullet now describes a deleted class;
- the "Colliding names stay path-only" **Changed** bullet describes collisions that no longer exist;
- the adapters bullet says `MemoryEventStore` is "re-exported as `eventsource.MemoryEventStore`".

Since `[Unreleased]` has not shipped, edit these in place to describe the end state rather than layering a correction on top — and say in the commit body that you did.

Call the behavior deltas out **loudly** (spec §9): the `TypeConverter` removal (§5.1), first-vs-last `AppendResult.position` (§1.1), duplicate-append raising (§1.1), the category-read `stored_at`/inclusive change (§1.3), the `stored_at` fabrication fix (§1.6), the empty-batch `ValueError`, `current_position()` returning `None` on an empty store, and the loss of BACKWARD feed reads and feed timestamp filters. A user upgrading past this line needs each of them.

- [ ] **Step 6: final sweep**

```bash
grep -rn "eventsource\.stores\|LegacyStoreAdapter\|StoredEvent\|EventStream\b\|ReadOptions\|DefaultTypeConverter\|EventStoreConformanceSuite\|MemoryEventStore" \
  src/ tests/ bench/ examples/ docs/ CLAUDE.md README.md pyproject.toml | grep -v "docs/superpowers/" | grep -v "InMemoryEventStore"
```
Expected: **zero** matches. This is the acceptance for the whole slice.

```bash
grep -rn "append_events\|get_events_by_type\|get_global_position" docs/ examples/ README.md CLAUDE.md | grep -v "docs/superpowers/"
```
Expected: zero matches.

```bash
git status --porcelain src/eventsource/migrations/
```
Expected: exactly one modified file, `additive/checkpoints_position_token.sql`. If any `schemas/` or `templates/` file shows as modified, revert it and report.

Run: `uv run ruff check examples/` — Expected: clean.
Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: PASS (docs changes must not move the surface).
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 7: commit**

```bash
git add docs/ CLAUDE.md CHANGELOG.md examples/ \
        src/eventsource/adapters/README.md \
        src/eventsource/migrations/additive/checkpoints_position_token.sql
git commit -m "docs: adr 0025 and the store retirement documentation sweep"
```

Record in the commit body: the two Decision items added beyond spec §9's enumeration (the binary-search lookup and the frozen BIGINT columns), which ADR Status pointers were added versus already present, the three corrected changelog entries, and any doc page rewritten wholesale rather than patched.

---

## Slice Completion Criteria

The orchestrator runs these; implementers do not.

- [ ] `make check` passes (lint, mypy, import-linter, bandit/pip-audit, full unit suite).
- [ ] Integration suites pass with Docker services up: `uv run pytest tests/integration/ -v`, **including** `tests/integration/e2e/test_full_flow.py::TestOutboxIntegration::test_outbox_enabled_stores_events` — the test skipped since slice (a), which is Task 1's acceptance — and `tests/integration/repositories/test_outbox.py`.
- [ ] The full suite runs at least once in default random order (do not pass `-p no:randomly`). **Spec §11 risk 6:** the memory adapter's real-clock `stored_at` (versus the legacy `occurred_at` fabrication) breaks time-frozen tests in non-obvious places; random order is how those surface.
- [ ] `uv run python -c "import eventsource.stores"` raises `ModuleNotFoundError`.
- [ ] `grep -rn "eventsource\.stores" src/ tests/ bench/ examples/ pyproject.toml` is empty.
- [ ] `grep -rn "\bMemoryEventStore\b" src/ tests/ bench/ examples/ docs/ | grep -v InMemoryEventStore | grep -v docs/superpowers/` is empty.
- [ ] `uv run mkdocs build --strict` (or the project's docs build command — check `Makefile` for a `docs` target) succeeds, catching dead internal links left by Step 3's page rewrites.
- [ ] `docs/adrs/0025-legacy-store-retirement.md` has `Status: Accepted`, a Decision covering all fifteen items, a Consequences section, and an ADR Impact table; and 0016, 0019, 0024 each carry an "Amended by ADR 0025" pointer.
- [ ] Benchmarks still run: `make bench-up && make bench` on at least the memory scenarios, confirming the rename did not orphan a bench adapter. Numbers are not comparable across the retirement (different code path); this is a smoke check.
- [ ] A `git log --stat` review confirms the deletion commit removed ~4,536 lines of `src/eventsource/stores/` and ~5,700 lines of legacy tests, and that no unrelated file rode along.

## Plan Self-Review

- **Spec coverage, slice (d) only.** Every numbered item of spec §6 slice (d) has a task: (1) outbox port → Task 1; (2) `MemoryEventStore` rename → Task 4 Steps 1 and 3; (3) delete `stores/` → Task 4 Step 5; (4) `__init__.py` rewrite → Task 4 Step 4; (5) retire `EventStoreConformanceSuite` → Task 2 (gap check first, per §5.3); (6) contracts + mutmut + docs + ADR → Task 4 Step 6 and Task 5. §4.1's end-state export table is Task 4's Interfaces block and is pinned by `test_public_api.py`; §4.2's dying names are pinned by the same file's `DEAD_NAMES` case; §4.3's `SQLITE_AVAILABLE` guard is Task 4 Step 4, with the shape change spelled out. §4.4's constructor differences drive Task 1 (postgres engine-vs-session_factory), Task 3 Step 4 (sqlite lifecycle), and Task 5 Step 3 (docs). §5.1 and §5.2 are Task 4 Step 5 (module deletion) and Task 3 Step 2 (their tests). §7 slice (d)'s DELETE list is Task 4 Step 5 verbatim; its REWRITE list splits between Task 3 (edge_cases, additional_coverage, timestamp_types) and Task 4 Step 2 (public_api); its outbox line is Task 1. §8's contract edits are Task 4 Step 6 — reduced to two row deletions, because the independence-contract swap and the absence of a mutmut `stores/` entry were both verified already true at HEAD. §9's ADR and docs plan is Task 5. §11 risk 6 is a Completion Criterion. Nothing from slices (a)-(c) appears here.
- **Placeholder scan.** No TODO, TBD, or `???` remains. Every code block is complete or is a bounded instruction over code the implementer is reading anyway. The five places where reality may differ from this plan — the outbox call-site position and `json_dumps` byte-compatibility (Task 1 Step 1), the `postgres_engine` fixture scope (Task 1 Step 4), the eight-case gap verdicts (Task 2 Step 1), the sqlite conftest fixture's property consumers (Task 3 Step 4), and whether slice (b) already amended ADR 0024 (Task 5 Step 2) — each say to read first and what to do if reality differs.
- **Signature consistency.** `InMemoryEventStore` is the post-rename memory adapter everywhere after Task 4 and `MemoryEventStore` everywhere before it; Task 3 uses the pre-rename spelling deliberately and says why. `outbox_enabled: bool = False` is keyword-only and spelled identically in Task 1's Interfaces block and Step 3. `PostgreSQLEventStore(engine, event_registry=None, *, store_id=None, create_schema=False, outbox_enabled=False)` matches the adapter's real constructor at HEAD plus exactly one new parameter. The §4.1 export table matches `ports/__init__.py` and the three adapter `__init__.py` files as they exist at HEAD.
- **Every task leaves the tree importable, type-clean, and lint-imports green.** Tasks 1 and 2 are additive and subtractive-within-a-module respectively; neither moves a binding. Task 3 touches only tests, both surfaces being live. Task 4 is one commit precisely because no partial ordering of the rename, the swap, and the deletion is green — the argument is in the Global Constraints, with the three interlocking constraints stated separately so a reviewer can check each. Task 5 touches no importable code except one SQL comment and one new README. Every task's verify step lists `uv run lint-imports`, and Task 4's is the one that must be run *after* the `pyproject.toml` row deletion, since import-linter fails on a `source_modules` entry naming a module that no longer exists.

## Spec Gaps and Contradictions Found (report, do not silently deviate)

1. **§8's contract diff is already applied.** The spec presents the independence-contract swap from `eventsource.stores.*` to `eventsource.adapters.*` as slice (d) work ("latest with (d)"); at HEAD `pyproject.toml` already lists the three adapter packages. An earlier slice took it, as §8 permitted. Task 4 Step 6 is therefore reduced to deleting the two Tier-0 `source_modules` rows. Likewise §8's claim that `eventsource.sync.adapter` and `eventsource.testing.sync_facade` should be added as "free hardening" is already done. No deviation — the work is simply smaller than the spec anticipated.
2. **§7 slice (d) lists `tests/unit/test_conformance.py` (123) under DELETE with the note "keep its `EventBusConformanceSuite` half".** Those instructions are in tension: a deleted file has no half. Read as "trim to the bus half", which is what Task 2 Step 3 does. If the bus half turns out to duplicate an existing bus conformance module, the implementer reports rather than deciding to delete.
3. **The spec's §7 slice (d) line counts have drifted, and one file is misfiled.** Verified at HEAD: `test_in_memory_event_store.py` is 1157 (spec: 1157 ✓), `test_event_store_interface.py` 706 ✓, `test_postgresql_event_store.py` 1473 ✓, `test_sqlite_event_store.py` 1214 ✓, `test_read_all_tenant_filter.py` 437 ✓, `test_legacy_adapter.py` 269 ✓, `test_type_converter.py` 333 ✓, `test_eventstore_global_position.py` 299 ✓, `test_conformance.py` 123 ✓, `test_public_api.py` **126** (spec: 125), `test_edge_cases.py` 621 ✓, `test_additional_coverage.py` 495 ✓, `test_timestamp_types.py` 228 ✓. The tracing suites the spec lists as "593/445/538" are `test_memory_tracing.py` 593, `test_sqlite_tracing.py` **445**, `test_postgresql_tracing.py` **538** — the spec's ordering of the last two is transposed against its own `{memory,sqlite,postgresql}` label. Cosmetic; recorded so a reviewer counting lines is not surprised. Also: `src/eventsource/stores/sqlite.py` is **1102** lines, not the 1098 in spec §6's deletion table.
4. **§4.3 does not address that `SQLITE_AVAILABLE`'s current definition is entangled with the outbox import.** The spec says the outbox guard at `__init__.py:225` is "untouched (outbox slice's problem)" and separately that the sqlite store's `try/except` becomes a plain import. But today it is *one* `try/except` covering both, and it is what defines `SQLITE_AVAILABLE`. Untangling it is unavoidable, and Task 4 Step 4 spells out the resulting shape: the flag comes from `adapters.sqlite`, the store import becomes unconditional, and the outbox import keeps a guard of its own. The outbox *repository* is still untouched — only the statement that used to define the flag changes.
5. **Spec §9's ADR 0025 enumeration omits two decisions the slices actually made.** The binary-search nearest-position lookup (slice (c) Task 2, flagged in that plan's own gap list as "deserv[ing] a line in ADR 0025") and the frozen-not-dropped legacy BIGINT position columns are both architecturally significant and neither appears in §9's list. Task 5 Step 1 adds them as items 14 and 15 and instructs the implementer to say so in the commit body.
6. **§3's `testing/conformance.py` paragraph and §5.3 disagree on rigor.** §3 says the ABC suite "is retired in the final slice" flatly; §5.3 conditions the retirement on a gap check for two named cases (`test_event_metadata_preserved`, `test_append_and_get_roundtrip`). Task 2 follows §5.3 and widens it to all eight cases, because a two-case check over an eight-case suite is not a gap check. The plan author's own reading (recorded in Task 2 Step 1's table) finds two-and-a-half likely gaps, including `test_stream_isolation`, which §5.3 does not name at all — so the narrow check would have missed one.
7. **Neither the spec nor any slice plan assigns the two stale adapter module docstrings.** `adapters/postgresql/store.py:3-4` and `adapters/sqlite/store.py:3` both assert that the legacy module "remains" / is "untouched" — statements that Task 4 makes false in the same commit that deletes their subject. Folded into Task 4 Step 7 alongside the two carry-forward one-liners in `migration/router.py` and `migration/dual_write.py`.
