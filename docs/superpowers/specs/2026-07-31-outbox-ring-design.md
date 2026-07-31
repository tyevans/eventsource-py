# Outbox Ring Migration — Design

**Status:** Design complete, ready for planning
**Plan:** `docs/superpowers/plans/2026-07-31-outbox-ring.md`
**Predecessor:** `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md` (slice (d) deferred the outbox ring explicitly, §10)
**Source of scope:** slice-(d) final review residue list item 1; `docs/core-surface.md` "Recommended pre-extraction cleanup" steps 3, 4, and 5; `BACKLOG.md` "Migrate outbox repository to ports/adapters (P2)"

## 1. Problem

`src/eventsource/repositories/outbox.py` is 1,080 lines that mix four things a Clean
Architecture boundary keeps apart:

| Lines | What | Ring it belongs to |
|---|---|---|
| `:41-92` | `OutboxEntry`, `OutboxStats` — value objects | ports (pure) |
| `:95-195` | `OutboxRepository` — `@runtime_checkable` Protocol | ports (pure) |
| `:198-494` | `PostgreSQLOutboxRepository` — sqlalchemy `text()` + `AsyncConnection`/`AsyncEngine` | adapters/postgresql |
| `:497-751` | `InMemoryOutboxRepository` — dict + `asyncio.Lock` | adapters/memory |
| `:754-1076` | `SQLiteOutboxRepository` — raw `aiosqlite.Connection` | adapters/sqlite |
| `:1080` | `OutboxRepositoryProtocol = OutboxRepository` — a self-alias | nowhere |

The module's import block (`:21-22`, `:34`) pulls `sqlalchemy` and
`repositories/_connection.py` at module scope, so importing the *Protocol* requires
sqlalchemy. This is boundary rule 5 in `docs/core-surface.md`: a file defining both a
Protocol and a sqlalchemy-backed implementation must be split before the interface can
move to Tier 0. ADR 0024 already did exactly this for the checkpoint and DLQ
repositories; outbox is the third of three modules that were one shape and should have
been one refactor.

Three consequences follow from leaving it:

1. **`repositories/__init__.py` re-imports sqlalchemy for anyone who touches the
   package**, because it eagerly names `PostgreSQLOutboxRepository` and
   `SQLiteOutboxRepository` (`:29-37`). The top-level `eventsource/__init__.py` imports
   from it at `:191`, so the front-door cost is paid on `import eventsource`.
2. **Two SQL connection helpers coexist.** `repositories/_connection.py`
   (`execute_with_connection(conn, transactional=)`) and `adapters/_sql/connection.py`
   (`sql_connection(conn, *, write=)`) are byte-for-byte the same control flow with
   different parameter spellings. `adapters/_sql/connection.py`'s own module docstring
   says the two "merge when the outbox slice removes its last non-adapter caller" — this
   is that slice.
3. **`repositories/` cannot disappear** while it hosts the outbox, and the package is
   otherwise empty: after `outbox.py` and `_connection.py` leave, only `__init__.py`
   remains.

## 2. Decision

### 2.1 The split

| New module | Contents | Purity |
|---|---|---|
| `src/eventsource/ports/outbox.py` | `OutboxEntry`, `OutboxStats`, `OutboxRepository` Protocol, `outbox_event_data()` | stdlib + `eventsource.events.base` only — Tier 0 |
| `src/eventsource/adapters/memory/outbox.py` | `InMemoryOutboxRepository` (+ `clear()`) | stdlib only — Tier 0 |
| `src/eventsource/adapters/postgresql/outbox.py` | `PostgreSQLOutboxRepository` | sqlalchemy |
| `src/eventsource/adapters/sqlite/outbox.py` | `SQLiteOutboxRepository` | aiosqlite (guarded) |

`src/eventsource/repositories/` is **deleted entirely** — `outbox.py`, `_connection.py`,
and `__init__.py`.

### 2.2 Why outbox splits per-backend and not dialect-parameterized

Checkpoint and DLQ landed in ADR 0024 as a single dialect-parameterized
`adapters/sql/{checkpoints,dlq}.py` serving both PostgreSQL and SQLite. Outbox does not
follow that shape, and this is deliberate rather than an oversight to be corrected later:

- `PostgreSQLOutboxRepository` takes a sqlalchemy `AsyncConnection | AsyncEngine` and
  issues `text()` queries.
- `SQLiteOutboxRepository` takes a raw `aiosqlite.Connection` and issues positional-`?`
  SQL directly, committing itself.

These are different drivers, not two dialects behind one driver. Unifying them would
mean rewriting the SQLite outbox onto sqlalchemy — a behavior change to a working
adapter, with its own transaction-ownership question, and no caller asking for it. The
split follows the connection type, which is what `adapters/<technology>/` means.

The consequence to accept honestly: `adapters/sql/` (dialect-parameterized) and
`adapters/{postgresql,sqlite}/` (driver-specific) now both exist as legitimate homes, and
which one a new repository belongs in is decided by "does it take a sqlalchemy connection
for both backends?", not by the entity it stores. That rule is recorded in ADR 0026.

### 2.3 The connection-helper consolidation

**`adapters/_sql/connection.py::sql_connection` survives. `repositories/_connection.py`
is deleted. All five non-outbox consumers retarget in this slice.**

Consumers of `execute_with_connection` today, all module-level imports:

| Consumer | Call sites |
|---|---|
| `readmodels/postgresql.py:29` | 16 |
| `migration/repositories/position_mapping.py:70` | 11 |
| `migration/repositories/migration.py:67` | 10 |
| `migration/repositories/routing.py:72` | 7 |
| `migration/repositories/audit_log.py:64` | 5 |
| `repositories/outbox.py:34` | 7 (die with the module) |

Rationale for the direction of the merge:

- **No shims are permitted** (unreleased software; standing campaign rule), so moving the
  helper and re-exporting from the old path is not available. Either the five consumers
  retarget, or the helper stays in a package this slice is deleting. Retargeting is the
  only shim-free option.
- **`sql_connection` is the better of the two signatures.** `write` is keyword-only and
  has no default, so every call site states its transaction intent; `transactional=True`
  is a defaulted positional-capable flag, and four of the outbox's seven call sites relied
  on the default meaning "write". The mapping is exactly `transactional=X` → `write=X`;
  the control flow is identical, so this is a rename, not a semantic change.
- **`adapters/_sql/` is where the ring map puts shared SQL infrastructure**, and it
  already has two consumers (`adapters/sql/checkpoints.py`, `adapters/sql/dlq.py`, 15 call
  sites between them). `repositories/` has no successor to host it.

Rejected: moving `repositories/_connection.py` to `adapters/_sql/legacy_connection.py`
and keeping both helpers. That preserves the duplication the backlog item exists to kill
and leaves the next reader choosing between two identical functions.

**One wart this creates, recorded rather than fixed here.** `migration/` is nominally the
use-case ring (`.claude/rules/architecture.md`), and after this change four of its
repository modules import `eventsource.adapters._sql.connection` by name. That is an
inward-ring module naming an adapter module. Two facts make it tolerable and one makes it
temporary: the import-linter "Application ring must not import adapters" contract scopes
`source_modules` to `eventsource.application` only, so no contract is violated; all four
modules already import sqlalchemy directly and are non-Tier-0 backend code by every test
in `docs/core-surface.md`; and their real destination is `adapters/`, in a later migration
slice that this one does not open. The alternative — leaving them on a duplicate helper
until that slice — costs more than it protects. ADR 0026 records the debt.

### 2.4 The shared payload contract

The outbox `event_data` payload dict is currently written out as a literal in **four**
places: `repositories/outbox.py:258-265` (PostgreSQL), `:551-558` (memory), `:831-838`
(SQLite), and `adapters/postgresql/store.py:377-384` (the same-transaction write path
ported in slice (d) Task 1). All four produce the identical six-key shape:

```python
{
    "event_id": str, "aggregate_id": str, "aggregate_type": str,
    "tenant_id": str | None, "occurred_at": str (ISO 8601),
    "payload": dict,  # event.model_dump(mode="json")
}
```

They are a single contract between the store's *writer* and the repository's *reader*,
and nothing in the tree enforces that they stay in step. This slice pins it in one place:

```python
# ports/outbox.py
def outbox_event_data(event: DomainEvent) -> dict[str, Any]: ...
```

Pure, stdlib + `DomainEvent`, returning a JSON-safe dict. All four writers call it. Each
caller still owns its own serialization, because they legitimately differ (see §3.3).

The **table contract** — the name `event_outbox` and its column set — is not expressible
in a pure port and stays in SQL. It is pinned by pointing at one authority:
`src/eventsource/migrations/templates/outbox.sql` (PostgreSQL) and
`templates/sqlite/outbox.sql` (SQLite) are the shipped schemas; the module docstrings of
`adapters/postgresql/outbox.py` and `adapters/sqlite/outbox.py` name them, and
`adapters/postgresql/store.py::_write_to_outbox` gains a pointer to
`adapters/postgresql/outbox.py` as the reader of the rows it writes. No SQL changes.

### 2.5 Names that die

Per house rules on aliases:

| Name | Why it dies |
|---|---|
| `OutboxRepositoryProtocol` | Self-alias (`OutboxRepositoryProtocol = OutboxRepository`, `outbox.py:1080`). A `*Protocol` suffix alias for a Protocol is noise; the campaign has removed the equivalent on every other port. |
| `OutboxRepository.list_pending_events` (+ its 3 implementations) | Documented as "an alias for `get_pending_events()`". Two names for one method on a Protocol means every third-party implementer writes both. `get_pending_events` survives as the single spelling. |
| `eventsource.repositories` (package) | Empty after the move. |
| `EventSourceJSONEncoder`/`json_dumps`/`json_loads` re-exported from `eventsource.repositories` | Die with the package; `eventsource.serialization` is their home and is unchanged. |

`InMemoryOutboxRepository.clear()` **survives** and stays off the Protocol, matching
`InMemoryDLQRepository.clear()`.

### 2.6 Public API

Top-level `eventsource` keeps every outbox name it exports today except the two dying
above, rebound to the new paths:

| Name | Old source | New source |
|---|---|---|
| `OutboxRepository`, `OutboxEntry`, `OutboxStats` | `eventsource.repositories` | `eventsource.ports` |
| `InMemoryOutboxRepository` | `eventsource.repositories` | `eventsource.adapters.memory` |
| `PostgreSQLOutboxRepository` | `eventsource.repositories` | `eventsource.adapters.postgresql` |
| `SQLiteOutboxRepository` (guarded) | `eventsource.repositories.outbox` | `eventsource.adapters.sqlite` |
| `outbox_event_data` | — (new) | `eventsource.ports` |

The `if SQLITE_AVAILABLE:` guard at `__init__.py:382-385` keeps its shape. Its
`try/except ImportError` at `:205-208` changes target only. Slice (d) already untangled
`SQLITE_AVAILABLE`'s *definition* from this import (it now comes from
`adapters.sqlite`), so this is a one-line retarget, not a restructure.

### 2.7 Contracts

- `pyproject.toml`, Tier-0 forbidden contract: add `"eventsource.adapters.memory.outbox"`.
  `"eventsource.ports"` is already listed and covers `ports/outbox.py`.
- `pyproject.toml`, "Application ring must not import adapters": remove
  `"eventsource.repositories"` from `forbidden_modules` — import-linter resolves the
  named module and the package will not exist.
- `[tool.mutmut]` needs **no** edit: `only_mutate` already lists `src/eventsource/ports`
  and `src/eventsource/adapters` wholesale, and
  `pytest_add_cli_args_test_selection` already lists `tests/unit/ports/` and
  `tests/unit/adapters/`. This holds **only if** the new tests land in those two
  directories, which the plan requires.

## 3. Behavior deltas

Spec decisions, not implementation latitude. Each is a user-visible change and belongs in
the CHANGELOG.

| # | Delta | Before | After |
|---|---|---|---|
| 1 | `list_pending_events` | present on the Protocol and all three backends | removed; call `get_pending_events` |
| 2 | `OutboxRepositoryProtocol` | importable alias | removed |
| 3 | `eventsource.repositories` | importable package re-exporting outbox + JSON helpers | `ModuleNotFoundError` |
| 4 | `InMemoryOutboxRepository.event_data` serialization | orjson (`json_dumps`), compact separators `{"a":1}` | stdlib `json.dumps`, `{"a": 1}` |
| 5 | `execute_with_connection` | importable from `eventsource.repositories._connection` | removed; `sql_connection(conn, *, write=)` in `eventsource.adapters._sql.connection` |

**On delta 4.** The in-memory adapter is the only one of the three using orjson; both SQL
adapters use stdlib `json.dumps` with `EventSourceJSONEncoder`. Once `outbox_event_data()`
returns an already-JSON-safe dict, the encoder is unnecessary and the in-memory adapter
can drop its `eventsource.serialization` import entirely — leaving
`eventsource.observability` (optional-dep-guarded, no-op by default per ADR 0016) and
`eventsource.ports.outbox` as its only non-stdlib imports. That is one caveat better than
`adapters/memory/dlq.py`, whose orjson dependency `docs/core-surface.md` boundary
finding 2 still carries as open. The cost is that `event_data` strings differ by
whitespace. `event_data` is
documented as "JSON string or dict" and every consumer parses it; only a test asserting
an exact string is affected, and such a test was asserting orjson's formatting, not the
contract.

**Non-deltas, stated so a reviewer does not go looking.** Query text, column sets, table
names, transaction boundaries, tracing span names and attributes, retry/cleanup semantics,
and the `event_data` payload *shape* are all unchanged. The SQLite adapter's
midnight-truncation-free cleanup and the PostgreSQL adapter's `NOW() - INTERVAL` cleanup
keep their existing (differing) behavior; the conformance suite is written to the
intersection they share, with the divergence tested per-backend (this is exactly the
pattern `conformance_ports/dlq.py`'s module docstring already establishes for DLQ
cleanup).

## 4. Testing

### 4.1 Conformance

New `src/eventsource/testing/conformance_ports/outbox.py` exporting
`OutboxRepositoryConformance`, matching the established shape: an ABC with an abstract
`store` pytest fixture, importing only from `eventsource.ports`, `eventsource.events`, and
pytest/stdlib. Wired to all three backends:

| Backend | Runner |
|---|---|
| memory | `tests/unit/adapters/test_memory_outbox_conformance.py` (new) |
| sqlite | `tests/unit/adapters/test_sqlite_conformance.py` (existing module, one more base class + fixture) |
| postgresql | `tests/integration/repositories/test_outbox.py` (existing module) |

Cases the suite must carry, derived from the Protocol's seven surviving methods:
add-then-read round-trip of every `OutboxEntry` field; pending ordering oldest-first;
`limit` honored; `mark_published` removes an entry from pending and stamps
`published_at`; `mark_failed` sets status and `last_error`; `increment_retry` increments
and records the error without changing status; `cleanup_published` deletes only published
entries and returns the count; `get_stats` counts each status and reports
`oldest_pending`/`avg_retries`. Backend-divergent cleanup cutoffs stay in the per-backend
modules.

### 4.2 Property tests

Two hypothesis suites in `tests/unit/adapters/test_memory_outbox_properties.py`, matching
`test_memory_dlq_properties.py` for style. Warranted because both semantics are stateful
counters over a set:

- **Retry monotonicity.** For any sequence of `increment_retry` calls interleaved with
  `mark_failed`, an entry's `retry_count` equals the number of `increment_retry` calls
  applied to it, and `last_error` is the most recent non-`None` error argument.
- **Cleanup partition.** For any set of entries in mixed statuses and any `days >= 0`,
  `cleanup_published` returns exactly the count it removed, removes only entries whose
  status is `published`, and leaves `get_stats().pending_count + failed_count` unchanged.

Not warranted, and not to be written: property tests over the SQL adapters (they need a
live connection; the conformance suite is the right instrument) or over `OutboxEntry`
construction (a plain dataclass).

### 4.3 Existing test modules

| Module | Disposition |
|---|---|
| `tests/unit/test_outbox_repository.py` (895) | Split by backend into `tests/unit/adapters/`; imports retarget; `list_pending_events`/`OutboxRepositoryProtocol` cases die |
| `tests/repositories/test_sqlite_repos.py` (947) | Its outbox portion (`:262` onward) moves to `tests/unit/adapters/`; the `SQLCheckpointRepository` half (`:60-261`) stays put |
| `tests/integration/repositories/test_outbox.py` (478) | Imports retarget; gains the conformance base class |
| `tests/unit/test_connection_helper.py` (316) | Dies with `execute_with_connection`; replaced by a new `tests/unit/adapters/test_sql_connection.py` for `sql_connection` |
| `tests/unit/migration/test_{routing,audit_log,migration,position_mapping}_repository.py` | ~90 `mock.patch` dotted strings `...<module>.execute_with_connection` become `...<module>.sql_connection` |
| `tests/conftest.py`, `tests/integration/conftest.py`, `tests/benchmarks/{conftest,test_repositories}.py`, `tests/unit/test_edge_cases.py`, `tests/unit/serialization/test_json.py:652` | Import retargets only |

Placing the new unit tests under `tests/unit/adapters/` and `tests/unit/ports/` is what
keeps the existing mutmut selection valid (§2.7).

## 5. Out of scope

- **Lazy top-level `__init__`.** Deleting `repositories/` shortens one front-door
  sqlalchemy chain but `import eventsource` still pulls sqlalchemy via
  `adapters.postgresql` and `engine`. The PEP 562 lazy front door stays its own backlog
  item.
- **Making sqlalchemy an optional dependency.** This slice removes one of that item's
  blockers; it does not attempt the extras split.
- **Moving `migration/repositories/` and `readmodels/postgresql.py` into `adapters/`.**
  They retarget one import here and nothing else (§2.3).
- **Rewriting `SQLiteOutboxRepository` onto sqlalchemy** to merge it with the PostgreSQL
  implementation (§2.2).
- **Any SQL schema change.** `migrations/` files are untouched; no new fragment.
- **The `SnapshotStore` port redesign** and the store-lifecycle/`close()` port — separate
  backlog items with their own shape.
- **Adding an outbox drain worker.** The library still ships no drain loop; the guide
  documents how to write one, and that stays true.

## 6. ADR Impact

Per `.claude/rules/definition-of-done.md`.

| ADR | Verdict |
|---|---|
| 0015 (optional dependency extras) | **Stands.** `SQLiteOutboxRepository` keeps its guarded import and its conditional `__all__` entry; only the path changes. |
| 0016 (optional tracing, no-op by default) | **Stands.** All span names, attributes, and the `tracer`/`enable_tracing` constructor parameters move verbatim. Note this differs from the store adapters, which ADR 0025 amended 0016 to say carry no spans — the outbox repositories are not stores and keep theirs. |
| 0019 (clean architecture store ports) | **Stands.** Establishes the ports/adapters shape this slice applies to a non-store port. |
| 0024 (projection persistence ports) | **Stands, extended.** 0024 split checkpoint and DLQ; this is the third module of the same shape. Not an amendment — nothing 0024 decided changes. ADR 0026 cites it as precedent and records where outbox deliberately diverges (§2.2, per-backend not dialect-parameterized). |
| 0025 (legacy store retirement) | **Stands.** §10 of its spec deferred the outbox ring to a later slice; this is that slice, and it does not revisit 0025's decisions. Its item 13 (outbox write ported onto the PostgreSQL adapter) is the writer whose reader this slice re-homes. |
| **0026 (new)** | Records: the outbox port split; the per-backend rather than dialect-parameterized adapter shape and the rule for choosing between `adapters/sql/` and `adapters/<technology>/`; the single-helper connection consolidation and the `migration/`→`adapters/_sql` import debt it accepts; the death of `OutboxRepositoryProtocol` and `list_pending_events`; the deletion of the `repositories/` package; and `outbox_event_data()` as the one authority for the writer/reader payload contract. |

## 7. Risks

1. **The ~90 mock-patch dotted strings** in the migration test modules are the largest
   mechanical surface and the easiest place for a silent miss: a stale patch target
   raises `AttributeError` at test time, so a missed one fails loudly rather than passing
   vacuously. Verified by a grep for zero remaining `execute_with_connection` occurrences.
2. **`tests/repositories/test_sqlite_repos.py` is not outbox-only.** Moving the whole file
   would drag unrelated coverage; the plan requires reading it and moving only the outbox
   classes.
3. **Delta 4 (orjson → stdlib json in the memory adapter)** can break an equality
   assertion on `event_data` anywhere in the suite, including modules not otherwise
   touched. Grep for `event_data` assertions, do not rely on the retargeted modules alone.
4. **import-linter fails on `forbidden_modules` naming a nonexistent module**, so the
   `pyproject.toml` edit must land in the same commit as the package deletion.
5. **`tests/unit/repositories/` still exists** (it holds `test_dialect.py`, referenced by
   the mutmut selection) and has nothing to do with `src/eventsource/repositories/`. Do
   not delete it because the source package went away.
