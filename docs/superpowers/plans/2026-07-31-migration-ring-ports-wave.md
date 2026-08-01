# Plan: migration/ ring move + layers contract + lazy front door + ports redesign

Wave after ADR 0033 (PR #96). Branch: `migration-ring-ports-wave`. One PR,
serial slices. No open sibling PRs at recon time; next ADR number is 0034.

## Scope

1. Dissolve `src/eventsource/migration/` into the rings (skill workflow).
2. Add the full `layers` import-linter contract (campaign-completion item).
3. PEP 562 lazy `eventsource/__init__` (BACKLOG P3).
4. SnapshotStore port → composed Protocols (BACKLOG P2).
5. Store lifecycle port + engine-ownership semantics (BACKLOG P2).

**Not in scope:** `eventsource.migrations` (plural — the schema-DDL package)
stays put. Different package; sweep greps must use `migration\b` boundaries.

## Ring assignment — migration/

Recon facts: `eventsource.migration` is a **leaf** — zero `src/` importers
outside the package. Not exported from `eventsource/__init__` (public root
API unchanged, trivially byte-identical). The OTel attribute strings
`"eventsource.migration.*"` in `observability/attributes.py` are telemetry
schema names, not imports — **keep unchanged** (Kafka logger-name precedent,
ADR 0031).

| Module | Destination | Why |
|---|---|---|
| bulk_copier, consistency, coordinator, cutover, dual_write, metrics, models, position_mapper, router, status_streamer, subscription_migrator, sync_lag_tracker, write_pause, `__init__`, README | `application/migration/` | Use-case orchestration; imports ports + observability + domain only, no drivers. `models.py` imports `ports.positions` → application, not domain. `metrics.py` is pure-stdlib in-process collectors, migration-specific → travels with its use case. |
| exceptions.py | `application/migration/exceptions.py` | NOT merged into domain/exceptions.py: 1531 lines including `ErrorHandler` retry/circuit-breaker machinery and the classification system — behavior, not entity types, and entirely migration-specific. **Do** rebase `MigrationError(Exception)` → `MigrationError(EventSourceError)` (widening-only; verify except-sites first). |
| repositories/{audit_log, migration, position_mapping, routing} | `adapters/sql/migration/` (same basenames) | sqlalchemy + `adapters._sql.connection`. Nobody in migration/ imports them (standalone wiring surface) — clean adapters-ring citizens. Keep basenames so test `patch()` strings retarget mechanically. |

Tests: `tests/unit/migration/` → `tests/unit/application/migration/`, except
the four repository tests (`test_audit_log_repository`,
`test_migration_repository`, `test_position_mapping_repository`,
`test_routing_repository`) → `tests/unit/adapters/sql/migration/`.
Integration tests stay in place, imports repointed. Add
`ModuleNotFoundError` guard for `eventsource.migration` in
`test_public_api.py`.

pyproject touchpoints (all three named by the skill):
- importlinter "Ports must not import …" forbidden list: drop the now-dangling
  `eventsource.migration` entry (subsumed by `eventsource.application`).
- mutmut `only_mutate`: delete the two `src/eventsource/migration/*` lines
  (covered by the existing `src/eventsource/application` entry).
- `pytest_add_cli_args_test_selection`: delete the two
  `tests/unit/migration/*` lines (covered by `tests/unit/application/`).

## Slices (serial)

1. **migration/ move** — git mv per table above, retarget imports +
   `patch()` strings, MigrationError rebase, guard test, pyproject edits,
   delete debris dirs (incl. `__pycache__`). Targeted gate.
2. **layers contract** — `type = "layers"`:
   `adapters > application > ports > domain`. Delete the two subsumed
   forbidden contracts ("Application ring must not import adapters", "Ports
   must not import …"), folding their comment rationale into the new
   contract's comment. Keep independence + Tier 0 contracts. New coverage:
   domain must not import ports/application/adapters (spot-checked clean).
3. **lazy front door** — PEP 562 `__getattr__`/`__dir__` in
   `eventsource/__init__.py`; `__all__` byte-identical; `TYPE_CHECKING`
   block for static consumers. Payoff test now possible: runtime
   `import eventsource; assert "sqlalchemy" not in sys.modules`. Note the
   ast-based readmodels port-purity test can stay as-is.
4. **SnapshotStore composed Protocols** — split ABC into save/get(/exists)
   core Protocol + separate optional bulk-invalidation capability port; no
   default bodies, no NotImplementedError; update memory/sqlite/postgresql
   snapshot adapters + conformance suite + snapshotting application code
   (isinstance/capability checks at the delete-by-type call site).
5. **store lifecycle port** — optional close/lifecycle Protocol in ports;
   `SyncStoreFacade.close()` stops `getattr` duck-typing; PostgreSQL adapter
   gets explicit engine-ownership semantics (dispose only engines it
   created; caller-injected engines are caller-owned unless an explicit
   ownership flag says otherwise).
6. **docs/meta + gate + PR** — ADR 0034 (migration ring + layers contract),
   0035 (lazy front door), 0036 (snapshot port composition), 0037 (store
   lifecycle port); index + nav; docs/api/migration*.md + core-surface.md
   repoints; CHANGELOG BREAKING entries; architecture.md rules; BACKLOG
   item removals; sweep.sh; `make check` + integration + validate_examples
   + mkdocs --strict; PR.
