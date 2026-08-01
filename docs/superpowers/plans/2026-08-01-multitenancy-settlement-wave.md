# Plan: wave N+1 — multitenancy dissolution + migrations relocation + out-of-ring settlement

Final wave of the rings campaign (standing directive: continue until the
architecture is achieved). Branch: `multitenancy-settlement-wave`, stacked on
`migration-ring-ports-wave` (PR #97 unmerged at branch time — note base in
PR body). ADR numbers 0038-0040 (no sibling PRs besides our own #97).

Target end-state: every top-level package under `src/eventsource/` is one of
the four rings (`domain/`, `application/`, `ports/`, `adapters/`) or
ADR-settled out-of-ring (`observability/`, `testing/`). Achieving this
completes the campaign.

## 1. Multitenancy dissolution (ADR 0038)

| Module | Destination | Why |
|---|---|---|
| context.py (contextvar machinery, 9 public names) | `domain/tenant_context.py` | Pure stdlib; tenant identity is a domain concern. Enables killing the `importlib` soft-dependency hack in `domain/aggregate.py:480` — replace with a direct domain-internal import (behavior preserved: returns None when unset). |
| events.py (`TenantDomainEvent`) | `domain/tenant_events.py` | Extends `DomainEvent`; entity type. |
| exceptions.py (3 classes, already `EventSourceError`-rooted) | merge into `domain/exceptions.py` | Standing rule; no rebase needed, verify except-sites anyway. |
| repository.py (`TenantAwareRepository`) | `application/aggregates/tenant_repository.py` | Wraps `AggregateRepository`; colocate with it. |

Root `__init__` `_LAZY` entries (~12 names at lines 497-505+) repoint to new
modules — names unchanged, `__all__` byte-identical. Tier 0 source_modules:
replace the four `eventsource.multitenancy.*` entries with
`eventsource.domain.tenant_context` / `domain.tenant_events` (exceptions +
repository covered by existing `domain.exceptions` / `eventsource.application`
entries). Guard test for `eventsource.multitenancy`. Tests:
`tests/unit/multitenancy/` → `tests/unit/domain/` (context/events/exceptions)
+ `tests/unit/application/aggregates/` (repository).

## 2. migrations/ → adapters/sql/schemas/ (ADR 0039)

`eventsource.migrations` is schema DDL — the storage format itself; adapters
ring by definition. Zero code deps; consumed by exactly two adapters
(postgresql/sqlite `store.py` `get_schema`). Move whole package →
`src/eventsource/adapters/sql/schemas/` (subdirs additive/, schemas/,
templates/, updates/, SCHEMA_DESIGN.md intact). Bonus: erases the standing
`migration`/`migrations` name-confusion hazard.

MUST verify packaging: the .sql files are package data — check
`[tool.hatch.build]`/MANIFEST ships them from the new path (build a wheel and
inspect). Tests: `tests/unit/migrations/` → `tests/unit/adapters/sql/schemas/`;
`tests/integration/migrations/` stays, imports repointed. Guard test. Check
`[tool.mutmut]`/pytest lists + importlinter for `eventsource.migrations`
entries.

## 3. Out-of-ring settlement (ADR 0040)

- `observability/`: cross-cutting telemetry toolkit (OTel optional/guarded);
  consumed by adapters+application. Settled OUT of the rings. New forbidden
  contract: `eventsource.domain` and `eventsource.ports` must not import
  `eventsource.observability` (true today; ports mentions are docstrings).
- `testing/`: public test toolkit; imports adapters by design. Settled OUT.
  New forbidden contract: none of the four rings may import
  `eventsource.testing`.
- Telemetry naming: logger/meter/attribute names (`eventsource.bus.*`,
  `eventsource.migration.*`) are a STABLE PUBLIC SCHEMA decoupled from module
  paths — ADR 0040 records this, closing the ADR 0031 open question without
  churn.

## Slices (serial)

1. multitenancy dissolution (agent).
2. migrations relocation + packaging verification (agent).
3. settlement contracts in pyproject (orchestrator).
4. docs/meta (agent): ADRs 0038-0040, index/nav, docs repoints (multitenancy
   guide, schema docs pages docs/api/{migrations,migration-schema}.md),
   CHANGELOG, architecture.md settled entries + fix stale "during transition
   `stores/`" wording (flagged in wave N slice 6), BACKLOG updates, sweeps
   (`sweep.sh multitenancy`, `sweep.sh migrations`). Then orchestrator: full
   gate + PR.

## Campaign-completion check (after this wave)

`ls src/eventsource/` should show exactly: `__init__.py`, `domain/`,
`application/`, `ports/`, `adapters/`, `observability/`, `testing/` — the
last two ADR-settled. If so: architecture achieved; report and stop.
