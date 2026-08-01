# 0039. Schema DDL Package Relocated to Adapters

`eventsource.migrations` (plural — the schema-DDL package, distinct from
the now-dissolved `eventsource.migration` singular orchestration package;
see ADR 0034) was the last top-level package outside the ring map. This
ADR relocates it wholesale to `eventsource.adapters.sql.schemas`, ending
the standing `migration`/`migrations` name-confusion hazard the campaign
has been navigating around for two waves.

## Status

**Accepted.** Implemented in `52f34ee`. `import eventsource.migrations`
raises `ModuleNotFoundError`, no shim, no transition window.

## Decision

The package is the storage format itself — SQL schema files, Alembic
templates, and append-only migration fragments for the `events`, `outbox`,
`checkpoints`, `dlq`, and `snapshots` tables, for both the PostgreSQL and
SQLite backends. That makes it an adapters-ring citizen by definition, not
an ambiguous cross-cutting concern: it has zero code dependencies (the
`__init__.py`'s `get_schema()`/`get_all_schemas()`/`list_schemas()`
functions are pure `pathlib.Path` file reads, gated only by
`typing.Literal` string types) and exactly two consumers in the entire
codebase, both of them store adapters:
`adapters/postgresql/store.py` and `adapters/sqlite/store.py`, each calling
`get_schema()` to create their tables. The whole tree — `__init__.py`,
`SCHEMA_DESIGN.md`, `additive/`, `schemas/`, `templates/`, `updates/` —
moved as one `git mv`, preserving the subtree structure exactly.

`__init__.py`'s path-resolution logic (`_PACKAGE_DIR = Path(__file__).parent`
and everything derived from it) is entirely relative to the module's own
location, so the move required no logic changes at all — only its
docstring and doctest examples, which named `eventsource.migrations`
directly, were repointed to `eventsource.adapters.sql.schemas`.

## Context

Packaging was the one property that had to be verified, not assumed: the
`.sql`, `.md`, and `.py.template` files are package *data*, shipped inside
the wheel but never imported as Python modules. `[tool.hatch.build.targets.wheel]`
declares `packages = ["src/eventsource"]` and nothing more specific —
hatchling's default wheel builder ships every non-excluded file under a
declared package directory regardless of whether each subdirectory carries
its own `__init__.py` (the `additive/`, `schemas/`, `templates/`, and
`updates/` subdirectories never had one, before or after this move). That
default behavior was already how these files shipped at the old path, so
no `pyproject.toml` build-configuration change was needed — but the
default was verified rather than trusted: `uv build` was run twice (once
before, once after auto-fix/reformat touched the moved files), and the
resulting wheel was inspected directly with `unzip -l`. Both times, all 37
files landed at `eventsource/adapters/sql/schemas/...` and zero remained at
`eventsource/migrations/...`.

`lint-imports` confirmed the two new consumer imports
(`adapters/postgresql/store.py`, `adapters/sqlite/store.py` importing
`eventsource.adapters.sql.schemas`) do not trip the "Infrastructure
backends must not import each other" independence contract: that contract
only names `eventsource.adapters.{postgresql,sqlite,memory,redis,kafka,
rabbitmq}` against each other, and `eventsource.adapters.sql` — the shared
adapters-internal package already holding `adapters/sql/checkpoints.py`,
`adapters/sql/dlq.py`, `adapters/sql/projection.py`, and (since ADR 0034)
`adapters/sql/migration/` — sits outside that list by design, the same
`adapters/_sql`/`adapters/sql` shared-internals pattern every prior
adapters-ring move in this campaign has used.

Three provenance comments inside the moved `.sql` files
(`additive/checkpoints_position_token.sql`,
`additive/migration_position_tokens.sql`, `additive/events_txid.sql`) named
`eventsource.migrations.get_schema` in prose describing which function
composes the fragment; these were updated to the new path as a drive-by
fix, since they live inside the moved tree itself.

## Consequences

### Positive

- `eventsource.migrations` joins the ring map; `src/eventsource/` now
  contains only the four rings plus the two ADR-0040-settled packages —
  see that ADR for the campaign-completion claim.
- The `migration`/`migrations` name-confusion hazard this campaign has
  flagged since ADR 0034 (singular orchestration package vs. plural
  schema-DDL package, both real, easy to conflate in a sweep or a grep) is
  gone: the plural package no longer exists as a top-level name at all,
  and its new home (`adapters/sql/schemas`) reads unambiguously as "the
  SQL schema files the SQL adapters use."
- Packaging is now wheel-verified rather than assumed, for both the old
  location (which shipped correctly, per the prior release) and the new
  one.

### Negative

- `import eventsource.migrations` now raises `ModuleNotFoundError` with no
  transition period. Roughly a dozen `tests/`/`bench/` files needed
  retargeting, plus the two real `src/` consumers.
- Every documentation page that told a reader to run
  `from eventsource.migrations import get_schema` (the CLI-facing schema
  setup story) needs a rewrite — tracked as this wave's docs slice, not
  part of this ADR's implementation.

## Alternatives Considered

**Leave `eventsource.migrations` in place as an intentionally-out-of-ring
package, parallel to `observability/` and `testing/` (see ADR 0040).**
Rejected: unlike `observability/` (cross-cutting telemetry, consumed by
every ring) and `testing/` (a public toolkit consumed by users, not an
implementation detail), `eventsource.migrations` has exactly the shape of
an adapter — technology-specific storage format, two concrete consumers,
zero cross-ring fan-out. There was no principled argument for settling it
out-of-ring the way there is for the other two; it belonged in the ring
map from the start; it was retired from `eventsource.migrations` only
because the ring-migration campaign had not reached it yet.

## References

- `src/eventsource/adapters/sql/schemas/`
- `src/eventsource/adapters/postgresql/store.py`,
  `src/eventsource/adapters/sqlite/store.py` — the two consumers
- [ADR 0034](0034-migration-ring-and-layers-contract.md) — dissolved the
  singular `eventsource.migration` orchestration package; this ADR
  dissolves its plural, unrelated sibling
- [ADR 0029](0029-locks-readmodels-and-engine-rings.md) — the
  `adapters/sql/`, `adapters/_sql/` shared-adapters-internals precedent

## Related

- `docs/guides/database-schema.md`, `docs/guides/live-migration.md`,
  `docs/guides/repository-operations.md`, `docs/guides/snapshotting.md`,
  `docs/tutorials/{06-projections,11-postgresql,14-snapshotting}.md`,
  `docs/api/{migrations,migration-schema,index}.md` — import paths updated
  for the new module layout
- `.claude/rules/architecture.md` — ring map updated to mark
  `adapters/sql/schemas/` as settled
</content>
