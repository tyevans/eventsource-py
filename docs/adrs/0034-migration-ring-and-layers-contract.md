# 0034. Migration Ring Migration and Full Layers Contract

`eventsource.migration` was the last top-level package outside the
`domain`/`application`/`ports`/`adapters` ring map — the "during transition"
entry `.claude/rules/architecture.md` had carried alongside `application/`
since the ring migration began. This ADR dissolves it, extracts its
persistence Protocols into a new `ports/migration/` package (a design
question the move itself surfaced), and replaces the two targeted
`import-linter` forbidden contracts guarding the ring boundary with a single
full `layers` contract covering all four rings, including `domain` for the
first time.

## Status

**Accepted.** Implemented in two commits: `a3b0e8c` (the ring move plus
`ports/migration/` extraction) and `4ca62ae` (the layers contract). 14
top-level modules (including `__init__.py`) land in
`src/eventsource/application/migration/`; the four SQL repository
implementations land in `src/eventsource/adapters/sql/migration/`;
`models.py` and a new `repositories.py` (the four persistence Protocols cut
out of the adapter modules) land in a new `src/eventsource/ports/migration/`
package. `import eventsource.migration` raises `ModuleNotFoundError`, no
shim, no transition window — the same standing rule ADR 0025, ADR 0026,
ADR 0030, ADR 0031, ADR 0032, and ADR 0033 already applied to every other
pre-ring package.

## Decision Table

| Old module | New home | Ring | Rationale |
|---|---|---|---|
| `migration/{__init__,bulk_copier,consistency,coordinator,cutover,dual_write,exceptions,metrics,position_mapper,router,status_streamer,subscription_migrator,sync_lag_tracker,write_pause}.py`, `migration/README.md` | `application/migration/` | application | Use-case orchestration — imports ports, observability, and domain only, no drivers. |
| `migration/repositories/{audit_log,migration,position_mapping,routing}.py` | `adapters/sql/migration/` | adapters | sqlalchemy-backed implementations; same basenames preserved so `patch()` strings in tests retarget mechanically. |
| `migration/models.py` | `ports/migration/models.py` | ports | Pure pydantic/stdlib + `eventsource.ports.positions` — the same "value objects that cross a port boundary live in ports" rule that already places `EventEnvelope` in `ports/envelopes.py` and `Position` in `ports/positions.py`. |
| Four `Protocol` classes cut from the repository modules (`MigrationRepository`, `TenantRoutingRepository`, `PositionMappingRepository`, `MigrationAuditLogRepository`) | `ports/migration/repositories.py` (new) | ports | See Context — extracted mid-slice once the move surfaced a latent Dependency Rule violation. |
| `MigrationError(Exception)` | `application/migration/exceptions.py`, rebased onto `EventSourceError` | application | Widening-only; verified against every `except MigrationError`/`except EventSourceError` site first — no site depended on the narrower base. |

## Context

The ring-assignment recon that planned this move found `eventsource.migration`
a leaf: zero `src/` importers outside the package, never exported from
`eventsource/__init__.py`. That made the four top-level modules and the SQL
repositories look like a clean, mechanical `git mv` — application-ring use
cases importing adapters-ring repositories directly, the same shape every
prior ring migration in this series has moved verbatim.

It wasn't clean. `coordinator.py`, `cutover.py`, `router.py`,
`position_mapper.py`, and `bulk_copier.py` all imported the repository
classes directly, with no Protocol indirection — a real "application ring
must not import adapters" violation that had existed since these modules
were first written. It was invisible before this move because
`eventsource.migration` sat entirely outside every ring contract; moving it
onto `application`/`adapters` made the pre-existing "Application ring must
not import adapters" `import-linter` contract correctly catch it, and the
commit that performed the move was rejected by the `import-linter`
pre-commit hook. The fix was not to relax the contract or add an exemption
— it was to finish the Protocol/implementation split the original code
should have had. The four repository classes' bodies were already pure
Protocols in shape (`@runtime_checkable class MigrationRepository(Protocol)`
etc., defined *inside* the adapter modules that also held the sqlalchemy
implementation) — cutting them out to `ports/migration/repositories.py` and
retargeting the five application-ring callers' `TYPE_CHECKING` imports
closed the violation without changing any behavior.

`models.py` had to travel to `ports/` alongside the Protocols, not stay in
`application/`, because the Protocol method signatures reference the model
types directly (`async def get(self, migration_id: UUID) -> Migration |
None`) — and ports must not import application. This is the same
data-crosses-the-boundary-as-a-value-object rule that already places
`EventEnvelope` and `Position` in `ports/`, applied to a second boundary
(migration persistence) that had never been drawn out as its own ports
subpackage before. `ports/migration/` follows the `ports/readmodels/`
subpackage precedent (ADR 0029) rather than a flat `ports/migration.py`:
multiple distinct pure artifacts (twelve model types, four Protocols) that
callers import for different reasons.

The four legacy `*RepositoryProtocol = *Repository` aliases at the bottom
of the adapter modules were deleted outright — grepping for consumers found
none outside the adapter modules' own definitions and test class *names*
(e.g. `TestMigrationRepositoryProtocol`, a name, not a reference). No-shims
policy; nothing depended on them.

Two things were deliberately left unchanged. The OTel attribute string
constants in `observability/attributes.py` (`"eventsource.migration.id"`,
etc.) are telemetry schema names, not Python import paths — the same
Kafka-logger-name precedent ADR 0031 already established for not touching
telemetry identifiers during a ring move. `metrics.py`'s
`get_meter("eventsource.migration", version="1.0.0")` is the same kind of
string: a meter name, not an import.

With the migration ring in place, every module under `src/eventsource/`
other than `eventsource.migrations` (plural — the schema-DDL package,
unrelated and untouched by this ADR) now sits inside one of the four rings.
That made it possible to replace the two narrow, targeted `import-linter`
forbidden contracts — "Application ring must not import adapters" and
"Ports must not import adapters, application, or migration" — with a single
`type = "layers"` contract covering `adapters > application > ports >
domain` in one declaration, adding domain-ring coverage (domain may not
import ports, application, or adapters) that neither predecessor contract
carried. The `layers` contract type also catches by-name dependencies the
"Tier 0 modules must not import sqlalchemy" forbidden contract cannot see
— a `ports/readmodels/` module importing `adapters/sql/readmodel_schema.py`
would break layering even though `readmodel_schema.py` itself imports no
sqlalchemy, a gap ADR 0029 left open and this contract closes.

Neither prior contract's introduction is attributed to a specific ADR
decision — grepping every existing ADR for "Application ring must not
import adapters" and "Ports must not import adapters" finds only ADR 0030
mentioning the former in passing (an entry removed from it), never a record
that decided to create either contract. No Status-line amendment is
recorded on any prior ADR for this reason: there is no Decision to amend,
only an implementation detail (the exact `pyproject.toml` contract shape)
that predates the ADR-per-slice convention.

## Consequences

### Positive

- `eventsource.migration` joins every other top-level package this
  multi-ADR campaign (0025, 0026, 0029, 0030, 0031, 0032, 0033) has already
  moved onto the ring map; `.claude/rules/architecture.md`'s "during
  transition `migration/`" wording is retired.
- The latent application-ring-imports-adapters violation in `coordinator.py`,
  `cutover.py`, `router.py`, `position_mapper.py`, and `bulk_copier.py` is
  closed, not merely documented — `import-linter` now enforces it going
  forward for this ring the same way it already did for every other one.
- `ports/migration/` gives the four persistence Protocols and their model
  types a home a future second backend (a non-PostgreSQL migration
  repository, say) could implement without touching `application/migration/`
  at all.
- The layers contract replaces two hand-maintained forbidden-module lists
  with one declarative rule that automatically covers any future module
  added to any of the four rings, and adds domain-ring coverage neither
  predecessor contract had.

### Negative

- `import eventsource.migration` now raises `ModuleNotFoundError` with no
  transition period. Every internal importer (`src/`, `tests/`) needed
  retargeting — 14 application modules, 5 adapter modules, and roughly 45
  test files.
- The four `*RepositoryProtocol` aliases are gone; any code that imported
  them (none found in this repository) must import the Protocol names
  directly from `eventsource.ports.migration.repositories`.
- `SnapshotConformance`-style naming aside, this ADR's mid-slice pivot
  (ports extraction added after the initial move was already staged) means
  the migration landed as two commits rather than one; both are cited above
  for the full picture.

## Alternatives Considered

**Accept the application-ring-imports-adapters violation as recorded debt,
matching how it was undocumented-but-unenforced before the move.** Rejected
by the team lead's explicit ruling: the `import-linter` contract exists
precisely to catch this shape of violation, and the four repository classes
were already pure Protocols in structure — extracting them cost an
afternoon, not a redesign, and left no exception to track.

**Put `models.py` in `application/migration/` and have the ports-ring
Protocols import it with a `TYPE_CHECKING`-only exception.** Rejected: ports
must never import application, full stop, even under `TYPE_CHECKING` —
that is the rule the `import-linter` Tier 0 and layers contracts both
enforce without a "for typing only" carve-out anywhere else in the
codebase.

## References

- `src/eventsource/application/migration/`, `src/eventsource/adapters/sql/migration/`,
  `src/eventsource/ports/migration/`
- `pyproject.toml`'s `[tool.importlinter]` contracts
- [ADR 0029](0029-locks-readmodels-and-engine-rings.md) — the
  `ports/readmodels/` subpackage precedent `ports/migration/` follows
- [ADR 0031](0031-bus-ring-split.md) — the telemetry-name-stays-unchanged
  precedent applied to the OTel attribute strings and meter name here
- [ADR 0025](0025-legacy-store-retirement.md), [ADR 0026](0026-outbox-ring-migration.md),
  [ADR 0030](0030-top-level-module-ring-consolidation.md),
  [ADR 0032](0032-subscriptions-ring-migration.md),
  [ADR 0033](0033-events-handlers-internal-ring-migration.md) — the same
  no-shim, ring-map-completion pattern this ADR applies to the last
  top-level package

## Related

- `docs/api/migration.md`, `docs/api/migration-schema.md` — import paths
  updated for the new module layout
- `.claude/rules/architecture.md` — ring map updated to mark
  `application/migration/`, `adapters/sql/migration/`, and
  `ports/migration/` as settled
