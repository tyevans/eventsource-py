# 0040. Out-of-Ring Settlement: `observability/` and `testing/`

Every top-level package under `src/eventsource/` has now either dissolved
into `domain`/`application`/`ports`/`adapters` (ADRs 0024–0034, 0038, 0039)
or remains as one of exactly two packages that were never candidates for
the ring map to begin with. This ADR records that `observability/` and
`testing/` are *settled* out-of-ring — not transitional, not overlooked,
not next in line for a future dissolution — and adds the `import-linter`
contracts that enforce it.

## Status

**Accepted.** Implemented in `5c40cdd` (two new forbidden contracts) ahead
of this ADR, following the same order every prior ADR in this campaign has
used when a mechanical change was ready before its record was written.

## Decision

`observability/` (`attributes.py`, `tracer.py`, `tracing.py`) is the
cross-cutting telemetry toolkit: an optional, guarded OpenTelemetry
integration (`OTEL_AVAILABLE` probe, `NullTracer` no-op fallback per ADR
0016) consumed by `application/` and `adapters/` wherever a span or a
metric is recorded. It is cross-cutting by nature — every ring except the
two innermost reports telemetry — which is precisely why it cannot itself
be *inside* a ring: a `domain/` or `ports/` module importing it would
invert the Dependency Rule (an entity or a port contract would depend on
an infrastructure concern). The two innermost rings must stay free of it;
`application/` and `adapters/` may use it freely.

`testing/` (`assertions.py`, `bdd.py`, `builder.py`, `conformance.py`,
`conformance_ports/`, `harness.py`, `partitioned_memory.py`,
`recording.py`, `sync_facade.py`) is the public test toolkit: conformance
suites every adapter (in-tree or third-party) runs against, plus builders,
BDD helpers, and a synchronous facade for test code that cannot use
`asyncio`. It imports adapters by design — the conformance suites exist
specifically to exercise concrete adapter implementations — so it must sit
outside the ring map entirely rather than inside `application/` or
`adapters/` itself; a testing utility that both depends on adapters and is
depended on by application code would create the exact adapters-depend-on
in a direction application must not.

Two new `import-linter` forbidden contracts enforce both boundaries:

```toml
[[tool.importlinter.contracts]]
name = "Domain and ports must not import observability"
type = "forbidden"
source_modules = ["eventsource.domain", "eventsource.ports"]
forbidden_modules = ["eventsource.observability"]

[[tool.importlinter.contracts]]
name = "Rings must not import the testing toolkit"
type = "forbidden"
source_modules = [
    "eventsource.domain",
    "eventsource.ports",
    "eventsource.application",
    "eventsource.adapters",
]
forbidden_modules = ["eventsource.testing"]
```

Both were true today before the contracts existed — this ADR does not fix
a violation, it closes a gap where nothing enforced a property that was
already correct, the same as ADR 0034's `layers` contract did for the
domain-ring direction.

## Context: Campaign Completion

With ADR 0038 (multi-tenancy) and ADR 0039 (schema DDL) dissolved and this
ADR settling the remaining two packages, `src/eventsource/` now contains
exactly:

```
__init__.py
domain/
application/
ports/
adapters/
observability/   -- settled out-of-ring by this ADR
testing/         -- settled out-of-ring by this ADR
```

confirmed by `ls src/eventsource/`. This is the end state the standing
directive (memory: `standing-order-finish-architecture`) named as
"achieved": every top-level package is either ringed or explicitly settled
as out-of-ring with documented rationale and contract coverage. No further
top-level package dissolution work remains from this campaign.

## Telemetry Naming Is a Stable Public Schema

A related, previously open question is closed here rather than left to
drift: logger names, meter names, and OTel attribute-string constants
(`"eventsource.bus.*"` from the bus-ring split, `"eventsource.migration.*"`
in `observability/attributes.py` from the migration-ring move) are a
**stable public telemetry schema, deliberately decoupled from Python
import paths**. Every ring-migration ADR in this campaign that touched a
module emitting one of these strings (0031 for the bus, 0034 for
migration) chose not to rename the string when the module moved — the
precedent was applied consistently but never stated as a general rule
until now: a telemetry identifier is an operator-facing contract (dashboards,
alerts, and log queries key on it), not an implementation detail that
should churn every time a module changes address internally.

**On amending ADR 0031's Status line:** checked and decided *not to add
an "Amended by 0040" pointer*. ADR 0031's body documents the
Kafka collaborator move and states its own precedent inline (moving
`bus/kafka.py` to `adapters/kafka/` without touching telemetry strings),
but the *body itself never records an open question* about whether that
precedent generalizes — the "still pinned to `eventsource.bus.*`, flagged
as an open question" language lives in the PR #90 description, not in the
ADR text. The `.claude/rules/definition-of-done.md` convention is to amend
a prior ADR's Status line only when *that ADR's own Decision* is affected;
ADR 0031's Decision (move the Kafka collaborators, keep the telemetry
strings unchanged) stands exactly as written and is not superseded,
narrowed, or reversed by this ADR — this ADR only makes explicit, as its
own new Decision, a naming principle that ADR 0031 already practiced
without stating generally. Amending 0031 would misrepresent what changed:
nothing in that ADR's recorded decision needs revisiting; a *later*,
separate decision (this one) simply names the pattern for the first time.

## Consequences

### Positive

- The campaign-completion claim is now backed by both a directory listing
  and two enforced `import-linter` contracts, not just convention.
- `observability/` and `testing/` get the same "settled, not transitional"
  treatment `.claude/rules/architecture.md` already gives every ringed
  package, closing the last piece of stale "during transition" language
  that document carried.
- The telemetry-naming principle is now written down once, generally,
  instead of being re-derived ADR-by-ADR every time a module housing a
  logger or meter name moves.

### Negative

- None identified: both contracts assert properties that were already
  true; no code changed to satisfy them.

## Alternatives Considered

**Dissolve `observability/` into `adapters/_observability/` as an
adapters-internal shared package, the same pattern as `adapters/_sql/` and
`adapters/_bus/`.** Rejected: those two internal packages are consumed
exclusively by sibling adapters within the same ring. `observability/` is
consumed by `application/` as much as by `adapters/` (application-ring use
cases record spans and metrics directly, not only through an adapter
boundary) — folding it under `adapters/` would either violate the layers
contract the moment `application/` imported it, or require duplicating the
toolkit per-ring, both worse than settling it out-of-ring where it already
sat correctly.

**Dissolve `testing/` into `adapters/testing/` since it imports
adapters.** Rejected for the same underlying reason as the schema-DDL
question in ADR 0039 answered the opposite way: `testing/` is not itself
an adapter (it implements no port), it is a toolkit *for exercising*
adapters and other rings, symmetric to how a test framework sits beside
the code it tests rather than inside it.

## References

- `src/eventsource/observability/`, `src/eventsource/testing/`
- `pyproject.toml`'s two new `[tool.importlinter.contracts]` entries
- [ADR 0031](0031-bus-ring-split.md) — the Kafka logger/meter-name
  precedent this ADR generalizes into an explicit rule; not amended, see
  reasoning above
- [ADR 0034](0034-migration-ring-and-layers-contract.md) — the same
  telemetry-string-stays-unchanged precedent for the migration meter name;
  not amended for the same reason
- [ADR 0016](0016-optional-tracing-no-op-by-default.md) — the
  `NullTracer`/`OTEL_AVAILABLE` guarded-optional design `observability/`
  still follows
- Memory: `standing-order-finish-architecture` — the directive this ADR's
  campaign-completion claim satisfies

## Related

- `.claude/rules/architecture.md` — records the out-of-ring settlement for
  `observability/` and `testing/` alongside the ring map
</content>
