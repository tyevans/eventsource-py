# ADR-0064. The Telemetry Attribute Catalogue Is Not a Wishlist

`observability/attributes.py` declares the span-attribute keys this library
puts on spans. A group of those constants had no write site anywhere in the
tree: nothing set them, no adapter emitted them, no test observed them. They
are deleted. The lock family is the exception — the PostgreSQL lock manager was
already emitting those attributes under hand-written string literals, so the
literals are replaced by the declared constants and the family stays.

## Status

**Accepted.**

**Date:** 2026-08-09

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0016](0016-optional-tracing-no-op-by-default.md) | Amended. 0016 §9 decided the naming rule — OTel semantic conventions for `db.*`/`messaging.*`, `eventsource.` prefix for everything else — and stated that "the constant list in `attributes.py` doubles as the catalogue of what this library will put on a span." The naming rule stands unchanged. The catalogue claim is the part this ADR repairs: it was aspirational, not descriptive, and 0016's own text cites keys that were never emitted. Nothing about the `telemetry` extra, `OTEL_AVAILABLE`, `NullTracer`, or the `Tracer` protocol is disturbed. |
| [0040](0040-out-of-ring-settlement.md) | Stands. `observability/` remains a settled out-of-ring package; removing constants from it does not move it. |

## Context

The 2026-08-09 backpressure audit surfaced a class of defect this project keeps
repeating: a capability the library declares, documents, and is supposed to
dispatch to, and never does. The criterion that separates it from ordinary
public API is **who is contractually obligated to invoke this — the user, or
us?** `EventStore.append` has no internal caller because users call it. A span
attribute is the opposite: a user never sets one. If the library does not emit
the key, nobody does.

That makes the attribute catalogue an unusually clean case. Every constant here
is either emitted by our own code or emitted by nobody, and the second kind is
worse than absent. It reads to a user as a contract — a key they can build a
dashboard query or an alert on — and the query returns nothing, forever, with
no error to explain why. 0016 §9 made this explicit in the direction that
turned out to be false: the catalogue *would* tell you what the library puts on
a span. It did not.

Verification inverted one of the audit's assumptions. The lock family was
assumed inert on the same evidence as the rest — no reference outside the
declaration and the package re-export. In fact the PostgreSQL lock manager
opened acquire and release spans and set the key, id, and timeout on them, with
literals (`"lock.key"`) that differed from the declared constants
(`"eventsource.lock.key"`). Two declaration sites for one fact, disagreeing,
with nothing failing — and the grep for the constant name could not see it,
because the emitting site never used the name. So the lock finding was not
"declared but never dispatched" at all; it was the naming rule of 0016 §9 being
violated at the only site that mattered, in a way that also broke the prefix
guarantee the rule exists to provide.

## Decision

**A telemetry attribute constant exists only if the library emits it.** The
catalogue in `attributes.py` is descriptive of current behavior, not a reserved
namespace for behavior someone might add.

Constants with no write site are deleted rather than implemented. Deleting is
the default because the alternative — inventing a plausible emission site to
justify a key nobody asked for — adds unowned surface and untested code to
satisfy a declaration that was itself the mistake. A future need for a key is
cheap: add the constant in the same change that sets it.

Where a write site already existed under a string literal, the literal is
replaced by the constant and the constant survives. This is the second half of
0016 §9's rule ("constants rather than string literals") applied where it had
lapsed, and it restores the `eventsource.` prefix on those keys.

Attribute keys are a **public telemetry schema**. Their removal is a breaking
change for anyone importing the names, and the emitted key strings change where
literals were corrected. Pre-1.0 the project takes such removals without a
deprecation shim, so both land directly and are recorded in the changelog.

## Consequences

Positive:

- The catalogue means what 0016 said it means. Importing a constant is now
  evidence that something emits it.
- The lock spans carry prefixed, collision-safe keys consistent with every
  other `eventsource.*` attribute, and a test drives the real `acquire()` and
  `release()` paths and asserts the attributes reached the exported spans —
  the gate in `.claude/rules/definition-of-done.md` for a mechanism the library
  is obligated to invoke.
- The failure mode that produced this ADR is now visible: a constant with no
  emitter is a one-line diff away from being noticed, because the emitters no
  longer hide behind literals.

Negative, accepted:

- Users importing a deleted constant get an `ImportError` with no deprecation
  window. That is the standing pre-1.0 policy and the cost is bounded: a name
  that was never emitted cannot appear in a working dashboard.
- Dashboards keyed on the old literal lock attributes stop matching. The
  decision accepts this rather than emitting both spellings, which would
  reinstate the two-declaration-sites defect it exists to remove.
- Only the PostgreSQL lock manager emits lock spans; the in-memory lock manager
  emits none. The decision accepts that asymmetry — tracing is per-adapter
  under 0016, not a port obligation, and the in-memory manager is a test and
  single-process double where a distributed-lock span has no consumer.

## Alternatives Rejected

**Implement every declared attribute.** This is the shape that produced the
problem. Each key would need an emission site chosen to fit the name rather
than a need, plus a test asserting a value nobody consumes. The catalogue would
stay honest by construction and be uniformly useless.

**Keep them, documented as reserved.** A reserved namespace is a promise with
no delivery date, and the audit trail shows this project does not come back for
them. It also does not fix the user-facing failure: a documented-but-unemitted
key still returns an empty query.

**Emit both the old literal and the prefixed constant on lock spans.** Doubles
every lock span's attribute count permanently to protect dashboards that this
library never advertised, and re-creates exactly the divergence — one fact,
two spellings — that the change removes.
