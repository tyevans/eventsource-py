# 0038. Multi-Tenancy Ring Dissolution

`eventsource.multitenancy` was one of the last top-level packages outside
the `domain`/`application`/`ports`/`adapters` ring map. This ADR dissolves
it: the `ContextVar` machinery and the tenant-scoped event type join
`domain/`, the three exceptions merge into `domain/exceptions.py`, and the
tenant-aware repository wrapper joins `application/aggregates/`. It also
retires the `importlib` soft-dependency the aggregate base class used to
read tenant context, now that the module it reached for lives inside the
same ring.

## Status

**Accepted.** Implemented in `44255da`. `import eventsource.multitenancy`
raises `ModuleNotFoundError`, no shim, no transition window — the same
standing rule ADR 0025, ADR 0026, ADR 0030, ADR 0031, ADR 0032, ADR 0033,
and ADR 0034 already applied to every other pre-ring package.

**Amended by [ADR 0042](0042-domain-event-strictness.md).** This ADR's
relocation of `clear_tenant_context()` (and the rest of
`multitenancy/context.py`) to `domain/tenant_context.py` stands unchanged.
ADR 0042 changes `clear_tenant_context()`'s semantics at that same
location: it now empties the token stack, so a subsequent
`reset_tenant_context()` (including a `tenant_scope()` exit) raises
`TenantContextResetError` instead of silently restoring a pre-clear
tenant.

## Decision Table

| Old module | New home | Ring | Rationale |
|---|---|---|---|
| `multitenancy/context.py` (`tenant_context` ContextVar, `TenantContextToken`, `get_current_tenant`, `get_required_tenant`, `set_current_tenant`, `reset_tenant_context`, `clear_tenant_context`, `tenant_scope`, `tenant_scope_sync`) | `domain/tenant_context.py` | domain | Pure stdlib (`contextvars`, `itertools`, `logging`, `contextlib`) plus one intra-package import; tenant identity is a domain concern, not an adapter or use-case one. |
| `multitenancy/events.py` (`TenantDomainEvent`) | `domain/tenant_events.py` | domain | Extends `DomainEvent`; an entity type, same as every other event class living in `domain/`. |
| `multitenancy/exceptions.py` (`TenantContextNotSetError`, `TenantContextResetError`, `TenantMismatchError`) | merged into `domain/exceptions.py` | domain | All three were already rooted at `EventSourceError`; no rebase needed, only relocation. Except-site audit found no site depending on any of them being narrower than `EventSourceError`. |
| `multitenancy/repository.py` (`TenantAwareRepository`) | `application/aggregates/tenant_repository.py` | application | Wraps `AggregateRepository`; colocated with the repository it wraps rather than left as an orphaned top-level module. |
| `multitenancy/__init__.py` | deleted | — | Barrel module; its re-exports are now the root `_LAZY` entries directly. |

## Context

`domain/aggregate.py`'s `AggregateRoot._get_tenant_from_context()` used to
read the current tenant via a dynamic import:

```python
import importlib
multitenancy = importlib.import_module("eventsource.multitenancy")
get_current_tenant = getattr(multitenancy, "get_current_tenant", None)
```

guarded by `except (ImportError, ModuleNotFoundError): return None`. That
existed because `eventsource.multitenancy` sat *outside* the ring map:
`domain/aggregate.py` is a Tier 0 module (the "must not import
sqlalchemy" forbidden contract's source list includes it), and multi-tenancy
support was designed as an optional, load-bearing-only-if-used feature —
`AggregateRoot` could not take a module-level dependency on a package that
might not even exist as a concept for a given application, so it reached
for it lazily and swallowed the absence.

That premise is gone now. `domain/tenant_context.py` is a domain-ring
sibling of `domain/aggregate.py` itself — both are Tier 0, both are pure
stdlib (`tenant_context.py` imports nothing beyond `contextvars` and
`domain/exceptions.py`), and `domain/aggregate.py` importing it is not a
new dependency direction, just a same-ring import that was already implied
by the multi-tenancy feature's design. The dynamic-import dance was solving
a problem — "the module might not be there" — that dissolving the package
into the ring the caller already lives in makes structurally impossible:
`domain/tenant_context.py` ships unconditionally with the library the same
way `domain/aggregate.py` does. The method body collapses to:

```python
def _get_tenant_from_context(self) -> UUID | None:
    return get_current_tenant()
```

with `get_current_tenant` imported at module level. Behavior is unchanged
end to end: `get_current_tenant()` already returned `None` when no tenant
context was set, which is exactly what the old code returned on
`ImportError` — the two paths converge on the same result for every caller
that never touched tenant context, and callers that do use it see no
change at all.

The except-site audit (grep for `except Tenant(ContextNotSet|ContextReset
|Mismatch)Error` across `src/` and `tests/`) found zero sites depending on
these three exceptions being anything other than `EventSourceError`
subclasses — no `ErrorClassifier` or `isinstance` check keys on them more
narrowly. The merge into `domain/exceptions.py` is a pure relocation, the
same as every prior exception merge in this campaign (ADR 0032's
`SubscriptionError`, ADR 0033's three merges).

`TenantAwareRepository` was never re-exported from root `eventsource.__all__`
before this move — it was reachable only as
`eventsource.multitenancy.TenantAwareRepository`, never
`eventsource.TenantAwareRepository`. That asymmetry is preserved exactly:
root `__all__` is byte-identical before and after this move (diffed), and
`TenantAwareRepository` is imported in its new home
(`eventsource.application.aggregates.tenant_repository`) the same way it
always had to be reached — by its full module path, not the front door.

## Consequences

### Positive

- `eventsource.multitenancy` joins every other top-level package this
  multi-ADR campaign has already moved onto the ring map;
  `.claude/rules/architecture.md`'s "during transition" wording for it is
  retired.
- The `importlib`/`getattr` dynamic-import dance in `domain/aggregate.py`
  is gone; the method is a direct, statically-analyzable call with a
  module-level import mypy strict can actually check.
- Root `__init__.py`'s `_LAZY`/`TYPE_CHECKING` entries for the twelve
  multi-tenancy names now point at `domain.tenant_context`,
  `domain.tenant_events`, and `domain.exceptions` directly, one hop closer
  to their actual implementation than the old barrel-module indirection.

### Negative

- `import eventsource.multitenancy` now raises `ModuleNotFoundError` with
  no transition period. Two `src/` docstring examples
  (`application/projections/base.py`, `adapters/sql/projection.py`) and
  five `tests/unit/multitenancy/` test files needed retargeting.
- Two of the aggregate's own unit tests exercised the old
  `ImportError`-fallback path directly (mocking
  `sys.modules["eventsource.multitenancy"]`); that path no longer exists,
  so those tests were rewritten to exercise the current direct-call
  behavior instead of the removed dynamic-import mechanism.

## Alternatives Considered

**Leave the `importlib` dynamic import in place even after the module
moved in-ring, for symmetry with any future genuinely-optional feature.**
Rejected: the dynamic import's only justification was "the module might
not exist," which was true when multi-tenancy was an out-of-ring package
and is false the moment it becomes a same-ring sibling shipped
unconditionally with every install. Keeping it would be defending against
a condition that can no longer occur, at the cost of an unanalyzable
`getattr`-based call the type checker cannot verify.

## References

- `src/eventsource/domain/tenant_context.py`,
  `src/eventsource/domain/tenant_events.py`,
  `src/eventsource/application/aggregates/tenant_repository.py`
- `src/eventsource/domain/aggregate.py` —
  `AggregateRoot._get_tenant_from_context()`
- [ADR 0018](0018-tenant-isolation-model.md) — the original multi-tenancy
  design (ambient `ContextVar` tenancy, `TenantAwareRepository` composition
  wrapper); the Decision itself is unaffected by this move, only the
  module's location
- [ADR 0032](0032-subscriptions-ring-migration.md),
  [ADR 0033](0033-events-handlers-internal-ring-migration.md),
  [ADR 0034](0034-migration-ring-and-layers-contract.md) — the same
  no-shim, ring-map-completion pattern this ADR applies to multi-tenancy

## Related

- `docs/guides/multi-tenant.md`, `docs/tutorials/16-multi-tenancy.md`,
  `docs/api/multitenancy.md`, `docs/api/exceptions.md`, `docs/api/index.md`,
  `docs/api/types.md` — import paths updated for the new module layout
- `.claude/rules/architecture.md` — ring map updated to mark
  `domain/tenant_context.py`, `domain/tenant_events.py`, and
  `application/aggregates/tenant_repository.py` as settled
</content>
