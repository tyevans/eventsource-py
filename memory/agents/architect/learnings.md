# Learnings: architect

## Codebase Patterns
- `protocols.py` contains both Protocols (structural subtyping) AND ABCs (EventSubscriber, AsyncEventHandler require explicit inheritance) -- not purely protocol-based (added: 2026-02-13)
- Events auto-register via `DomainEvent.__init_subclass__` into the global EventRegistry (added: 2026-02-13)
- Public API re-exported from top-level `__init__.py` -- changes here affect backward compatibility (added: 2026-02-13)

## Codebase Details
- `DomainEvent` uses `ConfigDict(frozen=True)` -- events are truly immutable pydantic models (added: 2026-02-13)
- After dropping redis from core deps, core dependencies are just pydantic + sqlalchemy (Tier 0 weight goal). Redis optional extra already had correct constraint `>=5.0,<6.0` (updated: 2026-02-14)
- `subscriptions/` is substantial (~20 files): lifecycle management, health checks, retry, flow control, pause/resume (added: 2026-02-13)
- `migration/` is a full live-migration toolkit (dual-write, cutover, sync lag tracking, position mapping) (added: 2026-02-13)

## Gotchas
- repositories/checkpoint.py and repositories/dlq.py mix Protocol definitions with sqlalchemy implementations -- main blocker for clean Tier 0 extraction (added: 2026-02-14)
- projections/base.py is NOT dependency-free: transitively imports sqlalchemy via repositories (added: 2026-02-14)
- testing/harness.py blocked from Tier 0 by same repository import chain (added: 2026-02-14)

## Preferences
- (none yet)

## Cross-Agent Notes
- (none yet)
