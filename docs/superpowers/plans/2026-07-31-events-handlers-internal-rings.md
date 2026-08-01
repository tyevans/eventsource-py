# Plan: events/, handlers/, and _internal/ ring migration

Date: 2026-07-31
Branch: `events-handlers-internal-rings`
ADR: 0033 (no open sibling PRs; 0032 is current max)

Dissolves the last transitional top-level packages named in
`.claude/rules/architecture.md` — `events/` (ring 1 transitional) and
`handlers/` (ring 2 transitional) — plus the `_internal/` shared-utility
package. Clean breaks, no shims (standing ADR 0025/0026/0030 rule).
Public API surface of `eventsource/__init__.py` stays byte-identical.

## Ring assignments

| Old module | New home | Ring | Rationale |
|---|---|---|---|
| `events/base.py` (DomainEvent) | `domain/event.py` | domain | Pure pydantic entity |
| `events/registry.py` (EventRegistry, register_event, default_registry, lookup funcs) | `domain/event_registry.py` | domain | Pure stdlib (threading) + domain types |
| `EventTypeNotFoundError`, `DuplicateEventTypeError` | `domain/exceptions.py` | domain | Full-hierarchy rule (ADR 0030); rebased onto `EventSourceError` with `KeyError`/`ValueError` mixins retained. Except-sites verified: only tests/unit/test_event_registry.py, all compatible |
| `handlers/decorators.py` (@handles, get_handled_event_type, is_event_handler) | `domain/decorators.py` | domain | `DeclarativeAggregate` (domain/aggregate.py:716) discovers `_handles_event_type`; the attribute contract is owned by the innermost consuming ring |
| `handlers/registry.py` (HandlerRegistry, HandlerInfo, UnregisteredEventHandling) | `application/projections/handlers.py` | application | Extracted from DeclarativeProjection (ADR 0013); sole src importer is `application/projections/base.py` |
| `HandlerSignatureError` | `domain/exceptions.py` | domain | Full-hierarchy rule; `ValueError` mixin retained. Except-sites: tests/unit/handlers/test_registry.py only |
| `handlers/adapter.py` (HandlerAdapter, AsyncHandlerFunc, get_handler_name) | `adapters/_bus/handler_adapter.py` | adapters | Every importer is a bus adapter (`_bus/base`, `_bus/registry`, kafka, rabbitmq, redis, memory). Drop the ports re-export compat block (clean break) |
| `_internal/background_tasks.py` (BackgroundTaskManager) | `application/background_tasks.py` | application | Shared by application (snapshotting) and adapters (`_bus/base` drain-on-shutdown). Adapters may depend inward; application ring owns it. No code refactor needed — placement is the fix |

## Slices (serial)

0. Commit Ty's pending adapter.py change (`inspect.iscoroutinefunction`
   swap; `import asyncio` stays — `asyncio.iscoroutine` still used at two
   call sites) so it travels with the `git mv`.
1. **events/ move**: exception merge first, then `git mv`, delete
   `events/`, repoint ~40 src importers + `__init__.py` + bench/ +
   testing/. Move root-level unit tests (`test_domain_event.py`,
   `test_event_registry.py`, `test_event_type_auto.py`) →
   `tests/unit/domain/`. Delete `tests/unit/events/` (pycache debris).
   Import-linter Tier 0: `events.base`/`events.registry` →
   `domain.event`/`domain.event_registry`. Guard test
   (`ModuleNotFoundError` for `eventsource.events`) in test_public_api.py.
2. **handlers/ + _internal/ move**: HandlerSignatureError merge, three
   `git mv`s + background_tasks, delete both packages. Repoint importers
   incl. ~25 inline imports in tests/unit/test_rabbitmq_event_bus.py and
   the compat import at tests/unit/ports/test_handlers.py:478 (→ ports).
   Tests: `handlers/test_decorators.py` → `tests/unit/domain/`;
   `handlers/test_registry.py` →
   `tests/unit/application/projections/test_handler_registry.py`;
   `_internal/test_background_tasks.py` →
   `tests/unit/application/`. Delete debris dirs (`tests/unit/handlers`,
   `tests/unit/_internal`, `tests/unit/bus` pycache leftover from ADR
   0031). Import-linter Tier 0: `handlers.decorators` →
   `domain.decorators`; drop `handlers.registry` (covered by
   `eventsource.application`); `handlers.adapter` → add explicit
   `eventsource.adapters._bus`. Guard tests for `eventsource.handlers`
   and `eventsource._internal`.
3. **Docs/meta**: ADR 0033; Amended-by pointers (ADR 0013 certainly;
   grep ADR bodies for others); index.md; docs/api/{events,handlers,
   exceptions}.md mkdocstrings paths; `.claude/rules/architecture.md`
   (remove `events/` and `handlers/` from transitional lists, add settled
   sentences); CHANGELOG `**BREAKING**` entries (old path →
   ModuleNotFoundError + all replacement paths). mkdocs nav: page set
   unchanged, no nav edits expected.
4. **Sweep + gate**: `sweep.sh events`, `sweep.sh handlers`,
   `sweep.sh _internal`; `make check`; Docker integration suite;
   `validate_examples.py`; `mkdocs build --strict`. Then PR.

## Notes

- `only_mutate` and mutmut test-selection in pyproject already cover all
  destination trees; no changes needed there.
- `eventsource.multitenancy.events` is a different module; sweep greps
  must not flag it (dotted-path anchor prevents this).
- Per-slice gate: targeted pytest + `uv run lint-imports` + ruff + mypy.
  Full gate only at the end.
