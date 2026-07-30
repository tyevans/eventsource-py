---
paths:
  - "src/**/*.py"
  - "tests/**/*.py"
  - "docs/**/*.md"
---

# Definition of Done

## Architectural Decisions (applies to ALL work)

No work is complete if the decisions it made or changed are not properly
documented and amended:

1. **No brainstorming or design spec is complete** until it has been run
   against the existing ADRs in `docs/adrs/`. Every spec must include an
   **ADR Impact** section listing each related ADR and whether it
   **stands**, is **amended**, or is **superseded** by the proposed work.
2. If work amends or supersedes an ADR, that is part of the work, not a
   follow-up: write the new ADR (or amend the old one's Consequences), and
   update the old ADR's **Status** section with an "Amended by" /
   "Superseded by" pointer. ADR bodies are immutable records -- never
   rewrite a Decision retroactively; supersede it.
3. New architecturally significant decisions (delivery guarantees, layer
   boundaries, public contracts, rejected alternatives) get their own ADR
   in `docs/adrs/`, numbered after the current highest.

## New Feature

1. Implementation in `src/eventsource/` with type annotations (mypy strict passes)
2. Unit tests in `tests/unit/` covering happy path and edge cases
3. Integration tests if feature touches a backend (postgres, sqlite, redis, kafka, rabbitmq)
4. Public API re-exported from `src/eventsource/__init__.py` with `__all__` entry
5. `uv run ruff check` and `uv run ruff format` pass
6. `uv run mypy src/eventsource/` passes

## Bug Fix

1. Failing test that reproduces the bug
2. Fix implementation
3. All existing tests pass
4. Lint and type check pass

## Refactor

1. No behavior change -- existing tests pass without modification
2. Lint and type check pass
3. No public API changes unless explicitly intended

## New Backend Implementation

1. Implements the relevant protocol/interface (EventStore, EventBus, etc.)
2. Lives under `src/eventsource/infrastructure/<backend>/` or appropriate module
3. Optional dependency guard with try/except ImportError
4. Integration tests with appropriate pytest marker
5. Docker service added to `docker-compose.test.yml` if needed
