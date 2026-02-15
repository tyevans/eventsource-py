---
paths:
  - "src/**/*.py"
  - "tests/**/*.py"
---

# Definition of Done

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
