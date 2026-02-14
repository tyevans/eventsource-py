# Agent Catalog

Quick reference for which agent to dispatch for each task type.

## Agent Summary

| Agent | Purpose | Model | Invoke When |
|-------|---------|-------|-------------|
| `code-reviewer` | Review changes for quality, architecture, protocol conformance | sonnet | Before merging, after implementation |
| `test-generator` | Create/update tests matching async-first patterns | sonnet | After implementation, when coverage gaps found |
| `debugger` | Diagnose bugs through event sourcing pipeline tracing | sonnet | Test failures, unexpected behavior, race conditions |
| `backend-implementer` | Add new EventStore/EventBus/repository backends | sonnet | Adding PostgreSQL, SQLite, Redis, Kafka, RabbitMQ, or new backends |
| `refactorer` | Safe restructuring without behavior change | sonnet | Technical debt cleanup, module extraction, deduplication |
| `event-modeler` | Design events, aggregates, projections, @handles wiring | sonnet | New domain features, event schema design, aggregate behavior |

## Agent Capabilities Matrix

| Agent | Reads Code | Writes Code | Runs Tests | Runs Linter | Uses Beads |
|-------|-----------|-------------|-----------|------------|-----------|
| code-reviewer | Y | N | N | Y | Y |
| test-generator | Y | Y | Y | N | Y |
| debugger | Y | N | Y | N | Y |
| backend-implementer | Y | Y | Y | Y | Y |
| refactorer | Y | Y | Y | Y | Y |
| event-modeler | Y | Y | Y | Y | Y |

## Common Workflows

### New Feature (domain-level)
1. `event-modeler` -- Design events, aggregate, and projections
2. `test-generator` -- Unit tests for aggregate behavior
3. `code-reviewer` -- Review for architecture compliance

### New Backend Implementation
1. `backend-implementer` -- Implement the protocol for the new backend
2. `test-generator` -- Integration tests with proper markers
3. `code-reviewer` -- Review protocol conformance and dependency guards

### Bug Fix
1. `debugger` -- Diagnose root cause through pipeline tracing
2. Appropriate implementation agent -- Apply the fix
3. `test-generator` -- Regression test
4. `code-reviewer` -- Review fix

### Refactoring
1. `refactorer` -- Restructure code safely
2. `code-reviewer` -- Verify no API changes, all tests pass

### Test Coverage Improvement
1. `test-generator` -- Identify gaps and generate tests

## Key Project Commands

```bash
# Unit tests (fast, no Docker)
uv run pytest tests/unit/ -v

# Integration tests (requires Docker)
docker-compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/ -v

# Lint and format
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/

# Type check
uv run mypy src/eventsource/ --config-file=pyproject.toml
```
