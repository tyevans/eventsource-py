---
id: '0012'
title: Enforce Hexagonal Ring Layering and Strict Blackbox Frontdoor Verification
status: Accepted
created: 2026-10-08
persona: Morgan (The Autonomous Coding Agent & Pair Programmer)
target_bc: core
feature: FEAT-ARCHITECTURE-INVARIANTS
governing_prd: PRD-0001
scenarios:
- Blackbox frontdoor verification exercises public contracts without private mock backdoors
- Modular file length ceiling strictly prevents source files exceeding 500 lines
- Hexagonal ring layering contracts prohibit inward dependency violations and cross-ring leakage
- Native modern Python 3.13 typing contracts enforce compile-time bounds and defaults
- Worktree concurrency and backlog isolation prevent multi-agent git merge contention
- Universal base exception EventSourceError ensures uniform library error handling
governing_adrs:
- ADR-0002
- ADR-0003
- ADR-0004
- ADR-0005
- ADR-0007
- ADR-0130
- ADR-0134
- ADR-0140
- ADR-0143
- ADR-0145
- ADR-0155
- ADR-0158
---

# US-0012 — Enforce Hexagonal Ring Layering and Strict Blackbox Frontdoor Verification

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** autonomous AI coding agent and pair programmer (Morgan),
**I want** strict hexagonal ring layering, modular file length ceilings (<500 lines), modern Python 3.13 typing contracts, git worktree backlog isolation, and blackbox frontdoor verification rooted in `EventSourceError`,
**So that** I can autonomously implement features and verify contracts without private mock backdoors, context window rot, dependency leaks, or git merge collisions.

## Acceptance Criteria

```gherkin
Scenario: Blackbox frontdoor verification exercises public contracts without private mock backdoors
  Given an autonomous agent or test harness verifying domain, storage, bus, or migration components
  When tests execute scenarios for aggregate commands, store replays, or projection processing
  Then all test steps interact exclusively via public API entry points, domain models, and port protocols
  And no test doubles reach into private attributes ("_fields") or bypass aggregate lifecycle checks
  And assertions verify observable outcomes, emitted events, and domain return types.
```

```gherkin
Scenario: Modular file length ceiling strictly prevents source files exceeding 500 lines
  Given source and test files across all bounded contexts in "src/eventsource" and "tests"
  When "spec-ops health" or preflight static analysis evaluates file lengths
  Then zero source files exceed the hard 500-line modularity limit
  And any file reaching or exceeding 400 lines triggers a proactive refactoring warning
  And agents decompose growing modules into single-responsibility collaborators before adding new code.
```

```gherkin
Scenario: Hexagonal ring layering contracts prohibit inward dependency violations and cross-ring leakage
  Given the Clean Architecture layered hierarchy: adapters over application over ports over domain
  When "import-linter" or static architecture checks inspect module imports across the codebase
  Then inner rings ("domain", "ports") never import from outer rings ("application", "adapters")
  And "domain" and "ports" remain completely free of imports from "observability"
  And inner library rings never import testing toolkits or test fixtures.
```

```gherkin
Scenario: Native modern Python 3.13 typing contracts enforce compile-time bounds and defaults
  Given core domain models, decider aggregates, and generic store projections
  When static type checkers (mypy, pyright) inspect signatures and class declarations
  Then PEP 695 type parameter syntax ("class AggregateRoot[TState: BaseModel]") is used natively
  And PEP 696 type parameter defaults and PEP 692 "Unpack" typing contracts provide static inference
  And legacy "TypeVar" and "Generic[T]" boilerplate is eliminated from core domain models.
```

```gherkin
Scenario: Worktree concurrency and backlog isolation prevent multi-agent git merge contention
  Given multiple autonomous agents working concurrently on separate backlog tasks
  When each agent executes in an isolated git worktree (".worktrees/<task-id>") on a task branch
  Then shared planning files in "docs/project/backlog/" and "PRIORITY.md" are never modified on feature branches
  And the supply-chain lockfile ("uv.lock") remains byte-immutable without unapproved dependency alterations
  And backlog state transitions synchronize atomically only during mainline integration.
```

```gherkin
Scenario: Universal base exception EventSourceError ensures uniform library error handling
  Given errors originating anywhere across domain, storage, bus, projection, or migration modules
  When an exception is raised due to validation failure, version conflict, bus dispatch, or connection drop
  Then the raised exception inherits from the universal root class "EventSourceError"
  And caller perimeter handlers reliably catch all framework exceptions via "except EventSourceError"
  And specific failure semantics retain original causal exceptions in "__cause__".
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0007, ADR-0130, ADR-0134, ADR-0140, ADR-0143, ADR-0145, ADR-0155, ADR-0158
- **Verified Test Suites**:
  - `uv run spec-ops health`: Verifies 0 file length limit violations (<500 lines) and strict priority queue synchronization.
  - Ring layering verification: Confirms inward dependency compliance (`adapters` > `application` > `ports` > `domain`).
  - `tests/unit/domain/`: Verifies PEP 695 typing syntax and clean blackbox frontdoor execution.
  - `tests/unit/ports/test_handlers.py`: Verifies port abstraction and isolation from concrete adapters.
- **Architectural Invariants Verified**:
  - *Blackbox Frontdoor Verification*: All tests exercise public interfaces without private mock backdoors or state tampering.
  - *Modular File Length Limit*: All source files strictly under 500 lines.
  - *Hexagonal Ring Layering*: Domain and ports maintain zero imports of infrastructure, testing, or observability.
  - *Universal Base Exception*: All library errors derive from `EventSourceError`.
