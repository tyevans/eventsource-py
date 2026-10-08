---
id: '0102'
title: Hexagonal Ring Layering and Dependency Isolation
status: Accepted
target_bc: core
governing_prds:
- PRD-0001
- PRD-0004
governing_stories:
- US-0012
---

# ADR-0102: Hexagonal Ring Layering and Dependency Isolation

## Summary
Strict layered architecture: adapters > application > ports > domain.

## Context
In complex event sourcing frameworks, business logic frequently becomes coupled to database models, broker drivers, or telemetry frameworks. Prior refactoring waves established ring layering contracts to keep pure domain invariants isolated from infrastructure.

## Decision
1. Ring dependency order is strictly enforced: `adapters` over `application` over `ports` over `domain`.
2. `domain` and `ports` must never import `observability` or infrastructure technologies.
3. Inner rings must never import the `testing` toolkit.
4. Enforced automatically in CI via `import-linter` contracts.

## Consequences
- Domain models and ports remain 100% portable with zero vendor coupling.
- Swapping storage or messaging adapters requires zero changes to domain business logic.
- Modular architectural boundaries prevent dependency spaghetti.
