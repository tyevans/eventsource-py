---
id: '0103'
title: Pure Functional Decider Pattern and Immutable State Folding
status: Accepted
target_bc: domain
governing_prds:
- PRD-0001
governing_stories:
- US-0001
- US-0016
---

# ADR-0103: Pure Functional Decider Pattern and Immutable State Folding

## Summary
Typed DeciderAggregate, immutable Pydantic states, and pure state folding.

## Context
Traditional object-oriented aggregates mutate private internal state during command handling, complicating testing, time-travel debugging, and concurrency verification. The Decider pattern models domain behavior as pure mathematical functions: decide(state, command) -> list[event] and evolve(state, event) -> state.

## Decision
1. Provide typed `DeciderAggregate[TState, TCommand]` using PEP 695 generics (`class DeciderAggregate[TState: BaseModel, TCommand: DomainCommand]`).
2. Aggregate state is an immutable Pydantic model (`frozen=True`) that is never mutated in place.
3. State rehydration uses pure folding without side effects.
4. Provide nullary `initial_state()` construct so initial state does not depend on uninstantiated aggregate IDs.
5. Aggregate type names are defined single-source via `ClassVar[str]`.

## Consequences
- Deterministic, easily testable business logic without mocks.
- Pure state transitions make event replay trivial.
- Eliminates state mutation bugs across coroutines.
