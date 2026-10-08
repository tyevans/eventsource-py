---
id: REFACTOR-tests-unit-application-subscriptions-test_transition
title: Refactor and Decompose Legacy File test_transition.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_transition: Refactor Legacy File test_transition.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_transition.py` contains 1015 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_transition_event.py, test_transition_subscriber.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_transition/` with submodules:
- `test_transition_event.py`: TransitionTestEvent, AnotherTransitionEvent, event_store, event_bus, checkpoint_repo, config, subscription, coordinator, add_events_to_store, current, TestTransitionResult, TestTransitionPhase, TestTransitionCoordinatorBasic, TestTransitionAlreadyCaughtUp, TestTransitionBufferMode, TestTransitionErrorHandling, TestTransitionStop, TestTransitionPhaseTracking, TestStartFromResolver, TestTransitionLiveRunnerAccess, TestTransitionImports, TestTransitionEdgeCases
- `test_transition_subscriber.py`: MockTransitionSubscriber, subscriber

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_transition.py (1015 lines):
  Submodule 'test_transition_event.py' (~853 lines):
    - [class] TransitionTestEvent (lines 47-51)
    - [class] AnotherTransitionEvent (lines 55-59)
    - [function] event_store (lines 88-90)
    - [function] event_bus (lines 94-96)
    - [function] checkpoint_repo (lines 100-102)
    - [function] config (lines 112-117)
    - [function] subscription (lines 121-127)
    - [function] coordinator (lines 131-143)
    - [function] add_events_to_store (lines 146-161)
    - [function] current (lines 164-168)
    - [class] TestTransitionResult (lines 174-206)
    - [class] TestTransitionPhase (lines 212-223)
    - [class] TestTransitionCoordinatorBasic (lines 229-301)
    - [class] TestTransitionAlreadyCaughtUp (lines 307-348)
    - [class] TestTransitionBufferMode (lines 354-423)
    - [class] TestTransitionErrorHandling (lines 429-495)
    - [class] TestTransitionStop (lines 501-551)
    - [class] TestTransitionPhaseTracking (lines 557-590)
    - [class] TestStartFromResolver (lines 596-733)
    - [class] TestTransitionLiveRunnerAccess (lines 739-822)
    - [class] TestTransitionImports (lines 828-866)
    - [class] TestTransitionEdgeCases (lines 872-1015)
  Submodule 'test_transition_subscriber.py' (~20 lines):
    - [class] MockTransitionSubscriber (lines 65-81)
    - [function] subscriber (lines 106-108)
  Suggested barrel exports:
    from .test_transition_event import TransitionTestEvent, AnotherTransitionEvent, event_store, event_bus, checkpoint_repo, config, subscription, coordinator, add_events_to_store, current, TestTransitionResult, TestTransitionPhase, TestTransitionCoordinatorBasic, TestTransitionAlreadyCaughtUp, TestTransitionBufferMode, TestTransitionErrorHandling, TestTransitionStop, TestTransitionPhaseTracking, TestStartFromResolver, TestTransitionLiveRunnerAccess, TestTransitionImports, TestTransitionEdgeCases
    from .test_transition_subscriber import MockTransitionSubscriber, subscriber

    __all__ = ["TransitionTestEvent", "AnotherTransitionEvent", "event_store", "event_bus", "checkpoint_repo", "config", "subscription", "coordinator", "add_events_to_store", "current", "TestTransitionResult", "TestTransitionPhase", "TestTransitionCoordinatorBasic", "TestTransitionAlreadyCaughtUp", "TestTransitionBufferMode", "TestTransitionErrorHandling", "TestTransitionStop", "TestTransitionPhaseTracking", "TestStartFromResolver", "TestTransitionLiveRunnerAccess", "TestTransitionImports", "TestTransitionEdgeCases", "MockTransitionSubscriber", "subscriber"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
