---
id: REFACTOR-tests-unit-application-subscriptions-test_manager_pause_resume
title: Refactor and Decompose Legacy File test_manager_pause_resume.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_manager_pause_resume: Refactor Legacy File test_manager_pause_resume.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_manager_pause_resume.py` contains 683 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_manager_pause_resume_event.py, test_manager_pause_resume_projection.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_manager_pause_resume/` with submodules:
- `test_manager_pause_resume_event.py`: PauseTestEvent, event_store, event_bus, MockSubscriber, checkpoint_repo, manager, add_events_to_store, TestManagerPauseSubscription, TestManagerResumeSubscription, TestManagerPauseAll, TestManagerResumeAll, TestPausedSubscriptionsProperties, TestPauseResumeLifecycle, TestHealthStatusWithPaused
- `test_manager_pause_resume_projection.py`: OrderProjection, CustomerProjection

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_manager_pause_resume.py (683 lines):
  Submodule 'test_manager_pause_resume_event.py' (~577 lines):
    - [class] PauseTestEvent (lines 38-42)
    - [function] event_store (lines 78-80)
    - [function] event_bus (lines 84-86)
    - [class] MockSubscriber (lines 48-59)
    - [function] checkpoint_repo (lines 90-92)
    - [function] manager (lines 96-102)
    - [function] add_events_to_store (lines 105-120)
    - [class] TestManagerPauseSubscription (lines 126-239)
    - [class] TestManagerResumeSubscription (lines 245-322)
    - [class] TestManagerPauseAll (lines 328-411)
    - [class] TestManagerResumeAll (lines 417-483)
    - [class] TestPausedSubscriptionsProperties (lines 489-556)
    - [class] TestPauseResumeLifecycle (lines 562-633)
    - [class] TestHealthStatusWithPaused (lines 639-683)
  Submodule 'test_manager_pause_resume_projection.py' (~8 lines):
    - [class] OrderProjection (lines 62-65)
    - [class] CustomerProjection (lines 68-71)
  Suggested barrel exports:
    from .test_manager_pause_resume_event import PauseTestEvent, event_store, event_bus, MockSubscriber, checkpoint_repo, manager, add_events_to_store, TestManagerPauseSubscription, TestManagerResumeSubscription, TestManagerPauseAll, TestManagerResumeAll, TestPausedSubscriptionsProperties, TestPauseResumeLifecycle, TestHealthStatusWithPaused
    from .test_manager_pause_resume_projection import OrderProjection, CustomerProjection

    __all__ = ["PauseTestEvent", "event_store", "event_bus", "MockSubscriber", "checkpoint_repo", "manager", "add_events_to_store", "TestManagerPauseSubscription", "TestManagerResumeSubscription", "TestManagerPauseAll", "TestManagerResumeAll", "TestPausedSubscriptionsProperties", "TestPauseResumeLifecycle", "TestHealthStatusWithPaused", "OrderProjection", "CustomerProjection"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
