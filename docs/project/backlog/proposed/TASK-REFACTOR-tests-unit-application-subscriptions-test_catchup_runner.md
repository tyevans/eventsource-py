---
id: REFACTOR-tests-unit-application-subscriptions-test_catchup_runner
title: Refactor and Decompose Legacy File test_catchup_runner.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_catchup_runner: Refactor Legacy File test_catchup_runner.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_catchup_runner.py` contains 927 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_catchup_runner_event.py, test_catchup_runner_subscriber.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_catchup_runner/` with submodules:
- `test_catchup_runner_event.py`: SampleTestEvent, SampleFailingEvent, event_store, checkpoint_repo, config, subscription, runner, add_events_to_store, current, position_after, TestCatchUpResult, TestCatchUpRunnerBasic, TestCatchUpRunnerBatching, TestCatchUpRunnerCheckpointStrategies, TestCatchUpRunnerErrorHandling, TestCatchUpRunnerStop, TestCatchUpRunnerPositionTracking, TestCatchUpRunnerImports, TestCatchUpRunnerEdgeCases
- `test_catchup_runner_subscriber.py`: MockSubscriber, subscriber

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_catchup_runner.py (927 lines):
  Submodule 'test_catchup_runner_event.py' (~790 lines):
    - [class] SampleTestEvent (lines 41-45)
    - [class] SampleFailingEvent (lines 49-53)
    - [function] event_store (lines 79-81)
    - [function] checkpoint_repo (lines 85-87)
    - [function] config (lines 97-102)
    - [function] subscription (lines 106-112)
    - [function] runner (lines 116-122)
    - [function] add_events_to_store (lines 125-140)
    - [function] current (lines 143-147)
    - [function] position_after (lines 150-155)
    - [class] TestCatchUpResult (lines 161-206)
    - [class] TestCatchUpRunnerBasic (lines 212-326)
    - [class] TestCatchUpRunnerBatching (lines 332-391)
    - [class] TestCatchUpRunnerCheckpointStrategies (lines 397-512)
    - [class] TestCatchUpRunnerErrorHandling (lines 518-612)
    - [class] TestCatchUpRunnerStop (lines 618-722)
    - [class] TestCatchUpRunnerPositionTracking (lines 728-798)
    - [class] TestCatchUpRunnerImports (lines 804-819)
    - [class] TestCatchUpRunnerEdgeCases (lines 825-927)
  Submodule 'test_catchup_runner_subscriber.py' (~17 lines):
    - [class] MockSubscriber (lines 59-72)
    - [function] subscriber (lines 91-93)
  Suggested barrel exports:
    from .test_catchup_runner_event import SampleTestEvent, SampleFailingEvent, event_store, checkpoint_repo, config, subscription, runner, add_events_to_store, current, position_after, TestCatchUpResult, TestCatchUpRunnerBasic, TestCatchUpRunnerBatching, TestCatchUpRunnerCheckpointStrategies, TestCatchUpRunnerErrorHandling, TestCatchUpRunnerStop, TestCatchUpRunnerPositionTracking, TestCatchUpRunnerImports, TestCatchUpRunnerEdgeCases
    from .test_catchup_runner_subscriber import MockSubscriber, subscriber

    __all__ = ["SampleTestEvent", "SampleFailingEvent", "event_store", "checkpoint_repo", "config", "subscription", "runner", "add_events_to_store", "current", "position_after", "TestCatchUpResult", "TestCatchUpRunnerBasic", "TestCatchUpRunnerBatching", "TestCatchUpRunnerCheckpointStrategies", "TestCatchUpRunnerErrorHandling", "TestCatchUpRunnerStop", "TestCatchUpRunnerPositionTracking", "TestCatchUpRunnerImports", "TestCatchUpRunnerEdgeCases", "MockSubscriber", "subscriber"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
