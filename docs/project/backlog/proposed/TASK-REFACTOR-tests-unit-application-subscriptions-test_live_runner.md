---
id: REFACTOR-tests-unit-application-subscriptions-test_live_runner
title: Refactor and Decompose Legacy File test_live_runner.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_live_runner: Refactor Legacy File test_live_runner.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_live_runner.py` contains 1144 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_live_runner_event.py, test_live_runner_subscriber.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_live_runner/` with submodules:
- `test_live_runner_event.py`: LiveTestEvent, AnotherTestEvent, event_bus, event_store, append_event, TestLiveRunnerEventProcessing, checkpoint_repo, config, subscription, runner, wake, TestLiveRunnerStats, TestLiveRunnerBasic, TestLiveRunnerBufferMode, TestLiveRunnerFeedOrdering, TestLiveRunnerCheckpointStrategies, TestLiveRunnerErrorHandling, TestLiveRunnerImports, TestLiveRunnerEdgeCases, _RecordingFeed, TestLiveRunnerBoundedDrain, TestLiveRunnerStopPauseResponsiveness, TestLiveRunnerTenantIsolation
- `test_live_runner_subscriber.py`: MockLiveSubscriber, subscriber

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_live_runner.py (1144 lines):
  Submodule 'test_live_runner_event.py' (~963 lines):
    - [class] LiveTestEvent (lines 49-53)
    - [class] AnotherTestEvent (lines 57-61)
    - [function] event_bus (lines 90-92)
    - [function] event_store (lines 96-98)
    - [function] append_event (lines 148-156)
    - [class] TestLiveRunnerEventProcessing (lines 316-383)
    - [function] checkpoint_repo (lines 102-104)
    - [function] config (lines 114-119)
    - [function] subscription (lines 123-129)
    - [function] runner (lines 133-145)
    - [function] wake (lines 159-166)
    - [class] TestLiveRunnerStats (lines 172-194)
    - [class] TestLiveRunnerBasic (lines 200-310)
    - [class] TestLiveRunnerBufferMode (lines 389-477)
    - [class] TestLiveRunnerFeedOrdering (lines 488-529)
    - [class] TestLiveRunnerCheckpointStrategies (lines 535-665)
    - [class] TestLiveRunnerErrorHandling (lines 671-740)
    - [class] TestLiveRunnerImports (lines 746-770)
    - [class] TestLiveRunnerEdgeCases (lines 776-909)
    - [class] _RecordingFeed (lines 921-934)
    - [class] TestLiveRunnerBoundedDrain (lines 937-966)
    - [class] TestLiveRunnerStopPauseResponsiveness (lines 969-1032)
    - [class] TestLiveRunnerTenantIsolation (lines 1045-1144)
  Submodule 'test_live_runner_subscriber.py' (~20 lines):
    - [class] MockLiveSubscriber (lines 67-83)
    - [function] subscriber (lines 108-110)
  Suggested barrel exports:
    from .test_live_runner_event import LiveTestEvent, AnotherTestEvent, event_bus, event_store, append_event, TestLiveRunnerEventProcessing, checkpoint_repo, config, subscription, runner, wake, TestLiveRunnerStats, TestLiveRunnerBasic, TestLiveRunnerBufferMode, TestLiveRunnerFeedOrdering, TestLiveRunnerCheckpointStrategies, TestLiveRunnerErrorHandling, TestLiveRunnerImports, TestLiveRunnerEdgeCases, _RecordingFeed, TestLiveRunnerBoundedDrain, TestLiveRunnerStopPauseResponsiveness, TestLiveRunnerTenantIsolation
    from .test_live_runner_subscriber import MockLiveSubscriber, subscriber

    __all__ = ["LiveTestEvent", "AnotherTestEvent", "event_bus", "event_store", "append_event", "TestLiveRunnerEventProcessing", "checkpoint_repo", "config", "subscription", "runner", "wake", "TestLiveRunnerStats", "TestLiveRunnerBasic", "TestLiveRunnerBufferMode", "TestLiveRunnerFeedOrdering", "TestLiveRunnerCheckpointStrategies", "TestLiveRunnerErrorHandling", "TestLiveRunnerImports", "TestLiveRunnerEdgeCases", "_RecordingFeed", "TestLiveRunnerBoundedDrain", "TestLiveRunnerStopPauseResponsiveness", "TestLiveRunnerTenantIsolation", "MockLiveSubscriber", "subscriber"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
