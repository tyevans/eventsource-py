---
id: REFACTOR-tests-unit-application-subscriptions-test_pause_resume
title: Refactor and Decompose Legacy File test_pause_resume.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_pause_resume: Refactor Legacy File test_pause_resume.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_pause_resume.py` contains 563 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_pause_resume_subscription.py, test_pause_resume_subscriber.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_pause_resume/` with submodules:
- `test_pause_resume_subscription.py`: subscription, TestSubscriptionPause, TestSubscriptionPauseInvalidStates, TestSubscriptionResume, TestSubscriptionResumeInvalidStates, pos, config, TestPauseReasonEnum, TestPauseProperties, TestWaitIfPaused, TestConcurrentPauseResume, TestMultiplePauseResumeCycles, TestPauseResumeModuleImports
- `test_pause_resume_subscriber.py`: MockSubscriber, mock_subscriber

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_pause_resume.py (563 lines):
  Submodule 'test_pause_resume_subscription.py' (~457 lines):
    - [function] subscription (lines 63-69)
    - [class] TestSubscriptionPause (lines 102-190)
    - [class] TestSubscriptionPauseInvalidStates (lines 196-232)
    - [class] TestSubscriptionResume (lines 238-295)
    - [class] TestSubscriptionResumeInvalidStates (lines 301-345)
    - [function] pos (lines 35-37)
    - [function] config (lines 57-59)
    - [class] TestPauseReasonEnum (lines 75-96)
    - [class] TestPauseProperties (lines 351-394)
    - [class] TestWaitIfPaused (lines 400-446)
    - [class] TestConcurrentPauseResume (lines 452-498)
    - [class] TestMultiplePauseResumeCycles (lines 504-541)
    - [class] TestPauseResumeModuleImports (lines 547-563)
  Submodule 'test_pause_resume_subscriber.py' (~11 lines):
    - [class] MockSubscriber (lines 40-47)
    - [function] mock_subscriber (lines 51-53)
  Suggested barrel exports:
    from .test_pause_resume_subscription import subscription, TestSubscriptionPause, TestSubscriptionPauseInvalidStates, TestSubscriptionResume, TestSubscriptionResumeInvalidStates, pos, config, TestPauseReasonEnum, TestPauseProperties, TestWaitIfPaused, TestConcurrentPauseResume, TestMultiplePauseResumeCycles, TestPauseResumeModuleImports
    from .test_pause_resume_subscriber import MockSubscriber, mock_subscriber

    __all__ = ["subscription", "TestSubscriptionPause", "TestSubscriptionPauseInvalidStates", "TestSubscriptionResume", "TestSubscriptionResumeInvalidStates", "pos", "config", "TestPauseReasonEnum", "TestPauseProperties", "TestWaitIfPaused", "TestConcurrentPauseResume", "TestMultiplePauseResumeCycles", "TestPauseResumeModuleImports", "MockSubscriber", "mock_subscriber"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
