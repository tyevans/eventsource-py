---
id: REFACTOR-tests-unit-application-migration-test_status_streamer
title: Refactor and Decompose Legacy File test_status_streamer.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_status_streamer: Refactor Legacy File test_status_streamer.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_status_streamer.py` contains 963 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_status_streamer_manager.py, test_status_streamer_init.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_status_streamer/` with submodules:
- `test_status_streamer_manager.py`: TestStatusStreamManagerInit, TestStatusStreamManagerGetStreamer, TestStatusStreamManagerClose, TestStatusStreamManagerCleanup, TestStatusStreamerStreamStatus, TestStatusStreamerStatusChanged, TestStatusStreamerMultipleSubscribers, TestStatusStreamerClose, TestCoordinatorIntegration
- `test_status_streamer_init.py`: TestStatusStreamerInit

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_status_streamer.py (963 lines):
  Submodule 'test_status_streamer_manager.py' (~852 lines):
    - [class] TestStatusStreamManagerInit (lines 672-694)
    - [class] TestStatusStreamManagerGetStreamer (lines 697-728)
    - [class] TestStatusStreamManagerClose (lines 731-776)
    - [class] TestStatusStreamManagerCleanup (lines 779-864)
    - [class] TestStatusStreamerStreamStatus (lines 96-318)
    - [class] TestStatusStreamerStatusChanged (lines 321-521)
    - [class] TestStatusStreamerMultipleSubscribers (lines 524-633)
    - [class] TestStatusStreamerClose (lines 636-669)
    - [class] TestCoordinatorIntegration (lines 867-963)
  Submodule 'test_status_streamer_init.py' (~59 lines):
    - [class] TestStatusStreamerInit (lines 35-93)
  Suggested barrel exports:
    from .test_status_streamer_manager import TestStatusStreamManagerInit, TestStatusStreamManagerGetStreamer, TestStatusStreamManagerClose, TestStatusStreamManagerCleanup, TestStatusStreamerStreamStatus, TestStatusStreamerStatusChanged, TestStatusStreamerMultipleSubscribers, TestStatusStreamerClose, TestCoordinatorIntegration
    from .test_status_streamer_init import TestStatusStreamerInit

    __all__ = ["TestStatusStreamManagerInit", "TestStatusStreamManagerGetStreamer", "TestStatusStreamManagerClose", "TestStatusStreamManagerCleanup", "TestStatusStreamerStreamStatus", "TestStatusStreamerStatusChanged", "TestStatusStreamerMultipleSubscribers", "TestStatusStreamerClose", "TestCoordinatorIntegration", "TestStatusStreamerInit"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
