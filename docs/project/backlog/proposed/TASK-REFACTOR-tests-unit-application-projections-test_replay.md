---
id: REFACTOR-tests-unit-application-projections-test_replay
title: Refactor and Decompose Legacy File test_replay.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-projections-test_replay: Refactor Legacy File test_replay.py

## Summary
The grandfathered debt file `tests/unit/application/projections/test_replay.py` contains 592 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_replay_the.py, test_replay_feed.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/projections/test_replay/` with submodules:
- `test_replay_the.py`: TestAPoisonEventDoesNotStopTheRebuild, TestStrictRaisesOnTheFirstRejection, TestTheFailureListIsBoundedAndSaysSo, TestTheReadIsBounded, TestScopingReachesTheAdaptersQuery, TestTheScopedReplaySeesOnlyThatSlice, OrderPlaced, InvoiceIssued, Collecting, RejectsEverything, RejectsOne, _envelope, _append, _store_with, TestFailedCountsEventsAndFailuresCountRejections, TestMaxEventsGuardsAgainstANonAdvancingCursor
- `test_replay_feed.py`: RecordingFeed, NonAdvancingFeed, PositionlessFeed

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/projections/test_replay.py (592 lines):
  Submodule 'test_replay_the.py' (~448 lines):
    - [class] TestAPoisonEventDoesNotStopTheRebuild (lines 181-226)
    - [class] TestStrictRaisesOnTheFirstRejection (lines 294-334)
    - [class] TestTheFailureListIsBoundedAndSaysSo (lines 337-398)
    - [class] TestTheReadIsBounded (lines 401-451)
    - [class] TestScopingReachesTheAdaptersQuery (lines 454-518)
    - [class] TestTheScopedReplaySeesOnlyThatSlice (lines 521-579)
    - [class] OrderPlaced (lines 51-52)
    - [class] InvoiceIssued (lines 56-57)
    - [class] Collecting (lines 60-67)
    - [class] RejectsEverything (lines 70-77)
    - [class] RejectsOne (lines 80-90)
    - [function] _envelope (lines 156-163)
    - [function] _append (lines 166-171)
    - [function] _store_with (lines 174-178)
    - [class] TestFailedCountsEventsAndFailuresCountRejections (lines 229-291)
    - [class] TestMaxEventsGuardsAgainstANonAdvancingCursor (lines 582-592)
  Submodule 'test_replay_feed.py' (~57 lines):
    - [class] RecordingFeed (lines 93-109)
    - [class] NonAdvancingFeed (lines 112-129)
    - [class] PositionlessFeed (lines 132-153)
  Suggested barrel exports:
    from .test_replay_the import TestAPoisonEventDoesNotStopTheRebuild, TestStrictRaisesOnTheFirstRejection, TestTheFailureListIsBoundedAndSaysSo, TestTheReadIsBounded, TestScopingReachesTheAdaptersQuery, TestTheScopedReplaySeesOnlyThatSlice, OrderPlaced, InvoiceIssued, Collecting, RejectsEverything, RejectsOne, _envelope, _append, _store_with, TestFailedCountsEventsAndFailuresCountRejections, TestMaxEventsGuardsAgainstANonAdvancingCursor
    from .test_replay_feed import RecordingFeed, NonAdvancingFeed, PositionlessFeed

    __all__ = ["TestAPoisonEventDoesNotStopTheRebuild", "TestStrictRaisesOnTheFirstRejection", "TestTheFailureListIsBoundedAndSaysSo", "TestTheReadIsBounded", "TestScopingReachesTheAdaptersQuery", "TestTheScopedReplaySeesOnlyThatSlice", "OrderPlaced", "InvoiceIssued", "Collecting", "RejectsEverything", "RejectsOne", "_envelope", "_append", "_store_with", "TestFailedCountsEventsAndFailuresCountRejections", "TestMaxEventsGuardsAgainstANonAdvancingCursor", "RecordingFeed", "NonAdvancingFeed", "PositionlessFeed"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
