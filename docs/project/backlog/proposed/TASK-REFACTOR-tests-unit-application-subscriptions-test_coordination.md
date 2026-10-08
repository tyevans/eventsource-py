---
id: REFACTOR-tests-unit-application-subscriptions-test_coordination
title: Refactor and Decompose Legacy File test_coordination.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_coordination: Refactor Legacy File test_coordination.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_coordination.py` contains 1499 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_coordination_leader.py, test_coordination_elector.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_coordination/` with submodules:
- `test_coordination_leader.py`: TestLeaderElectorProtocol, TestInMemoryLeaderElector, TestSharedLeaderState, TestModuleExports, TestModuleExportsWithInMemory, TestTopicConstants, TestShutdownIntent, TestShutdownNotification, TestHeartbeatMessage, TestWorkAssignment, TestPeerInfo, TestWorkRedistributionCoordinator, TestModuleExportsP3003
- `test_coordination_elector.py`: TestMockElectorBehavior

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_coordination.py (1499 lines):
  Submodule 'test_coordination_leader.py' (~1348 lines):
    - [class] TestLeaderElectorProtocol (lines 36-116)
    - [class] TestInMemoryLeaderElector (lines 237-520)
    - [class] TestSharedLeaderState (lines 523-542)
    - [class] TestModuleExports (lines 119-148)
    - [class] TestModuleExportsWithInMemory (lines 545-581)
    - [class] TestTopicConstants (lines 589-606)
    - [class] TestShutdownIntent (lines 609-622)
    - [class] TestShutdownNotification (lines 625-766)
    - [class] TestHeartbeatMessage (lines 769-867)
    - [class] TestWorkAssignment (lines 870-944)
    - [class] TestPeerInfo (lines 947-1018)
    - [class] TestWorkRedistributionCoordinator (lines 1022-1430)
    - [class] TestModuleExportsP3003 (lines 1433-1499)
  Submodule 'test_coordination_elector.py' (~83 lines):
    - [class] TestMockElectorBehavior (lines 152-234)
  Suggested barrel exports:
    from .test_coordination_leader import TestLeaderElectorProtocol, TestInMemoryLeaderElector, TestSharedLeaderState, TestModuleExports, TestModuleExportsWithInMemory, TestTopicConstants, TestShutdownIntent, TestShutdownNotification, TestHeartbeatMessage, TestWorkAssignment, TestPeerInfo, TestWorkRedistributionCoordinator, TestModuleExportsP3003
    from .test_coordination_elector import TestMockElectorBehavior

    __all__ = ["TestLeaderElectorProtocol", "TestInMemoryLeaderElector", "TestSharedLeaderState", "TestModuleExports", "TestModuleExportsWithInMemory", "TestTopicConstants", "TestShutdownIntent", "TestShutdownNotification", "TestHeartbeatMessage", "TestWorkAssignment", "TestPeerInfo", "TestWorkRedistributionCoordinator", "TestModuleExportsP3003", "TestMockElectorBehavior"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
