---
id: REFACTOR-tests-unit-application-migration-test_cutover_manager
title: Refactor and Decompose Legacy File test_cutover_manager.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_cutover_manager: Refactor Legacy File test_cutover_manager.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_cutover_manager.py` contains 1487 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_cutover_manager_lock.py, test_cutover_manager_mock.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_cutover_manager/` with submodules:
- `test_cutover_manager_lock.py`: mock_lock_manager, create_lock_context_manager, create_failing_lock_context_manager, TestLockAcquisition, tenant_id, migration_id, target_store_id, config, strict_config, cutover_manager, TestCutoverManagerInit, TestSuccessfulCutover, TestStrictZeroLagDefault, TestSyncLagValidation, TestTimeoutEnforcement, TestAutomaticRollback, TestWritePauseResume, TestTargetStoreVerification, TestRoutingStateTransitions, TestCutoverReadinessValidation, TestCutoverResult, TestEdgeCases
- `test_cutover_manager_mock.py`: mock_router, mock_routing_repo, mock_lag_tracker

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_cutover_manager.py (1487 lines):
  Submodule 'test_cutover_manager_lock.py' (~1285 lines):
    - [function] mock_lock_manager (lines 59-74)
    - [function] create_lock_context_manager (lines 149-164)
    - [function] create_failing_lock_context_manager (lines 167-178)
    - [class] TestLockAcquisition (lines 558-657)
    - [function] tenant_id (lines 41-43)
    - [function] migration_id (lines 47-49)
    - [function] target_store_id (lines 53-55)
    - [function] config (lines 132-137)
    - [function] strict_config (lines 141-146)
    - [function] cutover_manager (lines 182-194)
    - [class] TestCutoverManagerInit (lines 202-254)
    - [class] TestSuccessfulCutover (lines 262-439)
    - [class] TestStrictZeroLagDefault (lines 442-550)
    - [class] TestSyncLagValidation (lines 665-765)
    - [class] TestTimeoutEnforcement (lines 773-848)
    - [class] TestAutomaticRollback (lines 856-1005)
    - [class] TestWritePauseResume (lines 1013-1086)
    - [class] TestTargetStoreVerification (lines 1094-1138)
    - [class] TestRoutingStateTransitions (lines 1146-1216)
    - [class] TestCutoverReadinessValidation (lines 1224-1350)
    - [class] TestCutoverResult (lines 1358-1394)
    - [class] TestEdgeCases (lines 1402-1487)
  Submodule 'test_cutover_manager_mock.py' (~45 lines):
    - [function] mock_router (lines 78-91)
    - [function] mock_routing_repo (lines 95-110)
    - [function] mock_lag_tracker (lines 114-128)
  Suggested barrel exports:
    from .test_cutover_manager_lock import mock_lock_manager, create_lock_context_manager, create_failing_lock_context_manager, TestLockAcquisition, tenant_id, migration_id, target_store_id, config, strict_config, cutover_manager, TestCutoverManagerInit, TestSuccessfulCutover, TestStrictZeroLagDefault, TestSyncLagValidation, TestTimeoutEnforcement, TestAutomaticRollback, TestWritePauseResume, TestTargetStoreVerification, TestRoutingStateTransitions, TestCutoverReadinessValidation, TestCutoverResult, TestEdgeCases
    from .test_cutover_manager_mock import mock_router, mock_routing_repo, mock_lag_tracker

    __all__ = ["mock_lock_manager", "create_lock_context_manager", "create_failing_lock_context_manager", "TestLockAcquisition", "tenant_id", "migration_id", "target_store_id", "config", "strict_config", "cutover_manager", "TestCutoverManagerInit", "TestSuccessfulCutover", "TestStrictZeroLagDefault", "TestSyncLagValidation", "TestTimeoutEnforcement", "TestAutomaticRollback", "TestWritePauseResume", "TestTargetStoreVerification", "TestRoutingStateTransitions", "TestCutoverReadinessValidation", "TestCutoverResult", "TestEdgeCases", "mock_router", "mock_routing_repo", "mock_lag_tracker"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
