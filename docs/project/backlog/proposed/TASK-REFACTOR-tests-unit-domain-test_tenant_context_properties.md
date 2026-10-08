---
id: REFACTOR-tests-unit-domain-test_tenant_context_properties
title: Refactor and Decompose Legacy File test_tenant_context_properties.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_tenant_context_properties: Refactor Legacy File test_tenant_context_properties.py

## Summary
The grandfathered debt file `tests/unit/domain/test_tenant_context_properties.py` contains 519 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_tenant_context_properties_restores.py, test_tenant_context_properties_scope.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_tenant_context_properties/` with submodules:
- `test_tenant_context_properties_restores.py`: test_sync_nesting_restores_exactly, test_async_nesting_restores_exactly, test_sync_exception_at_any_depth_restores_context, test_async_exception_at_any_depth_restores_context, test_abandoned_async_scope_mid_await_still_restores_on_cancellation, _clear_context, test_concurrent_tasks_never_observe_others_tenant, test_concurrency_isolation_test_actually_detects_breakage, _BoomError, test_get_required_tenant_raises_documented_exception_type_and_message, test_unclosed_sync_generator_leaves_context_set, test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state, test_reset_tenant_context_rejects_out_of_lifo_order, test_reset_tenant_context_rejects_double_reset, test_reset_sequence_is_lifo_or_raises, test_copy_context_run_is_isolated_from_caller
- `test_tenant_context_properties_scope.py`: test_clear_inside_scope_makes_scope_exit_raise, test_clear_inside_nested_scope_makes_inner_scope_exit_raise, test_clear_inside_async_scope_makes_scope_exit_raise, test_get_required_tenant_raises_after_scope_exit

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/domain/test_tenant_context_properties.py (519 lines):
  Submodule 'test_tenant_context_properties_restores.py' (~333 lines):
    - [function] test_sync_nesting_restores_exactly (lines 70-86)
    - [function] test_async_nesting_restores_exactly (lines 91-106)
    - [function] test_sync_exception_at_any_depth_restores_context (lines 198-215)
    - [function] test_async_exception_at_any_depth_restores_context (lines 220-236)
    - [function] test_abandoned_async_scope_mid_await_still_restores_on_cancellation (lines 466-491)
    - [function] _clear_context (lines 56-60)
    - [function] test_concurrent_tasks_never_observe_others_tenant (lines 120-144)
    - [function] test_concurrency_isolation_test_actually_detects_breakage (lines 147-184)
    - [class] _BoomError (lines 192-193)
    - [function] test_get_required_tenant_raises_documented_exception_type_and_message (lines 290-301)
    - [function] test_unclosed_sync_generator_leaves_context_set (lines 317-345)
    - [function] test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state (lines 348-383)
    - [function] test_reset_tenant_context_rejects_out_of_lifo_order (lines 386-407)
    - [function] test_reset_tenant_context_rejects_double_reset (lines 410-422)
    - [function] test_reset_sequence_is_lifo_or_raises (lines 427-463)
    - [function] test_copy_context_run_is_isolated_from_caller (lines 500-519)
  Submodule 'test_tenant_context_properties_scope.py' (~41 lines):
    - [function] test_clear_inside_scope_makes_scope_exit_raise (lines 244-255)
    - [function] test_clear_inside_nested_scope_makes_inner_scope_exit_raise (lines 258-269)
    - [function] test_clear_inside_async_scope_makes_scope_exit_raise (lines 272-282)
    - [function] test_get_required_tenant_raises_after_scope_exit (lines 304-309)
  Suggested barrel exports:
    from .test_tenant_context_properties_restores import test_sync_nesting_restores_exactly, test_async_nesting_restores_exactly, test_sync_exception_at_any_depth_restores_context, test_async_exception_at_any_depth_restores_context, test_abandoned_async_scope_mid_await_still_restores_on_cancellation, _clear_context, test_concurrent_tasks_never_observe_others_tenant, test_concurrency_isolation_test_actually_detects_breakage, _BoomError, test_get_required_tenant_raises_documented_exception_type_and_message, test_unclosed_sync_generator_leaves_context_set, test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state, test_reset_tenant_context_rejects_out_of_lifo_order, test_reset_tenant_context_rejects_double_reset, test_reset_sequence_is_lifo_or_raises, test_copy_context_run_is_isolated_from_caller
    from .test_tenant_context_properties_scope import test_clear_inside_scope_makes_scope_exit_raise, test_clear_inside_nested_scope_makes_inner_scope_exit_raise, test_clear_inside_async_scope_makes_scope_exit_raise, test_get_required_tenant_raises_after_scope_exit

    __all__ = ["test_sync_nesting_restores_exactly", "test_async_nesting_restores_exactly", "test_sync_exception_at_any_depth_restores_context", "test_async_exception_at_any_depth_restores_context", "test_abandoned_async_scope_mid_await_still_restores_on_cancellation", "_clear_context", "test_concurrent_tasks_never_observe_others_tenant", "test_concurrency_isolation_test_actually_detects_breakage", "_BoomError", "test_get_required_tenant_raises_documented_exception_type_and_message", "test_unclosed_sync_generator_leaves_context_set", "test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state", "test_reset_tenant_context_rejects_out_of_lifo_order", "test_reset_tenant_context_rejects_double_reset", "test_reset_sequence_is_lifo_or_raises", "test_copy_context_run_is_isolated_from_caller", "test_clear_inside_scope_makes_scope_exit_raise", "test_clear_inside_nested_scope_makes_inner_scope_exit_raise", "test_clear_inside_async_scope_makes_scope_exit_raise", "test_get_required_tenant_raises_after_scope_exit"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
