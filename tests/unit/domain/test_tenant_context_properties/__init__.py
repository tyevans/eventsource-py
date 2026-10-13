"""Barrel export facade for tenant_context_properties tests."""

from __future__ import annotations

from .test_tenant_context_properties_reset import (
    test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state,
    test_reset_sequence_is_lifo_or_raises,
    test_reset_tenant_context_rejects_double_reset,
    test_reset_tenant_context_rejects_out_of_lifo_order,
    test_unclosed_sync_generator_leaves_context_set,
)
from .test_tenant_context_properties_restores import (
    _POOL,
    _BoomError,
    _clear_context,
    tenant_id_strategy,
    test_abandoned_async_scope_mid_await_still_restores_on_cancellation,
    test_async_exception_at_any_depth_restores_context,
    test_async_nesting_restores_exactly,
    test_concurrency_isolation_test_actually_detects_breakage,
    test_concurrent_tasks_never_observe_others_tenant,
    test_copy_context_run_is_isolated_from_caller,
    test_get_required_tenant_raises_documented_exception_type_and_message,
    test_sync_exception_at_any_depth_restores_context,
    test_sync_nesting_restores_exactly,
)
from .test_tenant_context_properties_scope import (
    test_clear_inside_async_scope_makes_scope_exit_raise,
    test_clear_inside_nested_scope_makes_inner_scope_exit_raise,
    test_clear_inside_scope_makes_scope_exit_raise,
    test_get_required_tenant_raises_after_scope_exit,
)

__all__ = [  # pragma: allowlist secret
    "_BoomError",
    "_clear_context",
    "_POOL",
    "tenant_id_strategy",
    "test_abandoned_async_scope_mid_await_still_restores_on_cancellation",
    "test_async_exception_at_any_depth_restores_context",
    "test_async_nesting_restores_exactly",
    "test_clear_inside_async_scope_makes_scope_exit_raise",
    "test_clear_inside_nested_scope_makes_inner_scope_exit_raise",
    "test_clear_inside_scope_makes_scope_exit_raise",
    "test_concurrency_isolation_test_actually_detects_breakage",
    "test_concurrent_tasks_never_observe_others_tenant",
    "test_copy_context_run_is_isolated_from_caller",
    "test_get_required_tenant_raises_after_scope_exit",
    "test_get_required_tenant_raises_documented_exception_type_and_message",
    "test_raw_contextvars_token_reset_out_of_lifo_order_silently_corrupts_state",
    "test_reset_sequence_is_lifo_or_raises",
    "test_reset_tenant_context_rejects_double_reset",
    "test_reset_tenant_context_rejects_out_of_lifo_order",
    "test_sync_exception_at_any_depth_restores_context",
    "test_sync_nesting_restores_exactly",
    "test_unclosed_sync_generator_leaves_context_set",
]
