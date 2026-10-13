"""Pytest fixtures for final migration integration tests."""

from __future__ import annotations

from tests.unit.application.migration.test_final_integration._fixtures import (
    audit_log_repo,
    checkpoint_repo,
    cleanup_metrics,
    lock_manager,
    migration_id,
    migration_repo,
    position_mapper,
    position_mapping_repo,
    router,
    routing_repo,
    source_store,
    target_store,
    tenant_id,
    write_pause_manager,
)

__all__ = [  # pragma: allowlist secret
    "audit_log_repo",
    "checkpoint_repo",
    "cleanup_metrics",
    "lock_manager",
    "migration_id",
    "migration_repo",
    "position_mapper",
    "position_mapping_repo",
    "router",
    "routing_repo",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
