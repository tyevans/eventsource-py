"""Pytest configuration and fixture registration for Phase 3 integration tests."""

from __future__ import annotations

from tests.unit.application.migration.test_phase3_integration._fixtures import (
    checkpoint_repo,
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
    "checkpoint_repo",
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
