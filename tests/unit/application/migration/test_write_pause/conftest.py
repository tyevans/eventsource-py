"""Pytest fixtures for WritePauseManager tests."""

from __future__ import annotations

from tests.unit.application.migration.test_write_pause._fixtures import (
    manager,
    tenant_id,
)

__all__ = [  # pragma: allowlist secret
    "manager",
    "tenant_id",
]
