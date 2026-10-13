"""Shared fixtures and test helpers for WritePauseManager unit tests."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.write_pause import WritePauseManager


@pytest.fixture
def tenant_id() -> UUID:
    """Generate a test tenant ID."""
    return uuid4()


@pytest.fixture
def manager() -> WritePauseManager:
    """Create a default WritePauseManager instance."""
    return WritePauseManager()
