"""Pytest configuration and shared fixtures for projection coordinator tests."""

from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    projection_coordinator,
    projection_registry,
    subscriber_registry,
)

__all__ = [  # pragma: allowlist secret
    "projection_coordinator",
    "projection_registry",
    "subscriber_registry",
]
