"""Unit tests for projection tracing support (O11Y-013)."""

from __future__ import annotations

from .test_projection_tracing_checkpoint import (
    TestCheckpointTrackingProjectionTracing,
)
from .test_projection_tracing_order import (
    OrderCreated,
    OrderShipped,
    TestBackwardCompatibility,
    TestDatabaseProjectionTracing,
    TestDeclarativeProjectionTracing,
    TestProjectionCoordinatorTracing,
    TestProjectionRegistryTracing,
)

__all__ = [
    "OrderCreated",
    "OrderShipped",
    "TestBackwardCompatibility",
    "TestCheckpointTrackingProjectionTracing",
    "TestDatabaseProjectionTracing",
    "TestDeclarativeProjectionTracing",
    "TestProjectionCoordinatorTracing",
    "TestProjectionRegistryTracing",
]  # pragma: allowlist secret
