"""
Data models for the multi-tenant live migration system.

This package defines the core data structures used throughout the migration
system, including migration state, tenant routing, position mappings, and
status tracking.
"""

from __future__ import annotations

from eventsource.ports.migration.models.audit import (
    AuditEventType,
    MigrationAuditEntry,
)
from eventsource.ports.migration.models.config import (
    MigrationConfig,
)
from eventsource.ports.migration.models.migration import (
    Migration,
)
from eventsource.ports.migration.models.phases import (
    MigrationPhase,
    TenantMigrationState,
)
from eventsource.ports.migration.models.routing import (
    CutoverResult,
    PositionMapping,
    SyncLag,
    TenantRouting,
)
from eventsource.ports.migration.models.status import (
    MigrationResult,
    MigrationStatus,
)

__all__ = [
    "AuditEventType",
    "CutoverResult",
    "Migration",
    "MigrationAuditEntry",
    "MigrationConfig",
    "MigrationPhase",
    "MigrationResult",
    "MigrationStatus",
    "PositionMapping",
    "SyncLag",
    "TenantMigrationState",
    "TenantRouting",
]
