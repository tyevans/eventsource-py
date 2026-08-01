"""Migration port: the pure contract half of live-migration persistence.

A subpackage rather than a flat `ports/migration.py`, following the
`ports/readmodels/` precedent: the data models (`Migration`, `TenantRouting`,
`PositionMapping`, `MigrationAuditEntry`, and their supporting enums) and the
four repository Protocols that operate on them are colocated here because
the Protocol signatures reference the model types and ports must not import
application.

The adapter half (PostgreSQL implementations) lives under
`eventsource.adapters.sql.migration`. The use cases that consume these ports
live under `eventsource.application.migration`.
"""

from eventsource.ports.migration.models import (
    AuditEventType,
    CutoverResult,
    Migration,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    MigrationStatus,
    PositionMapping,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.ports.migration.repositories import (
    MigrationAuditLogRepository,
    MigrationRepository,
    PositionMappingRepository,
    TenantRoutingRepository,
)

__all__ = [
    "AuditEventType",
    "CutoverResult",
    "Migration",
    "MigrationAuditEntry",
    "MigrationAuditLogRepository",
    "MigrationConfig",
    "MigrationPhase",
    "MigrationRepository",
    "MigrationResult",
    "MigrationStatus",
    "PositionMapping",
    "PositionMappingRepository",
    "SyncLag",
    "TenantMigrationState",
    "TenantRouting",
    "TenantRoutingRepository",
]
