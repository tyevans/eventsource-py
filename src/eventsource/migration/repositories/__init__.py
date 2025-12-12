"""
Repository implementations for the migration system.

This submodule provides data access repositories for migration state,
tenant routing, position mappings, and audit logs. Each repository has
both a protocol (interface) and PostgreSQL implementation.

Repositories:
    - MigrationRepository: CRUD operations for Migration entities
    - PostgreSQLMigrationRepository: PostgreSQL implementation
    - TenantRoutingRepository: Tenant-to-store routing configuration
    - PositionMappingRepository: Source-to-target position mappings
    - MigrationAuditLogRepository: Audit trail for migration operations

Usage:
    >>> from eventsource.migration.repositories import (
    ...     MigrationRepository,
    ...     PostgreSQLMigrationRepository,
    ...     TenantRoutingRepository,
    ...     PositionMappingRepository,
    ...     MigrationAuditLogRepository,
    ...     PostgreSQLMigrationAuditLogRepository,
    ... )
    >>>
    >>> migration_repo = PostgreSQLMigrationRepository(conn)
    >>> routing_repo = TenantRoutingRepository(session_factory)
    >>> position_repo = PositionMappingRepository(session_factory)
    >>> audit_repo = PostgreSQLMigrationAuditLogRepository(conn)

See Also:
    - Task: P1-003-migration-repository.md
    - Task: P1-004-routing-repository.md
    - Task: P3-001-position-mapping-repository.md
    - Task: P4-001-audit-log.md
"""

from eventsource.migration.repositories.audit_log import (
    MigrationAuditLogRepository,
    MigrationAuditLogRepositoryProtocol,
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.migration.repositories.migration import (
    VALID_TRANSITIONS,
    MigrationRepository,
    MigrationRepositoryProtocol,
    PostgreSQLMigrationRepository,
)
from eventsource.migration.repositories.position_mapping import (
    PositionMappingRepository,
    PositionMappingRepositoryProtocol,
    PostgreSQLPositionMappingRepository,
)
from eventsource.migration.repositories.routing import (
    PostgreSQLTenantRoutingRepository,
    TenantRoutingRepository,
    TenantRoutingRepositoryProtocol,
)

__all__ = [
    "MigrationRepository",
    "MigrationRepositoryProtocol",
    "PostgreSQLMigrationRepository",
    "TenantRoutingRepository",
    "TenantRoutingRepositoryProtocol",
    "PostgreSQLTenantRoutingRepository",
    "PositionMappingRepository",
    "PositionMappingRepositoryProtocol",
    "PostgreSQLPositionMappingRepository",
    "MigrationAuditLogRepository",
    "MigrationAuditLogRepositoryProtocol",
    "PostgreSQLMigrationAuditLogRepository",
    "VALID_TRANSITIONS",
]
