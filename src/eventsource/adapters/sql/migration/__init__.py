"""
PostgreSQL repository implementations for the migration system.

This submodule provides data access repositories for migration state,
tenant routing, position mappings, and audit logs. The Protocols these
implement live in `eventsource.ports.migration.repositories`.

Repositories:
    - PostgreSQLMigrationRepository
    - PostgreSQLTenantRoutingRepository
    - PostgreSQLPositionMappingRepository
    - PostgreSQLMigrationAuditLogRepository

Usage:
    >>> from eventsource.adapters.sql.migration import (
    ...     PostgreSQLMigrationRepository,
    ...     PostgreSQLTenantRoutingRepository,
    ...     PostgreSQLPositionMappingRepository,
    ...     PostgreSQLMigrationAuditLogRepository,
    ... )
    >>>
    >>> migration_repo = PostgreSQLMigrationRepository(conn)
    >>> routing_repo = PostgreSQLTenantRoutingRepository(session_factory)
    >>> position_repo = PostgreSQLPositionMappingRepository(session_factory)
    >>> audit_repo = PostgreSQLMigrationAuditLogRepository(conn)

See Also:
    - Task: P1-003-migration-repository.md
    - Task: P1-004-routing-repository.md
    - Task: P3-001-position-mapping-repository.md
    - Task: P4-001-audit-log.md
"""

from eventsource.adapters.sql.migration.audit_log import (
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.adapters.sql.migration.migration import (
    VALID_TRANSITIONS,
    PostgreSQLMigrationRepository,
)
from eventsource.adapters.sql.migration.position_mapping import (
    PostgreSQLPositionMappingRepository,
)
from eventsource.adapters.sql.migration.routing import (
    PostgreSQLTenantRoutingRepository,
)

__all__ = [
    "PostgreSQLMigrationRepository",
    "PostgreSQLTenantRoutingRepository",
    "PostgreSQLPositionMappingRepository",
    "PostgreSQLMigrationAuditLogRepository",
    "VALID_TRANSITIONS",
]
