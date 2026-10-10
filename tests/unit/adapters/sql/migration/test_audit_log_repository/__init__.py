"""Migration audit log repository unit tests package."""

from __future__ import annotations

from .test_helpers_and_factories import (
    TestAuditEventTypeEnum,
    TestMigrationAuditEntryFactoryMethods,
    TestPostgreSQLMigrationAuditLogRepositoryHelpers,
)
from .test_queries_and_counts import (
    TestPostgreSQLMigrationAuditLogRepositoryCountByMigration,
    TestPostgreSQLMigrationAuditLogRepositoryGetByMigration,
    TestPostgreSQLMigrationAuditLogRepositoryGetLatest,
)
from .test_record_and_get import (
    TestMigrationAuditLogRepositoryProtocol,
    TestPostgreSQLMigrationAuditLogRepositoryGetById,
    TestPostgreSQLMigrationAuditLogRepositoryInit,
    TestPostgreSQLMigrationAuditLogRepositoryRecord,
)

__all__ = [
    "TestAuditEventTypeEnum",
    "TestMigrationAuditEntryFactoryMethods",
    "TestMigrationAuditLogRepositoryProtocol",
    "TestPostgreSQLMigrationAuditLogRepositoryCountByMigration",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryGetById",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryGetByMigration",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryGetLatest",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryHelpers",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryInit",  # pragma: allowlist secret
    "TestPostgreSQLMigrationAuditLogRepositoryRecord",  # pragma: allowlist secret
]
