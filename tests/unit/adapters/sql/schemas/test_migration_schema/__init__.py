"""Unit tests package for migration schema template."""

from __future__ import annotations

from .test_mappings_audit_and_constraints import (
    TestMigrationAuditLogTable,
    TestMigrationPositionMappingsTable,
    TestMigrationPositionTokenColumns,
    TestSchemaComments,
    TestSchemaIdempotency,
    TestSchemaTriggers,
)
from .test_tables import (
    TestMigrationSchemaTemplate,
    TestTenantMigrationsTable,
    TestTenantRoutingTable,
)

__all__ = [
    "TestMigrationAuditLogTable",
    "TestMigrationPositionMappingsTable",
    "TestMigrationPositionTokenColumns",
    "TestMigrationSchemaTemplate",
    "TestSchemaComments",
    "TestSchemaIdempotency",
    "TestSchemaTriggers",
    "TestTenantMigrationsTable",
    "TestTenantRoutingTable",
]
