"""Unit tests for PostgreSQLReadModelRepository."""

from __future__ import annotations

from tests.unit.readmodels.test_postgresql.test_postgresql_model import (
    CustomTableModel,
    OrderSummary,
    TestAsyncOperations,
    TestFilterToSQL,
    TestModelClassProperty,
    TestPostgreSQLReadModelRepositoryConstruction,
    TestRowToModel,
    TestSQLSecurityAnnotations,
    TestTracingConfiguration,
)
from tests.unit.readmodels.test_postgresql.test_postgresql_query import (
    TestBuildCountQuery,
    TestBuildSelectQuery,
    TestQueryBuilderIntegration,
)

__all__ = [
    "CustomTableModel",  # pragma: allowlist secret
    "OrderSummary",  # pragma: allowlist secret
    "TestAsyncOperations",  # pragma: allowlist secret
    "TestBuildCountQuery",  # pragma: allowlist secret
    "TestBuildSelectQuery",  # pragma: allowlist secret
    "TestFilterToSQL",  # pragma: allowlist secret
    "TestModelClassProperty",  # pragma: allowlist secret
    "TestPostgreSQLReadModelRepositoryConstruction",  # pragma: allowlist secret
    "TestQueryBuilderIntegration",  # pragma: allowlist secret
    "TestRowToModel",  # pragma: allowlist secret
    "TestSQLSecurityAnnotations",  # pragma: allowlist secret
    "TestTracingConfiguration",  # pragma: allowlist secret
]
