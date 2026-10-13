"""Unit tests for SQLiteReadModelRepository."""

from __future__ import annotations

from tests.unit.readmodels.test_sqlite._fixtures import (
    CustomTableModel,
    OrderSummary,
)
from tests.unit.readmodels.test_sqlite.test_sqlite_construction import (
    TestFilterToSQL,
    TestModelClassProperty,
    TestModelToValues,
    TestRowToModel,
    TestSQLiteReadModelRepositoryConstruction,
)
from tests.unit.readmodels.test_sqlite.test_sqlite_operations import (
    TestAsyncOperations,
    TestExportFromModule,
    TestSQLSecurityAnnotations,
    TestTracingConfiguration,
)
from tests.unit.readmodels.test_sqlite.test_sqlite_queries import (
    TestBuildCountQuery,
    TestBuildSelectQuery,
    TestQueryBuilderIntegration,
    TestSQLiteSyntaxDifferences,
)

__all__ = [
    "CustomTableModel",  # pragma: allowlist secret
    "OrderSummary",  # pragma: allowlist secret
    "TestAsyncOperations",  # pragma: allowlist secret
    "TestBuildCountQuery",  # pragma: allowlist secret
    "TestBuildSelectQuery",  # pragma: allowlist secret
    "TestExportFromModule",  # pragma: allowlist secret
    "TestFilterToSQL",  # pragma: allowlist secret
    "TestModelClassProperty",  # pragma: allowlist secret
    "TestModelToValues",  # pragma: allowlist secret
    "TestQueryBuilderIntegration",  # pragma: allowlist secret
    "TestRowToModel",  # pragma: allowlist secret
    "TestSQLSecurityAnnotations",  # pragma: allowlist secret
    "TestSQLiteReadModelRepositoryConstruction",  # pragma: allowlist secret
    "TestSQLiteSyntaxDifferences",  # pragma: allowlist secret
    "TestTracingConfiguration",  # pragma: allowlist secret
]
