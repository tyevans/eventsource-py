"""Unit tests for schema generation utilities."""

from __future__ import annotations

from tests.unit.readmodels.test_schema._fixtures import (
    AllTypesModel,
    ComplexModel,
    CustomTableModel,
    IndexedModel,
    ModelWithCustomSqlType,
    ModelWithDefaults,
    SimpleModel,
)
from tests.unit.readmodels.test_schema.test_schema_generation import (
    TestGenerateFullSchema,
    TestGenerateIndexes,
    TestGenerateSchema,
)
from tests.unit.readmodels.test_schema.test_schema_helpers import (
    TestExtractType,
    TestFormatDefault,
    TestGetCustomSqlType,
    TestIntegration,
    TestIsOptional,
    TestTypeMaps,
)

__all__ = [
    "AllTypesModel",  # pragma: allowlist secret
    "ComplexModel",  # pragma: allowlist secret
    "CustomTableModel",  # pragma: allowlist secret
    "IndexedModel",  # pragma: allowlist secret
    "ModelWithCustomSqlType",  # pragma: allowlist secret
    "ModelWithDefaults",  # pragma: allowlist secret
    "SimpleModel",  # pragma: allowlist secret
    "TestExtractType",  # pragma: allowlist secret
    "TestFormatDefault",  # pragma: allowlist secret
    "TestGenerateFullSchema",  # pragma: allowlist secret
    "TestGenerateIndexes",  # pragma: allowlist secret
    "TestGenerateSchema",  # pragma: allowlist secret
    "TestGetCustomSqlType",  # pragma: allowlist secret
    "TestIntegration",  # pragma: allowlist secret
    "TestIsOptional",  # pragma: allowlist secret
    "TestTypeMaps",  # pragma: allowlist secret
]
