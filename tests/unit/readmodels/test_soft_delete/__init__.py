"""Unit tests for soft delete functionality."""

from tests.unit.readmodels.test_soft_delete._fixtures import TestModel, repo
from tests.unit.readmodels.test_soft_delete.test_soft_delete_lifecycle import (
    TestRestore,
    TestSoftDelete,
    TestSoftDeleteCycle,
)
from tests.unit.readmodels.test_soft_delete.test_soft_delete_queries import (
    TestCountExcludesSoftDeleted,
    TestExistsExcludesSoftDeleted,
    TestFindDeleted,
    TestFindExcludesSoftDeleted,
    TestGetDeleted,
    TestGetExcludesSoftDeleted,
    TestGetManyExcludesSoftDeleted,
    TestTruncateRemovesSoftDeleted,
)

__all__ = [  # pragma: allowlist secret
    "TestCountExcludesSoftDeleted",
    "TestExistsExcludesSoftDeleted",
    "TestFindDeleted",
    "TestFindExcludesSoftDeleted",
    "TestGetDeleted",
    "TestGetExcludesSoftDeleted",
    "TestGetManyExcludesSoftDeleted",
    "TestModel",
    "TestRestore",
    "TestSoftDelete",
    "TestSoftDeleteCycle",
    "TestTruncateRemovesSoftDeleted",
    "repo",
]
