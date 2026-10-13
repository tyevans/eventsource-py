"""
Unit tests for migration exceptions decomposed package.
"""

from .test_exceptions_lifecycle_errors import (
    TestCutoverError,
    TestCutoverLagError,
    TestCutoverTimeoutError,
    TestInvalidPhaseTransitionError,
    TestMigrationAlreadyExistsError,
    TestMigrationError,
    TestMigrationNotFoundError,
    TestMigrationStateError,
)
from .test_exceptions_operation_errors import (
    TestBulkCopyError,
    TestCircuitBreakerOpenError,
    TestConsistencyError,
    TestDualWriteError,
    TestExceptionClassification,
    TestExceptionHierarchy,
    TestPositionMappingError,
)

__all__ = [  # pragma: allowlist secret
    "TestBulkCopyError",
    "TestCircuitBreakerOpenError",
    "TestConsistencyError",
    "TestCutoverError",
    "TestCutoverLagError",
    "TestCutoverTimeoutError",
    "TestDualWriteError",
    "TestExceptionClassification",
    "TestExceptionHierarchy",
    "TestInvalidPhaseTransitionError",
    "TestMigrationAlreadyExistsError",
    "TestMigrationError",
    "TestMigrationNotFoundError",
    "TestMigrationStateError",
    "TestPositionMappingError",
]
