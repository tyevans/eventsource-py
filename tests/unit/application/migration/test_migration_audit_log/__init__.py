"""Unit tests package for MigrationCoordinator audit log integration."""

from __future__ import annotations

from ._fakes import FakeAuditLogRepository
from .test_cutover_and_verification_audit import (
    TestAuditFailureDoesNotFailOperation,
    TestCutoverAudit,
    TestVerifyConsistencyAudit,
)
from .test_lifecycle_audit import (
    TestAbortMigrationAudit,
    TestAuditLogIsOptional,
    TestFailMigrationAudit,
    TestStartMigrationAudit,
    TestTransitionToDualWriteAudit,
)

__all__ = [
    "FakeAuditLogRepository",
    "TestAbortMigrationAudit",
    "TestAuditFailureDoesNotFailOperation",
    "TestAuditLogIsOptional",
    "TestCutoverAudit",
    "TestFailMigrationAudit",
    "TestStartMigrationAudit",
    "TestTransitionToDualWriteAudit",
    "TestVerifyConsistencyAudit",
]
