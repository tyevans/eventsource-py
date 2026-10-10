"""Migration repository unit tests package."""

from __future__ import annotations

from .test_helpers_and_transitions import (
    TestPhaseTransitionValidation,
    TestPostgreSQLMigrationRepositoryHelpers,
)
from .test_listing import (
    TestPostgreSQLMigrationRepositoryListActive,
)
from .test_protocol_and_lifecycle import (
    TestMigrationRepositoryProtocol,
    TestPostgreSQLMigrationRepositoryCreate,
    TestPostgreSQLMigrationRepositoryGet,
    TestPostgreSQLMigrationRepositoryGetByTenant,
    TestPostgreSQLMigrationRepositoryInit,
    TestValidTransitions,
)
from .test_updates import (
    TestPostgreSQLMigrationRepositoryRecordError,
    TestPostgreSQLMigrationRepositorySetEventsTotal,
    TestPostgreSQLMigrationRepositorySetPaused,
    TestPostgreSQLMigrationRepositoryUpdatePhase,
    TestPostgreSQLMigrationRepositoryUpdateProgress,
)

__all__ = [
    "TestMigrationRepositoryProtocol",
    "TestPhaseTransitionValidation",
    "TestPostgreSQLMigrationRepositoryCreate",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryGet",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryGetByTenant",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryHelpers",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryInit",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryListActive",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryRecordError",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositorySetEventsTotal",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositorySetPaused",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryUpdatePhase",  # pragma: allowlist secret
    "TestPostgreSQLMigrationRepositoryUpdateProgress",  # pragma: allowlist secret
    "TestValidTransitions",
]
