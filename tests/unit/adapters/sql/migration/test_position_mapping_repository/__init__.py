"""Position mapping repository unit tests package."""

from __future__ import annotations

from ._helpers import pos
from .test_helpers_and_workflow import (
    TestPositionMappingWorkflow,
    TestPostgreSQLPositionMappingRepositoryHelpers,
)
from .test_lookups import (
    TestPostgreSQLPositionMappingRepositoryFindByEventId,
    TestPostgreSQLPositionMappingRepositoryFindBySourcePosition,
    TestPostgreSQLPositionMappingRepositoryFindByTargetPosition,
    TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition,
)
from .test_protocol_and_lifecycle import (
    TestPositionMappingRepositoryProtocol,
    TestPostgreSQLPositionMappingRepositoryCreate,
    TestPostgreSQLPositionMappingRepositoryCreateBatch,
    TestPostgreSQLPositionMappingRepositoryGet,
    TestPostgreSQLPositionMappingRepositoryInit,
)
from .test_queries import (
    TestPostgreSQLPositionMappingRepositoryCountByMigration,
    TestPostgreSQLPositionMappingRepositoryDeleteByMigration,
    TestPostgreSQLPositionMappingRepositoryGetPositionBounds,
    TestPostgreSQLPositionMappingRepositoryListByMigration,
    TestPostgreSQLPositionMappingRepositoryListInSourceRange,
)

__all__ = [
    "TestPositionMappingRepositoryProtocol",
    "TestPositionMappingWorkflow",
    "TestPostgreSQLPositionMappingRepositoryCountByMigration",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryCreate",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryCreateBatch",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryDeleteByMigration",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryFindByEventId",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryFindBySourcePosition",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryFindByTargetPosition",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryGet",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryGetPositionBounds",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryHelpers",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryInit",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryListByMigration",  # pragma: allowlist secret
    "TestPostgreSQLPositionMappingRepositoryListInSourceRange",  # pragma: allowlist secret
    "pos",
]
