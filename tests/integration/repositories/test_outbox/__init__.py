"""Integration tests for PostgreSQLOutboxRepository."""

from __future__ import annotations

from tests.integration.repositories.test_outbox.test_outbox_basics import (
    TestPostgreSQLOutboxRepositoryBasics,
    TestPostgreSQLOutboxRepositoryMultipleEventTypes,
    TestPostgreSQLOutboxRepositoryPending,
)
from tests.integration.repositories.test_outbox.test_outbox_lifecycle import (
    TestPostgreSQLOutboxRepositoryCleanup,
    TestPostgreSQLOutboxRepositoryPublishing,
    TestPostgreSQLOutboxRepositoryRetries,
    TestPostgreSQLOutboxRepositoryStatistics,
)

__all__ = [
    "TestPostgreSQLOutboxRepositoryBasics",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryCleanup",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryMultipleEventTypes",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryPending",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryPublishing",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryRetries",  # pragma: allowlist secret
    "TestPostgreSQLOutboxRepositoryStatistics",  # pragma: allowlist secret
]
