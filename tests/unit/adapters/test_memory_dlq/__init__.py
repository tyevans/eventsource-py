"""DLQ repository unit tests package."""

from __future__ import annotations

from .test_entry import TestDLQEntryTypedReturns
from .test_memory import (
    TestDLQRepositoryProtocol,
    TestInMemoryDLQRepository,
)
from .test_memory_concurrency import (
    TestInMemoryDLQRepositoryConcurrency,
)
from .test_sql_crud import (
    TestSQLDLQRepositoryCrud,
    TestSQLDLQRepositoryProtocol,
)
from .test_sql_queries import (
    TestSQLDLQRepositoryQueries,
)

# Backward-compatible alias
TestSQLDLQRepository = TestSQLDLQRepositoryCrud

__all__ = [
    "TestDLQEntryTypedReturns",
    "TestDLQRepositoryProtocol",
    "TestInMemoryDLQRepository",  # pragma: allowlist secret
    "TestInMemoryDLQRepositoryConcurrency",  # pragma: allowlist secret
    "TestSQLDLQRepository",
    "TestSQLDLQRepositoryCrud",  # pragma: allowlist secret
    "TestSQLDLQRepositoryProtocol",  # pragma: allowlist secret
    "TestSQLDLQRepositoryQueries",  # pragma: allowlist secret
]
