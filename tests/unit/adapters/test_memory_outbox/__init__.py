"""Unit tests for InMemoryOutboxRepository."""

from __future__ import annotations

from tests.unit.adapters.test_memory_outbox._events import SampleEvent
from tests.unit.adapters.test_memory_outbox.test_concurrency import (
    TestInMemoryOutboxRepositoryConcurrency,
    TestOutboxEntryTypedReturns,
)
from tests.unit.adapters.test_memory_outbox.test_repository import (
    TestInMemoryOutboxRepository,
    TestOutboxRepositoryProtocol,
)

__all__ = [
    "SampleEvent",
    "TestInMemoryOutboxRepository",
    "TestInMemoryOutboxRepositoryConcurrency",  # pragma: allowlist secret
    "TestOutboxEntryTypedReturns",
    "TestOutboxRepositoryProtocol",
]
