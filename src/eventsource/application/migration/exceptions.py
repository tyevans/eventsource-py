"""
Migration exception taxonomy.

`MigrationError` and its subclasses, rooted in `EventSourceError`. Each
declares its default classification (severity, recoverability, retry
policy) using the vocabulary from `error_classification.py`.

Sibling modules own the runtime machinery that used to live here:
`circuit_breaker.py` (failure gating) and `error_handling.py`
(`ErrorHandler`, `classify_exception`). See ADR 0044.
"""

from __future__ import annotations

from eventsource.application.migration.exceptions_base import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.application.migration.exceptions_cutover import (
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
)
from eventsource.application.migration.exceptions_operations import (
    BulkCopyError,
    CircuitBreakerOpenError,
    ConsistencyError,
    DualWriteError,
    PositionMappingError,
)

__all__ = [
    "BulkCopyError",
    "CircuitBreakerOpenError",
    "ConsistencyError",
    "CutoverError",
    "CutoverLagError",
    "CutoverTimeoutError",
    "DualWriteError",
    "InvalidPhaseTransitionError",
    "MigrationAlreadyExistsError",
    "MigrationError",
    "MigrationNotFoundError",
    "MigrationStateError",
    "PositionMappingError",
]
