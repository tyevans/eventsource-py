"""Read-model port: the pure contract half of read-model persistence.

A subpackage rather than a flat `ports/readmodels.py` (ADR 0029): four
genuinely distinct pure artifacts -- a user-subclassable pydantic base, a
query specification language, a 15-method repository Protocol, and an
exception family -- that users import for four different reasons.
`eventsource.ports.readmodels` is the import path users see either way.

The adapter half lives under `eventsource.adapters.{memory,postgresql,
sqlite,sql}`.

`ReadModelVersionConflictError` (`model_id`, `expected_version`,
`actual_version`) is the read-model conflict, a `ReadModelError` subclass.
It was called `OptimisticLockError` until ADR 0050, which shared the name
with `eventsource.domain.exceptions.OptimisticLockError` -- an unrelated
`EventSourceError` subclass raised on event append, with a different
constructor, that never caught it and was never caught by it.
"""

from eventsource.ports.readmodels.exceptions import (
    ReadModelError,
    ReadModelNotFoundError,
    ReadModelSchemaMismatchError,
    ReadModelVersionConflictError,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query
from eventsource.ports.readmodels.repository import (
    ReadModelRepository,
    ReadModelRepositoryProtocol,
)

__all__ = [
    "Filter",
    "ReadModelVersionConflictError",
    "Query",
    "ReadModel",
    "ReadModelError",
    "ReadModelSchemaMismatchError",
    "ReadModelNotFoundError",
    "ReadModelRepository",
    "ReadModelRepositoryProtocol",
]
