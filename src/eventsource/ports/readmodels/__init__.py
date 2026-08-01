"""Read-model port: the pure contract half of read-model persistence.

A subpackage rather than a flat `ports/readmodels.py` (ADR 0029): four
genuinely distinct pure artifacts -- a user-subclassable pydantic base, a
query specification language, a 15-method repository Protocol, and an
exception family -- that users import for four different reasons.
`eventsource.ports.readmodels` is the import path users see either way.

The adapter half lives under `eventsource.adapters.{memory,postgresql,
sqlite,sql}`.

Note: `OptimisticLockError` here is the read-model one
(`model_id`, `expected_version`, `actual_version`), a `ReadModelError`
subclass. It is a **different class** from
`eventsource.domain.exceptions.OptimisticLockError`, which is raised on event
append and derives from `EventSourceError`. Neither catches the other. See
ADR 0029's recorded exception and the backlog item that resolves the name
collision.
"""

from eventsource.ports.readmodels.exceptions import (
    OptimisticLockError,
    ReadModelError,
    ReadModelNotFoundError,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query
from eventsource.ports.readmodels.repository import (
    ReadModelRepository,
    ReadModelRepositoryProtocol,
)

__all__ = [
    "Filter",
    "OptimisticLockError",
    "Query",
    "ReadModel",
    "ReadModelError",
    "ReadModelNotFoundError",
    "ReadModelRepository",
    "ReadModelRepositoryProtocol",
]
