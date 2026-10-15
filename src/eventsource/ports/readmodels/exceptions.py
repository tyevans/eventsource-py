"""Exceptions for read model repositories."""

from uuid import UUID

from eventsource.domain.exceptions import EventSourceError


class ReadModelError(EventSourceError):
    """Base exception for read model repository operations."""


class ReadModelVersionConflictError(ReadModelError):
    """Raised when optimistic locking fails due to version mismatch."""

    def __init__(
        self,
        model_id: UUID,
        expected_version: int,
        actual_version: int | None = None,
    ) -> None:
        """Initialize ReadModelVersionConflictError."""
        self.model_id = model_id
        self.expected_version = expected_version
        self.actual_version = actual_version

        if actual_version is not None:
            message = (
                f"Optimistic lock failed for model {model_id}: "
                f"expected version {expected_version}, "
                f"found version {actual_version}"
            )
        else:
            message = (
                f"Optimistic lock failed for model {model_id}: "
                f"expected version {expected_version}, "
                f"but model was not found or was modified"
            )

        super().__init__(message)


class ReadModelNotFoundError(ReadModelError):
    """Raised when a read model is not found but was expected to exist."""

    def __init__(self, model_id: UUID) -> None:
        """Initialize ReadModelNotFoundError."""
        self.model_id = model_id
        super().__init__(f"Read model {model_id} not found")


class ReadModelSchemaMismatchError(ReadModelError):
    """Raised when a read model's table cannot be reconciled with the model."""

    def __init__(self, table_name: str, column: str, reason: str) -> None:
        """Initialize ReadModelSchemaMismatchError."""
        self.table_name = table_name
        self.column = column
        self.reason = reason
        super().__init__(f"Cannot reconcile {table_name}: {reason}")


__all__ = [
    "ReadModelError",
    "ReadModelNotFoundError",
    "ReadModelSchemaMismatchError",
    "ReadModelVersionConflictError",
]
