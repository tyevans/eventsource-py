"""
Exceptions for read model repositories.

This module provides exception classes for error handling in read model
repository operations, including optimistic locking conflicts and
model lookup failures.
"""

from uuid import UUID


class ReadModelError(Exception):
    """
    Base exception for read model operations.

    All read model repository exceptions inherit from this class,
    allowing callers to catch all read model errors with a single
    except clause if desired.

    Example:
        >>> try:
        ...     await repo.save_with_version_check(model)
        ... except ReadModelError as e:
        ...     print(f"Read model error: {e}")
    """

    pass


class OptimisticLockError(ReadModelError):
    """
    Raised when optimistic locking fails due to version mismatch.

    This occurs when attempting to save a read model whose version
    in the database has changed since it was loaded. This indicates
    a concurrent modification conflict.

    The optimistic locking pattern:
    1. Read model with current version N
    2. When saving, check that DB version is still N
    3. If match, save with version N+1
    4. If mismatch, raise OptimisticLockError

    Attributes:
        model_id: ID of the read model
        expected_version: Version the client expected (from loaded model)
        actual_version: Version currently in the database (None if unknown)

    Example:
        >>> summary = await repo.get(order_id)
        >>> summary.status = "shipped"
        >>> try:
        ...     await repo.save_with_version_check(summary)
        ... except OptimisticLockError as e:
        ...     # Handle conflict - reload and retry or fail
        ...     print(f"Conflict: expected v{e.expected_version}, found v{e.actual_version}")
        ...     # Option 1: Reload and retry
        ...     fresh_summary = await repo.get(order_id)
        ...     # Option 2: Return conflict error to caller
    """

    def __init__(
        self,
        model_id: UUID,
        expected_version: int,
        actual_version: int | None = None,
    ) -> None:
        """
        Initialize OptimisticLockError.

        Args:
            model_id: ID of the read model that had the conflict
            expected_version: Version the client expected based on loaded model
            actual_version: Version currently in the database, or None if
                the model was not found during the check
        """
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
    """
    Raised when a read model is not found but was expected to exist.

    This is typically raised by operations that require an existing
    model, such as `save_with_version_check()`.

    Attributes:
        model_id: ID of the missing read model

    Example:
        >>> model = OrderSummary(id=uuid4(), ...)  # New model, not in DB
        >>> try:
        ...     await repo.save_with_version_check(model)
        ... except ReadModelNotFoundError as e:
        ...     print(f"Model {e.model_id} not found - use save() for new models")
    """

    def __init__(self, model_id: UUID) -> None:
        """
        Initialize ReadModelNotFoundError.

        Args:
            model_id: ID of the read model that was not found
        """
        self.model_id = model_id
        super().__init__(f"Read model {model_id} not found")
