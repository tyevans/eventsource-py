"""
Snapshot-specific exceptions.

These exceptions are used internally by the snapshot system and are
typically caught and handled gracefully (falling back to full event replay)
rather than propagated to user code.
"""

from __future__ import annotations

from uuid import UUID


class SnapshotError(Exception):
    """
    Base exception for snapshot-related errors.

    All snapshot exceptions inherit from this class, allowing callers
    to catch all snapshot errors with a single except clause.

    Example:
        >>> try:
        ...     snapshot = await load_snapshot(aggregate_id)
        ... except SnapshotError:
        ...     # Fall back to full event replay
        ...     pass
    """

    pass


class SnapshotDeserializationError(SnapshotError):
    """
    Raised when a snapshot cannot be deserialized.

    This typically occurs when:
    - The state JSON is malformed
    - The state doesn't match the expected Pydantic model
    - Required fields are missing from the state

    The system handles this gracefully by falling back to full event
    replay, which will recreate the snapshot with valid data.

    Attributes:
        aggregate_id: ID of the aggregate whose snapshot failed
        aggregate_type: Type of the aggregate
        original_error: The underlying deserialization error

    Example:
        >>> try:
        ...     state = state_type.model_validate(snapshot.state)
        ... except ValidationError as e:
        ...     raise SnapshotDeserializationError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         original_error=e,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        original_error: Exception | None = None,
        message: str | None = None,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.original_error = original_error

        if message:
            self.message = message
        else:
            self.message = f"Failed to deserialize snapshot for {aggregate_type}/{aggregate_id}"
            if original_error:
                self.message += f": {original_error}"

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotDeserializationError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"original_error={self.original_error!r})"
        )


class SnapshotSchemaVersionError(SnapshotError):
    """
    Raised when snapshot schema version doesn't match aggregate schema version.

    This is a normal occurrence during schema evolution. When an aggregate's
    state model changes and its schema_version is incremented, existing
    snapshots become incompatible and must be regenerated.

    The system handles this gracefully by:
    1. Logging the version mismatch
    2. Falling back to full event replay
    3. Optionally creating a new snapshot with the updated schema

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate
        snapshot_schema_version: Schema version stored in the snapshot
        expected_schema_version: Schema version expected by the aggregate

    Example:
        >>> if snapshot.schema_version != aggregate_class.schema_version:
        ...     raise SnapshotSchemaVersionError(
        ...         aggregate_id=snapshot.aggregate_id,
        ...         aggregate_type=snapshot.aggregate_type,
        ...         snapshot_schema_version=snapshot.schema_version,
        ...         expected_schema_version=aggregate_class.schema_version,
        ...     )
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        snapshot_schema_version: int,
        expected_schema_version: int,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.snapshot_schema_version = snapshot_schema_version
        self.expected_schema_version = expected_schema_version

        self.message = (
            f"Schema version mismatch for {aggregate_type}/{aggregate_id}: "
            f"snapshot has schema_version={snapshot_schema_version}, "
            f"but aggregate expects schema_version={expected_schema_version}"
        )

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotSchemaVersionError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"snapshot_schema_version={self.snapshot_schema_version}, "
            f"expected_schema_version={self.expected_schema_version})"
        )


class SnapshotNotFoundError(SnapshotError):
    """
    Raised when a snapshot is expected but not found.

    Note: This exception is rarely raised in practice because missing
    snapshots are a normal condition (graceful fallback to event replay).
    It's provided for cases where a snapshot is explicitly required.

    Attributes:
        aggregate_id: ID of the aggregate
        aggregate_type: Type of the aggregate

    Example:
        >>> snapshot = await store.get_snapshot(aggregate_id, "Order")
        >>> if snapshot is None and require_snapshot:
        ...     raise SnapshotNotFoundError(aggregate_id, "Order")
    """

    def __init__(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type

        self.message = f"No snapshot found for {aggregate_type}/{aggregate_id}"

        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"SnapshotNotFoundError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r})"
        )
