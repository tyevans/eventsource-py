"""Snapshot-related exceptions for the eventsource package."""

from __future__ import annotations

from uuid import UUID

from eventsource.domain.exceptions.base import EventSourceError


class SnapshotError(EventSourceError):
    """Base exception for snapshot-related errors."""

    pass


class SnapshotDeserializationError(SnapshotError):
    """
    Signals that a snapshot cannot be deserialized.

    Who raises this:
        `SQLiteSnapshotStore` and `PostgreSQLSnapshotStore` raise it from
        `get_snapshot()` when the stored `state` column fails to deserialize
        as JSON (SQLite's `TEXT` column has no server-side validation, so
        this is a real corruption path there; PostgreSQL's `JSONB` column
        rejects malformed JSON at write time, so the check there is
        defense-in-depth against state written outside `save_snapshot()`).
        `InMemorySnapshotStore` never deserializes at all -- it holds the
        actual `Snapshot` object -- so it has no equivalent failure mode and
        never raises this.

        Regardless of source, callers never see it through the normal load
        path: `read_valid_snapshot()` catches every failure from the store
        and returns `None` so the aggregate replays in full -- a snapshot is
        a read optimization, so the worst outcome of any snapshot failure is
        a slower load, never a failed one (ADR 0017).

        This type is also published for third-party `SnapshotStore`
        implementors, who should raise it the same way to say precisely why
        a snapshot was unusable rather than letting a bare `ValidationError`
        or driver-specific exception escape, and for callers who wrap a
        store to get stricter behavior than the default degradation. Catch
        `SnapshotError` to handle the whole family at once.
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

    def __repr__(self) -> str:
        return (
            f"SnapshotDeserializationError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r}, "
            f"original_error={self.original_error!r})"
        )


class SnapshotSchemaVersionError(SnapshotError):
    """
    Signals that a snapshot's schema version does not match the aggregate's.

    Who raises this:
        Not the library, and here that is by design rather than by tolerance.
        Bumping `schema_version` is the documented way to invalidate stale
        snapshots, so a mismatch is an *expected* condition on a deliberate
        migration -- `read_valid_snapshot()` logs it at INFO and replays. An
        exception would turn a routine schema rollout into an outage. Use
        `SnapshotStore.delete_snapshots_by_type(schema_version_below=N)` to
        clear the dead snapshots afterwards.

        This type is published for `SnapshotStore` implementors that validate
        versions themselves and for callers wanting stricter behavior than the
        default degradation. Catch `SnapshotError` to handle the whole family.
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
    Signals that a snapshot was required but not found.

    Who raises this:
        Not the library. A missing snapshot is not an error -- every
        aggregate's first load has none -- so `get_snapshot()` returns `None`
        and the aggregate replays in full, silently.

        This type is published for callers who genuinely require a snapshot to
        exist (a tool that reports snapshot coverage, or a load path that
        refuses to replay a very long stream) and for `SnapshotStore`
        implementors with the same requirement. Catch `SnapshotError` to
        handle the whole family.
    """

    def __init__(self, aggregate_id: UUID, aggregate_type: str) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.message = f"No snapshot found for {aggregate_type}/{aggregate_id}"
        super().__init__(self.message)

    def __repr__(self) -> str:
        return (
            f"SnapshotNotFoundError("
            f"aggregate_id={self.aggregate_id!r}, "
            f"aggregate_type={self.aggregate_type!r})"
        )


__all__ = [
    "SnapshotDeserializationError",
    "SnapshotError",
    "SnapshotNotFoundError",
    "SnapshotSchemaVersionError",
]
