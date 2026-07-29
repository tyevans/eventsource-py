"""
Dead Letter Queue (DLQ) repository for failed event processing.

DLQ handles storage and retrieval of events that failed processing after
all retry attempts. This enables:
- Manual investigation of failures
- Replay/retry mechanisms
- Failure monitoring and alerting
"""

import asyncio
import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.repositories._dialect import (
    Dialect,
    dialect_of,
    json_param,
    json_result,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)
from eventsource.serialization import json_dumps


@dataclass
class DLQEntry:
    """
    Data structure for a dead letter queue entry.

    Attributes:
        id: Unique DLQ entry identifier
        event_id: Original event ID that failed
        projection_name: Name of the projection that failed
        event_type: Type of the failed event
        event_data: Serialized event data (JSON string or dict)
        error_message: Error message from the failure
        error_stacktrace: Full stack trace of the error
        retry_count: Number of retry attempts made
        first_failed_at: When the event first failed
        last_failed_at: When the event most recently failed
        status: Current status (failed, retrying, resolved)
        resolved_at: When the entry was resolved (if applicable)
        resolved_by: Who resolved the entry (if applicable)
    """

    id: int | str
    event_id: UUID
    projection_name: str
    event_type: str
    event_data: str | dict[str, Any]
    error_message: str
    error_stacktrace: str | None = None
    retry_count: int = 0
    first_failed_at: datetime | None = None
    last_failed_at: datetime | None = None
    status: str = "failed"
    resolved_at: datetime | None = None
    resolved_by: str | None = None


@dataclass(frozen=True)
class DLQStats:
    """
    Aggregate statistics for the dead letter queue.

    Attributes:
        total_failed: Total number of entries in failed status
        total_retrying: Total number of entries being retried
        affected_projections: Number of unique projections with failures
        oldest_failure: Timestamp of the oldest failure
    """

    total_failed: int = 0
    total_retrying: int = 0
    affected_projections: int = 0
    oldest_failure: str | None = None


@dataclass(frozen=True)
class ProjectionFailureCount:
    """
    Failure count for a specific projection.

    Attributes:
        projection_name: Name of the projection
        failure_count: Number of failures for this projection
        oldest_failure: Timestamp of oldest failure
        most_recent_failure: Timestamp of most recent failure
    """

    projection_name: str
    failure_count: int = 0
    oldest_failure: str | None = None
    most_recent_failure: str | None = None


@runtime_checkable
class DLQRepository(Protocol):
    """
    Protocol for dead letter queue repositories.

    DLQ repositories store events that failed processing after all retries,
    enabling manual review, replay, and resolution.
    """

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """
        Add or update a failed event in the DLQ.

        Uses UPSERT pattern - if event already exists for this projection,
        updates the retry count and error information.

        Args:
            event_id: Event ID that failed
            projection_name: Name of projection that failed to process it
            event_type: Type of event
            event_data: Event data as dict
            error: Exception that occurred
            retry_count: Number of retry attempts
        """
        ...

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        Get failed events from the DLQ.

        Args:
            projection_name: Filter by projection name (optional)
            status: Filter by status (default: "failed")
            limit: Maximum number of events to return

        Returns:
            List of DLQEntry instances
        """
        ...

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        ...

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ record ID
            resolved_by: User ID or identifier of resolver
        """
        ...

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """
        Mark a DLQ entry as being retried.

        Args:
            dlq_id: DLQ record ID
        """
        ...

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        ...

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        ...

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        Useful for periodic cleanup to prevent DLQ table growth.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        ...

    async def list_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        List failed events from the DLQ.

        This is an alias for get_failed_events() for naming consistency.
        Prefer this method when fetching multiple entries.

        Args:
            projection_name: Filter by projection name (optional)
            status: Filter by status (default: "failed")
            limit: Maximum number of events to return

        Returns:
            List of DLQEntry instances
        """
        ...

    async def get_failed_event(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        This is an alias for get_failed_event_by_id() for naming consistency.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        ...


class SQLDLQRepository:
    """
    SQLAlchemy-backed DLQ repository, serving both PostgreSQL and SQLite.

    Stores failed events in the `dead_letter_queue` table. Dialect
    differences (UUID/timestamp/JSON representation) are resolved per call
    via `eventsource.repositories._dialect`.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = SQLDLQRepository(conn)
        ...     await repo.add_failed_event(
        ...         event_id=event.event_id,
        ...         projection_name="MyProjection",
        ...         event_type="MyEvent",
        ...         event_data=event.to_dict(),
        ...         error=exc,
        ...     )
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the DLQ repository.

        Args:
            conn: Database connection or engine
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn
        # Kept for backwards-compatible attribute access.
        self.conn = conn

    @asynccontextmanager
    async def _connect(self, *, write: bool) -> AsyncIterator[AsyncConnection]:
        """
        Yield a connection to execute on.

        If this repository was constructed with a live connection, that
        connection is yielded directly and NOT committed -- the caller owns the
        transaction. If constructed with an engine, a connection is opened here
        and, for writes, committed on successful exit.
        """
        if isinstance(self._conn, AsyncEngine):
            if write:
                async with self._conn.begin() as conn:
                    yield conn
            else:
                async with self._conn.connect() as conn:
                    yield conn
        else:
            yield self._conn

    @staticmethod
    def _row_to_entry(row: Any, dialect: Dialect) -> DLQEntry:
        return DLQEntry(
            id=row[0],
            event_id=uuid_result(row[1]),  # type: ignore[arg-type]
            projection_name=row[2],
            event_type=row[3],
            event_data=json_result(row[4]),
            error_message=row[5],
            error_stacktrace=row[6],
            retry_count=row[7],
            first_failed_at=ts_result(row[8]),
            last_failed_at=ts_result(row[9]),
            status=row[10],
        )

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """
        Add or update a failed event in the DLQ.

        Args:
            event_id: Event ID that failed
            projection_name: Name of projection that failed to process it
            event_type: Type of event
            event_data: Event data as dict
            error: Exception that occurred
            retry_count: Number of retry attempts
        """
        with self._tracer.span(
            "eventsource.dlq.add",
            {
                ATTR_EVENT_ID: str(event_id),
                ATTR_EVENT_TYPE: event_type,
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_ERROR_TYPE: str(error)[:100],
                ATTR_RETRY_COUNT: retry_count,
            },
        ):
            now = datetime.now(UTC)
            query = text("""
                INSERT INTO dead_letter_queue
                    (event_id, projection_name, event_type, event_data,
                     error_message, error_stacktrace, retry_count,
                     first_failed_at, last_failed_at, status)
                VALUES (:event_id, :projection_name, :event_type, :event_data,
                        :error_message, :error_stacktrace, :retry_count,
                        :now, :now, 'failed')
                ON CONFLICT (event_id, projection_name) DO UPDATE
                SET retry_count = EXCLUDED.retry_count,
                    last_failed_at = EXCLUDED.last_failed_at,
                    error_message = EXCLUDED.error_message,
                    error_stacktrace = EXCLUDED.error_stacktrace,
                    status = 'failed'
            """)

            async with self._connect(write=True) as conn:
                dialect = dialect_of(conn)
                params = {
                    "event_id": uuid_param(event_id, dialect),
                    "projection_name": projection_name,
                    "event_type": event_type,
                    "event_data": json_param(event_data, dialect),
                    "error_message": str(error),
                    "error_stacktrace": traceback.format_exc(),
                    "retry_count": retry_count,
                    "now": ts_param(now, dialect),
                }
                await conn.execute(query, params)

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        Get failed events from the DLQ.

        Args:
            projection_name: Filter by projection name (optional)
            status: Filter by status (default: "failed")
            limit: Maximum number of events to return

        Returns:
            List of DLQEntry instances
        """
        span_attributes: dict[str, Any] = {
            "limit": limit,
            "status_filter": status,
        }
        if projection_name:
            span_attributes[ATTR_PROJECTION_NAME] = projection_name

        with self._tracer.span("eventsource.dlq.get", span_attributes):
            # Build query dynamically based on filters
            where_clauses = ["status = :status"]
            params: dict[str, Any] = {"status": status, "limit": limit}

            if projection_name:
                where_clauses.append("projection_name = :projection_name")
                params["projection_name"] = projection_name

            where_clause = " AND ".join(where_clauses)

            # where_clause is built from safe static strings only
            query = text(f"""
                SELECT id, event_id, projection_name, event_type, event_data,
                       error_message, error_stacktrace, retry_count,
                       first_failed_at, last_failed_at, status
                FROM dead_letter_queue
                WHERE {where_clause}
                ORDER BY first_failed_at DESC
                LIMIT :limit
            """)  # nosec B608

            async with self._connect(write=False) as conn:
                dialect = dialect_of(conn)
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [self._row_to_entry(row, dialect) for row in rows]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        with self._tracer.span(
            "eventsource.dlq.get_by_id",
            {"dlq.id": str(dlq_id)},
        ):
            query = text("""
                SELECT id, event_id, projection_name, event_type, event_data,
                       error_message, error_stacktrace, retry_count,
                       first_failed_at, last_failed_at, status,
                       resolved_at, resolved_by
                FROM dead_letter_queue
                WHERE id = :dlq_id
            """)

            async with self._connect(write=False) as conn:
                dialect = dialect_of(conn)
                result = await conn.execute(query, {"dlq_id": dlq_id})
                row = result.fetchone()

            if not row:
                return None

            entry = self._row_to_entry(row, dialect)
            entry.resolved_at = ts_result(row[11])
            entry.resolved_by = row[12]
            return entry

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ record ID
            resolved_by: User ID or identifier of resolver
        """
        with self._tracer.span(
            "eventsource.dlq.resolve",
            {
                "dlq.id": str(dlq_id),
                "resolved_by": str(resolved_by) if resolved_by else None,
            },
        ):
            now = datetime.now(UTC)
            resolved_by_str = str(resolved_by) if resolved_by else None

            query = text("""
                UPDATE dead_letter_queue
                SET status = 'resolved',
                    resolved_at = :now,
                    resolved_by = :resolved_by
                WHERE id = :dlq_id
            """)

            async with self._connect(write=True) as conn:
                dialect = dialect_of(conn)
                await conn.execute(
                    query,
                    {
                        "now": ts_param(now, dialect),
                        "resolved_by": resolved_by_str,
                        "dlq_id": dlq_id,
                    },
                )

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """
        Mark a DLQ entry as being retried.

        Args:
            dlq_id: DLQ record ID
        """
        with self._tracer.span(
            "eventsource.dlq.retry",
            {"dlq.id": str(dlq_id)},
        ):
            query = text("""
                UPDATE dead_letter_queue
                SET status = 'retrying'
                WHERE id = :dlq_id
            """)

            async with self._connect(write=True) as conn:
                await conn.execute(query, {"dlq_id": dlq_id})

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        with self._tracer.span("eventsource.dlq.get_stats", {}):
            # SUM(CASE WHEN ...) works on both PostgreSQL and SQLite, unlike
            # PostgreSQL-only COUNT(*) FILTER (WHERE ...) -- take the SQLite
            # formulation for both dialects.
            query = text("""
                SELECT
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_failed,
                    SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) as total_retrying,
                    COUNT(DISTINCT projection_name) as affected_projections,
                    MIN(first_failed_at) as oldest_failure
                FROM dead_letter_queue
                WHERE status IN ('failed', 'retrying')
            """)

            async with self._connect(write=False) as conn:
                result = await conn.execute(query)
                row = result.fetchone()

            oldest_failure = ts_result(row[3]) if row else None

            return DLQStats(
                total_failed=row[0] if row and row[0] else 0,
                total_retrying=row[1] if row and row[1] else 0,
                affected_projections=row[2] if row and row[2] else 0,
                oldest_failure=oldest_failure.isoformat() if oldest_failure else None,
            )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        with self._tracer.span("eventsource.dlq.get_projection_counts", {}):
            query = text("""
                SELECT
                    projection_name,
                    COUNT(*) as failure_count,
                    MIN(first_failed_at) as oldest_failure,
                    MAX(last_failed_at) as most_recent_failure
                FROM dead_letter_queue
                WHERE status IN ('failed', 'retrying')
                GROUP BY projection_name
                ORDER BY failure_count DESC
            """)

            async with self._connect(write=False) as conn:
                result = await conn.execute(query)
                rows = result.fetchall()

            counts = []
            for row in rows:
                oldest = ts_result(row[2])
                most_recent = ts_result(row[3])
                counts.append(
                    ProjectionFailureCount(
                        projection_name=row[0],
                        failure_count=row[1],
                        oldest_failure=oldest.isoformat() if oldest else None,
                        most_recent_failure=most_recent.isoformat() if most_recent else None,
                    )
                )
            return counts

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        Useful for periodic cleanup to prevent DLQ table growth.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        with self._tracer.span(
            "eventsource.dlq.delete_resolved",
            {"older_than_days": older_than_days},
        ):
            # Cutoff computed in Python (rather than a dialect-specific SQL
            # interval expression) so one query serves both dialects.
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)

            query = text("""
                DELETE FROM dead_letter_queue
                WHERE status = 'resolved'
                AND resolved_at < :cutoff
                RETURNING id
            """)

            async with self._connect(write=True) as conn:
                dialect = dialect_of(conn)
                result = await conn.execute(query, {"cutoff": ts_param(cutoff, dialect)})
                return len(result.fetchall())

    async def list_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """Alias for get_failed_events() - preferred for naming consistency."""
        return await self.get_failed_events(projection_name, status, limit)

    async def get_failed_event(self, dlq_id: int | str) -> DLQEntry | None:
        """Alias for get_failed_event_by_id() - preferred for naming consistency."""
        return await self.get_failed_event_by_id(dlq_id)


class InMemoryDLQRepository:
    """
    In-memory implementation of DLQ repository for testing.

    Stores failed events in memory. All data is lost when process terminates.

    Example:
        >>> repo = InMemoryDLQRepository()
        >>> await repo.add_failed_event(
        ...     event_id=event_id,
        ...     projection_name="MyProjection",
        ...     event_type="MyEvent",
        ...     event_data={},
        ...     error=Exception("Test error"),
        ... )
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory DLQ repository.

        Args:
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._entries: dict[str, DLQEntry] = {}  # key: "{event_id}:{projection_name}"
        self._id_counter: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    def _make_key(self, event_id: UUID, projection_name: str) -> str:
        """Create a unique key for event_id + projection_name combination."""
        return f"{event_id}:{projection_name}"

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """
        Add or update a failed event in the DLQ.

        Args:
            event_id: Event ID that failed
            projection_name: Name of projection that failed to process it
            event_type: Type of event
            event_data: Event data as dict
            error: Exception that occurred
            retry_count: Number of retry attempts
        """
        with self._tracer.span(
            "eventsource.dlq.add",
            {
                ATTR_EVENT_ID: str(event_id),
                ATTR_EVENT_TYPE: event_type,
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_ERROR_TYPE: str(error)[:100],
                ATTR_RETRY_COUNT: retry_count,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            now = datetime.now(UTC)
            key = self._make_key(event_id, projection_name)

            async with self._lock:
                existing = self._entries.get(key)
                if existing:
                    # Update existing entry
                    existing.retry_count = retry_count
                    existing.last_failed_at = now
                    existing.error_message = str(error)
                    existing.error_stacktrace = traceback.format_exc()
                    existing.status = "failed"
                else:
                    # Create new entry
                    self._id_counter += 1
                    self._entries[key] = DLQEntry(
                        id=self._id_counter,
                        event_id=event_id,
                        projection_name=projection_name,
                        event_type=event_type,
                        event_data=json_dumps(event_data),
                        error_message=str(error),
                        error_stacktrace=traceback.format_exc(),
                        retry_count=retry_count,
                        first_failed_at=now,
                        last_failed_at=now,
                        status="failed",
                    )

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        Get failed events from the DLQ.

        Args:
            projection_name: Filter by projection name (optional)
            status: Filter by status (default: "failed")
            limit: Maximum number of events to return

        Returns:
            List of DLQEntry instances
        """
        span_attributes: dict[str, Any] = {
            "limit": limit,
            "status_filter": status,
            ATTR_DB_SYSTEM: "memory",
        }
        if projection_name:
            span_attributes[ATTR_PROJECTION_NAME] = projection_name

        with self._tracer.span("eventsource.dlq.get", span_attributes):
            async with self._lock:
                entries = list(self._entries.values())

                # Filter by status
                entries = [e for e in entries if e.status == status]

                # Filter by projection name if provided
                if projection_name:
                    entries = [e for e in entries if e.projection_name == projection_name]

                # Sort by first_failed_at descending
                entries.sort(key=lambda e: e.first_failed_at or datetime.min, reverse=True)

                # Apply limit
                return entries[:limit]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """
        Get a specific failed event by its DLQ ID.

        Args:
            dlq_id: DLQ record ID

        Returns:
            DLQEntry instance, or None if not found
        """
        with self._tracer.span(
            "eventsource.dlq.get_by_id",
            {
                "dlq.id": str(dlq_id),
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        return entry
                return None

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """
        Mark a DLQ entry as resolved.

        Args:
            dlq_id: DLQ record ID
            resolved_by: User ID or identifier of resolver
        """
        with self._tracer.span(
            "eventsource.dlq.resolve",
            {
                "dlq.id": str(dlq_id),
                "resolved_by": str(resolved_by) if resolved_by else None,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        entry.status = "resolved"
                        entry.resolved_at = now
                        entry.resolved_by = str(resolved_by) if resolved_by else None
                        break

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """
        Mark a DLQ entry as being retried.

        Args:
            dlq_id: DLQ record ID
        """
        with self._tracer.span(
            "eventsource.dlq.retry",
            {
                "dlq.id": str(dlq_id),
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            async with self._lock:
                for entry in self._entries.values():
                    if entry.id == dlq_id:
                        entry.status = "retrying"
                        break

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        with self._tracer.span(
            "eventsource.dlq.get_stats",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                active_entries = [
                    e for e in self._entries.values() if e.status in ("failed", "retrying")
                ]

                total_failed = sum(1 for e in active_entries if e.status == "failed")
                total_retrying = sum(1 for e in active_entries if e.status == "retrying")
                affected_projections = len({e.projection_name for e in active_entries})

                oldest_failure = None
                if active_entries:
                    failures_with_dates = [
                        e.first_failed_at for e in active_entries if e.first_failed_at
                    ]
                    if failures_with_dates:
                        oldest_failure = min(failures_with_dates).isoformat()

                return DLQStats(
                    total_failed=total_failed,
                    total_retrying=total_retrying,
                    affected_projections=affected_projections,
                    oldest_failure=oldest_failure,
                )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        with self._tracer.span(
            "eventsource.dlq.get_projection_counts",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                active_entries = [
                    e for e in self._entries.values() if e.status in ("failed", "retrying")
                ]

                # Group by projection
                projection_stats: dict[str, list[DLQEntry]] = {}
                for entry in active_entries:
                    if entry.projection_name not in projection_stats:
                        projection_stats[entry.projection_name] = []
                    projection_stats[entry.projection_name].append(entry)

                result = []
                for projection_name, entries in projection_stats.items():
                    failures_with_first = [e.first_failed_at for e in entries if e.first_failed_at]
                    failures_with_last = [e.last_failed_at for e in entries if e.last_failed_at]

                    result.append(
                        ProjectionFailureCount(
                            projection_name=projection_name,
                            failure_count=len(entries),
                            oldest_failure=(
                                min(failures_with_first).isoformat()
                                if failures_with_first
                                else None
                            ),
                            most_recent_failure=(
                                max(failures_with_last).isoformat() if failures_with_last else None
                            ),
                        )
                    )

                # Sort by failure count descending
                result.sort(key=lambda x: x.failure_count, reverse=True)
                return result

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """
        Delete resolved events older than specified days.

        Args:
            older_than_days: Delete resolved events older than this many days

        Returns:
            Number of events deleted
        """
        with self._tracer.span(
            "eventsource.dlq.delete_resolved",
            {
                "older_than_days": older_than_days,
                ATTR_DB_SYSTEM: "memory",
            },
        ):
            cutoff = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            # Subtract days (simplified for in-memory implementation)
            from datetime import timedelta

            cutoff = cutoff - timedelta(days=older_than_days)

            deleted = 0
            async with self._lock:
                keys_to_delete = []
                for key, entry in self._entries.items():
                    if (
                        entry.status == "resolved"
                        and entry.resolved_at
                        and entry.resolved_at < cutoff
                    ):
                        keys_to_delete.append(key)

                for key in keys_to_delete:
                    del self._entries[key]
                    deleted += 1

            return deleted

    async def list_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """Alias for get_failed_events() - preferred for naming consistency."""
        return await self.get_failed_events(projection_name, status, limit)

    async def get_failed_event(self, dlq_id: int | str) -> DLQEntry | None:
        """Alias for get_failed_event_by_id() - preferred for naming consistency."""
        return await self.get_failed_event_by_id(dlq_id)

    async def clear(self) -> None:
        """Clear all entries. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.dlq.clear",
            {ATTR_DB_SYSTEM: "memory"},
        ):
            async with self._lock:
                self._entries.clear()
                self._id_counter = 0


# Type alias for backwards compatibility
DLQRepositoryProtocol = DLQRepository
