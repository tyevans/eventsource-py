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
from dataclasses import dataclass
from datetime import UTC, datetime
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
from eventsource.repositories._connection import execute_with_connection
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


class PostgreSQLDLQRepository:
    """
    PostgreSQL implementation of DLQ repository.

    Stores failed events in the `dead_letter_queue` table.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLDLQRepository(conn)
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
        self.conn = conn

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
                ATTR_DB_SYSTEM: "postgresql",
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
            params = {
                "event_id": event_id,
                "projection_name": projection_name,
                "event_type": event_type,
                "event_data": json_dumps(event_data),
                "error_message": str(error),
                "error_stacktrace": traceback.format_exc(),
                "retry_count": retry_count,
                "now": now,
            }

            async with execute_with_connection(self.conn, transactional=True) as conn:
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
            ATTR_DB_SYSTEM: "postgresql",
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

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [
                DLQEntry(
                    id=row[0],
                    event_id=row[1],
                    projection_name=row[2],
                    event_type=row[3],
                    event_data=row[4],
                    error_message=row[5],
                    error_stacktrace=row[6],
                    retry_count=row[7],
                    first_failed_at=row[8],
                    last_failed_at=row[9],
                    status=row[10],
                )
                for row in rows
            ]

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT id, event_id, projection_name, event_type, event_data,
                       error_message, error_stacktrace, retry_count,
                       first_failed_at, last_failed_at, status,
                       resolved_at, resolved_by
                FROM dead_letter_queue
                WHERE id = :dlq_id
            """)

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query, {"dlq_id": dlq_id})
                row = result.fetchone()

            if not row:
                return None

            return DLQEntry(
                id=row[0],
                event_id=row[1],
                projection_name=row[2],
                event_type=row[3],
                event_data=row[4],
                error_message=row[5],
                error_stacktrace=row[6],
                retry_count=row[7],
                first_failed_at=row[8],
                last_failed_at=row[9],
                status=row[10],
                resolved_at=row[11],
                resolved_by=row[12],
            )

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
                ATTR_DB_SYSTEM: "postgresql",
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

            async with execute_with_connection(self.conn, transactional=True) as conn:
                await conn.execute(
                    query,
                    {"now": now, "resolved_by": resolved_by_str, "dlq_id": dlq_id},
                )

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
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                UPDATE dead_letter_queue
                SET status = 'retrying'
                WHERE id = :dlq_id
            """)

            async with execute_with_connection(self.conn, transactional=True) as conn:
                await conn.execute(query, {"dlq_id": dlq_id})

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        with self._tracer.span(
            "eventsource.dlq.get_stats",
            {ATTR_DB_SYSTEM: "postgresql"},
        ):
            query = text("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'failed') as total_failed,
                    COUNT(*) FILTER (WHERE status = 'retrying') as total_retrying,
                    COUNT(DISTINCT projection_name) as affected_projections,
                    MIN(first_failed_at) as oldest_failure
                FROM dead_letter_queue
                WHERE status IN ('failed', 'retrying')
            """)

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query)
                row = result.fetchone()

            return DLQStats(
                total_failed=row[0] if row else 0,
                total_retrying=row[1] if row else 0,
                affected_projections=row[2] if row else 0,
                oldest_failure=row[3].isoformat() if (row and row[3]) else None,
            )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        with self._tracer.span(
            "eventsource.dlq.get_projection_counts",
            {ATTR_DB_SYSTEM: "postgresql"},
        ):
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

            async with execute_with_connection(self.conn, transactional=False) as conn:
                result = await conn.execute(query)
                rows = result.fetchall()

            return [
                ProjectionFailureCount(
                    projection_name=row[0],
                    failure_count=row[1],
                    oldest_failure=row[2].isoformat() if row[2] else None,
                    most_recent_failure=row[3].isoformat() if row[3] else None,
                )
                for row in rows
            ]

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
            {
                "older_than_days": older_than_days,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                DELETE FROM dead_letter_queue
                WHERE status = 'resolved'
                AND resolved_at < NOW() - INTERVAL '1 day' * :days
                RETURNING id
            """)

            async with execute_with_connection(self.conn, transactional=True) as conn:
                result = await conn.execute(query, {"days": older_than_days})
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


class SQLiteDLQRepository:
    """
    SQLite implementation of DLQ repository.

    Stores failed events in the `dead_letter_queue` table.

    SQLite-specific notes:
    - UUID stored as TEXT (36 characters, hyphenated format)
    - Timestamps stored as TEXT in ISO 8601 format
    - JSON stored as TEXT (no native JSONB support)
    - Uses ? positional parameters instead of named parameters
    - Uses SUM(CASE WHEN...) instead of COUNT(*) FILTER

    Example:
        >>> import aiosqlite
        >>> async with aiosqlite.connect("events.db") as db:
        ...     repo = SQLiteDLQRepository(db)
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
        connection: Any,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the DLQ repository.

        Args:
            connection: aiosqlite database connection
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._connection = connection

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        """Parse ISO 8601 timestamp string to datetime."""
        if value is None:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

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
        with self._tracer.span(
            "eventsource.dlq.add",
            {
                ATTR_EVENT_ID: str(event_id),
                ATTR_EVENT_TYPE: event_type,
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_ERROR_TYPE: str(error)[:100],
                ATTR_RETRY_COUNT: retry_count,
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            now = datetime.now(UTC).isoformat()
            await self._connection.execute(
                """
                INSERT INTO dead_letter_queue
                    (event_id, projection_name, event_type, event_data,
                     error_message, error_stacktrace, retry_count,
                     first_failed_at, last_failed_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'failed')
                ON CONFLICT (event_id, projection_name) DO UPDATE
                SET retry_count = excluded.retry_count,
                    last_failed_at = excluded.last_failed_at,
                    error_message = excluded.error_message,
                    error_stacktrace = excluded.error_stacktrace,
                    status = 'failed'
                """,
                (
                    str(event_id),
                    projection_name,
                    event_type,
                    json_dumps(event_data),
                    str(error),
                    traceback.format_exc(),
                    retry_count,
                    now,
                    now,
                ),
            )
            await self._connection.commit()

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
            ATTR_DB_SYSTEM: "sqlite",
        }
        if projection_name:
            span_attributes[ATTR_PROJECTION_NAME] = projection_name

        with self._tracer.span("eventsource.dlq.get", span_attributes):
            # Build query dynamically based on filters
            where_clauses = ["status = ?"]
            params: list[Any] = [status]

            if projection_name:
                where_clauses.append("projection_name = ?")
                params.append(projection_name)

            params.append(limit)
            where_clause = " AND ".join(where_clauses)

            # where_clause is built from safe static strings only
            cursor = await self._connection.execute(
                f"""
                SELECT id, event_id, projection_name, event_type, event_data,
                       error_message, error_stacktrace, retry_count,
                       first_failed_at, last_failed_at, status
                FROM dead_letter_queue
                WHERE {where_clause}
                ORDER BY first_failed_at DESC
                LIMIT ?
                """,  # nosec B608
                params,
            )
            rows = await cursor.fetchall()

            return [
                DLQEntry(
                    id=row[0],
                    event_id=UUID(row[1]),
                    projection_name=row[2],
                    event_type=row[3],
                    event_data=row[4],
                    error_message=row[5],
                    error_stacktrace=row[6],
                    retry_count=row[7],
                    first_failed_at=self._parse_datetime(row[8]),
                    last_failed_at=self._parse_datetime(row[9]),
                    status=row[10],
                )
                for row in rows
            ]

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            cursor = await self._connection.execute(
                """
                SELECT id, event_id, projection_name, event_type, event_data,
                       error_message, error_stacktrace, retry_count,
                       first_failed_at, last_failed_at, status,
                       resolved_at, resolved_by
                FROM dead_letter_queue
                WHERE id = ?
                """,
                (dlq_id,),
            )
            row = await cursor.fetchone()

            if not row:
                return None

            return DLQEntry(
                id=row[0],
                event_id=UUID(row[1]),
                projection_name=row[2],
                event_type=row[3],
                event_data=row[4],
                error_message=row[5],
                error_stacktrace=row[6],
                retry_count=row[7],
                first_failed_at=self._parse_datetime(row[8]),
                last_failed_at=self._parse_datetime(row[9]),
                status=row[10],
                resolved_at=self._parse_datetime(row[11]),
                resolved_by=row[12],
            )

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            now = datetime.now(UTC).isoformat()
            resolved_by_str = str(resolved_by) if resolved_by else None

            await self._connection.execute(
                """
                UPDATE dead_letter_queue
                SET status = 'resolved',
                    resolved_at = ?,
                    resolved_by = ?
                WHERE id = ?
                """,
                (now, resolved_by_str, dlq_id),
            )
            await self._connection.commit()

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
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            await self._connection.execute(
                """
                UPDATE dead_letter_queue
                SET status = 'retrying'
                WHERE id = ?
                """,
                (dlq_id,),
            )
            await self._connection.commit()

    async def get_failure_stats(self) -> DLQStats:
        """
        Get aggregate statistics about DLQ health.

        Returns:
            DLQStats with failure statistics
        """
        with self._tracer.span(
            "eventsource.dlq.get_stats",
            {ATTR_DB_SYSTEM: "sqlite"},
        ):
            # SQLite uses CASE WHEN instead of FILTER clause
            cursor = await self._connection.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_failed,
                    SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) as total_retrying,
                    COUNT(DISTINCT projection_name) as affected_projections,
                    MIN(first_failed_at) as oldest_failure
                FROM dead_letter_queue
                WHERE status IN ('failed', 'retrying')
                """
            )
            row = await cursor.fetchone()

            return DLQStats(
                total_failed=row[0] if row and row[0] else 0,
                total_retrying=row[1] if row and row[1] else 0,
                affected_projections=row[2] if row and row[2] else 0,
                oldest_failure=row[3] if row else None,
            )

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """
        Get failure counts grouped by projection.

        Returns:
            List of ProjectionFailureCount for each affected projection
        """
        with self._tracer.span(
            "eventsource.dlq.get_projection_counts",
            {ATTR_DB_SYSTEM: "sqlite"},
        ):
            cursor = await self._connection.execute(
                """
                SELECT
                    projection_name,
                    COUNT(*) as failure_count,
                    MIN(first_failed_at) as oldest_failure,
                    MAX(last_failed_at) as most_recent_failure
                FROM dead_letter_queue
                WHERE status IN ('failed', 'retrying')
                GROUP BY projection_name
                ORDER BY failure_count DESC
                """
            )
            rows = await cursor.fetchall()

            return [
                ProjectionFailureCount(
                    projection_name=row[0],
                    failure_count=row[1],
                    oldest_failure=row[2],
                    most_recent_failure=row[3],
                )
                for row in rows
            ]

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
            {
                "older_than_days": older_than_days,
                ATTR_DB_SYSTEM: "sqlite",
            },
        ):
            cursor = await self._connection.execute(
                """
                DELETE FROM dead_letter_queue
                WHERE status = 'resolved'
                AND resolved_at < datetime('now', '-' || ? || ' days')
                """,
                (older_than_days,),
            )
            await self._connection.commit()
            rowcount: int = cursor.rowcount
            return rowcount

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


# Type alias for backwards compatibility
DLQRepositoryProtocol = DLQRepository
