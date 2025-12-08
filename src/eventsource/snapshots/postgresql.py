"""
PostgreSQL snapshot store implementation.

Provides production-ready snapshot storage using PostgreSQL with
async SQLAlchemy sessions and optional OpenTelemetry tracing.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import TYPE_CHECKING, cast
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.snapshots.interface import Snapshot, SnapshotStore

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

logger = logging.getLogger(__name__)


class PostgreSQLSnapshotStore(SnapshotStore):
    """
    PostgreSQL implementation of SnapshotStore.

    Uses SQLAlchemy async sessions for database operations.
    Requires the snapshots table to be created (see migrations).

    Features:
    - Upsert semantics for save (INSERT ON CONFLICT UPDATE)
    - Efficient single-row lookups
    - Bulk delete by aggregate type
    - Optional OpenTelemetry tracing

    Example:
        >>> from sqlalchemy.ext.asyncio import (
        ...     create_async_engine,
        ...     async_sessionmaker,
        ... )
        >>> from eventsource.snapshots import PostgreSQLSnapshotStore
        >>>
        >>> engine = create_async_engine("postgresql+asyncpg://...")
        >>> session_factory = async_sessionmaker(engine, expire_on_commit=False)
        >>> store = PostgreSQLSnapshotStore(session_factory)
        >>>
        >>> # Use in repository
        >>> repo = AggregateRepository(
        ...     event_store=event_store,
        ...     aggregate_factory=OrderAggregate,
        ...     aggregate_type="Order",
        ...     snapshot_store=store,
        ... )

        >>> # With OpenTelemetry tracing
        >>> from opentelemetry import trace
        >>> tracer = trace.get_tracer(__name__)
        >>> store = PostgreSQLSnapshotStore(session_factory, tracer=tracer)

    Note:
        Ensure the snapshots table exists before using this store.
        Use `get_schema("snapshots")` from migrations to create it.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        tracer: Tracer | None = None,
    ) -> None:
        """
        Initialize the PostgreSQL snapshot store.

        Args:
            session_factory: SQLAlchemy async session factory.
                           Should be configured with expire_on_commit=False
                           for best performance.
            tracer: Optional OpenTelemetry tracer for distributed tracing.
                   If provided, spans are created for each operation.
        """
        self._session_factory = session_factory
        self._tracer = tracer
        logger.debug("PostgreSQLSnapshotStore initialized")

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot using upsert.

        Uses INSERT ... ON CONFLICT DO UPDATE to atomically insert
        or replace the snapshot for an aggregate.

        Args:
            snapshot: The snapshot to save
        """
        span_context = (
            self._tracer.start_as_current_span(
                "snapshot_store.save",
                attributes={
                    "snapshot.aggregate_id": str(snapshot.aggregate_id),
                    "snapshot.aggregate_type": snapshot.aggregate_type,
                    "snapshot.version": snapshot.version,
                    "snapshot.schema_version": snapshot.schema_version,
                },
            )
            if self._tracer
            else contextlib.nullcontext()
        )

        with span_context:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        text("""
                            INSERT INTO snapshots (
                                aggregate_id,
                                aggregate_type,
                                version,
                                schema_version,
                                state,
                                created_at
                            ) VALUES (
                                :aggregate_id,
                                :aggregate_type,
                                :version,
                                :schema_version,
                                :state,
                                :created_at
                            )
                            ON CONFLICT (aggregate_id, aggregate_type)
                            DO UPDATE SET
                                version = EXCLUDED.version,
                                schema_version = EXCLUDED.schema_version,
                                state = EXCLUDED.state,
                                created_at = EXCLUDED.created_at
                        """),
                        {
                            "aggregate_id": snapshot.aggregate_id,
                            "aggregate_type": snapshot.aggregate_type,
                            "version": snapshot.version,
                            "schema_version": snapshot.schema_version,
                            "state": json.dumps(snapshot.state),
                            "created_at": snapshot.created_at,
                        },
                    )

        logger.debug(
            "Saved snapshot for %s/%s at version %d",
            snapshot.aggregate_type,
            snapshot.aggregate_id,
            snapshot.version,
        )

    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """
        Get the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            The snapshot if found, None otherwise
        """
        span_context = (
            self._tracer.start_as_current_span(
                "snapshot_store.get",
                attributes={
                    "snapshot.aggregate_id": str(aggregate_id),
                    "snapshot.aggregate_type": aggregate_type,
                },
            )
            if self._tracer
            else contextlib.nullcontext()
        )

        with span_context as span:
            async with self._session_factory() as session:
                result = await session.execute(
                    text("""
                        SELECT
                            aggregate_id,
                            aggregate_type,
                            version,
                            schema_version,
                            state,
                            created_at
                        FROM snapshots
                        WHERE aggregate_id = :aggregate_id
                          AND aggregate_type = :aggregate_type
                    """),
                    {
                        "aggregate_id": aggregate_id,
                        "aggregate_type": aggregate_type,
                    },
                )
                row = result.fetchone()

            if row is None:
                if span:
                    span.set_attribute("snapshot.found", False)
                logger.debug(
                    "No snapshot found for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
                return None

            # Parse state from JSON
            state = row.state
            if isinstance(state, str):
                state = json.loads(state)

            snapshot = Snapshot(
                aggregate_id=row.aggregate_id,
                aggregate_type=row.aggregate_type,
                version=row.version,
                schema_version=row.schema_version,
                state=state,
                created_at=row.created_at,
            )

            if span:
                span.set_attribute("snapshot.found", True)
                span.set_attribute("snapshot.version", snapshot.version)

            logger.debug(
                "Retrieved snapshot for %s/%s at version %d",
                aggregate_type,
                aggregate_id,
                snapshot.version,
            )

            return snapshot

    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Delete the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot was deleted, False otherwise
        """
        span_context = (
            self._tracer.start_as_current_span(
                "snapshot_store.delete",
                attributes={
                    "snapshot.aggregate_id": str(aggregate_id),
                    "snapshot.aggregate_type": aggregate_type,
                },
            )
            if self._tracer
            else contextlib.nullcontext()
        )

        with span_context as span:
            async with self._session_factory() as session, session.begin():
                result = await session.execute(
                    text("""
                            DELETE FROM snapshots
                            WHERE aggregate_id = :aggregate_id
                              AND aggregate_type = :aggregate_type
                        """),
                    {
                        "aggregate_id": aggregate_id,
                        "aggregate_type": aggregate_type,
                    },
                )
                deleted = cast(int, result.rowcount) > 0  # type: ignore[attr-defined]

            if span:
                span.set_attribute("snapshot.deleted", deleted)

        if deleted:
            logger.debug(
                "Deleted snapshot for %s/%s",
                aggregate_type,
                aggregate_id,
            )
        else:
            logger.debug(
                "No snapshot to delete for %s/%s",
                aggregate_type,
                aggregate_id,
            )

        return deleted

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists (optimized query).

        Uses EXISTS for efficiency instead of fetching full row.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if snapshot exists, False otherwise
        """
        span_context = (
            self._tracer.start_as_current_span(
                "snapshot_store.exists",
                attributes={
                    "snapshot.aggregate_id": str(aggregate_id),
                    "snapshot.aggregate_type": aggregate_type,
                },
            )
            if self._tracer
            else contextlib.nullcontext()
        )

        with span_context as span:
            async with self._session_factory() as session:
                result = await session.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM snapshots
                            WHERE aggregate_id = :aggregate_id
                              AND aggregate_type = :aggregate_type
                        )
                    """),
                    {
                        "aggregate_id": aggregate_id,
                        "aggregate_type": aggregate_type,
                    },
                )
                exists = result.scalar() or False

            if span:
                span.set_attribute("snapshot.exists", exists)

            return exists

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete all snapshots for a given aggregate type.

        Useful for bulk invalidation during schema migrations.

        Args:
            aggregate_type: Type name of aggregates
            schema_version_below: If provided, only delete snapshots with
                                 schema_version < this value

        Returns:
            Number of snapshots deleted
        """
        span_context = (
            self._tracer.start_as_current_span(
                "snapshot_store.delete_by_type",
                attributes={
                    "snapshot.aggregate_type": aggregate_type,
                    "snapshot.schema_version_below": schema_version_below or -1,
                },
            )
            if self._tracer
            else contextlib.nullcontext()
        )

        with span_context as span:
            async with self._session_factory() as session, session.begin():
                if schema_version_below is not None:
                    result = await session.execute(
                        text("""
                                DELETE FROM snapshots
                                WHERE aggregate_type = :aggregate_type
                                  AND schema_version < :schema_version_below
                            """),
                        {
                            "aggregate_type": aggregate_type,
                            "schema_version_below": schema_version_below,
                        },
                    )
                else:
                    result = await session.execute(
                        text("""
                                DELETE FROM snapshots
                                WHERE aggregate_type = :aggregate_type
                            """),
                        {
                            "aggregate_type": aggregate_type,
                        },
                    )
                count = cast(int, result.rowcount)  # type: ignore[attr-defined]

            if span:
                span.set_attribute("snapshot.deleted_count", count)

        if count > 0:
            logger.info(
                "Deleted %d snapshots for aggregate type %s%s",
                count,
                aggregate_type,
                f" (schema_version < {schema_version_below})" if schema_version_below else "",
            )

        return count

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Get the session factory for external use (e.g., transactions)."""
        return self._session_factory

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"PostgreSQLSnapshotStore(tracer={'enabled' if self._tracer else 'disabled'})"
