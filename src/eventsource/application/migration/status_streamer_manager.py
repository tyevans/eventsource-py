"""
StatusStreamManager - Manager for multiple StatusStreamer instances.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.status_streamer_core import StatusStreamer
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.application.migration.coordinator import MigrationCoordinator

logger = logging.getLogger(__name__)


class StatusStreamManager:
    """
    Manager for multiple StatusStreamer instances.

    Provides centralized management of status streamers across multiple
    migrations. Automatically creates and reuses streamers, and handles
    cleanup when migrations complete.

    This class is useful when you need to manage status streaming for
    many migrations from a single point, such as an API endpoint or
    monitoring service.

    Thread Safety:
        This class is designed for asyncio and is not thread-safe.

    Attributes:
        _coordinator: MigrationCoordinator instance.
        _streamers: Dictionary of active streamers by migration ID.

    Example:
        >>> manager = StatusStreamManager(coordinator)
        >>>
        >>> # Get or create streamer for a migration
        >>> streamer = await manager.get_streamer(migration_id)
        >>>
        >>> # Stream status
        >>> async for status in streamer.stream_status():
        ...     handle_status(status)
        >>>
        >>> # Cleanup when done
        >>> await manager.close_all()
    """

    def __init__(
        self,
        coordinator: MigrationCoordinator,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the StatusStreamManager.

        Args:
            coordinator: MigrationCoordinator instance.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._coordinator = coordinator
        self._streamers: dict[UUID, StatusStreamer] = {}
        self._lock = asyncio.Lock()

    @property
    def active_streamers(self) -> int:
        """Get the number of active streamers."""
        return len(self._streamers)

    async def get_streamer(self, migration_id: UUID) -> StatusStreamer:
        """
        Get or create a StatusStreamer for a migration.

        If a streamer already exists for the migration, it is returned.
        Otherwise, a new streamer is created.

        Args:
            migration_id: UUID of the migration.

        Returns:
            StatusStreamer for the migration.
        """
        with self._tracer.span(
            "eventsource.status_stream_manager.get_streamer",
            {"migration.id": str(migration_id)},
        ):
            async with self._lock:
                if migration_id not in self._streamers:
                    self._streamers[migration_id] = StatusStreamer(
                        coordinator=self._coordinator,
                        migration_id=migration_id,
                        enable_tracing=self._enable_tracing,
                    )
                    logger.debug(
                        "Created new streamer for migration %s",
                        migration_id,
                    )

                return self._streamers[migration_id]

    async def close_streamer(self, migration_id: UUID) -> None:
        """
        Close and remove a specific streamer.

        Args:
            migration_id: UUID of the migration.
        """
        with self._tracer.span(
            "eventsource.status_stream_manager.close_streamer",
            {"migration.id": str(migration_id)},
        ):
            async with self._lock:
                if migration_id in self._streamers:
                    streamer = self._streamers.pop(migration_id)
                    await streamer.close()

    async def close_all(self) -> None:
        """
        Close all active streamers.

        Typically called during shutdown to clean up resources.
        """
        with self._tracer.span(
            "eventsource.status_stream_manager.close_all",
            {},
        ):
            async with self._lock:
                for streamer in self._streamers.values():
                    await streamer.close()
                self._streamers.clear()

            logger.info("Closed all status streamers")

    async def cleanup_terminal_migrations(self) -> int:
        """
        Clean up streamers for migrations in terminal states.

        Checks each active streamer's migration status and closes
        those that have reached terminal states (COMPLETED, ABORTED, FAILED).

        Returns:
            Number of streamers cleaned up.
        """
        with self._tracer.span(
            "eventsource.status_stream_manager.cleanup_terminal_migrations",
            {},
        ):
            to_remove: list[UUID] = []

            async with self._lock:
                for migration_id, _streamer in self._streamers.items():
                    try:
                        status = await self._coordinator.get_status(migration_id)
                        if status.phase.is_terminal:
                            to_remove.append(migration_id)
                    except Exception:
                        # Migration not found - remove streamer
                        to_remove.append(migration_id)

            # Close removed streamers outside lock
            for migration_id in to_remove:
                await self.close_streamer(migration_id)

            if to_remove:
                logger.info(
                    "Cleaned up %d terminal migration streamers",
                    len(to_remove),
                )

            return len(to_remove)


__all__ = ["StatusStreamManager"]
