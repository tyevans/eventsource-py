"""StatusStreamManager - Manager for multiple StatusStreamer instances."""

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
    """Centralized manager for multiple StatusStreamer instances across migrations."""

    def __init__(
        self,
        coordinator: MigrationCoordinator,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the StatusStreamManager."""
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
        """Get or create a StatusStreamer for a migration."""
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
                    logger.debug("Created new streamer for migration %s", migration_id)
                return self._streamers[migration_id]

    async def close_streamer(self, migration_id: UUID) -> None:
        """Close and remove a specific streamer."""
        with self._tracer.span(
            "eventsource.status_stream_manager.close_streamer",
            {"migration.id": str(migration_id)},
        ):
            async with self._lock:
                if migration_id in self._streamers:
                    streamer = self._streamers.pop(migration_id)
                    await streamer.close()

    async def close_all(self) -> None:
        """Close all active streamers."""
        with self._tracer.span("eventsource.status_stream_manager.close_all", {}):
            async with self._lock:
                for streamer in self._streamers.values():
                    await streamer.close()
                self._streamers.clear()
            logger.info("Closed all status streamers")

    async def cleanup_terminal_migrations(self) -> int:
        """Clean up streamers for migrations in terminal states (COMPLETED, ABORTED, FAILED)."""
        with self._tracer.span(
            "eventsource.status_stream_manager.cleanup_terminal_migrations",
            {},
        ):
            to_remove: list[UUID] = []
            async with self._lock:
                for migration_id in self._streamers:
                    try:
                        status = await self._coordinator.get_status(migration_id)
                        if status.phase.is_terminal:
                            to_remove.append(migration_id)
                    except Exception:
                        to_remove.append(migration_id)

            for migration_id in to_remove:
                await self.close_streamer(migration_id)

            if to_remove:
                logger.info("Cleaned up %d terminal migration streamers", len(to_remove))
            return len(to_remove)


__all__ = ["StatusStreamManager"]
