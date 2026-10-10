"""
StatusStreamer core - Real-time migration status streaming iterator.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.ports.migration.models import MigrationStatus

if TYPE_CHECKING:
    from eventsource.application.migration.coordinator import MigrationCoordinator

logger = logging.getLogger(__name__)


class StatusStreamer:
    """
    Real-time status streaming for migration monitoring.

    Provides an async iterator interface for streaming MigrationStatus updates
    to clients. Supports multiple simultaneous subscribers per migration and
    automatically cleans up resources when subscriptions end or migrations complete.

    The streamer uses the observer pattern internally, registering with the
    MigrationCoordinator to receive status update notifications. Updates are
    pushed to subscribers when:
    - Phase changes occur
    - Progress updates are recorded
    - Errors occur
    - The configurable update interval elapses

    Thread Safety:
        This class is designed for asyncio and is not thread-safe. All operations
        should be performed within the same event loop.

    Attributes:
        _coordinator: MigrationCoordinator instance for fetching status.
        _migration_id: UUID of the migration to stream status for.
        _subscribers: Dictionary of active subscriber queues by subscriber ID.
        _cleanup_task: Background task for cleanup on migration completion.

    Example:
        >>> streamer = StatusStreamer(coordinator, migration_id)
        >>> async for status in streamer.stream_status():
        ...     print(f"Migration {status.migration_id}: {status.phase.value}")
        ...     if status.phase.is_terminal:
        ...         break
    """

    def __init__(
        self,
        coordinator: MigrationCoordinator,
        migration_id: UUID,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the StatusStreamer.

        Args:
            coordinator: MigrationCoordinator instance for fetching status
            migration_id: UUID of the migration to stream
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._coordinator = coordinator
        self._migration_id = migration_id
        self._subscribers: dict[int, asyncio.Queue[MigrationStatus]] = {}
        self._next_subscriber_id = 0
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def migration_id(self) -> UUID:
        """Get the migration ID being streamed."""
        return self._migration_id

    @property
    def subscriber_count(self) -> int:
        """Get the number of active subscribers."""
        return len(self._subscribers)

    @property
    def is_closed(self) -> bool:
        """Check if the streamer has been closed."""
        return self._closed

    async def stream_status(
        self,
        *,
        update_interval: float = 1.0,
        include_initial: bool = True,
    ) -> AsyncIterator[MigrationStatus]:
        """
        Stream migration status updates as an async iterator.

        Yields MigrationStatus updates as they occur, including phase changes,
        progress updates, and errors. The iterator completes when:
        - The migration reaches a terminal state (COMPLETED, ABORTED, FAILED)
        - The subscriber explicitly breaks from the loop
        - The streamer is closed

        Args:
            update_interval: Seconds between forced status checks (default 1.0).
                Even without explicit updates from the coordinator, the status
                will be polled at this interval to ensure progress is reported.
            include_initial: Whether to yield the initial status immediately
                (default True).

        Yields:
            MigrationStatus: Current migration status on each update.

        Raises:
            ValueError: If update_interval is <= 0.
            RuntimeError: If streamer has been closed.

        Example:
            >>> async for status in streamer.stream_status(update_interval=0.5):
            ...     print(f"Progress: {status.progress_percent:.1f}%")
            ...     if status.phase == MigrationPhase.COMPLETED:
            ...         print("Migration completed!")
            ...         break
        """
        if update_interval <= 0:
            raise ValueError(f"update_interval must be > 0, got {update_interval}")

        if self._closed:
            raise RuntimeError("StatusStreamer has been closed")

        with self._tracer.span(
            "eventsource.status_streamer.stream_status",
            {
                "migration.id": str(self._migration_id),
                "update_interval": update_interval,
            },
        ):
            # Create subscriber queue and register
            subscriber_id = await self._register_subscriber()

            try:
                # Optionally yield initial status
                if include_initial:
                    initial_status = await self._get_current_status()
                    if initial_status is not None:
                        yield initial_status
                        if initial_status.phase.is_terminal:
                            return

                # Stream updates
                queue = self._subscribers.get(subscriber_id)
                if queue is None:
                    return

                # Also register with coordinator for notifications
                notification_queue: asyncio.Queue[UUID] = asyncio.Queue(maxsize=100)
                self._coordinator.register_status_queue(self._migration_id, notification_queue)

                try:
                    last_status: MigrationStatus | None = None

                    while not self._closed:
                        try:  # noqa: SIM105 - Need to continue after timeout
                            # Wait for notification or timeout
                            await asyncio.wait_for(
                                notification_queue.get(),
                                timeout=update_interval,
                            )
                        except TimeoutError:
                            # Timeout - poll for update anyway
                            pass

                        # Get current status
                        status = await self._get_current_status()
                        if status is None:
                            # Migration not found - stop streaming
                            logger.warning(
                                "Migration %s not found during streaming",
                                self._migration_id,
                            )
                            return

                        # Yield if status changed or this is a periodic update
                        if last_status is None or self._status_changed(last_status, status):
                            yield status
                            last_status = status

                        # Stop if terminal
                        if status.phase.is_terminal:
                            logger.info(
                                "Migration %s reached terminal state %s, stopping stream",
                                self._migration_id,
                                status.phase.value,
                            )
                            return

                finally:
                    # Unregister from coordinator
                    self._coordinator.unregister_status_queue(
                        self._migration_id, notification_queue
                    )

            finally:
                # Unregister subscriber
                await self._unregister_subscriber(subscriber_id)

    async def _register_subscriber(self) -> int:
        """
        Register a new subscriber and return its ID.

        Returns:
            Subscriber ID for tracking.
        """
        async with self._lock:
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            self._subscribers[subscriber_id] = asyncio.Queue(maxsize=100)

            logger.debug(
                "Registered subscriber %d for migration %s (total: %d)",
                subscriber_id,
                self._migration_id,
                len(self._subscribers),
            )

            return subscriber_id

    async def _unregister_subscriber(self, subscriber_id: int) -> None:
        """
        Unregister a subscriber.

        Args:
            subscriber_id: ID of subscriber to unregister.
        """
        async with self._lock:
            if subscriber_id in self._subscribers:
                del self._subscribers[subscriber_id]

                logger.debug(
                    "Unregistered subscriber %d for migration %s (remaining: %d)",
                    subscriber_id,
                    self._migration_id,
                    len(self._subscribers),
                )

    async def _get_current_status(self) -> MigrationStatus | None:
        """
        Get the current migration status from the coordinator.

        Returns:
            MigrationStatus or None if migration not found.
        """
        try:
            return await self._coordinator.get_status(self._migration_id)
        except Exception:
            # Migration not found
            return None

    def _status_changed(
        self,
        previous: MigrationStatus,
        current: MigrationStatus,
    ) -> bool:
        """
        Check if the status has changed significantly.

        A status is considered changed if:
        - Phase changed
        - Progress changed by at least 1%
        - Sync lag changed by at least 10 events
        - Error count increased
        - Pause state changed

        Args:
            previous: Previous status.
            current: Current status.

        Returns:
            True if status has changed significantly.
        """
        # Phase change is always significant
        if previous.phase != current.phase:
            return True

        # Progress change of at least 1%
        if abs(previous.progress_percent - current.progress_percent) >= 1.0:
            return True

        # Significant sync lag change
        if abs(previous.sync_lag_events - current.sync_lag_events) >= 10:
            return True

        # Error count increased
        if current.error_count > previous.error_count:
            return True

        # Pause state changed
        return previous.is_paused != current.is_paused

    async def close(self) -> None:
        """
        Close the streamer and clean up resources.

        Marks the streamer as closed, which will cause all active
        stream_status() iterators to complete on their next iteration.
        """
        with self._tracer.span(
            "eventsource.status_streamer.close",
            {"migration.id": str(self._migration_id)},
        ):
            async with self._lock:
                self._closed = True
                self._subscribers.clear()

            logger.info(
                "StatusStreamer closed for migration %s",
                self._migration_id,
            )


__all__ = ["StatusStreamer"]
