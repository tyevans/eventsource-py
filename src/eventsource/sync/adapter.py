"""
Synchronous adapter for async EventStore implementations.

This module provides SyncEventStoreAdapter, which wraps any async EventStore
implementation and provides synchronous versions of all its methods.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, TypeVar
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ReadOptions,
    StoredEvent,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class SyncEventStoreAdapter:
    """
    Synchronous adapter for async EventStore implementations.

    Provides sync versions of all EventStore methods for use in
    synchronous contexts like Celery tasks, Django management commands,
    or RQ workers.

    Handles three event loop scenarios:
    1. No event loop exists -> uses asyncio.run()
    2. Event loop exists but not running -> uses loop.run_until_complete()
    3. Event loop is running -> uses run_coroutine_threadsafe() with thread pool

    Thread Safety:
        The adapter is thread-safe for concurrent calls from multiple threads.
        Each sync call is independent and uses appropriate synchronization.

    Example:
        >>> from eventsource.stores import PostgreSQLEventStore
        >>> from eventsource.sync import SyncEventStoreAdapter
        >>>
        >>> async_store = PostgreSQLEventStore(database_url)
        >>> sync_store = SyncEventStoreAdapter(async_store, timeout=30.0)
        >>>
        >>> # In a Celery task
        >>> @celery.task
        >>> def process_order(order_id: str):
        ...     events = sync_store.get_events_sync(UUID(order_id), "Order")
        ...     # Process events...
        ...     sync_store.append_events_sync(
        ...         aggregate_id=UUID(order_id),
        ...         aggregate_type="Order",
        ...         events=[new_event],
        ...         expected_version=events.version,
        ...     )

    Warning:
        Using this adapter from within a running async context (e.g., an
        async view that calls a sync library) will log a warning and use
        run_coroutine_threadsafe(), which has additional overhead. Consider
        using the async EventStore directly in async contexts.
    """

    # Class-level executor for running loop case
    _executor: ThreadPoolExecutor | None = None
    _executor_lock: threading.Lock = threading.Lock()

    def __init__(
        self,
        event_store: EventStore,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialize the sync adapter.

        Args:
            event_store: The async EventStore to wrap
            timeout: Default timeout in seconds for all operations (default: 30.0)

        Raises:
            TypeError: If event_store doesn't implement EventStore protocol
        """
        if not isinstance(event_store, EventStore):
            raise TypeError(
                f"event_store must be an EventStore instance, got {type(event_store).__name__}"
            )
        self._store = event_store
        self._timeout = timeout

    @classmethod
    def _get_executor(cls) -> ThreadPoolExecutor:
        """Get or create the shared thread pool executor."""
        with cls._executor_lock:
            if cls._executor is None:
                cls._executor = ThreadPoolExecutor(
                    max_workers=4,
                    thread_name_prefix="sync_adapter",
                )
            return cls._executor

    @classmethod
    def shutdown_executor(cls) -> None:
        """
        Shutdown the shared thread pool executor.

        Call this during application shutdown to clean up resources.
        After calling this, the executor will be recreated on next use.
        """
        with cls._executor_lock:
            if cls._executor is not None:
                cls._executor.shutdown(wait=True)
                cls._executor = None

    def _run_sync(self, coro: Coroutine[Any, Any, T], timeout: float | None = None) -> T:
        """
        Execute coroutine synchronously, handling all event loop scenarios.

        Args:
            coro: The coroutine to execute
            timeout: Optional timeout override

        Returns:
            The result of the coroutine

        Raises:
            TimeoutError: If operation exceeds timeout
            Exception: Any exception raised by the coroutine
        """
        effective_timeout = timeout if timeout is not None else self._timeout

        try:
            # Check if there's a running event loop
            loop = asyncio.get_running_loop()

            # We're in a running loop - use thread pool
            logger.warning(
                "SyncEventStoreAdapter called from running event loop. "
                "Consider using async EventStore directly for better performance."
            )

            future = asyncio.run_coroutine_threadsafe(coro, loop)

            try:
                return future.result(timeout=effective_timeout)
            except TimeoutError:
                future.cancel()
                raise TimeoutError(
                    f"Sync operation timed out after {effective_timeout}s "
                    "(called from running event loop)"
                ) from None

        except RuntimeError:
            # No running event loop - this is the common case for sync contexts
            pass

        # Create a new event loop with asyncio.run()
        # This is the recommended approach for Python 3.10+
        try:
            return asyncio.run(asyncio.wait_for(coro, timeout=effective_timeout))
        except TimeoutError as e:
            raise TimeoutError(f"Sync operation timed out after {effective_timeout}s") from e

    def append_events_sync(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: Sequence[DomainEvent],
        expected_version: int,
        *,
        timeout: float | None = None,
    ) -> AppendResult:
        """
        Synchronously append events to an aggregate stream.

        Args:
            aggregate_id: The aggregate's unique identifier
            aggregate_type: The type name of the aggregate
            events: Sequence of events to append
            expected_version: Expected current version for optimistic locking
            timeout: Override default timeout for this operation

        Returns:
            AppendResult with new version and global position

        Raises:
            OptimisticLockError: If expected_version doesn't match current
            TimeoutError: If operation exceeds timeout
            EventStoreError: If storage operation fails
        """
        return self._run_sync(
            self._store.append_events(aggregate_id, aggregate_type, list(events), expected_version),
            timeout=timeout,
        )

    def get_events_sync(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        *,
        timeout: float | None = None,
    ) -> EventStream:
        """
        Synchronously get events for an aggregate.

        Args:
            aggregate_id: The aggregate's unique identifier
            aggregate_type: Optional type filter
            from_version: Start from this version (0 = all events)
            from_timestamp: Only get events after this timestamp
            to_timestamp: Only get events before this timestamp
            timeout: Override default timeout for this operation

        Returns:
            EventStream with events and metadata
        """
        return self._run_sync(
            self._store.get_events(
                aggregate_id,
                aggregate_type,
                from_version,
                from_timestamp,
                to_timestamp,
            ),
            timeout=timeout,
        )

    def get_events_by_type_sync(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
        *,
        timeout: float | None = None,
    ) -> list[DomainEvent]:
        """
        Synchronously get all events for a specific aggregate type.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order')
            tenant_id: Filter by tenant (optional, for multi-tenant systems)
            from_timestamp: Only get events after this datetime (optional)
            timeout: Override default timeout for this operation

        Returns:
            List of events in chronological order
        """
        return self._run_sync(
            self._store.get_events_by_type(aggregate_type, tenant_id, from_timestamp),
            timeout=timeout,
        )

    def get_stream_version_sync(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        *,
        timeout: float | None = None,
    ) -> int:
        """
        Synchronously get the current version of an aggregate stream.

        Args:
            aggregate_id: The aggregate's unique identifier
            aggregate_type: Type of aggregate
            timeout: Override default timeout for this operation

        Returns:
            Current stream version (0 if no events)
        """
        return self._run_sync(
            self._store.get_stream_version(aggregate_id, aggregate_type),
            timeout=timeout,
        )

    def event_exists_sync(
        self,
        event_id: UUID,
        *,
        timeout: float | None = None,
    ) -> bool:
        """
        Synchronously check if an event exists.

        Args:
            event_id: The event's unique identifier
            timeout: Override default timeout for this operation

        Returns:
            True if event exists, False otherwise
        """
        return self._run_sync(
            self._store.event_exists(event_id),
            timeout=timeout,
        )

    def read_all_sync(
        self,
        options: ReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[StoredEvent]:
        """
        Synchronously read events across all aggregates.

        Note: This method collects all events from the async iterator into a list.
        For large event stores, consider using pagination via ReadOptions.limit.

        Args:
            options: Read options for filtering and pagination
            timeout: Override default timeout for this operation

        Returns:
            List of StoredEvent with matching events
        """

        async def collect_events() -> list[StoredEvent]:
            events: list[StoredEvent] = []
            async for stored_event in self._store.read_all(options):
                events.append(stored_event)
            return events

        return self._run_sync(collect_events(), timeout=timeout)

    def get_global_position_sync(
        self,
        *,
        timeout: float | None = None,
    ) -> int:
        """
        Synchronously get the current maximum global position.

        Args:
            timeout: Override default timeout for this operation

        Returns:
            The maximum global position, or 0 if the store is empty.
        """
        return self._run_sync(
            self._store.get_global_position(),
            timeout=timeout,
        )

    @property
    def wrapped_store(self) -> EventStore:
        """Get the underlying async EventStore."""
        return self._store

    @property
    def timeout(self) -> float:
        """Get the default timeout for operations."""
        return self._timeout

    def __repr__(self) -> str:
        """Return string representation."""
        return f"SyncEventStoreAdapter({type(self._store).__name__}, timeout={self._timeout})"


__all__ = ["SyncEventStoreAdapter"]
